use crate::{network, pb, storage::Storage};
use bytes::BytesMut;
use prost::Message;
use raft::{
    eraftpb::{ConfChange, ConfChangeType, EntryType, Snapshot},
    RawNode,
};
use sled::Db;
use slog::{Drain, OwnedKVList, Record};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::{future::FutureExt, sync::oneshot, timer};
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub enum Control {
    Propose(Proposal, oneshot::Sender<()>),
    Raft(pb::Message),
}

#[derive(Debug)]
pub enum Proposal {
    AddNode { id: u64 },
    InternalMessage(pb::InternalRaftMessage),
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RequestId(u8);

/// The kv node that contains all the state.
pub struct Node<S: Storage> {
    id: u64,
    raft: RawNode<S>,
    raft_inbound_events: network::RaftInboundEvents,
    router: network::RaftPeerRouter,
    next_request_id: RequestId,
    in_flight_proposals: HashMap<RequestId, oneshot::Sender<()>>,
    db: Db,
}

impl<S: Storage> Node<S> {
    pub fn bootstrap(
        id: u64,
        peers: network::Peers,
        raft_inbound_events: network::RaftInboundEvents,
        storage: S,
        db: Db,
    ) -> Result<Self, crate::Error> {
        info!("Bootstraping raft.");
        let mut me = Self::new(id, peers, raft_inbound_events, storage, db)?;
        for _ in 0..10 {
            me.raft.tick();
        }
        Ok(me)
    }

    pub async fn join(
        id: u64,
        target: u64,
        peers: network::Peers,
        mut raft_inbound_events: network::RaftInboundEvents,
        storage: S,
        db: Db,
    ) -> Result<Self, crate::Error> {
        let target = peers.get(&target).expect("Target id not in peer list!");

        info!(message = "Attempting to join.", joining_via = %target);

        let dst = format!("http://{}", target);
        let mut client = network::KvClient::connect(dst)?;

        loop {
            let req = pb::JoinRequest { id };
            match client.join(tonic::Request::new(req)).await {
                Ok(_) => {
                    info!(message = "Connected to peer.", %target);
                    break;
                }
                Err(error) => {
                    warn!(message = "Unable to connect to client; Retrying in 3 sec.", %error);
                    timer::delay_for(Duration::from_secs(3)).await;
                }
            }
        }

        info!("Waiting for next inbound raft message");
        while let Some(msg) = raft_inbound_events.recv().await {
            if let Control::Raft(msg) = msg {
                use pb::MessageType::*;

                let msg_type = pb::MessageType::from_i32(msg.msg_type);

                if Some(MsgRequestVote) == msg_type
                    || Some(MsgRequestPreVote) == msg_type
                    || Some(MsgHeartbeat) == msg_type && msg.commit == 0
                {
                    info!(message = "Recieved inbound raft message.", incoming_id = %msg.to, commit = %msg.commit);
                    let mut me = Self::new(id, peers, raft_inbound_events, storage, db)?;
                    me.raft.step(msg.into())?;
                    return Ok(me);
                }
            }
        }

        panic!("Unable to join!");
    }

    fn new(
        id: u64,
        peers: network::Peers,
        raft_inbound_events: network::RaftInboundEvents,
        storage: S,
        db: Db,
    ) -> Result<Self, crate::Error> {
        let config = raft::Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        config.validate()?;

        // let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
        // let temp_dir = std::env::temp_dir();
        // let storage = Storage::new(format!("{:?}/{}.log", temp_dir, id))?;
        let logger = slog::Logger::root(slog::Fuse(SlogTracer::default()), slog::o!());

        let raft = RawNode::new(&config, storage, &logger)?;

        let router = network::RaftPeerRouter::new(peers);

        Ok(Self {
            id,
            raft,
            raft_inbound_events,
            router,
            next_request_id: RequestId(0),
            in_flight_proposals: HashMap::new(),
            db,
        })
    }

    pub async fn run(&mut self) -> Result<(), crate::Error> {
        let timeout = Duration::from_millis(100);
        let mut remaining_timeout = timeout;

        let debug_timeout = Duration::from_secs(15);
        let mut debug_remaining_timeout = timeout;

        loop {
            let now = Instant::now();

            let msg = match self
                .raft_inbound_events
                .recv()
                .timeout(Duration::from_millis(100))
                .await
            {
                Ok(Some(msg)) => Some(msg),
                Ok(None) => return Ok(()),
                Err(_) => None,
            };

            if let Some(msg) = msg {
                match msg {
                    Control::Propose(proposal, tx) => {
                        debug!(message = "Inbound proposal.", ?proposal);
                        self.handle_proposal(proposal, tx)?;
                    }
                    Control::Raft(m) => {
                        trace!(message = "Inbound raft message.", message = ?m);
                        self.raft.step(m.into())?;
                    }
                }
            }

            let elapsed = now.elapsed();
            if elapsed >= debug_remaining_timeout {
                debug_remaining_timeout = debug_timeout;

                let raft::Raft {
                    id,
                    term,
                    state,
                    leader_id,
                    raft_log,
                    ..
                } = &self.raft.raft;
                let raft::RaftLog {
                    committed, applied, ..
                } = raft_log;

                let config = self.raft.raft.prs().configuration();

                let voters = config.voters().len();
                let learners = config.learners().len();

                info!(
                    %id,
                    %term,
                    ?state,
                    %voters,
                    %learners,
                    %leader_id,
                    %committed,
                    %applied,
                );
            } else {
                debug_remaining_timeout -= elapsed;
            }

            if elapsed >= remaining_timeout {
                remaining_timeout = timeout;

                // We drive Raft every 100ms.
                trace!("ticking raft node.");
                self.raft.tick();
            } else {
                remaining_timeout -= elapsed;
            }

            if self.raft.has_ready() {
                trace!("Handling ready state");
                self.handle_ready().await?;
            }
        }
    }

    fn handle_proposal(
        &mut self,
        proposal: Proposal,
        tx: oneshot::Sender<()>,
    ) -> Result<(), crate::Error> {
        let req_id = self.next_request_id.0.wrapping_add(1);
        self.in_flight_proposals.insert(RequestId(req_id), tx);
        let req_id_bytes = vec![req_id];

        match proposal {
            Proposal::AddNode { id } => {
                let mut conf_change = ConfChange::default();
                conf_change.node_id = id;
                conf_change.set_change_type(ConfChangeType::AddNode);
                self.raft.propose_conf_change(req_id_bytes, conf_change)?;
            }
            Proposal::InternalMessage(request) => {
                let mut buf = BytesMut::with_capacity(request.encoded_len());
                if let Err(error) = request.encode(&mut buf) {
                    error!(message = "Unable to propose request.", %error);
                    return Ok(());
                }

                self.raft.propose(req_id_bytes, buf.into_iter().collect())?;
            }
        }

        Ok(())
    }

    fn notify_proposal(&mut self, context: Vec<u8>) {
        if let Some(notifier) = self.in_flight_proposals.remove(&RequestId(context[0])) {
            let _ = notifier.send(());
        }
    }

    async fn handle_ready(&mut self) -> Result<(), crate::Error> {
        let mut ready = self.raft.ready();
        let store = self.raft.mut_store();

        if let Err(error) = store.append(ready.entries()) {
            error!(message = "Append entries failed.", %error);
            return Ok(());
        }

        if *ready.snapshot() != Snapshot::default() {
            if let Err(error) = store.apply_snapshot(ready.snapshot().clone()) {
                error!(message = "Apply snapshot failed.", %error);
                return Ok(());
            }
        }

        for msg in ready.messages.drain(..) {
            //if msg.to != self.id {
            self.router.send(msg.to, msg.into()).await?;
            //}
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in &committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }

                let entry_type = EntryType::from_i32(entry.entry_type);

                if let Some(EntryType::EntryConfChange) = entry_type {
                    let mut cc = ConfChange::default();
                    cc.merge(&entry.data).unwrap();

                    let cs = self.raft.apply_conf_change(&cc)?;
                    self.raft.mut_store().set_conf_state(cs)?;
                    self.notify_proposal(entry.context.clone());
                }

                if let Some(EntryType::EntryNormal) = entry_type {
                    let mut internal_raft_message = pb::InternalRaftMessage::default();
                    internal_raft_message.merge(&entry.data).unwrap();

                    if let Err(error) = self.apply(internal_raft_message) {
                        error!(message = "Unable to apply entry.", %error);
                        // TODO: return an error to the user
                    }

                    self.notify_proposal(entry.context.clone());
                }
            }

            if let Some(entry) = committed_entries.last() {
                self.raft
                    .mut_store()
                    .set_hard_state(entry.index, entry.term)?;
            }
        }

        self.raft.advance(ready);

        Ok(())
    }

    fn apply(&mut self, req: pb::InternalRaftMessage) -> Result<(), crate::Error> {
        if let Some(put) = req.put {
            self.db.insert(put.key, put.value)?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub(crate) struct SlogTracer;

impl Drain for SlogTracer {
    type Ok = ();
    type Err = ();

    fn log(&self, record: &Record, _values: &OwnedKVList) -> Result<Self::Ok, Self::Err> {
        let span = tracing::info_span!("raft");
        let _enter = span.enter();

        match record.level() {
            slog::Level::Error | slog::Level::Critical => tracing::error!("{}", record.msg()),
            slog::Level::Warning => tracing::warn!("{}", record.msg()),
            slog::Level::Info => tracing::info!("{}", record.msg()),
            slog::Level::Debug => tracing::debug!("{}", record.msg()),
            slog::Level::Trace => tracing::debug!("{}", record.msg()),
        }

        Ok(())
    }
}
