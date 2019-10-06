use crate::{network, pb};
use raft::{
    eraftpb::{ConfChange, ConfChangeType, Snapshot, EntryType},
    storage::MemStorage,
    RawNode,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use prost::Message;
use tokio::{future::FutureExt, sync::oneshot, timer};
use tracing::{info, warn, trace, error};
use slog::{Drain, OwnedKVList, Record};

#[derive(Debug)]
pub enum Control {
    Propose(Proposal, oneshot::Sender<()>),
    Raft(pb::Message),
}

#[derive(Debug)]
pub enum Proposal {
    AddNode { id: u64 },
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct RequestId(u8);

/// The kv node that contains all the state.
pub struct Node {
    #[allow(unused)]
    id: u64,
    raft: RawNode<MemStorage>,
    raft_inbound_events: network::RaftInboundEvents,
    router: network::RaftPeerRouter,
    next_request_id: RequestId,
    in_flight_proposals: HashMap<RequestId, oneshot::Sender<()>>,
}

impl Node {
    pub fn bootstrap(
        id: u64,
        peers: network::Peers,
        raft_inbound_events: network::RaftInboundEvents,
    ) -> Result<Self, crate::Error> {
        info!("Bootstraping raft.");
        Self::new(id, peers, raft_inbound_events)
    }

    pub async fn join(
        id: u64,
        target: u64,
        peers: network::Peers,
        mut raft_inbound_events: network::RaftInboundEvents,
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
            if let Control::Raft(pb::Message {
                msg_type, commit, ..
            }) = msg
            {
                use pb::MessageType::*;

                let msg_type = pb::MessageType::from_i32(msg_type);

                if Some(MsgRequestVote) == msg_type
                    || Some(MsgRequestPreVote)  == msg_type
                    || Some(MsgHeartbeat) == msg_type && commit == 0
                {
                    info!(message = "Recieved inbound raft message.", %commit);
                    return Self::new(id, peers, raft_inbound_events);
                }
            }
        }

        panic!("Unable to join!");
    }

    fn new(
        id: u64,
        peers: network::Peers,
        raft_inbound_events: network::RaftInboundEvents,
    ) -> Result<Self, crate::Error> {
        let config = raft::Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        config.validate()?;

        let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
        let logger = slog::Logger::root(slog::Fuse(SlogTracer::default()), slog::o!());

        let raft = RawNode::new(&config, storage, &logger)?;

        let router = network::RaftPeerRouter::new(peers);

        Ok(Self {
            id,
            raft,
            raft_inbound_events,
            router,
            next_request_id: RequestId(0),
            in_flight_proposals: HashMap::new()
        })
    }

    pub async fn run(&mut self) -> Result<(), crate::Error> {
        let timeout = Duration::from_millis(100);
        let mut remaining_timeout = timeout;

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
                    Control::Propose(proposal, tx) => self.handle_proposal(proposal, tx)?,
                    Control::Raft(m) => self.raft.step(m.into())?,
                }
            }

            let elapsed = now.elapsed();
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

    fn handle_proposal(&mut self, proposal: Proposal, tx: oneshot::Sender<()>) -> Result<(), crate::Error> {
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

        if let Err(error) = store.wl().append(ready.entries()) {
            error!(message = "Append entries failed.", %error);
            return Ok(());
        }

        if *ready.snapshot() != Snapshot::default() {
            if let Err(error) = store.wl().append(ready.entries()) {
                error!(message = "Apply snapshot failed.", %error);
                return Ok(());
            }
        }

        for msg in ready.messages.drain(..) {
            if msg.to != self.id {
                self.router.send(msg.to, msg.into()).await?;
            }
        }

        if let Some(committed_entries) = ready.committed_entries.take() {
            for entry in committed_entries {
                if entry.data.is_empty() {
                    // From new elected leaders.
                    continue;
                }

                if let Some(EntryType::EntryConfChange) = EntryType::from_i32(entry.entry_type) {
                    let mut cc = ConfChange::default();
                    cc.merge(&entry.data).unwrap();

                    let cs = self.raft.apply_conf_change(&cc)?.clone();
                    self.raft.mut_store().wl().set_conf_state(cs, None);
                    self.notify_proposal(entry.context);
                }
            }
        }

        self.raft.advance(ready);

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
