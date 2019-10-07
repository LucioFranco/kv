use crate::{
    node,
    pb::{self, client, server},
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tokio::sync::{mpsc, oneshot};
use tonic::{transport, Code, Request, Response, Status, Streaming};
use tracing::{debug, error, info, info_span, warn};

type RaftInboundSender = mpsc::Sender<node::Control>;
pub type Peers = Arc<HashMap<u64, SocketAddr>>;
pub type RaftInboundEvents = mpsc::Receiver<node::Control>;
pub type KvClient = client::KvClient<transport::Channel>;

#[derive(Debug)]
pub struct Server {
    bind: SocketAddr,
    peers: Peers,
    // TODO: add shutdown handle here.
}

impl Server {
    pub fn new(bind: SocketAddr, peers: Peers) -> Self {
        Self { bind, peers }
    }

    pub fn start(&mut self) -> Result<RaftInboundEvents, crate::Error> {
        let server_span = info_span!("server", bind = %self.bind);
        let _enter = server_span.enter();

        info!(message = "Starting server.", listening = %self.bind);

        let (raft_inbound_tx, raft_inbound_rx) = mpsc::channel(1024);

        let server = KvServer {
            inbound_tx: raft_inbound_tx,
            peers: self.peers.clone(),
        };

        let bind = self.bind;

        crate::spawn(
            async move {
                if let Err(error) = transport::Server::builder()
                    .serve(bind, server::KvServer::new(server))
                    .await
                {
                    error!(message = "Transport server error.", %error);
                }
            },
            server_span.clone(),
        );

        Ok(raft_inbound_rx)
    }
}

#[derive(Clone, Debug)]
pub struct RaftPeerRouter {
    sender: mpsc::Sender<(u64, pb::Message)>,
}

impl RaftPeerRouter {
    pub fn new(peers: Peers) -> Self {
        let (tx, rx) = mpsc::channel(1024);

        let mut bg = RaftPeerRouterTask::new(peers, rx);

        crate::spawn(
            async move {
                if let Err(error) = bg.run().await {
                    error!(message = "Raft router background task error.", %error);
                }
            },
            info_span!("background"),
        );

        Self { sender: tx }
    }

    pub async fn send(&mut self, to: u64, msg: pb::Message) -> Result<(), crate::Error> {
        self.sender.send((to, msg)).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RaftPeerRouterTask {
    peers: Peers,
    raft_events: mpsc::Receiver<(u64, pb::Message)>,
    connections: HashMap<u64, mpsc::Sender<pb::Message>>,
}

impl RaftPeerRouterTask {
    pub fn new(peers: Peers, raft_events: mpsc::Receiver<(u64, pb::Message)>) -> Self {
        Self {
            peers,
            raft_events,
            connections: HashMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), crate::Error> {
        while let Some((id, msg)) = self.raft_events.recv().await {
            if !self.connections.contains_key(&id) {
                if let Some(addr) = self.peers.get(&id) {
                    let (tx, rx) = mpsc::channel(1024);

                    self.connections.insert(id, tx);

                    let addr = *addr;
                    crate::spawn(
                        async move {
                            if let Err(error) = connect_to_peer(addr, rx).await {
                                error!(message = "Error connecting to peer.", %error);
                            }
                        },
                        info_span!("connection", %id, %addr),
                    );
                } else {
                    warn!(message = "Attempting to connection to peer that does not exist.", %id);
                }
            }

            if let Some(conn) = self.connections.get_mut(&id) {
                if let Err(_) = conn.send(msg).await {
                    self.connections.remove(&id);
                }
            }
        }

        Ok(())
    }
}

async fn connect_to_peer(
    peer: SocketAddr,
    mut rx: mpsc::Receiver<pb::Message>,
) -> Result<KvClient, crate::Error> {
    let dst = format!("http://{}", peer);
    let mut client = KvClient::connect(dst)?;

    client.handshake(Request::new(())).await?;

    let stream = async_stream::try_stream! {
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    };

    client.raft(Request::new(stream)).await?;

    Ok(client)
}

#[derive(Debug)]
struct KvServer {
    inbound_tx: RaftInboundSender,
    peers: Peers,
}

#[tonic::async_trait]
impl pb::server::Kv for KvServer {
    async fn join(
        &self,
        req: Request<pb::JoinRequest>,
    ) -> Result<Response<pb::JoinResponse>, Status> {
        let id = req.into_inner().id;

        info!(message = "Join request.", from = %id);

        if self.peers.contains_key(&id) {
            let (tx, rx) = oneshot::channel();
            let proposal = node::Control::Propose(node::Proposal::AddNode { id }, tx);

            debug!(message = "Submitting proposal for join.", %id);
            self.inbound_tx
                .clone()
                .send(proposal)
                .await
                .map_err(|_| Status::new(Code::Internal, "Inbound sender channel dropped."))?;

            rx.await
                .map_err(|_| Status::new(Code::Internal, "Proposal sender dropped."))?;

            Ok(Response::new(pb::JoinResponse { joined: true }))
        } else {
            Ok(Response::new(pb::JoinResponse { joined: false }))
        }
    }

    async fn put(&self, _req: Request<pb::PutRequest>) -> Result<Response<pb::PutResponse>, Status> {
        Ok(Response::new(pb::PutResponse {}))
    }

    async fn raft(&self, mut req: Request<Streaming<pb::Message>>) -> Result<Response<()>, Status> {
        while let Some(msg) = req.get_mut().message().await? {
            let msg = node::Control::Raft(msg.into());
            debug!(message = "Inbound message", ?msg);
            self.inbound_tx
                .clone()
                .send(msg)
                .await
                .map_err(|_| Status::new(Code::Internal, "Internal raft process shutdown"))?;
        }

        Ok(Response::new(()))
    }

    async fn handshake(&self, _: Request<()>) -> Result<Response<()>, Status> {
        Ok(Response::new(()))
    }
}
