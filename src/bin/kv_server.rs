use futures_util::StreamExt;
use structopt::StructOpt;
use tokio::net::signal;
use tracing::{error, info, info_span};
use tracing_futures::Instrument;
use kv::{Error, Node, Server, SledStorage, MemStorage};
use raft::eraftpb::ConfState;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "kv server", about = "A consitient kv store.")]
#[allow(non_camel_case_types)]
enum Opts {
    bootstrap { id: u64 },
    join { id: u64, target: u64 },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    kv::init_tracing("kv_server")?;

    info!("Starting the KV Store.");

    let peer_file = "peers.txt";

    info!(message = "Loading peers from.", file = %peer_file);

    let peers = kv::load_peers(peer_file).await?;

    let id = match opts {
        Opts::bootstrap { id, .. } => id,
        Opts::join { id, .. } => id,
    };

    let bind = peers.get(&id).expect("Provided id not in peers list.");

    let mut server = Server::new(*bind, peers.clone());

    let raft_inbound_events = server.start()?;

    let node_span = info_span!("node");
    let enter = node_span.enter();

    let mut node = match opts {
        Opts::bootstrap { .. } => {
            let storage = MemStorage::new_with_conf_state(ConfState::from((vec![id], vec![])));
            Node::bootstrap(id, peers, raft_inbound_events, storage)?
        },
        Opts::join { target, .. } => {
            let storage = MemStorage::default();
            Node::join(id, target, peers, raft_inbound_events, storage)
                .instrument(node_span.clone())
                .await?
        }
    };

    kv::spawn(
        async move {
            info!("Starting raft module.");
            if let Err(error) = node.run().await {
                error!(message = "Node error.", %error);
            }
        },
        node_span.clone(),
    );

    drop(enter);

    signal::ctrl_c()?.next().await;

    Ok(())
}
