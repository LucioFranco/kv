use futures_util::StreamExt;
use kv::{Error, MemStorage, Node, Server};
use raft::eraftpb::ConfState;
use structopt::StructOpt;
use tokio::net::signal;
use tracing::{error, info, info_span};
use tracing_futures::Instrument;

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

    let db_file = uuid::Uuid::new_v4();
    let mut db_path = std::env::temp_dir().join(db_file.to_string());
    db_path.set_extension("sled");

    let db = sled::Db::open(db_path)?;

    let mut server = Server::new(*bind, peers.clone(), db.clone());

    let raft_inbound_events = server.start()?;

    let node_span = info_span!("node");
    let enter = node_span.enter();

    let mut node = match opts {
        Opts::bootstrap { .. } => {
            let storage = MemStorage::new_with_conf_state(ConfState::from((vec![id], vec![])));
            Node::bootstrap(id, peers, raft_inbound_events, storage, db)?
        }
        Opts::join { target, .. } => {
            // TODO: find better method to let bootstrapper win its campagin
            tokio::timer::delay_for(std::time::Duration::from_secs(3)).await;
            let storage = MemStorage::new();
            Node::join(id, target, peers, raft_inbound_events, storage, db)
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
