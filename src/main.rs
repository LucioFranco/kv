mod network;
mod node;
mod pb;

use futures_util::StreamExt;
use std::{future::Future, net::SocketAddr, path::Path};
use structopt::StructOpt;
use tokio::net::signal;
use tracing::{debug, error, info, info_span};
use tracing_futures::Instrument;

use network::{Peers, Server};
use node::Node;

pub(crate) type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "kv", about = "A consitient kv store.")]
#[allow(non_camel_case_types)]
pub enum Opts {
    bootstrap { id: u64 },
    join { id: u64, target: u64 },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    let filter = if let Ok(_) = std::env::var("RUST_LOG") {
         tracing_subscriber::filter::EnvFilter::from_default_env()
    } else {
        tracing_subscriber::filter::EnvFilter::from_default_env().add_directive("kv=info".parse()?)
    };

    let subscriber = tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;
    tracing_log::LogTracer::init()?;

    info!("Starting the KV Store.");

    let peer_file = "peers.txt";

    info!(message = "Loading peers from.", file = %peer_file);

    let peers = load_peers(peer_file).await?;

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
        Opts::bootstrap { .. } => Node::bootstrap(id, peers, raft_inbound_events)?,
        Opts::join { target, .. } => {
            Node::join(id, target, peers, raft_inbound_events)
                .instrument(node_span.clone())
                .await?
        }
    };

    crate::spawn(
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

async fn load_peers(file: impl AsRef<Path>) -> Result<Peers, crate::Error> {
    let bytes = tokio::fs::read(file).await?;

    let s = String::from_utf8(bytes)?;

    let mut peers = std::collections::HashMap::new();

    for (id, line) in s.lines().enumerate() {
        let addr = line.parse::<SocketAddr>()?;
        peers.insert(id as u64 + 1, addr);
    }

    debug!(message = "Loaded peers.", ?peers);

    Ok(std::sync::Arc::new(peers))
}

fn spawn(f: impl Future<Output = ()> + Send + 'static, span: tracing::Span) {
    tokio::spawn(f.instrument(span));
}
