mod network;
mod node;
mod storage;

pub mod pb;

pub use network::{Peers, Server};
pub use storage::{MemStorage, SledStorage};
pub use node::Node;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

use std::{future::Future, net::SocketAddr, path::Path};
use tracing::debug;
use tracing_futures::Instrument;

pub async fn load_peers(file: impl AsRef<Path>) -> Result<Peers, crate::Error> {
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

pub fn spawn(f: impl Future<Output = ()> + Send + 'static, span: tracing::Span) {
    tokio::spawn(f.instrument(span));
}

pub fn init_tracing(name: &'static str) -> Result<(), Error> {
    if let Ok(_) = std::env::var("RUST_LOG") {
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
            .finish();

        tracing::subscriber::set_global_default(subscriber)?;
    } else {
        let subscriber = tracing_subscriber::fmt::Subscriber::builder()
            .with_env_filter(format!("kv=info,{}=info", name))
            .finish();

        tracing::subscriber::set_global_default(subscriber)?;
    }

    tracing_log::LogTracer::init()?;

    Ok(())
}
