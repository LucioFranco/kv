use futures_util::StreamExt;
use kv::{pb, Error};
use std::net::SocketAddr;
use structopt::StructOpt;
use tokio::net::signal;
use tonic::Request;
use tracing::{error, debug};

#[derive(Clone, Debug, StructOpt)]
#[structopt(name = "kv client", about = "A consitient kv store client")]
struct Opts {
    #[structopt(subcommand)]
    command: Command,

    #[structopt(short, long)]
    endpoint: Option<SocketAddr>,

    #[structopt(short, long)]
    peer_id: Option<u64>,

    #[structopt(long)]
    peer_config: Option<String>,
}

#[derive(Debug, Clone, StructOpt)]
#[allow(non_camel_case_types)]
enum Command {
    put { key: String, value: String },
    get { key: String },
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    tokio::spawn(async move {
        signal::ctrl_c().unwrap().next().await;
        std::process::exit(0);
    });

    kv::init_tracing("kv_server")?;

    let dst = if let Some(endpoint) = &opts.endpoint {
        endpoint.to_string()
    } else {
        let peer_file = opts
            .peer_config
            .clone()
            .unwrap_or_else(|| "peers.txt".to_string());

        debug!(message = "Loading peers from.", file = %peer_file);

        let peers = kv::load_peers(peer_file).await?;

        if let Some(peer_id) = &opts.peer_id {
            peers.get(peer_id).expect("Invalid peer id").to_string()
        } else {
            peers[&1].to_string()
        }
    };

    let mut client = pb::client::KvClient::connect(format!("http://{}", dst))?;

    match opts.command {
        Command::put { key, value } => {
            let put = pb::PutRequest {
                key: key.into_bytes(),
                value: value.into_bytes(),
            };

            debug!(message = "Sending put request.");
            match client.put(Request::new(put)).await {
                Ok(response) => {
                    debug!(message = "Put rpc completed.", ?response);
                    println!("Ok");
                },
                Err(error) => error!(message = "Put rpc failed.", %error),
            }
        }

        Command::get { key } => {
            let get = pb::RangeRequest {
                key: key.into_bytes(),
                range_end: Vec::new(),
            };

            debug!(message = "Sending get request.");
            match client.range(Request::new(get)).await {
                Ok(response) => {
                    for kv in &response.get_ref().kvs {
                        let s = String::from_utf8_lossy(&kv.value[..]);
                        println!("{}", s);
                    }

                    debug!(message = "Get rpc completed.", ?response);
                },
                Err(error) => error!(message = "Get rpc failed.", %error),
            }
        }
    }

    Ok(())
}
