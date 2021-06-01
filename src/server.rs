use tonic::transport::Server;

use crate::follower::Follower;
use crate::leader::Leader;

use proto::replication::leader_service_server::LeaderServiceServer;
use proto::store::store_service_server::StoreServiceServer;

use structopt::StructOpt;

mod follower;
mod leader;
mod proto;
mod store;

#[derive(Debug, StructOpt)]
struct LeaderOpt {}

#[derive(Debug, StructOpt)]
struct FollowerOpt {
    #[structopt(short = "l", long)]
    leader_address: String,
}

#[derive(Debug, StructOpt)]
enum Command {
    Leader(LeaderOpt),
    Follower(FollowerOpt),
}

#[derive(Debug, StructOpt)]
struct ServerOpt {
    #[structopt(subcommand)]
    command: Command,

    #[structopt(short = "a", long)]
    address: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = ServerOpt::from_args();

    let addr = opt.address.parse()?;

    match opt.command {
        Command::Follower(opt) => {
            let leader_address = opt.leader_address.parse()?;
            let follower = Follower::initialize(leader_address).await?;
            Server::builder()
                .add_service(StoreServiceServer::new(follower))
                // TODO: add replica service
                .serve(addr)
                .await?;
        }
        Command::Leader(_) => {
            let leader = Leader::default();
            Server::builder()
                .add_service(StoreServiceServer::new(leader.clone()))
                .add_service(LeaderServiceServer::new(leader))
                .serve(addr)
                .await?;
        }
    }

    Ok(())
}
