pub mod convert;
pub mod server;
pub mod client;

pub mod raft {
    tonic::include_proto!("raft");
}