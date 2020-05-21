use std::error::Error;
use crate::server::Server;
use crate::simple_sublist::{SimpleSubList};

mod client;
mod parser;
mod error;
mod server;
mod simple_sublist;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("server start..");
    let s: Server<SimpleSubList> = Server::default();
    s.start().await
}


