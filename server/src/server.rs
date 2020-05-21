use crate::simple_sublist::SubListTrait;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use std::collections::HashMap;
use crate::client::{ClientMessageSender, Client};
use std::error::Error;

#[derive(Debug, Default)]
pub struct Server<T: SubListTrait> {
    state: Arc<Mutex<ServerState<T>>>
}

#[derive(Debug, Default)]
pub struct ServerState<T: SubListTrait> {
    // u64 为id,clients为全局client集合,每个client拥有全局唯一的id
    clients: HashMap<u64, Arc<Mutex<ClientMessageSender>>>,
    pub sublist: T,
    pub gen_cid: u64,
}

impl<T: SubListTrait + Send + 'static> Server<T> {
    pub async fn start(self) -> Result<(),Box<dyn Error>> {
        let addr = "0.0.0.0:4222";
        let mut listener = TcpListener::bind(addr).await?;
        loop {
            let (conn,_) = listener.accept().await?;
            self.new_client(conn).await;
        }
    }

    async fn new_client(&self, conn: TcpStream) {
        let state = self.state.clone();
        let cid = {
            let mut state = state.lock().await;
            state.gen_cid += 1;
            state.gen_cid
        };
        let c = Client::process_connection(cid, state, conn);
        self.state.lock().await.clients.insert(cid, c);
    }
}