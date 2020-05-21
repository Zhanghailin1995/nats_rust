use tokio::net::TcpStream;
use std::sync::Arc;
use crate::server::ServerState;
use crate::simple_sublist::{SubListTrait, ArcSubscription, ArcSubResult, Subscription};
use tokio::sync::Mutex;
use tokio::io::*;
use std::collections::{HashMap};
use crate::parser::{SubArg, Parser, ParseResult, PubArg};
use rand::{SeedableRng, RngCore};
use crate::error::{NError, ERROR_CONNECTION_CLOSED};

#[derive(Debug)]
pub struct Client<T: SubListTrait> {
    pub srv: Arc<Mutex<ServerState<T>>>,
    pub cid: u64,
    pub msg_sender: Arc<Mutex<ClientMessageSender>>,
}

#[derive(Debug)]
pub struct ClientMessageSender {
    writer: WriteHalf<TcpStream>,
    msg_buf: Option<Vec<u8>>,
}

impl ClientMessageSender {
    pub fn new(writer: WriteHalf<TcpStream>) -> Self {
        Self {
            writer,
            msg_buf: Some(Vec::with_capacity(512)),
        }
    }
}


impl<T: SubListTrait + Send + 'static> Client<T> {
    #[allow(dead_code)]
    pub fn process_connection(
        cid: u64,
        srv: Arc<Mutex<ServerState<T>>>,
        conn: TcpStream,
    ) -> Arc<Mutex<ClientMessageSender>> {
        let (reader, writer) = tokio::io::split(conn);
        let msg_sender = Arc::new(Mutex::new(ClientMessageSender::new(writer)));
        let c = Client {
            srv,
            cid,
            msg_sender: msg_sender.clone(),
        };
        tokio::spawn(async move {
            Client::client_task(c, reader).await;
            println!("client {} client task quit", cid);
        });
        msg_sender
    }

    async fn client_task(self, mut reader: ReadHalf<TcpStream>) {
        let mut buf = [0; 1024];
        let mut parser = Parser::new();
        let mut subs = HashMap::new();
        loop {
            let r = reader.read(&mut buf[..]).await;
            if r.is_err() {
                let e = r.unwrap_err();
                self.process_error(e, subs).await;
                return;
            }
            let r = r.unwrap();
            let n = r;
            if n == 0 {
                self.process_error(NError::new(ERROR_CONNECTION_CLOSED), subs).await;
                return;
            }
            let mut buf = &buf[0..n];
            loop {
                let r = parser.parse(&buf[..]);
                if r.is_err() {
                    self.process_error(r.unwrap_err(), subs).await;
                    return;
                }
                let (result, left) = r.unwrap();

                match result {
                    ParseResult::NoMsg => break,
                    ParseResult::Sub(ref sub) => {
                        println!("parse result:{:?}",sub);
                        if let Err(e) = self.process_sub(sub, &mut subs).await {
                            self.process_error(e, subs).await;
                            return;
                        }
                    }
                    ParseResult::Pub(ref pub_arg) => {
                        println!("parse result:{:?}",pub_arg);
                        if let Err(e) = self.process_pub(pub_arg).await {
                            self.process_error(e, subs).await;
                            return;
                        }
                    }
                }
                // 读完消息,break
                if left == buf.len() {
                    break;
                }
                buf = &buf[left..];
            }
        }
    }

    async fn process_error<E: std::error::Error>(&self, err: E, mut subs: HashMap<String, ArcSubscription>) {
        println!("client {} process err {:?}", self.cid, err);
        // 删除所有的订阅
        {
            let mut sublist = &mut self.srv.lock().await.sublist;
            for (_,sub) in subs {
                sublist.remove(sub);
            }
        }
        // 关闭连接
        let r = self.msg_sender.lock().await.writer.shutdown().await;
        if r.is_err() {
            println!("shutdown err{:?}",r.unwrap_err());
        }
    }

    #[allow(dead_code)]
    async fn process_sub(
        &self,
        sub: &SubArg<'_>,
        subs: &mut HashMap<String, ArcSubscription>,
    ) -> crate::error::Result<()> {
        let sub = Subscription {
            subject: sub.subject.to_string(),
            queue: sub.queue.map(|q| q.to_string()),
            sid: sub.sid.to_string(),
            msg_sender: self.msg_sender.clone(),
        };
        let sub = Arc::new(sub);
        subs.insert(sub.subject.clone(), sub.clone());
        let sublist = &mut self.srv.lock().await.sublist;
        sublist.insert(sub)?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn process_pub(
        &self,
        pub_arg: &PubArg<'_>,
    ) -> crate::error::Result<()> {
        let sub_result = {
            let sub_list = &mut self.srv.lock().await.sublist;
            sub_list.match_subject(pub_arg.subject)
        };
        for sub in sub_result.psubs.iter() {
            self.send_message(sub.as_ref(),pub_arg).await.map_err(|e| {
                println!("send message error {}", e);
                NError::new(ERROR_CONNECTION_CLOSED)
            })?;
        }
        // qsubs考虑负载均衡的问题
        let mut rng= rand::rngs::StdRng::from_entropy();
        for qsubs in sub_result.qsubs.iter() {
            let n = rng.next_u32();
            let n = n as usize % qsubs.len();
            let sub = qsubs.get(n).unwrap();
            self.send_message(sub.as_ref(), pub_arg)
                .await
                .map_err(|_| NError::new(ERROR_CONNECTION_CLOSED))?;
        }
        Ok(())
    }

    ///消息格式
    ///```
    /// MSG <subject> <sid> <size>\r\n
    /// <message>\r\n
    /// ```
    ///
    #[allow(dead_code)]
    async fn send_message(
        &self,
        sub: &Subscription,
        pub_arg: &PubArg<'_>,
    ) -> std::io::Result<()> {
        let writer = &mut sub.msg_sender.lock().await.writer;
        writer.write("MSG ".as_bytes()).await?;
        writer.write(sub.subject.as_bytes()).await?;
        writer.write(" ".as_bytes()).await?;
        writer.write(sub.sid.as_bytes()).await?;
        writer.write(" ".as_bytes()).await?;
        writer.write(pub_arg.size_buf.as_bytes()).await?;
        writer.write("\r\n".as_bytes()).await?;
        writer.write(pub_arg.msg).await?;
        writer.write("\r\n".as_bytes()).await?;
        Ok(())
    }
}