use tokio::net::TcpStream;
use std::sync::Arc;
use crate::server::ServerState;
use crate::simple_sublist::{SubListTrait, ArcSubscription, ArcSubResult, Subscription};
use tokio::sync::Mutex;
use tokio::io::*;
use std::collections::{HashMap, BTreeSet};
use crate::parser::{SubArg, Parser, ParseResult, PubArg};
use rand::{SeedableRng, RngCore};
use crate::error::{NError, ERROR_CONNECTION_CLOSED};
use bitflags::_core::cmp::Ordering;
use std::ops::Deref;

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

    async fn send_all(&mut self) -> std::io::Result<()> {
        let writer = &mut self.writer;
        let r = writer.write_all(self.msg_buf.as_ref().unwrap().as_slice()).await;
        self.msg_buf.as_mut().unwrap().clear();
        r
    }
}

#[derive(Debug, Clone)]
pub struct ClientMessageSenderWrapper(Arc<Mutex<ClientMessageSender>>, usize);

impl std::cmp::PartialEq for ClientMessageSenderWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

//
// 为了能够将ArcSubscription,必须实现下面这些Trait
//
impl std::cmp::Eq for ClientMessageSenderWrapper {}

impl std::cmp::PartialOrd for ClientMessageSenderWrapper {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for ClientMessageSenderWrapper {
    fn cmp(&self, other: &Self) -> Ordering {
        self.1.cmp(&other.1)
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
        let mut buf = [0; 1024 * 64];
        let mut parser = Parser::new();
        let mut subs = HashMap::new();
        let mut rng = rand::rngs::StdRng::from_entropy();
        let mut cache = HashMap::new();
        let mut pendings = BTreeSet::new();// 可以考虑使用Vec代替而不是vec,因为目前好像不需要查找
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
            let mut parse_buf = &buf[0..n];
            loop {
                let r = parser.parse(&parse_buf[..]);
                if r.is_err() {
                    self.process_error(r.unwrap_err(), subs).await;
                    return;
                }
                let (result, left) = r.unwrap();

                match result {
                    ParseResult::NoMsg => break,
                    ParseResult::Sub(ref sub) => {
                        println!("parse result:{:?}", sub);
                        if let Err(e) = self.process_sub(sub, &mut subs).await {
                            self.process_error(e, subs).await;
                            return;
                        }
                    }
                    ParseResult::Pub(ref pub_arg) => {
                        println!("parse result:{:?}", pub_arg);
                        if let Err(e) = self.process_pub_with_cache(pub_arg, &mut cache, &mut rng, &mut pendings).await {
                            self.process_error(e, subs).await;
                            return;
                        }
                        parser.clear_msg_buf();
                    }
                }
                // 读完消息,break
                if left == buf.len() {
                    break;
                }
                parse_buf = &parse_buf[left..];
            }
            //批量处理发送
            for c in pendings.iter() {
                let c = c.clone();
                tokio::spawn(async move {
                    let mut sender = c.0.lock().await;
                    if let Err(e) = sender.send_all().await {
                        println!("send_all error {}", e);
                    }
                });
            }
            pendings.clear();
        }
    }

    async fn process_error<E: std::error::Error>(&self, err: E, subs: HashMap<String, ArcSubscription>) {
        println!("client {} process err {:?}", self.cid, err);
        // 删除所有的订阅
        {
            let sublist = &mut self.srv.lock().await.sublist;
            for (_, sub) in subs {
                if let Err(e) = sublist.remove(sub) {
                    println!("client {} remove err {} ", self.cid, e);
                }
            }
        }

        let mut sender = self.msg_sender.lock().await;
        sender.msg_buf.take();
        let writer = &mut sender.writer;
        // 关闭连接
        if let Err(e) = writer.shutdown().await {
            println!("shutdown err {:?}", e);
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
            self.send_message(sub.as_ref(), pub_arg).await.map_err(|e| {
                println!("send message error {}", e);
                NError::new(ERROR_CONNECTION_CLOSED)
            })?;
        }
        // qsubs考虑负载均衡的问题
        let mut rng = rand::rngs::StdRng::from_entropy();
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

    #[allow(dead_code)]
    async fn process_pub_with_cache(
        &self,
        pub_arg: &PubArg<'_>,
        cache: &mut HashMap<String, ArcSubResult>,
        rng: &mut rand::rngs::StdRng,
        pendings: &mut BTreeSet<ClientMessageSenderWrapper>,
    ) -> crate::error::Result<()> {
        // 从cache中取,没有从server中取,再插入缓存中,取出所有sub了某个subject(topic)的client
        let sub_result = {
            if let Some(r) = cache.get(pub_arg.subject) {
                r.clone()
            } else {
                let sub_list = &mut self.srv.lock().await.sublist;
                let r = sub_list.match_subject(pub_arg.subject);
                cache.insert(pub_arg.subject.to_ascii_lowercase(), Arc::clone(&r));
                r
            }
        };
        // 遍历发送数据
        if sub_result.psubs.len() > 0 {
            for sub in sub_result.psubs.iter() {
                self.send_message_pending(sub.as_ref(), pub_arg, pendings)
                    .await
                    .map_err(|e| {
                        println!("send message error {}", e);
                        NError::new(ERROR_CONNECTION_CLOSED)
                    })?;
            }
        }
        if sub_result.qsubs.len() > 0 {
            //qsubs 要考虑负载均衡问题
            for qsubs in sub_result.qsubs.iter() {
                let n = rng.next_u32();
                let n = n as usize % qsubs.len();
                let sub = qsubs.get(n).unwrap();
                self.send_message_pending(sub.as_ref(), pub_arg, pendings)
                    .await
                    .map_err(|_| NError::new(ERROR_CONNECTION_CLOSED))?;
            }
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

    async fn send_message_pending(
        &self,
        sub: &Subscription,
        pub_arg: &PubArg<'_>,
        pendings: &mut BTreeSet<ClientMessageSenderWrapper>,
    ) -> std::io::Result<()> {
        let mut msg_sender = sub.msg_sender.lock().await;
        if let Some(ref mut msg_buf) = msg_sender.msg_buf {
            let id = msg_sender.deref() as *const ClientMessageSender as usize;
            let msg_buf = msg_sender.msg_buf.as_mut().unwrap();

            msg_buf.extend_from_slice("MSG ".as_bytes());
            msg_buf.extend_from_slice(sub.subject.as_bytes());
            msg_buf.extend_from_slice(" ".as_bytes());
            msg_buf.extend_from_slice(sub.sid.as_bytes());
            msg_buf.extend_from_slice(" ".as_bytes());
            msg_buf.extend_from_slice(pub_arg.size_buf.as_bytes());
            msg_buf.extend_from_slice("\r\n".as_bytes());
            msg_buf.extend_from_slice(pub_arg.msg); //经测试,如果这里不使用缓存,而是多个await,性能会大幅下降.
            msg_buf.extend_from_slice("\r\n".as_bytes());
            pendings.insert(ClientMessageSenderWrapper(sub.msg_sender.clone(), id));
        }
        Ok(())
    }
}