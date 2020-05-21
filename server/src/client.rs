use tokio::net::TcpStream;
use std::sync::Arc;
use crate::server::ServerState;
use crate::simple_sublist::{SubListTrait, ArcSubscription, ArcSubResult, Subscription};
use tokio::sync::Mutex;
use tokio::io::*;
use std::collections::{HashMap, BTreeSet};
use crate::parser::{SubArg, Parser, ParseResult, PubArg};
use bitflags::_core::cmp::Ordering;
use rand::{SeedableRng, RngCore};
use crate::error::{NError, ERROR_CONNECTION_CLOSED};
use bitflags::_core::ops::Deref;

#[derive(Debug)]
pub struct Client<T: SubListTrait> {
    pub srv: Arc<Mutex<ServerState<T>>>,
    pub cid: u64,
    pub msg_sender: Arc<Mutex<ClientMessageSender>>,
}

#[derive(Debug)]
pub struct ClientMessageSender {
    writer: Option<WriteHalf<TcpStream>>,
    msg_buf: Option<Vec<u8>>,
}

impl ClientMessageSender {
    pub fn new(writer: WriteHalf<TcpStream>) -> Self {
        Self {
            writer: Some(writer),
            msg_buf: Some(Vec::with_capacity(512)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientMessageSenderWrapper(Arc<Mutex<ClientMessageSender>>, usize);

impl std::cmp::PartialEq for ClientMessageSenderWrapper {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
/*
为了能够将ArcSubscription,必须实现下面这些Trait
*/
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
        let mut rng = rand::rngs::StdRng::from_entropy();
        let mut cache = HashMap::new();
        let mut pendings = BTreeSet::new();
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
            let mut _buf = &buf[..];
            loop {
                let r = parser.parse(&_buf);
                if r.is_err() {
                    {
                        let err_desc = unsafe { std::str::from_utf8_unchecked(&_buf[..]) };
                        println!("parse err: {}", err_desc);
                    }
                    self.process_error(r.unwrap_err(), subs).await;
                    return;
                }
                let (result, left) = r.unwrap();
                match result {
                    ParseResult::NoMsg => break,
                    ParseResult::Sub(ref sub) => {
                        if let Err(e) = self.process_sub(sub, &mut subs).await {
                            self.process_error(e, subs).await;
                            return;
                        }
                    }
                    ParseResult::Pub(ref pub_arg) => {
                        if let Err(e) = self
                            .process_pub(pub_arg, &mut cache, &mut rng, &mut pendings)
                            .await
                        {
                            self.process_error(e, subs).await;
                            return;
                        }
                        parser.clear_msg_buf();
                    }
                }
                if left == _buf.len() {
                    break;
                }
                _buf = &buf[left..]
            }
        }
    }

    async fn process_error<E: std::error::Error>(&self, err: E, subs: HashMap<String, ArcSubscription>) {
        println!("client {} process err {:?}", self.cid, err);
        {
            let sublist = &mut self.srv.lock().await.sublist;
            for (_, sub) in subs {
                if let Err(e) = sublist.remove(sub) {
                    println!("client {} remove err {} ", self.cid, e);
                }
            }
        }
        let mut sender = self.msg_sender.lock().await;
        if let Some(mut writer) = sender.writer.take() {
            sender.msg_buf.take();
            if let Err(e) = writer.shutdown().await {
                println!("shutdown err {:?}", e);
            }
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
        cache: &mut HashMap<String, ArcSubResult>,
        rng: &mut rand::rngs::StdRng,
        pendings: &mut BTreeSet<ClientMessageSenderWrapper>,
    ) -> crate::error::Result<()> {
        let sub_result = {
            let sub_list = &mut self.srv.lock().await.sublist;
            let r = sub_list.match_subject(pub_arg.subject);
            cache.insert(pub_arg.subject.to_string(), Arc::clone(&r));
            r
        };
        if sub_result.psubs.len() > 0 {
            for sub in sub_result.psubs.iter() {
                self.send_message(sub.as_ref(),pub_arg,pendings)
                    .await
                    .map_err(|_|NError::new(ERROR_CONNECTION_CLOSED))?;
            }
        }
        if sub_result.qsubs.len() > 0 {
            //qsubs 要考虑负载均衡问题
            for qsubs in sub_result.qsubs.iter() {
                let n = rng.next_u32();
                let n = n as usize % qsubs.len();
                let sub = qsubs.get(n).unwrap();
                self.send_message(sub.as_ref(), pub_arg, pendings)
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
        pendings: &mut BTreeSet<ClientMessageSenderWrapper>,
    ) -> std::io::Result<()> {
        let mut msg_sender = &mut self.msg_sender.lock().await;
        if let Some(ref mut msg_buf) =msg_sender.msg_buf {
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