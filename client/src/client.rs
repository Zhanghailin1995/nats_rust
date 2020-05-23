use std::sync::Arc;
use tokio::io::*;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashMap;
use crate::parser::{Parser, ParseResult, MsgArg};
use futures::FutureExt;

type MessageHandler = Box<dyn FnMut(&[u8]) -> std::result::Result<(), ()> + Sync + Send>;

pub struct Client {
    addr: String,
    writer: Arc<Mutex<WriteHalf<TcpStream>>>,
    pub stop: Option<oneshot::Sender<()>>,
    sid: u64,
    handler: Arc<Mutex<HashMap<String, MessageHandler>>>,
}

impl Client {
    // 连接服务器
    #[allow(dead_code)]
    pub async fn connect(addr: &str) -> std::io::Result<Client> {
        // 建立到服务器的连接
        // 启动到后台任务
        let conn = TcpStream::connect(addr).await?;
        let (reader, writer) = tokio::io::split(conn);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let c = Client {
            addr: addr.into(),
            writer: Arc::new(Mutex::new(writer)),
            stop: Some(tx),
            sid: 0,
            handler: Arc::new(Default::default()),
        };
        // 一定要clone,因为tokio::spawn(move) 会move所有权,但是我们又要返回c,这是肯定不行的,算是一个技巧吧
        let handler = c.handler.clone();
        let writer = c.writer.clone();
        tokio::spawn(async move {
            //Self::receive_task(reader,rx,c.handler.clone(),c.writer.clone()).await;
            Self::receive_task(reader, rx, handler, writer).await;
        });
        Ok(c)
    }

    // 向服务器发布一条pub消息
    // pub消息格式为PUB subject size\r\n{message}
    #[allow(dead_code)]
    pub async fn pub_message(&mut self, subject: &str, msg: &[u8]) -> std::io::Result<()> {
        let mut writer = self.writer.lock().await;
        /*let unsafe_msg = unsafe {
            std::str::from_utf8_unchecked(msg)
        };*/
        //let m = format!("PUB {} {}\r\n{}\r\n", subject, msg.len());
        let m = format!("PUB {} {}\r\n", subject, msg.len());
        let _ = writer.write_all(m.as_bytes()).await;
        let _ = writer.write_all(msg).await;
        writer.write_all("\r\n".as_bytes()).await
    }

    // 向服务器发布一条sub消息,然后等待服务器推送相关消息
    // sub消息格式为SUB subject {queue} {sid} \r\n
    #[allow(dead_code)]
    pub async fn sub_message(&mut self, subject: String, queue: Option<String>, handler: MessageHandler) -> std::io::Result<()> {
        self.sid += 1;
        let mut writer = self.writer.lock().await;
        let m = if let Some(queue) = queue {
            format!("SUB {} {} {}\r\n", subject.as_str(), queue, self.sid)
        } else {
            format!("SUB {} {}\r\n", subject.as_str(), self.sid)
        };
        self.handler.lock().await.insert(subject,handler);
        writer.write_all(m.as_bytes()).await
    }

    pub fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            stop.send(());
        }
    }

    // 从服务器接收消息
    // 然后推送给相关的订阅方
    #[allow(dead_code)]
    async fn receive_task(mut reader: ReadHalf<TcpStream>,
                          stop: oneshot::Receiver<()>,
                          handler: Arc<Mutex<HashMap<String, MessageHandler>>>,
                          writer: Arc<Mutex<WriteHalf<TcpStream>>>,
    ) {
        let mut buf = [0 as u8; 512];
        let mut parser = Parser::new();
        use futures::*;
        let mut stop = stop.fuse();
        loop {
            select! {
                _ = stop => {
                    println!("client stopped.");
                    let r = writer.lock().await.shutdown().await;
                    if r.is_err() {
                        println!("receive_task err {:?}", r.unwrap_err());
                        return;
                    }
                    return;
                },
                r = reader.read(&mut buf[..]).fuse()=>{
                    let n = match r {
                        Err(e) => {
                            println!("read err {}", e);
                            let _ = writer.lock().await.shutdown().await;
                            return;
                        }
                        Ok(n) => n,
                    };
                    if n == 0 {
                        // EOF,说明对方关闭了连接
                        return;
                    }
                    let mut buf = &buf[0..n];

                    loop {
                        let result = parser.parse(buf);
                        let (r, n) = match result {
                            Err(e) => {
                                println!("read err {}", e);
                                let _ = writer.lock().await.shutdown().await;
                                return;
                            }
                            Ok(r) => r
                        };

                        match r {
                            ParseResult::NoMsg => {
                                break;
                            }
                            ParseResult::MsgArg(msg) => {
                                Self::process_message(msg, &handler).await;
                                parser.clear_msg_buf();
                            }
                        }

                        if n == buf.len() {
                            break;
                        }
                        buf = &buf[n..];
                    }
                }
            }
        }

    }

    // 根据消息的subject,找到订阅方,然后推送给他们
    pub async fn process_message(msg: MsgArg<'_>,
                                 handler: &Arc<Mutex<HashMap<String, MessageHandler>>>,
    )
    {
        let mut handler = handler.lock().await;
        let h = handler.get_mut(msg.subject);
        if let Some(h) = h {
            let _ = h(msg.msg);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {}
}


