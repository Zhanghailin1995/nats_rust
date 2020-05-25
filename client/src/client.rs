use bytes::buf::BufMutExt;
use bytes::{Buf, BytesMut};
use std::sync::Arc;
use tokio::io::*;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use std::collections::HashMap;
use crate::parser::{Parser, ParseResult, MsgArg};
use std::io::Write;

type MessageHandler = Box<dyn FnMut(&[u8]) -> std::result::Result<(), ()> + Sync + Send>;

pub struct Client {
    addr: String,
    writer: Arc<Mutex<WriteHalf<TcpStream>>>,
    msg_buf: Option<BytesMut>,
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
        let msg_sender = Arc::new(Mutex::new(HashMap::new()));
        let writer = Arc::new(Mutex::new(writer));
        tokio::spawn(Self::receive_task(
            reader,
            rx,
            msg_sender.clone(),
            writer.clone(),
        ));
        Ok(Client {
            addr: addr.to_string(),
            writer,
            stop: Some(tx),
            sid: 0,
            handler: msg_sender,
            msg_buf: Some(BytesMut::with_capacity(512)),
        })
    }

    // 向服务器发布一条pub消息
    // pub消息格式为PUB subject size\r\n{message}
    pub async fn pub_message(&mut self, subject: &str, msg: &[u8]) -> std::io::Result<()> {
        // 总感觉这里可以改一下,先take() 在 重新绑定,感觉脱了裤子放屁,多次一举
        // 先这样吧,水平不够,我也不知道怎么改
        let msg_buf = self.msg_buf.take().expect("must have");
        let mut msg_buf_writer = msg_buf.writer();
        msg_buf_writer.write("PUB ".as_bytes())?;
        msg_buf_writer.write(subject.as_bytes())?;
        write!(msg_buf_writer, " {}\r\n", msg.len())?;
        msg_buf_writer.write(msg)?; //todo 这个需要copy么?最好别copy
        msg_buf_writer.write("\r\n".as_bytes())?;
        let mut msg_buf = msg_buf_writer.into_inner();
        let mut writer = self.writer.lock().await;
        writer.write_all(msg_buf.bytes()).await?;
        msg_buf.clear();
        self.msg_buf = Some(msg_buf);
        Ok(())
    }

    //批量pub,
    pub async fn pub_messages(&mut self, subjects: &[&str], msgs: &[&[u8]]) -> std::io::Result<()> {
        let msg_buf = self.msg_buf.take().expect("must have");
        let mut writer = msg_buf.writer();
        for i in 0..subjects.len() {
            writer.write("PUB ".as_bytes())?;
            writer.write(subjects[i].as_bytes())?;
            //        write!(writer, subject)?;
            write!(writer, " {}\r\n", msgs[i].len())?;
            writer.write(msgs[i])?; //todo 这个需要copy么?最好别copy
            writer.write("\r\n".as_bytes())?;
        }
        let mut msg_buf = writer.into_inner();
        let mut writer = self.writer.lock().await;

        writer.write_all(msg_buf.bytes()).await?;
        msg_buf.clear();
        self.msg_buf = Some(msg_buf);
        Ok(())
    }

    // 向服务器发布一条sub消息,然后等待服务器推送相关消息
    // sub消息格式为SUB subject {queue} {sid} \r\n
    #[allow(dead_code)]
    pub async fn sub_message(&mut self, subject: String, queue: Option<String>, handler: MessageHandler) -> std::io::Result<()> {
        self.sid += 1;
        let mut writer = self.writer.lock().await;
        let m = if let Some(queue) = queue {
            format!("SUB {} {} {}\r\n", subject, queue, self.sid)
        } else {
            format!("SUB {} {}\r\n", subject, self.sid)
        };
        writer.write_all(m.as_bytes()).await?;
        self.handler.lock().await.insert(subject.to_string(), handler);
        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
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
                            //todo shutdown?
                            //let _ = writer.lock().await.shutdown().await;
                            return;
                        }
                        Ok(n) => n,
                    };
                    if n == 0 {
                        // EOF,说明对方关闭了连接
                        println!("EOF connection closed");
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
                                if let Some(handler) = handler.lock().await.get_mut(msg.subject) {
                                    let handle_result = handler(msg.msg);
                                    if handle_result.is_err() {
                                        println!("handle error {:?}", handle_result.unwrap_err());
                                        return;
                                    }
                                } else {
                                    println!("receive msg on subject {}, not found handler", msg.subject);
                                }
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
    struct A {
        a: String,
    }

    impl A {
        async fn test(
            &mut self,
            arg: &str,
            handler: Box<dyn Fn(&[u8]) -> std::result::Result<(), ()> + Send + Sync + '_>,
        ) {}
    }

    type MessageHandler = Box<dyn Fn(&[u8]) -> std::result::Result<(), ()> + Sync + Send>;

    async fn handle(handler: MessageHandler) {
        let args = "hello".to_string();
        handler(args.as_bytes());
    }

    fn print_hello(args: &[u8]) -> std::result::Result<(), ()> {
        println!("{}", unsafe { std::str::from_utf8_unchecked(args) });
        Ok(())
    }

    #[test]
    fn test() {
        use bytes::{BytesMut, BufMut};

        let mut buf = BytesMut::with_capacity(1024);
        buf.put(&b"hello world"[..]);
        buf.put_u16(1234);

        let a = buf.split();
        assert_eq!(a, b"hello world\x04\xD2"[..]);

        buf.put(&b"goodbye world"[..]);

        let b = buf.split();
        assert_eq!(b, b"goodbye world"[..]);

        assert_eq!(buf.capacity(), 998);
    }

    #[tokio::main]
    #[test]
    async fn test2() {
        handle(Box::new(print_hello)).await
    }


}


