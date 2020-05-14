/**
## pub
```
PUB <subject> <size>\r\n
<message>\r\n
```
## sub
```
SUB <subject> <sid>\r\n
SUB <subject> <queue> <sid>\r\n
```
## MSG
```
MSG <subject> <sid> <size>\r\n
<message>\r\n
```
*/

use crate::error::*;
use crate::parser::ParseState::*;

#[macro_export]
macro_rules! parse_error {
    ( ) => {{
        //        panic!("parse error");
        return Err(NError::new(ERROR_PARSE));
    }};
}

#[derive(Debug, Clone)]
pub enum ParseState {
    OpStart,
    OpS,
    OpSu,
    OpSub,
    OpSubSpace,
    OpSubArg,
    OpP,
    OpPu,
    OpPub,
    // pub argument
    OpPubSpace,
    OpPubArg,
    OpMsg,
    //pub message
    OpMsgFull,
}

struct SubArg<'a> {
    subject: &'a str,
    // 为什么是str而不是String,就是为了避免内存分配
    sid: &'a str,
    queue: Option<&'a str>,
}

struct PubArg<'a> {
    subject: &'a str,
    size_buf: &'a str,
    // 1024 字符串形式,避免后续再次转换
    size: i64,
    //1024 整数形式
    msg: &'a [u8],
}

pub enum ParseResult<'a> {
    NoMsg,
    // buf = "sub top.stevenbai.blog" sub 消息不完整，我肯定不能处理
    Sub(SubArg<'a>),
    Pub(PubArg<'a>),
}
/*
这个长度很有关系,必须能够将一个完整的主题以及参数放进去,
所以要限制subject的长度
*/
const BUF_LEN: usize = 512;

pub struct Parser {
    state: ParseState,
    buf: [u8; 512],
    // 消息解析缓冲区,如果消息不超过512,直接用这个,超过了就必须另分配
    arg_len: usize,
    msg_buf: Option<Vec<u8>>,
    // 解析过程中收到新消息,那么 新消息的总长度是msg_total_len,已收到的部分应该是msg_len
    msg_total_len: usize,
    msg_len: usize,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            state: ParseState::OpStart,
            buf: [0; BUF_LEN],
            arg_len: 0,
            msg_buf: None,
            msg_total_len: 0,
            msg_len: 0,
        }
    }

    // 对收到的字节序列进行解析,解析完毕后得到pub或者sub消息,同时有可能没有消息或者缓冲区里面还有其他消息
    pub fn parse(&mut self, buf: &[u8]) -> Result<(ParseResult, usize)> {
        let mut b;
        let mut i: usize = 0;
        while i < buf.len() {
            b = buf[i] as char;
            match self.state {
                OpStart => match b {
                    'S' => self.state = OpS,
                    'P' => self.state = OpP,
                    _ => return Err(NError::new(ERROR_PARSE))
                },
                OpS => match b {
                    'U' => self.state = OpSu,
                    _ => return Err(NError::new(ERROR_PARSE))
                },
                OpSu => match b {
                    'B' => self.state = OpSub,
                    _ => parse_error!(),
                },
                OpSub => match b {
                    // sub stevenbai.top 3 是OK的，但是substevenbai.top 3就不允许
                    ' ' | '\t' => self.state = OpSubSpace,
                    _ => parse_error!()
                }
                OpSubSpace => match b {
                    ' ' | '\t' => {}
                    _ => self.state = OpSubArg,
                }
                OpSubArg => match b {
                    '\r' => {}
                    '\n' => {
                        //todo process sub argument
                        self.state = OpStart;
                        let r = self.process_sub()?;
                        return Ok((r, i + 1));
                    }
                    _ => { self.add_arg(b as u8); }
                }
                OpP => match b {
                    'U' => self.state = OpPu,

                    _ => parse_error!(),
                },
                OpPu => match b {
                    'B' => self.state = OpPub,
                    _ => parse_error!(),
                }
                OpPub => match b {
                    ' ' | '\t' => self.state = OpPubSpace,
                    _ => parse_error!(),
                }
                OpPubSpace => match b {
                    ' ' | '\t' => {}
                    _ => {
                        self.state = OpPubArg;
                        self.arg_len = 0;
                    }
                }
                OpPubArg => match b {
                    '\r' => {}
                    '\n' => {
                        self.state = OpMsg;
                        // todo process pub arg,
                        let size = self.get_message_size()?;
                        // todo 如果消息太大,也需要处理
                        if size == 0 || size > 1 * 1024 * 1024 {
                            // 消息体不应该超过1M,避免Dos攻击
                            return Err(NError::new(ERROR_MESSAGE_SIZE_TOO_LARGE));
                        }
                        if size + self.arg_len > BUF_LEN {
                            self.msg_buf = Some(Vec::with_capacity(size))
                        }
                        self.msg_total_len = size;
                    }
                    _ => {
                        self.add_arg(b as u8);
                    }
                }
                OpMsg => {
                    //涉及消息长度
                    if self.msg_len < self.msg_total_len {
                        self.add_msg(b as u8);
                    } else {
                        self.state = OpMsgFull;
                    }
                }
                OpMsgFull => match b {
                    '\r' => {}
                    '\n' => {
                        self.state = OpStart;
                        /*let r = self.process_msg()?;
                        return Ok((r, i + 1));*/
                    }
                    _ => {
                        parse_error!();
                    }
                },
                // 108:11分钟

                //_ => panic!("unknown state{:?}", self.state)
            }
        }
        Err(NError::new(ERROR_PARSE))
    }

    fn add_arg(&mut self, b: u8) -> Result<()> {
        // 太长的subject,
        if self.arg_len > self.buf.len() {
            parse_error!();
        }
        self.buf[self.arg_len] = b;
        self.arg_len += 1;
        Ok(())
    }
    // 一种是消息体比较短,可以直接放在buf中,无需另外分配内存
    // 另一种是消息体很长,无法放在buf中,额外分配了msg_buf空间
    fn add_msg(&mut self, b: u8) -> Result<()> {
        if let Some(buf) = self.msg_buf.as_mut() {
            buf.push(b);
        } else {
            if self.arg_len + self.msg_total_len > BUF_LEN {
                panic!("message should allocate space")
            }
            self.buf[self.arg_len + self.msg_len] = b;
        }
        Ok(())
    }

    fn process_sub(&mut self) -> Result<ParseResult> {
        parse_error!();
    }


    //从接收到的pub消息中提前解析出来消息的长度
    fn get_message_size(&self) -> Result<usize> {
        //缓冲区中形如top.stevenbai.top 5
        let arg_buf = &self.buf[0..self.arg_len];
        let pos = arg_buf.iter().rev().position(|b| *b == ' ' as u8 || *b == '\t' as u8);
        if pos.is_none() {
            parse_error!()
        }
        let pos = pos.unwrap();
        let size_buf = &arg_buf[arg_buf.len() - pos..];
        let szb = unsafe { std::str::from_utf8_unchecked(size_buf) };
        szb.parse::<usize>().map_err(|_| NError::new(ERROR_PARSE))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {}
}