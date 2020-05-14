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
    OpPusSpace,
    OpMsg,// pub message
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

pub struct Parser {
    state: ParseState,
    buf: [u8; 512],
    // 消息解析缓冲区,如果消息不超过512,直接用这个,超过了就必须另分配
    arg_len: usize,
    msg_buf: Option<Vec<u8>>,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            state: ParseState::OpStart,
            buf: [0; 512],
            arg_len: 0,
            msg_buf: None,
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
                _ => panic!("unknown state{:?}", self.state)
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

    fn process_sub(&mut self) -> Result<ParseResult> {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {}
}