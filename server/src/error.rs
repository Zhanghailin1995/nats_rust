use std::fmt::Formatter;
use std::error::Error;

pub type Result<T> = std::result::Result<T, NError>;

pub const ERROR_PARSE: i32 = 1;
pub const ERROR_MESSAGE_SIZE_TOO_LARGE: i32 = 2;
pub const ERROR_INVALID_SUBJECT: i32 = 3;
pub const ERROR_SUBSCRIPTION_NOT_FOUND: i32 = 4;
pub const ERROR_CONNECTION_CLOSED: i32 = 5;

#[derive(Debug)]
pub struct NError {
    err_code: i32,
}

impl NError {
    pub fn new(err_code: i32) -> Self {
        Self {
            err_code
        }
    }

    pub fn error_description(&self) -> &'static str {
        match self.err_code {
            ERROR_PARSE => "parse error",
            _ => "unknown error",
        }
    }
}

impl std::error::Error for NError {}

impl std::fmt::Display for NError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "NError[{},{}]", self.err_code, self.error_description())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        println!("{}", NError::new(ERROR_PARSE));
    }
}