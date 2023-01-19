use std::{error, fmt};

#[derive(Debug)]
pub enum Error {
    ServerError(String),
    DBNotFoundError(String),
    ConnectionError,
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ServerError(ref msg) => &msg,
            Error::DBNotFoundError(ref dbname) => &dbname,
            Error::ConnectionError => "disconnection from database",
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ServerError(ref msg) => write!(f, "Error: {}", msg),
            Error::DBNotFoundError(ref dbname) => write!(f, "DBNotFoundError: {}", dbname),
            Error::ConnectionError => write!(f, "ConnectionError"),
        }
    }
}
