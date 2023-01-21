use super::{error::Error, insert_command::InsertCommand};
use byteorder::{BigEndian, ReadBytesExt};
use std::{
    io::{Read, Write},
    net::TcpStream,
    str,
};

use crate::protocol::file_format::{decode_buffer, read_one_batch};
use crate::update::UpdateVecConvert;

pub struct CxnStream {
    stream: TcpStream,
}

impl CxnStream {
    pub fn new(stream: TcpStream) -> Self {
        CxnStream { stream }
    }

    pub fn cmd(&mut self, command: &str) -> Result<String, Error> {
        let _ = self.stream.write(command.as_bytes());

        let success = match self.stream.read_u8() {
            Ok(re) => re == 0x1,
            Err(_) => return Err(Error::ConnectionError),
        };

        if command.starts_with("GET")
            && !command.contains("AS CSV")
            && !command.contains("AS JSON")
            && success
        {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0_u8; size as usize];
            let _ = self.stream.read_exact(&mut buf);

            let mut buf = buf.as_slice();
            let v = decode_buffer(&mut buf);
            Ok(format!("[{}]\n", v.as_json()))
        } else {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0; size as usize];
            let _ = self.stream.read_exact(&mut buf);
            let res = str::from_utf8(&buf).unwrap().to_owned();
            if success {
                Ok(res)
            } else if res.contains("ERR: DB") {
                let dbname = res.split(" ").nth(2).unwrap();
                Err(Error::DBNotFoundError(dbname.to_owned()))
            } else {
                Err(Error::ServerError(res))
            }
        }
    }
}

pub struct Cxn {
    stream: TcpStream,
}

impl Cxn {
    pub fn new(host: &str, port: &str) -> Result<Cxn, Error> {
        let addr = format!("{}:{}", host, port);

        info!("Connecting to {}", addr);

        let stream = match TcpStream::connect(&addr) {
            Ok(stm) => stm,
            Err(_) => return Err(Error::ConnectionError),
        };

        Ok(Cxn { stream })
    }

    pub fn create_db(&mut self, dbname: &str) -> Result<String, Error> {
        info!("Creating db {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    pub fn use_db(&mut self, dbname: &str) -> Result<String, Error> {
        self.cmd(&format!("USE {}\n", dbname))
    }

    pub fn cmd(&mut self, command: &str) -> Result<String, Error> {
        let _ = self.stream.write(command.as_bytes());
        let success = match self.stream.read_u8() {
            Ok(re) => re == 0x1,
            Err(_) => return Err(Error::ConnectionError),
        };

        if command.starts_with("GET")
            && !command.contains("AS CSV")
            && !command.contains("AS JSON")
            && success
        {
            let vecs = read_one_batch(&mut self.stream).unwrap();
            Ok(format!("[{}]\n", vecs.as_json()))
        } else {
            let size = self.stream.read_u64::<BigEndian>().unwrap();
            let mut buf = vec![0; size as usize];
            let _ = self.stream.read_exact(&mut buf);
            let res = str::from_utf8(&buf).unwrap().to_owned();
            if success {
                Ok(res)
            } else if res.contains("ERR: DB") {
                let dbname = res.split(" ").nth(2).unwrap();
                Err(Error::DBNotFoundError(dbname.to_owned()))
            } else {
                Err(Error::ServerError(res))
            }
        }
    }

    pub fn insert(&mut self, cmd: InsertCommand) -> Result<(), Error> {
        for cmd in &cmd.into_string() {
            let _res = self.cmd(cmd)?;
        }
        Ok(())
    }
}
