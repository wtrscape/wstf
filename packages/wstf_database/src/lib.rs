#[macro_use]
extern crate log;

use std::env;

pub mod circular_queue;
pub mod cxn;
pub mod error;
pub mod insert_command;
pub mod pool;

pub use self::cxn::Cxn;
pub use self::error::Error;
pub use self::insert_command::InsertCommand;
pub use self::pool::CxnPool;

fn key_or_default(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

fn get_waterscape_conf_from_env() -> (String, String, usize) {
    let waterscape_hostname: String = key_or_default("WATERSCAPEDB_HOSTNAME", "localhost");
    let waterscape_port: String = key_or_default("WATERSCAPEDB_PORT", "9001");
    let q_capacity: usize = key_or_default("QUEUE_CAPACITY", "70000000")
        .parse()
        .unwrap();

    (waterscape_hostname, waterscape_port, q_capacity)
}

pub fn get_cxn() -> Cxn {
    let (waterscape_hostname, waterscape_port, _capacity) = get_waterscape_conf_from_env();
    match Cxn::new(&waterscape_hostname, &waterscape_port) {
        Ok(cxn) => cxn,
        Err(Error::ConnectionError) => {
            panic!("DB cannot be connected!");
        }
        _ => unreachable!(),
    }
}

pub fn get_cxn_pool() -> CxnPool {
    let (waterscape_hostname, waterscape_port, capacity) = get_waterscape_conf_from_env();

    match CxnPool::new(1, &waterscape_hostname, &waterscape_port, capacity) {
        Ok(pool) => pool,
        Err(Error::ConnectionError) => {
            panic!("Connection Pool cannot be established!");
        }
        _ => unreachable!(),
    }
}
