use super::{circular_queue::CircularQueue, Cxn, Error, InsertCommand};
use std::{collections::VecDeque, thread, time};

type InsertQueue = CircularQueue<InsertCommand>;

pub struct CxnPool {
    cxns: Vec<Cxn>,
    host: String,
    port: String,
    available_workers: VecDeque<usize>,
    queue: InsertQueue,
}

impl CxnPool {
    pub fn new(n_workers: usize, host: &str, port: &str, capacity: usize) -> Result<Self, Error> {
        let mut cxns = vec![];
        let mut workers = VecDeque::new();
        let queue = CircularQueue::with_capacity(capacity);

        for i in 0..n_workers {
            let cxn = Cxn::new(host, port)?;
            cxns.push(cxn);
            workers.push_back(i);
        }

        Ok(CxnPool {
            cxns,
            host: host.to_owned(),
            port: port.to_owned(),
            available_workers: workers,
            queue,
        })
    }

    pub fn create_db(&mut self, dbname: &str) -> Result<String, Error> {
        info!("Creating database {}", dbname);
        self.cmd(&format!("CREATE {}\n", dbname))
    }

    pub fn cmd(&mut self, command: &str) -> Result<String, Error> {
        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                warn!("Growing CxnPool to {}", self.cxns.len());
                self.cxns.push(Cxn::new(&self.host, &self.port)?);
                self.cxns.len() - 1
            }
        };

        let result = self.cxns[n].cmd(command);
        let ret = match result {
            Err(Error::ConnectionError) => {
                thread::sleep(time::Duration::from_secs(1));
                self.cxns[n] = Cxn::new(&self.host, &self.port)?;
                error!("Replacing CXN");
                result
            }
            _ => result,
        };

        self.available_workers.push_back(n);

        ret
    }

    pub fn insert(&mut self, cmd: &InsertCommand) -> Result<(), Error> {
        let n = self.available_workers.pop_front();
        let n = match n {
            Some(n) => n,
            None => {
                self.cxns.push(Cxn::new(&self.host, &self.port)?);
                warn!("Growing CxnPool to {}", self.cxns.len());
                self.cxns.len() - 1
            }
        };

        for c in cmd.clone().into_string() {
            let result = self.cxns[n].cmd(&c);
            match result {
                Err(Error::ConnectionError) => {
                    thread::sleep(time::Duration::from_secs(1));
                    {
                        self.queue.push(cmd.clone());
                    }
                    self.cxns[n] = Cxn::new(&self.host, &self.port)?;
                    error!("Replacing CXN");
                    self.available_workers.push_back(n);
                    return Err(Error::ConnectionError);
                }

                Err(Error::DBNotFoundError(ref dbname)) => {
                    let _ = self.create_db(dbname);
                    {
                        self.queue.push(cmd.clone());
                    }
                    self.available_workers.push_back(n);
                    return Err(Error::DBNotFoundError(dbname.to_owned()));
                }

                Err(e) => {
                    self.available_workers.push_back(n);
                    return Err(e);
                }

                _ => (),
            }
        }

        self.available_workers.push_back(n);

        {
            let ins_cmd = self.queue.pop();
            if let Some(i) = ins_cmd {
                let _ = self.insert(&i)?;
            }
        }

        Ok(())
    }
}
