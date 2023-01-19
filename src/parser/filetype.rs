use csv::{DeserializeRecordsIntoIter, ReaderBuilder};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use crate::protocol::file_format::{append, encode, read_magic_value};
use crate::update::Update;

#[derive(Serialize)]
pub enum FileType {
    RawWstf,
}

impl Default for FileType {
    fn default() -> Self {
        FileType::RawWstf
    }
}

impl FileType {
    pub fn from_fname(fname: &str) -> FileType {
        let file = File::open(fname).expect("OPENING FILE");
        let mut rdr = BufReader::new(file);

        if read_magic_value(&mut rdr).unwrap() {
            return FileType::RawWstf;
        }

        unreachable!()
    }
}

#[derive(Deserialize)]
struct KaikoCsvEntry {
    pub id: String,
    pub date: u64,
    pub price: f32,
    pub amount: f32,
    pub sell: Option<bool>,
}

impl Into<Update> for KaikoCsvEntry {
    fn into(self) -> Update {
        Update {
            ts: self.date,
            seq: self.id.parse().unwrap_or(0),
            is_trade: true,
            is_bid: !self.sell.unwrap_or(false),
            price: self.price,
            size: self.amount,
        }
    }
}

pub fn parse_kaiko_csv_to_wstf_inner(
    symbol: &str,
    filename: &str,
    csv_str: &str,
) -> Option<String> {
    let csv_reader = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_str.as_bytes());

    let iter: DeserializeRecordsIntoIter<_, KaikoCsvEntry> = csv_reader.into_deserialize();
    let size_hint = iter.size_hint().0;
    let mut updates: Vec<Update> = Vec::with_capacity(size_hint);

    for kaiko_entry_res in iter {
        match kaiko_entry_res {
            Ok(kaiko_entry) => updates.push(kaiko_entry.into()),
            Err(err) => {
                return Some(format!("{:?}", err));
            }
        }
    }

    let fpath = Path::new(&filename);
    let res = if fpath.exists() {
        append(filename, &updates)
    } else {
        encode(filename, symbol, &updates)
    };

    match res {
        Ok(_) => None,
        Err(err) => Some(format!("Error writing WSTF to output file: {:?}", err)),
    }
}
