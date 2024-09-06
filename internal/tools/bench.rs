use std::time::SystemTime;
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use wstf::protocol::file_format::{encode, range};
use wstf::update::Update;

static FNAME: &str = "./internal/mocks/tmp.wstf";

fn prepare_data_range(from_ts: u64, to_ts: u64) {
    let ups = (from_ts..to_ts)
        .map(|ts| Update {
            ts: ts * 1000,
            seq: 0u32,
            size: 0.,
            price: 0.,
            is_bid: false,
            is_trade: false,
        })
        .collect::<Vec<Update>>();

    encode(FNAME, "default", &ups).unwrap();
}

fn benchmark_range() {
    prepare_data_range(1, 50_000_000);

    let range_min_ts = 2_500_000 * 1_000;
    let range_max_ts = 3_000_000 * 1_000;

    let file = File::open(FNAME).expect("Unable to open file");
    
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(0)).expect("Unable to seek");

    let start_time = SystemTime::now();

    range(&mut reader, range_min_ts, range_max_ts).expect("Error in range function");

    let end_time = SystemTime::now();

    let elapsed = end_time.duration_since(start_time)
        .expect("Clock may have gone backwards");

    println!("range: [protocol] elapsed time: {:?}", elapsed);
}

fn main() {
    benchmark_range();
}
