use clap::{App, Arg};
use std::time::SystemTime;
use wstf::algorithms::histogram::Histogram;
use wstf::algorithms::levels::Levels;
use wstf::protocol::file_format::{encode, get_range_in_file};
use wstf::update::Update;

static FNAME: &str = "./internal/mocks/tmp.wstf";
static EVENTS_PER_MS: u64 = 100;
static RANGE_FROM_TS: u64 = 1725000000000;
static RANGE_TO_TS: u64 = 1725000100000;
static RANGE_MIN_TS: u64 = 1725000000000;
static RANGE_MAX_TS: u64 = 1725000100000;

fn prepare_data_range(from_ts: u64, to_ts: u64, events_per_ms: u64) {
    let ups = (from_ts..to_ts)
        .map(|ts| {
            (0..events_per_ms).map({
                move |_| Update {
                    ts,
                    seq: 0,
                    size: 0f32,
                    price: 0f32,
                    is_bid: false,
                    is_trade: false,
                }
            })
        })
        .flatten()
        .collect::<Vec<Update>>();

    encode(FNAME, "default", &ups).unwrap();
}

fn benchmark_range(range_min_ts: u64, range_max_ts: u64) {
    let start_time = SystemTime::now();

    get_range_in_file(FNAME, range_min_ts, range_max_ts).expect("Error in range function");

    let end_time = SystemTime::now();

    let elapsed = end_time
        .duration_since(start_time)
        .expect("Clock may have gone backwards");

    println!("range: [protocol] elapsed time: {:?}", elapsed);
}

fn benchmark_levels(range_min_ts: u64, range_max_ts: u64) {
    let start_time = SystemTime::now();

    let ups =
        get_range_in_file(FNAME, range_min_ts, range_max_ts).expect("Error in range function");

    Levels::from(&ups, 10, 10, 2.0);

    let end_time = SystemTime::now();

    let elapsed = end_time
        .duration_since(start_time)
        .expect("Clock may have gone backwards");

    println!("levels: [algorithms] elapsed time: {:?}", elapsed);
}

fn bench_histogram(range_min_ts: u64, range_max_ts: u64) {
    let start_time = SystemTime::now();
    let ups =
        get_range_in_file(FNAME, range_min_ts, range_max_ts).expect("Error in range function");
    let prices = ups.iter().map(|up| up.price as f64).collect::<Vec<f64>>();

    Histogram::new(&prices, 100, 2.0);

    let end_time = SystemTime::now();

    let elapsed = end_time
        .duration_since(start_time)
        .expect("Clock may have gone backwards");

    println!("histogram: [algorithms] elapsed time: {:?}", elapsed);
}

fn main() {
    let matches = App::new("wstf-bench")
        .version("0.1.0")
        .author("alxshelepenok <alxshelepenok@gmail.com>")
        .about("Benchmarking tool for WSTF")
        .arg(
            Arg::with_name("from_ts")
                .short("f")
                .long("from_ts")
                .value_name("FROM_TS")
                .help("Sets the starting timestamp")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("to_ts")
                .short("t")
                .long("to_ts")
                .value_name("TO_TS")
                .help("Sets the ending timestamp")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("prepare")
                .short("p")
                .long("prepare")
                .help("Prepare data for benchmarking"),
        )
        .arg(
            Arg::with_name("range_min_ts")
                .short("r")
                .long("range_min_ts")
                .value_name("RANGE_MIN_TS")
                .help("Sets the minimum timestamp for range")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("range_max_ts")
                .short("m")
                .long("range_max_ts")
                .value_name("RANGE_MAX_TS")
                .help("Sets the maximum timestamp for range")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("events_per_second")
                .short("e")
                .long("events_per_second")
                .value_name("EVENTS_PER_SECOND")
                .help("Sets the number of events per second")
                .takes_value(true),
        )
        .get_matches();

    let range_from_ts = matches
        .value_of("from_ts")
        .unwrap_or(RANGE_FROM_TS.to_string().as_str())
        .parse::<u64>()
        .expect("Unable to parse from_ts");

    let range_to_ts = matches
        .value_of("to_ts")
        .unwrap_or(RANGE_TO_TS.to_string().as_str())
        .parse::<u64>()
        .expect("Unable to parse to_ts");

    let prepare = matches.is_present("prepare");

    let range_min_ts = matches
        .value_of("range_min_ts")
        .unwrap_or(RANGE_MIN_TS.to_string().as_str())
        .parse::<u64>()
        .expect("Unable to parse range_min_ts");

    let range_max_ts = matches
        .value_of("range_max_ts")
        .unwrap_or(RANGE_MAX_TS.to_string().as_str())
        .parse::<u64>()
        .expect("Unable to parse range_max_ts");

    let events_per_ms = matches
        .value_of("events_per_ms")
        .unwrap_or(EVENTS_PER_MS.to_string().as_str())
        .parse::<u64>()
        .expect("Unable to parse events_per_second");

    if prepare {
        prepare_data_range(range_from_ts, range_to_ts, events_per_ms);
    }

    benchmark_range(range_min_ts, range_max_ts);
    benchmark_levels(range_min_ts, range_max_ts);
    bench_histogram(range_min_ts, range_max_ts);
}
