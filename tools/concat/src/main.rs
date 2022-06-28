use clap::{App, Arg};
use serde_json::{from_str, to_string};
use std::{collections::HashSet, process::exit};
use wstf_protocol::file_format::{decode, encode, read_meta, Metadata};
use wstf_update::Update;

const USAGE: &'static str = "Usage: `wstf_concat first second output`";
const WSTF_ERROR: &'static str = "Unable to parse input wstf file!";

fn main() {
    let matches = App::new("concat")
        .version("0.1.0")
        .author("alxshelepenok <alxshelepenok@gmail.com>")
        .about(
            "Concatenates two WSTF files into a single output file.
       Examples:
       wstf_concat first.wstf second.wstf output.wstf",
        )
        .arg(
            Arg::with_name("first")
                .value_name("FIRST")
                .help("First file to read")
                .required(true)
                .takes_value(true)
                .index(1),
        )
        .arg(
            Arg::with_name("second")
                .value_name("SECOND")
                .help("Second file to read")
                .required(true)
                .takes_value(true)
                .index(2),
        )
        .arg(
            Arg::with_name("output")
                .value_name("OUTPUT")
                .help("Output file")
                .required(true)
                .takes_value(true)
                .index(3),
        )
        .get_matches();

    let first_filename = matches.value_of("first").expect(USAGE);
    let second_filename = matches.value_of("second").expect(USAGE);
    let filename_output = matches.value_of("output").expect(USAGE);

    let first_metadata = read_meta(first_filename).expect(WSTF_ERROR);
    let second_metadata = read_meta(second_filename).expect(WSTF_ERROR);

    if first_metadata.symbol != second_metadata.symbol {
        println!(
            "ERROR: The two input files provided have different symbols: {}, {}",
            first_metadata.symbol, second_metadata.symbol
        );
        exit(1);
    }

    let (start_filename, start_metadata, end_filename, end_metadata) =
        if first_metadata.min_ts > second_metadata.min_ts {
            (
                first_filename,
                first_metadata,
                second_filename,
                second_metadata,
            )
        } else {
            (
                second_filename,
                second_metadata,
                first_filename,
                first_metadata,
            )
        };

    match combine_files(
        start_filename,
        start_metadata,
        end_filename,
        end_metadata,
        filename_output,
    ) {
        Ok(()) => println!(
            "Successfully merged files and output to {}",
            filename_output
        ),
        Err(err) => {
            println!("{}", err);
            exit(1);
        }
    }
}

pub fn combine_files(
    start_filename: &str,
    start_metadata: Metadata,
    end_filename: &str,
    end_metadata: Metadata,
    filename_output: &str,
) -> Result<(), String> {
    if start_metadata.max_ts < end_metadata.min_ts {
        return Err("ERROR: The provided input files are not continuous!".into());
    }

    println!(
        "START METADATA: {:?}\nEND METADATA: {:?}",
        start_metadata, end_metadata
    );

    let symbol = start_metadata.symbol.clone();

    let full_file_first = decode(start_filename, None).map_err(|_| WSTF_ERROR)?;
    let file_updates_first: Vec<Update> = full_file_first
        .iter()
        .filter(|&&Update { ts, .. }| ts >= start_metadata.min_ts && ts < start_metadata.max_ts)
        .cloned()
        .collect();

    println!("FIRST UPDATES: {:?}", file_updates_first);

    let mut overlap_updates_first: Vec<Update> = full_file_first
        .iter()
        .filter(|&&Update { ts, .. }| ts == start_metadata.max_ts)
        .cloned()
        .collect();
    drop(full_file_first);
    let full_file_second = decode(end_filename, None).map_err(|_| WSTF_ERROR)?;
    let mut overlap_updates_second: Vec<Update> = full_file_second
        .iter()
        .filter(|&&Update { ts, .. }| ts == start_metadata.max_ts)
        .cloned()
        .collect();
    overlap_updates_first.append(&mut overlap_updates_second);

    let mut overlapping_updates: HashSet<String> = overlap_updates_first
        .iter()
        .map(to_string)
        .map(Result::unwrap)
        .collect();
    let mut overlapping_updates: Vec<Update> = overlapping_updates
        .drain()
        .map(|s| from_str(&s).unwrap())
        .collect();
    overlapping_updates.sort();

    println!("OVERLAP UPDATES: {:?}", overlapping_updates);

    let mut file_updates_second: Vec<Update> = full_file_second
        .iter()
        .filter(|&&Update { ts, .. }| ts >= start_metadata.max_ts + 1)
        .cloned()
        .collect();
    drop(full_file_second);

    println!("SECOND UPDATES: {:?}", file_updates_second);

    let mut joined_updates = file_updates_first;
    joined_updates.append(&mut overlapping_updates);
    joined_updates.append(&mut file_updates_second);

    encode(filename_output, &symbol, &joined_updates)
        .map_err(|_| String::from("Error while writing output file!"))?;

    Ok(())
}

#[test]
fn wstf_merging() {
    use std::fs::remove_file;

    let mut update_timestamps_first: Vec<u64> = (0..1000).collect();
    update_timestamps_first.append(&mut vec![
        1001, 1002, 1003, 1004, 1004, 1007, 1008, 1009, 1009, 1010,
    ]);
    let update_timestamps_second: &[u64] = &[1008, 1009, 1009, 1010, 1010, 1011, 1012];

    let map_into_updates = |timestamps: &[u64], seq_offset: usize| -> Vec<Update> {
        let mut last_timestamp = 0;

        timestamps
            .into_iter()
            .enumerate()
            .map(|(i, ts)| {
                let update = Update {
                    ts: *ts,
                    seq: i as u32 + seq_offset as u32,
                    is_trade: false,
                    is_bid: true,
                    price: *ts as f32 + if last_timestamp == *ts { 1. } else { 0. },
                    size: *ts as f32,
                };

                last_timestamp = *ts;

                update
            })
            .collect()
    };

    let updates_first = map_into_updates(&update_timestamps_first, 0);
    let updates_second = map_into_updates(update_timestamps_second, 1006);

    let filename_first = "../../internal/mocks/first.wstf";
    let filename_second = "../../internal/mocks/second.wstf";
    let filename_output = "../../internal/mocks/output.wstf";

    encode(filename_first, "test", &updates_first).unwrap();
    encode(filename_second, "test", &updates_second).unwrap();

    let metadata_first = read_meta(filename_first).unwrap();
    let metadata_second = read_meta(filename_second).unwrap();

    let expected_ts_price: &[(u64, f32)] = &[
        (1001, 1001.),
        (1002, 1002.),
        (1003, 1003.),
        (1004, 1004.),
        (1004, 1005.),
        (1007, 1007.),
        (1008, 1008.),
        (1009, 1009.),
        (1009, 1010.),
        (1010, 1010.),
        (1010, 1011.),
        (1011, 1011.),
        (1012, 1012.),
    ];

    combine_files(filename_first, metadata_first, filename_second, metadata_second, filename_output).unwrap();
    let merged_updates: Vec<Update> = decode(filename_output, None).unwrap();

    remove_file(filename_first).unwrap();
    remove_file(filename_second).unwrap();
    remove_file(filename_output).unwrap();

    let actual_ts_price: Vec<(u64, f32)> = merged_updates
        .into_iter()
        .skip(1000)
        .map(|Update { ts, price, .. }| (ts, price))
        .collect();

    assert_eq!(expected_ts_price, actual_ts_price.as_slice());
}
