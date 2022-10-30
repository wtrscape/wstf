use clap::{App, Arg};
use wstf_parser::utils::{scan_files_for_range, total_folder_updates_len};
use wstf_protocol::file_format::{decode, read_meta};
use wstf_update::UpdateVecConvert;

fn main() {
    let matches = App::new("client")
        .version("0.1.0")
        .author("alxshelepenok <alxshelepenok@gmail.com>")
        .about("Command line client for WSTF Protocol")
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("INPUT")
                .help("File to read")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("symbol")
                .long("symbol")
                .value_name("SYMBOL")
                .help("Symbol too lookup")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("min")
                .long("min")
                .value_name("MIN")
                .help("Minimum value to filter for")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("max")
                .long("max")
                .value_name("MAX")
                .help("Maximum value to filter for")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("folder")
                .long("folder")
                .value_name("FOLDER")
                .help("Folder to search")
                .required(false)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("meta")
                .short("m")
                .long("show_metadata")
                .help("Read only the metadata"),
        )
        .arg(
            Arg::with_name("csv")
                .short("c")
                .long("csv")
                .help("output csv (default is JSON)"),
        )
        .get_matches();

    let input = matches.value_of("input").unwrap_or("");

    let symbol = matches.value_of("symbol").unwrap_or("");
    let min = matches.value_of("min").unwrap_or("");
    let max = matches.value_of("max").unwrap_or("");
    let folder = matches.value_of("folder").unwrap_or("./");

    let print_metadata = matches.is_present("meta");
    let csv = matches.is_present("csv");

    if input == "" && (symbol == "" || min == "" || max == "") && (folder == "" && !print_metadata)
    {
        println!("Either supply a single file or construct a range query!");
        return;
    }

    let txt = if input != "" {
        if print_metadata {
            format!("{}", read_meta(input).unwrap())
        } else {
            let ups = decode(input, None).unwrap();
            if csv {
                format!("{}", ups.as_csv())
            } else {
                format!("[{}]", ups.as_json())
            }
        }
    } else {
        if print_metadata {
            format!(
                "Total updates in folder: {}",
                total_folder_updates_len(folder).unwrap()
            )
        } else {
            let ups =
                scan_files_for_range(folder, symbol, min.parse().unwrap(), max.parse().unwrap())
                    .unwrap();
            if csv {
                format!("{}", ups.as_csv())
            } else {
                format!("[{}]", ups.as_json())
            }
        }
    };

    println!("{}", txt);
}
