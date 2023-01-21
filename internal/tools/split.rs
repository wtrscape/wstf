use clap::{App, Arg};
use std::path::Path;
use wstf::protocol::file_format::{encode, read_meta, WSTFBufReader};

fn main() {
    let matches = App::new("split")
        .version("0.1.0")
        .author("alxshelepenok <alxshelepenok@gmail.com>")
        .about(
            "Splits big wstf files into smaller ones.
       Examples:
       wstf_split -i file.wstf -f file-{}.wstf",
        )
        .arg(
            Arg::with_name("input")
                .short("i")
                .long("input")
                .value_name("INPUT")
                .help("File to read")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("BATCH")
                .short("b")
                .long("batch_size")
                .value_name("BATCH_SIZE")
                .help("Specify the number of batches to read")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let fname = matches.value_of("input").expect("Must supply input");
    let batch_size = matches.value_of("BATCH").unwrap().parse().unwrap();
    let file_stem = Path::new(fname)
        .file_stem()
        .expect("Input not a valid file")
        .to_str()
        .unwrap();

    println!("Reading: {}", fname);
    let meta = read_meta(fname).unwrap();
    let rdr = WSTFBufReader::new(fname, batch_size);
    for (i, batch) in rdr.enumerate() {
        let outname = format!("{}-{}.wstf", file_stem, i);
        println!("Writing to {}", outname);
        encode(&outname, &meta.symbol, &batch).unwrap();
    }
}
