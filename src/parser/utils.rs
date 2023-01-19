use crate::protocol::file_format::{get_range_in_file, read_meta};
use crate::update::Update;
use crate::utils::within_range;
use std::{fs, io};

pub fn scan_files_for_range(
    folder: &str,
    symbol: &str,
    min_ts: u64,
    max_ts: u64,
) -> Result<Vec<Update>, io::Error> {
    let mut ret = Vec::new();
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ));
        }
        Ok(entries) => {
            let mut v = entries
                .map(|entry| {
                    let entry = entry.unwrap();
                    let fname = entry.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = read_meta(fname).unwrap();
                    (fname.to_owned(), meta)
                })
                .filter(|&(ref _fname, ref meta)| {
                    meta.symbol == symbol && within_range(min_ts, max_ts, meta.min_ts, meta.max_ts)
                })
                .collect::<Vec<_>>();

            v.sort_by(|&(ref _f0, ref m0), &(ref _f1, ref m1)| m0.cmp(m1));

            for &(ref fname, ref _meta) in v.iter() {
                let ups = get_range_in_file(fname, min_ts, max_ts)?;
                ret.extend(ups);
            }
        }
    };
    Ok(ret)
}

pub fn total_folder_updates_len(folder: &str) -> Result<usize, io::Error> {
    match fs::read_dir(folder) {
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Unable to read dir entries: {:?}", e),
            ));
        }
        Ok(entries) => {
            let count = entries
                .map(|entry| {
                    let entry = entry.unwrap();
                    let fname = entry.file_name();
                    let fname = fname.to_str().unwrap().to_owned();
                    let fname = &format!("{}/{}", folder, fname);
                    let meta = read_meta(fname).unwrap();
                    meta.nums as usize
                })
                .sum();

            Ok(count)
        }
    }
}
