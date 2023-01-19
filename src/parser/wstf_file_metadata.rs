use super::{file_metadata::FileMetadata, filetype::FileType};
use std::{env, fs, io, str::FromStr};
use crate::protocol::{
    file_format::{read_meta, Metadata},
    symbol::{AssetType, Symbol},
};

fn key_or_default(key: &str, default: &str) -> String {
    match env::var(key) {
        Ok(val) => val,
        Err(_) => default.into(),
    }
}

fn parse_wstf_metadata_tags() -> Vec<String> {
    key_or_default("WSTF_METADATA_TAGS", "")
        .split(',')
        .map(String::from)
        .collect()
}

use uuid::Uuid;

#[derive(Default, Serialize)]
pub struct WSTFFileMetadata {
    pub file_type: FileType,
    pub file_size: u64,
    pub exchange: String,
    pub currency: String,
    pub asset: String,
    pub asset_type: AssetType,
    pub first_epoch: u64,
    pub last_epoch: u64,
    pub total_updates: u64,
    pub assert_continuity: bool,
    pub discontinuities: Vec<(u64, u64)>,
    pub continuation_candles: bool,
    pub uuid: Uuid,
    pub filename: String,
    pub tags: Vec<String>,
    pub errors: Vec<String>,
}

impl FileMetadata for WSTFFileMetadata {}

impl WSTFFileMetadata {
    pub fn new(fname: &str) -> Result<WSTFFileMetadata, io::Error> {
        let metadata: Metadata = read_meta(fname)?;
        let file_size = fs::metadata(fname)?.len();
        let symbol = match Symbol::from_str(&metadata.symbol) {
            Ok(sym) => sym,
            Err(()) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Unable to parse symbol {}", metadata.symbol),
                ));
            }
        };

        let first_epoch = metadata.min_ts;
        let last_epoch = metadata.max_ts;
        let total_updates = metadata.nums;

        Ok(WSTFFileMetadata {
            file_type: FileType::RawWstf,
            file_size,
            exchange: symbol.exchange,
            currency: symbol.currency,
            asset: symbol.asset,
            asset_type: AssetType::SPOT,
            first_epoch,
            last_epoch,
            total_updates,
            assert_continuity: true,
            discontinuities: vec![],
            continuation_candles: false,
            filename: fname.to_owned(),
            tags: parse_wstf_metadata_tags(),
            ..Default::default()
        })
    }
}

#[test]
fn wstf_metadata_tags_parsing() {
    let sample_env = "foo,bar,key:value,test2";
    let parsed: Vec<String> = sample_env.split(',').map(String::from).collect();

    let expected: Vec<String> = ["foo", "bar", "key:value", "test2"]
        .iter()
        .map(|s| String::from(*s))
        .collect();

    assert_eq!(parsed, expected);
}
