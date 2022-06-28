use super::{filetype::FileType, wstf_file_metadata::WSTFFileMetadata};
use serde::ser::Serialize;
use std::io;

pub trait FileMetadata: Default + Serialize {}

pub fn from_fname(fname: &str) -> Result<impl FileMetadata, io::Error> {
    let ftype = FileType::from_fname(fname);

    match ftype {
        FileType::RawWstf => WSTFFileMetadata::new(fname),
    }
}
