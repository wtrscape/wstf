use libc::{c_char, c_uchar};
use crate::protocol::file_format::{decode, decode_buffer};
use crate::update::{Update, UpdateVecConvert};
use super::filetype::{parse_kaiko_csv_to_wstf_inner};

use std::ffi::{CStr, CString};
use std::{mem, ptr, slice};

#[repr(C)]
pub struct Slice {
    ptr: *mut Update,
    len: usize,
}

unsafe fn ptr_to_str<'a>(ptr: *const c_char) -> Result<&'a str, ()> {
    if ptr.is_null() {
        return Err(());
    }
    CStr::from_ptr(ptr).to_str().map_err(|_| ())
}

#[no_mangle]
pub extern "C" fn read_wstf_to_csv(fname: *const c_char) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = decode(fname, None).unwrap();
    let data = ups.as_csv();

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

#[no_mangle]
pub extern "C" fn read_wstf_to_csv_with_limit(fname: *const c_char, num: u32) -> *mut c_char {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };
    let fname = c_str.to_str().unwrap();

    let ups = decode(fname, Some(num)).unwrap();
    let data = ups.as_csv();

    let ret = String::from(data);
    let c_str_song = CString::new(ret).unwrap();
    c_str_song.into_raw()
}

#[no_mangle]
pub extern "C" fn read_wstf_to_arr(fname: *const c_char) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };

    let fname = c_str.to_str().unwrap();
    let mut ups = decode(fname, None).unwrap();
    let p = ups.as_mut_ptr();
    let len = ups.len();

    mem::forget(ups);

    Slice { ptr: p, len }
}

#[no_mangle]
pub extern "C" fn read_wstf_to_arr_with_limit(fname: *const c_char, num: u32) -> Slice {
    let c_str = unsafe {
        assert!(!fname.is_null());
        CStr::from_ptr(fname)
    };

    let fname = c_str.to_str().unwrap();
    let mut ups = decode(fname, Some(num)).unwrap();
    let p = ups.as_mut_ptr();
    let len = ups.len();

    mem::forget(ups);

    Slice { ptr: p, len }
}

#[no_mangle]
pub unsafe extern "C" fn parse_kaiko_csv_to_wstf(
    symbol: *const c_char,
    fname: *const c_char,
    csv_str: *const c_char,
) -> *const c_char {
    let symbol = match ptr_to_str(symbol) {
        Ok(symbol) => symbol,
        Err(()) => return CString::new("Symbol was invalid.").unwrap().into_raw(),
    };

    let fname = match ptr_to_str(fname) {
        Ok(fname) => fname,
        Err(()) => return CString::new("Filename was invalid.").unwrap().into_raw(),
    };

    let csv_str = match ptr_to_str(csv_str) {
        Ok(csv_str) => csv_str,
        Err(()) => return CString::new("CSV String was invalid.").unwrap().into_raw(),
    };

    match parse_kaiko_csv_to_wstf_inner(symbol, fname, csv_str) {
        Some(err) => CString::new(err).unwrap().into_raw(),
        None => ptr::null(),
    }
}

#[no_mangle]
pub extern "C" fn parse_stream(n: *mut c_uchar, len: u32) -> Slice {
    let mut byte_arr = unsafe {
        assert!(!n.is_null());
        slice::from_raw_parts(n, len as usize)
    };

    let mut v = decode_buffer(&mut byte_arr);

    let p = v.as_mut_ptr();
    let len = v.len();
    mem::forget(v);
    Slice { ptr: p, len }
}

#[no_mangle]
pub extern "C" fn str_free(s: *mut c_char) {
    unsafe {
        if s.is_null() {
            return;
        }
        CString::from_raw(s)
    };
}
