use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::cell::RefCell;
use std::io::Cursor;
use std::iter::Peekable;
use std::ops::DerefMut;
use std::sync::Mutex;
use std::{
    cmp, fmt,
    fs::{File, OpenOptions},
    io::{
        self, BufRead, BufReader, BufWriter, ErrorKind::InvalidData, Read, Seek, SeekFrom, Write,
    },
    str,
};

use crate::update::*;
use crate::utils::epoch_to_human;

const SYMBOL_LEN: usize = 20;
static MAGIC_VALUE: &[u8] = &[0x57, 0x53, 0x54, 0x46, 0x01];
static SYMBOL_OFFSET: u64 = 5;
static LEN_OFFSET: u64 = 25;
static MAX_TS_OFFSET: u64 = 33;
static MAIN_OFFSET: u64 = 80;

#[derive(Debug, Eq, PartialEq, PartialOrd)]
pub struct Metadata {
    pub symbol: String,
    pub nums: u64,
    pub max_ts: u64,
    pub min_ts: u64,
}

impl Ord for Metadata {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        u64::cmp(&self.min_ts, &other.min_ts)
    }
}

#[derive(Clone)]
pub struct BatchMetadata {
    pub ref_ts: u64,
    pub ref_seq: u32,
    pub count: u16,
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"{{"symbol": "{}","nums": {},"max_ts": {},"max_ts_human": "{}","min_ts": {},"min_ts_human": "{}"}}"#,
            self.symbol,
            self.nums,
            self.max_ts,
            epoch_to_human(self.max_ts / 1000),
            self.min_ts,
            epoch_to_human(self.min_ts / 1000)
        )
    }
}

pub fn get_max_ts_sorted(updates: &[Update]) -> u64 {
    updates.last().unwrap().ts
}

fn file_writer(fname: &str, create: bool) -> Result<BufWriter<File>, io::Error> {
    let new_file = if create {
        File::create(fname)?
    } else {
        OpenOptions::new().write(true).open(fname)?
    };

    Ok(BufWriter::new(new_file))
}

fn write_magic_value(wtr: &mut dyn Write) -> Result<usize, io::Error> {
    wtr.write(MAGIC_VALUE)
}

fn write_symbol(wtr: &mut dyn Write, symbol: &str) -> Result<usize, io::Error> {
    if symbol.len() > SYMBOL_LEN {
        return Err(io::Error::new(
            InvalidData,
            format!("Symbol length is longer than {}", SYMBOL_LEN),
        ));
    }
    let padded_symbol = format!("{:width$}", symbol, width = SYMBOL_LEN);
    assert_eq!(padded_symbol.len(), SYMBOL_LEN);
    wtr.write(padded_symbol.as_bytes())
}

fn write_len<T: Write + Seek>(wtr: &mut T, len: u64) -> Result<(), io::Error> {
    let _ = wtr.seek(SeekFrom::Start(LEN_OFFSET));
    wtr.write_u64::<BigEndian>(len)
}

fn write_max_ts<T: Write + Seek>(wtr: &mut T, max_ts: u64) -> Result<(), io::Error> {
    let _ = wtr.seek(SeekFrom::Start(MAX_TS_OFFSET));
    wtr.write_u64::<BigEndian>(max_ts)
}

fn write_metadata<T: Write + Seek>(wtr: &mut T, ups: &[Update]) -> Result<(), io::Error> {
    write_len(wtr, ups.len() as u64)?;
    write_max_ts(wtr, get_max_ts_sorted(ups))
}

fn write_reference(
    wtr: &mut dyn Write,
    ref_ts: u64,
    ref_seq: u32,
    len: u16,
) -> Result<(), io::Error> {
    wtr.write_u8(true as u8)?;
    wtr.write_u64::<BigEndian>(ref_ts)?;
    wtr.write_u32::<BigEndian>(ref_seq)?;
    wtr.write_u16::<BigEndian>(len)
}

use std::ops::Deref;
#[cfg_attr(feature = "count_alloc", count_alloc)]
pub fn write_batches<U: Deref<Target = Update>, I: Iterator<Item = U>>(
    mut wtr: &mut dyn Write,
    mut ups: Peekable<I>,
) -> Result<(), io::Error> {
    lazy_static! {
        static ref BUF: Mutex<RefCell<Vec<u8>>> = Mutex::new(RefCell::new(vec![0; 100_000_000]));
    }
    let mut b = BUF.lock().unwrap();
    let mut c = b.deref_mut().borrow_mut();
    let mut buf = Cursor::new(&mut c[..]);
    let head = ups.peek().unwrap();
    let mut ref_ts = head.ts;
    let mut ref_seq = head.seq;
    let mut count: u16 = 0;

    for elem in ups {
        if count != 0
            && (elem.ts >= ref_ts + 0xFFFF
                || elem.seq >= ref_seq + 0xF
                || elem.seq < ref_seq
                || elem.ts < ref_ts
                || count == 0xFFFF)
        {
            write_reference(&mut wtr, ref_ts, ref_seq, count)?;
            let _ = wtr.write(&buf.get_ref()[0..(buf.position() as usize)]);
            buf.set_position(0);

            ref_ts = elem.ts;
            ref_seq = elem.seq;
            count = 0;
        }

        elem.serialize_to_buffer(&mut buf, ref_ts, ref_seq);

        count += 1;
    }

    write_reference(&mut wtr, ref_ts, ref_seq, count)?;
    wtr.write_all(&buf.get_ref()[0..(buf.position() as usize)])
}

pub fn write_main<'a, D: Deref<Target = Update>, T: Write + Seek, I: Iterator<Item = D>>(
    wtr: &mut T,
    ups: Peekable<I>,
) -> Result<(), io::Error> {
    wtr.seek(SeekFrom::Start(MAIN_OFFSET))?;
    write_batches(wtr, ups)?;
    Ok(())
}

pub fn encode(fname: &str, symbol: &str, ups: &[Update]) -> Result<(), io::Error> {
    let mut wtr = file_writer(fname, true)?;
    encode_buffer(&mut wtr, symbol, ups)?;
    wtr.flush()
}

pub fn encode_buffer<T: Write + Seek>(
    wtr: &mut T,
    symbol: &str,
    ups: &[Update],
) -> Result<(), io::Error> {
    if !ups.is_empty() {
        write_magic_value(wtr)?;
        write_symbol(wtr, symbol)?;
        write_metadata(wtr, ups)?;
        write_main(wtr, ups.iter().peekable())?;
    }
    Ok(())
}

pub fn is_wstf(fname: &str) -> Result<bool, io::Error> {
    let file = File::open(fname)?;
    let mut rdr = BufReader::new(file);
    read_magic_value(&mut rdr)
}

pub fn read_magic_value<T: BufRead + Seek>(rdr: &mut T) -> Result<bool, io::Error> {
    rdr.seek(SeekFrom::Start(0))?;
    let mut buf = vec![0u8; 5];
    rdr.read_exact(&mut buf)?;
    Ok(buf == MAGIC_VALUE)
}

pub fn file_reader(fname: &str) -> Result<BufReader<File>, io::Error> {
    let file = File::open(fname)?;
    let mut rdr = BufReader::new(file);

    if !read_magic_value(&mut rdr)? {
        panic!("MAGIC VALUE INCORRECT");
    }
    Ok(rdr)
}

fn read_symbol<T: BufRead + Seek>(rdr: &mut T) -> Result<String, io::Error> {
    rdr.seek(SeekFrom::Start(SYMBOL_OFFSET))?;
    let mut buffer = [0; SYMBOL_LEN];
    rdr.read_exact(&mut buffer)?;
    let ret = str::from_utf8(&buffer).unwrap().trim().to_owned();
    Ok(ret)
}

fn read_len<T: BufRead + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    rdr.seek(SeekFrom::Start(LEN_OFFSET))?;
    rdr.read_u64::<BigEndian>()
}

fn read_min_ts<T: BufRead + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    Ok(read_first(rdr)?.ts)
}

fn read_max_ts<T: BufRead + Seek>(rdr: &mut T) -> Result<u64, io::Error> {
    rdr.seek(SeekFrom::Start(MAX_TS_OFFSET))?;
    rdr.read_u64::<BigEndian>()
}

pub fn get_range_in_file(fname: &str, min_ts: u64, max_ts: u64) -> Result<Vec<Update>, io::Error> {
    let mut rdr = file_reader(fname)?;
    range(&mut rdr, min_ts, max_ts)
}

pub fn range<T: BufRead + Seek>(
    rdr: &mut T,
    min_ts: u64,
    max_ts: u64,
) -> Result<Vec<Update>, io::Error> {
    if min_ts > max_ts {
        return Ok(vec![]);
    }
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    let mut v: Vec<Update> = vec![];

    loop {
        match rdr.read_u8() {
            Ok(byte) => {
                if byte != 0x1 {
                    return Ok(v);
                }
            }
            Err(_e) => {
                return Ok(v);
            }
        };

        let current_meta = read_one_batch_meta(rdr);
        let current_ref_ts = current_meta.ref_ts;
        let current_count = current_meta.count;

        let bytes_to_skip = current_count * 12;
        rdr.seek(SeekFrom::Current(bytes_to_skip as i64))
            .expect(&format!("Skipping {} rows", current_count));

        match rdr.read_u8() {
            Ok(byte) => {
                if byte != 0x1 {
                    return Ok(v);
                }
            }
            Err(_e) => {
                return Ok(v);
            }
        };
        let next_meta = read_one_batch_meta(rdr);
        let next_ref_ts = next_meta.ref_ts;

        if min_ts <= current_ref_ts && max_ts <= current_ref_ts {
            return Ok(v);
        } else if (min_ts <= current_ref_ts && max_ts <= next_ref_ts)
            || (min_ts < next_ref_ts && max_ts >= next_ref_ts)
            || (min_ts > current_ref_ts && max_ts < next_ref_ts)
        {
            let bytes_to_scrollback = -(bytes_to_skip as i64) - 14 - 1;
            rdr.seek(SeekFrom::Current(bytes_to_scrollback))
                .expect("scrolling back");
            let filtered = {
                let batch = read_one_batch_main(rdr, current_meta)?;
                if min_ts <= current_ref_ts && max_ts >= next_ref_ts {
                    batch
                } else {
                    batch
                        .into_iter()
                        .filter(|up| up.ts <= max_ts && up.ts >= min_ts)
                        .collect::<Vec<Update>>()
                }
            };
            v.extend(filtered);
        } else if min_ts >= next_ref_ts {
            let bytes_to_scrollback = -14 - 1;
            rdr.seek(SeekFrom::Current(bytes_to_scrollback))
                .expect("SKIPPING n ROWS");
        } else {
            println!(
                "{}, {}, {}, {}",
                min_ts, max_ts, current_ref_ts, next_ref_ts
            );
            panic!("Should have covered all the cases.");
        }
    }
}

pub fn read_one_batch(rdr: &mut impl Read) -> Result<Vec<Update>, io::Error> {
    let is_ref = rdr.read_u8()? == 0x1;
    if !is_ref {
        Ok(vec![])
    } else {
        let meta = read_one_batch_meta(rdr);
        read_one_batch_main(rdr, meta)
    }
}

pub fn read_one_batch_meta(rdr: &mut impl Read) -> BatchMetadata {
    let ref_ts = rdr.read_u64::<BigEndian>().unwrap();
    let ref_seq = rdr.read_u32::<BigEndian>().unwrap();
    let count = rdr.read_u16::<BigEndian>().unwrap();

    BatchMetadata {
        ref_ts,
        ref_seq,
        count,
    }
}

fn read_one_batch_main(rdr: &mut impl Read, meta: BatchMetadata) -> Result<Vec<Update>, io::Error> {
    let mut v: Vec<Update> = vec![];
    for _i in 0..meta.count {
        let up = read_one_update(rdr, &meta)?;
        v.push(up);
    }
    Ok(v)
}

fn read_one_update(rdr: &mut dyn Read, meta: &BatchMetadata) -> Result<Update, io::Error> {
    let ts = u64::from(rdr.read_u16::<BigEndian>()?) + meta.ref_ts;
    let seq = u32::from(rdr.read_u8()?) + meta.ref_seq;
    let flags = rdr.read_u8()?;
    let is_trade = (Flags::from_bits(flags).ok_or(InvalidData)? & Flags::FLAG_IS_TRADE).to_bool();
    let is_bid = (Flags::from_bits(flags).ok_or(InvalidData)? & Flags::FLAG_IS_BID).to_bool();
    let price = rdr.read_f32::<BigEndian>()?;
    let size = rdr.read_f32::<BigEndian>()?;
    Ok(Update {
        ts,
        seq,
        is_trade,
        is_bid,
        price,
        size,
    })
}

fn read_first_batch<T: BufRead + Seek>(mut rdr: &mut T) -> Result<Vec<Update>, io::Error> {
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
    read_one_batch(&mut rdr)
}

fn read_first<T: BufRead + Seek>(mut rdr: &mut T) -> Result<Update, io::Error> {
    let batch = read_first_batch(&mut rdr)?;
    Ok(batch[0].clone())
}

pub fn get_size(fname: &str) -> Result<u64, io::Error> {
    let mut rdr = file_reader(fname)?;
    read_len(&mut rdr)
}

pub fn read_meta_from_buf<T: BufRead + Seek>(mut rdr: &mut T) -> Result<Metadata, io::Error> {
    let symbol = read_symbol(&mut rdr)?;
    let nums = read_len(&mut rdr)?;
    let max_ts = read_max_ts(&mut rdr)?;
    let min_ts = if nums > 0 {
        read_min_ts(&mut rdr)?
    } else {
        max_ts
    };

    Ok(Metadata {
        symbol,
        nums,
        max_ts,
        min_ts,
    })
}

pub fn read_meta(fname: &str) -> Result<Metadata, io::Error> {
    let mut rdr = file_reader(fname)?;
    read_meta_from_buf(&mut rdr)
}

pub struct WSTFBufReader {
    pub rdr: BufReader<File>,
    batch_size: u32,
}

impl WSTFBufReader {
    pub fn new(fname: &str, batch_size: u32) -> Self {
        let mut rdr = file_reader(fname).expect("Cannot open file");
        rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");
        WSTFBufReader { rdr, batch_size }
    }
}

impl Iterator for WSTFBufReader {
    type Item = Vec<Update>;

    fn next(&mut self) -> Option<Self::Item> {
        let v = read_n_batches(&mut self.rdr, self.batch_size).ok()?;
        if 0 != v.len() {
            Some(v)
        } else {
            None
        }
    }
}

fn read_n_batches<T: BufRead + Seek>(
    mut rdr: &mut T,
    num_rows: u32,
) -> Result<Vec<Update>, io::Error> {
    let mut v: Vec<Update> = vec![];
    let mut count = 0;
    if num_rows == 0 {
        return Ok(v);
    }
    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x1 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            v.extend(read_one_batch(&mut rdr)?);
        }

        count += 1;

        if count > num_rows {
            break;
        }
    }
    Ok(v)
}

fn read_all<T: BufRead + Seek>(mut rdr: &mut T) -> Result<Vec<Update>, io::Error> {
    let mut v: Vec<Update> = vec![];
    while let Ok(is_ref) = rdr.read_u8() {
        if is_ref == 0x1 {
            rdr.seek(SeekFrom::Current(-1)).expect("ROLLBACK ONE BYTE");
            v.extend(read_one_batch(&mut rdr)?);
        }
    }
    Ok(v)
}

pub fn decode(fname: &str, num_rows: Option<u32>) -> Result<Vec<Update>, io::Error> {
    let mut rdr = file_reader(fname)?;
    rdr.seek(SeekFrom::Start(MAIN_OFFSET)).expect("SEEKING");

    match num_rows {
        Some(num_rows) => read_n_batches(&mut rdr, num_rows),
        None => read_all(&mut rdr),
    }
}

pub fn decode_buffer(mut buf: &mut dyn Read) -> Vec<Update> {
    let mut v = vec![];
    let mut res = read_one_batch(&mut buf);
    while let Ok(ups) = res {
        v.extend(ups);
        res = read_one_batch(&mut buf);
    }
    v
}

#[cfg_attr(feature = "count_alloc", count_alloc)]
pub fn append(fname: &str, ups: &[Update]) -> Result<(), io::Error> {
    let mut rdr = file_reader(fname)?;
    let _symbol = read_symbol(&mut rdr)?;

    let old_max_ts = read_max_ts(&mut rdr)?;

    let mut ups = ups.into_iter().filter(|up| up.ts > old_max_ts).peekable();

    if ups.peek().is_none() {
        return Ok(());
    }

    let new_min_ts = ups.clone().next().unwrap().ts;
    let new_max_ts = ups.clone().next_back().unwrap().ts;

    if new_min_ts <= old_max_ts {
        panic!("Cannot append data!(not implemented)");
    }

    let cur_len = read_len(&mut rdr)?;

    let new_len = cur_len + ups.clone().count() as u64;

    let mut wtr = file_writer(fname, false)?;
    write_len(&mut wtr, new_len)?;
    write_max_ts(&mut wtr, new_max_ts)?;

    if cur_len == 0 {
        wtr.seek(SeekFrom::Start(MAIN_OFFSET)).unwrap();
    } else {
        wtr.seek(SeekFrom::End(0)).unwrap();
    }
    write_batches(&mut wtr, ups.peekable())?;
    wtr.flush().unwrap();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    static SYMBOL: &str = "BTC_USDT";
    static FNAME: &str = "./internal/mocks/tmp.wstf";
    static FNAME_DATA: &str = "./internal/mocks/data.wstf";

    #[cfg(test)]
    fn sample_data() -> Vec<Update> {
        let mut ts: Vec<Update> = vec![];
        let t = Update {
            ts: 100,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        let t1 = Update {
            ts: 101,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 2.14564564645,
        };
        let t2 = Update {
            ts: 1000000,
            seq: 113,
            is_trade: true,
            is_bid: false,
            price: 5100.01,
            size: 1.123465,
        };
        ts.push(t);
        ts.push(t1);
        ts.push(t2);
        ts.sort();
        ts
    }

    #[cfg(test)]
    fn sample_data_one_item() -> Vec<Update> {
        let mut ts: Vec<Update> = vec![];
        let t = Update {
            ts: 100,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        ts.push(t);
        ts.sort();
        ts
    }

    #[cfg(test)]
    fn sample_data_append() -> Vec<Update> {
        let mut ts: Vec<Update> = vec![];
        let t2 = Update {
            ts: 00000002,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        let t1 = Update {
            ts: 20000001,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        let t = Update {
            ts: 20000000,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        ts.push(t);
        ts.push(t1);
        ts.push(t2);
        ts.sort();
        ts
    }

    #[cfg(test)]
    fn before() -> Vec<Update> {
        let ts = sample_data();
        encode(FNAME, SYMBOL, &ts).unwrap();
        ts
    }

    #[test]
    fn should_format_metadata_properly() {
        let meta = Metadata {
            symbol: SYMBOL.to_owned(),
            nums: 1,
            max_ts: 1,
            min_ts: 1,
        };

        assert_eq!(
            format!(
                r#"{{"symbol":{},"nums":{},"max_ts":{},"min_ts":{}}}"#,
                meta.symbol, meta.nums, meta.max_ts, meta.min_ts,
            ),
            r#"{"symbol":BTC_USDT,"nums":1,"max_ts":1,"min_ts":1}"#
        );
    }

    #[test]
    #[serial]
    fn should_encode_decode_one_item() {
        let ts = sample_data_one_item();
        encode(FNAME, SYMBOL, &ts).unwrap();
        let decoded_updates = decode(FNAME, None).unwrap();
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    #[serial]
    fn should_encode_and_decode_file() {
        let ts = before();
        let decoded_updates = decode(FNAME, None).unwrap();
        assert_eq!(decoded_updates, ts);
    }

    #[test]
    #[serial]
    fn should_return_the_correct_range() {
        {
            let ups = (1..50)
                .map(|i| Update {
                    ts: i * 1000 as u64,
                    seq: i as u32,
                    price: 0.,
                    size: 0.,
                    is_bid: false,
                    is_trade: false,
                })
                .collect::<Vec<Update>>();

            encode(FNAME, "BTC_USDT", &ups).unwrap();
        }

        let mut rdr = file_reader(FNAME).unwrap();
        let should_be = (10..21)
            .map(|i| Update {
                ts: i * 1000 as u64,
                seq: i as u32,
                price: 0.,
                size: 0.,
                is_bid: false,
                is_trade: false,
            })
            .collect::<Vec<Update>>();
        assert_eq!(should_be, range(&mut rdr, 10000, 20000).unwrap());
    }

    #[test]
    #[serial]
    fn should_return_the_correct_range_2() {
        {
            let ups = (1..1000)
                .map(|i| Update {
                    ts: i * 1000 as u64,
                    seq: i as u32 % 500 * 500,
                    price: 0.,
                    size: 0.,
                    is_bid: false,
                    is_trade: false,
                })
                .collect::<Vec<Update>>();

            encode(FNAME, "BTC_USDT", &ups).unwrap();
        }

        let mut rdr = file_reader(FNAME).unwrap();
        assert_eq!(
            (1..999)
                .map(|i| {
                    Update {
                        ts: i * 1000 as u64,
                        seq: i as u32 % 500 * 500,
                        price: 0.,
                        size: 0.,
                        is_bid: false,
                        is_trade: false,
                    }
                })
                .collect::<Vec<Update>>(),
            range(&mut rdr, 1000, 999000).unwrap()
        );
    }

    #[test]
    #[serial]
    fn should_return_correct_range_real() {
        let mut rdr = file_reader(FNAME_DATA).unwrap();

        let start = 1_510_168_156 * 1000;
        let end = 1_510_171_756 * 1000;

        let ups = range(&mut rdr, start, end).unwrap();
        println!("{}", ups.len());
        assert_eq!(ups.len(), 56564);

        for up in ups.iter() {
            assert!(up.ts >= start && up.ts <= end);
        }
    }

    #[test]
    #[serial]
    fn should_return_correct_symbol() {
        before();
        let mut rdr = file_reader(FNAME).unwrap();
        let sym = read_symbol(&mut rdr).unwrap();
        assert_eq!(sym, SYMBOL);
    }

    #[test]
    #[serial]
    fn should_return_first_record() {
        let vs = before();
        let mut rdr = file_reader(FNAME).unwrap();
        let v = read_first(&mut rdr).unwrap();
        assert_eq!(vs[0], v);
    }

    #[test]
    #[serial]
    fn should_return_correct_num_of_items() {
        let vs = before();
        let mut rdr = file_reader(FNAME).unwrap();
        let len = read_len(&mut rdr).unwrap();
        assert_eq!(vs.len() as u64, len);
    }

    #[test]
    #[serial]
    fn should_return_max_ts() {
        let vs = before();
        let mut rdr = file_reader(FNAME).unwrap();
        let max_ts = read_max_ts(&mut rdr).unwrap();
        assert_eq!(max_ts, get_max_ts_sorted(&vs));
    }

    #[test]
    #[serial]
    fn should_append_filtered_data() {
        should_encode_and_decode_file();

        println!("----DONE----");

        let old_data = sample_data();
        let old_max_ts = get_max_ts_sorted(&old_data);
        let append_data: Vec<Update> = sample_data_append()
            .into_iter()
            .filter(|up| up.ts >= old_max_ts)
            .collect();
        let new_size = append_data.len() + old_data.len();

        append(FNAME, &append_data).unwrap();

        println!("----APPENDED----");

        let mut rdr = file_reader(FNAME).unwrap();

        let max_ts = read_max_ts(&mut rdr).unwrap();
        assert_eq!(max_ts, get_max_ts_sorted(&append_data));

        let mut rdr = file_reader(FNAME).unwrap();
        let len = read_len(&mut rdr).unwrap();
        assert_eq!(new_size as u64, len);

        let mut all_the_data = sample_data();
        all_the_data.extend(append_data);
        all_the_data.sort();
        let decoded = decode(&FNAME, None).unwrap();
        assert_eq!(all_the_data, decoded);
    }

    #[test]
    fn should_speak_json() {
        let t1 = Update {
            ts: 20000001,
            seq: 113,
            is_trade: false,
            is_bid: false,
            price: 5100.01,
            size: 1.14564564645,
        };
        assert_eq!(
            t1.as_json(),
            r#"{"ts":20000.001,"seq":113,"is_trade":false,"is_bid":false,"price":5100.01,"size":1.1456456}"#
        );
    }

    #[test]
    fn should_write_to_bytes() {
        let up = Update {
            ts: 0,
            seq: 0,
            is_trade: false,
            is_bid: false,
            price: 0.,
            size: 0.,
        };
        let mut bytes = vec![];
        write_batches(&mut bytes, [up].iter().peekable()).unwrap();
        assert_eq!(
            vec![1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            bytes
        );
    }
}
