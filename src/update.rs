use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::cmp::Ordering;
use std::io::ErrorKind::InvalidData;
use std::io::{Cursor, Write};

pub trait UpdateVecConvert {
    fn as_json(&self) -> String;
    fn as_csv(&self) -> String;
}

impl UpdateVecConvert for [Update] {
    fn as_json(&self) -> String {
        update_vec_to_json(self)
    }

    fn as_csv(&self) -> String {
        update_vec_to_csv(&self)
    }
}

impl UpdateVecConvert for Vec<Update> {
    fn as_json(&self) -> String {
        update_vec_to_json(self)
    }

    fn as_csv(&self) -> String {
        update_vec_to_csv(&self)
    }
}

fn update_vec_to_csv(vecs: &[Update]) -> String {
    let objects: Vec<String> = vecs.into_iter().map(|up| up.as_csv()).collect();
    objects.join("\n")
}

fn update_vec_to_json(vecs: &[Update]) -> String {
    let objects: Vec<String> = vecs.into_iter().map(|up| up.as_json()).collect();
    objects.join(", ")
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Copy, Serialize, Deserialize)]
pub struct Update {
    pub ts: u64,
    pub seq: u32,
    pub is_trade: bool,
    pub is_bid: bool,
    pub price: f32,
    pub size: f32,
}

impl Update {
    pub fn serialize_raw_to_buffer(&self, buf: &mut dyn Write) -> Result<(), std::io::Error> {
        buf.write_u64::<BigEndian>(self.ts)?;
        buf.write_u32::<BigEndian>(self.seq)?;

        let mut flags = Flags::FLAG_EMPTY;
        if self.is_bid {
            flags |= Flags::FLAG_IS_BID;
        }
        if self.is_trade {
            flags |= Flags::FLAG_IS_TRADE;
        }
        buf.write_u8(flags.bits())?;

        buf.write_f32::<BigEndian>(self.price)?;
        buf.write_f32::<BigEndian>(self.size)?;
        Ok(())
    }

    pub fn from_raw(buf: &[u8]) -> Result<Self, std::io::Error> {
        let mut rdr = Cursor::new(buf);

        let ts = rdr.read_u64::<BigEndian>()?;
        let seq = rdr.read_u32::<BigEndian>()?;
        let flags = rdr.read_u8()?;
        let is_trade =
            (Flags::from_bits(flags).ok_or(InvalidData)? & Flags::FLAG_IS_TRADE).to_bool();
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

    pub fn serialize_to_buffer(&self, buf: &mut dyn Write, ref_ts: u64, ref_seq: u32) {
        if self.seq < ref_seq {
            panic!("reference seqno is bigger than the current seqno you are trying to encode");
        }
        let _ = buf.write_u16::<BigEndian>((self.ts - ref_ts) as u16);
        let _ = buf.write_u8((self.seq - ref_seq) as u8);

        let mut flags = Flags::FLAG_EMPTY;
        if self.is_bid {
            flags |= Flags::FLAG_IS_BID;
        }
        if self.is_trade {
            flags |= Flags::FLAG_IS_TRADE;
        }
        let _ = buf.write_u8(flags.bits());

        let _ = buf.write_f32::<BigEndian>(self.price);
        let _ = buf.write_f32::<BigEndian>(self.size);
    }

    pub fn as_json(&self) -> String {
        format!(
            r#"{{"ts":{},"seq":{},"is_trade":{},"is_bid":{},"price":{},"size":{}}}"#,
            (self.ts as f64) / 1000_f64,
            self.seq,
            self.is_trade,
            self.is_bid,
            self.price,
            self.size
        )
    }

    pub fn as_csv(&self) -> String {
        format!(
            r#"{},{},{},{},{},{}"#,
            (self.ts as f64) / 1000_f64,
            self.seq,
            self.is_trade,
            self.is_bid,
            self.price,
            self.size
        )
    }
}

impl PartialOrd for Update {
    fn partial_cmp(&self, other: &Update) -> Option<Ordering> {
        let selfts = self.ts;
        let otherts = other.ts;
        if selfts > otherts {
            Some(Ordering::Greater)
        } else if selfts == otherts {
            Some(self.seq.cmp(&other.seq))
        } else {
            Some(Ordering::Less)
        }
    }
}

impl Ord for Update {
    fn cmp(&self, other: &Update) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Eq for Update {}

bitflags! {
    pub struct Flags: u8 {
        const FLAG_EMPTY = 0b0000_0000;
        const FLAG_IS_BID = 0b0000_0001;
        const FLAG_IS_TRADE = 0b0000_0010;
    }
}

impl Flags {
    pub fn to_bool(&self) -> bool {
        (self.bits == 0b0000_0001) || (self.bits == 0b0000_0010)
    }
}
