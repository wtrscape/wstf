use std::collections::BTreeMap;
use wstf_update::Update;

type Time = u64;

pub enum EventType {
    CancelEvent,
    TradeEvent,
    CreateEvent,
}

#[derive(Debug)]
pub struct Events {
    pub cancelled: BTreeMap<Time, Vec<Update>>,
    pub trades: BTreeMap<Time, Vec<Update>>,
    pub created: BTreeMap<Time, Vec<Update>>,
}

impl<'a> From<&'a [Update]> for Events {
    fn from(ups: &[Update]) -> Events {
        let mut cancelled = BTreeMap::new();
        let mut trades = BTreeMap::new();
        let mut created = BTreeMap::new();

        let mut current_level = BTreeMap::new();

        for row in ups {
            let ts = row.ts;
            let price = row.price.to_bits();

            if row.is_trade {
                let v = trades.entry(ts).or_insert(Vec::new());
                (*v).push(row.clone());
            } else {
                let prev = if current_level.contains_key(&price) {
                    *current_level.get(&price).unwrap()
                } else {
                    0.
                };
                if row.size == 0. || row.size <= prev {
                    let v = cancelled.entry(ts).or_insert(Vec::new());
                    (*v).push(row.clone());
                } else if row.size > prev {
                    let v = created.entry(ts).or_insert(Vec::new());
                    (*v).push(row.clone());
                } else {
                    unreachable!();
                }
            }

            current_level.insert(price, row.size);
        }

        Events {
            cancelled,
            trades,
            created,
        }
    }
}

impl Events {
    pub fn filter_size(&self, event_type: EventType, from_size: f32, to_size: f32) -> Vec<Update> {
        let obj = match event_type {
            EventType::CancelEvent => &self.cancelled,
            EventType::CreateEvent => &self.created,
            EventType::TradeEvent => &self.trades,
        };

        let mut ret = Vec::new();
        for v in obj.values() {
            for up in v.iter() {
                if up.size >= from_size && up.size <= to_size {
                    ret.push(up.clone());
                }
            }
        }
        ret
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use wstf_protocol::file_format::decode;

    static FNAME: &str = "../../internal/mocks/data.wstf";

    #[test]
    fn test_size_filter() {
        let records = decode(FNAME, Some(10000)).unwrap();
        let ups = records.as_slice();

        let evts = Events::from(ups);

        let cancels = evts.filter_size(EventType::CancelEvent, 100., 200.);
        assert!(cancels.len() > 0);
        for up in cancels.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }

        let creates = evts.filter_size(EventType::CreateEvent, 100., 200.);
        assert!(creates.len() > 0);
        for up in creates.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }

        let trades = evts.filter_size(EventType::TradeEvent, 100., 200.);
        assert!(trades.len() > 0);
        for up in trades.iter() {
            assert!(up.size >= 100. && up.size <= 200.);
        }
    }
}
