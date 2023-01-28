use crate::algorithms::histogram::{BinCount, Histogram};
use crate::update::Update;
use indexmap::IndexMap;
use std::collections::BTreeMap;
use std::f64;
use std::fmt;

type Price = u64;
type Size = f64;
type Time = u64;

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct Orderbook {
    pub price_decimals: u8,
    pub bids: BTreeMap<Price, Size>,
    pub asks: BTreeMap<Price, Size>,
}

impl Orderbook {
    pub fn discretize(&self, p: f32) -> Price {
        (f64::from(p) * 10f64.powf(self.price_decimals as f64)) as Price
    }

    pub fn undiscretize(&self, p: u64) -> f32 {
        p as f32 / 10f32.powf(self.price_decimals as f32)
    }

    pub fn with_precision(price_decimals: u8) -> Orderbook {
        Orderbook {
            price_decimals,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub fn process_update(&mut self, up: &Update) {
        if up.is_trade {
            let p = self.discretize(up.price);
            let book = if up.is_bid {
                &mut self.bids
            } else {
                &mut self.asks
            };
            book.entry(p).and_modify(|size| *size -= up.size as Size);
        } else {
            let price = self.discretize(up.price);
            let book = if up.is_bid {
                &mut self.bids
            } else {
                &mut self.asks
            };
            book.insert(price, up.size as Size);
            if book[&price] == 0. {
                book.remove(&price);
            }
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn clean(&mut self) {
        self.bids = self
            .bids
            .iter()
            .map(|(&a, &b)| (a, b))
            .filter(|&(_p, s)| s != 0.)
            .collect::<BTreeMap<Price, Size>>();
        self.asks = self
            .asks
            .iter()
            .map(|(&a, &b)| (a, b))
            .filter(|&(_p, s)| s != 0.)
            .collect::<BTreeMap<Price, Size>>();
    }

    pub fn top(&self) -> Option<((f32, Size), (f32, Size))> {
        let bid_max = self.bids.iter().next_back()?;
        let ask_min = self.asks.iter().next()?;
        let (bid_p, bid_s) = (self.undiscretize(*bid_max.0), *bid_max.1);
        let (ask_p, ask_s) = (self.undiscretize(*ask_min.0), *ask_min.1);
        Some(((bid_p, bid_s), (ask_p, ask_s)))
    }

    pub fn best_bid_raw(&self) -> Option<u64> {
        let (bid_p, _bid_s) = self.bids.iter().next_back()?;
        Some(*bid_p)
    }

    pub fn best_ask_raw(&self) -> Option<u64> {
        let (ask_p, _ask_s) = self.asks.iter().next()?;
        Some(*ask_p)
    }

    pub fn midprice_raw(&self) -> Option<u64> {
        let bb = self.best_bid_raw()?;
        let ba = self.best_ask_raw()?;
        Some((bb + ba) / 2)
    }

    pub fn best_bid(&self) -> Option<f32> {
        let (bid_p, _bid_s) = self.bids.iter().next_back()?;
        Some(self.undiscretize(*bid_p))
    }

    pub fn best_ask(&self) -> Option<f32> {
        let (ask_p, _ask_s) = self.asks.iter().next()?;
        Some(self.undiscretize(*ask_p))
    }

    pub fn midprice(&self) -> Option<f32> {
        let bb = self.best_bid()?;
        let ba = self.best_ask()?;
        Some((bb + ba) / 2.)
    }
}

impl fmt::Debug for Orderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "bids:\n");
        for (&price, size) in self.bids.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                self.undiscretize(price),
                size
            );
        }
        let _ = write!(f, "\n");

        let _ = write!(f, "asks:\n");
        for (&price, size) in self.asks.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                self.undiscretize(price),
                size
            );
        }
        write!(f, "\n")
    }
}

pub struct RebinnedOrderbook {
    pub book: IndexMap<u64, Orderbook>,
    pub price_hist: Histogram,
}

impl RebinnedOrderbook {
    pub fn from(
        price_decimals: u8,
        ups: &[Update],
        step_bins: BinCount,
        tick_bins: BinCount,
        m: f64,
    ) -> RebinnedOrderbook {
        let (price_hist, step_hist) = Histogram::from(&ups, step_bins, tick_bins, m);
        let mut fine_level = Orderbook::with_precision(price_decimals);
        let mut temp_ob = Orderbook::with_precision(price_decimals);
        let mut ob_across_time = IndexMap::<Time, Orderbook>::new();

        for up in ups.iter() {
            if up.is_trade {
                continue;
            }

            let ts = step_hist.to_bin((up.ts / 1000) as Size);
            let price = price_hist.to_bin(up.price as f64);

            if ts == None || price == None {
                continue;
            }
            let coarse_time = ts.unwrap().to_bits();
            let coarse_price = temp_ob.discretize(price.unwrap() as f32);

            let coarse_size = {
                fine_level.clean();
                let fine_book = if up.is_bid {
                    &mut fine_level.bids
                } else {
                    &mut fine_level.asks
                };
                let fine_size = fine_book
                    .entry(temp_ob.discretize(up.price))
                    .or_insert(up.size as Size);

                let local_side = if up.is_bid {
                    &mut temp_ob.bids
                } else {
                    &mut temp_ob.asks
                };
                let coarse_size = (*local_side).entry(coarse_price).or_insert(up.size as Size);

                if (*fine_size) == up.size as Size {
                    ()
                } else if (*fine_size) > up.size as Size {
                    *coarse_size -= (*fine_size) - up.size as Size;
                } else {
                    *coarse_size += up.size as Size - *fine_size;
                }

                *fine_size = up.size as Size;

                if *coarse_size < 0. {
                    *coarse_size = 0.;
                }

                *coarse_size
            };

            if !ob_across_time.contains_key(&coarse_time) {
                ob_across_time.insert(coarse_time, temp_ob.clone());
            } else {
                let ob_at_time = ob_across_time.get_mut(&coarse_time).unwrap();
                let global_side = if up.is_bid {
                    &mut ob_at_time.bids
                } else {
                    &mut ob_at_time.asks
                };
                (*global_side).insert(coarse_price, coarse_size);
            }
        }

        for v in ob_across_time.values_mut() {
            v.clean();
        }

        RebinnedOrderbook {
            book: ob_across_time,
            price_hist,
        }
    }
}

impl fmt::Debug for RebinnedOrderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (&ts, ob) in self.book.iter() {
            let _ = write!(f, "ts: {}\n", f64::from_bits(ts));
            let _ = write!(f, "{:?}\n", ob);
        }
        write!(f, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::file_format::decode;

    static FNAME: &str = "./internal/mocks/data.wstf";

    #[test]
    fn test_level_orderbook() {
        let step_bins = 50;
        let tick_bins = 50;

        let ups = decode(FNAME, Some(1000)).unwrap();
        let ob = RebinnedOrderbook::from(10, ups.as_slice(), step_bins, tick_bins, 2.);

        assert_eq!(ob.book.len(), step_bins - 1);
        for v in ob.book.values() {
            assert!(v.bids.values().len() < tick_bins);
            assert!(v.asks.values().len() < tick_bins);
        }
    }
}
