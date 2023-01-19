use super::histogram::{BinCount, Histogram};
use crate::update::Update;
use indexmap::IndexMap;
use std::{f64, fmt};

type PriceBits = u64;
type Size = f32;
type Time = u64;
type OrderbookSide = IndexMap<PriceBits, Size>;

#[derive(Clone)]
pub struct Orderbook {
    pub bids: OrderbookSide,
    pub asks: OrderbookSide,
}

impl Orderbook {
    pub fn new() -> Orderbook {
        Orderbook {
            bids: IndexMap::new(),
            asks: IndexMap::new(),
        }
    }

    pub fn clean(&mut self) {
        self.bids = self
            .bids
            .iter()
            .map(|(&a, &b)| (a, b))
            .filter(|&(_p, s)| s != 0.)
            .collect::<IndexMap<PriceBits, Size>>();
        self.asks = self
            .asks
            .iter()
            .map(|(&a, &b)| (a, b))
            .filter(|&(_p, s)| s != 0.)
            .collect::<IndexMap<PriceBits, Size>>();
    }
}

impl fmt::Debug for Orderbook {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "bids:\n");
        for (&price, size) in self.bids.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                f64::from_bits(price),
                size
            );
        }
        let _ = write!(f, "\n");

        let _ = write!(f, "asks:\n");
        for (&price, size) in self.asks.iter() {
            let _ = write!(
                f,
                "- price: {} \t - size: {}\n",
                f64::from_bits(price),
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
        ups: &[Update],
        step_bins: BinCount,
        tick_bins: BinCount,
        m: f64,
    ) -> RebinnedOrderbook {
        let (price_hist, step_hist) = Histogram::from(&ups, step_bins, tick_bins, m);
        let mut fine_level = Orderbook::new();
        let mut temp_ob = Orderbook::new();
        let mut ob_across_time = IndexMap::<Time, Orderbook>::new();

        for up in ups.iter() {
            if up.is_trade {
                continue;
            }

            let ts = step_hist.to_bin((up.ts / 1000) as f64);
            let price = price_hist.to_bin(up.price as f64);

            if ts == None || price == None {
                continue;
            }
            let coarse_time = ts.unwrap().to_bits();
            let coarse_price = price.unwrap().to_bits();

            let coarse_size = {
                fine_level.clean();
                let fine_book = if up.is_bid {
                    &mut fine_level.bids
                } else {
                    &mut fine_level.asks
                };
                let fine_size = fine_book
                    .entry((up.price as f64).to_bits())
                    .or_insert(up.size);

                let local_side = if up.is_bid {
                    &mut temp_ob.bids
                } else {
                    &mut temp_ob.asks
                };
                let coarse_size = (*local_side).entry(coarse_price).or_insert(up.size);

                if (*fine_size) == up.size {
                    ()
                } else if (*fine_size) > up.size {
                    *coarse_size -= (*fine_size) - up.size;
                } else {
                    *coarse_size += up.size - (*fine_size);
                }

                *fine_size = up.size;

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
        let step_bins = 100;
        let tick_bins = 100;

        let ups = decode(FNAME, Some(1000)).unwrap();
        let ob = RebinnedOrderbook::from(ups.as_slice(), step_bins, tick_bins, 2.);
        print!("{:?}", ob.book.values().next_back().unwrap());

        assert_eq!(
            format!("{:?}", ob.book.values().next_back().unwrap()),
            "bids:\n- price: 0.04016996005719358 \t - size: 38.120235\n- price: \
             0.03667324341156266 \t - size: 0.0016224682\n- price: 0.03450860929760066 \t - size: \
             0.01509928\n- price: 0.03500814024697651 \t - size: 0.01485336\n- price: \
             0.03883787752552466 \t - size: 0.08259884\n- price: 0.03867136720906604 \t - size: \
             0.16104335\n- price: 0.03833834657614881 \t - size: 0.013514962\n- price: \
             0.03800532594323158 \t - size: 0.01599994\n- price: 0.037838815626772965 \t - size: \
             0.1136557\n- price: 0.036007202145728195 \t - size: 0.09498042\n- price: \
             0.03550767119635235 \t - size: 0.1076455\n- price: 0.03534116087989374 \t - size: \
             0.07305564\n- price: 0.034342098981142044 \t - size: 0.07188734\n- price: \
             0.03467511961405927 \t - size: 0.06212443\n- price: 0.03517465056343512 \t - size: \
             0.01685138\n- price: 0.03683975372802128 \t - size: 0.061147638\n\nasks:\n- price: \
             0.04016996005719358 \t - size: 18.646933\n- price: 0.0403364703736522 \t - size: \
             6.431776\n- price: 0.04849547588012435 \t - size: 0.05201613\n- price: \
             0.05066010999408635 \t - size: 0.01627521\n- price: 0.043000635436990044 \t - size: \
             0.15773247\n- price: 0.04316714575344866 \t - size: 0.08569955\n- price: \
             0.04333365606990727 \t - size: 0.09909299\n- price: 0.04050298069011081 \t - size: \
             7.665366\n- price: 0.04283412512053143 \t - size: 0.0914046\n- price: \
             0.04566480050032789 \t - size: 0.107617415\n- price: 0.0415020425888625 \t - size: \
             0.04366916\n- price: 0.04483224891803481 \t - size: 0.04749457\n- price: \
             0.04533177986741066 \t - size: 0.05052652\n- price: 0.04583131081678651 \t - size: \
             0.04781884\n- price: 0.04666386239907958 \t - size: 0.04340866\n- price: \
             0.04716339334845543 \t - size: 0.04984047\n- price: 0.04100251163948666 \t - size: \
             0.03891401\n- price: 0.04649735208262097 \t - size: 0.05086191\n- price: \
             0.047662924297831276 \t - size: 0.042505\n- price: 0.04866198619658297 \t - size: \
             0.056027785\n- price: 0.05049359967762774 \t - size: 0.05640602\n- price: \
             0.04499875923449343 \t - size: 655.1983\n- price: 0.04133553227240389 \t - size: \
             0.03494673\n- price: 0.04366667670282451 \t - size: 0.02538946\n- price: \
             0.04633084176616235 \t - size: 0.02565859\n- price: 0.0446657386015762 \t - size: \
             0.03094793\n- price: 0.04749641398137266 \t - size: 0.04114973\n- price: \
             0.04999406872825189 \t - size: 425.2304\n- price: 0.04699688303199681 \t - size: \
             0.13791765\n- price: 0.04216808385469697 \t - size: 0.03671673\n\n"
        );
    }
}
