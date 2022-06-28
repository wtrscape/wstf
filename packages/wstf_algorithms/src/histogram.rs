use std::cmp::Ordering::{self, Equal, Greater, Less};
use std::collections::HashMap;
use std::mem;
use wstf_update::Update;
use wstf_utils::{bigram, fill_digits};

type Price = f64;
pub type BinCount = usize;

#[derive(Debug)]
pub struct Histogram {
    pub(crate) bins: Option<Vec<BinCount>>,
    pub boundaries: Vec<Price>,
    boundary2idx: HashMap<u64, usize>,
    cached_bigram: Vec<(f64, f64)>,
}

impl Histogram {
    pub fn new(prices: &[Price], bin_count: BinCount, m: f64) -> Histogram {
        let filtered = reject_outliers(prices, m);
        build_histogram(filtered, bin_count)
    }

    pub fn to_bin(&self, price: Price) -> Option<Price> {
        let cb = &self.cached_bigram;
        for &(s, b) in cb.iter() {
            if (s == price) || (b > price && price > s) {
                return Some(s);
            }
        }
        return None;
    }

    fn new_boundaries(min_ts: u64, max_ts: u64, step_bins: usize) -> Histogram {
        let bucket_size = (max_ts - min_ts) / ((step_bins - 1) as u64);
        let mut boundaries = vec![];

        let mut lookup_table = HashMap::new();
        for i in 0..step_bins {
            let boundary = (min_ts + (i as u64) * bucket_size) as f64;
            boundaries.push(boundary);
            lookup_table.insert(boundary.to_bits(), i);
        }

        let cached_bigram = bigram(&boundaries);

        Histogram {
            bins: None,
            boundaries,
            boundary2idx: lookup_table,
            cached_bigram,
        }
    }

    pub fn from(
        ups: &[Update],
        step_bins: BinCount,
        tick_bins: BinCount,
        m: f64,
    ) -> (Histogram, Histogram) {
        let prices = ups.iter().map(|up| up.price as f64).collect::<Vec<f64>>();
        let price_hist = Histogram::new(&prices, tick_bins, m);

        let min_ts = fill_digits(ups.iter().next().unwrap().ts) / 1000;
        let max_ts = fill_digits(ups.iter().next_back().unwrap().ts) / 1000;
        let step_hist = Histogram::new_boundaries(min_ts, max_ts, step_bins);

        (price_hist, step_hist)
    }

    pub fn index(&self, price: Price) -> usize {
        *self.boundary2idx.get(&price.to_bits()).unwrap()
    }
}

fn reject_outliers(prices: &[Price], m: f64) -> Vec<Price> {
    let median = (*prices).median();

    let d = prices
        .iter()
        .map(|p| {
            let v = p - median;
            if v > 0. {
                v
            } else {
                -v
            }
        })
        .collect::<Vec<f64>>();
    let mdev = d.median();
    let s = d
        .iter()
        .map(|a| if mdev > 0. { a / mdev } else { 0. })
        .collect::<Vec<f64>>();
    let filtered = prices
        .iter()
        .enumerate()
        .filter(|&(i, _p)| s[i] < m)
        .map(|(_i, &p)| p)
        .collect::<Vec<f64>>();

    filtered
}

fn build_histogram(filtered_vals: Vec<Price>, bin_count: BinCount) -> Histogram {
    let max = &filtered_vals.max();
    let min = &filtered_vals.min();
    let bucket_size = (max - min) / ((bin_count - 1) as f64);

    let mut bins = vec![0; bin_count as usize];
    for price in filtered_vals.iter() {
        let mut bucket_index = 0;
        if bucket_size > 0.0 {
            bucket_index = ((price - min) / bucket_size) as usize;
            if bucket_index == bin_count {
                bucket_index -= 1;
            }
        }
        bins[bucket_index] += 1;
    }

    let mut boundaries = vec![];
    let mut lookup_table = HashMap::new();
    for i in 0..bin_count {
        let boundary = min + i as f64 * bucket_size;
        boundaries.push(boundary);
        lookup_table.insert(boundary.to_bits(), i);
    }

    let cached_bigram = bigram(&boundaries);

    Histogram {
        bins: Some(bins),
        boundaries,
        boundary2idx: lookup_table,
        cached_bigram,
    }
}

pub trait Stats {
    fn sum(&self) -> f64;
    fn min(&self) -> f64;
    fn max(&self) -> f64;
    fn mean(&self) -> f64;
    fn median(&self) -> f64;
    fn var(&self) -> f64;
    fn std_dev(&self) -> f64;
    fn std_dev_pct(&self) -> f64;
    fn median_abs_dev(&self) -> f64;
    fn median_abs_dev_pct(&self) -> f64;
    fn percentile(&self, pct: f64) -> f64;
    fn quartiles(&self) -> (f64, f64, f64);
    fn iqr(&self) -> f64;
}

impl Stats for [f64] {
    fn sum(&self) -> f64 {
        let mut partials = vec![];

        for &x in self {
            let mut x = x;
            let mut j = 0;
            for i in 0..partials.len() {
                let mut y: f64 = partials[i];
                if x.abs() < y.abs() {
                    mem::swap(&mut x, &mut y);
                }
                let hi = x + y;
                let lo = y - (hi - x);
                if lo != 0.0 {
                    partials[j] = lo;
                    j += 1;
                }
                x = hi;
            }
            if j >= partials.len() {
                partials.push(x);
            } else {
                partials[j] = x;
                partials.truncate(j + 1);
            }
        }
        let zero: f64 = 0.0;
        partials.iter().fold(zero, |p, q| p + *q)
    }

    fn min(&self) -> f64 {
        debug_assert!(!self.is_empty());
        self.iter().fold(self[0], |p, q| p.min(*q))
    }

    fn max(&self) -> f64 {
        debug_assert!(!self.is_empty());
        self.iter().fold(self[0], |p, q| p.max(*q))
    }

    fn mean(&self) -> f64 {
        debug_assert!(!self.is_empty());
        self.sum() / (self.len() as f64)
    }

    fn median(&self) -> f64 {
        self.percentile(50 as f64)
    }

    fn var(&self) -> f64 {
        if self.len() < 2 {
            0.0
        } else {
            let mean = self.mean();
            let mut v: f64 = 0.0;
            for s in self {
                let x = *s - mean;
                v = v + x * x;
            }
            let denom = (self.len() - 1) as f64;
            v / denom
        }
    }

    fn std_dev(&self) -> f64 {
        self.var().sqrt()
    }

    fn std_dev_pct(&self) -> f64 {
        let hundred = 100 as f64;
        (self.std_dev() / self.mean()) * hundred
    }

    fn median_abs_dev(&self) -> f64 {
        let med = self.median();
        let abs_devs: Vec<f64> = self.iter().map(|&v| (med - v).abs()).collect();
        let number = 1.4826;
        abs_devs.median() * number
    }

    fn median_abs_dev_pct(&self) -> f64 {
        let hundred = 100 as f64;
        (self.median_abs_dev() / self.median()) * hundred
    }

    fn percentile(&self, pct: f64) -> f64 {
        let mut tmp = self.to_vec();
        local_sort(&mut tmp);
        percentile_of_sorted(&tmp, pct)
    }

    fn quartiles(&self) -> (f64, f64, f64) {
        let mut tmp = self.to_vec();
        local_sort(&mut tmp);
        let first = 25f64;
        let a = percentile_of_sorted(&tmp, first);
        let second = 50f64;
        let b = percentile_of_sorted(&tmp, second);
        let third = 75f64;
        let c = percentile_of_sorted(&tmp, third);
        (a, b, c)
    }

    fn iqr(&self) -> f64 {
        let (a, _, c) = self.quartiles();
        c - a
    }
}

fn percentile_of_sorted(sorted_samples: &[f64], pct: f64) -> f64 {
    debug_assert!(!sorted_samples.is_empty());
    if sorted_samples.len() == 1 {
        return sorted_samples[0];
    }
    let zero: f64 = 0.0;
    debug_assert!(zero <= pct);
    let hundred = 100f64;
    debug_assert!(pct <= hundred);
    if pct == hundred {
        return sorted_samples[sorted_samples.len() - 1];
    }
    let length = (sorted_samples.len() - 1) as f64;
    let rank = (pct / hundred) * length;
    let lrank = rank.floor();
    let d = rank - lrank;
    let n = lrank as usize;
    let lo = sorted_samples[n];
    let hi = sorted_samples[n + 1];
    lo + (hi - lo) * d
}

fn local_sort(v: &mut [f64]) {
    v.sort_by(|x: &f64, y: &f64| local_cmp(*x, *y));
}

fn local_cmp(x: f64, y: f64) -> Ordering {
    if y.is_nan() {
        Less
    } else if x.is_nan() {
        Greater
    } else if x < y {
        Less
    } else if x == y {
        Equal
    } else {
        Greater
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wstf_protocol::file_format::decode;

    static FNAME: &str = "../../internal/mocks/data.wstf";

    use std::collections::HashMap;

    #[test]
    fn test_histogram() {
        let records = decode(FNAME, Some(10000)).unwrap();
        let prices: Vec<Price> = records.into_iter().map(|up| up.price as f64).collect();

        let _hist = Histogram::new(&prices, 100, 2.);
    }

    #[test]
    fn test_epoch_histogram() {
        let step_bins = 10;
        let min_ts = 1_000;
        let max_ts = 10_000;
        let bucket_size = (max_ts - min_ts) / (step_bins as u64 - 1);
        let mut boundaries = vec![];
        let mut boundary2idx = HashMap::new();
        for i in 0..step_bins {
            let boundary = min_ts as f64 + i as f64 * bucket_size as f64;
            boundaries.push(boundary);
            boundary2idx.insert(boundary.to_bits(), i);
        }

        let cached_bigram = bigram(&boundaries);

        let step_hist = Histogram {
            bins: None,
            boundaries,
            boundary2idx,
            cached_bigram,
        };

        assert_eq!(step_hist.boundaries.len(), step_bins as usize);
        for i in min_ts..max_ts {
            assert_eq!(Some((i / 1000 * 1000) as f64), step_hist.to_bin(i as f64));
        }
    }
}
