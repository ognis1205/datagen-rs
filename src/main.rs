use anyhow::Result;
use csv::Writer;
use h2o::iter::extension::{KeySet, OptionalIterator, SamplingIterator, UniqueValueIterator};
use indicatif::ProgressIterator;
use std::time::{Duration, Instant};
use tinyrand::{Rand, RandRange, StdRand};

fn main() -> Result<()> {
    let start = Instant::now();
    let N = 100;
    //    let N: u32 = 1000_000_000;
    //    let N: u32 = 100_000_000;
    let K: u32 = 1;
    //    let K = 100;
    let nas = 5;

    let mut rand = StdRand::default();
    let uniques = (0..N)
        .map(|_| rand.next_range(1..N / K))
        .unique()
        .choose(10_000_000);

    let mut rand = StdRand::default();
    let values: Vec<_> = (0..N)
        .map(|_| rand.next_range(1..N / K))
        //        .none_by_value(uniques)
        .none_by_index(KeySet::from_iter(vec![0, 1, 2, 3]))
        .collect();

    //    println!("{:?}", values);

    let end = start.elapsed();
    println!(
        "{}.{:03} [sec]",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );

    Ok(())
}
