use anyhow::Result;
use csv::Writer;
use h2o::iter::extension::{OptionalIterator, SamplingIterator, UniqueValueIterator};
use std::time::{Duration, Instant};
use tinyrand::{Rand, RandRange, StdRand};
//use rayon::prelude::*;
use indicatif::ProgressIterator;
//use polars::prelude::*;
//use rand::distributions::Distribution;
//use rand::distributions::Uniform;
//use rand::seq::IteratorRandom;

fn main() -> Result<()> {
    let start = Instant::now();
    //    let N = 100;
    //    let N: u32 = 1000_000_000;
    let N: u32 = 10_000_000;
    let K: u32 = 1;
    //    let K = 100;
    let nas = 5;

    let mut rand = StdRand::default();
    let uniques = (0..N)
        .map(|_| rand.next_range(1..N / K))
        .unique()
        .choose(1_000_000);

    let mut rand = StdRand::default();
    for v in (0..N)
        .map(|_| rand.next_range(1..N / K))
        .none_by_value(uniques)
    {
        println!("{:?}", v);
    }

    let end = start.elapsed();
    println!(
        "{}.{:03} [sec]",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );

    //    let mut wtr = Writer::from_path("foo.csv")?;
    //    wtr.write_record(&[Some("a"), Some("b"), Some("c")])?;
    //    wtr.write_record(&[Some("x"), None, Some("z")])?;
    //    wtr.flush()?;

    Ok(())
}
