use anyhow::Result;
use csv::Writer;
use h2o::hash_map::HashSet;
use h2o::hash_map::SampleHashMap;
use h2o::iter::Extension;
use std::time::{Duration, Instant};
use tinyrand::{Rand, StdRand, RandRange};
//use rayon::prelude::*;
use indicatif::ProgressIterator;
//use polars::prelude::*;
//use rand::distributions::Distribution;
//use rand::distributions::Uniform;
//use rand::seq::IteratorRandom;

fn main() -> Result<()> {
    let start = Instant::now();

    //    let N = 100;
    let N: u32 = 1000_000_000;
    let K: u32 = 1;
    //    let K = 100;
    let nas = 5;
    let mut rand = StdRand::default();

    //    let mut df = DataFrame::default();
    //    let step = Uniform::new(1, N / K);
    //    let mut rng = rand::thread_rng();
    //    let seed: [u8; 32] = [13; 32];
    //    let mut rng: rand::rngs::StdRng = rand::SeedableRng::from_seed(seed);
    let vals = (0..N)
    //        .map(|_| format!("id{:>010}", step.sample(&mut rng)))
    //        .map(|_| step.sample(&mut rng));
    //        .map(|_| fastrand::u64(1..N/K));
	.map(|_| rand.next_range(1..N/K));
    //	.progress();
    //        .collect::<Vec<u64>>();
    //    println!("nas");
    //    let to_nas = Series::new("id3", vals.clone())
    //        .unique_stable()?
    //        .sample_frac(nas as f64 / 100.0, false, false, None)?;

    //    println!("{:?}", &vals);

    //    let sample = HashSet::sample(vals, 100_000_000, 100_000_000);

    let uniques = vals.unique().choose(100_000_000);//.collect();

    //    let uniques = vec![1, 2, 3].unique();

    let end = start.elapsed();
    println!("{}.{:03} [sec]", end.as_secs(), end.subsec_nanos() / 1_000_000);

    //    let mut wtr = Writer::from_path("foo.csv")?;
    //    wtr.write_record(&[Some("a"), Some("b"), Some("c")])?;
    //    wtr.write_record(&[Some("x"), None, Some("z")])?;
    //    wtr.flush()?;

    Ok(())
}
