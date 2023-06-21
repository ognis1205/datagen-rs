use clap::Parser;
use h2o::io::config::Config;
use h2o::iter::extension::{KeySet, OptionalIterator, SamplingIterator, UniqueValueIterator};
use h2o::rand::{init as init_rand, rewind as rewind_rand, RandRange};
use indicatif::ProgressIterator;
use log::{debug, error, info, log_enabled, Level};
use std::time::{Duration, Instant};

/// Rust program to generate H2O groupby dataset.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Name of rows
    #[arg(short = 'N', long, value_parser = clap::value_parser!(u32).range(1..))]
    number_of_rows: u32,
    /// K groups factors
    #[arg(short = 'K', long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    k_groups_factors: u32,
    /// NAs ratio
    #[arg(short = 'n', long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(0..100))]
    nas_ratio: u32,
    /// Sort flag
    #[arg(short, long, default_value_t = false)]
    sort: bool,
}

fn main() {
    env_logger::init();
    let args = Args::parse();

    info!("number of rows: {}", args.number_of_rows);
    info!("K groups factors: {}", args.k_groups_factors);
    info!("NAs ratio: {}", args.nas_ratio);
    info!("Sort flag: {}", args.sort);
    let start = Instant::now();

    info!("Creating id1 column...");
    let config = Config::new("./id1.csv");
    let (seed, mut rand) = init_rand();
    let unique_id1s = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    let mut rand = rewind_rand(seed);
    let mut writer = config.writer().unwrap();
    for id1 in (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .progress()
        .none_by_value(unique_id1s)
    {
        if let Some(id1) = id1 {
            writer.write_record(&[id1.to_string()]);
        } else {
            writer.write_record(&[b""]);
        }
    }

    info!("Creating id2 column...");
    let config = Config::new("./id2.csv");
    let (seed, mut rand) = init_rand();
    let unique_id1s = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    let mut rand = rewind_rand(seed);
    let mut writer = config.writer().unwrap();
    for id1 in (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .progress()
        .none_by_value(unique_id1s)
    {
        if let Some(id1) = id1 {
            writer.write_record(&[id1.to_string()]);
        } else {
            writer.write_record(&[b""]);
        }
    }

    info!("Creating id3 column...");
    let config = Config::new("./id3.csv");
    let (seed, mut rand) = init_rand();
    let unique_id1s = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    let mut rand = rewind_rand(seed);
    let mut writer = config.writer().unwrap();
    for id1 in (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors))
        .progress()
        .none_by_value(unique_id1s)
    {
        if let Some(id1) = id1 {
            writer.write_record(&[id1.to_string()]);
        } else {
            writer.write_record(&[b""]);
        }
    }

    let end = start.elapsed();
    info!(
        "Elapsed time: {}.{:03} [sec]",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );
}
