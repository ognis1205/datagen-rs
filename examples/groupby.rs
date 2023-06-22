use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use datagen::io::config::Config;
use datagen::io::manipulate::zip;
use datagen::iter::extension::{KeySet, OptionalIterator, SamplingIterator, UniqueValueIterator};
use datagen::utils::rand::{init as init_rand, rewind as rewind_rand, RandRange};
use indicatif::ProgressIterator;
use log::info;
use std::io::Seek;
use std::time::Instant;
use tempfile::tempfile;

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
    #[arg(short = 'n', long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..100))]
    nas_ratio: u32,
    /// Sort flag
    #[arg(short, long, default_value_t = false)]
    sort: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    info!(
        "number of rows: {}, K groups factors: {}, NAs ratio: {}, Sort flag: {}",
        args.number_of_rows, args.k_groups_factors, args.nas_ratio, args.sort
    );
    let config = Config::default();
    let start = Instant::now();

    info!("Creating id1 column...");
    let mut id1_csv = tempfile().context("failed to create temporary file for the id1 column")?;
    {
        info!("Creating id1 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id1 N/A values...");
        info!("Dumping id1 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id1_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[format!("id{:03}", id)])
                    .context("failed to write data into the id1 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id1 column")?;
            }
        }
        info!("Dumped id1 column...");
    }
    id1_csv
        .rewind()
        .context("failed to rewind file descriptor for the id1 column")?;
    info!("Created id1 column...");

    info!("Creating id2 column...");
    let mut id2_csv = tempfile().context("failed to create temporary file for the id2 column")?;
    {
        info!("Creating id2 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id2 N/A values...");
        info!("Dumping id2 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id2_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[format!("id{:03}", id)])
                    .context("failed to write data into the id2 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id2 column")?;
            }
        }
        info!("Dumped id2 column...");
    }
    id2_csv
        .rewind()
        .context("failed to rewind file descriptor for the id2 column")?;
    info!("Created id2 column...");

    info!("Creating id3 column...");
    let mut id3_csv = tempfile().context("failed to create temporary file for the id3 column")?;
    {
        info!("Creating id3 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.number_of_rows / args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id3 N/A values...");
        info!("Dumping id3 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id3_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[format!("id{:010}", id)])
                    .context("failed to write data into the id3 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id3 column")?;
            }
        }
        info!("Dumped id3 column...");
    }
    id3_csv
        .rewind()
        .context("failed to rewind file descriptor for the id3 column")?;
    info!("Created id3 column...");

    info!("Creating id4 column...");
    let mut id4_csv = tempfile().context("failed to create temporary file for the id4 column")?;
    {
        info!("Creating id4 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id4 N/A values...");
        info!("Dumping id4 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id4_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[id.to_string()])
                    .context("failed to write data into the id4 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id4 column")?;
            }
        }
        info!("Dumped id4 column...");
    }
    id4_csv
        .rewind()
        .context("failed to rewind file descriptor for the id4 column")?;
    info!("Created id4 column...");

    info!("Creating id5 column...");
    let mut id5_csv = tempfile().context("failed to create temporary file for the id5 column")?;
    {
        info!("Creating id5 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id5 N/A values...");
        info!("Dumping id5 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id5_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[id.to_string()])
                    .context("failed to write data into the id5 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id5 column")?;
            }
        }
        info!("Dumped id5 column...");
    }
    id5_csv
        .rewind()
        .context("failed to rewind file descriptor for the id5 column")?;
    info!("Created id5 column...");

    info!("Creating id6 column...");
    let mut id6_csv = tempfile().context("failed to create temporary file for the id6 column")?;
    {
        info!("Creating id6 N/A values (this may take a while)...");
        let (seed, mut rand) = init_rand();
        let unique_ids = (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
            .unique()
            .choose(
                (args.number_of_rows / args.k_groups_factors * args.nas_ratio / 100)
                    .try_into()
                    .unwrap(),
            );
        info!("Created id6 N/A values...");
        info!("Dumping id6 column...");
        let mut rand = rewind_rand(seed);
        let mut writer = config.from_writer(&mut id6_csv);
        for id in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
            .progress()
            .none_by_value(unique_ids)
        {
            if let Some(id) = id {
                writer
                    .write_record(&[id.to_string()])
                    .context("failed to write data into the id6 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the id6 column")?;
            }
        }
        info!("Dumped id6 column...");
    }
    id6_csv
        .rewind()
        .context("failed to rewind file descriptor for the id6 column")?;
    info!("Created id6 column...");

    info!("Creating v1 column...");
    let mut v1_csv = tempfile().context("failed to create temporary file for the v1 column")?;
    {
        info!("Creating v1 N/A indices (this may take a while)...");
        let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
            (args.number_of_rows * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
        info!("Created v1 N/A indices...");
        info!("Dumping v1 column...");
        let (_, mut rand) = init_rand();
        let mut writer = config.from_writer(&mut v1_csv);
        for v in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..6 as u32))
            .progress()
            .none_by_index(indices)
        {
            if let Some(v) = v {
                writer
                    .write_record(&[v.to_string()])
                    .context("failed to write data into the v1 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the v1 column")?;
            }
        }
        info!("Dumped v1 column...");
    }
    v1_csv
        .rewind()
        .context("failed to rewind file descriptor for the v1 column")?;
    info!("Created v1 column...");

    info!("Creating v2 column...");
    let mut v2_csv = tempfile().context("failed to create temporary file for the v2 column")?;
    {
        info!("Creating v2 N/A indices (this may take a while)...");
        let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
            (args.number_of_rows * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
        info!("Created v2 N/A indices...");
        info!("Dumping v2 column...");
        let (_, mut rand) = init_rand();
        let mut writer = config.from_writer(&mut v2_csv);
        for v in (0..args.number_of_rows)
            .map(|_| rand.next_range(1..16 as u32))
            .progress()
            .none_by_index(indices)
        {
            if let Some(v) = v {
                writer
                    .write_record(&[v.to_string()])
                    .context("failed to write data into the v2 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the v2 column")?;
            }
        }
        info!("Dumped v2 column...");
    }
    v2_csv
        .rewind()
        .context("failed to rewind file descriptor for the v2 column")?;
    info!("Created v2 column...");

    info!("Creating v3 column...");
    let mut v3_csv = tempfile().context("failed to create temporary file for the v3 column")?;
    {
        info!("Creating v3 N/A indices (this may take a while)...");
        let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
            (args.number_of_rows * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
        info!("Created v3 N/A indices...");
        info!("Dumping v3 column...");
        let (_, mut rand) = init_rand();
        let mut writer = config.from_writer(&mut v3_csv);
        for v in (0..args.number_of_rows)
            .map(|_| rand.next_range(0..100_000_001 as u32))
            .progress()
            .none_by_index(indices)
        {
            if let Some(v) = v {
                writer
                    .write_record(&[format!("{:.6}", v as f32 / 1000_000f32)])
                    .context("failed to write data into the v3 column")?;
            } else {
                writer
                    .write_record(&[""])
                    .context("failed to write data into the v3 column")?;
            }
        }
        info!("Dumped v3 column...");
    }
    v3_csv
        .rewind()
        .context("failed to rewind file descriptor for the v3 column")?;
    info!("Created v3 column...");

    info!("Merging columns...");
    {
        let mut id1_reader = config.from_reader(&mut id1_csv);
        let mut id2_reader = config.from_reader(&mut id2_csv);
        let mut id3_reader = config.from_reader(&mut id3_csv);
        let mut id4_reader = config.from_reader(&mut id4_csv);
        let mut id5_reader = config.from_reader(&mut id5_csv);
        let mut id6_reader = config.from_reader(&mut id6_csv);
        let mut v1_reader = config.from_reader(&mut v1_csv);
        let mut v2_reader = config.from_reader(&mut v2_csv);
        let mut v3_reader = config.from_reader(&mut v3_csv);
        let config = Config::new(
            format!(
                "./G1_{:e}_{:e}_{}_{}.csv",
                args.number_of_rows, args.k_groups_factors, args.nas_ratio, args.sort as i32
            )
            .as_str(),
        );
        let mut writer = config
            .writer()
            .context("failed to create the output file writer")?;
        zip(
            &mut writer,
            vec![
                &mut id1_reader,
                &mut id2_reader,
                &mut id3_reader,
                &mut id4_reader,
                &mut id5_reader,
                &mut id6_reader,
                &mut v1_reader,
                &mut v2_reader,
                &mut v3_reader,
            ],
        )
        .context("failed to zip columns")?;
    }
    info!("Merged columns...");

    let end = start.elapsed();
    info!(
        "Elapsed time: {}.{:03} [sec]",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );

    Ok(())
}
