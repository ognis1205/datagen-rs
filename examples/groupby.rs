use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use datagen::io::config::Config;
use datagen::io::manipulate::{hstack, merge_sort, sort_chunk, zip};
use datagen::iter::extensions::{KeySet, OptionalIterator, SamplingIterator, UniqueValueIterator};
use datagen::utils::rand::{init as init_rand, rewind as rewind_rand, RandRange};
use indicatif::ProgressIterator;
use std::fs;
use std::io::Seek;
use std::path;
use std::time;

/// Rust program to generate H2O groupby dataset.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of rows
    #[arg(short = 'N', long, value_parser = clap::value_parser!(u32).range(1..))]
    number_of_rows: u32,
    /// K groups factors
    #[arg(short = 'K', long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
    k_groups_factors: u32,
    /// N/A ratio
    #[arg(short = 'n', long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..100))]
    nas_ratio: u32,
    /// Sort flag
    #[arg(short, long, default_value_t = false)]
    sort: bool,
    /// External merge sort, run size
    #[arg(short, long, default_value_t = 1024 * 1024)]
    run_size: u32,
    /// Output directory
    #[arg(short, long, default_value_t = String::from("./"))]
    dir: String,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    log::info!(
        "number of rows: {}, K groups factors: {}, NAs ratio: {}, Sort flag: {}",
        args.number_of_rows,
        args.k_groups_factors,
        args.nas_ratio,
        args.sort
    );

    let config = Config::default();
    let start = time::Instant::now();

    let mut id1_csv =
        tempfile::tempfile().context("failed to create temporary file for the id1 column")?;
    create_id1(&mut id1_csv, &args, &config).context("failed to create the id1 column")?;
    id1_csv
        .rewind()
        .context("failed to rewind file descriptor for the id1 column")?;

    let mut id2_csv =
        tempfile::tempfile().context("failed to create temporary file for the id2 column")?;
    create_id2(&mut id2_csv, &args, &config).context("failed to create the id2 column")?;
    id2_csv
        .rewind()
        .context("failed to rewind file descriptor for the id2 column")?;

    let mut id3_csv =
        tempfile::tempfile().context("failed to create temporary file for the id3 column")?;
    create_id3(&mut id3_csv, &args, &config).context("failed to create the id3 column")?;
    id3_csv
        .rewind()
        .context("failed to rewind file descriptor for the id3 column")?;

    let mut id4_csv =
        tempfile::tempfile().context("failed to create temporary file for the id4 column")?;
    create_id4(&mut id4_csv, &args, &config).context("failed to create the id4 column")?;
    id4_csv
        .rewind()
        .context("failed to rewind file descriptor for the id4 column")?;

    let mut id5_csv =
        tempfile::tempfile().context("failed to create temporary file for the id5 column")?;
    create_id5(&mut id5_csv, &args, &config).context("failed to create the id5 column")?;
    id5_csv
        .rewind()
        .context("failed to rewind file descriptor for the id5 column")?;

    let mut id6_csv =
        tempfile::tempfile().context("failed to create temporary file for the id6 column")?;
    create_id6(&mut id6_csv, &args, &config).context("failed to create the id6 column")?;
    id6_csv
        .rewind()
        .context("failed to rewind file descriptor for the id6 column")?;

    let mut v1_csv =
        tempfile::tempfile().context("failed to create temporary file for the v1 column")?;
    create_v1(&mut v1_csv, &args, &config).context("failed to create the v1 column")?;
    v1_csv
        .rewind()
        .context("failed to rewind file descriptor for the v1 column")?;

    let mut v2_csv =
        tempfile::tempfile().context("failed to create temporary file for the v2 column")?;
    create_v2(&mut v2_csv, &args, &config).context("failed to create the v2 column")?;
    v2_csv
        .rewind()
        .context("failed to rewind file descriptor for the v2 column")?;

    let mut v3_csv =
        tempfile::tempfile().context("failed to create temporary file for the v3 column")?;
    create_v3(&mut v3_csv, &args, &config).context("failed to create the v3 column")?;
    v3_csv
        .rewind()
        .context("failed to rewind file descriptor for the v3 column")?;

    if !args.sort {
        join(
            id1_csv, id2_csv, id3_csv, id4_csv, id5_csv, id6_csv, v1_csv, v2_csv, v3_csv, &args,
            &config,
        )
        .context("failed to join columns")?;
    } else {
        join_with_sort(
            id1_csv, id2_csv, id3_csv, id4_csv, id5_csv, id6_csv, v1_csv, v2_csv, v3_csv, &args,
            &config,
        )
        .context("failed to join columns")?;
    }

    let end = start.elapsed();
    log::info!(
        "Elapsed time: {}.{:03} [sec]",
        end.as_secs(),
        end.subsec_nanos() / 1_000_000
    );

    Ok(())
}

fn create_id1(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id1 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id1 N/A values...");
    log::info!("Dumping id1 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id1 column...");
    Ok(())
}

fn create_id2(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id2 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id2 N/A values...");
    log::info!("Dumping id2 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id2 column...");
    Ok(())
}

fn create_id3(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id3 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.number_of_rows / args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id3 N/A values...");
    log::info!("Dumping id3 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id3 column...");
    Ok(())
}

fn create_id4(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id4 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id4 N/A values...");
    log::info!("Dumping id4 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id4 column...");
    Ok(())
}

fn create_id5(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id5 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id5 N/A values...");
    log::info!("Dumping id5 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id5 column...");
    Ok(())
}

fn create_id6(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating id6 N/A values (this may take a while)...");
    let (seed, mut rand) = init_rand();
    let unique_ids = (0..args.number_of_rows)
        .map(|_| rand.next_range(1..args.number_of_rows / args.k_groups_factors + 1))
        .unique()
        .choose(
            (args.number_of_rows / args.k_groups_factors * args.nas_ratio / 100)
                .try_into()
                .unwrap(),
        );
    log::info!("Created id6 N/A values...");
    log::info!("Dumping id6 column...");
    let mut rand = rewind_rand(seed);
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped id6 column...");
    Ok(())
}

fn create_v1(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating v1 N/A indices (this may take a while)...");
    let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
        (args.number_of_rows * args.nas_ratio / 100)
            .try_into()
            .unwrap(),
    );
    log::info!("Created v1 N/A indices...");
    log::info!("Dumping v1 column...");
    let (_, mut rand) = init_rand();
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped v1 column...");
    Ok(())
}

fn create_v2(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating v2 N/A indices (this may take a while)...");
    let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
        (args.number_of_rows * args.nas_ratio / 100)
            .try_into()
            .unwrap(),
    );
    log::info!("Created v2 N/A indices...");
    log::info!("Dumping v2 column...");
    let (_, mut rand) = init_rand();
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped v2 column...");
    Ok(())
}

fn create_v3(file: &mut fs::File, args: &Args, config: &Config) -> Result<()> {
    log::info!("Creating v3 N/A indices (this may take a while)...");
    let indices: KeySet<usize> = (0..args.number_of_rows as usize).choose(
        (args.number_of_rows * args.nas_ratio / 100)
            .try_into()
            .unwrap(),
    );
    log::info!("Created v3 N/A indices...");
    log::info!("Dumping v3 column...");
    let (_, mut rand) = init_rand();
    let mut writer = config.from_writer(file);
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
    log::info!("Dumped v3 column...");
    Ok(())
}

fn join(
    mut id1_csv: fs::File,
    mut id2_csv: fs::File,
    mut id3_csv: fs::File,
    mut id4_csv: fs::File,
    mut id5_csv: fs::File,
    mut id6_csv: fs::File,
    mut v1_csv: fs::File,
    mut v2_csv: fs::File,
    mut v3_csv: fs::File,
    args: &Args,
    config: &Config,
) -> Result<()> {
    log::info!("Joining columns...");
    let mut path = path::PathBuf::new();
    path.push(&args.dir);
    path.push(format!(
        "./G1_{:e}_{:e}_{}_{}.csv",
        args.number_of_rows, args.k_groups_factors, args.nas_ratio, args.sort as i32
    ));
    let mut g1_csv = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .context("failed to open the output")?;
    let mut csv_writer = config.from_writer(&mut g1_csv);
    let mut id1_reader = config.from_reader(&mut id1_csv);
    let mut id2_reader = config.from_reader(&mut id2_csv);
    let mut id3_reader = config.from_reader(&mut id3_csv);
    let mut id4_reader = config.from_reader(&mut id4_csv);
    let mut id5_reader = config.from_reader(&mut id5_csv);
    let mut id6_reader = config.from_reader(&mut id6_csv);
    let mut v1_reader = config.from_reader(&mut v1_csv);
    let mut v2_reader = config.from_reader(&mut v2_csv);
    let mut v3_reader = config.from_reader(&mut v3_csv);
    let mut zipped_iter = zip(vec![
        &mut id1_reader,
        &mut id2_reader,
        &mut id3_reader,
        &mut id4_reader,
        &mut id5_reader,
        &mut id6_reader,
        &mut v1_reader,
        &mut v2_reader,
        &mut v3_reader,
    ]);
    hstack(&mut csv_writer, &mut zipped_iter).context("failed to join columns")?;
    log::info!("Joined columns...");
    Ok(())
}

fn join_with_sort(
    mut id1_csv: fs::File,
    mut id2_csv: fs::File,
    mut id3_csv: fs::File,
    mut id4_csv: fs::File,
    mut id5_csv: fs::File,
    mut id6_csv: fs::File,
    mut v1_csv: fs::File,
    mut v2_csv: fs::File,
    mut v3_csv: fs::File,
    args: &Args,
    config: &Config,
) -> Result<()> {
    log::info!("Sorting rows...");
    let mut path = path::PathBuf::new();
    path.push(&args.dir);
    path.push(format!(
        "./G1_{:e}_{:e}_{}_{}.csv",
        args.number_of_rows, args.k_groups_factors, args.nas_ratio, args.sort as i32
    ));
    let mut g1_csv = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .context("failed to open the output")?;
    let mut csv_writer = config.from_writer(&mut g1_csv);
    let mut id1_reader = config.from_reader(&mut id1_csv);
    let mut id2_reader = config.from_reader(&mut id2_csv);
    let mut id3_reader = config.from_reader(&mut id3_csv);
    let mut id4_reader = config.from_reader(&mut id4_csv);
    let mut id5_reader = config.from_reader(&mut id5_csv);
    let mut id6_reader = config.from_reader(&mut id6_csv);
    let mut v1_reader = config.from_reader(&mut v1_csv);
    let mut v2_reader = config.from_reader(&mut v2_csv);
    let mut v3_reader = config.from_reader(&mut v3_csv);
    let mut zipped_iter = zip(vec![
        &mut id1_reader,
        &mut id2_reader,
        &mut id3_reader,
        &mut id4_reader,
        &mut id5_reader,
        &mut id6_reader,
        &mut v1_reader,
        &mut v2_reader,
        &mut v3_reader,
    ]);

    let number_of_runs = args.number_of_rows / args.run_size + 1;
    if number_of_runs <= 1 {
        log::info!("Sorting single run...");
        sort_chunk(None, &mut csv_writer, &mut zipped_iter).context("failed to sort a chunk")?;
        log::info!("Sorted single run...");
    } else {
        log::info!("Sorting {} runs...", number_of_runs);
        let working_dir = tempfile::tempdir().context("failed to create a temporary directory")?;
        let mut runs = vec![];
        for i in (0..number_of_runs).progress() {
            let name = format!("{}.csv", i);
            let chunk = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(working_dir.path().join(name.clone()))
                .context("failed to create a chunk file")?;
            let mut chunk_writer = config.from_writer(chunk);
            sort_chunk(
                Some(args.run_size as usize),
                &mut chunk_writer,
                &mut zipped_iter,
            )
            .context("failed to sort a chunk")?;
            runs.push(name.clone());
        }
        log::info!("Sorted {} runs...", number_of_runs);

        drop(id1_csv);
        drop(id2_csv);
        drop(id3_csv);
        drop(id4_csv);
        drop(id5_csv);
        drop(id6_csv);
        drop(v1_csv);
        drop(v2_csv);
        drop(v3_csv);

        log::info!("Joining rows...");
        let mut runs: Vec<_> = runs
            .iter()
            .filter_map(|run| {
                fs::OpenOptions::new()
                    .read(true)
                    .open(working_dir.path().join(run.clone()))
                    .ok()
            })
            .map(|chunk| config.from_reader(chunk))
            .collect();
        merge_sort(&mut csv_writer, &mut runs).context("failed to merge-sort chunks")?;
        log::info!("Joined rows...");
    }
    log::info!("Sorted rows...");
    Ok(())
}
