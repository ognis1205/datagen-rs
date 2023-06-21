use clap::Parser;
use log::{debug, error, info, log_enabled, Level};

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

    for _ in 0..args.number_of_rows {
        println!("Hello {}!", args.number_of_rows)
    }
}
