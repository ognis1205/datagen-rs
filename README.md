datagen-rs
==============================

 Datagen is a Rust-based data generation library designed for low-memory environments.

H2O Group-by Dataset
==============================

 To display the help message, execute the following command within the project directory:

```bash
 $ cargo run --release --example groupby -- --help
Rust program to generate H2O groupby dataset

Usage: groupby [OPTIONS] --number-of-rows <NUMBER_OF_ROWS>

Options:
  -N, --number-of-rows <NUMBER_OF_ROWS>      Number of rows
  -K, --k-groups-factors <K_GROUPS_FACTORS>  K groups factors [default: 1]
  -n, --nas-ratio <NAS_RATIO>                N/A ratio [default: 0]
  -s, --sort                                 Sort flag
  -r, --run-size <RUN_SIZE>                  External merge sort, run size [default: 1048576]
  -d, --dir <DIR>                            Output directory [default: ./]
  -h, --help                                 Print help
  -V, --version                              Print version
```

 For instance, if you intend to generate 1E9 rows of data with 10 group factors, 10% NA values,
and without sorting, run the following command in the project directory:

```bash
 $ export RUST_LOG=INFO; cargo run --release --example groupby -- -N 1000000000 -K 10 -n 10 --sort
```

TODO
==============================
 - Support arbitrary working directory. The current implementation uses the system's temporary directory.
   If this lack of the feature issues, please consider using something like `sudo mount -o remount,size=125G /tmp/`.
 - Consider using more memory/disk efficient formats (filing a PR for, e.g., arrow-rs, polars, datafusion, and so forth).
