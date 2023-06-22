use rand::Rng;
pub use tinyrand::RandRange;
use tinyrand::{Seeded, StdRand};

pub fn init() -> (u64, StdRand) {
    let seed: u64 = rand::thread_rng().gen();
    (seed, StdRand::seed(seed))
}

pub fn rewind(seed: u64) -> StdRand {
    StdRand::seed(seed)
}
