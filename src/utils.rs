use rand::Rng;
pub use tinyrand::RandRange;
use tinyrand::{Seeded, StdRand};

pub fn init_rand() -> (u64, StdRand) {
    let seed: u64 = rand::thread_rng().gen();
    (seed, StdRand::seed(seed))
}

pub fn rewind_rand(seed: u64) -> StdRand {
    StdRand::seed(seed)
}
