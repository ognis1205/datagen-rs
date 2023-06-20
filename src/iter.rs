use crate::unique::{Unique, UniqueBy, unique, unique_by};
use std::hash::Hash;
use crate::types::HashSet;
use tinyrand::{Rand, StdRand, RandRange};

pub trait Extension: Iterator {
    fn unique(self) -> Unique<Self>
        where Self: Sized,
              Self::Item: Clone + Eq + Hash
    {
        unique(self)
    }

    fn unique_by<V, F>(self, f: F) -> UniqueBy<Self, V, F>
        where Self: Sized,
              V: Eq + Hash,
              F: FnMut(&Self::Item) -> V
    {
        unique_by(self, f)
    }

    fn choose(mut self, amount: usize) -> HashSet<Self::Item>
         where Self: Sized,
               Self::Item: Clone + Eq + Hash
    {
	if amount < 1 {
	    return HashSet::with_capacity_and_hasher(0, Default::default());
	}
	let mut rand = StdRand::default();
        let mut reservoir = Vec::with_capacity(amount);
        reservoir.extend(self.by_ref().take(amount));
        if reservoir.len() == amount {
            for (i, e) in self.enumerate() {
		//		let k = fastrand::usize(0..(i + 1 + amount));
		let k = rand.next_range(0..(i + 1 + amount));
                if let Some(s) = reservoir.get_mut(k) {
                    *s = e;
                }
            }
        } else {
            reservoir.shrink_to_fit();
        }
        HashSet::from_iter(reservoir)
    }
}

impl<T: ?Sized> Extension for T where T: Iterator { }
