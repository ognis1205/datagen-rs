use crate::iter::none_by::{none_by_index, none_by_value, NoneByIndex, NoneByValue};
pub use crate::iter::types::KeySet;
use crate::iter::unique_by::{unique, unique_by, Unique, UniqueBy};
use crate::rand::{init as init_rand, RandRange};
use std::hash::Hash;

pub trait UniqueValueIterator: Iterator {
    fn unique(self) -> Unique<Self>
    where
        Self: Sized,
        Self::Item: Clone + Eq + Hash,
    {
        unique(self)
    }

    fn unique_by<V, F>(self, f: F) -> UniqueBy<Self, V, F>
    where
        Self: Sized,
        V: Eq + Hash,
        F: FnMut(&Self::Item) -> V,
    {
        unique_by(self, f)
    }
}

impl<T: ?Sized> UniqueValueIterator for T where T: Iterator {}

pub trait SamplingIterator: Iterator {
    fn choose(mut self, amount: usize) -> KeySet<Self::Item>
    where
        Self: Sized,
        Self::Item: Clone + Eq + Hash,
    {
        if amount < 1 {
            return KeySet::with_capacity_and_hasher(0, Default::default());
        }
        let (_, mut rand) = init_rand();
        let mut reservoir = Vec::with_capacity(amount);
        reservoir.extend(self.by_ref().take(amount));
        if reservoir.len() == amount {
            for (i, e) in self.enumerate() {
                let k = rand.next_range(0..(i + 1 + amount));
                if let Some(s) = reservoir.get_mut(k) {
                    *s = e;
                }
            }
        } else {
            reservoir.shrink_to_fit();
        }
        KeySet::from_iter(reservoir)
    }
}

impl<T: ?Sized> SamplingIterator for T where T: Iterator {}

pub trait OptionalIterator: Iterator {
    fn none_by_index(self, indices: KeySet<usize>) -> NoneByIndex<Self>
    where
        Self: Sized,
    {
        none_by_index(self, indices)
    }

    fn none_by_value(self, values: KeySet<Self::Item>) -> NoneByValue<Self>
    where
        Self: Sized,
    {
        none_by_value(self, values)
    }
}

impl<T: ?Sized> OptionalIterator for T where T: Iterator {}
