use crate::iter::none::{none_by_index, none_by_value, NoneByIndex, NoneByValue};
use crate::iter::types::HashSet;
use crate::iter::unique::{unique, unique_by, Unique, UniqueBy};
use std::hash::Hash;
use tinyrand::{RandRange, StdRand};

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
    fn choose(mut self, amount: usize) -> HashSet<Self::Item>
    where
        Self: Sized,
        Self::Item: Clone + Eq + Hash,
    {
        if amount < 1 {
            return HashSet::with_capacity_and_hasher(0, Default::default());
        }
        let mut rand = StdRand::default();
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
        HashSet::from_iter(reservoir)
    }
}

impl<T: ?Sized> SamplingIterator for T where T: Iterator {}

pub trait OptionalIterator: Iterator {
    fn none_by_index(self, indices: HashSet<usize>) -> NoneByIndex<Self>
    where
        Self: Sized,
    {
        none_by_index(self, indices)
    }

    fn none_by_value(self, values: HashSet<Self::Item>) -> NoneByValue<Self>
    where
        Self: Sized,
    {
        none_by_value(self, values)
    }
}

impl<T: ?Sized> OptionalIterator for T where T: Iterator {}
