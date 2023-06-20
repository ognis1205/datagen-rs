//use rand::thread_rng;
//use rand::Rng;
use std::hash::Hash;

pub type HashSet<V> = hashbrown::HashSet<V, ahash::RandomState>;

pub trait SampleHashMap<V>
where
    V: Hash + Eq + Copy,
{
    type Type;

    fn sample(iter: impl Iterator<Item = V>, capacity: usize, sample_size: usize) -> Self::Type;
}

impl<V> SampleHashMap<V> for HashSet<V>
where
    V: Hash + Eq + Copy,
{
    type Type = Self;

    fn sample(iter: impl Iterator<Item = V>, capacity: usize, sample_size: usize) -> Self::Type {
        let mut vs = arg_unique(iter, capacity);
        sample(vs, sample_size)
    }
}

fn arg_unique<V>(iter: impl Iterator<Item = V>, capacity: usize) -> Vec<V>
where
    V: Hash + Eq + Copy,
{
    let mut us = HashSet::with_capacity_and_hasher(capacity, Default::default());
    iter.for_each(|t| {
        us.insert(t);
    });
    us.into_iter().collect()
}

fn sample<V>(mut vs: Vec<V>, sample_size: usize) -> HashSet<V>
where
    V: Hash + Eq + Copy,
{
    let mut ss = HashSet::with_capacity_and_hasher(sample_size, Default::default());
    for _ in 0..sample_size {
        if vs.is_empty() {
            break;
        }
	//        let i = thread_rng().gen_range(0..vs.len());
	let i = fastrand::usize(0..vs.len());
        let v = vs.swap_remove(i);
        ss.insert(v);
    }
    ss
}
