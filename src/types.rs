pub type HashSet<V> = hashbrown::HashSet<V, ahash::RandomState>;

pub type Entry<'a, K, V, S> = hashbrown::hash_set::Entry<'a, K, V, S>;
