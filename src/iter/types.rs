pub type KeySet<V> = hashbrown::HashSet<V, ahash::RandomState>;

pub type KeyEntry<'a, K, V, S> = hashbrown::hash_set::Entry<'a, K, V, S>;
