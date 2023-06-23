pub mod extensions;
mod multiple_zip;
mod none_by;
mod unique_by;

pub type KeySet<V> = hashbrown::HashSet<V, ahash::RandomState>;

type KeyEntry<'a, K, V, S> = hashbrown::hash_set::Entry<'a, K, V, S>;
