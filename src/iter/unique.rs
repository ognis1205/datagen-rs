use crate::debug_fmt_fields;
use crate::iter::types::{KeyEntry, KeySet};
use std::fmt;
use std::hash::Hash;
use std::iter::FusedIterator;

#[derive(Clone)]
pub struct UniqueBy<I: Iterator, V, F> {
    iter: I,
    used: KeySet<V>,
    f: F,
}

impl<I, V, F> fmt::Debug for UniqueBy<I, V, F>
where
    I: Iterator + fmt::Debug,
    V: fmt::Debug + Hash + Eq,
{
    debug_fmt_fields!(UniqueBy, iter, used);
}

pub fn unique_by<I, V, F>(iter: I, f: F) -> UniqueBy<I, V, F>
where
    V: Eq + Hash,
    F: FnMut(&I::Item) -> V,
    I: Iterator,
{
    UniqueBy {
        iter,
        used: KeySet::with_capacity_and_hasher(0, Default::default()),
        f,
    }
}

fn count_new_keys<I, V>(mut used: KeySet<V>, iterable: I) -> usize
where
    I: IntoIterator<Item = V>,
    V: Hash + Eq,
{
    let iter = iterable.into_iter();
    let current_used = used.len();
    used.extend(iter.map(|v| v));
    used.len() - current_used
}

impl<I, V, F> Iterator for UniqueBy<I, V, F>
where
    I: Iterator,
    V: Eq + Hash,
    F: FnMut(&I::Item) -> V,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.next() {
            let k = (self.f)(&v);
            if self.used.insert(k) {
                return Some(v);
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (l, h) = self.iter.size_hint();
        ((l > 0 && self.used.is_empty()) as usize, h)
    }

    fn count(self) -> usize {
        let mut key_f = self.f;
        count_new_keys(self.used, self.iter.map(move |e| key_f(&e)))
    }
}

impl<I, V, F> DoubleEndedIterator for UniqueBy<I, V, F>
where
    I: DoubleEndedIterator,
    V: Eq + Hash,
    F: FnMut(&I::Item) -> V,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.next_back() {
            let k = (self.f)(&v);
            if self.used.insert(k) {
                return Some(v);
            }
        }
        None
    }
}

impl<I, V, F> FusedIterator for UniqueBy<I, V, F>
where
    I: FusedIterator,
    V: Eq + Hash,
    F: FnMut(&I::Item) -> V,
{
}

#[derive(Clone)]
pub struct Unique<I: Iterator> {
    iter: UniqueBy<I, I::Item, ()>,
}

impl<I> fmt::Debug for Unique<I>
where
    I: Iterator + fmt::Debug,
    I::Item: Hash + Eq + fmt::Debug,
{
    debug_fmt_fields!(Unique, iter);
}

pub fn unique<I>(iter: I) -> Unique<I>
where
    I: Iterator,
    I::Item: Eq + Hash,
{
    Unique {
        iter: UniqueBy {
            iter,
            used: KeySet::with_capacity_and_hasher(0, Default::default()),
            f: (),
        },
    }
}

impl<I> Iterator for Unique<I>
where
    I: Iterator,
    I::Item: Eq + Hash + Clone,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.iter.next() {
            if let KeyEntry::Vacant(e) = self.iter.used.entry(v) {
                let k = e.get().clone();
                e.insert();
                return Some(k);
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (l, h) = self.iter.iter.size_hint();
        ((l > 0 && self.iter.used.is_empty()) as usize, h)
    }

    fn count(self) -> usize {
        count_new_keys(self.iter.used, self.iter.iter)
    }
}

impl<I> DoubleEndedIterator for Unique<I>
where
    I: DoubleEndedIterator,
    I::Item: Eq + Hash + Clone,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.iter.next_back() {
            if let KeyEntry::Vacant(e) = self.iter.used.entry(v) {
                let k = e.get().clone();
                e.insert();
                return Some(k);
            }
        }
        None
    }
}

impl<I> FusedIterator for Unique<I>
where
    I: FusedIterator,
    I::Item: Eq + Hash + Clone,
{
}
