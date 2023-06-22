use crate::debug_fmt_fields;
use crate::iter::types::KeySet;
use std::fmt;
use std::hash::Hash;
use std::iter::FusedIterator;

#[derive(Clone)]
pub struct NoneByValue<I: Iterator> {
    iter: I,
    values: KeySet<I::Item>,
}

impl<I> fmt::Debug for NoneByValue<I>
where
    I: Iterator + fmt::Debug,
    I::Item: fmt::Debug + Hash + Eq,
{
    debug_fmt_fields!(NoneByValue, iter, values);
}

pub fn none_by_value<I>(iter: I, values: KeySet<I::Item>) -> NoneByValue<I>
where
    I: Iterator,
{
    NoneByValue {
        iter,
        values: values,
    }
}

impl<I> Iterator for NoneByValue<I>
where
    I: Iterator,
    I::Item: Hash + Eq,
{
    type Item = Option<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.next() {
            if self.values.contains(&v) {
                return Some(None);
            } else {
                return Some(Some(v));
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn count(self) -> usize {
        self.iter.count()
    }
}

impl<I> DoubleEndedIterator for NoneByValue<I>
where
    I: DoubleEndedIterator,
    I::Item: Hash + Eq,
{
    fn next_back(&mut self) -> Option<Option<I::Item>> {
        while let Some(v) = self.iter.next_back() {
            if self.values.contains(&v) {
                return Some(None);
            } else {
                return Some(Some(v));
            }
        }
        None
    }
}

impl<I> FusedIterator for NoneByValue<I>
where
    I: FusedIterator,
    I::Item: Hash + Eq,
{
}

#[derive(Clone)]
pub struct NoneByIndex<I: Iterator> {
    iter: I,
    head: usize,
    indices: KeySet<usize>,
}

impl<I> fmt::Debug for NoneByIndex<I>
where
    I: Iterator + fmt::Debug,
    I::Item: fmt::Debug + Hash + Eq,
{
    debug_fmt_fields!(NoneByIndex, iter, head, indices);
}

pub fn none_by_index<I>(iter: I, indices: KeySet<usize>) -> NoneByIndex<I>
where
    I: Iterator,
{
    NoneByIndex {
        iter,
        head: 0,
        indices: indices,
    }
}

impl<I> Iterator for NoneByIndex<I>
where
    I: Iterator,
    I::Item: Hash + Eq,
{
    type Item = Option<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(v) = self.iter.next() {
            if self.indices.contains(&self.head) {
                self.head += 1;
                return Some(None);
            } else {
                self.head += 1;
                return Some(Some(v));
            }
        }
        None
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    fn count(self) -> usize {
        self.iter.count()
    }
}

impl<I> FusedIterator for NoneByIndex<I>
where
    I: FusedIterator,
    I::Item: Hash + Eq,
{
}
