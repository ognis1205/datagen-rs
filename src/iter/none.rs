use crate::debug_fmt_fields;
use crate::iter::types::HashSet;
use std::fmt;
use std::hash::Hash;
use std::iter::FusedIterator;

#[derive(Clone)]
pub struct NoneByValue<I: Iterator> {
    iter: I,
    values: HashSet<I::Item>,
}

impl<I> fmt::Debug for NoneByValue<I>
where
    I: Iterator + fmt::Debug,
    I::Item: fmt::Debug + Hash + Eq,
{
    debug_fmt_fields!(NoneByValue, iter, values);
}

pub fn none_by_value<I>(iter: I, values: HashSet<I::Item>) -> NoneByValue<I>
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
