use std::iter::FusedIterator;

#[derive(Clone)]
pub struct MultipleZip<I>(Vec<I>);

impl<I> Iterator for MultipleZip<I>
where
    I: Iterator,
{
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter_mut().map(Iterator::next).collect()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.0.iter().map(|it| it.size_hint().0).min().unwrap_or(0),
            self.0.iter().flat_map(|it| it.size_hint().1).min(),
        )
    }

    fn count(self) -> usize {
        self.0.into_iter().map(Iterator::count).min().unwrap_or(0)
    }
}

impl<I> DoubleEndedIterator for MultipleZip<I>
where
    I: DoubleEndedIterator,
{
    fn next_back(&mut self) -> Option<Vec<I::Item>> {
        self.0
            .iter_mut()
            .map(DoubleEndedIterator::next_back)
            .collect()
    }
}

impl<I> FusedIterator for MultipleZip<I> where I: FusedIterator {}
