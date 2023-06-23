use csv;
use std::cmp;
use std::fs;
use std::io;
use std::iter::FusedIterator;
use tempfile::tempfile;

pub struct Zip<I>(Vec<I>);

impl<I> Iterator for Zip<I>
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

impl<I> DoubleEndedIterator for Zip<I>
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

impl<I> FusedIterator for Zip<I> where I: FusedIterator {}

pub fn zip<R: io::Read>(readers: Vec<&mut csv::Reader<R>>) -> Zip<csv::ByteRecordsIter<'_, R>> {
    Zip(readers.into_iter().map(csv::Reader::byte_records).collect())
}

pub fn hstack<R: io::Read, W: io::Write>(
    writer: &mut csv::Writer<W>,
    readers: Vec<&mut csv::Reader<R>>,
) -> csv::Result<()> {
    let zipped_byte_records = Zip(readers.into_iter().map(csv::Reader::byte_records).collect());
    for rows in zipped_byte_records {
        let row = rows
            .iter()
            .filter_map(|f| f.as_ref().ok())
            .map(csv::ByteRecord::iter)
            .flatten();
        writer.write_record(row)?;
    }

    //    let mut zipped_byte_records: Vec<_> =
    //        MultipleZip(readers.into_iter().map(csv::Reader::byte_records).collect()).collect();
    //    zipped_byte_records.sort_by(|lhs, rhs| {
    //        let lhs = lhs
    //            .iter()
    //            .filter_map(|f| f.as_ref().ok())
    //            .map(csv::ByteRecord::iter)
    //            .flatten();
    //        let rhs = rhs
    //            .iter()
    //            .filter_map(|f| f.as_ref().ok())
    //            .map(csv::ByteRecord::iter)
    //            .flatten();
    //        lex_ordering(lhs, rhs)
    //    });
    //    for rows in zipped_byte_records {
    //        let row = rows
    //            .iter()
    //            .filter_map(|f| f.as_ref().ok())
    //            .map(csv::ByteRecord::iter)
    //            .flatten();
    //        writer.write_record(row)?;
    //    }

    Ok(())
}

fn lex_ordering<V, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    V: Ord,
    L: Iterator<Item = V>,
    R: Iterator<Item = V>,
{
    loop {
        match (a.next(), b.next()) {
            (None, None) => return cmp::Ordering::Equal,
            (None, _) => return cmp::Ordering::Less,
            (_, None) => return cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(&y) {
                cmp::Ordering::Equal => (),
                non_eq => return non_eq,
            },
        }
    }
}

pub fn sorted_chunk() -> Result<fs::File, io::Error> {
    tempfile()
}
