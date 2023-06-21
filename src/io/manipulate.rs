use csv;
use std::io;

struct MultipleZip<T>(Vec<T>);

impl<T> Iterator for MultipleZip<T>
where
    T: Iterator,
    T::Item: std::fmt::Debug,
{
    type Item = Vec<T::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.iter_mut().map(Iterator::next).collect()
    }
}

pub fn zip<R: io::Read, W: io::Write>(
    writer: &mut csv::Writer<W>,
    readers: Vec<&mut csv::Reader<R>>,
) -> csv::Result<()> {
    let zipped_byte_records =
        MultipleZip(readers.into_iter().map(csv::Reader::byte_records).collect());
    for rows in zipped_byte_records {
        let rows: Vec<_> = rows.into_iter().filter_map(|f| f.ok()).collect();
        let row = rows
            .iter()
            .map(csv::ByteRecord::iter)
            .flat_map(|it| it.clone());
        writer.write_record(row)?;
    }
    Ok(())
}
