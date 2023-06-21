use crate::config::Config;
use std::cmp;
use std::ops;

#[derive(Clone, Debug)]
struct Selector(Vec<usize>);

type _GetField = for<'c> fn(&mut &'c csv::ByteRecord, &usize) -> Option<&'c [u8]>;

impl Selector {
    pub fn select<'a, 'b>(
        &'a self,
        row: &'b csv::ByteRecord,
    ) -> iter::Scan<slice::Iter<'a, usize>, &'b csv::ByteRecord, _GetField> {
        fn get_field<'c>(row: &mut &'c csv::ByteRecord, idx: &usize) -> Option<&'c [u8]> {
            Some(&row[*idx])
        }
        let get_field: _GetField = get_field;
        self.iter().scan(row, get_field)
    }
}

impl ops::Deref for Selector {
    type Target = [usize];

    fn deref(&self) -> &[usize] {
        &self.0
    }
}

fn cmp_lex<A, L, R>(mut a: L, mut b: R) -> cmp::Ordering
where
    A: Ord,
    L: Iterator<Item = A>,
    R: Iterator<Item = A>,
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

pub fn run(argv: &[&str]) -> CliResult<()> {
    let args: Args = util::get_args(USAGE, argv)?;
    let numeric = args.flag_numeric;
    let reverse = args.flag_reverse;
    let rconfig = Config::new(&args.arg_input)
        .delimiter(args.flag_delimiter)
        .no_headers(args.flag_no_headers)
        .select(args.flag_select);

    let mut rdr = rconfig.reader()?;
    let headers = rdr.byte_headers()?.clone();
    let sel = rconfig.selection(&headers)?;

    let mut all = rdr.byte_records().collect::<Result<Vec<_>, _>>()?;
    match (numeric, reverse) {
        (false, false) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp(a, b)
        }),
        (true, false) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(a, b)
        }),
        (false, true) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp(b, a)
        }),
        (true, true) => all.sort_by(|r1, r2| {
            let a = sel.select(r1);
            let b = sel.select(r2);
            iter_cmp_num(b, a)
        }),
    }

    let mut wtr = Config::new(&args.flag_output).writer()?;
    rconfig.write_headers(&mut rdr, &mut wtr)?;
    for r in all.into_iter() {
        wtr.write_byte_record(&r)?;
    }
    Ok(wtr.flush()?)
}
