use csv;
use std::fs;
use std::io::{self, Read};
use std::ops::Deref;
use std::path::PathBuf;

#[derive(Debug)]
pub struct Config {
    path: Option<PathBuf>,
    delimiter: u8,
    pub no_headers: bool,
    flexible: bool,
    terminator: csv::Terminator,
    quote: u8,
    quote_style: csv::QuoteStyle,
    double_quote: bool,
    escape: Option<u8>,
    quoting: bool,
}

impl Config {
    pub fn new(path: &Option<String>) -> Config {
        let (path, delimiter) = match *path {
            None => (None, b','),
            Some(ref s) if s.deref() == "-" => (None, b','),
            Some(ref s) => {
                let path = PathBuf::from(s);
                let delimiter = if path.extension().map_or(false, |v| v == "tsv" || v == "tab") {
                    b'\t'
                } else {
                    b','
                };
                (Some(path), delimiter)
            }
        };
        Config {
            path: path,
            delimiter: delimiter,
            no_headers: true,
            flexible: false,
            terminator: csv::Terminator::Any(b'\n'),
            quote: b'"',
            quote_style: csv::QuoteStyle::Never,
            double_quote: true,
            escape: None,
            quoting: false,
        }
    }

    pub fn delimiter(mut self, delimiter: Option<u8>) -> Config {
        if let Some(delimiter) = delimiter {
            self.delimiter = delimiter;
        }
        self
    }

    pub fn no_headers(mut self, yes: bool) -> Config {
        self.no_headers = yes;
        self
    }

    pub fn flexible(mut self, yes: bool) -> Config {
        self.flexible = yes;
        self
    }

    pub fn crlf(mut self, yes: bool) -> Config {
        if yes {
            self.terminator = csv::Terminator::CRLF;
        } else {
            self.terminator = csv::Terminator::Any(b'\n');
        }
        self
    }

    pub fn terminator(mut self, terminator: csv::Terminator) -> Config {
        self.terminator = terminator;
        self
    }

    pub fn quote(mut self, quote: u8) -> Config {
        self.quote = quote;
        self
    }

    pub fn quote_style(mut self, quote_style: csv::QuoteStyle) -> Config {
        self.quote_style = quote_style;
        self
    }

    pub fn double_quote(mut self, yes: bool) -> Config {
        self.double_quote = yes;
        self
    }

    pub fn escape(mut self, escape: Option<u8>) -> Config {
        self.escape = escape;
        self
    }

    pub fn quoting(mut self, yes: bool) -> Config {
        self.quoting = yes;
        self
    }

    pub fn is_stdout(&self) -> bool {
        self.path.is_none()
    }

    pub fn write_headers<R: io::Read, W: io::Write>(
        &self,
        r: &mut csv::Reader<R>,
        w: &mut csv::Writer<W>,
    ) -> csv::Result<()> {
        if !self.no_headers {
            let r = r.byte_headers()?;
            if !r.is_empty() {
                w.write_record(r)?;
            }
        }
        Ok(())
    }

    pub fn reader(&self) -> io::Result<csv::Reader<Box<dyn io::Read + 'static>>> {
        Ok(self.from_reader(self.io_reader()?))
    }

    pub fn writer(&self) -> io::Result<csv::Writer<Box<dyn io::Write + 'static>>> {
        Ok(self.from_writer(self.io_writer()?))
    }

    pub fn io_reader(&self) -> io::Result<Box<dyn io::Read + 'static>> {
        Ok(match self.path {
            None => Box::new(io::stdin()),
            Some(ref p) => match fs::File::open(p) {
                Ok(x) => Box::new(x),
                Err(err) => {
                    let msg = format!("failed to open {}: {}", p.display(), err);
                    return Err(io::Error::new(io::ErrorKind::NotFound, msg));
                }
            },
        })
    }

    pub fn io_writer(&self) -> io::Result<Box<dyn io::Write + 'static>> {
        Ok(match self.path {
            None => Box::new(io::stdout()),
            Some(ref p) => Box::new(fs::File::create(p)?),
        })
    }

    pub fn from_reader<R: Read>(&self, rdr: R) -> csv::Reader<R> {
        csv::ReaderBuilder::new()
            .flexible(self.flexible)
            .delimiter(self.delimiter)
            .has_headers(!self.no_headers)
            .quote(self.quote)
            .quoting(self.quoting)
            .escape(self.escape)
            .from_reader(rdr)
    }

    pub fn from_writer<W: io::Write>(&self, wtr: W) -> csv::Writer<W> {
        csv::WriterBuilder::new()
            .flexible(self.flexible)
            .delimiter(self.delimiter)
            .terminator(self.terminator)
            .quote(self.quote)
            .quote_style(self.quote_style)
            .double_quote(self.double_quote)
            .escape(self.escape.unwrap_or(b'\\'))
            .buffer_capacity(32 * (1 << 10))
            .from_writer(wtr)
    }

    pub fn reader_file(&self) -> io::Result<csv::Reader<fs::File>> {
        match self.path {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "cannot use <stdin> here",
            )),
            Some(ref p) => fs::File::open(p).map(|f| self.from_reader(f)),
        }
    }
}
