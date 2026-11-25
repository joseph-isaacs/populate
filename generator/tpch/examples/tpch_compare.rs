//! Compare all TPC-H tables and formats in memory on one thread.
//! See the crate README for the measurement contract and invocation.
use arrow_array::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use populate_tpch::{TableName, Tpch};
use std::hint::black_box;
use std::io::{self, Write};
use std::time::Instant;
use tpchgen::csv::*;
use tpchgen::generators::*;
use tpchgen::text::TextPool;
use tpchgen_arrow::*;

const BATCH: usize = 8192;
const ROW_GROUP: usize = 262_144;

#[derive(Default)]
struct Output(u64);
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 += black_box(bytes).len() as u64;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn props(format: &str) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(if format == "parquet_snappy" {
            Compression::SNAPPY
        } else {
            Compression::UNCOMPRESSED
        })
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(ROW_GROUP))
        .build()
}

fn reference_arrow(t: TableName, sf: f64) -> Box<dyn Iterator<Item = RecordBatch>> {
    macro_rules! batches {
        ($gen:ident, $arrow:ident) => {
            Box::new(
                $arrow::new($gen::new(sf, 1, 1))
                    .with_batch_size(BATCH)
                    .map(Result::unwrap),
            )
        };
    }
    match t {
        TableName::Nation => batches!(NationGenerator, NationArrow),
        TableName::Region => batches!(RegionGenerator, RegionArrow),
        TableName::Part => batches!(PartGenerator, PartArrow),
        TableName::Supplier => batches!(SupplierGenerator, SupplierArrow),
        TableName::PartSupp => batches!(PartSuppGenerator, PartSuppArrow),
        TableName::Customer => batches!(CustomerGenerator, CustomerArrow),
        TableName::Orders => batches!(OrderGenerator, OrderArrow),
        TableName::LineItem => batches!(LineItemGenerator, LineItemArrow),
    }
}

fn reference_text(t: TableName, sf: f64, csv: bool) -> u64 {
    let mut output = Output::default();
    let mut buffer = Vec::with_capacity(1024 * 1024);
    macro_rules! rows {
        ($gen:ident, $csv:ident) => {{
            if csv {
                writeln!(buffer, "{}", $csv::header_with_delimiter(',')).unwrap();
            }
            for row in $gen::new(sf, 1, 1).iter() {
                if csv {
                    writeln!(buffer, "{}", $csv::with_delimiter(row, ',')).unwrap();
                } else {
                    writeln!(buffer, "{row}").unwrap();
                }
                if buffer.len() >= 1024 * 1024 {
                    output.write_all(&buffer).unwrap();
                    buffer.clear();
                }
            }
        }};
    }
    match t {
        TableName::Nation => rows!(NationGenerator, NationCsv),
        TableName::Region => rows!(RegionGenerator, RegionCsv),
        TableName::Part => rows!(PartGenerator, PartCsv),
        TableName::Supplier => rows!(SupplierGenerator, SupplierCsv),
        TableName::PartSupp => rows!(PartSuppGenerator, PartSuppCsv),
        TableName::Customer => rows!(CustomerGenerator, CustomerCsv),
        TableName::Orders => rows!(OrderGenerator, OrderCsv),
        TableName::LineItem => rows!(LineItemGenerator, LineItemCsv),
    }
    output.write_all(&buffer).unwrap();
    output.0
}

fn run(tool: &str, format: &str, name: TableName, sf: f64) -> u64 {
    if tool == "populate" {
        let table = Tpch::new(sf)
            .table(name)
            .batch_size(BATCH)
            .chunk_rows(ROW_GROUP as u64);
        match format {
            "arrow" => {
                let mut rows = 0;
                table.for_each_batch(1, |b| {
                    rows += b.num_rows() as u64;
                    drop(black_box(b));
                });
                rows
            }
            "tbl" => table.write_tbl(Output::default(), 1).unwrap().0,
            _ => {
                table
                    .write_parquet(Output::default(), 1, Some(props(format)))
                    .unwrap()
                    .0
            }
        }
    } else {
        match format {
            "tbl" | "csv" => reference_text(name, sf, format == "csv"),
            "arrow" => reference_arrow(name, sf)
                .map(|b| {
                    let n = b.num_rows();
                    drop(black_box(b));
                    n as u64
                })
                .sum(),
            _ => {
                let mut out = Output::default();
                let mut writer =
                    ArrowWriter::try_new(&mut out, name.schema(), Some(props(format))).unwrap();
                for batch in reference_arrow(name, sf) {
                    writer.write(&batch).unwrap();
                }
                writer.close().unwrap();
                out.0
            }
        }
    }
}

fn main() {
    let args: Vec<_> = std::env::args().collect();
    let sf: f64 = args.get(1).map_or(1.0, |s| s.parse().unwrap());
    let repeats: usize = args.get(2).map_or(3, |s| s.parse().unwrap());
    let only = args.get(3).map_or("both", String::as_str);
    assert!(["both", "populate", "tpchgen"].contains(&only));
    assert!(sf.is_finite() && sf >= 0.01 && repeats > 0);
    // Both pools are resident before any measurements, including warm-up.
    Tpch::prepare(1);
    black_box(TextPool::get_or_init_default());
    println!("run,tool,format,table,seconds,rows_or_bytes");
    for repeat in 0..=repeats {
        let order = if repeat % 2 == 0 {
            ["populate", "tpchgen"]
        } else {
            ["tpchgen", "populate"]
        };
        for format in ["arrow", "tbl", "parquet_none", "parquet_snappy", "csv"] {
            for tool in order {
                if (only != "both" && only != tool) || (tool == "populate" && format == "csv") {
                    continue;
                }
                for name in TableName::ALL {
                    let start = Instant::now();
                    let count = black_box(run(tool, format, name, sf));
                    let elapsed = start.elapsed().as_secs_f64();
                    if repeat > 0 {
                        println!("{repeat},{tool},{format},{name},{elapsed:.9},{count}");
                    }
                }
            }
        }
    }
}
