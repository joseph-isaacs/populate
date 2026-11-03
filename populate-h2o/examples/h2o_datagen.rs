//! Write db-benchmark CSVs exactly like the R reference scripts.
//!
//! ```text
//! cargo run --release -p populate-h2o --example h2o_datagen -- groupby N K nas sort [DIR|-]
//! cargo run --release -p populate-h2o --example h2o_datagen -- join    N 0 nas sort [DIR|-]
//! ```
//!
//! Files are named as the reference names them (`G1_1e7_1e2_0_0.csv`, ...).
//! With `-` instead of a directory, all tables go to stdout back to back
//! (handy for hashing without touching disk).

use populate_h2o::csv::Table;
use populate_h2o::{GroupByConfig, GroupByTable, JoinConfig, JoinTables};
use std::env::args;
use std::fs::File;
use std::io::{BufWriter, Write, stdout};
use std::path::Path;

fn parse(s: &str) -> usize {
    s.parse::<f64>().expect("number") as usize
}

fn write(dir: &str, name: &str, table: &Table) {
    if dir == "-" {
        let mut w = BufWriter::new(stdout().lock());
        table.write_csv(&mut w).expect("write");
        w.flush().expect("flush");
    } else {
        let path = Path::new(dir).join(format!("{name}.csv"));
        let mut w = BufWriter::new(File::create(&path).expect("create"));
        table.write_csv(&mut w).expect("write");
        w.flush().expect("flush");
        eprintln!("wrote {}", path.display());
    }
}

fn main() {
    let a: Vec<String> = args().skip(1).collect();
    let usage = "usage: h2o_datagen (groupby N K nas sort | join N 0 nas sort) [DIR|-]";
    assert!(a.len() >= 5, "{usage}");
    let (n, k, nas, sort) = (
        parse(&a[1]),
        parse(&a[2]),
        parse(&a[3]) as u32,
        parse(&a[4]) == 1,
    );
    let dir = a.get(5).map_or(".", String::as_str);
    match a[0].as_str() {
        "groupby" => {
            let cfg = GroupByConfig::new(n, k, nas, sort);
            write(dir, &cfg.name(), &GroupByTable::reference(cfg).into_table());
        }
        "join" => {
            let cfg = JoinConfig::new(n, nas, sort);
            let names = cfg.names();
            for (name, table) in names.iter().zip(JoinTables::reference(cfg).into_tables()) {
                write(dir, name, &table);
            }
        }
        _ => panic!("{usage}"),
    }
}
