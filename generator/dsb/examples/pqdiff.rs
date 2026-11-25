use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
fn main() {
    let a: Vec<String> = std::env::args().skip(1).collect();
    let ra = SerializedFileReader::new(File::open(&a[0]).unwrap()).unwrap();
    let rb = SerializedFileReader::new(File::open(&a[1]).unwrap()).unwrap();
    let (ma, mb) = (ra.metadata(), rb.metadata());
    println!("rgs {} {}", ma.num_row_groups(), mb.num_row_groups());
    let mut shown = 0;
    for g in 0..ma.num_row_groups().min(mb.num_row_groups()) {
        let (ga, gb) = (ma.row_group(g), mb.row_group(g));
        if ga.num_rows() != gb.num_rows() { println!("rg {g} rows {} {}", ga.num_rows(), gb.num_rows()); }
        for c in 0..ga.num_columns() {
            let (ca, cb) = (ga.column(c), gb.column(c));
            if ca.compressed_size() != cb.compressed_size() && shown < 15 {
                shown += 1;
                println!("rg {g} col {} size {} {} enc {:?} {:?} stats {:?} | {:?}", ca.column_path(), ca.compressed_size(), cb.compressed_size(), ca.encodings().collect::<Vec<_>>(), cb.encodings().collect::<Vec<_>>(), ca.statistics(), cb.statistics());
            }
        }
    }
}
