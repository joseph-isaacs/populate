//! Values produced by R 4.3.3 / data.table 1.14.10 (see validate/validate.sh
//! for the full byte-for-byte comparison against the reference scripts). The
//! R dialect's primitives (`sample.int`, `runif`, `round`, `split_xlr`,
//! `sample_all`) are checked against R in populate-2's own tests.

use crate::{GroupByConfig, GroupByTable};

fn csv_head(table: GroupByTable, lines: usize) -> String {
    let mut out = Vec::new();
    table.into_table().write_csv(&mut out).unwrap();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .take(lines)
        .collect::<Vec<_>>()
        .join("\n")
}

/// head of `Rscript groupby-datagen.R 1e4 1e2 0 0`
#[test]
fn groupby_1e4_1e2_0_0_head() {
    let t = GroupByTable::reference(GroupByConfig::new(10_000, 100, 0, false));
    assert_eq!(
        csv_head(t, 4),
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n\
         id016,id056,id0000000042,82,64,50,4,3,26.407777\n\
         id039,id075,id0000000067,30,28,17,1,6,7.913725\n\
         id047,id077,id0000000027,40,67,23,5,14,54.691464"
    );
}

/// head of `Rscript groupby-datagen.R 1e4 1e1 5 1` (NAs and sorted)
#[test]
fn groupby_1e4_1e1_5_1_head() {
    let t = GroupByTable::reference(GroupByConfig::new(10_000, 10, 5, true));
    assert_eq!(
        csv_head(t, 6),
        "id1,id2,id3,id4,id5,id6,v1,v2,v3\n\
         id001,id001,,4,6,204,4,9,97.212754\n\
         id001,id001,,7,1,276,3,7,1.078204\n\
         id001,id001,,7,9,53,3,10,\n\
         id001,id001,,7,9,721,5,1,37.63933\n\
         id001,id001,,8,5,970,3,15,14.464437"
    );
}
