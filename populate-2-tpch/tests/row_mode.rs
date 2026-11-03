//! Row mode (`row_iter`, each column's `value`) must produce the same values
//! as columnar collection (`write`), which the parity tests check against
//! tpchgen. Every value of every table at SF 1. Also checks `copy_text`.

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, Int64Type};
use populate_2::traits::{Distribution, SampleIter};
use populate_2_tpch::tables::{NoRng, customer, nation, part, partsupp, region, supplier};
use populate_2_tpch::values::{Dec, Str, copy_text};
use populate_2_tpch::{Customer, Nation, Part, PartSupp, Region, Supplier, Table, batches_copied};

const SF: f64 = 1.0;

fn s(b: &RecordBatch, c: usize, i: usize) -> &str {
    b.column(c).as_string_view().value(i)
}
fn i64_(b: &RecordBatch, c: usize, i: usize) -> i64 {
    b.column(c).as_primitive::<Int64Type>().value(i)
}
fn i32_(b: &RecordBatch, c: usize, i: usize) -> i32 {
    b.column(c).as_primitive::<Int32Type>().value(i)
}
fn dec(b: &RecordBatch, c: usize, i: usize) -> i64 {
    b.column(c).as_primitive::<Decimal128Type>().value(i) as i64
}

/// Run `check(row_item, batch, i)` over every row: row mode vs one columnar batch.
fn each_row<T: Table, D: Distribution<populate_2::traits::VecBackend>>(
    mut dist: D,
    check: impl Fn(D::Item, &RecordBatch, usize),
) -> usize {
    let n = T::row_count(SF) as usize;
    let batch = T::batch(SF, 0, n);
    let mut rows = dist.row_iter();
    for i in 0..n {
        check(rows.next(&mut NoRng), &batch, i);
    }
    n
}

#[test]
fn region_and_nation() {
    each_row::<Region, _>(region(0), |(k, name, comment): (i64, Str, Str), b, i| {
        assert_eq!(
            (k, &*name.0, &*comment.0),
            (i64_(b, 0, i), s(b, 1, i), s(b, 2, i))
        );
    });
    each_row::<Nation, _>(
        nation(0),
        |(k, name, r, comment): (i64, Str, i64, Str), b, i| {
            assert_eq!(
                (k, &*name.0, r, &*comment.0),
                (i64_(b, 0, i), s(b, 1, i), i64_(b, 2, i), s(b, 3, i))
            );
        },
    );
}

#[test]
fn part_rows() {
    each_row::<Part, _>(
        part(0),
        |((k, name, mfgr, brand), (ty, size, cont, Dec(price)), comment), b, i| {
            assert_eq!(k, i64_(b, 0, i));
            assert_eq!(&*name.0, s(b, 1, i));
            assert_eq!(&*mfgr.0, s(b, 2, i));
            assert_eq!(&*brand.0, s(b, 3, i));
            assert_eq!(&*ty.0, s(b, 4, i));
            assert_eq!(size, i32_(b, 5, i));
            assert_eq!(&*cont.0, s(b, 6, i));
            assert_eq!(price, dec(b, 7, i));
            assert_eq!(&*comment.0, s(b, 8, i));
        },
    );
}

#[test]
fn supplier_rows_including_bbb_splices() {
    let spliced = std::cell::Cell::new(0);
    let n = {
        each_row::<Supplier, _>(
            supplier(0),
            |((k, name, addr, nk), (phone, Dec(bal), comment)), b, i| {
                assert_eq!(k, i64_(b, 0, i));
                assert_eq!(&*name.0, s(b, 1, i));
                assert_eq!(&*addr.0, s(b, 2, i));
                assert_eq!(nk, i64_(b, 3, i));
                assert_eq!(&*phone.0, s(b, 4, i));
                assert_eq!(bal, dec(b, 5, i));
                assert_eq!(&*comment.0, s(b, 6, i));
                if comment.0.contains("Customer")
                    && (comment.0.contains("Complaints") || comment.0.contains("Recommends"))
                {
                    spliced.set(spliced.get() + 1);
                }
            },
        )
    };
    assert_eq!(n, 10_000);
    assert!(spliced.get() > 0, "BBB branch exercised");
}

#[test]
fn customer_rows() {
    each_row::<Customer, _>(
        customer(0),
        |((k, name, addr, nk), (phone, Dec(bal), seg, comment)), b, i| {
            assert_eq!(k, i64_(b, 0, i));
            assert_eq!(&*name.0, s(b, 1, i));
            assert_eq!(&*addr.0, s(b, 2, i));
            assert_eq!(nk, i64_(b, 3, i));
            assert_eq!(&*phone.0, s(b, 4, i));
            assert_eq!(bal, dec(b, 5, i));
            assert_eq!(&*seg.0, s(b, 6, i));
            assert_eq!(&*comment.0, s(b, 7, i));
        },
    );
}

#[test]
fn partsupp_rows() {
    each_row::<PartSupp, _>(
        partsupp(SF, 0),
        |((pk, sk, qty, Dec(cost)), comment), b, i| {
            assert_eq!(pk, i64_(b, 0, i));
            assert_eq!(sk, i64_(b, 1, i));
            assert_eq!(qty, i32_(b, 2, i));
            assert_eq!(cost, dec(b, 3, i));
            assert_eq!(&*comment.0, s(b, 4, i));
        },
    );
}

#[test]
fn copy_text_keeps_values_and_detaches_from_pool() {
    let shared = PartSupp::batch(SF, 0, 8000);
    let copied = copy_text(&shared);
    assert_eq!(shared, copied);
    // the shared pool (~300 MB) is charged to `shared`, not to `copied`
    assert!(shared.get_array_memory_size() > 100_000_000);
    assert!(copied.get_array_memory_size() < 10_000_000);
    // batches_copied: same values as batches, every batch detached
    for (a, b) in
        populate_2_tpch::batches::<Supplier>(SF, 4000).zip(batches_copied::<Supplier>(SF, 4000))
    {
        assert_eq!(a, b);
        assert!(b.get_array_memory_size() < 10_000_000);
    }
}
