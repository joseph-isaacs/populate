//! TPC-C against the spec (TPC-C 5.11, clause 4.3.3.1): row counts, keys,
//! value ranges and distributions, and chunked output equal to one
//! sequential run. There are no reference bytes to compare with: go-tpc,
//! BenchBase and tpcc-mysql all seed from the clock.

use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Int32Type, TimestampMicrosecondType};
use arrow_array::{Array, RecordBatch};
use arrow_select::concat::concat_batches;
use populate_gen_tpcc::tpcc::{DEFAULT_LOAD_TIME, SYLLABLES};
use populate_gen_tpcc::{Tpcc, TpccTableName};
use std::collections::{HashMap, HashSet};

use TpccTableName::*;

fn table(db: &Tpcc, t: TpccTableName) -> RecordBatch {
    let batches = db.table(t).collect(0);
    concat_batches(&t.schema(), &batches).expect("concat")
}

fn i32s<'a>(b: &'a RecordBatch, c: &str) -> &'a [i32] {
    b.column_by_name(c)
        .unwrap_or_else(|| panic!("no column {c}"))
        .as_primitive::<Int32Type>()
        .values()
}

fn decs<'a>(b: &'a RecordBatch, c: &str) -> &'a [i128] {
    b.column_by_name(c)
        .expect("column")
        .as_primitive::<Decimal128Type>()
        .values()
}

fn strs<'a>(b: &'a RecordBatch, c: &str) -> Vec<&'a str> {
    b.column_by_name(c)
        .expect("column")
        .as_string_view()
        .iter()
        .map(|s| s.expect("not null"))
        .collect()
}

fn in_range<T: PartialOrd + Copy + std::fmt::Debug>(v: &[T], lo: T, hi: T, what: &str) {
    for &x in v {
        assert!(lo <= x && x <= hi, "{what}: {x:?} not in {lo:?}..={hi:?}");
    }
}

/// a-strings: letters and digits, of length `lo..=hi`, every length used.
fn a_strings(v: &[&str], lo: usize, hi: usize, what: &str) {
    let mut lens = HashSet::new();
    for s in v {
        assert!(
            (lo..=hi).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric()),
            "{what}: `{s}`"
        );
        lens.insert(s.len());
    }
    if v.len() > 1000 {
        assert_eq!(lens.len(), hi - lo + 1, "{what}: lengths used");
    }
}

fn zips(v: &[&str]) {
    for z in v {
        assert!(
            z.len() == 9 && z[..4].bytes().all(|b| b.is_ascii_digit()) && &z[4..] == "11111",
            "zip `{z}`"
        );
    }
}

fn states(v: &[&str]) {
    for s in v {
        assert!(
            s.len() == 2 && s.bytes().all(|b| b.is_ascii_uppercase()),
            "state `{s}`"
        );
    }
}

/// `ORIGINAL` in about 10% of `v`, which are a-strings `[26 .. 50]`.
fn original(v: &[&str], what: &str) {
    let hits = v.iter().filter(|s| s.contains("ORIGINAL")).count();
    let p = hits as f64 / v.len() as f64;
    assert!((0.095..0.105).contains(&p), "{what}: ORIGINAL in {p}");
    for s in v {
        assert!(
            (26..=50).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric()),
            "{what}: `{s}`"
        );
    }
}

#[test]
fn row_counts() {
    for w in [1u32, 3] {
        let db = Tpcc::new(w);
        let w = w as usize;
        let want = [
            (Warehouse, w),
            (District, 10 * w),
            (Customer, 30_000 * w),
            (History, 30_000 * w),
            (NewOrder, 9_000 * w),
            (Orders, 30_000 * w),
            (Item, 100_000),
            (Stock, 100_000 * w),
        ];
        for (t, n) in want {
            let rows: usize = db.table(t).collect(0).iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, n, "{t} at {w} warehouses");
        }
        let orders = table(&db, Orders);
        let lines: i64 = i32s(&orders, "o_ol_cnt").iter().map(|&c| c as i64).sum();
        let got: usize = db
            .table(OrderLine)
            .collect(0)
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(got as i64, lines);
        // 10 lines per order on average: 300,000 per warehouse
        let per = got as f64 / w as f64;
        assert!((298_000.0..302_000.0).contains(&per), "order lines {per}");
    }
}

#[test]
fn chunked_equals_sequential() {
    let db = Tpcc::new(2).seed(7);
    for t in TpccTableName::ALL {
        let whole = db
            .table(t)
            .chunk_source_rows(u64::MAX / 2)
            .batch_size(1 << 30);
        let seq = concat_batches(&t.schema(), &whole.collect(1)).expect("concat");
        for (chunk, batch, threads) in [(997, 333, 4), (1, 7, 3), (30_000, 8192, 2)] {
            let t2 = db
                .table(t)
                .chunk_source_rows(chunk)
                .batch_size(batch)
                .limit_source_rows(if chunk == 1 { 5_000 } else { u64::MAX });
            let got = concat_batches(&t.schema(), &t2.collect(threads)).expect("concat");
            let want = if chunk == 1 {
                let n = got.num_rows();
                seq.slice(0, n)
            } else {
                seq.clone()
            };
            assert_eq!(
                got, want,
                "{t} chunk {chunk} batch {batch} threads {threads}"
            );
        }
        // any range on its own
        let src = db.table(t).source_rows();
        let (a, b) = (src / 3, src / 3 + 1234.min(src - src / 3));
        let part = db.table(t).range(a, b);
        let part = concat_batches(&t.schema(), &part).expect("concat");
        let all = db.table(t).range(0, b);
        let all = concat_batches(&t.schema(), &all).expect("concat");
        let skip = db.table(t).range(0, a).iter().map(|x| x.num_rows()).sum();
        assert_eq!(part, all.slice(skip, all.num_rows() - skip), "{t} range");
    }
}

#[test]
fn seeds() {
    let a = table(&Tpcc::new(1).seed(1), Item);
    let b = table(&Tpcc::new(1).seed(1), Item);
    let c = table(&Tpcc::new(1).seed(2), Item);
    assert_eq!(a, b);
    assert_ne!(strs(&a, "i_name"), strs(&c, "i_name"));
}

#[test]
fn warehouse_and_district() {
    let db = Tpcc::new(4);
    let w = table(&db, Warehouse);
    assert_eq!(i32s(&w, "w_id"), [1, 2, 3, 4]);
    a_strings(&strs(&w, "w_name"), 6, 10, "w_name");
    for c in ["w_street_1", "w_street_2", "w_city"] {
        a_strings(&strs(&w, c), 10, 20, c);
    }
    states(&strs(&w, "w_state"));
    zips(&strs(&w, "w_zip"));
    in_range(decs(&w, "w_tax"), 0, 2000, "w_tax");
    assert!(decs(&w, "w_ytd").iter().all(|&v| v == 30_000_000));

    let d = table(&db, District);
    let keys: HashSet<(i32, i32)> = i32s(&d, "d_w_id")
        .iter()
        .zip(i32s(&d, "d_id"))
        .map(|(&w, &d)| (w, d))
        .collect();
    assert_eq!(keys.len(), 40);
    in_range(i32s(&d, "d_id"), 1, 10, "d_id");
    a_strings(&strs(&d, "d_name"), 6, 10, "d_name");
    states(&strs(&d, "d_state"));
    zips(&strs(&d, "d_zip"));
    in_range(decs(&d, "d_tax"), 0, 2000, "d_tax");
    assert!(decs(&d, "d_ytd").iter().all(|&v| v == 3_000_000));
    assert!(i32s(&d, "d_next_o_id").iter().all(|&v| v == 3001));
}

#[test]
fn item_and_stock() {
    let db = Tpcc::new(2);
    let i = table(&db, Item);
    assert_eq!(i32s(&i, "i_id"), (1..=100_000).collect::<Vec<_>>());
    in_range(i32s(&i, "i_im_id"), 1, 10_000, "i_im_id");
    a_strings(&strs(&i, "i_name"), 14, 24, "i_name");
    let price = decs(&i, "i_price");
    in_range(price, 100, 10_000, "i_price");
    let mean = price.iter().sum::<i128>() as f64 / price.len() as f64;
    assert!((5_000.0..5_100.0).contains(&mean), "i_price mean {mean}");
    original(&strs(&i, "i_data"), "i_data");

    let s = table(&db, Stock);
    let keys: HashSet<(i32, i32)> = i32s(&s, "s_w_id")
        .iter()
        .zip(i32s(&s, "s_i_id"))
        .map(|(&w, &i)| (w, i))
        .collect();
    assert_eq!(keys.len(), 200_000);
    in_range(i32s(&s, "s_i_id"), 1, 100_000, "s_i_id");
    in_range(i32s(&s, "s_w_id"), 1, 2, "s_w_id");
    in_range(i32s(&s, "s_quantity"), 10, 100, "s_quantity");
    for k in 1..=10 {
        let c = format!("s_dist_{k:02}");
        a_strings(&strs(&s, &c), 24, 24, &c);
    }
    for c in ["s_ytd", "s_order_cnt", "s_remote_cnt"] {
        assert!(i32s(&s, c).iter().all(|&v| v == 0), "{c}");
    }
    original(&strs(&s, "s_data"), "s_data");
}

/// The number whose syllables are `name`.
fn last_name_number(name: &str) -> usize {
    let mut rest = name;
    let mut n = 0;
    for _ in 0..3 {
        let (k, s) = SYLLABLES
            .iter()
            .enumerate()
            .find(|(_, s)| rest.starts_with(**s))
            .unwrap_or_else(|| panic!("c_last `{name}`"));
        n = n * 10 + k;
        rest = &rest[s.len()..];
    }
    assert!(rest.is_empty(), "c_last `{name}`");
    n
}

#[test]
fn customer_and_history() {
    let db = Tpcc::new(2).seed(3);
    let c = table(&db, Customer);
    let (cid, did, wid) = (i32s(&c, "c_id"), i32s(&c, "c_d_id"), i32s(&c, "c_w_id"));
    let keys: HashSet<(i32, i32, i32)> = (0..c.num_rows())
        .map(|r| (wid[r], did[r], cid[r]))
        .collect();
    assert_eq!(keys.len(), 60_000);
    in_range(cid, 1, 3000, "c_id");
    in_range(did, 1, 10, "c_d_id");
    a_strings(&strs(&c, "c_first"), 8, 16, "c_first");
    assert!(strs(&c, "c_middle").iter().all(|&m| m == "OE"));
    for col in ["c_street_1", "c_street_2", "c_city"] {
        a_strings(&strs(&c, col), 10, 20, col);
    }
    states(&strs(&c, "c_state"));
    zips(&strs(&c, "c_zip"));
    for p in strs(&c, "c_phone") {
        assert!(
            p.len() == 16 && p.bytes().all(|b| b.is_ascii_digit()),
            "phone {p}"
        );
    }
    let since = c
        .column_by_name("c_since")
        .expect("c_since")
        .as_primitive::<TimestampMicrosecondType>();
    assert!(since.values().iter().all(|&t| t == DEFAULT_LOAD_TIME));
    let credit = strs(&c, "c_credit");
    let bc = credit.iter().filter(|&&s| s == "BC").count() as f64 / credit.len() as f64;
    assert!(credit.iter().all(|&s| s == "BC" || s == "GC"));
    assert!((0.095..0.105).contains(&bc), "BC share {bc}");
    assert!(decs(&c, "c_credit_lim").iter().all(|&v| v == 5_000_000));
    in_range(decs(&c, "c_discount"), 0, 5000, "c_discount");
    assert!(decs(&c, "c_balance").iter().all(|&v| v == -10_00));
    assert!(decs(&c, "c_ytd_payment").iter().all(|&v| v == 10_00));
    assert!(i32s(&c, "c_payment_cnt").iter().all(|&v| v == 1));
    assert!(i32s(&c, "c_delivery_cnt").iter().all(|&v| v == 0));
    a_strings(&strs(&c, "c_data"), 300, 500, "c_data");

    // c_last: the first 1,000 customers of a district iterate 0..=999,
    // the rest are NURand(255, 0, 999) with this load's C
    let last = strs(&c, "c_last");
    let mut counts = vec![0u64; 1000];
    for r in 0..c.num_rows() {
        let n = last_name_number(last[r]);
        if cid[r] <= 1000 {
            assert_eq!(n, cid[r] as usize - 1);
        } else {
            counts[n] += 1;
        }
    }
    let c_load = db.c_load();
    assert!((0..=255).contains(&c_load));
    // exact NURand pmf: (((r1 | r2) + C) % 1000) for r1 in 0..=255, r2 in 0..=999
    let mut pmf = vec![0f64; 1000];
    for r1 in 0..=255i64 {
        for r2 in 0..=999i64 {
            pmf[(((r1 | r2) + c_load) % 1000) as usize] += 1.0 / (256.0 * 1000.0);
        }
    }
    let n: u64 = counts.iter().sum();
    assert_eq!(n, 2 * 10 * 2000);
    let chi2: f64 = counts
        .iter()
        .zip(&pmf)
        .filter(|(_, p)| **p > 0.0)
        .map(|(&o, &p)| (o as f64 - n as f64 * p).powi(2) / (n as f64 * p))
        .sum();
    // 999 degrees of freedom: mean 999, sd ~45
    assert!(chi2 < 1200.0, "c_last chi^2 {chi2}");
    // and it is not uniform: NURand's skew is visible at this size
    let uniform: f64 = counts
        .iter()
        .map(|&o| (o as f64 - n as f64 / 1000.0).powi(2) / (n as f64 / 1000.0))
        .sum();
    assert!(uniform > 1500.0, "c_last looks uniform: chi^2 {uniform}");

    let h = table(&db, History);
    assert_eq!(i32s(&h, "h_c_id"), cid);
    assert_eq!(i32s(&h, "h_c_d_id"), did);
    assert_eq!(i32s(&h, "h_d_id"), did);
    assert_eq!(i32s(&h, "h_c_w_id"), wid);
    assert_eq!(i32s(&h, "h_w_id"), wid);
    assert!(decs(&h, "h_amount").iter().all(|&v| v == 10_00));
    a_strings(&strs(&h, "h_data"), 12, 24, "h_data");
}

#[test]
fn orders_new_orders_and_lines() {
    let db = Tpcc::new(2).seed(11);
    let o = table(&db, Orders);
    let (oid, did, wid, cid) = (
        i32s(&o, "o_id"),
        i32s(&o, "o_d_id"),
        i32s(&o, "o_w_id"),
        i32s(&o, "o_c_id"),
    );
    // o_c_id: a permutation of 1..=3000 in each district
    let mut per: HashMap<(i32, i32), Vec<i32>> = HashMap::new();
    for r in 0..o.num_rows() {
        per.entry((wid[r], did[r])).or_default().push(cid[r]);
        assert_eq!(oid[r] as usize, r % 3000 + 1);
    }
    assert_eq!(per.len(), 20);
    for (k, mut v) in per {
        assert_ne!(
            v[..10],
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "{k:?} not shuffled"
        );
        v.sort();
        assert_eq!(v, (1..=3000).collect::<Vec<_>>(), "{k:?}");
    }
    let carrier = o.column_by_name("o_carrier_id").expect("carrier");
    let cv = carrier.as_primitive::<Int32Type>();
    for (r, &id) in oid.iter().enumerate() {
        if id < 2101 {
            assert!(carrier.is_valid(r) && (1..=10).contains(&cv.value(r)));
        } else {
            assert!(carrier.is_null(r));
        }
    }
    let cnt = i32s(&o, "o_ol_cnt");
    in_range(cnt, 5, 15, "o_ol_cnt");
    let mut hist = [0u32; 16];
    cnt.iter().for_each(|&c| hist[c as usize] += 1);
    for (c, &h) in hist.iter().enumerate().skip(5) {
        // 60,000 orders over 11 counts: ~5,455 each (sd ~70)
        assert!((5_100..5_800).contains(&h), "o_ol_cnt {c}: {h}");
    }
    assert!(i32s(&o, "o_all_local").iter().all(|&v| v == 1));

    let no = table(&db, NewOrder);
    let keys: HashSet<(i32, i32, i32)> = (0..no.num_rows())
        .map(|r| {
            (
                i32s(&no, "no_w_id")[r],
                i32s(&no, "no_d_id")[r],
                i32s(&no, "no_o_id")[r],
            )
        })
        .collect();
    let want: HashSet<(i32, i32, i32)> = (0..o.num_rows())
        .filter(|&r| oid[r] >= 2101)
        .map(|r| (wid[r], did[r], oid[r]))
        .collect();
    assert_eq!(keys, want);

    let ol = table(&db, OrderLine);
    let (lo, ld, lw, ln) = (
        i32s(&ol, "ol_o_id"),
        i32s(&ol, "ol_d_id"),
        i32s(&ol, "ol_w_id"),
        i32s(&ol, "ol_number"),
    );
    // lines of each order, in order, numbered 1..=o_ol_cnt
    let mut r = 0;
    for k in 0..o.num_rows() {
        for n in 1..=cnt[k] {
            assert_eq!((lw[r], ld[r], lo[r], ln[r]), (wid[k], did[k], oid[k], n));
            r += 1;
        }
    }
    assert_eq!(r, ol.num_rows());
    let item = i32s(&ol, "ol_i_id");
    in_range(item, 1, 100_000, "ol_i_id");
    let mean = item.iter().map(|&i| i as f64).sum::<f64>() / item.len() as f64;
    assert!((49_500.0..51_500.0).contains(&mean), "ol_i_id mean {mean}");
    assert_eq!(i32s(&ol, "ol_supply_w_id"), lw);
    assert!(i32s(&ol, "ol_quantity").iter().all(|&q| q == 5));
    let del = ol.column_by_name("ol_delivery_d").expect("delivery");
    let amount = decs(&ol, "ol_amount");
    for r in 0..ol.num_rows() {
        if lo[r] < 2101 {
            assert!(del.is_valid(r), "delivered line has a date");
            assert_eq!(amount[r], 0);
        } else {
            assert!(del.is_null(r));
            assert!(
                (1..=999_999).contains(&amount[r]),
                "ol_amount {}",
                amount[r]
            );
        }
    }
    a_strings(&strs(&ol, "ol_dist_info"), 24, 24, "ol_dist_info");
}

#[test]
fn csv() {
    let db = Tpcc::new(1);
    let w = db.table(Warehouse).write_csv(Vec::new(), 0).expect("csv");
    let line = String::from_utf8(w).expect("ascii");
    let f: Vec<&str> = line.trim_end().split(',').collect();
    assert_eq!(f.len(), 9);
    assert_eq!(f[0], "1");
    assert!(f[7].starts_with("0.") && f[7].len() == 6, "w_tax {}", f[7]);
    assert_eq!(f[8], "300000.00");
    let o = db
        .table(Orders)
        .limit_source_rows(2101)
        .write_csv(Vec::new(), 0)
        .expect("csv");
    let o = String::from_utf8(o).expect("ascii");
    let lines: Vec<&str> = o.lines().collect();
    assert_eq!(lines.len(), 2101);
    assert!(lines[0].contains(",2026-01-01 00:00:00,"));
    assert!(lines[2100].starts_with("2101,1,1,"));
    assert_eq!(lines[2100].split(',').nth(5), Some("NULL"));
}
