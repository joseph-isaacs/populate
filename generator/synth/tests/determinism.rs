//! There is no reference generator for these shapes, so the checks are
//! determinism and self-consistency:
//!
//! - chunked, multi-threaded output equals one sequential run, for any
//!   chunk size, batch size and thread count, and as Parquet bytes;
//! - the same seed gives the same bytes (pinned by a hash of the Parquet
//!   file, so an accidental change to the data fails here), and a
//!   different seed gives different data;
//! - the values hold the invariants each shape promises.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrowPrimitiveType, RecordBatch, StringViewArray};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use arrow_select::concat::concat as concat_arrays;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use populate_core::{Columnar, Columns, Distribution};
use populate_sink_arrow::IntoArrowColumns;
use populate_synth::{Embeddings, Features, Hits, Shape, Synth, Taxi};
use populate_table::{Chunks, mix64};
use std::collections::HashMap;

const ROWS: u64 = 20_011;

/// The distributions behind a [`Synth`].
trait Table: Distribution<Item: Columnar<Columns: IntoArrowColumns>> + Clone + Sync {}
impl<D> Table for D
where
    D: Distribution + Clone + Sync,
    Columns<D::Item>: IntoArrowColumns,
{
}

fn shapes(seed: u64) -> Vec<Box<dyn Fn(u64, usize) -> RecordBatch>> {
    vec![
        Box::new(move |s, n| Taxi::new(seed).table(s + n as u64).slice(s, n)),
        Box::new(move |s, n| {
            Hits::with_sizes(seed, 500, 5_000)
                .table(s + n as u64)
                .slice(s, n)
        }),
        Box::new(move |s, n| Features::new(seed).table(s + n as u64).slice(s, n)),
        Box::new(move |s, n| {
            Embeddings::<64>::with_sizes(seed, 16, 0.6)
                .table(s + n as u64)
                .slice(s, n)
        }),
    ]
}

fn concat(batches: &[RecordBatch]) -> RecordBatch {
    let schema = batches[0].schema();
    let cols = (0..schema.fields().len())
        .map(|c| {
            let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            concat_arrays(&arrays).unwrap()
        })
        .collect();
    RecordBatch::try_new(schema, cols).unwrap()
}

/// Every way of cutting the table gives the same rows.
fn check_chunking<S: Shape>(shape: S) {
    let whole = shape.table(ROWS).collect();
    assert_eq!(whole.num_rows() as u64, ROWS);
    for (batch, chunk) in [
        (1000, 1000),
        (4096, 3 * 4096),
        (777, 5000),
        (ROWS as usize, ROWS),
    ] {
        for threads in [1, 3] {
            let t = shape.table(ROWS).batch_size(batch).chunk_rows(chunk);
            let mut got = Vec::new();
            t.for_each_batch(threads, |b| got.push(b));
            assert!(got.iter().all(|b| b.num_rows() <= batch));
            assert_eq!(
                concat(&got),
                whole,
                "{} batch {batch} chunk {chunk} threads {threads}",
                shape.name()
            );
        }
    }
    // a single row and an odd slice, generated on their own
    for (s, n) in [(0, 1), (ROWS - 1, 1), (12_345, 321)] {
        assert_eq!(shape.table(ROWS).slice(s, n), whole.slice(s as usize, n));
    }
}

#[test]
fn taxi_chunked_equals_sequential() {
    check_chunking(Taxi::new(1));
}

#[test]
fn hits_chunked_equals_sequential() {
    check_chunking(Hits::with_sizes(1, 500, 5_000));
}

#[test]
fn features_chunked_equals_sequential() {
    check_chunking(Features::new(1));
}

#[test]
fn embeddings_chunked_equals_sequential() {
    check_chunking(Embeddings::<64>::with_sizes(1, 16, 0.6));
}

fn parquet<D: Table>(t: &Synth<D>, threads: usize) -> Vec<u8> {
    t.write_parquet(Vec::new(), threads, None).unwrap()
}

/// FNV-1a over bytes.
fn fnv(h: u64, b: &[u8]) -> u64 {
    b.iter()
        .fold(h, |h, &x| (h ^ x as u64).wrapping_mul(0x100_0000_01b3))
}

/// A hash of the values (not of a file format), to pin a table's data.
fn fingerprint(b: &RecordBatch) -> u64 {
    fn data(h: u64, d: &ArrayData) -> u64 {
        // the valid values of every buffer, plus validity
        let mut h = fnv(h, &(d.len() as u64).to_le_bytes());
        let t = d.data_type();
        if let DataType::Utf8View = t {
            let a = StringViewArray::from(d.clone());
            for v in a.iter() {
                h = fnv(h, v.map(str::as_bytes).unwrap_or(b"\xff"));
            }
            return h;
        }
        if let Some(n) = d.nulls() {
            for i in 0..d.len() {
                h = fnv(h, &[n.is_valid(i) as u8]);
            }
        }
        if let Some(w) = t.primitive_width() {
            let buf = &d.buffers()[0];
            h = fnv(
                h,
                &buf.as_slice()[d.offset() * w..(d.offset() + d.len()) * w],
            );
        }
        d.child_data().iter().fold(h, data)
    }
    b.columns()
        .iter()
        .fold(0xcbf2_9ce4_8422_2325, |h, c| data(h, &c.to_data()))
}

fn check_parquet<D: Table>(t: Synth<D>) -> u64 {
    let one = parquet(&t, 1);
    for threads in [2, 4, 0] {
        assert_eq!(one, parquet(&t, threads), "parquet bytes vary with threads");
    }
    // reads back as the table
    let mut back = Vec::new();
    for b in ParquetRecordBatchReaderBuilder::try_new(Bytes::from(one.clone()))
        .unwrap()
        .build()
        .unwrap()
    {
        back.push(b.unwrap());
    }
    assert_eq!(concat(&back), t.collect());
    assert_eq!(Chunks::num_chunks(&t), ROWS.div_ceil(4096));
    fingerprint(&t.collect())
}

#[test]
fn parquet_bytes_identical_for_any_thread_count_and_pinned() {
    let small = |n| (n, 4096u64);
    let mut got = Vec::new();
    let (r, c) = small(ROWS);
    got.push(check_parquet(
        Taxi::new(7).table(r).batch_size(1024).chunk_rows(c),
    ));
    got.push(check_parquet(
        Hits::with_sizes(7, 500, 5_000)
            .table(r)
            .batch_size(1024)
            .chunk_rows(c),
    ));
    got.push(check_parquet(
        Features::new(7).table(r).batch_size(1024).chunk_rows(c),
    ));
    got.push(check_parquet(
        Embeddings::<64>::with_sizes(7, 16, 0.6)
            .table(r)
            .batch_size(1024)
            .chunk_rows(c),
    ));
    // Change these only on purpose: they pin the values for seed 7.
    let pinned: [u64; 4] = [
        0x6af6aa2a50bf68fa,
        0x69bbcd48d983f1cb,
        0x723509e790cfd903,
        0x5ccd3780aa210107,
    ];
    assert_eq!(got, pinned, "data changed for a fixed seed: {got:#x?}");
}

#[test]
fn same_seed_same_data_other_seed_other_data() {
    let (a, b, c) = (shapes(5), shapes(5), shapes(6));
    for i in 0..a.len() {
        let (x, y, z) = (a[i](100, 500), b[i](100, 500), c[i](100, 500));
        assert_eq!(x, y);
        // every column except the row ids changes with the seed
        for col in 0..x.num_columns() {
            let name = x.schema().field(col).name().clone();
            if name == "id" {
                continue;
            }
            let constant = {
                let v = x.column(col);
                v.len() > 1 && (1..v.len()).all(|k| v.slice(k, 1) == v.slice(0, 1))
            };
            if !constant {
                assert_ne!(
                    x.column(col),
                    z.column(col),
                    "column {name} ignores the seed"
                );
            }
        }
    }
}

fn col<'a, T: ArrowPrimitiveType>(b: &'a RecordBatch, name: &str) -> &'a [T::Native] {
    b.column_by_name(name)
        .unwrap_or_else(|| panic!("no column {name}"))
        .as_primitive::<T>()
        .values()
}

#[test]
fn taxi_invariants() {
    let b = Taxi::new(3).trips_per_day(50_000).table(120_000).collect();
    let pickup = col::<TimestampMicrosecondType>(&b, "tpep_pickup_datetime");
    let dropoff = col::<TimestampMicrosecondType>(&b, "tpep_dropoff_datetime");
    assert!(pickup.windows(2).all(|w| w[0] <= w[1]), "pickups sorted");
    assert!(pickup.iter().zip(dropoff).all(|(p, d)| d > p));
    // 120k trips at 50k a day: three days from 2024-01-01
    let day = |us: i64| (us - 1_704_067_200_000_000).div_euclid(86_400_000_000);
    assert_eq!((day(pickup[0]), day(*pickup.last().unwrap())), (0, 2));
    // evening busier than 4am
    let hour = |us: i64| (us.rem_euclid(86_400_000_000) / 3_600_000_000) as usize;
    let mut per_hour = [0u32; 24];
    pickup.iter().for_each(|&p| per_hour[hour(p)] += 1);
    assert!(per_hour[18] > 5 * per_hour[4], "{per_hour:?}");

    let pc = b.column_by_name("passenger_count").unwrap();
    let nulls = pc.null_count() as f64 / b.num_rows() as f64;
    assert!((0.025..0.035).contains(&nulls), "{nulls}");
    for name in [
        "RatecodeID",
        "store_and_fwd_flag",
        "congestion_surcharge",
        "Airport_fee",
    ] {
        assert_eq!(
            b.column_by_name(name).unwrap().nulls(),
            pc.nulls(),
            "{name}"
        );
    }
    let payment = col::<Int32Type>(&b, "payment_type");
    let tip = col::<Float64Type>(&b, "tip_amount");
    for i in 0..b.num_rows() {
        assert_eq!(pc.is_null(i), payment[i] == 0);
        if payment[i] != 1 {
            assert_eq!(tip[i], 0.0);
        }
    }
    // total is the sum of its parts, to the cent
    let parts = [
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
    ];
    let total = col::<Float64Type>(&b, "total_amount");
    let cong = b
        .column_by_name("congestion_surcharge")
        .unwrap()
        .as_primitive::<Float64Type>();
    let fee = b
        .column_by_name("Airport_fee")
        .unwrap()
        .as_primitive::<Float64Type>();
    for (i, &t) in total.iter().enumerate() {
        let mut sum: f64 = parts.iter().map(|p| col::<Float64Type>(&b, p)[i]).sum();
        if cong.is_valid(i) {
            sum += cong.value(i) + fee.value(i);
        }
        assert!((sum - t).abs() < 0.006, "row {i}: {sum} vs {t}");
    }
    let dist = col::<Float64Type>(&b, "trip_distance");
    let mut sorted = dist.to_vec();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];
    assert!((1.5..1.9).contains(&median), "{median}");
    let zones = col::<Int32Type>(&b, "PULocationID");
    assert!(zones.iter().all(|z| (1..=265).contains(z)));
}

#[test]
fn hits_invariants() {
    let b = Hits::with_sizes(3, 1_000, 20_000).table(100_000).collect();
    let time = col::<TimestampSecondType>(&b, "EventTime");
    let date = col::<Date32Type>(&b, "EventDate");
    assert!(time.windows(2).all(|w| w[0] <= w[1]));
    assert!(
        time.iter()
            .zip(date)
            .all(|(t, d)| t.div_euclid(86_400) == *d as i64)
    );
    assert_eq!(date[0], 15887, "2013-07-01");

    let url = b.column_by_name("URL").unwrap().as_string_view();
    let hash = col::<Int64Type>(&b, "URLHash");
    let counter = col::<Int32Type>(&b, "CounterID");
    // URL and hash agree: same hash, same URL
    let mut seen = HashMap::new();
    for i in 0..b.num_rows() {
        let u = url.value(i);
        assert!(u.starts_with("https://") && u.ends_with(".html"), "{u}");
        assert_eq!(*seen.entry(hash[i]).or_insert(u), u);
        assert!((1..=1_000).contains(&counter[i]));
    }
    // skew: the top site has far more than its share
    let top = counter.iter().filter(|&&c| c == 1).count();
    assert!(top > 100_000 / 1_000 * 20, "{top}");

    // per-user attributes are consistent
    let user = col::<Int64Type>(&b, "UserID");
    let region = col::<Int32Type>(&b, "RegionID");
    let width = col::<Int16Type>(&b, "ResolutionWidth");
    let mut users = HashMap::new();
    for i in 0..b.num_rows() {
        assert_eq!(
            *users.entry(user[i]).or_insert((region[i], width[i])),
            (region[i], width[i])
        );
    }

    let engine = col::<Int16Type>(&b, "SearchEngineID");
    let phrase = b.column_by_name("SearchPhrase").unwrap().as_string_view();
    let referer = b.column_by_name("Referer").unwrap().as_string_view();
    let source = col::<Int16Type>(&b, "TraficSourceID");
    for i in 0..b.num_rows() {
        assert_eq!(engine[i] > 0, !phrase.value(i).is_empty());
        assert_eq!(engine[i] > 0, source[i] == 2);
        if engine[i] > 0 {
            assert!(
                referer
                    .value(i)
                    .ends_with(&phrase.value(i).replace(' ', "+"))
            );
        }
        assert_eq!(
            source[i] == 0 || source[i] == -1 || source[i] == 3,
            referer.value(i).is_empty()
        );
    }
    let searches = engine.iter().filter(|&&e| e > 0).count() as f64 / 1e5;
    assert!((0.28..0.32).contains(&searches), "{searches}");
}

#[test]
fn features_invariants() {
    let f = Features::new(3);
    let b = f.table(50_000).collect();
    assert_eq!(b.num_columns(), 3 + 32 + 8 + 2);
    let split = b.column_by_name("split").unwrap().as_string_view();
    let train = (0..b.num_rows())
        .filter(|&i| split.value(i) == "train")
        .count() as f64
        / 5e4;
    assert!((0.79..0.81).contains(&train), "{train}");
    for i in 0..32 {
        let a = b.column_by_name(&format!("f_{i}")).unwrap();
        let rate = a.null_count() as f64 / 5e4;
        assert!((0.015..0.025).contains(&rate), "f_{i} nulls {rate}");
        let v = a.as_primitive::<Float32Type>().values();
        assert!(v.iter().all(|x| x.is_finite()));
        match i % 4 {
            1 => assert!(v.iter().all(|&x| x > 0.0)),
            3 => assert!(v.iter().all(|&x| (0.0..1.0).contains(&x))),
            _ => {}
        }
    }
    // correlated features follow the one before them
    let corr = |a: &[f32], b: &[f32]| {
        let n = a.len() as f64;
        let (ma, mb) = (
            a.iter().map(|&x| x as f64).sum::<f64>() / n,
            b.iter().map(|&x| x as f64).sum::<f64>() / n,
        );
        let mut c = 0.0;
        let (mut va, mut vb) = (0.0, 0.0);
        for (&x, &y) in a.iter().zip(b) {
            let (dx, dy) = (x as f64 - ma, y as f64 - mb);
            c += dx * dy;
            va += dx * dx;
            vb += dy * dy;
        }
        c / (va * vb).sqrt()
    };
    // f_2 is built from f_1's normal draw (f_1 is its log-normal)
    let r = corr(col::<Float32Type>(&b, "f_1"), col::<Float32Type>(&b, "f_2"));
    // about 0.86 rho, rho in 0.5..0.95
    assert!((0.4..0.85).contains(&r), "{r}");
    let r = corr(col::<Float32Type>(&b, "f_0"), col::<Float32Type>(&b, "f_4"));
    assert!(r.abs() < 0.03, "{r}");
    // the label is learnable: it moves with the signal features
    let label = col::<Int32Type>(&b, "label");
    let lf: Vec<f32> = label.iter().map(|&l| l as f32).collect();
    let best = (0..8)
        .map(|i| corr(&lf, col::<Float32Type>(&b, &format!("f_{i}"))).abs())
        .fold(0.0, f64::max);
    assert!(best > 0.1, "{best}");
    let c4 = col::<Int32Type>(&b, "c_4");
    assert!(c4.iter().all(|&c| (0..100_000).contains(&c)));
}

#[test]
fn embeddings_invariants() {
    let b = Embeddings::<128>::with_sizes(3, 32, 0.6)
        .table(2_000)
        .collect();
    let e = b.column_by_name("embedding").unwrap().as_fixed_size_list();
    assert_eq!(e.value_length(), 128);
    let v = e.values().as_primitive::<Float32Type>().values();
    let cluster = col::<Int32Type>(&b, "cluster");
    let row = |i: usize| &v[i * 128..(i + 1) * 128];
    let dot = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
    for i in 0..b.num_rows() {
        assert!((dot(row(i), row(i)) - 1.0).abs() < 1e-5);
    }
    // same cluster: similar; other clusters: near orthogonal
    let (mut same, mut other) = (Vec::new(), Vec::new());
    for i in 0..200 {
        for j in i + 1..200 {
            let d = dot(row(i), row(j));
            if cluster[i] == cluster[j] {
                same.push(d)
            } else {
                other.push(d)
            }
        }
    }
    let mean = |x: &[f32]| x.iter().sum::<f32>() / x.len() as f32;
    assert!(mean(&same) > 0.6, "{}", mean(&same));
    assert!(mean(&other).abs() < 0.05, "{}", mean(&other));
}

#[test]
fn row_hash_is_stable() {
    // the per-row hash every value derives from
    assert_eq!(mix64(0), 0xe220_a839_7b1d_cdaf);
}
