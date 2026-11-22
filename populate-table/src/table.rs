//! [`Table`]: a named, typed, any-width table built from populate
//! distributions, generated as Arrow `RecordBatch`es.
//!
//! ```
//! use populate_builtins::{choose, uniform};
//! use populate_table::Table;
//!
//! let sales = Table::new("sales", 10_000)
//!     .key("id")                                         // 1, 2, 3, ...
//!     .col("store", choose(&["north", "south", "east"]))
//!     .col("sold", uniform(10_000i32, 10_365)).date()    // Date32
//!     .col("qty", uniform(1i64, 10))
//!     .col("price", uniform(100i64, 9_999)).decimal(7, 2) // cents
//!     .derive2("total", ["qty", "price"], |q: i64, p: i64| q * p).decimal(9, 2)
//!     .col("note", choose(&["", "gift"])).nullable(0.1);
//!
//! let mut rows = 0;
//! for batch in sales.batches(4096) {
//!     rows += batch.num_rows();
//! }
//! assert_eq!(rows, 10_000);
//! assert_eq!(sales.schema().fields().len(), 7);
//! ```
//!
//! ## How a batch is generated
//!
//! Batch `k` covers rows `k * batch_size ..`. Each column is generated whole,
//! in declaration order, by one call into its generator:
//!
//! - `col` columns run the distribution's columnar `collect_into` with an
//!   RNG seeded from `(table seed, column name, k)`;
//! - `row` / `row_str` / `key` columns are pure functions of the row number;
//! - `derive*` columns read earlier columns of the same batch as slices.
//!
//! Values are generated into exactly-sized buffers in their storage type
//! (`i32` days, `i64` cents, Arrow-layout strings) and moved into Arrow
//! arrays; `date()` and `decimal64` are buffer reinterpretations, not copies.
//!
//! Because every batch depends only on its index, batches can be generated
//! in any order and on any number of threads with identical output:
//! [`Table::for_each_batch`] does that and hands batches back in order.

use crate::stream::{hash_str, mix64};
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, Decimal64Type, Decimal128Type, Int32Type, Int64Type};
use arrow_array::{Array, ArrayRef, PrimitiveArray, RecordBatch, make_array};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_cast::cast;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use populate_core::string::{StrBuf, StrCol};
use populate_core::traits::{DefaultSink, Distribution};
use populate_sink_arrow::{
    ArrowBackend, ArrowNative, IntoArrowArray, IntoArrowColumns, columns_to_value_array,
};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::Relaxed};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, OnceLock};
use std::thread;

/// Everything a column generator sees for one batch.
pub struct BatchCtx<'a> {
    /// First row of the batch (0-based).
    pub start: u64,
    /// Rows in the batch.
    pub len: usize,
    /// RNG seeded from `(table seed, column name, batch index)`.
    pub rng: &'a mut SmallRng,
    /// Columns generated so far in this batch, in storage types.
    done: &'a [ArrayRef],
}

impl BatchCtx<'_> {
    /// Row numbers of this batch.
    pub fn rows(&self) -> std::ops::Range<u64> {
        self.start..self.start + self.len as u64
    }
}

type Gen = Box<dyn Fn(&mut BatchCtx<'_>) -> ArrayRef + Send + Sync>;

/// How decimal columns are stored in the output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecimalRepr {
    /// `Decimal128`: what most engines expect (values widened from `i64`).
    #[default]
    Decimal128,
    /// `Decimal64`: the generated `i64` buffer as is, zero-copy.
    Decimal64,
}

#[derive(Debug, Clone, PartialEq)]
enum Logical {
    Storage,
    Date,
    Decimal(u8, i8),
    Arrow(DataType),
}

struct Column {
    name: String,
    seed: u64,
    logical: Logical,
    null_rate: f64,
    null_if: Option<NullIf>,
    hidden: bool,
    generate: Gen,
}

type NullIf = Arc<dyn Fn(u64) -> bool + Send + Sync>;

/// A table declaration. See the [module docs](self).
pub struct Table {
    name: String,
    rows: u64,
    seed: u64,
    decimal: DecimalRepr,
    columns: Vec<Column>,
    schema: OnceLock<SchemaRef>,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("rows", &self.rows)
            .field("schema", self.schema())
            .finish()
    }
}

fn prim<T: ArrowNative>(v: Vec<T>) -> ArrayRef {
    IntoArrowArray::into_array(v)
}

/// Typed view of an earlier column, for `derive*`.
fn input<'a, T: ArrowNative>(done: &'a [ArrayRef], idx: usize, out: &str, inp: &str) -> &'a [T] {
    let a = &done[idx];
    match a.as_any().downcast_ref::<PrimitiveArray<T::Arrow>>() {
        Some(p) => p.values(),
        None => panic!(
            "derive `{out}`: input `{inp}` is {}, but the closure takes {}",
            a.data_type(),
            <T::Arrow as arrow_array::ArrowPrimitiveType>::DATA_TYPE
        ),
    }
}

impl Table {
    /// An empty table of `rows` rows. Add columns with the builder methods.
    pub fn new(name: impl Into<String>, rows: u64) -> Self {
        Table {
            name: name.into(),
            rows,
            seed: 0,
            decimal: DecimalRepr::default(),
            columns: Vec::new(),
            schema: OnceLock::new(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Change the row count (e.g. to sample a prefix of a large table).
    pub fn with_rows(mut self, rows: u64) -> Self {
        self.rows = rows;
        self
    }

    /// Seed for every `col` column (default 0). Same seed and batch size,
    /// same data, on any number of threads.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Output representation for `decimal(p, s)` columns.
    pub fn decimal_repr(mut self, repr: DecimalRepr) -> Self {
        self.decimal = repr;
        self.schema = OnceLock::new();
        self
    }

    fn index_of(&self, name: &str, user: &str) -> usize {
        self.columns
            .iter()
            .position(|c| c.name == name)
            .unwrap_or_else(|| {
                panic!(
                    "table `{}`: column `{user}` reads `{name}`, which is not declared before it",
                    self.name
                )
            })
    }

    fn push(mut self, name: &str, generate: Gen) -> Self {
        assert!(
            self.columns.iter().all(|c| c.name != name),
            "table `{}`: duplicate column `{name}`",
            self.name
        );
        self.columns.push(Column {
            name: name.to_owned(),
            seed: hash_str(name),
            logical: Logical::Storage,
            null_rate: 0.0,
            null_if: None,
            hidden: false,
            generate,
        });
        self.schema = OnceLock::new();
        self
    }

    // ------------------------------------------------------------------
    // Columns
    // ------------------------------------------------------------------

    /// Column drawn from any populate distribution, collected columnar.
    /// A tuple-valued distribution becomes a `Struct` column.
    pub fn col<D>(self, name: &str, dist: D) -> Self
    where
        D: Distribution<ArrowBackend> + Clone + Send + Sync + 'static,
        <D::Item as DefaultSink<ArrowBackend>>::Sink: Default + IntoArrowColumns,
    {
        self.push(
            name,
            Box::new(move |ctx| {
                let sink = dist
                    .clone()
                    .collect_into(Default::default(), ctx.rng, ctx.len);
                columns_to_value_array(sink)
            }),
        )
    }

    /// Surrogate key: `row + 1` as `i64` (1, 2, 3, ...).
    pub fn key(self, name: &str) -> Self {
        self.row(name, |r| r as i64 + 1)
    }

    /// Column that is a pure function of the 0-based row number.
    pub fn row<T, F>(self, name: &str, f: F) -> Self
    where
        T: ArrowNative,
        F: Fn(u64) -> T + Send + Sync + 'static,
    {
        self.push(
            name,
            Box::new(move |ctx| prim(ctx.rows().map(&f).collect::<Vec<T>>())),
        )
    }

    /// String column that is a pure function of the row number; `f` writes
    /// the value straight into the column buffer.
    pub fn row_str<F>(self, name: &str, f: F) -> Self
    where
        F: Fn(u64, &mut StrBuf<'_>) + Send + Sync + 'static,
    {
        // bytes per row seen last time, so later batches allocate once
        let hint = AtomicUsize::new(0);
        self.push(
            name,
            Box::new(move |ctx| {
                let mut col = StrCol::new();
                populate_core::traits::Sink::<&str>::reserve(&mut col, ctx.len);
                col.reserve_bytes(hint.load(Relaxed) * ctx.len);
                let mut bytes = 0;
                for r in ctx.rows() {
                    col.push_with(|b| {
                        f(r, b);
                        bytes = b.len();
                    });
                }
                if ctx.len > 0 {
                    hint.store(bytes.div_ceil(ctx.len) + 1, Relaxed);
                }
                col.into_array()
            }),
        )
    }

    /// Column computed from one earlier column (in its storage type: dates
    /// are `i32` days, decimals `i64` units).
    pub fn derive<A, O, F>(self, name: &str, a: &str, f: F) -> Self
    where
        A: ArrowNative,
        O: ArrowNative,
        F: Fn(A) -> O + Send + Sync + 'static,
    {
        let ia = self.index_of(a, name);
        let (out, a) = (name.to_owned(), a.to_owned());
        self.push(
            name,
            Box::new(move |ctx| {
                let xa = input::<A>(ctx.done, ia, &out, &a);
                prim(xa.iter().map(|&x| f(x)).collect::<Vec<O>>())
            }),
        )
    }

    /// Column computed from two earlier columns.
    pub fn derive2<A, B, O, F>(self, name: &str, [a, b]: [&str; 2], f: F) -> Self
    where
        A: ArrowNative,
        B: ArrowNative,
        O: ArrowNative,
        F: Fn(A, B) -> O + Send + Sync + 'static,
    {
        let (ia, ib) = (self.index_of(a, name), self.index_of(b, name));
        let (out, a, b) = (name.to_owned(), a.to_owned(), b.to_owned());
        self.push(
            name,
            Box::new(move |ctx| {
                let xa = input::<A>(ctx.done, ia, &out, &a);
                let xb = input::<B>(ctx.done, ib, &out, &b);
                prim(
                    xa.iter()
                        .zip(xb)
                        .map(|(&x, &y)| f(x, y))
                        .collect::<Vec<O>>(),
                )
            }),
        )
    }

    /// Column computed from three earlier columns.
    pub fn derive3<A, B, C, O, F>(self, name: &str, [a, b, c]: [&str; 3], f: F) -> Self
    where
        A: ArrowNative,
        B: ArrowNative,
        C: ArrowNative,
        O: ArrowNative,
        F: Fn(A, B, C) -> O + Send + Sync + 'static,
    {
        let (ia, ib, ic) = (
            self.index_of(a, name),
            self.index_of(b, name),
            self.index_of(c, name),
        );
        let (out, a, b, c) = (name.to_owned(), a.to_owned(), b.to_owned(), c.to_owned());
        self.push(
            name,
            Box::new(move |ctx| {
                let xa = input::<A>(ctx.done, ia, &out, &a);
                let xb = input::<B>(ctx.done, ib, &out, &b);
                let xc = input::<C>(ctx.done, ic, &out, &c);
                prim(
                    xa.iter()
                        .zip(xb)
                        .zip(xc)
                        .map(|((&x, &y), &z)| f(x, y, z))
                        .collect::<Vec<O>>(),
                )
            }),
        )
    }

    /// Escape hatch: build the whole batch's array yourself.
    pub fn custom<F>(self, name: &str, f: F) -> Self
    where
        F: Fn(&mut BatchCtx<'_>) -> ArrayRef + Send + Sync + 'static,
    {
        self.push(name, Box::new(f))
    }

    // ------------------------------------------------------------------
    // Modifiers (apply to the column declared last)
    // ------------------------------------------------------------------

    fn last(&mut self, what: &str) -> &mut Column {
        self.schema = OnceLock::new();
        let name = &self.name;
        self.columns
            .last_mut()
            .unwrap_or_else(|| panic!("table `{name}`: `{what}` needs a column before it"))
    }

    /// Output as `Date32` (storage: `i32` days since 1970-01-01). Zero-copy.
    pub fn date(mut self) -> Self {
        self.last("date").logical = Logical::Date;
        self
    }

    /// Output as a decimal of precision `p`, scale `s` (storage: integer
    /// units of `10^-s`, e.g. cents for `s = 2`).
    pub fn decimal(mut self, p: u8, s: i8) -> Self {
        self.last("decimal").logical = Logical::Decimal(p, s);
        self
    }

    /// Output cast to any Arrow type (via `arrow_cast`, so this may copy).
    pub fn arrow_type(mut self, t: DataType) -> Self {
        self.last("arrow_type").logical = Logical::Arrow(t);
        self
    }

    /// Replace a fraction `rate` of values with NULL (validity bitmap only;
    /// the value buffer is untouched).
    pub fn nullable(mut self, rate: f64) -> Self {
        assert!((0.0..=1.0).contains(&rate), "nullable: rate in 0..=1");
        self.last("nullable").null_rate = rate;
        self
    }

    /// Make the value NULL wherever `f(row)` is true (a deterministic rule,
    /// e.g. "the current revision has no end date"). Combines with
    /// [`nullable`](Self::nullable).
    pub fn null_if(mut self, f: impl Fn(u64) -> bool + Send + Sync + 'static) -> Self {
        self.last("null_if").null_if = Some(Arc::new(f));
        self
    }

    /// Generate the column (so later columns can derive from it) but leave
    /// it out of the output.
    pub fn hidden(mut self) -> Self {
        self.last("hidden").hidden = true;
        self
    }

    // ------------------------------------------------------------------
    // Output
    // ------------------------------------------------------------------

    /// Output schema (hidden columns excluded).
    pub fn schema(&self) -> &SchemaRef {
        self.schema.get_or_init(|| {
            // a 0-row batch fixes every column's type, and surfaces derive
            // type mismatches immediately
            let (fields, _) = self.generate(0, 0, 0);
            Arc::new(Schema::new(fields))
        })
    }

    /// Number of batches of `batch_size` rows.
    pub fn num_batches(&self, batch_size: usize) -> u64 {
        assert!(batch_size > 0, "batch_size must be > 0");
        self.rows.div_ceil(batch_size as u64)
    }

    fn generate(&self, index: u64, start: u64, len: usize) -> (Vec<Field>, Vec<ArrayRef>) {
        let batch_seed = mix64(self.seed ^ mix64(hash_str(&self.name) ^ mix64(index)));
        let mut done: Vec<ArrayRef> = Vec::with_capacity(self.columns.len());
        for c in &self.columns {
            let mut rng = SmallRng::seed_from_u64(mix64(batch_seed ^ c.seed));
            let mut ctx = BatchCtx {
                start,
                len,
                rng: &mut rng,
                done: &done,
            };
            let a = (c.generate)(&mut ctx);
            assert_eq!(
                a.len(),
                len,
                "table `{}`: column `{}` produced {} rows, expected {len}",
                self.name,
                c.name,
                a.len()
            );
            done.push(a);
        }
        let mut fields = Vec::new();
        let mut out = Vec::new();
        for (c, a) in self.columns.iter().zip(done) {
            if c.hidden {
                continue;
            }
            let mut a = self.to_logical(c, a);
            let nullable = c.null_rate > 0.0 || c.null_if.is_some();
            if nullable {
                let mut rng = SmallRng::seed_from_u64(mix64(batch_seed ^ c.seed ^ 0x6e75_6c6c));
                a = with_nulls(a, start, c.null_rate, c.null_if.as_deref(), &mut rng);
            }
            fields.push(Field::new(&c.name, a.data_type().clone(), nullable));
            out.push(a);
        }
        (fields, out)
    }

    fn to_logical(&self, c: &Column, a: ArrayRef) -> ArrayRef {
        let bad = |want: &str| -> ! {
            panic!(
                "table `{}`: column `{}` is {}, `{want}` needs {}",
                self.name,
                c.name,
                a.data_type(),
                if want == "date" {
                    "Int32 days"
                } else {
                    "Int32/Int64 units"
                }
            )
        };
        match &c.logical {
            Logical::Storage => a,
            Logical::Date => match a.data_type() {
                DataType::Int32 => Arc::new(
                    a.as_primitive::<Int32Type>()
                        .clone()
                        .reinterpret_cast::<Date32Type>(),
                ),
                DataType::Date32 => a,
                _ => bad("date"),
            },
            &Logical::Decimal(p, s) => {
                let units: PrimitiveArray<Int64Type> = match a.data_type() {
                    DataType::Int64 => a.as_primitive::<Int64Type>().clone(),
                    DataType::Int32 => a.as_primitive::<Int32Type>().unary(|v| v as i64),
                    _ => bad("decimal"),
                };
                let r: ArrayRef = match self.decimal {
                    DecimalRepr::Decimal64 => Arc::new(
                        units
                            .reinterpret_cast::<Decimal64Type>()
                            .with_precision_and_scale(p, s)
                            .expect("valid decimal precision/scale"),
                    ),
                    DecimalRepr::Decimal128 => Arc::new(
                        units
                            .unary::<_, Decimal128Type>(|v| v as i128)
                            .with_precision_and_scale(p, s)
                            .expect("valid decimal precision/scale"),
                    ),
                };
                r
            }
            Logical::Arrow(t) => cast(&a, t)
                .unwrap_or_else(|e| panic!("table `{}`: column `{}`: {e}", self.name, c.name)),
        }
    }

    /// Batch `index` of `batch_size` rows (the last one may be shorter).
    pub fn batch(&self, index: u64, batch_size: usize) -> RecordBatch {
        let start = index * batch_size as u64;
        assert!(start < self.rows.max(1), "batch {index} is past the end");
        let len = (self.rows - start).min(batch_size as u64) as usize;
        let (_, cols) = self.generate(index, start, len);
        RecordBatch::try_new(self.schema().clone(), cols).expect("columns match schema")
    }

    /// The whole table as one batch.
    pub fn collect(&self) -> RecordBatch {
        let n = usize::try_from(self.rows).expect("table fits in memory");
        if n == 0 {
            return RecordBatch::new_empty(self.schema().clone());
        }
        self.batch(0, n)
    }

    /// All batches, in order, on this thread.
    pub fn batches(&self, batch_size: usize) -> impl Iterator<Item = RecordBatch> + '_ {
        (0..self.num_batches(batch_size)).map(move |i| self.batch(i, batch_size))
    }

    /// Generate every batch on `threads` threads (0 = all cores) and call
    /// `f` with each, in order, on the calling thread. Output is identical
    /// for any thread count.
    pub fn for_each_batch(
        &self,
        batch_size: usize,
        threads: usize,
        mut f: impl FnMut(RecordBatch),
    ) {
        let r: Result<(), Infallible> = self.try_for_each_batch(batch_size, threads, |b| {
            f(b);
            Ok(())
        });
        r.unwrap_or_else(|e| match e {});
    }

    /// [`for_each_batch`](Self::for_each_batch) that stops at the first error.
    pub fn try_for_each_batch<E>(
        &self,
        batch_size: usize,
        threads: usize,
        f: impl FnMut(RecordBatch) -> Result<(), E>,
    ) -> Result<(), E> {
        self.schema(); // init once, not racily in every worker
        par_ordered(
            self.num_batches(batch_size),
            threads,
            |i| self.batch(i, batch_size),
            f,
        )
    }
}

/// Run `work(0..n)` on `threads` threads (0 = all cores) and feed the
/// results to `consume` in index order on the calling thread, stopping at
/// the first error. Workers run at most about `2 * threads` tasks ahead.
pub fn par_ordered<T: Send, E>(
    n: u64,
    threads: usize,
    work: impl Fn(u64) -> T + Sync,
    mut consume: impl FnMut(T) -> Result<(), E>,
) -> Result<(), E> {
    let threads = match threads {
        0 => thread::available_parallelism().map_or(1, |t| t.get()),
        t => t,
    }
    .min(n.max(1) as usize);
    if threads <= 1 {
        return (0..n).try_for_each(|i| consume(work(i)));
    }
    let next = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    thread::scope(|s| {
        let (tx, rx) = sync_channel::<(u64, T)>(threads * 2);
        for _ in 0..threads {
            let tx = tx.clone();
            let (next, stop, work) = (&next, &stop, &work);
            s.spawn(move || {
                while !stop.load(Relaxed) {
                    let i = next.fetch_add(1, Relaxed);
                    if i >= n || tx.send((i, work(i))).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        // Reorder. `rx` is dropped when this closure returns, before the
        // scope joins, so a worker blocked on a full channel wakes up (its
        // send fails) when we stop early.
        let mut pending = BTreeMap::new();
        let mut want = 0u64;
        for (i, t) in rx.iter() {
            pending.insert(i, t);
            while let Some(t) = pending.remove(&want) {
                want += 1;
                if let Err(e) = consume(t) {
                    stop.store(true, Relaxed);
                    return Err(e);
                }
            }
        }
        Ok(())
    })
}

/// Attach a validity bitmap (about `rate` random nulls, plus rows where
/// `rule` holds); the value buffers are untouched.
fn with_nulls(
    a: ArrayRef,
    start: u64,
    rate: f64,
    rule: Option<&(dyn Fn(u64) -> bool + Send + Sync)>,
    rng: &mut SmallRng,
) -> ArrayRef {
    let threshold = (rate * (1u64 << 32) as f64) as u64;
    let valid = match rule {
        None => BooleanBuffer::collect_bool(a.len(), |_| (rng.random::<u32>() as u64) >= threshold),
        Some(rule) => BooleanBuffer::collect_bool(a.len(), |i| {
            let random_ok = threshold == 0 || (rng.random::<u32>() as u64) >= threshold;
            random_ok && !rule(start + i as u64)
        }),
    };
    let data = a
        .to_data()
        .into_builder()
        .nulls(Some(NullBuffer::new(valid)));
    // SAFETY: only a null buffer of the array's own length is added; the
    // buffers it already had were valid.
    make_array(unsafe { data.build_unchecked() })
}
