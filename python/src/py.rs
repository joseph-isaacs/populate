//! The `populate` Python module (feature `python`).
//!
//! Distributions are built from Python as [`Dist`] values, each an
//! [`AnyDist`], and a dict of them becomes a [`Source`]: anything that
//! implements [`Chunks`] (a [`DynTable`], a generator's table, or a
//! `DistTable` of a Rust distribution) behind one Python type. A `Source`
//! is an Arrow stream (`__arrow_c_stream__`): `pyarrow.table(src)`,
//! `polars.DataFrame(src)` or `duckdb.from_arrow(src)` take its batches,
//! generated on all cores, without copying a buffer.
//!
//! A Rust crate with its own distributions can reuse this: wrap a
//! `Distribution` in [`Dist::from_dist`], or any [`Chunks`] in
//! [`Source::new`], and return it from its own `#[pyfunction]`.

use crate::dynamic::{AnyDist, BoxDist, DynTable, Normal, Owned};
use arrow_array::ffi::to_ffi;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, SchemaRef};
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use populate_builtins::{chars, choose, constant, list, range, text, uniform, weighted};
use populate_core::{Columnar, Date32, Distribution};
use populate_table::chunks::try_for_each_batch;
use populate_table::{Chunks, write_parquet};
use pyo3::exceptions::{PyIOError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyCapsule, PyDict, PyFloat, PyInt, PyString};
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::fs::File;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, sync_channel};
use std::thread::JoinHandle;

// ============================================================================
// Dist
// ============================================================================

/// A distribution of one column. Build one with the module's functions
/// (`range`, `uniform`, `choose`, ...) and combine them with `list` and
/// the methods below.
#[pyclass(frozen, module = "populate")]
#[derive(Clone)]
pub struct Dist {
    inner: AnyDist,
}

impl Dist {
    /// A Rust distribution as a Python `Dist`. `T` is one of the row types
    /// [`AnyDist`] has a variant for.
    pub fn from_dist<D, T>(dist: D) -> Self
    where
        D: Distribution<Item = T> + Clone + Send + Sync + 'static,
        T: Columnar,
        AnyDist: From<BoxDist<T>>,
    {
        Dist {
            inner: BoxDist::new(dist).into(),
        }
    }

    pub fn any(&self) -> &AnyDist {
        &self.inner
    }
}

impl From<AnyDist> for Dist {
    fn from(inner: AnyDist) -> Self {
        Dist { inner }
    }
}

#[pymethods]
impl Dist {
    /// The row type: `int64`, `string?` (nullable), `list<float64>`, ...
    #[getter]
    fn r#type(&self) -> &'static str {
        self.inner.type_name()
    }

    /// Each row missing with probability `rate`.
    fn nullable(&self, rate: f64) -> PyResult<Dist> {
        if !(0.0..=1.0).contains(&rate) {
            return Err(PyValueError::new_err("nullable: rate must be in [0, 1]"));
        }
        let kind = self.inner.type_name();
        self.inner
            .clone()
            .nullable(rate)
            .map(Dist::from)
            .ok_or_else(|| PyTypeError::new_err(format!("nullable: can't make {kind} nullable")))
    }

    /// Integers as dates: days since 1970-01-01.
    fn date(&self) -> PyResult<Dist> {
        let kind = self.inner.type_name();
        self.inner
            .clone()
            .date()
            .map(Dist::from)
            .ok_or_else(|| PyTypeError::new_err(format!("date: needs integers, not {kind}")))
    }

    /// The first `n` rows as an Arrow array (`pyarrow.array(d.collect(n))`).
    #[pyo3(signature = (n, seed = 0))]
    fn collect(&self, py: Python<'_>, n: usize, seed: u64) -> ArrowArray {
        let mut d = self.inner.clone();
        let array = py.detach(move || d.collect_array(&mut SmallRng::seed_from_u64(seed), n));
        ArrowArray(array)
    }

    fn __repr__(&self) -> String {
        format!("Dist({})", self.inner.type_name())
    }
}

/// The row type a Python value or `dtype` names.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Kind {
    I32,
    I64,
    F64,
    Bool,
    Str,
    Date,
}

impl Kind {
    fn of(v: &Bound<'_, PyAny>) -> PyResult<Kind> {
        Ok(if v.is_instance_of::<PyBool>() {
            Kind::Bool
        } else if v.is_instance_of::<PyInt>() {
            Kind::I64
        } else if v.is_instance_of::<PyFloat>() {
            Kind::F64
        } else if v.is_instance_of::<PyString>() {
            Kind::Str
        } else if v.hasattr("toordinal")? {
            Kind::Date
        } else {
            return Err(PyTypeError::new_err(format!(
                "no column type for a {}",
                v.get_type().name()?
            )));
        })
    }

    fn named(dtype: &str) -> PyResult<Kind> {
        Ok(match dtype {
            "int32" => Kind::I32,
            "int64" => Kind::I64,
            "float64" => Kind::F64,
            "bool" => Kind::Bool,
            "string" => Kind::Str,
            "date32" | "date" => Kind::Date,
            _ => return Err(PyValueError::new_err(format!("unknown dtype `{dtype}`"))),
        })
    }

    /// `dtype` if given, else the type of `v`.
    fn pick(v: &Bound<'_, PyAny>, dtype: Option<&str>) -> PyResult<Kind> {
        dtype.map_or_else(|| Kind::of(v), Kind::named)
    }
}

/// Days since 1970-01-01 of a `datetime.date` (or an int, taken as days).
fn days(v: &Bound<'_, PyAny>) -> PyResult<i32> {
    if v.is_instance_of::<PyInt>() {
        return v.extract();
    }
    let ordinal: i64 = v.call_method0("toordinal")?.extract()?;
    // date(1970, 1, 1).toordinal()
    i32::try_from(ordinal - 719_163).map_err(|_| PyValueError::new_err("date out of range"))
}

/// The values of a `choose` or `text` live as long as the process:
/// populate's pickers hold `&'static` slices.
fn leak<T>(v: Vec<T>) -> &'static [T] {
    Box::leak(v.into_boxed_slice())
}

fn values<'py, T>(
    vals: &[Bound<'py, PyAny>],
    f: impl Fn(&Bound<'py, PyAny>) -> PyResult<T>,
) -> PyResult<Vec<T>> {
    vals.iter().map(f).collect()
}

fn pick<T>(vals: Vec<T>, weights: &Option<Vec<u32>>) -> BoxDist<T>
where
    T: Copy + Columnar + Send + Sync + 'static,
{
    match weights {
        None => BoxDist::new(choose(leak(vals))),
        Some(w) => BoxDist::new(weighted(leak(
            vals.into_iter().zip(w.iter().copied()).collect(),
        ))),
    }
}

/// `start`, `start + step`, ...: a key column. Skips straight to any row.
#[pyfunction(name = "range")]
#[pyo3(signature = (start = None, step = None, dtype = None))]
fn range_(
    start: Option<Bound<'_, PyAny>>,
    step: Option<Bound<'_, PyAny>>,
    dtype: Option<&str>,
) -> PyResult<Dist> {
    let py_kind = match (&start, &step) {
        (Some(v), _) | (None, Some(v)) => Kind::pick(v, dtype)?,
        (None, None) => dtype.map_or(Ok(Kind::I64), Kind::named)?,
    };
    macro_rules! num {
        ($t:ty, $zero:expr, $one:expr) => {{
            let a: $t = start.as_ref().map_or(Ok($zero), |v| v.extract())?;
            let s: $t = step.as_ref().map_or(Ok($one), |v| v.extract())?;
            (a, s)
        }};
    }
    Ok(match py_kind {
        Kind::I64 => {
            let (a, s) = num!(i64, 0, 1);
            Dist::from_dist(range(a, s))
        }
        Kind::I32 => {
            let (a, s) = num!(i32, 0, 1);
            Dist::from_dist(range(a, s))
        }
        Kind::F64 => {
            let (a, s) = num!(f64, 0.0, 1.0);
            Dist::from_dist(range(a, s))
        }
        Kind::Date => {
            let a = start.as_ref().map_or(Ok(0), days)?;
            let s: i32 = step.as_ref().map_or(Ok(1), |v| v.extract())?;
            AnyDist::I32(BoxDist::new(range(a, s)))
                .date()
                .expect("int32")
                .into()
        }
        k => return Err(PyTypeError::new_err(format!("range: no {k:?} range"))),
    })
}

/// The same value in every row.
#[pyfunction(name = "constant")]
#[pyo3(signature = (value, dtype = None))]
fn constant_(value: Bound<'_, PyAny>, dtype: Option<&str>) -> PyResult<Dist> {
    Ok(match Kind::pick(&value, dtype)? {
        Kind::I32 => Dist::from_dist(constant(value.extract::<i32>()?)),
        Kind::I64 => Dist::from_dist(constant(value.extract::<i64>()?)),
        Kind::F64 => Dist::from_dist(constant(value.extract::<f64>()?)),
        Kind::Bool => Dist::from_dist(constant(value.extract::<bool>()?)),
        Kind::Date => Dist::from_dist(constant(Date32(days(&value)?))),
        Kind::Str => {
            let s: &'static str = String::leak(value.extract()?);
            Dist::from_dist(Owned(constant(s)))
        }
    })
}

/// Uniform over `lo..=hi`: integers, floats or dates.
#[pyfunction(name = "uniform")]
#[pyo3(signature = (lo, hi, dtype = None))]
fn uniform_(lo: Bound<'_, PyAny>, hi: Bound<'_, PyAny>, dtype: Option<&str>) -> PyResult<Dist> {
    let kind = Kind::pick(&lo, dtype)?;
    macro_rules! u {
        ($t:ty) => {{
            let (a, b): ($t, $t) = (lo.extract()?, hi.extract()?);
            if a > b {
                return Err(PyValueError::new_err("uniform: lo must be <= hi"));
            }
            Dist::from_dist(uniform(a, b))
        }};
    }
    Ok(match kind {
        Kind::I32 => u!(i32),
        Kind::I64 => u!(i64),
        Kind::F64 => u!(f64),
        Kind::Date => {
            let (a, b) = (days(&lo)?, days(&hi)?);
            if a > b {
                return Err(PyValueError::new_err("uniform: lo must be <= hi"));
            }
            AnyDist::I32(BoxDist::new(uniform(a, b)))
                .date()
                .expect("int32")
                .into()
        }
        k => return Err(PyTypeError::new_err(format!("uniform: no {k:?} range"))),
    })
}

/// A uniform pick from `values`, or weighted by `weights`.
#[pyfunction(name = "choose")]
#[pyo3(signature = (values, weights = None, dtype = None))]
fn choose_(
    values: Vec<Bound<'_, PyAny>>,
    weights: Option<Vec<u32>>,
    dtype: Option<&str>,
) -> PyResult<Dist> {
    let first = values
        .first()
        .ok_or_else(|| PyValueError::new_err("choose: no values"))?;
    if let Some(w) = &weights {
        if w.len() != values.len() {
            return Err(PyValueError::new_err("choose: one weight per value"));
        }
        if w.iter().all(|&w| w == 0) {
            return Err(PyValueError::new_err("choose: every weight is 0"));
        }
    }
    let v = &values;
    Ok(match Kind::pick(first, dtype)? {
        Kind::I32 => pick(self::values(v, |x| x.extract::<i32>())?, &weights).into(),
        Kind::I64 => pick(self::values(v, |x| x.extract::<i64>())?, &weights).into(),
        Kind::F64 => pick(self::values(v, |x| x.extract::<f64>())?, &weights).into(),
        Kind::Bool => pick(self::values(v, |x| x.extract::<bool>())?, &weights).into(),
        Kind::Date => pick(self::values(v, |x| days(x).map(Date32))?, &weights).into(),
        Kind::Str => {
            let s = self::values(v, |x| Ok(String::leak(x.extract::<String>()?) as &str))?;
            let d = match &weights {
                None => BoxDist::new(Owned(choose(leak(s)))),
                Some(w) => BoxDist::new(Owned(weighted(leak(
                    s.into_iter().zip(w.iter().copied()).collect(),
                )))),
            };
            AnyDist::Str(d)
        }
    }
    .into())
}

/// `min..=max` words from `words`, space separated.
#[pyfunction(name = "text")]
fn text_(words: Vec<String>, min: usize, max: usize) -> PyResult<Dist> {
    if words.is_empty() || min > max {
        return Err(PyValueError::new_err("text: needs words and min <= max"));
    }
    let words = leak(words.into_iter().map(|w| &*String::leak(w)).collect());
    Ok(Dist::from_dist(text(words, min, max)))
}

/// `min..=max` characters drawn from an ASCII `alphabet`.
#[pyfunction(name = "chars")]
fn chars_(alphabet: String, min: usize, max: usize) -> PyResult<Dist> {
    if alphabet.is_empty() || !alphabet.is_ascii() || min > max {
        return Err(PyValueError::new_err(
            "chars: needs a non-empty ASCII alphabet and min <= max",
        ));
    }
    Ok(Dist::from_dist(chars(
        String::leak(alphabet).as_bytes(),
        min,
        max,
    )))
}

/// A list per row: its length from `lengths` (integers), its elements
/// from `values` (int64, float64 or string).
#[pyfunction(name = "list")]
fn list_(lengths: &Dist, values: &Dist) -> PyResult<Dist> {
    let kind = lengths.inner.type_name();
    let lengths = lengths.inner.clone().lengths().ok_or_else(|| {
        PyTypeError::new_err(format!("list: lengths must be integers, not {kind}"))
    })?;
    Ok(match values.inner.clone() {
        AnyDist::I64(v) => Dist::from_dist(list(lengths, v)),
        AnyDist::F64(v) => Dist::from_dist(list(lengths, v)),
        AnyDist::Str(v) => Dist::from_dist(list(lengths, v)),
        v => {
            return Err(PyTypeError::new_err(format!(
                "list: no lists of {}",
                v.type_name()
            )));
        }
    })
}

/// Normally distributed floats. A distribution written in this crate
/// (`populate_py::dynamic::Normal`), exposed in one line.
#[pyfunction]
#[pyo3(signature = (mean = 0.0, std = 1.0))]
fn normal(mean: f64, std: f64) -> Dist {
    Dist::from_dist(Normal { mean, std })
}

// ============================================================================
// Source
// ============================================================================

type SharedChunks = Arc<dyn Chunks + Send + Sync>;

/// Lets `&impl Chunks` drivers take a shared trait object.
struct Shared(SharedChunks);

impl Chunks for Shared {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }
    fn num_chunks(&self) -> u64 {
        self.0.num_chunks()
    }
    fn chunk(&self, i: u64, emit: &mut dyn FnMut(RecordBatch)) {
        self.0.chunk(i, emit)
    }
}

/// A table: its batches are generated on `threads` threads (0 = all
/// cores) each time it is read, the same rows every time.
///
/// `pyarrow.table(src)`, `pyarrow.RecordBatchReader.from_stream(src)`,
/// `polars.DataFrame(src)` and DuckDB read it through the Arrow C stream
/// interface; `write_parquet` writes it with row groups encoded on the
/// workers.
#[pyclass(frozen, module = "populate")]
pub struct Source {
    chunks: SharedChunks,
    props: fn() -> WriterPropertiesBuilder,
    #[pyo3(get)]
    threads: usize,
}

fn snappy() -> WriterPropertiesBuilder {
    WriterProperties::builder().set_compression(Compression::SNAPPY)
}

impl Source {
    /// Any [`Chunks`] as a Python `Source`, written to Parquet with Snappy.
    pub fn new(chunks: impl Chunks + Send + 'static) -> Self {
        Source {
            chunks: Arc::new(chunks),
            props: snappy,
            threads: 0,
        }
    }

    /// Parquet writer properties to start from (a generator's own).
    pub fn with_props(mut self, props: fn() -> WriterPropertiesBuilder) -> Self {
        self.props = props;
        self
    }

    /// Batches as they are made, in order, with the workers kept a few
    /// chunks ahead of the reader.
    fn reader(&self) -> BatchReader {
        let (tx, rx) = sync_channel(2 * populate_table::resolve_threads(self.threads).max(1));
        let src = Shared(self.chunks.clone());
        let threads = self.threads;
        let worker = std::thread::spawn(move || {
            // a send fails once the reader is gone: stop generating
            let _ = try_for_each_batch(&src, threads, |b| tx.send(b));
        });
        BatchReader {
            schema: self.chunks.schema(),
            rx,
            worker: Some(worker),
        }
    }
}

#[pymethods]
impl Source {
    /// The same table generated on `threads` threads (0 = all cores).
    fn with_threads(&self, threads: usize) -> Source {
        Source {
            chunks: self.chunks.clone(),
            props: self.props,
            threads,
        }
    }

    /// The Arrow schema (`pyarrow.schema(src.schema)`).
    #[getter]
    fn schema(&self) -> ArrowSchema {
        ArrowSchema(self.chunks.schema())
    }

    /// Chunks (Parquet row groups) the table is made in.
    #[getter]
    fn num_chunks(&self) -> u64 {
        self.chunks.num_chunks()
    }

    /// The Arrow PyCapsule stream interface. `requested_schema` is
    /// ignored, as the protocol allows: the schema is the table's.
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let stream = FFI_ArrowArrayStream::new(Box::new(self.reader()));
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }

    /// Write the table to one Parquet file at `path`, one row group per
    /// chunk. `compression` is `snappy`, `none`, or `None` for the
    /// source's default.
    #[pyo3(signature = (path, compression = None))]
    fn write_parquet(
        &self,
        py: Python<'_>,
        path: std::path::PathBuf,
        compression: Option<&str>,
    ) -> PyResult<()> {
        let mut props = (self.props)();
        if let Some(c) = compression {
            props = props.set_compression(match c {
                "snappy" => Compression::SNAPPY,
                "none" | "uncompressed" => Compression::UNCOMPRESSED,
                _ => return Err(PyValueError::new_err(format!("unknown compression `{c}`"))),
            });
        }
        let props = props.build();
        let file = File::create(&path).map_err(|e| PyIOError::new_err(e.to_string()))?;
        let src = Shared(self.chunks.clone());
        let threads = self.threads;
        py.detach(move || write_parquet(&src, file, threads, Some(props)))
            .map(|_| ())
            .map_err(|e| PyIOError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        let fields: Vec<String> = self
            .chunks
            .schema()
            .fields()
            .iter()
            .map(|f| format!("{}: {}", f.name(), f.data_type()))
            .collect();
        format!("Source({})", fields.join(", "))
    }
}

/// The batches of a [`Source`] from its worker thread.
struct BatchReader {
    schema: SchemaRef,
    rx: Receiver<RecordBatch>,
    worker: Option<JoinHandle<()>>,
}

impl Iterator for BatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(b) = self.rx.recv() {
            return Some(Ok(b));
        }
        // the worker hung up: finished, or a generator panicked
        let panic = self.worker.take()?.join().err()?;
        let msg = panic
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "generator panicked".into());
        Some(Err(ArrowError::ExternalError(msg.into())))
    }
}

impl RecordBatchReader for BatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// `rows` rows of the named columns in `columns` (a dict of `Dist`, in
/// order). The output depends on `seed` and `batch_rows`, not on the
/// thread count.
#[pyfunction]
#[pyo3(signature = (rows, columns, seed = 0, chunk_rows = None, batch_rows = None))]
fn table(
    rows: u64,
    columns: &Bound<'_, PyDict>,
    seed: u64,
    chunk_rows: Option<u64>,
    batch_rows: Option<u64>,
) -> PyResult<Source> {
    let cols = columns
        .iter()
        .map(|(k, v)| {
            Ok((
                k.extract::<String>()?,
                v.cast::<Dist>()?.get().inner.clone(),
            ))
        })
        .collect::<PyResult<Vec<_>>>()?;
    if cols.is_empty() {
        return Err(PyValueError::new_err("table: no columns"));
    }
    let mut t = DynTable::new(cols, rows, seed);
    if let Some(n) = chunk_rows.filter(|&n| n > 0) {
        t = t.chunk_rows(n);
    }
    if let Some(n) = batch_rows.filter(|&n| n > 0) {
        t = t.batch_rows(n);
    }
    Ok(Source::new(t))
}

// ============================================================================
// Generators
// ============================================================================

/// TPC-H table `table` (`lineitem`, `orders`, ...) at scale factor `sf`,
/// byte-identical to dbgen.
#[cfg(feature = "tpch")]
#[pyfunction]
#[pyo3(signature = (table, sf = 1.0))]
fn tpch(table: &str, sf: f64) -> PyResult<Source> {
    use populate_tpch::{MAX_SCALE_FACTOR, TableName, Tpch, parquet_props};
    if !(sf > 0.0 && sf <= MAX_SCALE_FACTOR) {
        return Err(PyValueError::new_err(format!(
            "sf must be in (0, {MAX_SCALE_FACTOR}]"
        )));
    }
    let t: TableName = table.parse().map_err(PyValueError::new_err)?;
    Ok(Source::new(Tpch::new(sf).table(t)).with_props(parquet_props))
}

/// SSB table `table` (`lineorder`, `lineorder_flat`, ...) at scale factor
/// `sf`, byte-identical to ssb-dbgen.
#[cfg(feature = "ssb")]
#[pyfunction]
#[pyo3(signature = (table, sf = 1.0))]
fn ssb(table: &str, sf: f64) -> PyResult<Source> {
    use populate_ssb::{MAX_SCALE_FACTOR, Ssb, TableName, parquet_props};
    if !(sf > 0.0 && sf <= MAX_SCALE_FACTOR) {
        return Err(PyValueError::new_err(format!(
            "sf must be in (0, {MAX_SCALE_FACTOR}]"
        )));
    }
    let t: TableName = table.parse().map_err(PyValueError::new_err)?;
    Ok(Source::new(Ssb::new(sf).table(t)).with_props(parquet_props))
}

// ============================================================================
// Arrow C data interface
// ============================================================================

/// An Arrow schema (`__arrow_c_schema__`).
#[pyclass(frozen, module = "populate")]
pub struct ArrowSchema(SchemaRef);

#[pymethods]
impl ArrowSchema {
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi = FFI_ArrowSchema::try_from(self.0.as_ref())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        PyCapsule::new_with_value(py, ffi, c"arrow_schema")
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0.fields())
    }
}

/// An Arrow array (`__arrow_c_array__`).
#[pyclass(frozen, module = "populate")]
pub struct ArrowArray(ArrayRef);

#[pymethods]
impl ArrowArray {
    #[pyo3(signature = (requested_schema = None))]
    fn __arrow_c_array__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyCapsule>, Bound<'py, PyCapsule>)> {
        let _ = requested_schema;
        let (array, schema) =
            to_ffi(&self.0.to_data()).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok((
            PyCapsule::new_with_value(py, schema, c"arrow_schema")?,
            PyCapsule::new_with_value(py, array, c"arrow_array")?,
        ))
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __repr__(&self) -> String {
        format!("ArrowArray({}, len={})", self.0.data_type(), self.0.len())
    }
}

#[pymodule]
fn populate(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Dist>()?;
    m.add_class::<Source>()?;
    m.add_class::<ArrowSchema>()?;
    m.add_class::<ArrowArray>()?;
    m.add_function(wrap_pyfunction!(range_, m)?)?;
    m.add_function(wrap_pyfunction!(constant_, m)?)?;
    m.add_function(wrap_pyfunction!(uniform_, m)?)?;
    m.add_function(wrap_pyfunction!(choose_, m)?)?;
    m.add_function(wrap_pyfunction!(text_, m)?)?;
    m.add_function(wrap_pyfunction!(chars_, m)?)?;
    m.add_function(wrap_pyfunction!(list_, m)?)?;
    m.add_function(wrap_pyfunction!(normal, m)?)?;
    m.add_function(wrap_pyfunction!(table, m)?)?;
    #[cfg(feature = "tpch")]
    m.add_function(wrap_pyfunction!(tpch, m)?)?;
    #[cfg(feature = "ssb")]
    m.add_function(wrap_pyfunction!(ssb, m)?)?;
    Ok(())
}
