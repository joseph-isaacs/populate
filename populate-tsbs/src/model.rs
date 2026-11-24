//! What a simulator emits: [`Point`]s, plus the [`Statics`] they refer to
//! (measurement names, field and tag keys, each host's tag values).

use std::sync::OnceLock;

/// A field value, as TSBS's `interface{}` holds it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Val {
    /// Cleared by the IoT simulator (`ClearFieldValue`).
    Nil,
    /// `int64`.
    I(i64),
    /// `float64`.
    F(f64),
}

/// The Go type of a field or tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    /// `int64`.
    I64,
    /// `float64`.
    F64,
    /// `float32` (IoT's numeric tags).
    F32,
    /// `string`.
    Str,
}

impl Type {
    /// `reflect.TypeOf(v).String()`.
    pub fn go_name(self) -> &'static str {
        match self {
            Type::I64 => "int64",
            Type::F64 => "float64",
            Type::F32 => "float32",
            Type::Str => "string",
        }
    }
}

/// A tag value of a host or truck.
#[derive(Debug, Clone, PartialEq)]
pub enum TagVal {
    /// `string`.
    Str(Box<str>),
    /// `float32`.
    F32(f32),
}

/// One measurement's layout: its name, the tags it adds after the host's,
/// and its fields in point order.
#[derive(Debug, Clone)]
pub struct Measurement {
    /// Measurement name (`cpu`, `readings`, ...).
    pub name: Box<str>,
    /// Keys of the tags this measurement appends after the generator's.
    pub extra_tags: Vec<Box<str>>,
    /// Field keys, in point order.
    pub fields: Vec<Box<str>>,
    /// Field types.
    pub types: Vec<Type>,
}

/// Everything about the points that doesn't change over time: the
/// generator-level tag keys and types, the measurements, and each
/// generator's tag values.
#[derive(Debug, Clone)]
pub struct Statics {
    /// The run's true start time, Unix nanoseconds (cassandra's date).
    pub start: i128,
    /// Tag keys every point starts with.
    pub tag_keys: Vec<Box<str>>,
    /// Their Go types (header lines).
    pub tag_types: Vec<Type>,
    /// Measurements, in emission order.
    pub measurements: Vec<Measurement>,
    /// Per generator (host or truck): its tag values.
    pub gen_tags: Vec<Vec<TagVal>>,
    /// Per generator, per measurement: the measurement's extra tag values.
    pub extra_tags: Vec<Vec<Vec<Box<str>>>>,
    /// Field keys for the header line, per measurement: devops-generic's
    /// header lists the host with the most metrics.
    pub header_fields: Vec<Vec<Box<str>>>,
    /// Each generator's tags, formatted once for the text serializers.
    pub tag_cache: OnceLock<TagCache>,
}

/// Per generator, its tags as the serializers write them when none is
/// cleared.
#[derive(Debug, Clone, Default)]
pub struct TagCache {
    /// `,k=v` for the string tags (influx: numeric tags go to the fields).
    pub kv_str: Vec<Box<[u8]>>,
    /// `,k=v` for every tag (timescaledb, cassandra).
    pub kv_all: Vec<Box<[u8]>>,
    /// `"k":"v"` for every tag, comma-separated (cratedb).
    pub json: Vec<Box<[u8]>>,
    /// Whether any generator tag is numeric.
    pub has_f32: bool,
    /// Per measurement, per field: `,k=`.
    pub field_kv: Vec<Vec<Box<[u8]>>>,
}

/// No zeroed tag.
pub const NO_TAG: u8 = u8::MAX;

/// One data point: measurement `meas` of generator `gen` at `ts`.
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    /// Index into [`Statics::measurements`].
    pub meas: u32,
    /// Host or truck.
    pub source: u32,
    /// Unix nanoseconds.
    pub ts: i64,
    /// Index of a generator tag cleared to nil, or [`NO_TAG`].
    pub zero_tag: u8,
    /// Field values (devops-generic hosts have fewer than the
    /// measurement's maximum).
    pub fields: Vec<Val>,
}

impl Default for Point {
    fn default() -> Self {
        Point {
            meas: 0,
            source: 0,
            ts: 0,
            zero_tag: NO_TAG,
            fields: Vec::new(),
        }
    }
}
