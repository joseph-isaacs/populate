//! Influx line protocol exactly as TSBS's `pkg/targets/influx` serializer
//! writes it:
//!
//! ```text
//! measurement[,tag=value...] [nonstring_tag=v,...]field=v[,field=v...] <unix ns>\n
//! ```
//!
//! - `None` tags/fields are skipped (TSBS's cleared values);
//! - non-string tags are written as the first *fields* (TSBS "fake tags");
//! - ints get an `i` suffix; floats use Go's shortest `'f'` formatting;
//! - a point with no field written produces no line at all.

/// A field (or non-string tag) value.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FieldValue {
    Int(i64),
    Float(f64),
    Float32(f32),
}

/// Builds one line; discarded on [`finish`](Self::finish) if no field was written.
pub struct LineWriter<'a> {
    out: &'a mut String,
    start: usize,
    any_field: bool,
}

impl<'a> LineWriter<'a> {
    pub fn new(out: &'a mut String, measurement: &str) -> Self {
        let start = out.len();
        out.push_str(measurement);
        LineWriter {
            out,
            start,
            any_field: false,
        }
    }

    /// A string tag (`None` = cleared, skipped).
    pub fn tag(&mut self, key: &str, value: Option<&str>) {
        if let Some(v) = value {
            self.out.push(',');
            self.out.push_str(key);
            self.out.push('=');
            self.out.push_str(v);
        }
    }

    /// Start the field section; `has_keys` is whether the point has any field
    /// or non-string tag *keys* at all (TSBS writes the space even if all
    /// their values end up cleared).
    pub fn begin_fields(&mut self, has_keys: bool) {
        if has_keys {
            self.out.push(' ');
        }
    }

    /// A field or non-string tag (`None` = cleared, skipped).
    pub fn field(&mut self, key: &str, value: Option<FieldValue>) {
        let Some(v) = value else { return };
        if self.any_field {
            self.out.push(',');
        }
        self.any_field = true;
        self.out.push_str(key);
        self.out.push('=');
        match v {
            FieldValue::Int(i) => {
                self.out.push_str(&i.to_string());
                self.out.push('i');
            }
            FieldValue::Float(f) => self.out.push_str(&go_format_float(f)),
            FieldValue::Float32(f) => self.out.push_str(&go_format_float32(f)),
        }
    }

    pub fn finish(self, ts_ns: i64) {
        if !self.any_field {
            self.out.truncate(self.start);
            return;
        }
        self.out.push(' ');
        self.out.push_str(&ts_ns.to_string());
        self.out.push('\n');
    }
}

/// Go `strconv.FormatFloat(f, 'f', -1, 64)`: the shortest digits that
/// round-trip, no exponent, and — when two shortest candidates are equally
/// close (an exact decimal tie) — the even one.
///
/// Rust's `Display` finds the same *length* but may break such ties upward
/// (`77.72293090820313` vs Go's `77.72293090820312` for
/// `77.722930908203125`), so the digits are re-rounded at that length with
/// Rust's exact, half-to-even fixed-precision formatting.
pub fn go_format_float(f: f64) -> String {
    go_shortest(
        f,
        f.to_string(),
        |s| s.parse::<f64>().ok() == Some(f),
        |d| format!("{f:.d$}"),
    )
}

/// Go `strconv.FormatFloat(float64(f), 'f', -1, 32)`.
pub fn go_format_float32(f: f32) -> String {
    go_shortest(
        f64::from(f),
        f.to_string(),
        |s| s.parse::<f32>().ok() == Some(f),
        |d| format!("{f:.d$}"),
    )
}

fn go_shortest(
    f: f64,
    shortest: String,
    round_trips: impl Fn(&str) -> bool,
    fixed: impl Fn(usize) -> String,
) -> String {
    if f.is_nan() {
        return "NaN".into();
    }
    if f.is_infinite() {
        return if f > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    // Integers print as shortest digits padded with zeros (as Go does), so
    // only a fractional tail can need re-rounding.
    let Some((_, digits)) = shortest.split_once('.') else {
        return shortest;
    };
    let nearest = fixed(digits.len());
    if nearest != shortest && round_trips(&nearest) {
        nearest
    } else {
        shortest
    }
}
