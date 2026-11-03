//! Named, nullable columns in columnar layout (populate-2's [`NullableCol`]
//! over value buffers or dictionary codes), with Arrow and
//! `data.table::fwrite`-compatible CSV output.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema};
use populate_2::arrow::IntoArrowArray;
use populate_2::column::{DictCol, NullableCol, StrCol};
use std::fmt::Write as _;
use std::io::{self, Write};
use std::iter::repeat_n;
use std::str::from_utf8;
use std::sync::Arc;

/// One nullable column.
#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Int(NullableCol<Vec<i32>>),
    Float(NullableCol<Vec<f64>>),
    /// Dictionary-encoded strings: a code per row into one label list.
    Dict(NullableCol<DictCol<String>>),
    /// `prefix` followed by the integer (`"id" + id1`), formatted on output.
    Prefixed(&'static str, NullableCol<Vec<i32>>),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Int(v) | Column::Prefixed(_, v) => v.len(),
            Column::Float(v) => v.len(),
            Column::Dict(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Row `i` as a string, `None` if null (for tests and inspection).
    pub fn cell(&self, i: usize) -> Option<String> {
        let mut out = String::new();
        let mut scratch = String::new();
        self.write_cell(i, &mut out, &mut scratch).then_some(out)
    }

    /// Dictionary columns stay dictionary-encoded; prefixed integers become
    /// a string column.
    pub fn into_array(self) -> ArrayRef {
        match self {
            Column::Int(v) => v.into_arrow_array(),
            Column::Float(v) => v.into_arrow_array(),
            Column::Dict(v) => v.into_arrow_array(),
            Column::Prefixed(prefix, v) => {
                let mut values = StrCol::with_capacity(v.len(), v.len() * (prefix.len() + 8));
                let mut buf = String::new();
                for &x in &v.values {
                    buf.clear();
                    buf.push_str(prefix);
                    push_int(&mut buf, x);
                    values.push_str(&buf);
                }
                NullableCol {
                    values,
                    validity: v.validity,
                }
                .into_arrow_array()
            }
        }
    }

    /// Append row `i` as fwrite would print it; returns `false` (and appends
    /// nothing) for NA, which fwrite writes as an empty field.
    fn write_cell(&self, i: usize, out: &mut String, scratch: &mut String) -> bool {
        match self {
            Column::Int(v) => {
                let valid = v.is_valid(i);
                if valid {
                    push_int(out, v.values[i]);
                }
                valid
            }
            Column::Float(v) => {
                let valid = v.is_valid(i);
                if valid {
                    fwrite_f64_with(v.values[i], out, scratch);
                }
                valid
            }
            Column::Dict(v) => {
                let valid = v.is_valid(i);
                if valid {
                    out.push_str(v.values.get(i));
                }
                valid
            }
            Column::Prefixed(prefix, v) => {
                let valid = v.is_valid(i);
                if valid {
                    out.push_str(prefix);
                    push_int(out, v.values[i]);
                }
                valid
            }
        }
    }
}

/// Decimal digits of `x`, without allocating.
fn push_int(out: &mut String, x: i32) {
    let mut digits = [0u8; 11];
    let mut pos = digits.len();
    let mut v = x.unsigned_abs();
    loop {
        pos -= 1;
        digits[pos] = b'0' + (v % 10) as u8;
        v /= 10;
        if v == 0 {
            break;
        }
    }
    if x < 0 {
        out.push('-');
    }
    out.push_str(from_utf8(&digits[pos..]).expect("ASCII digits"));
}

/// An ordered set of equally long named columns.
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub columns: Vec<(String, Column)>,
}

/// Bytes buffered before each write to the output.
const CSV_CHUNK: usize = 1 << 20;

impl Table {
    pub fn num_rows(&self) -> usize {
        self.columns.first().map_or(0, |(_, c)| c.len())
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|(n, _)| n == name).map(|(_, c)| c)
    }

    pub fn names(&self) -> Vec<&str> {
        self.columns.iter().map(|(n, _)| n.as_str()).collect()
    }

    pub fn into_record_batch(self) -> Result<RecordBatch, ArrowError> {
        let (fields, arrays): (Vec<Field>, Vec<ArrayRef>) = self
            .columns
            .into_iter()
            .map(|(name, col)| {
                let a = col.into_array();
                (Field::new(name, a.data_type().clone(), true), a)
            })
            .unzip();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
    }

    /// Write as CSV byte-for-byte like `data.table::fwrite(DT, file)`.
    /// Rows are formatted straight from the column buffers into one reused
    /// buffer, flushed every [`CSV_CHUNK`] bytes.
    pub fn write_csv(&self, w: &mut impl Write) -> io::Result<()> {
        let mut buf = String::with_capacity(CSV_CHUNK + 4096);
        let mut scratch = String::new();
        buf.push_str(&self.names().join(","));
        buf.push('\n');
        for i in 0..self.num_rows() {
            for (j, (_, col)) in self.columns.iter().enumerate() {
                if j > 0 {
                    buf.push(',');
                }
                col.write_cell(i, &mut buf, &mut scratch);
            }
            buf.push('\n');
            if buf.len() >= CSV_CHUNK {
                w.write_all(buf.as_bytes())?;
                buf.clear();
            }
        }
        w.write_all(buf.as_bytes())
    }
}

/// fwrite's double format: 15 significant digits with trailing zeros
/// dropped, in fixed notation unless scientific (`1e-04`, `1.5e+15`) is
/// strictly shorter.
pub fn fwrite_f64(x: f64, out: &mut String) {
    fwrite_f64_with(x, out, &mut String::new());
}

/// [`fwrite_f64`] with a caller-owned scratch buffer (no allocation per call).
fn fwrite_f64_with(x: f64, out: &mut String, scratch: &mut String) {
    if x == 0.0 {
        out.push('0');
        return;
    }
    if !x.is_finite() {
        out.push_str(if x.is_nan() {
            ""
        } else if x > 0.0 {
            "Inf"
        } else {
            "-Inf"
        });
        return;
    }
    if x < 0.0 {
        out.push('-');
    }
    // d.dddddddddddddde±x, correctly rounded to 15 significant digits.
    scratch.clear();
    write!(scratch, "{:.14e}", x.abs()).expect("write to String");
    let (mant, exp) = scratch.split_once('e').expect("exponent");
    let exp: i32 = exp.parse().expect("exponent");
    // mantissa "d.dddddddddddddd": 15 significant digits around the point
    let mut digit_buf = [0u8; 15];
    let mut n = 0;
    for &b in mant.as_bytes() {
        if b != b'.' {
            digit_buf[n] = b;
            n += 1;
        }
    }
    let digits = from_utf8(&digit_buf[..n])
        .expect("ASCII digits")
        .trim_end_matches('0');
    let s = digits.len() as i32;

    let fixed_width = if exp >= 0 {
        (exp + 1).max(s) + i32::from(s > exp + 1)
    } else {
        2 + (-exp - 1) + s
    };
    let exp_abs = exp.unsigned_abs();
    let exp_len = if exp_abs >= 100 {
        3
    } else if exp_abs >= 10 {
        2
    } else {
        1
    };
    let sci_width = s + i32::from(s > 1) + 2 + exp_len.max(2);

    if sci_width < fixed_width {
        out.push_str(&digits[..1]);
        if s > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        if exp_len < 2 {
            out.push('0');
        }
        push_int(out, exp_abs as i32);
    } else if exp >= 0 {
        let int_len = (exp + 1) as usize;
        if digits.len() <= int_len {
            out.push_str(digits);
            out.extend(repeat_n('0', int_len - digits.len()));
        } else {
            out.push_str(&digits[..int_len]);
            out.push('.');
            out.push_str(&digits[int_len..]);
        }
    } else {
        out.push_str("0.");
        out.extend(repeat_n('0', (-exp - 1) as usize));
        out.push_str(digits);
    }
}
