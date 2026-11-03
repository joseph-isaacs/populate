//! Named, nullable columns with Arrow and `data.table::fwrite`-compatible
//! CSV output.

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema};
use populate_2::arrow::IntoArrowArray;
use std::io::{self, Write};
use std::iter::repeat_n;
use std::sync::Arc;

/// One nullable column.
#[derive(Clone, Debug, PartialEq)]
pub enum Column {
    Int(Vec<Option<i32>>),
    Float(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Int(v) => v.len(),
            Column::Float(v) => v.len(),
            Column::Str(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_array(self) -> ArrayRef {
        match self {
            Column::Int(v) => v.into_arrow_array(),
            Column::Float(v) => v.into_arrow_array(),
            Column::Str(v) => v.into_arrow_array(),
        }
    }

    /// Append row `i` as fwrite would print it (NA is an empty field).
    fn write_cell(&self, i: usize, out: &mut String) {
        match self {
            Column::Int(v) => {
                if let Some(x) = v[i] {
                    out.push_str(&x.to_string());
                }
            }
            Column::Float(v) => {
                if let Some(x) = v[i] {
                    fwrite_f64(x, out);
                }
            }
            Column::Str(v) => {
                if let Some(s) = &v[i] {
                    out.push_str(s);
                }
            }
        }
    }
}

/// An ordered set of equally long named columns.
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    pub columns: Vec<(String, Column)>,
}

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
    pub fn write_csv(&self, w: &mut impl Write) -> io::Result<()> {
        let mut line = self.names().join(",");
        line.push('\n');
        w.write_all(line.as_bytes())?;
        for i in 0..self.num_rows() {
            line.clear();
            for (j, (_, col)) in self.columns.iter().enumerate() {
                if j > 0 {
                    line.push(',');
                }
                col.write_cell(i, &mut line);
            }
            line.push('\n');
            w.write_all(line.as_bytes())?;
        }
        Ok(())
    }
}

/// fwrite's double format: 15 significant digits with trailing zeros
/// dropped, in fixed notation unless scientific (`1e-04`, `1.5e+15`) is
/// strictly shorter.
pub fn fwrite_f64(x: f64, out: &mut String) {
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
    let sci = format!("{:.14e}", x.abs());
    let (mant, exp) = sci.split_once('e').expect("exponent");
    let exp: i32 = exp.parse().expect("exponent");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_end_matches('0');
    let s = digits.len() as i32;

    let fixed_width = if exp >= 0 {
        (exp + 1).max(s) + i32::from(s > exp + 1)
    } else {
        2 + (-exp - 1) + s
    };
    let exp_digits = exp.abs().to_string();
    let sci_width = s + i32::from(s > 1) + 2 + (exp_digits.len() as i32).max(2);

    if sci_width < fixed_width {
        out.push_str(&digits[..1]);
        if s > 1 {
            out.push('.');
            out.push_str(&digits[1..]);
        }
        out.push('e');
        out.push(if exp < 0 { '-' } else { '+' });
        if exp_digits.len() < 2 {
            out.push('0');
        }
        out.push_str(&exp_digits);
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
