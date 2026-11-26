//! The `usertable` load as a populate distribution: a key column and as
//! many field columns as `fieldcount`, every value a function of its
//! record number, written straight into its `Utf8View` column.

use crate::ycsb::{RandomLayout, Ycsb, deterministic_value, java_hash};
use populate_combinators::views::{Str, Unshared, Views};
use populate_core::{Columns, Distribution, StrSink, Wide, WideCols};
use rand::Rng;
use std::sync::Arc;

/// A string (ASCII): every YCSB string is written per row.
pub(crate) type Text = Str<Unshared>;

/// A `Utf8View` column of [`Text`].
pub(crate) type TextCol = Views<Unshared>;

/// The `usertable` records: the key, then a column per field.
///
/// One distribution for the whole record rather than a column per field,
/// because deterministic values are built from the key: it is formatted
/// and hashed once per record.
#[derive(Clone)]
pub(crate) struct Records {
    pub(crate) load: Arc<Load>,
    pub(crate) next: u64,
}

/// What a load's records are made from.
pub(crate) struct Load {
    pub(crate) ycsb: Ycsb,
    pub(crate) names: Vec<String>,
    /// Where each field's ints are, for random values.
    pub(crate) layout: Option<RandomLayout>,
}

impl Distribution for Records {
    type Item = (Text, Wide<Text>);

    fn sample(&mut self, rng: &mut impl Rng) -> Self::Item {
        let mut c = self.columns();
        self.collect_into(&mut c, rng, 1);
        let fields = c.1.0.iter().map(|f| Text::from(f.get(0))).collect();
        (Text::from(c.0.get(0)), Wide(fields))
    }

    fn collect_into(
        &mut self,
        (keys, WideCols(fields)): &mut Columns<Self::Item>,
        _rng: &mut impl Rng,
        n: usize,
    ) {
        let load = &*self.load;
        let y = &load.ycsb;
        let len = y.field_length;
        keys.reserve_bytes(n, n * 24);
        for c in fields.iter_mut() {
            c.reserve_bytes(n, n * len);
        }
        let (mut key, mut buf) = (String::new(), Vec::new());
        for j in self.next..self.next + n as u64 {
            key.clear();
            y.push_key(y.insert_start + j, &mut key);
            keys.push_with(|s| s.push_str(&key));
            match &load.layout {
                Some(l) => {
                    for (f, c) in fields.iter_mut().enumerate() {
                        c.push_with(|s| s.push_ascii_blocks(len, l.blocks(y.tlr, j, f)));
                    }
                }
                // `buildDeterministicValue`: from the key and the field name
                None => {
                    let h = java_hash(0, key.as_bytes());
                    for (name, c) in load.names.iter().zip(fields.iter_mut()) {
                        buf.clear();
                        deterministic_value(key.as_bytes(), h, name.as_bytes(), len, &mut buf);
                        c.push_with(|s| s.push_ascii_bytes(&buf));
                    }
                }
            }
        }
        self.next += n as u64;
    }

    fn skip(&mut self, n: u64) {
        self.next += n;
    }

    fn column_names(&self, out: &mut Vec<String>) {
        out.push("ycsb_key".into());
        out.extend(self.load.names.iter().cloned());
    }

    fn columns(&self) -> Columns<Self::Item> {
        let fields = self.load.names.iter().map(|_| TextCol::default()).collect();
        (TextCol::default(), WideCols(fields))
    }
}
