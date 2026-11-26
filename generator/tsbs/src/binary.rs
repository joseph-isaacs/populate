// The FlatBuffers Builder is modified from FlatBuffers v1.11.0 go/builder.go:
// translated to Rust and restricted to TSBS serialization operations.
// That portion is licensed under Apache-2.0; see ../NOTICE.md and
// ../LICENSE-Apache-2.0.txt.

//! `tsbs_generate_data`'s binary formats: Prometheus (length-prefixed
//! protobuf `TimeSeries`, one per field), SiriDB (qpack) and MongoDB
//! (flatbuffers).

use crate::model::{Point, Statics, TagVal, Val};
use crate::text::tags;
use std::cell::RefCell;
use std::mem::take;

/// The Prometheus file header: `uvarint(serializerVersion)`, written before
/// the first point.
pub const PROMETHEUS_HEADER: &[u8] = &[1];

fn uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn uvarint_len(v: u64) -> usize {
    (64 - (v | 1).leading_zeros() as usize).div_ceil(7)
}

/// A protobuf length-delimited field's size: `tag`, `len`, the body.
fn delimited_len(body: usize) -> usize {
    1 + uvarint_len(body as u64) + body
}

/// gogoproto's `Label{Name, Value}` body size; empty strings are left out.
fn label_body_len(name: &[u8], value: &[u8]) -> usize {
    let f = |s: &[u8]| {
        if s.is_empty() {
            0
        } else {
            delimited_len(s.len())
        }
    };
    f(name) + f(value)
}

/// A `TimeSeries.labels` entry.
fn label(out: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    out.push(0x0a);
    uvarint(out, label_body_len(name, value) as u64);
    for (tag, s) in [(0x0a, name), (0x12, value)] {
        if !s.is_empty() {
            out.push(tag);
            uvarint(out, s.len() as u64);
            out.extend_from_slice(s);
        }
    }
}

/// The point's tags as strings. Every tag of a use case that reaches here
/// is a string ([`crate::Format::check`]).
fn str_tags<'a>(st: &'a Statics, p: &'a Point) -> impl Iterator<Item = (&'a str, &'a str)> + 'a {
    tags(st, p).filter_map(|(k, v, extra)| match (v, extra) {
        (_, Some(s)) => Some((k, s)),
        (Some(TagVal::Str(s)), None) => Some((k, &**s)),
        _ => None,
    })
}

#[derive(Debug, Clone, Copy)]
enum Tag {
    Gen,
    Extra,
}

/// Prometheus labels: each generator tag's label encoded once, and per
/// measurement the tags in label-name order with `__name__`'s place.
#[derive(Debug, Clone)]
pub struct LabelCache {
    sources: Vec<Vec<Box<[u8]>>>,
    order: Vec<Vec<(Tag, usize)>>,
    name_at: Vec<usize>,
    /// Per measurement, per field: its `__name__` label.
    names: Vec<Vec<Box<[u8]>>>,
}

impl LabelCache {
    fn new(st: &Statics) -> Self {
        let sources = st
            .gen_tags
            .iter()
            .map(|tags| {
                st.tag_keys
                    .iter()
                    .zip(tags)
                    .map(|(k, v)| {
                        let mut b = Vec::new();
                        if let TagVal::Str(v) = v {
                            label(&mut b, k.as_bytes(), v.as_bytes());
                        }
                        b.into()
                    })
                    .collect()
            })
            .collect();
        let mut order = Vec::new();
        let mut name_at = Vec::new();
        for m in &st.measurements {
            let mut o: Vec<(&str, Tag, usize)> = st
                .tag_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (&**k, Tag::Gen, i))
                .chain(
                    m.extra_tags
                        .iter()
                        .enumerate()
                        .map(|(i, k)| (&**k, Tag::Extra, i)),
                )
                .collect();
            o.sort_by(|a, b| a.0.cmp(b.0));
            name_at.push(o.partition_point(|t| t.0 < "__name__"));
            order.push(o.into_iter().map(|(_, t, i)| (t, i)).collect());
        }
        let names = st
            .measurements
            .iter()
            .map(|m| {
                m.fields
                    .iter()
                    .map(|k| {
                        let mut b = Vec::new();
                        label(&mut b, b"__name__", k.as_bytes());
                        b.into()
                    })
                    .collect()
            })
            .collect();
        LabelCache {
            names,
            sources,
            order,
            name_at,
        }
    }
}

/// The prometheus serializer: per field, a `TimeSeries` with the tags and
/// `__name__ = field key` as labels sorted by name, and one sample
/// `(float64(value), UnixNano / 1e6)`, each prefixed by its uvarint length.
pub fn prometheus(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    let c = st.label_cache.get_or_init(|| LabelCache::new(st));
    let (g, mi) = (p.source as usize, p.meas as usize);
    let mut before = Vec::new();
    let mut after = Vec::new();
    for (i, &(tag, pos)) in c.order[mi].iter().enumerate() {
        let to = if i < c.name_at[mi] {
            &mut before
        } else {
            &mut after
        };
        match tag {
            Tag::Gen => to.extend_from_slice(&c.sources[g][pos]),
            Tag::Extra => {
                let k = &st.measurements[mi].extra_tags[pos];
                label(to, k.as_bytes(), st.extra_tags[g][mi][pos].as_bytes());
            }
        }
    }
    let ms = p.ts / 1_000_000;
    for (name, &v) in c.names[mi].iter().zip(&p.fields) {
        let f = match v {
            Val::I(i) => i as f64,
            v => v.as_f64().expect("IoT's nil fields are rejected by check"),
        };
        let sample = if f != 0.0 { 9 } else { 0 }
            + if ms != 0 {
                1 + uvarint_len(ms as u64)
            } else {
                0
            };
        let len = before.len() + name.len() + after.len() + delimited_len(sample);
        uvarint(out, len as u64);
        out.extend_from_slice(&before);
        out.extend_from_slice(name);
        out.extend_from_slice(&after);
        out.push(0x12);
        uvarint(out, sample as u64);
        if f != 0.0 {
            out.push(0x09);
            out.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        if ms != 0 {
            out.push(0x10);
            uvarint(out, ms as u64);
        }
    }
}

/// go-qpack's `packInt`.
fn qpack_int(out: &mut Vec<u8>, i: i64) {
    if (0..64).contains(&i) {
        out.push(i as u8);
    } else if (-60..0).contains(&i) {
        out.push((63 - i) as u8);
    } else if i8::try_from(i).is_ok() {
        out.push(0xe8);
        out.push(i as u8);
    } else if i16::try_from(i).is_ok() {
        out.push(0xe9);
        out.extend_from_slice(&(i as i16).to_le_bytes());
    } else if i32::try_from(i).is_ok() {
        out.push(0xea);
        out.extend_from_slice(&(i as i32).to_le_bytes());
    } else {
        out.push(0xeb);
        out.extend_from_slice(&i.to_le_bytes());
    }
}

/// go-qpack's `float64`.
fn qpack_f64(out: &mut Vec<u8>, f: f64) {
    if f == 0.0 {
        out.push(0x7e);
    } else if f == -1.0 {
        out.push(0x7d);
    } else if f == 1.0 {
        out.push(0x7f);
    } else {
        out.push(0xec);
        out.extend_from_slice(&f.to_bits().to_le_bytes());
    }
}

/// The siridb serializer: `u32 fields, u32 len, name|k=v,...`, then per
/// field `u32 len('|' key), u32 len(packed), |key, qpack([ts, value])`,
/// little-endian.
pub fn siridb(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; 8]);
    let m = &st.measurements[p.meas as usize];
    out.extend_from_slice(m.name.as_bytes());
    out.push(b'|');
    for (i, (k, v)) in str_tags(st, p).enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(k.as_bytes());
        out.push(b'=');
        out.extend_from_slice(v.as_bytes());
    }
    let name_len = out.len() - start - 8;
    let mut count = 0u32;
    for (k, &v) in m.fields.iter().zip(&p.fields) {
        out.extend_from_slice(&(1 + k.len() as u32).to_le_bytes());
        let len_at = out.len();
        out.extend_from_slice(&[0; 4]);
        out.push(b'|');
        out.extend_from_slice(k.as_bytes());
        let packed = out.len();
        out.push(0xed + 2);
        qpack_int(out, p.ts);
        match v {
            Val::Nil => out.push(0xfb),
            Val::I(i) => qpack_int(out, i),
            v => qpack_f64(out, v.as_f64().expect("a float")),
        }
        let n = (out.len() - packed) as u32;
        out[len_at..len_at + 4].copy_from_slice(&n.to_le_bytes());
        count += 1;
    }
    out[start..start + 4].copy_from_slice(&count.to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&(name_len as u32).to_le_bytes());
}

/// Go flatbuffers v1.11's `Builder`, ported line for line: it builds back
/// to front, aligns, and shares identical vtables, and every one of those
/// choices shows in the bytes.
#[derive(Default)]
struct Fb {
    bytes: Vec<u8>,
    head: usize,
    minalign: usize,
    vtable: Vec<u32>,
    object_end: u32,
    vtables: Vec<u32>,
    /// Scratch for `mongo`: table offsets.
    offsets: Vec<u32>,
}

impl Fb {
    fn reset(&mut self) {
        self.vtables.clear();
        self.vtable.clear();
        self.head = self.bytes.len();
        self.minalign = 1;
    }

    fn offset(&self) -> u32 {
        (self.bytes.len() - self.head) as u32
    }

    #[cold]
    fn grow(&mut self) {
        let old = self.bytes.len();
        let new = (old * 2).max(1);
        self.bytes.resize(new, 0);
        self.bytes.copy_within(..old, new - old);
        self.head += new - old;
    }

    #[inline]
    fn prep(&mut self, size: usize, additional: usize) {
        self.minalign = self.minalign.max(size);
        let align = (self.offset() as usize + additional).wrapping_neg() & (size - 1);
        while self.head <= align + size + additional {
            self.grow();
        }
        self.pad(align);
    }

    #[inline]
    fn pad(&mut self, n: usize) {
        // at most 7 bytes: a loop beats a memset call
        for _ in 0..n {
            self.head -= 1;
            self.bytes[self.head] = 0;
        }
    }

    #[inline]
    fn place(&mut self, b: &[u8]) {
        self.head -= b.len();
        self.bytes[self.head..self.head + b.len()].copy_from_slice(b);
    }

    fn prepend_soffset(&mut self, off: i32) {
        self.prep(4, 0);
        let v = self.offset() as i32 - off + 4;
        self.place(&v.to_le_bytes());
    }

    fn prepend_uoffset(&mut self, off: u32) {
        self.prep(4, 0);
        let v = self.offset() - off + 4;
        self.place(&v.to_le_bytes());
    }

    fn prepend_voffset(&mut self, v: u16) {
        self.prep(2, 0);
        self.place(&v.to_le_bytes());
    }

    fn start_vector(&mut self, elem: usize, n: usize, align: usize) {
        self.prep(4, elem * n);
        self.prep(align, elem * n);
    }

    fn end_vector(&mut self, n: usize) -> u32 {
        self.place(&(n as u32).to_le_bytes());
        self.offset()
    }

    fn string(&mut self, s: &[u8]) -> u32 {
        self.prep(4, s.len() + 1);
        self.place(&[0]);
        self.place(s);
        self.end_vector(s.len())
    }

    fn start_object(&mut self, n: usize) {
        self.vtable.clear();
        self.vtable.resize(n, 0);
        self.object_end = self.offset();
    }

    fn slot(&mut self, i: usize) {
        self.vtable[i] = self.offset();
    }

    fn uoffset_slot(&mut self, i: usize, x: u32) {
        if x != 0 {
            self.prepend_uoffset(x);
            self.slot(i);
        }
    }

    fn i64_slot(&mut self, i: usize, x: i64) {
        if x != 0 {
            self.prep(8, 0);
            self.place(&x.to_le_bytes());
            self.slot(i);
        }
    }

    fn f64_slot(&mut self, i: usize, x: f64) {
        if x != 0.0 {
            self.prep(8, 0);
            self.place(&x.to_bits().to_le_bytes());
            self.slot(i);
        }
    }

    fn vtable_equal(&self, object: u32, vt2: &[u8]) -> bool {
        if self.vtable.len() * 2 != vt2.len() {
            return false;
        }
        self.vtable.iter().enumerate().all(|(i, &a)| {
            let x = u16::from_le_bytes([vt2[2 * i], vt2[2 * i + 1]]);
            (x == 0 && a == 0) || i32::from(x) == object as i32 - a as i32
        })
    }

    /// `EndObject` (`WriteVtable`).
    fn end_object(&mut self) -> u32 {
        self.prepend_soffset(0);
        let object = self.offset();
        while self.vtable.last() == Some(&0) {
            self.vtable.pop();
        }
        let len = self.bytes.len();
        let existing = self.vtables.iter().rev().copied().find(|&vt| {
            let start = len - vt as usize;
            let vlen = u16::from_le_bytes([self.bytes[start], self.bytes[start + 1]]) as usize;
            self.vtable_equal(object, &self.bytes[start + 4..start + vlen])
        });
        match existing {
            None => {
                for i in (0..self.vtable.len()).rev() {
                    let v = self.vtable[i];
                    let off = if v != 0 { object - v } else { 0 };
                    self.prepend_voffset(off as u16);
                }
                self.prepend_voffset((object - self.object_end) as u16);
                self.prepend_voffset(((self.vtable.len() + 2) * 2) as u16);
                // after the writes: they may have grown the buffer
                let object_start = self.bytes.len() - object as usize;
                let v = self.offset() as i32 - object as i32;
                self.bytes[object_start..object_start + 4].copy_from_slice(&v.to_le_bytes());
                self.vtables.push(self.offset());
            }
            Some(vt) => {
                let object_start = len - object as usize;
                self.head = object_start;
                let v = vt as i32 - object as i32;
                self.bytes[object_start..object_start + 4].copy_from_slice(&v.to_le_bytes());
            }
        }
        self.vtable.clear();
        object
    }

    fn finish(&mut self, root: u32) -> &[u8] {
        self.prep(self.minalign, 4);
        self.prepend_uoffset(root);
        &self.bytes[self.head..]
    }
}

thread_local! {
    static FB: RefCell<Fb> = RefCell::new(Fb::default());
}

/// The mongo serializer: a `MongoPoint` flatbuffer (tags and fields each
/// built last to first, nil fields left out, values as `float64`) after
/// its little-endian `u64` length.
pub fn mongo(st: &Statics, p: &Point, out: &mut Vec<u8>) {
    FB.with_borrow_mut(|b| {
        b.reset();
        let mut offs = take(&mut b.offsets);
        offs.clear();
        let tags_kv: Vec<(&str, &str)> = str_tags(st, p).collect();
        for &(k, v) in tags_kv.iter().rev() {
            let key = b.string(k.as_bytes());
            let val = b.string(v.as_bytes());
            b.start_object(2);
            b.uoffset_slot(0, key);
            b.uoffset_slot(1, val);
            offs.push(b.end_object());
        }
        b.start_vector(4, offs.len(), 4);
        for &t in &offs {
            b.prepend_uoffset(t);
        }
        let tags_arr = b.end_vector(offs.len());
        offs.clear();
        let m = &st.measurements[p.meas as usize];
        for (k, &v) in m.fields.iter().zip(&p.fields).rev() {
            let f = match v {
                Val::Nil => continue,
                Val::I(i) => i as f64,
                v => v.as_f64().expect("a float"),
            };
            let key = b.string(k.as_bytes());
            b.start_object(2);
            b.uoffset_slot(0, key);
            b.f64_slot(1, f);
            offs.push(b.end_object());
        }
        b.start_vector(4, offs.len(), 4);
        for &f in &offs {
            b.prepend_uoffset(f);
        }
        let fields_arr = b.end_vector(offs.len());
        b.offsets = offs;
        let name = b.string(m.name.as_bytes());
        b.start_object(4);
        b.uoffset_slot(0, name);
        b.i64_slot(1, p.ts);
        b.uoffset_slot(2, tags_arr);
        b.uoffset_slot(3, fields_arr);
        let point = b.end_object();
        let buf = b.finish(point);
        out.extend_from_slice(&(buf.len() as u64).to_le_bytes());
        out.extend_from_slice(buf);
    });
}
