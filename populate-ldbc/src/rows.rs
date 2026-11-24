//! Column buffers for the generated tables, and their schemas.

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use std::sync::Arc;

/// Logical type of a column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Epoch milliseconds, written as a timestamp.
    Ts,
    /// Epoch milliseconds of a midnight, written as a date.
    Date,
    I64,
    I32,
    Bool,
    Str,
    /// Nullable `i64`.
    OptI64,
    /// Nullable `i32`.
    OptI32,
    /// Nullable string.
    OptStr,
}

/// Column storage.
#[derive(Debug, Clone)]
pub enum Col {
    I64(Vec<i64>),
    I32(Vec<i32>),
    Bool(Vec<bool>),
    /// UTF-8 bytes and end offsets.
    Str(Vec<u8>, Vec<u32>),
    OptI64(Vec<i64>, Vec<bool>),
    OptI32(Vec<i32>, Vec<bool>),
    OptStr(Vec<u8>, Vec<u32>, Vec<bool>),
}

impl Col {
    fn new(k: Kind) -> Self {
        match k {
            Kind::Ts | Kind::Date | Kind::I64 => Col::I64(Vec::new()),
            Kind::I32 => Col::I32(Vec::new()),
            Kind::Bool => Col::Bool(Vec::new()),
            Kind::Str => Col::Str(Vec::new(), Vec::new()),
            Kind::OptI64 => Col::OptI64(Vec::new(), Vec::new()),
            Kind::OptI32 => Col::OptI32(Vec::new(), Vec::new()),
            Kind::OptStr => Col::OptStr(Vec::new(), Vec::new(), Vec::new()),
        }
    }

    fn clear(&mut self) {
        match self {
            Col::I64(v) => v.clear(),
            Col::I32(v) => v.clear(),
            Col::Bool(v) => v.clear(),
            Col::Str(d, o) => {
                d.clear();
                o.clear()
            }
            Col::OptI64(v, n) => {
                v.clear();
                n.clear()
            }
            Col::OptI32(v, n) => {
                v.clear();
                n.clear()
            }
            Col::OptStr(d, o, n) => {
                d.clear();
                o.clear();
                n.clear()
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Col::I64(v) => v.len(),
            Col::I32(v) => v.len(),
            Col::Bool(v) => v.len(),
            Col::Str(_, o) | Col::OptStr(_, o, _) => o.len(),
            Col::OptI64(v, _) => v.len(),
            Col::OptI32(v, _) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline(always)]
    pub fn i64(&mut self, x: i64) {
        match self {
            Col::I64(v) => v.push(x),
            _ => unreachable!("not an i64 column"),
        }
    }

    #[inline(always)]
    pub fn i32(&mut self, x: i32) {
        match self {
            Col::I32(v) => v.push(x),
            _ => unreachable!("not an i32 column"),
        }
    }

    #[inline(always)]
    pub fn bool(&mut self, x: bool) {
        match self {
            Col::Bool(v) => v.push(x),
            _ => unreachable!("not a bool column"),
        }
    }

    #[inline(always)]
    pub fn opt_i64(&mut self, x: Option<i64>) {
        match self {
            Col::OptI64(v, n) => {
                v.push(x.unwrap_or(0));
                n.push(x.is_some());
            }
            _ => unreachable!("not an optional i64 column"),
        }
    }

    #[inline(always)]
    pub fn opt_i32(&mut self, x: Option<i32>) {
        match self {
            Col::OptI32(v, n) => {
                v.push(x.unwrap_or(0));
                n.push(x.is_some());
            }
            _ => unreachable!("not an optional i32 column"),
        }
    }

    /// The byte buffer of a string column, to append one value to; finish
    /// it with [`Col::end_str`].
    #[inline(always)]
    pub fn str_buf(&mut self) -> &mut Vec<u8> {
        match self {
            Col::Str(d, _) | Col::OptStr(d, _, _) => d,
            _ => unreachable!("not a string column"),
        }
    }

    #[inline(always)]
    pub fn end_str(&mut self) {
        match self {
            Col::Str(d, o) => {
                o.push(u32::try_from(d.len()).expect("a chunk's string column is over 4 GiB"))
            }
            Col::OptStr(d, o, n) => {
                o.push(u32::try_from(d.len()).expect("a chunk's string column is over 4 GiB"));
                n.push(true);
            }
            _ => unreachable!("not a string column"),
        }
    }

    #[inline(always)]
    pub fn str(&mut self, s: &[u8]) {
        self.str_buf().extend_from_slice(s);
        self.end_str();
    }

    #[inline(always)]
    pub fn null_str(&mut self) {
        match self {
            Col::OptStr(d, o, n) => {
                o.push(u32::try_from(d.len()).expect("a chunk's string column is over 4 GiB"));
                n.push(false);
            }
            _ => unreachable!("not an optional string column"),
        }
    }
}

impl Col {
    /// The values of an `i64` column.
    pub fn i64s(&self) -> &[i64] {
        match self {
            Col::I64(v) => v,
            _ => unreachable!("not an i64 column"),
        }
    }

    /// The values of an `i32` column.
    pub fn i32s(&self) -> &[i32] {
        match self {
            Col::I32(v) => v,
            _ => unreachable!("not an i32 column"),
        }
    }

    /// The values of a bool column.
    pub fn bools(&self) -> &[bool] {
        match self {
            Col::Bool(v) => v,
            _ => unreachable!("not a bool column"),
        }
    }

    /// Row `r` of a string column (`None` if null).
    pub fn str_at(&self, r: usize) -> Option<&[u8]> {
        let at = |o: &[u32]| {
            let s = if r == 0 { 0 } else { o[r - 1] as usize };
            (s, o[r] as usize)
        };
        match self {
            Col::Str(d, o) => {
                let (a, b) = at(o);
                Some(&d[a..b])
            }
            Col::OptStr(d, o, n) => {
                let (a, b) = at(o);
                n[r].then(|| &d[a..b])
            }
            _ => unreachable!("not a string column"),
        }
    }

    /// Row `r` of an optional `i32` column.
    pub fn opt_i32_at(&self, r: usize) -> Option<i32> {
        match self {
            Col::OptI32(v, n) => n[r].then_some(v[r]),
            Col::I32(v) => Some(v[r]),
            _ => unreachable!("not an i32 column"),
        }
    }
}

impl Col {
    /// The values at rows `idx`, in that order.
    pub fn gather(&self, idx: &[u32]) -> Col {
        fn strs(d: &[u8], o: &[u32], idx: &[u32]) -> (Vec<u8>, Vec<u32>) {
            let (mut nd, mut no) = (Vec::new(), Vec::with_capacity(idx.len()));
            for &i in idx {
                let i = i as usize;
                let s = if i == 0 { 0 } else { o[i - 1] as usize };
                nd.extend_from_slice(&d[s..o[i] as usize]);
                no.push(u32::try_from(nd.len()).expect("a chunk's string column is over 4 GiB"));
            }
            (nd, no)
        }
        fn pick<T: Copy>(v: &[T], idx: &[u32]) -> Vec<T> {
            idx.iter().map(|&i| v[i as usize]).collect()
        }
        match self {
            Col::I64(v) => Col::I64(pick(v, idx)),
            Col::I32(v) => Col::I32(pick(v, idx)),
            Col::Bool(v) => Col::Bool(pick(v, idx)),
            Col::Str(d, o) => {
                let (d, o) = strs(d, o, idx);
                Col::Str(d, o)
            }
            Col::OptI64(v, n) => Col::OptI64(pick(v, idx), pick(n, idx)),
            Col::OptI32(v, n) => Col::OptI32(pick(v, idx), pick(n, idx)),
            Col::OptStr(d, o, n) => {
                let (d, o) = strs(d, o, idx);
                Col::OptStr(d, o, pick(n, idx))
            }
        }
    }

    /// Append `other`'s values (a column of the same kind).
    pub fn append(&mut self, other: &Col) {
        fn strs(d: &mut Vec<u8>, o: &mut Vec<u32>, od: &[u8], oo: &[u32]) {
            let base = u32::try_from(d.len()).expect("a chunk's string column is over 4 GiB");
            d.extend_from_slice(od);
            o.extend(oo.iter().map(|&e| {
                base.checked_add(e)
                    .expect("a chunk's string column is over 4 GiB")
            }));
        }
        match (self, other) {
            (Col::I64(a), Col::I64(b)) => a.extend_from_slice(b),
            (Col::I32(a), Col::I32(b)) => a.extend_from_slice(b),
            (Col::Bool(a), Col::Bool(b)) => a.extend_from_slice(b),
            (Col::Str(d, o), Col::Str(od, oo)) => strs(d, o, od, oo),
            (Col::OptI64(a, n), Col::OptI64(b, m)) => {
                a.extend_from_slice(b);
                n.extend_from_slice(m);
            }
            (Col::OptI32(a, n), Col::OptI32(b, m)) => {
                a.extend_from_slice(b);
                n.extend_from_slice(m);
            }
            (Col::OptStr(d, o, n), Col::OptStr(od, oo, m)) => {
                strs(d, o, od, oo);
                n.extend_from_slice(m);
            }
            _ => unreachable!("columns of different kinds"),
        }
    }
}

/// A table's columns for one chunk.
#[derive(Debug, Clone)]
pub struct Rows {
    pub cols: Vec<Col>,
}

impl Rows {
    pub fn new(def: &TableDef) -> Self {
        Rows {
            cols: def.cols.iter().map(|&(_, k)| Col::new(k)).collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.cols.first().map_or(0, Col::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&mut self) {
        for c in &mut self.cols {
            c.clear();
        }
    }
}

/// A table: its name (directory of the datagen's output) and columns.
#[derive(Debug)]
pub struct TableDef {
    pub name: &'static str,
    pub cols: &'static [(&'static str, Kind)],
}

impl TableDef {
    /// Arrow schema: timestamps as `Timestamp(ms, UTC)`, dates as `Date32`.
    pub fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .cols
            .iter()
            .map(|&(n, k)| {
                let (t, null) = match k {
                    Kind::Ts => (
                        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                        false,
                    ),
                    Kind::Date => (DataType::Date32, false),
                    Kind::I64 => (DataType::Int64, false),
                    Kind::I32 => (DataType::Int32, false),
                    Kind::Bool => (DataType::Boolean, false),
                    Kind::Str => (DataType::Utf8, false),
                    Kind::OptI64 => (DataType::Int64, true),
                    Kind::OptI32 => (DataType::Int32, true),
                    Kind::OptStr => (DataType::Utf8, true),
                };
                Field::new(n, t, null)
            })
            .collect();
        Arc::new(Schema::new(fields))
    }
}

use Kind::*;

pub const PERSON: TableDef = TableDef {
    name: "Person",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("id", I64),
        ("firstName", Str),
        ("lastName", Str),
        ("gender", Str),
        ("birthday", Date),
        ("locationIP", Str),
        ("browserUsed", Str),
        ("LocationCityId", I32),
        ("language", Str),
        ("email", Str),
    ],
};

pub const PERSON_HAS_INTEREST: TableDef = TableDef {
    name: "Person_hasInterest_Tag",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("PersonId", I64),
        ("TagId", I32),
    ],
};

pub const PERSON_STUDY_AT: TableDef = TableDef {
    name: "Person_studyAt_University",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("PersonId", I64),
        ("UniversityId", I64),
        ("classYear", I32),
    ],
};

pub const PERSON_WORK_AT: TableDef = TableDef {
    name: "Person_workAt_Company",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("PersonId", I64),
        ("CompanyId", I64),
        ("workFrom", I32),
    ],
};

pub const PERSON_KNOWS: TableDef = TableDef {
    name: "Person_knows_Person",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("Person1Id", I64),
        ("Person2Id", I64),
    ],
};

pub const FORUM: TableDef = TableDef {
    name: "Forum",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("id", I64),
        ("title", Str),
        ("ModeratorPersonId", I64),
    ],
};

pub const FORUM_HAS_TAG: TableDef = TableDef {
    name: "Forum_hasTag_Tag",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("ForumId", I64),
        ("TagId", I32),
    ],
};

pub const FORUM_HAS_MEMBER: TableDef = TableDef {
    name: "Forum_hasMember_Person",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("ForumId", I64),
        ("PersonId", I64),
    ],
};

pub const POST: TableDef = TableDef {
    name: "Post",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("id", I64),
        ("imageFile", OptStr),
        ("locationIP", Str),
        ("browserUsed", Str),
        ("language", OptStr),
        ("content", OptStr),
        ("length", I32),
        ("CreatorPersonId", I64),
        ("ContainerForumId", I64),
        ("LocationCountryId", I64),
    ],
};

pub const POST_HAS_TAG: TableDef = TableDef {
    name: "Post_hasTag_Tag",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("PostId", I64),
        ("TagId", I32),
    ],
};

pub const COMMENT: TableDef = TableDef {
    name: "Comment",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("id", I64),
        ("locationIP", Str),
        ("browserUsed", Str),
        ("content", Str),
        ("length", I32),
        ("CreatorPersonId", I64),
        ("LocationCountryId", I32),
        ("ParentPostId", OptI64),
        ("ParentCommentId", OptI64),
    ],
};

pub const COMMENT_HAS_TAG: TableDef = TableDef {
    name: "Comment_hasTag_Tag",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("CommentId", I64),
        ("TagId", I32),
    ],
};

pub const PERSON_LIKES_POST: TableDef = TableDef {
    name: "Person_likes_Post",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("PersonId", I64),
        ("PostId", I64),
    ],
};

pub const PERSON_LIKES_COMMENT: TableDef = TableDef {
    name: "Person_likes_Comment",
    cols: &[
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("explicitlyDeleted", Bool),
        ("PersonId", I64),
        ("CommentId", I64),
    ],
};

pub const ORGANISATION: TableDef = TableDef {
    name: "Organisation",
    cols: &[
        ("id", I32),
        ("type", Str),
        ("name", Str),
        ("url", Str),
        ("LocationPlaceId", I32),
    ],
};

pub const PLACE: TableDef = TableDef {
    name: "Place",
    cols: &[
        ("id", I32),
        ("name", Str),
        ("url", Str),
        ("type", Str),
        ("PartOfPlaceId", OptI32),
    ],
};

pub const TAG: TableDef = TableDef {
    name: "Tag",
    cols: &[
        ("id", I32),
        ("name", Str),
        ("url", Str),
        ("TypeTagClassId", I32),
    ],
};

pub const TAG_CLASS: TableDef = TableDef {
    name: "TagClass",
    cols: &[
        ("id", I32),
        ("name", Str),
        ("url", Str),
        ("SubclassOfTagClassId", OptI32),
    ],
};

/// The dynamic tables, in the order [`crate::Ldbc`] emits them.
pub const DYNAMIC: [&TableDef; 14] = [
    &PERSON,
    &PERSON_HAS_INTEREST,
    &PERSON_STUDY_AT,
    &PERSON_WORK_AT,
    &PERSON_KNOWS,
    &FORUM,
    &FORUM_HAS_TAG,
    &FORUM_HAS_MEMBER,
    &POST,
    &POST_HAS_TAG,
    &COMMENT,
    &COMMENT_HAS_TAG,
    &PERSON_LIKES_POST,
    &PERSON_LIKES_COMMENT,
];

/// The static tables.
pub const STATIC: [&TableDef; 4] = [&ORGANISATION, &PLACE, &TAG, &TAG_CLASS];

/// Indices into [`DYNAMIC`].
pub mod t {
    pub const PERSON: usize = 0;
    pub const HAS_INTEREST: usize = 1;
    pub const STUDY_AT: usize = 2;
    pub const WORK_AT: usize = 3;
    pub const KNOWS: usize = 4;
    pub const FORUM: usize = 5;
    pub const FORUM_TAG: usize = 6;
    pub const FORUM_MEMBER: usize = 7;
    pub const POST: usize = 8;
    pub const POST_TAG: usize = 9;
    pub const COMMENT: usize = 10;
    pub const COMMENT_TAG: usize = 11;
    pub const LIKES_POST: usize = 12;
    pub const LIKES_COMMENT: usize = 13;
}
