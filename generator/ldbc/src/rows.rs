//! The generated tables: their row types, which are the schema, and
//! [`Col`] / [`Rows`], the same columns held by kind for the transforms
//! (layouts, BI batches, factor tables) and the CSV writer.
//!
//! A dynamic table's row is a tuple: the Arrow schema of
//! [`crate::Ldbc::raw`] is read off these types and [`DYNAMIC_COLUMNS`]
//! (`populate_sink_arrow::schemas_of`), and [`DYNAMIC`] (the same tables
//! as [`TableDef`]s) is derived from them too.

use crate::java::MS_PER_DAY;
use arrow_array::ArrayRef;
use arrow_schema::{Field, Schema, SchemaRef};
use populate_core::{
    ChildCols, Children, Columns, Date32, Nullable, StrBuf, StrCol, Timestamp, Validity, time_unit,
};
use populate_sink_arrow::IntoArrowArray;
use std::borrow::Cow;
use std::sync::{Arc, LazyLock};

/// A timestamp: epoch milliseconds, UTC.
pub type Ts = Timestamp<{ time_unit::MILLISECOND }, true>;

/// `creationDate`, `deletionDate`, `explicitlyDeleted`: nested so a row
/// stays within a tuple's 12 fields (Arrow flattens it).
pub type Life = (Ts, Ts, bool);

pub type PersonRow = (
    Life,
    i64,
    String,
    String,
    String,
    Date32,
    String,
    String,
    i32,
    String,
    String,
);
/// `Person_hasInterest_Tag`, and every `..._hasTag_Tag`.
pub type TagEdgeRow = (Ts, Ts, i64, i32);
/// `Person_studyAt_University` and `Person_workAt_Company`.
pub type OrgEdgeRow = (Ts, Ts, i64, i64, i32);
/// `Person_knows_Person`, `Forum_hasMember_Person` and the likes.
pub type EdgeRow = (Ts, Ts, bool, i64, i64);
pub type ForumRow = (Ts, Ts, bool, i64, String, i64);
pub type PostRow = (
    Life,
    i64,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<String>,
    i32,
    i64,
    i64,
    i64,
);
pub type CommentRow = (
    Ts,
    Ts,
    bool,
    i64,
    String,
    String,
    String,
    i32,
    i64,
    i32,
    Option<i64>,
    Option<i64>,
);

/// One chunk of the 14 dynamic tables, in [`DYNAMIC_NAMES`] order: persons
/// and their edges, forums, then messages and likes.
pub type Raw = (
    (
        Children<PersonRow>,
        Children<TagEdgeRow>,
        Children<OrgEdgeRow>,
        Children<OrgEdgeRow>,
        Children<EdgeRow>,
    ),
    (Children<ForumRow>, Children<TagEdgeRow>, Children<EdgeRow>),
    (
        Children<PostRow>,
        Children<TagEdgeRow>,
        Children<CommentRow>,
        Children<TagEdgeRow>,
        Children<EdgeRow>,
        Children<EdgeRow>,
    ),
);

/// The columns of [`Raw`].
pub type RawCols = Columns<Raw>;

/// The tables of [`RawCols`] by name, for writers.
pub mod raw {
    use super::*;

    pub fn person(o: &mut RawCols) -> &mut Columns<PersonRow> {
        &mut o.0.0.values
    }
    pub fn has_interest(o: &mut RawCols) -> &mut Columns<TagEdgeRow> {
        &mut o.0.1.values
    }
    pub fn study_at(o: &mut RawCols) -> &mut Columns<OrgEdgeRow> {
        &mut o.0.2.values
    }
    pub fn work_at(o: &mut RawCols) -> &mut Columns<OrgEdgeRow> {
        &mut o.0.3.values
    }
    pub fn knows(o: &mut RawCols) -> &mut Columns<EdgeRow> {
        &mut o.0.4.values
    }
    pub fn forum(o: &mut RawCols) -> &mut Columns<ForumRow> {
        &mut o.1.0.values
    }
    pub fn forum_tag(o: &mut RawCols) -> &mut Columns<TagEdgeRow> {
        &mut o.1.1.values
    }
    pub fn forum_member(o: &mut RawCols) -> &mut Columns<EdgeRow> {
        &mut o.1.2.values
    }
    pub fn post(o: &mut RawCols) -> &mut Columns<PostRow> {
        &mut o.2.0.values
    }
    pub fn post_tag(o: &mut RawCols) -> &mut Columns<TagEdgeRow> {
        &mut o.2.1.values
    }
    pub fn comment(o: &mut RawCols) -> &mut Columns<CommentRow> {
        &mut o.2.2.values
    }
    pub fn comment_tag(o: &mut RawCols) -> &mut Columns<TagEdgeRow> {
        &mut o.2.3.values
    }
    pub fn likes_post(o: &mut RawCols) -> &mut Columns<EdgeRow> {
        &mut o.2.4.values
    }
    pub fn likes_comment(o: &mut RawCols) -> &mut Columns<EdgeRow> {
        &mut o.2.5.values
    }
}

/// The dynamic tables' names (the datagen's directories), in [`Raw`] order.
pub const DYNAMIC_NAMES: [&str; 14] = [
    "Person",
    "Person_hasInterest_Tag",
    "Person_studyAt_University",
    "Person_workAt_Company",
    "Person_knows_Person",
    "Forum",
    "Forum_hasTag_Tag",
    "Forum_hasMember_Person",
    "Post",
    "Post_hasTag_Tag",
    "Comment",
    "Comment_hasTag_Tag",
    "Person_likes_Post",
    "Person_likes_Comment",
];

/// Each dynamic table's column names, in [`Raw`] order.
pub const DYNAMIC_COLUMNS: [&[&str]; 14] = [
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "id",
        "firstName",
        "lastName",
        "gender",
        "birthday",
        "locationIP",
        "browserUsed",
        "LocationCityId",
        "language",
        "email",
    ],
    &["creationDate", "deletionDate", "PersonId", "TagId"],
    &[
        "creationDate",
        "deletionDate",
        "PersonId",
        "UniversityId",
        "classYear",
    ],
    &[
        "creationDate",
        "deletionDate",
        "PersonId",
        "CompanyId",
        "workFrom",
    ],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "Person1Id",
        "Person2Id",
    ],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "id",
        "title",
        "ModeratorPersonId",
    ],
    &["creationDate", "deletionDate", "ForumId", "TagId"],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "ForumId",
        "PersonId",
    ],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "id",
        "imageFile",
        "locationIP",
        "browserUsed",
        "language",
        "content",
        "length",
        "CreatorPersonId",
        "ContainerForumId",
        "LocationCountryId",
    ],
    &["creationDate", "deletionDate", "PostId", "TagId"],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "id",
        "locationIP",
        "browserUsed",
        "content",
        "length",
        "CreatorPersonId",
        "LocationCountryId",
        "ParentPostId",
        "ParentCommentId",
    ],
    &["creationDate", "deletionDate", "CommentId", "TagId"],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "PersonId",
        "PostId",
    ],
    &[
        "creationDate",
        "deletionDate",
        "explicitlyDeleted",
        "PersonId",
        "CommentId",
    ],
];

pub type OrganisationRow = (i32, String, String, String, i32);
pub type PlaceRow = (i32, String, String, String, Option<i32>);
pub type TagRow = (i32, String, String, i32);
pub type TagClassRow = (i32, String, String, Option<i32>);

/// The static tables' names and column names.
pub const STATIC_TABLES: [(&str, &[&str]); 4] = [
    (
        "Organisation",
        &["id", "type", "name", "url", "LocationPlaceId"],
    ),
    ("Place", &["id", "name", "url", "type", "PartOfPlaceId"]),
    ("Tag", &["id", "name", "url", "TypeTagClassId"]),
    ("TagClass", &["id", "name", "url", "SubclassOfTagClassId"]),
];

// ============================================================================
// Columns by kind
// ============================================================================

/// Which column buffer a column is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Ts,
    Date,
    I64,
    I32,
    Bool,
    Str,
    OptI64,
    OptI32,
    OptStr,
}

impl Kind {
    /// The Arrow type and nullability of the column, as its buffer's sink
    /// reports them.
    fn field(self, name: &str) -> Field {
        fn f<C: IntoArrowArray>(name: &str) -> Field {
            Field::new(name, C::data_type(), C::nullable())
        }
        match self {
            Kind::Ts => f::<Vec<Ts>>(name),
            Kind::Date => f::<Vec<Date32>>(name),
            Kind::I64 => f::<Vec<i64>>(name),
            Kind::I32 => f::<Vec<i32>>(name),
            Kind::Bool => f::<Vec<bool>>(name),
            Kind::Str => f::<StrCol>(name),
            Kind::OptI64 => f::<Nullable<Vec<i64>>>(name),
            Kind::OptI32 => f::<Nullable<Vec<i32>>>(name),
            Kind::OptStr => f::<Nullable<StrCol>>(name),
        }
    }
}

/// A column, held by kind.
#[derive(Debug, Clone)]
pub enum Col {
    Ts(Vec<Ts>),
    Date(Vec<Date32>),
    I64(Vec<i64>),
    I32(Vec<i32>),
    Bool(Vec<bool>),
    Str(StrCol),
    OptI64(Nullable<Vec<i64>>),
    OptI32(Nullable<Vec<i32>>),
    OptStr(Nullable<StrCol>),
}

impl Col {
    pub fn new(k: Kind) -> Self {
        match k {
            Kind::Ts => Col::Ts(Vec::new()),
            Kind::Date => Col::Date(Vec::new()),
            Kind::I64 => Col::I64(Vec::new()),
            Kind::I32 => Col::I32(Vec::new()),
            Kind::Bool => Col::Bool(Vec::new()),
            Kind::Str => Col::Str(StrCol::new()),
            Kind::OptI64 => Col::OptI64(Nullable::default()),
            Kind::OptI32 => Col::OptI32(Nullable::default()),
            Kind::OptStr => Col::OptStr(Nullable::default()),
        }
    }

    pub fn kind(&self) -> Kind {
        match self {
            Col::Ts(_) => Kind::Ts,
            Col::Date(_) => Kind::Date,
            Col::I64(_) => Kind::I64,
            Col::I32(_) => Kind::I32,
            Col::Bool(_) => Kind::Bool,
            Col::Str(_) => Kind::Str,
            Col::OptI64(_) => Kind::OptI64,
            Col::OptI32(_) => Kind::OptI32,
            Col::OptStr(_) => Kind::OptStr,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Col::Ts(v) => v.len(),
            Col::Date(v) => v.len(),
            Col::I64(v) => v.len(),
            Col::I32(v) => v.len(),
            Col::Bool(v) => v.len(),
            Col::Str(v) => v.len(),
            Col::OptI64(v) => v.len(),
            Col::OptI32(v) => v.len(),
            Col::OptStr(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The Arrow array: the buffers move into it.
    pub fn into_array(self) -> ArrayRef {
        match self {
            Col::Ts(v) => IntoArrowArray::into_array(v),
            Col::Date(v) => IntoArrowArray::into_array(v),
            Col::I64(v) => IntoArrowArray::into_array(v),
            Col::I32(v) => IntoArrowArray::into_array(v),
            Col::Bool(v) => IntoArrowArray::into_array(v),
            Col::Str(v) => IntoArrowArray::into_array(v),
            Col::OptI64(v) => IntoArrowArray::into_array(v),
            Col::OptI32(v) => IntoArrowArray::into_array(v),
            Col::OptStr(v) => IntoArrowArray::into_array(v),
        }
    }

    /// The same values as epoch milliseconds, for the `--epoch-millis`
    /// layout: timestamps as they are, dates at their midnight.
    pub fn into_epoch_millis(self) -> Col {
        match self {
            Col::Ts(v) => Col::I64(v.iter().map(|t| t.0).collect()),
            Col::Date(v) => Col::I64(v.iter().map(|d| d.0 as i64 * MS_PER_DAY).collect()),
            c => c,
        }
    }

    /// Epoch milliseconds of a timestamp column (held as timestamps, or as
    /// numbers by the `--epoch-millis` layout).
    pub fn epoch_ms(&self) -> Cow<'_, [i64]> {
        match self {
            Col::Ts(v) => Cow::Owned(v.iter().map(|t| t.0).collect()),
            Col::I64(v) => Cow::Borrowed(v),
            _ => unreachable!("not a timestamp column"),
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
    pub fn str_at(&self, r: usize) -> Option<&str> {
        match self {
            Col::Str(v) => Some(v.get(r)),
            Col::OptStr(v) => v.valid.get(r).then(|| v.values.get(r)),
            _ => unreachable!("not a string column"),
        }
    }

    /// Row `r` of an optional `i32` column.
    pub fn opt_i32_at(&self, r: usize) -> Option<i32> {
        match self {
            Col::OptI32(v) => v.valid.get(r).then(|| v.values[r]),
            Col::I32(v) => Some(v[r]),
            _ => unreachable!("not an i32 column"),
        }
    }

    /// The values at rows `idx`, in that order.
    pub fn gather(&self, idx: &[u32]) -> Col {
        Col::picked(self.kind(), idx.iter().map(|&i| (self, i as usize)))
    }

    /// Value `r` of column `c` for every `(c, r)` of `picks`, in order: a
    /// gather from one or more columns of kind `kind`.
    pub fn picked<'a>(kind: Kind, picks: impl Iterator<Item = (&'a Col, usize)> + Clone) -> Col {
        macro_rules! pick {
            ($v:ident) => {
                picks
                    .map(|(c, r)| match c {
                        Col::$v(v) => v[r],
                        _ => unreachable!("columns of different kinds"),
                    })
                    .collect()
            };
        }
        macro_rules! opt {
            ($v:ident) => {{
                let mut valid = Validity::default();
                valid.reserve(picks.clone().count());
                let values = picks
                    .map(|(c, r)| match c {
                        Col::$v(v) => {
                            valid.push(v.valid.get(r));
                            v.values[r]
                        }
                        _ => unreachable!("columns of different kinds"),
                    })
                    .collect();
                Nullable { values, valid }
            }};
        }
        let strs = |c: &'a Col| match c {
            Col::Str(v) => v,
            Col::OptStr(v) => &v.values,
            _ => unreachable!("columns of different kinds"),
        };
        match kind {
            Kind::Ts => Col::Ts(pick!(Ts)),
            Kind::Date => Col::Date(pick!(Date)),
            Kind::I64 => Col::I64(pick!(I64)),
            Kind::I32 => Col::I32(pick!(I32)),
            Kind::Bool => Col::Bool(pick!(Bool)),
            Kind::OptI64 => Col::OptI64(opt!(OptI64)),
            Kind::OptI32 => Col::OptI32(opt!(OptI32)),
            Kind::Str => {
                let mut v = StrCol::new();
                v.extend_picked(picks.map(|(c, r)| (strs(c), r)));
                Col::Str(v)
            }
            Kind::OptStr => {
                let mut values = StrCol::new();
                values.extend_picked(picks.clone().map(|(c, r)| (strs(c), r)));
                let mut valid = Validity::default();
                for (c, r) in picks {
                    match c {
                        Col::OptStr(v) => valid.push(v.valid.get(r)),
                        _ => unreachable!("columns of different kinds"),
                    }
                }
                Col::OptStr(Nullable { values, valid })
            }
        }
    }

    /// Bytes of row `r` of a string column (0 for other columns).
    pub fn value_bytes(&self, r: usize) -> usize {
        let len = |v: &StrCol| (v.offsets()[r + 1] - v.offsets()[r]) as usize;
        match self {
            Col::Str(v) => len(v),
            Col::OptStr(v) => len(&v.values),
            _ => 0,
        }
    }
}

/// Append a present string value that `f` writes in place.
#[inline]
pub fn push_some_with(col: &mut Nullable<StrCol>, f: impl FnOnce(&mut StrBuf<'_>)) {
    col.values.push_with(f);
    col.valid.push(true);
}

/// Columns that are one or more [`Col`]s: every typed column buffer, and
/// tuples of them (flattened, as Arrow flattens them).
pub trait IntoCols {
    fn into_cols(self, out: &mut Vec<Col>);
}

macro_rules! into_col {
    ($($t:ty => $v:ident),* $(,)?) => {
        $(impl IntoCols for $t {
            fn into_cols(self, out: &mut Vec<Col>) {
                out.push(Col::$v(self));
            }
        })*
    };
}

into_col!(
    Vec<Ts> => Ts,
    Vec<Date32> => Date,
    Vec<i64> => I64,
    Vec<i32> => I32,
    Vec<bool> => Bool,
    StrCol => Str,
    Nullable<Vec<i64>> => OptI64,
    Nullable<Vec<i32>> => OptI32,
    Nullable<StrCol> => OptStr,
);

macro_rules! tuple_cols {
    ($($T:ident $i:tt),+) => {
        impl<$($T: IntoCols),+> IntoCols for ($($T,)+) {
            fn into_cols(self, out: &mut Vec<Col>) {
                $(self.$i.into_cols(out);)+
            }
        }
    };
}

tuple_cols!(A 0, B 1, C 2);
tuple_cols!(A 0, B 1, C 2, D 3);
tuple_cols!(A 0, B 1, C 2, D 3, E 4);
tuple_cols!(A 0, B 1, C 2, D 3, E 4, F 5);
tuple_cols!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuple_cols!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

/// Several tables' columns (a [`ChildCols`] is one table, a tuple of them
/// its fields' tables in order) as one [`Rows`] per table.
pub trait IntoRows {
    fn into_rows(self, out: &mut Vec<Rows>);
}

impl<V: IntoCols> IntoRows for ChildCols<V> {
    fn into_rows(self, out: &mut Vec<Rows>) {
        let mut cols = Vec::new();
        self.values.into_cols(&mut cols);
        out.push(Rows { cols });
    }
}

macro_rules! tuple_rows {
    ($($T:ident $i:tt),+) => {
        impl<$($T: IntoRows),+> IntoRows for ($($T,)+) {
            fn into_rows(self, out: &mut Vec<Rows>) {
                $(self.$i.into_rows(out);)+
            }
        }
    };
}

tuple_rows!(A 0, B 1, C 2);
tuple_rows!(A 0, B 1, C 2, D 3);
tuple_rows!(A 0, B 1, C 2, D 3, E 4);
tuple_rows!(A 0, B 1, C 2, D 3, E 4, F 5);

/// Tables' columns as one [`Rows`] per table.
pub fn rows_of(cols: impl IntoRows) -> Vec<Rows> {
    let mut out = Vec::new();
    cols.into_rows(&mut out);
    out
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
}

/// A table: its name (directory of the datagen's output) and columns.
#[derive(Debug)]
pub struct TableDef {
    pub name: &'static str,
    pub cols: &'static [(&'static str, Kind)],
}

impl TableDef {
    /// Its Arrow schema: each column's type and nullability are its
    /// buffer's, as an Arrow sink makes it.
    pub fn schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self.cols.iter().map(|&(n, k)| k.field(n)).collect();
        Arc::new(Schema::new(fields))
    }

    /// The table of `name` whose columns are `cols`, named `names`.
    fn of(name: &'static str, names: &[&'static str], cols: &[Col]) -> TableDef {
        assert_eq!(names.len(), cols.len(), "{name}: a name per column");
        let cols: Vec<(&'static str, Kind)> = names
            .iter()
            .zip(cols)
            .map(|(&n, c)| (n, c.kind()))
            .collect();
        TableDef {
            name,
            cols: Box::leak(cols.into_boxed_slice()),
        }
    }
}

/// The dynamic tables, in [`Raw`] order: their columns are the kinds of
/// [`RawCols`]' buffers.
pub static DYNAMIC: LazyLock<Vec<TableDef>> = LazyLock::new(|| {
    rows_of(RawCols::default())
        .iter()
        .zip(DYNAMIC_NAMES.iter().zip(DYNAMIC_COLUMNS))
        .map(|(r, (name, names))| TableDef::of(name, names, &r.cols))
        .collect()
});

/// The static tables, in [`STATIC_TABLES`] order.
pub type Statics = (
    Children<OrganisationRow>,
    Children<PlaceRow>,
    Children<TagRow>,
    Children<TagClassRow>,
);

/// The columns of [`Statics`].
pub type StaticCols = Columns<Statics>;

/// The static tables, as [`STATIC_TABLES`] names them.
pub static STATIC: LazyLock<Vec<TableDef>> = LazyLock::new(|| {
    rows_of(StaticCols::default())
        .iter()
        .zip(STATIC_TABLES)
        .map(|(r, (name, names))| TableDef::of(name, names, &r.cols))
        .collect()
});
