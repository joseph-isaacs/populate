// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! The datagen's output layouts: `--explode-edges` and `--explode-attrs`
//! (`ExplodeEdges`, `ExplodeAttrs`), and `--epoch-millis`.
//!
//! The raw tables ([`DYNAMIC`], [`STATIC`]) are the `composite-merged-fk`
//! layout: foreign keys and multi-valued attributes are columns. Exploding
//! edges turns each foreign key column into its own edge table (and drops
//! it); exploding attributes does the same for a person's `email` and
//! `language` lists. Spark explodes `split(column, ";")`, so a list is cut
//! at every `;` (empty pieces kept), a number is one value and a null
//! none. `--epoch-millis` keeps dates as milliseconds since the epoch.

use crate::rows::{Col, DYNAMIC, Kind, Rows, STATIC, TableDef};
use crate::{Ldbc, csv, to_batch};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use populate_core::StrCol;
use populate_table::MultiChunks;
use populate_table::text::TextChunks;

/// How one output table is made from a raw one.
#[derive(Debug, Clone)]
enum Op {
    /// The raw table's columns `cols`.
    Project(Vec<usize>),
    /// Columns `keep` of each row, then each value of column `list` (in
    /// that order, or with the value first if `list_first`).
    Explode {
        keep: Vec<usize>,
        id: usize,
        list: usize,
        list_first: bool,
    },
}

/// An output table of a layout.
#[derive(Debug)]
pub struct OutTable {
    pub def: &'static TableDef,
    /// Index of the raw table, in [`DYNAMIC`] or [`STATIC`].
    pub src: usize,
    /// Made by exploding a column (`OneN`/`NOne` edges and attributes);
    /// BI mode makes no delete batches for these.
    pub derived: bool,
    op: Op,
}

impl OutTable {
    /// This table's rows of the raw table's `rows`.
    pub fn apply(&self, rows: &Rows) -> Rows {
        let cols = match &self.op {
            Op::Project(cols) => cols.iter().map(|&c| rows.cols[c].clone()).collect(),
            Op::Explode {
                keep,
                id,
                list,
                list_first,
            } => {
                let mut idx = Vec::new();
                let mut strs = StrCol::new();
                let mut nums: Vec<i64> = Vec::new();
                let src = &rows.cols[*list];
                for r in 0..rows.len() {
                    let r32 = r as u32;
                    let num = match src {
                        Col::Str(_) | Col::OptStr(_) => {
                            if let Some(s) = src.str_at(r) {
                                // `explode(split(s, ";"))`: every piece, empty ones included
                                for piece in s.split(';') {
                                    idx.push(r32);
                                    strs.push_str(piece);
                                }
                            }
                            continue;
                        }
                        Col::I64(v) => Some(v[r]),
                        Col::I32(v) => Some(v[r] as i64),
                        Col::OptI64(v) => v.valid.get(r).then(|| v.values[r]),
                        Col::OptI32(v) => v.valid.get(r).then(|| v.values[r] as i64),
                        _ => unreachable!("not a list column"),
                    };
                    if let Some(v) = num {
                        idx.push(r32);
                        nums.push(v);
                    }
                }
                let value = match src {
                    Col::Str(_) | Col::OptStr(_) => Col::Str(strs),
                    _ => Col::I64(nums),
                };
                let mut cols: Vec<Col> = keep.iter().map(|&c| rows.cols[c].gather(&idx)).collect();
                let id = rows.cols[*id].gather(&idx);
                if *list_first {
                    cols.extend([value, id]);
                } else {
                    cols.extend([id, value]);
                }
                cols
            }
        };
        // `--epoch-millis`: timestamps and dates the table holds as numbers
        let cols = cols
            .into_iter()
            .zip(self.def.cols)
            .map(|(c, &(_, k))| {
                if c.kind() == k {
                    c
                } else {
                    c.into_epoch_millis()
                }
            })
            .collect();
        Rows { cols }
    }
}

/// A set of output tables, and the directory name the datagen gives it.
#[derive(Debug)]
pub struct Layout {
    pub dynamic: Vec<OutTable>,
    pub statics: Vec<OutTable>,
    explode_edges: bool,
    explode_attrs: bool,
}

/// Which layout: the datagen's `--explode-edges`, `--explode-attrs` and
/// `--epoch-millis`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LayoutOptions {
    pub explode_edges: bool,
    pub explode_attrs: bool,
    pub epoch_millis: bool,
}

fn leak_def(name: String, cols: Vec<(&'static str, Kind)>) -> &'static TableDef {
    Box::leak(Box::new(TableDef {
        name: Box::leak(name.into_boxed_str()),
        cols: Box::leak(cols.into_boxed_slice()),
    }))
}

/// An exploded edge or attribute of raw table `def`.
struct Explode {
    name: &'static str,
    /// Column with the list (or foreign key).
    list: &'static str,
    /// Output names of the row's id and the value.
    id_name: &'static str,
    value_name: &'static str,
    list_first: bool,
}

const fn ex(
    name: &'static str,
    list: &'static str,
    id_name: &'static str,
    value_name: &'static str,
) -> Explode {
    Explode {
        name,
        list,
        id_name,
        value_name,
        list_first: false,
    }
}

/// The exploded edges, per raw table name.
fn edges(table: &str) -> Vec<Explode> {
    match table {
        "Organisation" => vec![ex(
            "Organisation_isLocatedIn_Place",
            "LocationPlaceId",
            "OrganisationId",
            "PlaceId",
        )],
        "Place" => vec![ex(
            "Place_isPartOf_Place",
            "PartOfPlaceId",
            "Place1Id",
            "Place2Id",
        )],
        "Tag" => vec![ex(
            "Tag_hasType_TagClass",
            "TypeTagClassId",
            "TagId",
            "TagClassId",
        )],
        "TagClass" => vec![ex(
            "TagClass_isSubclassOf_TagClass",
            "SubclassOfTagClassId",
            "TagClass1Id",
            "TagClass2Id",
        )],
        "Comment" => vec![
            ex(
                "Comment_hasCreator_Person",
                "CreatorPersonId",
                "CommentId",
                "PersonId",
            ),
            ex(
                "Comment_isLocatedIn_Country",
                "LocationCountryId",
                "CommentId",
                "CountryId",
            ),
            ex(
                "Comment_replyOf_Post",
                "ParentPostId",
                "CommentId",
                "PostId",
            ),
            ex(
                "Comment_replyOf_Comment",
                "ParentCommentId",
                "Comment1Id",
                "Comment2Id",
            ),
        ],
        "Forum" => vec![ex(
            "Forum_hasModerator_Person",
            "ModeratorPersonId",
            "ForumId",
            "PersonId",
        )],
        "Person" => vec![ex(
            "Person_isLocatedIn_City",
            "LocationCityId",
            "PersonId",
            "CityId",
        )],
        "Post" => vec![
            ex(
                "Post_hasCreator_Person",
                "CreatorPersonId",
                "PostId",
                "PersonId",
            ),
            Explode {
                list_first: true,
                ..ex(
                    "Forum_containerOf_Post",
                    "ContainerForumId",
                    "PostId",
                    "ForumId",
                )
            },
            ex(
                "Post_isLocatedIn_Country",
                "LocationCountryId",
                "PostId",
                "CountryId",
            ),
        ],
        _ => Vec::new(),
    }
}

fn attrs(table: &str) -> Vec<Explode> {
    match table {
        "Person" => vec![
            ex(
                "Person_email_EmailAddress",
                "email",
                "PersonId",
                "EmailAddressId",
            ),
            ex(
                "Person_speaks_Language",
                "language",
                "PersonId",
                "LanguageId",
            ),
        ],
        _ => Vec::new(),
    }
}

impl Layout {
    pub fn new(o: LayoutOptions) -> Self {
        let build = |raw: &'static [TableDef]| -> Vec<OutTable> {
            let kind = |k: Kind| match k {
                Kind::Ts | Kind::Date if o.epoch_millis => Kind::I64,
                k => k,
            };
            let mut nodes = Vec::new();
            let mut derived = Vec::new();
            for (src, def) in raw.iter().enumerate() {
                let mut ex = Vec::new();
                if o.explode_attrs {
                    ex.extend(attrs(def.name));
                }
                if o.explode_edges {
                    ex.extend(edges(def.name));
                }
                let pos = |n: &str| def.cols.iter().position(|c| c.0 == n).expect("column");
                let dropped: Vec<usize> = ex.iter().map(|e| pos(e.list)).collect();
                let raw_cols: Vec<usize> = ["creationDate", "deletionDate", "explicitlyDeleted"]
                    .iter()
                    .filter_map(|n| def.cols.iter().position(|c| c.0 == *n))
                    .collect();
                let cols: Vec<usize> = (0..def.cols.len())
                    .filter(|c| !dropped.contains(c))
                    .collect();
                let out_cols = cols
                    .iter()
                    .map(|&c| (def.cols[c].0, kind(def.cols[c].1)))
                    .collect();
                nodes.push(OutTable {
                    def: leak_def(def.name.to_string(), out_cols),
                    src,
                    derived: false,
                    op: Op::Project(cols),
                });
                for e in ex {
                    let list = pos(e.list);
                    let value_kind = match def.cols[list].1 {
                        Kind::Str | Kind::OptStr => Kind::Str,
                        Kind::I32 | Kind::OptI32 => Kind::I32,
                        _ => Kind::I64,
                    };
                    let id = pos("id");
                    let mut out_cols: Vec<(&'static str, Kind)> = raw_cols
                        .iter()
                        .map(|&c| (def.cols[c].0, kind(def.cols[c].1)))
                        .collect();
                    let id_col = (e.id_name, def.cols[id].1);
                    // numbers explode as i64 here; i32 sources print the same
                    let value_col = (
                        e.value_name,
                        if value_kind == Kind::Str {
                            Kind::Str
                        } else {
                            Kind::I64
                        },
                    );
                    if e.list_first {
                        out_cols.extend([value_col, id_col]);
                    } else {
                        out_cols.extend([id_col, value_col]);
                    }
                    derived.push(OutTable {
                        def: leak_def(e.name.to_string(), out_cols),
                        src,
                        derived: true,
                        op: Op::Explode {
                            keep: raw_cols.clone(),
                            id,
                            list,
                            list_first: e.list_first,
                        },
                    });
                }
            }
            nodes.extend(derived);
            nodes
        };
        Layout {
            dynamic: build(&DYNAMIC),
            statics: build(&STATIC),
            explode_edges: o.explode_edges,
            explode_attrs: o.explode_attrs,
        }
    }

    /// The datagen's directory name for it (`composite-merged-fk`, ...).
    pub fn dir(&self) -> &'static str {
        match (self.explode_attrs, self.explode_edges) {
            (true, true) => "singular-projected-fk",
            (true, false) => "singular-merged-fk",
            (false, true) => "composite-projected-fk",
            (false, false) => "composite-merged-fk",
        }
    }

    /// The output tables of raw tables `raw` (one [`Rows`] per raw table,
    /// [`DYNAMIC`] or [`STATIC`] order).
    pub fn apply(tables: &[OutTable], raw: &[Rows]) -> Vec<Rows> {
        tables.iter().map(|t| t.apply(&raw[t.src])).collect()
    }
}

/// A network's dynamic tables in a layout, as a [`MultiChunks`] /
/// [`TextChunks`] source (one output per [`Layout::dynamic`] table).
pub struct LayoutRun<'a> {
    pub g: &'a Ldbc,
    pub layout: &'a Layout,
}

impl LayoutRun<'_> {
    fn rows(&self, i: u64) -> Vec<Rows> {
        Layout::apply(&self.layout.dynamic, &self.g.chunk_rows(i))
    }

    /// The static tables in the layout.
    pub fn static_rows(&self) -> Vec<Rows> {
        Layout::apply(&self.layout.statics, &self.g.static_rows())
    }
}

impl MultiChunks for LayoutRun<'_> {
    fn schemas(&self) -> Vec<SchemaRef> {
        self.layout.dynamic.iter().map(|t| t.def.schema()).collect()
    }

    fn num_chunks(&self) -> u64 {
        self.g.num_chunks()
    }

    fn chunk(&self, i: u64, emit: &mut dyn FnMut(usize, RecordBatch)) {
        for (k, (t, r)) in self.layout.dynamic.iter().zip(self.rows(i)).enumerate() {
            if !r.is_empty() {
                emit(k, to_batch(t.def, r));
            }
        }
    }
}

impl TextChunks for LayoutRun<'_> {
    fn outputs(&self) -> usize {
        self.layout.dynamic.len()
    }

    fn num_chunks(&self) -> u64 {
        self.g.num_chunks()
    }

    fn chunk_text(&self, i: u64, bufs: &mut [Vec<u8>]) {
        for ((t, r), buf) in self
            .layout
            .dynamic
            .iter()
            .zip(&self.rows(i))
            .zip(bufs.iter_mut())
        {
            if i == 0 {
                csv::header(t.def, buf);
            }
            csv::write(t.def, r, buf);
        }
    }
}
