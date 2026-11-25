//! The datagen's factor tables (`--generate-factors`): 34 summary tables
//! of the raw graph (counts per country, day, tag, person, ...; friends of
//! friends; 2- and 4-hop person pairs; connected components), as
//! `FactorGenerationStage` computes them with Spark.
//!
//! Inputs are gathered while the graph is generated ([`FactorInput::add`],
//! one call per chunk): the person-sized tables in raw order, and the
//! message-sized ones folded into counts. [`FactorInput::finish`] then
//! computes every table, in parallel.
//!
//! Spark's semantics are reproduced: `frequency` tables are ordered by
//! frequency (descending, nulls last), then by their grouping columns
//! (ascending, nulls first); left outer joins count 0 and sum to null;
//! `personNumFriendTags` groups by the interest's (nullable) person id;
//! the n-hop tables keep the datagen's bug (person 2's dates are person
//! 1's) and its walk semantics (a person is two hops from itself), and
//! sample with Spark's `XORShiftRandom` (`Dataset.sample(f, 42)` over one
//! partition).

use crate::csv::spark_micros;
use crate::par_map;
use crate::rows::{Col, Kind, OrgEdgeRow, RawCols, Rows, TableDef};
use populate_core::{Columns, Sink, Timestamp};
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::hash::{BuildHasherDefault, Hash, Hasher};
use std::mem::{swap, take};
use std::sync::Mutex;

use Kind::{I32, I64, OptI64, Str, Ts};

const DAY_US: i64 = 86_400_000_000;

/// A multiply-rotate hash (as rustc's `FxHasher`) with a final mix, for
/// the integer keys here: SipHash is most of the cost otherwise.
#[derive(Default)]
struct Fx(u64);

impl Fx {
    #[inline]
    fn add(&mut self, x: u64) {
        self.0 = (self.0.rotate_left(5) ^ x).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
}

impl Hasher for Fx {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let (words, tail) = bytes.as_chunks::<8>();
        for w in words {
            self.add(u64::from_le_bytes(*w));
        }
        for &b in tail {
            self.add(b as u64);
        }
    }

    #[inline]
    fn write_u64(&mut self, x: u64) {
        self.add(x);
    }

    #[inline]
    fn write_i64(&mut self, x: i64) {
        self.add(x as u64);
    }

    #[inline]
    fn write_u32(&mut self, x: u32) {
        self.add(x as u64);
    }

    #[inline]
    fn write_i32(&mut self, x: i32) {
        self.add(x as u32 as u64);
    }

    #[inline]
    fn write_usize(&mut self, x: usize) {
        self.add(x as u64);
    }

    #[inline]
    fn finish(&self) -> u64 {
        let h = self.0;
        (h ^ (h >> 29)).wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ (h >> 32)
    }
}

type Map<K, V> = HashMap<K, V, BuildHasherDefault<Fx>>;

/// Midnight (epoch milliseconds) of the day of a timestamp, as Spark's
/// `date_trunc("day", ts)` in UTC.
fn day(ms: i64) -> i64 {
    spark_micros(ms).div_euclid(DAY_US) * 86_400_000
}

/// A person, as the factors use it.
struct Person {
    id: i64,
    created: i64,
    deleted: i64,
    first_name: Box<str>,
    city: i32,
}

/// One chunk's person-sized rows, in raw order.
#[derive(Default)]
struct Part {
    persons: Vec<Person>,
    /// `(Person1Id, Person2Id, creationDate, deletionDate)`.
    knows: Vec<[i64; 4]>,
    /// `(PersonId, UniversityId, creationDate, deletionDate)`.
    study: Vec<[i64; 4]>,
    /// `(PersonId, CompanyId, creationDate, deletionDate)`.
    work: Vec<[i64; 4]>,
    /// `(PersonId, TagId)`.
    interests: Vec<(i64, i32)>,
}

/// Counts over the message-sized tables (order does not matter).
#[derive(Default)]
struct Counts {
    /// Messages per `LocationCountryId`.
    country: Map<i64, i64>,
    /// Messages per creation day.
    day: Map<i64, i64>,
    /// Messages per creation day and `length / 10`.
    day_len: Map<(i64, i64), i64>,
    /// Messages per length.
    len: Map<i32, i64>,
    /// Message tags per creation day and tag (a tag's `creationDate` is its
    /// message's).
    day_tag: Map<(i64, i32), i64>,
    /// Posts per language (non-null).
    lang: Map<Box<str>, i64>,
    /// Posts, comments, likes and memberships per person.
    posts: Map<i64, i64>,
    comments: Map<i64, i64>,
    likes: Map<i64, i64>,
    forums: Map<i64, i64>,
}

fn add_all<K: Hash + Eq>(into: &mut Map<K, i64>, from: Map<K, i64>) {
    if into.is_empty() {
        *into = from;
        return;
    }
    for (k, v) in from {
        *into.entry(k).or_default() += v;
    }
}

impl Counts {
    fn merge(&mut self, o: Counts) {
        add_all(&mut self.country, o.country);
        add_all(&mut self.day, o.day);
        add_all(&mut self.day_len, o.day_len);
        add_all(&mut self.len, o.len);
        add_all(&mut self.day_tag, o.day_tag);
        add_all(&mut self.lang, o.lang);
        add_all(&mut self.posts, o.posts);
        add_all(&mut self.comments, o.comments);
        add_all(&mut self.likes, o.likes);
        add_all(&mut self.forums, o.forums);
    }
}

/// The factor tables' inputs, gathered chunk by chunk.
pub struct FactorInput {
    parts: Mutex<Vec<Option<Part>>>,
    counts: Mutex<Counts>,
}

/// Chunk `i` was taken in twice, or not at all.
fn chunk_once(i: usize) -> String {
    format!("chunk {i}: the factor tables need every chunk generated exactly once")
}

/// A factor table: its definition and rows, in the datagen's order.
pub struct Factor {
    pub def: &'static TableDef,
    pub rows: Rows,
}

impl FactorInput {
    pub fn new(num_chunks: u64) -> Self {
        FactorInput {
            parts: Mutex::new((0..num_chunks).map(|_| None).collect()),
            counts: Mutex::new(Counts::default()),
        }
    }

    /// Take in chunk `i`'s raw rows.
    pub fn add(&self, i: u64, raw: &RawCols) {
        let (
            (person, interest, study, work, knows),
            (_, _, member),
            (post, post_tag, comment, comment_tag, likes_post, likes_comment),
        ) = &raw.tables;
        let mut p = Part::default();
        {
            let ((c, d, _), id, first, _, _, _, _, _, city, _, _) = &person.values;
            p.persons = (0..id.len())
                .map(|j| Person {
                    id: id[j],
                    created: c[j].0,
                    deleted: d[j].0,
                    first_name: first.get(j).into(),
                    city: city[j],
                })
                .collect();
        }
        let quad = |(c, d, x, y, _): &Columns<OrgEdgeRow>| -> Vec<[i64; 4]> {
            (0..x.len()).map(|j| [x[j], y[j], c[j].0, d[j].0]).collect()
        };
        {
            let (c, d, _, x, y) = &knows.values;
            p.knows = (0..x.len()).map(|j| [x[j], y[j], c[j].0, d[j].0]).collect();
        }
        p.study = quad(&study.values);
        p.work = quad(&work.values);
        {
            let (_, _, pid, tag) = &interest.values;
            p.interests = pid.iter().copied().zip(tag.iter().copied()).collect();
        }

        let mut k = Counts::default();
        let count = |m: &mut Map<i64, i64>, ids: &[i64]| {
            for &x in ids {
                *m.entry(x).or_default() += 1;
            }
        };
        {
            let ((c, _, _), _, _, _, _, lang, _, len, creator, _, country) = &post.values;
            for j in 0..len.len() {
                message(&mut k, c[j].0, len[j], country[j]);
                if lang.valid.get(j) {
                    *k.lang.entry(lang.values.get(j).into()).or_default() += 1;
                }
            }
            count(&mut k.posts, creator);
        }
        {
            let (c, _, _, _, _, _, _, len, creator, country, _, _) = &comment.values;
            for j in 0..len.len() {
                message(&mut k, c[j].0, len[j], country[j] as i64);
            }
            count(&mut k.comments, creator);
        }
        for t in [post_tag, comment_tag] {
            let (c, _, _, tag) = &t.values;
            for j in 0..tag.len() {
                *k.day_tag.entry((day(c[j].0), tag[j])).or_default() += 1;
            }
        }
        count(&mut k.likes, &likes_post.values.3);
        count(&mut k.likes, &likes_comment.values.3);
        count(&mut k.forums, &member.values.4);

        {
            let mut parts = self.parts.lock().expect("parts");
            let slot = &mut parts[i as usize];
            assert!(slot.is_none(), "{}", chunk_once(i as usize));
            *slot = Some(p);
        }
        self.counts.lock().expect("counts").merge(k);
    }

    /// Every factor table, on `threads` threads (0 = all cores), from the
    /// chunks added and the static tables ([`rows::STATIC`](crate::rows::STATIC)).
    pub fn finish(&self, statics: &[Rows], threads: usize) -> Vec<Factor> {
        let parts = take(&mut *self.parts.lock().expect("parts"));
        let counts = take(&mut *self.counts.lock().expect("counts"));
        let mut all = Part::default();
        for (i, p) in parts.into_iter().enumerate() {
            let p = p.unwrap_or_else(|| panic!("{}", chunk_once(i)));
            all.persons.extend(p.persons);
            all.knows.extend(p.knows);
            all.study.extend(p.study);
            all.work.extend(p.work);
            all.interests.extend(p.interests);
        }
        let ctx = Ctx::new(all, counts, statics, threads);
        par_map(FACTORS.len(), threads, |k| {
            let (def, f) = FACTORS[k];
            Factor { def, rows: f(&ctx) }
        })
    }
}

fn message(k: &mut Counts, created: i64, len: i32, country: i64) {
    let d = day(created);
    *k.country.entry(country).or_default() += 1;
    *k.day.entry(d).or_default() += 1;
    *k.day_len.entry((d, (len / 10) as i64)).or_default() += 1;
    *k.len.entry(len).or_default() += 1;
}

/// A place of the static `Place` table.
struct Place {
    name: Box<str>,
    kind: Box<str>,
    part_of: Option<i32>,
}

/// Everything the factors read, indexed.
struct Ctx {
    g: Part,
    k: Counts,
    /// Person id to index into `g.persons`.
    idx: Map<i64, u32>,
    /// Undirected friendships (both directions of each `knows` row, in
    /// raw order), by person index.
    adj: Vec<Vec<u32>>,
    places: Map<i32, Place>,
    /// Tag id to `(name, TypeTagClassId)`.
    tags: Vec<(i32, Box<str>, i32)>,
    tag_of: Map<i32, usize>,
    /// Tag class id to name.
    classes: Map<i32, Box<str>>,
    /// Company id to name (organisations of type `Company`).
    companies: Map<i64, Box<str>>,
    threads: usize,
}

impl Ctx {
    fn new(g: Part, k: Counts, statics: &[Rows], threads: usize) -> Self {
        let idx: Map<i64, u32> = g
            .persons
            .iter()
            .enumerate()
            .map(|(i, p)| (p.id, i as u32))
            .collect();
        let mut adj = vec![Vec::new(); g.persons.len()];
        for e in &g.knows {
            let (a, b) = (idx[&e[0]], idx[&e[1]]);
            adj[a as usize].push(b);
            adj[b as usize].push(a);
        }
        let s =
            |r: &Rows, c: usize, j: usize| -> Box<str> { r.cols[c].str_at(j).expect("str").into() };
        let (org, place, tag, class) = (&statics[0], &statics[1], &statics[2], &statics[3]);
        let places = (0..place.len())
            .map(|j| {
                let p = Place {
                    name: s(place, 1, j),
                    kind: s(place, 3, j),
                    part_of: place.cols[4].opt_i32_at(j),
                };
                (place.cols[0].i32s()[j], p)
            })
            .collect();
        let tags: Vec<(i32, Box<str>, i32)> = (0..tag.len())
            .map(|j| (tag.cols[0].i32s()[j], s(tag, 1, j), tag.cols[3].i32s()[j]))
            .collect();
        let tag_of = tags.iter().enumerate().map(|(i, t)| (t.0, i)).collect();
        let classes = (0..class.len())
            .map(|j| (class.cols[0].i32s()[j], s(class, 1, j)))
            .collect();
        let companies = (0..org.len())
            .filter(|&j| org.cols[1].str_at(j) == Some("Company"))
            .map(|j| (org.cols[0].i32s()[j] as i64, s(org, 2, j)))
            .collect();
        Ctx {
            g,
            k,
            idx,
            adj,
            places,
            tags,
            tag_of,
            classes,
            companies,
            threads,
        }
    }

    fn of_kind(&self, id: i32, kind: &str) -> Option<&Place> {
        self.places.get(&id).filter(|p| &*p.kind == kind)
    }

    /// A person's city and its country, if both are what they should be.
    fn city_country(&self, p: &Person) -> Option<(i32, &Place, i32, &Place)> {
        let city = self.of_kind(p.city, "City")?;
        let cid = city.part_of?;
        let country = self.of_kind(cid, "Country")?;
        Some((p.city, city, cid, country))
    }

    fn tag(&self, id: i32) -> Option<&(i32, Box<str>, i32)> {
        self.tag_of.get(&id).map(|&i| &self.tags[i])
    }

    /// Messages per tag (`tagNumMessages` before sorting).
    fn tag_messages(&self) -> Map<i32, i64> {
        let mut m: Map<i32, i64> = Map::default();
        for (&(_, tag), &n) in &self.k.day_tag {
            *m.entry(tag).or_default() += n;
        }
        m
    }
}

// ---- output helpers ----

/// A value of an output row.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum V<'a> {
    /// Null sorts first, as Spark's ascending order.
    Null,
    Int(i64),
    Str(&'a str),
}

impl From<i64> for V<'_> {
    fn from(x: i64) -> Self {
        V::Int(x)
    }
}

impl From<i32> for V<'_> {
    fn from(x: i32) -> Self {
        V::Int(x as i64)
    }
}

impl From<Option<i64>> for V<'_> {
    fn from(x: Option<i64>) -> Self {
        x.map_or(V::Null, V::Int)
    }
}

impl<'a> From<&'a str> for V<'a> {
    fn from(x: &'a str) -> Self {
        V::Str(x)
    }
}

impl<'a> From<&'a Box<str>> for V<'a> {
    fn from(x: &'a Box<str>) -> Self {
        V::Str(x)
    }
}

/// Spark's `orderBy(frequency.desc, by.asc...)`: the last value is the
/// frequency (nulls last), the others the grouping columns (nulls first).
fn freq_order(a: &[V], b: &[V]) -> Ordering {
    let (fa, ka) = a.split_last().expect("row");
    let (fb, kb) = b.split_last().expect("row");
    let desc = match (fa, fb) {
        (V::Null, V::Null) => Ordering::Equal,
        (V::Null, _) => Ordering::Greater,
        (_, V::Null) => Ordering::Less,
        (x, y) => y.cmp(x),
    };
    desc.then_with(|| ka.cmp(kb))
}

/// Append one row.
fn push_row(out: &mut Rows, r: &[V]) {
    for (c, v) in out.cols.iter_mut().zip(r) {
        match (c, v) {
            (Col::I64(c), V::Int(x)) => c.push(*x),
            (Col::Ts(c), V::Int(x)) => c.push(Timestamp(*x)),
            (Col::I32(c), V::Int(x)) => c.push(*x as i32),
            (Col::OptI64(c), V::Int(x)) => c.push(Some(*x)),
            (Col::OptI64(c), V::Null) => c.push(None),
            (Col::Str(c), V::Str(s)) => c.push_str(s),
            (c, v) => unreachable!("{v:?} in a {c:?} column"),
        }
    }
}

/// Rows of `def` from `rows` (in order).
fn to_rows(def: &TableDef, rows: &[Vec<V>]) -> Rows {
    let mut out = Rows::new(def);
    for r in rows {
        push_row(&mut out, r);
    }
    out
}

/// A `frequency` table: `(by..., frequency)` rows sorted as Spark does.
fn frequency(def: &TableDef, mut rows: Vec<Vec<V>>) -> Rows {
    rows.sort_by(|a, b| freq_order(a, b));
    to_rows(def, &rows)
}

/// `(key, count)` pairs as frequency rows: `key(k)` gives the grouping
/// columns, or `None` to drop the group (an inner join that misses).
/// The grouping columns must order as `K` does (they start with `K`'s
/// values, the rest are functions of them), so the pairs are sorted as
/// they are, before the columns are built.
fn counted<'a, K: Copy + Ord>(
    def: &TableDef,
    m: impl IntoIterator<Item = (K, i64)>,
    key: impl Fn(K) -> Option<Vec<V<'a>>>,
) -> Rows {
    let mut m: Vec<(K, i64)> = m.into_iter().collect();
    m.sort_unstable_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    let mut out = Rows::new(def);
    for (k, n) in m {
        if let Some(mut r) = key(k) {
            r.push(V::Int(n));
            push_row(&mut out, &r);
        }
    }
    out
}

/// Group `items` by key and count them.
fn group_count<K: Hash + Eq>(items: impl IntoIterator<Item = K>) -> Map<K, i64> {
    let mut m = Map::default();
    for k in items {
        *m.entry(k).or_default() += 1;
    }
    m
}

/// Spark's `sum` over a group: null if every value is null (or there are
/// none).
fn sum(vals: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    vals.into_iter().fold(None, |acc, v| match (acc, v) {
        (a, None) => a,
        (None, Some(x)) => Some(x),
        (Some(a), Some(x)) => Some(a + x),
    })
}

// ---- the tables ----

macro_rules! def {
    ($name:ident, $tname:literal, [$(($c:literal, $k:expr)),* $(,)?]) => {
        const $name: TableDef = TableDef { name: $tname, cols: &[$(($c, $k)),*] };
    };
}

def!(
    COUNTRY_NUM_PERSONS,
    "countryNumPersons",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    CITY_NUM_PERSONS,
    "cityNumPersons",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    COUNTRY_NUM_MESSAGES,
    "countryNumMessages",
    [
        ("LocationCountryId", I64),
        ("name", Str),
        ("frequency", I64)
    ]
);
def!(
    CITY_PAIRS,
    "cityPairsNumFriends",
    [
        ("city1Id", I32),
        ("city2Id", I32),
        ("city1Name", Str),
        ("city2Name", Str),
        ("country1Id", I32),
        ("country2Id", I32),
        ("country1Name", Str),
        ("country2Name", Str),
        ("frequency", I64),
    ]
);
def!(
    COUNTRY_PAIRS,
    "countryPairsNumFriends",
    [
        ("country1Id", I32),
        ("country2Id", I32),
        ("country1Name", Str),
        ("country2Name", Str),
        ("frequency", I64),
    ]
);
def!(
    DAY_MESSAGES,
    "creationDayNumMessages",
    [("creationDay", Ts), ("frequency", I64)]
);
def!(
    DAY_CLASS_MESSAGES,
    "creationDayAndTagClassNumMessages",
    [
        ("creationDay", Ts),
        ("id", I32),
        ("name", Str),
        ("frequency", I64),
    ]
);
def!(
    DAY_TAG_MESSAGES,
    "creationDayAndTagNumMessages",
    [
        ("creationDay", Ts),
        ("id", I32),
        ("name", Str),
        ("frequency", I64),
    ]
);
def!(
    DAY_LEN_MESSAGES,
    "creationDayAndLengthCategoryNumMessages",
    [
        ("creationDay", Ts),
        ("lengthCategory", I64),
        ("frequency", I64),
    ]
);
def!(
    LEN_MESSAGES,
    "lengthNumMessages",
    [("length", I32), ("frequency", I64)]
);
def!(
    TAG_MESSAGES,
    "tagNumMessages",
    [("tagId", I32), ("tagName", Str), ("frequency", I64)]
);
def!(
    CLASS_MESSAGES,
    "tagClassNumMessages",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    PERSON_NUM_FRIENDS,
    "personNumFriends",
    [
        ("id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("frequency", I64),
    ]
);
def!(
    LANGUAGE_POSTS,
    "languageNumPosts",
    [("language", Str), ("frequency", I64)]
);
def!(
    TAG_PERSONS,
    "tagNumPersons",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    CLASS_TAGS,
    "tagClassNumTags",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    DISJOINT_EMPLOYERS,
    "personDisjointEmployerPairs",
    [
        ("person2id", I64),
        ("companyName", Str),
        ("companyId", I32),
        ("person2creationDate", Ts),
        ("person2deletionDate", Ts),
    ]
);
def!(
    COMPANY_EMPLOYEES,
    "companyNumEmployees",
    [("id", I32), ("name", Str), ("frequency", I64)]
);
def!(
    PEOPLE_4_HOPS,
    "people4Hops",
    [
        ("Person1Id", I64),
        ("Person2Id", I64),
        ("Person1CreationDate", Ts),
        ("Person1DeletionDate", Ts),
        ("Person2CreationDate", Ts),
        ("Person2DeletionDate", Ts),
    ]
);
def!(
    PEOPLE_2_HOPS,
    "people2Hops",
    [
        ("Person1Id", I64),
        ("Person2Id", I64),
        ("Person1CreationDate", Ts),
        ("Person1DeletionDate", Ts),
        ("Person2CreationDate", Ts),
        ("Person2DeletionDate", Ts),
    ]
);
def!(
    FIRST_NAMES,
    "personFirstNames",
    [("firstName", Str), ("frequency", I64)]
);
def!(
    FRIENDS_3,
    "personNumFriendsOfFriendsOfFriends",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numFriends", I64),
        ("numFriendsOfFriends", OptI64),
        ("numFriendsOfFriendsOfFriends", OptI64),
    ]
);
def!(
    FOF_POSTS,
    "personNumFriendOfFriendPosts",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numDirectPosts", I64),
        ("numFriendPosts", OptI64),
        ("numFriendOfFriendPosts", OptI64),
    ]
);
def!(
    FOF_COMMENTS,
    "personNumFriendOfFriendComments",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numDirectComments", I64),
        ("numFriendComments", OptI64),
        ("numFriendOfFriendComments", OptI64),
    ]
);
def!(
    LIKES_MESSAGES,
    "personLikesNumMessages",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("frequency", I64),
    ]
);
def!(
    FRIEND_TAGS,
    "personNumFriendTags",
    [
        ("Person1Id", OptI64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numDirectTags", I64),
        ("numFriendTags", OptI64),
    ]
);
def!(
    FOF_FORUMS,
    "personNumFriendOfFriendForums",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numDirectForums", I64),
        ("numFriendForums", OptI64),
        ("numFriendOfFriendForums", OptI64),
    ]
);
def!(
    FOF_COMPANIES,
    "personNumFriendOfFriendCompanies",
    [
        ("Person1Id", I64),
        ("creationDate", Ts),
        ("deletionDate", Ts),
        ("numDirectCompanies", I64),
        ("numFriendCompanies", OptI64),
        ("numFriendOfFriendCompanies", OptI64),
    ]
);
def!(
    SAME_UNIVERSITY,
    "sameUniversityConnected",
    [("PersonId", I64), ("Component", I64), ("count", I64)]
);
def!(
    KNOWS_CONNECTED,
    "personKnowsPersonConnected",
    [("PersonId", I64), ("Component", I64), ("count", I64)]
);
def!(
    KNOWS_DAYS,
    "personKnowsPersonDays",
    [
        ("Person1Id", I64),
        ("Person2Id", I64),
        ("creationDay", Ts),
        ("deletionDay", Ts),
    ]
);
def!(
    STUDY_DAYS,
    "personStudyAtUniversityDays",
    [
        ("PersonId", I64),
        ("UniversityId", I64),
        ("creationDay", Ts),
        ("deletionDay", Ts),
    ]
);
def!(
    WORK_DAYS,
    "personWorkAtCompanyDays",
    [
        ("PersonId", I64),
        ("CompanyId", I64),
        ("creationDay", Ts),
        ("deletionDay", Ts),
    ]
);
def!(
    PERSON_DAYS,
    "personDays",
    [("id", I64), ("creationDay", Ts), ("deletionDay", Ts)]
);

type FactorFn = fn(&Ctx) -> Rows;

/// Every factor table, by name order of the datagen's output.
const FACTORS: [(&TableDef, FactorFn); 34] = [
    (&CITY_NUM_PERSONS, city_num_persons),
    (&CITY_PAIRS, city_pairs),
    (&COMPANY_EMPLOYEES, company_employees),
    (&COUNTRY_NUM_MESSAGES, country_num_messages),
    (&COUNTRY_NUM_PERSONS, country_num_persons),
    (&COUNTRY_PAIRS, country_pairs),
    (&DAY_LEN_MESSAGES, day_len_messages),
    (&DAY_CLASS_MESSAGES, day_class_messages),
    (&DAY_TAG_MESSAGES, day_tag_messages),
    (&DAY_MESSAGES, day_messages),
    (&LANGUAGE_POSTS, language_posts),
    (&LEN_MESSAGES, len_messages),
    (&PEOPLE_2_HOPS, people_2_hops),
    (&PEOPLE_4_HOPS, people_4_hops),
    (&PERSON_DAYS, person_days),
    (&DISJOINT_EMPLOYERS, disjoint_employers),
    (&FIRST_NAMES, first_names),
    (&KNOWS_CONNECTED, knows_connected),
    (&KNOWS_DAYS, knows_days),
    (&LIKES_MESSAGES, likes_messages),
    (&FOF_COMMENTS, fof_comments),
    (&FOF_COMPANIES, fof_companies),
    (&FOF_FORUMS, fof_forums),
    (&FOF_POSTS, fof_posts),
    (&FRIEND_TAGS, friend_tags),
    (&PERSON_NUM_FRIENDS, person_num_friends),
    (&FRIENDS_3, friends_3),
    (&STUDY_DAYS, study_days),
    (&WORK_DAYS, work_days),
    (&SAME_UNIVERSITY, same_university),
    (&CLASS_MESSAGES, class_messages),
    (&CLASS_TAGS, class_tags),
    (&TAG_MESSAGES, tag_messages),
    (&TAG_PERSONS, tag_persons),
];

/// The tables whose row order is not defined by the datagen (a `distinct`,
/// or a graph's connected components): compare them as sets.
pub const UNORDERED: [&str; 3] = [
    "personDisjointEmployerPairs",
    "personKnowsPersonConnected",
    "sameUniversityConnected",
];

fn country_num_persons(c: &Ctx) -> Rows {
    let m = group_count(
        c.g.persons
            .iter()
            .filter_map(|p| Some(c.city_country(p)?.2)),
    );
    counted(&COUNTRY_NUM_PERSONS, m, |id| {
        Some(vec![id.into(), (&c.places[&id].name).into()])
    })
}

fn city_num_persons(c: &Ctx) -> Rows {
    let m = group_count(
        c.g.persons
            .iter()
            .filter(|p| c.of_kind(p.city, "City").is_some())
            .map(|p| p.city),
    );
    counted(&CITY_NUM_PERSONS, m, |id| {
        Some(vec![id.into(), (&c.places[&id].name).into()])
    })
}

fn country_num_messages(c: &Ctx) -> Rows {
    counted(
        &COUNTRY_NUM_MESSAGES,
        c.k.country.iter().map(|(&k, &n)| (k, n)),
        |id| {
            let p = c.of_kind(i32::try_from(id).ok()?, "Country")?;
            Some(vec![id.into(), (&p.name).into()])
        },
    )
}

/// Both directions of every friendship, as person indices.
fn undirected(c: &Ctx) -> impl Iterator<Item = (u32, u32)> + '_ {
    c.g.knows.iter().flat_map(|e| {
        let (a, b) = (c.idx[&e[0]], c.idx[&e[1]]);
        [(a, b), (b, a)]
    })
}

fn city_pairs(c: &Ctx) -> Rows {
    let m = group_count(undirected(c).filter_map(|(a, b)| {
        let x = c.city_country(&c.g.persons[a as usize])?;
        let y = c.city_country(&c.g.persons[b as usize])?;
        (x.0 < y.0).then_some((x.0, y.0))
    }));
    counted(&CITY_PAIRS, m, |(a, b)| {
        let (ca, cb) = (&c.places[&a], &c.places[&b]);
        let (na, nb) = (ca.part_of?, cb.part_of?);
        Some(vec![
            a.into(),
            b.into(),
            (&ca.name).into(),
            (&cb.name).into(),
            na.into(),
            nb.into(),
            (&c.places[&na].name).into(),
            (&c.places[&nb].name).into(),
        ])
    })
}

fn country_pairs(c: &Ctx) -> Rows {
    let m = group_count(undirected(c).filter_map(|(a, b)| {
        let x = c.city_country(&c.g.persons[a as usize])?;
        let y = c.city_country(&c.g.persons[b as usize])?;
        (x.2 < y.2).then_some((x.2, y.2))
    }));
    counted(&COUNTRY_PAIRS, m, |(a, b)| {
        Some(vec![
            a.into(),
            b.into(),
            (&c.places[&a].name).into(),
            (&c.places[&b].name).into(),
        ])
    })
}

fn day_messages(c: &Ctx) -> Rows {
    counted(&DAY_MESSAGES, c.k.day.iter().map(|(&k, &n)| (k, n)), |d| {
        Some(vec![d.into()])
    })
}

fn day_class_messages(c: &Ctx) -> Rows {
    let mut m: Map<(i64, i32), i64> = Map::default();
    for (&(d, tag), &n) in &c.k.day_tag {
        if let Some(t) = c.tag(tag) {
            *m.entry((d, t.2)).or_default() += n;
        }
    }
    counted(&DAY_CLASS_MESSAGES, m, |(d, class)| {
        Some(vec![d.into(), class.into(), c.classes.get(&class)?.into()])
    })
}

fn day_tag_messages(c: &Ctx) -> Rows {
    counted(
        &DAY_TAG_MESSAGES,
        c.k.day_tag.iter().map(|(&k, &n)| (k, n)),
        |(d, tag)| Some(vec![d.into(), tag.into(), (&c.tag(tag)?.1).into()]),
    )
}

fn day_len_messages(c: &Ctx) -> Rows {
    counted(
        &DAY_LEN_MESSAGES,
        c.k.day_len.iter().map(|(&k, &n)| (k, n)),
        |(d, l)| Some(vec![d.into(), l.into()]),
    )
}

fn len_messages(c: &Ctx) -> Rows {
    counted(&LEN_MESSAGES, c.k.len.iter().map(|(&k, &n)| (k, n)), |l| {
        Some(vec![l.into()])
    })
}

fn tag_messages(c: &Ctx) -> Rows {
    counted(&TAG_MESSAGES, c.tag_messages(), |tag| {
        Some(vec![tag.into(), (&c.tag(tag)?.1).into()])
    })
}

fn class_messages(c: &Ctx) -> Rows {
    let mut m: Map<i32, i64> = Map::default();
    for (tag, n) in c.tag_messages() {
        if let Some(t) = c.tag(tag) {
            *m.entry(t.2).or_default() += n;
        }
    }
    counted(&CLASS_MESSAGES, m, |class| {
        Some(vec![class.into(), c.classes.get(&class)?.into()])
    })
}

fn language_posts(c: &Ctx) -> Rows {
    let rows =
        c.k.lang
            .iter()
            .map(|(l, &n)| vec![l.into(), n.into()])
            .collect();
    frequency(&LANGUAGE_POSTS, rows)
}

fn tag_persons(c: &Ctx) -> Rows {
    let m = group_count(c.g.interests.iter().map(|&(_, tag)| tag));
    counted(&TAG_PERSONS, m, |tag| {
        Some(vec![tag.into(), (&c.tag(tag)?.1).into()])
    })
}

fn class_tags(c: &Ctx) -> Rows {
    let m = group_count(c.tags.iter().map(|t| t.2));
    counted(&CLASS_TAGS, m, |class| {
        Some(vec![class.into(), c.classes.get(&class)?.into()])
    })
}

fn company_employees(c: &Ctx) -> Rows {
    let m = group_count(c.g.work.iter().map(|w| w[1]));
    counted(&COMPANY_EMPLOYEES, m, |id| {
        Some(vec![(id as i32).into(), c.companies.get(&id)?.into()])
    })
}

fn first_names(c: &Ctx) -> Rows {
    let mut m: Map<&str, i64> = Map::default();
    for p in &c.g.persons {
        *m.entry(&p.first_name).or_default() += 1;
    }
    let rows = m
        .into_iter()
        .map(|(f, n)| vec![f.into(), n.into()])
        .collect();
    frequency(&FIRST_NAMES, rows)
}

/// `(id, creationDate, deletionDate)` of person `i`.
fn person_key(p: &Person) -> [V<'_>; 3] {
    [p.id.into(), p.created.into(), p.deleted.into()]
}

fn person_num_friends(c: &Ctx) -> Rows {
    let rows =
        c.g.persons
            .iter()
            .zip(&c.adj)
            .map(|(p, f)| {
                let mut r = person_key(p).to_vec();
                r.push((f.len() as i64).into());
                r
            })
            .collect();
    frequency(&PERSON_NUM_FRIENDS, rows)
}

fn likes_messages(c: &Ctx) -> Rows {
    let rows =
        c.g.persons
            .iter()
            .map(|p| {
                let mut r = person_key(p).to_vec();
                r.push(c.k.likes.get(&p.id).copied().unwrap_or(0).into());
                r
            })
            .collect();
    frequency(&LIKES_MESSAGES, rows)
}

/// The friend-of-friend pattern: per person its direct count, the sum of
/// its friends' direct counts and the sum of its friends' friend sums
/// (sums over no friends are null).
fn friends_of_friends(c: &Ctx, def: &TableDef, direct: impl Fn(usize, &Person) -> i64) -> Rows {
    let d: Vec<i64> =
        c.g.persons
            .iter()
            .enumerate()
            .map(|(i, p)| direct(i, p))
            .collect();
    let f1: Vec<Option<i64>> = c
        .adj
        .iter()
        .map(|fs| sum(fs.iter().map(|&f| Some(d[f as usize]))))
        .collect();
    let rows =
        c.g.persons
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let f2 = sum(c.adj[i].iter().map(|&f| f1[f as usize]));
                let mut r = person_key(p).to_vec();
                r.extend([d[i].into(), f1[i].into(), f2.into()]);
                r
            })
            .collect();
    frequency(def, rows)
}

fn friends_3(c: &Ctx) -> Rows {
    friends_of_friends(c, &FRIENDS_3, |i, _| c.adj[i].len() as i64)
}

fn fof_posts(c: &Ctx) -> Rows {
    friends_of_friends(c, &FOF_POSTS, |_, p| {
        c.k.posts.get(&p.id).copied().unwrap_or(0)
    })
}

fn fof_comments(c: &Ctx) -> Rows {
    friends_of_friends(c, &FOF_COMMENTS, |_, p| {
        c.k.comments.get(&p.id).copied().unwrap_or(0)
    })
}

fn fof_forums(c: &Ctx) -> Rows {
    friends_of_friends(c, &FOF_FORUMS, |_, p| {
        c.k.forums.get(&p.id).copied().unwrap_or(0)
    })
}

fn fof_companies(c: &Ctx) -> Rows {
    let m = group_count(c.g.work.iter().map(|w| w[0]));
    friends_of_friends(c, &FOF_COMPANIES, |_, p| m.get(&p.id).copied().unwrap_or(0))
}

fn friend_tags(c: &Ctx) -> Rows {
    // grouped by (interest.PersonId, creationDate, deletionDate): a person
    // without interests has a null id, so such persons with equal dates
    // are one group, and none of them is anyone's friend in the join
    let tags = group_count(c.g.interests.iter().map(|&(p, _)| p));
    let direct = |p: &Person| tags.get(&p.id).copied();
    let mut none: Map<(i64, i64), ()> = Map::default();
    let mut rows = Vec::new();
    for (i, p) in c.g.persons.iter().enumerate() {
        match direct(p) {
            Some(n) => {
                let f = sum(c.adj[i].iter().map(|&f| direct(&c.g.persons[f as usize])));
                rows.push(vec![
                    p.id.into(),
                    p.created.into(),
                    p.deleted.into(),
                    n.into(),
                    f.into(),
                ]);
            }
            None => {
                none.insert((p.created, p.deleted), ());
            }
        }
    }
    for (created, deleted) in none.into_keys() {
        rows.push(vec![
            V::Null,
            created.into(),
            deleted.into(),
            0i64.into(),
            V::Null,
        ]);
    }
    frequency(&FRIEND_TAGS, rows)
}

fn disjoint_employers(c: &Ctx) -> Rows {
    let mut sample: Vec<u32> = (0..c.g.persons.len() as u32).collect();
    sample.sort_unstable_by_key(|&i| c.g.persons[i as usize].id);
    sample.truncate(20);
    let mut works: Map<i64, Vec<i64>> = Map::default();
    for w in &c.g.work {
        works.entry(w[0]).or_default().push(w[1]);
    }
    let mut out = BTreeSet::new();
    for &i in &sample {
        let p = &c.g.persons[i as usize];
        for &f in &c.adj[i as usize] {
            for co in works.get(&c.g.persons[f as usize].id).into_iter().flatten() {
                if let Some(name) = c.companies.get(co) {
                    out.insert(vec![
                        p.id.into(),
                        name.into(),
                        (*co as i32).into(),
                        p.created.into(),
                        p.deleted.into(),
                    ]);
                }
            }
        }
    }
    to_rows(&DISJOINT_EMPLOYERS, &out.into_iter().collect::<Vec<_>>())
}

/// Connected components of `edges` over all persons: `(PersonId,
/// Component, count)`, the component being its least person id (as
/// graphx labels them), persons in raw order.
fn components(c: &Ctx, def: &TableDef, edges: impl Iterator<Item = (u32, u32)>) -> Rows {
    let n = c.g.persons.len();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            p[x as usize] = p[p[x as usize] as usize];
            x = p[x as usize];
        }
        x
    }
    for (a, b) in edges {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra.max(rb) as usize] = ra.min(rb);
        }
    }
    let mut min_id: Map<u32, i64> = Map::default();
    let mut size: Map<u32, i64> = Map::default();
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    for (i, &r) in roots.iter().enumerate() {
        let id = c.g.persons[i].id;
        min_id
            .entry(r)
            .and_modify(|m| *m = (*m).min(id))
            .or_insert(id);
        *size.entry(r).or_default() += 1;
    }
    let rows: Vec<Vec<V>> = roots
        .iter()
        .enumerate()
        .map(|(i, r)| vec![c.g.persons[i].id.into(), min_id[r].into(), size[r].into()])
        .collect();
    to_rows(def, &rows)
}

fn knows_connected(c: &Ctx) -> Rows {
    let edges: Vec<(u32, u32)> =
        c.g.knows
            .iter()
            .map(|e| (c.idx[&e[0]], c.idx[&e[1]]))
            .collect();
    components(c, &KNOWS_CONNECTED, edges.into_iter())
}

fn same_university(c: &Ctx) -> Rows {
    let mut study: Map<i64, Vec<&[i64; 4]>> = Map::default();
    for s in &c.g.study {
        study.entry(s[0]).or_default().push(s);
    }
    let mut edges = Vec::new();
    for e in &c.g.knows {
        for (a, b) in [(e[0], e[1]), (e[1], e[0])] {
            for s1 in study.get(&a).into_iter().flatten() {
                for s2 in study.get(&b).into_iter().flatten() {
                    let created = e[2].max(s1[2]).max(s2[2]);
                    let deleted = e[3].min(s1[3]).min(s2[3]);
                    if s1[1] == s2[1] && created < deleted {
                        edges.push((c.idx[&a], c.idx[&b]));
                    }
                }
            }
        }
    }
    components(c, &SAME_UNIVERSITY, edges.into_iter())
}

fn days(def: &TableDef, rows: impl Iterator<Item = [i64; 4]>) -> Rows {
    let rows: Vec<Vec<V>> = rows
        .map(|r| vec![r[0].into(), r[1].into(), day(r[2]).into(), day(r[3]).into()])
        .collect();
    to_rows(def, &rows)
}

fn knows_days(c: &Ctx) -> Rows {
    days(&KNOWS_DAYS, c.g.knows.iter().copied())
}

fn study_days(c: &Ctx) -> Rows {
    days(&STUDY_DAYS, c.g.study.iter().copied())
}

fn work_days(c: &Ctx) -> Rows {
    days(&WORK_DAYS, c.g.work.iter().copied())
}

fn person_days(c: &Ctx) -> Rows {
    let rows: Vec<Vec<V>> =
        c.g.persons
            .iter()
            .map(|p| vec![p.id.into(), day(p.created).into(), day(p.deleted).into()])
            .collect();
    to_rows(&PERSON_DAYS, &rows)
}

// ---- people2Hops / people4Hops ----

fn people_2_hops(c: &Ctx) -> Rows {
    hops(c, &PEOPLE_2_HOPS, 2)
}

fn people_4_hops(c: &Ctx) -> Rows {
    hops(c, &PEOPLE_4_HOPS, 4)
}

/// `nHops(n)` from a sample of the persons in China (country 1): pairs
/// `(s, x)` where the shortest walk from `s` to `x` has `n` steps (so `s`
/// is two steps from itself), sorted, then sampled down to ~10,000.
fn hops(c: &Ctx, def: &TableDef, n: u8) -> Rows {
    let mut china: Vec<u32> = (0..c.g.persons.len() as u32)
        .filter(|&i| {
            let p = &c.g.persons[i as usize];
            c.of_kind(p.city, "City").and_then(|x| x.part_of) == Some(1)
        })
        .collect();
    china.sort_unstable_by_key(|&i| c.g.persons[i as usize].id);
    let count = china.len() as f64;
    let size = count.min(100f64.max(count / (1.0 + (count * 1e-3).exp()) * 2.0));
    let sources = bernoulli(&china, (size / count).min(1.0));

    // one BFS per source, sources in parallel: `seen[x] == k` marks `x`
    // reached from the `k`-th source
    let per = sources.len().div_ceil(64).max(1);
    let mut pairs: Vec<(i64, i64, u32)> = par_map(sources.len().div_ceil(per), c.threads, |b| {
        let n_persons = c.g.persons.len();
        let mut seen = vec![u32::MAX; n_persons];
        let (mut frontier, mut next) = (Vec::new(), Vec::new());
        let mut out = Vec::new();
        for (k, &s) in sources.iter().enumerate().skip(b * per).take(per) {
            let mark = k as u32;
            frontier.clear();
            for &f in &c.adj[s as usize] {
                if seen[f as usize] != mark {
                    seen[f as usize] = mark;
                    frontier.push(f);
                }
            }
            for _ in 2..=n {
                next.clear();
                for &x in &frontier {
                    for &f in &c.adj[x as usize] {
                        if seen[f as usize] != mark {
                            seen[f as usize] = mark;
                            next.push(f);
                        }
                    }
                }
                swap(&mut frontier, &mut next);
            }
            let sid = c.g.persons[s as usize].id;
            out.extend(
                frontier
                    .iter()
                    .map(|&x| (sid, c.g.persons[x as usize].id, s)),
            );
        }
        out
    })
    .into_iter()
    .flatten()
    .collect();
    pairs.sort_unstable();
    let pairs = bernoulli(&pairs, (10000.0 / pairs.len() as f64).min(1.0));
    let rows: Vec<Vec<V>> = pairs
        .iter()
        .map(|&(a, b, s)| {
            // the datagen joins person 2 on Person1Id too
            let p = &c.g.persons[s as usize];
            vec![
                a.into(),
                b.into(),
                p.created.into(),
                p.deleted.into(),
                p.created.into(),
                p.deleted.into(),
            ]
        })
        .collect();
    to_rows(def, &rows)
}

/// Spark's `sample(fraction, seed = 42)` of one partition (index 0): a
/// `BernoulliCellSampler` keeping the items whose `XORShiftRandom` draw is
/// below `fraction`.
fn bernoulli<T: Copy>(items: &[T], fraction: f64) -> Vec<T> {
    if fraction <= 0.0 {
        return Vec::new();
    }
    let mut rng = XorShift::new(42);
    items
        .iter()
        .copied()
        .filter(|_| rng.next_double() < fraction)
        .collect()
}

/// Spark's `org.apache.spark.util.random.XORShiftRandom`.
struct XorShift(u64);

impl XorShift {
    fn new(seed: i64) -> Self {
        XorShift(hash_seed(seed))
    }

    fn next(&mut self, bits: u32) -> u32 {
        let mut s = self.0;
        s ^= s << 21;
        s ^= s >> 35;
        s ^= s << 4;
        self.0 = s;
        (s & ((1u64 << bits) - 1)) as u32
    }

    /// `java.util.Random.nextDouble` over this `next`.
    fn next_double(&mut self) -> f64 {
        let hi = self.next(26) as u64;
        let lo = self.next(27) as u64;
        ((hi << 27) + lo) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

/// `XORShiftRandom.hashSeed`: MurmurHash3 of the seed's big-endian bytes,
/// twice.
fn hash_seed(seed: i64) -> u64 {
    let b = seed.to_be_bytes();
    let lo = bytes_hash(&b, 0x3c07_4a61);
    let hi = bytes_hash(&b, lo);
    ((hi as u32 as u64) << 32) | (lo as u32 as u64)
}

/// Scala's `MurmurHash3.bytesHash`.
fn bytes_hash(data: &[u8], seed: i32) -> i32 {
    fn mix_last(h: u32, k: u32) -> u32 {
        let k = k
            .wrapping_mul(0xcc9e_2d51)
            .rotate_left(15)
            .wrapping_mul(0x1b87_3593);
        h ^ k
    }
    let mut h = seed as u32;
    let (words, tail) = data.as_chunks::<4>();
    for w in words {
        h = mix_last(h, u32::from_le_bytes(*w))
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let mut k = 0u32;
        for (i, &b) in tail.iter().enumerate() {
            k ^= (b as u32) << (8 * i);
        }
        h = mix_last(h, k);
    }
    let mut h = h ^ data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^= h >> 16;
    h as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn xorshift_matches_spark() {
        // org.apache.spark.util.random.XORShiftRandom(42).nextDouble(), Spark 3.2.1
        let mut r = XorShift::new(42);
        let want = [
            0.619189370225301,
            0.5096018842446481,
            0.8325259388871524,
            0.26322809041172357,
            0.6702867696264135,
        ];
        for w in want {
            assert_eq!(r.next_double(), w);
        }
    }

    #[test]
    fn sums_are_null_over_nothing() {
        assert_eq!(sum([]), None);
        assert_eq!(sum([None, None]), None);
        assert_eq!(sum([None, Some(2), Some(3)]), Some(5));
    }

    #[test]
    fn frequency_order() {
        let mut rows = vec![
            vec![V::Int(2), V::Null],
            vec![V::Int(1), V::Int(5)],
            vec![V::Null, V::Int(5)],
            vec![V::Int(0), V::Int(7)],
        ];
        rows.sort_by(|a, b| freq_order(a, b));
        assert_eq!(
            rows,
            vec![
                vec![V::Int(0), V::Int(7)],
                vec![V::Null, V::Int(5)],
                vec![V::Int(1), V::Int(5)],
                vec![V::Int(2), V::Null],
            ]
        );
    }

    #[test]
    fn unordered_names_exist() {
        for n in UNORDERED {
            assert!(FACTORS.iter().any(|(d, _)| d.name == n));
        }
        let names: HashSet<&str> = FACTORS.iter().map(|(d, _)| d.name).collect();
        assert_eq!(names.len(), FACTORS.len());
    }
}
