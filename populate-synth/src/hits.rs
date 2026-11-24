//! ClickBench-like web-analytics hits: 30 of the `hits` table's 105
//! columns, under the same names and types.
//!
//! A hit is a pageview of one page of one site (counter) by one user:
//!
//! - `CounterID` is Zipf-skewed over the sites (a few sites get most of
//!   the traffic) and each site has its own domain; the page is
//!   Zipf-skewed within the site, and `URL`, `Title` and `URLHash` are
//!   functions of `(site, page)`, so popular URLs repeat;
//! - `UserID` is Zipf-skewed over a fixed population, and everything about
//!   the user's device and place (`RegionID`, `OS`, `UserAgent`,
//!   `IsMobile`, `MobilePhoneModel`, screen and window sizes, `ClientIP`)
//!   is a function of the user, so it is consistent across their hits;
//! - the traffic source decides the rest: search hits carry a search
//!   engine, a search phrase and a search-results `Referer`, link hits the
//!   URL of a page on a uniformly drawn site (possibly the same one), ad
//!   hits an `AdvEngineID`, direct hits nothing;
//! - `EventTime` is non-decreasing over the rows (from 2013-07-01, with a
//!   daily profile), and `EventDate` is its day.

use crate::Shape;
use crate::kernel::{
    Pool, Quantiles, ViewWriter, Weighted, Zipf, prim_ref, push_u64, threshold, unit,
};
use arrow_array::types::{Date32Type, Int16Type, Int32Type, Int64Type, TimestampSecondType};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use populate_table::{Stream, mix64};
use std::sync::Arc;

/// 2013-07-01T00:00:00Z.
const DEFAULT_START_SECS: i64 = 1_372_636_800;

/// Words for domains, page paths, titles and search phrases.
const WORDS: [&str; 128] = [
    "news",
    "weather",
    "cars",
    "auto",
    "sale",
    "buy",
    "cheap",
    "best",
    "free",
    "online",
    "game",
    "games",
    "music",
    "video",
    "movie",
    "film",
    "photo",
    "map",
    "city",
    "hotel",
    "travel",
    "flight",
    "ticket",
    "bank",
    "loan",
    "credit",
    "card",
    "job",
    "work",
    "home",
    "house",
    "rent",
    "flat",
    "garden",
    "tools",
    "phone",
    "mobile",
    "tablet",
    "laptop",
    "computer",
    "price",
    "review",
    "shop",
    "store",
    "market",
    "food",
    "recipe",
    "cook",
    "pizza",
    "coffee",
    "tea",
    "sport",
    "football",
    "hockey",
    "tennis",
    "health",
    "doctor",
    "clinic",
    "school",
    "study",
    "course",
    "book",
    "books",
    "library",
    "kids",
    "baby",
    "toys",
    "fashion",
    "dress",
    "shoes",
    "watch",
    "beauty",
    "hair",
    "salon",
    "dating",
    "chat",
    "forum",
    "blog",
    "mail",
    "search",
    "download",
    "software",
    "app",
    "android",
    "windows",
    "linux",
    "code",
    "data",
    "cloud",
    "server",
    "domain",
    "web",
    "site",
    "page",
    "portal",
    "info",
    "guide",
    "help",
    "support",
    "service",
    "repair",
    "parts",
    "tyres",
    "bike",
    "boat",
    "fish",
    "pets",
    "dog",
    "cat",
    "horse",
    "farm",
    "energy",
    "solar",
    "oil",
    "gold",
    "stock",
    "trade",
    "money",
    "tax",
    "law",
    "insurance",
    "estate",
    "realty",
    "moscow",
    "london",
    "paris",
    "berlin",
    "tokyo",
];

const TLDS: [&str; 6] = [".ru", ".com", ".net", ".org", ".info", ".ua"];

/// Search engines, ids 1.. (a search hit's `Referer` is on its domain).
const ENGINES: [&str; 5] = [
    "search.example.ru",
    "find.example.com",
    "go.example.net",
    "look.example.org",
    "seek.example.ua",
];

const PHONES: [&str; 12] = [
    "iPhone",
    "iPad",
    "Galaxy S4",
    "Galaxy Note",
    "Nexus 4",
    "Lumia 920",
    "HTC One",
    "Xperia Z",
    "Optimus G",
    "Desire",
    "Kindle",
    "Nokia",
];

const DESKTOP_SCREENS: [(i16, i16); 8] = [
    (1920, 1080),
    (1366, 768),
    (1280, 1024),
    (1440, 900),
    (1600, 900),
    (1280, 800),
    (1680, 1050),
    (2560, 1440),
];

const MOBILE_SCREENS: [(i16, i16); 5] =
    [(320, 568), (375, 667), (360, 640), (768, 1024), (414, 736)];

/// Hits per hour of day.
const HOURLY: [f64; 24] = [
    2.0, 1.4, 1.0, 0.8, 0.8, 1.0, 1.6, 2.6, 3.8, 4.8, 5.4, 5.6, 5.6, 5.6, 5.5, 5.4, 5.3, 5.2, 5.0,
    5.0, 5.2, 5.0, 4.2, 3.2,
];

/// Web hits. See the [module docs](self).
#[derive(Debug, Clone)]
pub struct Hits {
    seed: u64,
    hits_per_day: u64,
    start_secs: i64,
    schema: SchemaRef,
    s: Streams,
    tod: Quantiles,
    counter: Zipf,
    page: Zipf,
    user: Zipf,
    region: Zipf,
    phrase: Zipf,
    adv: Zipf,
    source: Weighted<i16>,
    engine: Weighted<i16>,
    os: Weighted<i16>,
    agent: Weighted<i16>,
    desktop: Weighted<usize>,
    /// Domain of each site, by `CounterID - 1`.
    domains: Vec<String>,
    titles: Pool,
    phrases: Pool,
    /// Each phrase as a query string (`+` for spaces).
    queries: Vec<Vec<u8>>,
    phones: Pool,
}

#[derive(Debug, Clone, Copy)]
struct Streams {
    watch: Stream,
    time: Stream,
    counter: Stream,
    page: Stream,
    user: Stream,
    source: Stream,
    engine: Stream,
    phrase: Stream,
    adv: Stream,
    link_counter: Stream,
    link_page: Stream,
    flags: Stream,
    /// Per-user attributes (indexed by user rank, not row).
    u_region: Stream,
    u_device: Stream,
    u_screen: Stream,
    u_window: Stream,
    u_ip: Stream,
    u_id: Stream,
    /// Per-site attributes (indexed by counter).
    c_domain: Stream,
    /// Per-page attributes (indexed by `(counter, page)`).
    p_title: Stream,
    p_path: Stream,
}

impl Hits {
    /// 10,000 sites, 1,000,000 users, 3,000,000 hits a day from
    /// 2013-07-01.
    pub fn new(seed: u64) -> Self {
        Self::with_sizes(seed, 10_000, 1_000_000)
    }

    /// `sites` counters and a population of `users`.
    pub fn with_sizes(seed: u64, sites: u64, users: u64) -> Self {
        let st = |n| Stream::new(seed, n);
        let s = Streams {
            watch: st("hits.watch"),
            time: st("hits.time"),
            counter: st("hits.counter"),
            page: st("hits.page"),
            user: st("hits.user"),
            source: st("hits.source"),
            engine: st("hits.engine"),
            phrase: st("hits.phrase"),
            adv: st("hits.adv"),
            link_counter: st("hits.link_counter"),
            link_page: st("hits.link_page"),
            flags: st("hits.flags"),
            u_region: st("hits.u_region"),
            u_device: st("hits.u_device"),
            u_screen: st("hits.u_screen"),
            u_window: st("hits.u_window"),
            u_ip: st("hits.u_ip"),
            u_id: st("hits.u_id"),
            c_domain: st("hits.c_domain"),
            p_title: st("hits.p_title"),
            p_path: st("hits.p_path"),
        };
        let word = |b: u64| WORDS[(b % WORDS.len() as u64) as usize];
        let domains = (0..sites)
            .map(|c| {
                let b = s.c_domain.at(c);
                // two words plus the site number keeps every domain distinct
                format!(
                    "{}{}{c}{}",
                    word(b),
                    word(b >> 8),
                    TLDS[((b >> 16) % TLDS.len() as u64) as usize]
                )
            })
            .collect();
        let gen_phrase = |st: Stream, i: u64, min: u64, extra: u64, cap: bool| {
            let b = st.at(i);
            let n = min + (b >> 60) % (extra + 1);
            let mut out = String::new();
            for k in 0..n {
                if k > 0 {
                    out.push(' ');
                }
                let w = word(mix64(b ^ k));
                if cap && k == 0 {
                    let mut c = w.chars();
                    let f = c.next().unwrap().to_ascii_uppercase();
                    out.push(f);
                    out.push_str(c.as_str());
                } else {
                    out.push_str(w);
                }
            }
            out
        };
        let phrase_pool = st("hits.phrase_pool");
        let title_pool = st("hits.title_pool");
        let phrases: Vec<String> = (0..50_000)
            .map(|i| gen_phrase(phrase_pool, i, 1, 3, false))
            .collect();
        let queries = phrases
            .iter()
            .map(|p| p.replace(' ', "+").into_bytes())
            .collect();
        Hits {
            seed,
            hits_per_day: 3_000_000,
            start_secs: DEFAULT_START_SECS,
            schema: schema(),
            s,
            tod: Quantiles::histogram(0.0, 86_400.0, &HOURLY),
            counter: Zipf::new(sites, 1.3),
            page: Zipf::new(100_000, 1.05),
            user: Zipf::new(users, 0.9),
            region: Zipf::new(10_000, 1.2),
            phrase: Zipf::new(50_000, 1.0),
            adv: Zipf::new(30, 1.5),
            source: Weighted::new(&[
                (-1, 0.03),
                (0, 0.38),
                (1, 0.17),
                (2, 0.3),
                (3, 0.08),
                (4, 0.02),
                (5, 0.02),
            ]),
            engine: Weighted::new(&[(1, 0.6), (2, 0.25), (3, 0.08), (4, 0.05), (5, 0.02)]),
            os: Weighted::new(&[(2, 0.45), (44, 0.2), (126, 0.15), (16, 0.1), (3, 0.1)]),
            agent: Weighted::new(&[(7, 0.3), (2, 0.25), (3, 0.2), (12, 0.15), (40, 0.1)]),
            desktop: Weighted::new(&[
                (0, 0.3),
                (1, 0.25),
                (2, 0.12),
                (3, 0.1),
                (4, 0.08),
                (5, 0.06),
                (6, 0.05),
                (7, 0.04),
            ]),
            domains,
            titles: Pool::new((0..20_000).map(|i| gen_phrase(title_pool, i, 3, 5, true))),
            phrases: Pool::new(&phrases),
            queries,
            phones: Pool::new(PHONES),
        }
    }

    /// Hits per day (sets how many days the rows span).
    pub fn hits_per_day(mut self, n: u64) -> Self {
        assert!(n > 0, "hits_per_day must be > 0");
        self.hits_per_day = n;
        self
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// `https://<domain>/<word>/<word>/<page>.html` of `(counter, page)`.
    fn write_url(&self, out: &mut Vec<u8>, counter: u64, page: u64) {
        let b = self.s.p_path.at(counter << 20 ^ page);
        out.extend_from_slice(b"https://");
        out.extend_from_slice(self.domains[counter as usize].as_bytes());
        out.push(b'/');
        out.extend_from_slice(WORDS[(b & 127) as usize].as_bytes());
        out.push(b'/');
        out.extend_from_slice(WORDS[((b >> 7) & 127) as usize].as_bytes());
        out.push(b'/');
        push_u64(out, page);
        out.extend_from_slice(b".html");
    }
}

fn url_hash(counter: u64, page: u64) -> i64 {
    (mix64(counter << 20 ^ page ^ 0x5552_4c00) >> 1) as i64
}

fn schema() -> SchemaRef {
    use DataType::*;
    let cols: [(&str, DataType); 30] = [
        ("WatchID", Int64),
        ("JavaEnable", Int16),
        ("Title", Utf8View),
        ("GoodEvent", Int16),
        ("EventTime", Timestamp(TimeUnit::Second, None)),
        ("EventDate", Date32),
        ("CounterID", Int32),
        ("ClientIP", Int32),
        ("RegionID", Int32),
        ("UserID", Int64),
        ("OS", Int16),
        ("UserAgent", Int16),
        ("URL", Utf8View),
        ("Referer", Utf8View),
        ("IsRefresh", Int16),
        ("ResolutionWidth", Int16),
        ("ResolutionHeight", Int16),
        ("IsMobile", Int16),
        ("MobilePhoneModel", Utf8View),
        ("TraficSourceID", Int16),
        ("SearchEngineID", Int16),
        ("SearchPhrase", Utf8View),
        ("AdvEngineID", Int16),
        ("WindowClientWidth", Int16),
        ("WindowClientHeight", Int16),
        ("IsLink", Int16),
        ("IsDownload", Int16),
        ("DontCountHits", Int16),
        ("URLHash", Int64),
        ("RefererHash", Int64),
    ];
    Arc::new(Schema::new(
        cols.into_iter()
            .map(|(n, t)| Field::new(n, t, false))
            .collect::<Vec<_>>(),
    ))
}

fn i16s(v: Vec<i16>) -> ArrayRef {
    prim_ref::<Int16Type>(v, None)
}

impl Shape for Hits {
    fn name(&self) -> &'static str {
        "hits"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn batch(&self, start: u64, len: usize) -> RecordBatch {
        let s = &self.s;
        let rows = || start..start + len as u64;
        let per_day = self.hits_per_day;

        let watch: Vec<i64> = rows().map(|r| (s.watch.at(r) >> 1) as i64).collect();
        let time: Vec<i64> = rows()
            .map(|r| {
                let (day, k) = (r / per_day, r % per_day);
                let p = (k as f64 + unit(s.time.at(r))) / per_day as f64;
                self.start_secs + day as i64 * 86_400 + self.tod.at(p) as i64
            })
            .collect();
        let date: Vec<i32> = time.iter().map(|t| t.div_euclid(86_400) as i32).collect();

        let counter: Vec<u64> = rows()
            .map(|r| self.counter.sample(s.counter.at(r)))
            .collect();
        let page: Vec<u64> = rows().map(|r| self.page.sample(s.page.at(r))).collect();
        let user: Vec<u64> = rows().map(|r| self.user.sample(s.user.at(r))).collect();

        // per-user attributes
        let user_id: Vec<i64> = user.iter().map(|&u| (s.u_id.at(u) >> 1) as i64).collect();
        let region: Vec<i32> = user
            .iter()
            .map(|&u| self.region.sample(s.u_region.at(u)) as i32 + 1)
            .collect();
        let ip: Vec<i32> = user.iter().map(|&u| s.u_ip.at(u) as i32).collect();
        let device: Vec<u64> = user.iter().map(|&u| s.u_device.at(u)).collect();
        let mobile_t = threshold(0.2);
        let is_mobile: Vec<i16> = device.iter().map(|&d| (d < mobile_t) as i16).collect();
        let os: Vec<i16> = device.iter().map(|&d| self.os.sample(d << 8)).collect();
        let agent: Vec<i16> = device.iter().map(|&d| self.agent.sample(d << 16)).collect();
        // a middle bit: the top bits decide IsMobile
        let java: Vec<i16> = device.iter().map(|&d| ((d >> 40) & 1) as i16).collect();
        let phone_view: Vec<u32> = device
            .iter()
            .zip(&is_mobile)
            .map(|(&d, &m)| {
                if m == 1 {
                    ((d >> 24) % PHONES.len() as u64) as u32
                } else {
                    Pool::EMPTY
                }
            })
            .collect();
        let mut res_w = Vec::with_capacity(len);
        let mut res_h = Vec::with_capacity(len);
        let mut win_w = Vec::with_capacity(len);
        let mut win_h = Vec::with_capacity(len);
        for (&u, &m) in user.iter().zip(&is_mobile) {
            let b = s.u_screen.at(u);
            let (w, h) = if m == 1 {
                MOBILE_SCREENS[(b % MOBILE_SCREENS.len() as u64) as usize]
            } else {
                DESKTOP_SCREENS[self.desktop.index(b)]
            };
            let wb = s.u_window.at(u);
            res_w.push(w);
            res_h.push(h);
            // the browser window is the screen less some chrome
            win_w.push(w - (wb % 64) as i16 * (1 - m) * 4);
            win_h.push(h - 60 - ((wb >> 8) % 200) as i16 * (1 - m));
        }

        // traffic source and what it brings
        let source: Vec<i16> = rows().map(|r| self.source.sample(s.source.at(r))).collect();
        let engine: Vec<i16> = rows()
            .zip(&source)
            .map(|(r, &src)| {
                if src == 2 {
                    self.engine.sample(s.engine.at(r))
                } else {
                    0
                }
            })
            .collect();
        let phrase_idx: Vec<usize> = rows()
            .map(|r| self.phrase.sample(s.phrase.at(r)) as usize)
            .collect();
        let phrase: Vec<u32> = engine
            .iter()
            .zip(&phrase_idx)
            .map(|(&e, &p)| if e > 0 { p as u32 } else { Pool::EMPTY })
            .collect();
        let adv: Vec<i16> = rows()
            .zip(&source)
            .map(|(r, &src)| {
                if src == 3 {
                    self.adv.sample(s.adv.at(r)) as i16 + 1
                } else {
                    0
                }
            })
            .collect();

        let title_empty = threshold(0.06);
        let title: Vec<u32> = counter
            .iter()
            .zip(&page)
            .map(|(&c, &p)| {
                let b = s.p_title.at(c << 20 ^ p);
                if b < title_empty {
                    Pool::EMPTY
                } else {
                    (b % self.titles.len() as u64) as u32
                }
            })
            .collect();

        let mut url = ViewWriter::with_capacity(len, len * 48);
        for (&c, &p) in counter.iter().zip(&page) {
            let at = url.buf().len();
            self.write_url(url.buf(), c, p);
            url.finish_value(at);
        }
        let url_hashes: Vec<i64> = counter
            .iter()
            .zip(&page)
            .map(|(&c, &p)| url_hash(c, p))
            .collect();

        let mut referer = ViewWriter::with_capacity(len, len * 40);
        let mut referer_hash = Vec::with_capacity(len);
        let sites = self.domains.len() as u64;
        for (i, r) in rows().enumerate() {
            let at = referer.buf().len();
            match source[i] {
                2 => {
                    let b = referer.buf();
                    b.extend_from_slice(b"https://");
                    b.extend_from_slice(ENGINES[engine[i] as usize - 1].as_bytes());
                    b.extend_from_slice(b"/search?q=");
                    b.extend_from_slice(&self.queries[phrase_idx[i]]);
                    referer.finish_value(at);
                    referer_hash
                        .push((mix64(phrase_idx[i] as u64 ^ (engine[i] as u64) << 32) >> 1) as i64);
                }
                1 | 4 | 5 => {
                    let c = s.link_counter.at(r) % sites;
                    let p = self.page.sample(s.link_page.at(r));
                    self.write_url(referer.buf(), c, p);
                    referer.finish_value(at);
                    referer_hash.push(url_hash(c, p));
                }
                _ => {
                    referer.push_empty();
                    referer_hash.push(0);
                }
            }
        }

        let flags: Vec<u64> = rows().map(|r| s.flags.at(r)).collect();
        let flag = |shift: u32, p: f64| -> Vec<i16> {
            let t = threshold(p);
            flags.iter().map(|&b| ((b << shift) < t) as i16).collect()
        };

        let cols: Vec<ArrayRef> = vec![
            prim_ref::<Int64Type>(watch, None),
            i16s(java),
            self.titles.column(&title, None),
            i16s(vec![1; len]),
            prim_ref::<TimestampSecondType>(time, None),
            prim_ref::<Date32Type>(date, None),
            prim_ref::<Int32Type>(counter.iter().map(|&c| c as i32 + 1).collect(), None),
            prim_ref::<Int32Type>(ip, None),
            prim_ref::<Int32Type>(region, None),
            prim_ref::<Int64Type>(user_id, None),
            i16s(os),
            i16s(agent),
            url.finish(None),
            referer.finish(None),
            i16s(flag(0, 0.08)),
            i16s(res_w),
            i16s(res_h),
            i16s(is_mobile),
            self.phones.column(&phone_view, None),
            i16s(source),
            i16s(engine),
            self.phrases.column(&phrase, None),
            i16s(adv),
            i16s(win_w),
            i16s(win_h),
            i16s(flag(16, 0.01)),
            i16s(flag(32, 0.005)),
            i16s(flag(48, 0.03)),
            prim_ref::<Int64Type>(url_hashes, None),
            prim_ref::<Int64Type>(referer_hash, None),
        ];
        RecordBatch::try_new(self.schema.clone(), cols).expect("columns match schema")
    }
}
