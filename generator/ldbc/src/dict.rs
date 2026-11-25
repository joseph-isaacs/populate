// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! The datagen's dictionaries (`src/main/resources/dictionaries`), loaded
//! the way its Java classes load them.

use crate::dates::Dates;
use crate::fdlibm::{floor_div, log1p};
use crate::java::{binary_search_f32, lines, parse_f32, parse_f64, parse_i32, split, trim};
use crate::jrand::{Aspect, Farm, JavaRandom};
use crate::params::Params;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread::{available_parallelism, scope};
use unicode_normalization::UnicodeNormalization;

fn read(dir: &Path, rel: &str) -> io::Result<String> {
    let p: PathBuf = dir.join(rel);
    let b = fs::read(&p).map_err(|e| io::Error::new(e.kind(), format!("{}: {e}", p.display())))?;
    Ok(match String::from_utf8(b) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    })
}

/// `ssj` `PowerDist(a, b, c).inverseF(u)`.
#[inline]
pub fn power_inverse(a: f64, b: f64, c: f64, u: f64) -> f64 {
    if u == 0.0 {
        a
    } else if u == 1.0 {
        b
    } else {
        a + (b - a) * u.powf(1.0 / c)
    }
}

/// `f` of every line of `txt` (as [`lines`] splits it), parsed in pieces on
/// all cores: the pieces' results, in file order.
fn parallel_lines<T: Send>(txt: &str, f: impl Fn(&str) -> T + Sync) -> Vec<Vec<T>> {
    let n = available_parallelism().map_or(1, |t| t.get());
    let b = txt.as_bytes();
    let mut cuts = vec![0];
    for k in 1..n {
        let mut i = (b.len() * k / n)
            .max(*cuts.last().expect("cut"))
            .clamp(1.min(b.len()), b.len());
        // cut after a line feed
        while i < b.len() && b[i - 1] != b'\n' {
            i += 1;
        }
        cuts.push(i);
    }
    cuts.push(b.len());
    scope(|s| {
        let hs: Vec<_> = cuts
            .windows(2)
            .map(|w| {
                let piece = &txt[w[0]..w[1]];
                let f = &f;
                s.spawn(move || lines(piece).into_iter().map(f).collect::<Vec<T>>())
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("parser")).collect()
    })
}

/// `PowerDistribution`: a power law on `[a, b]`.
#[derive(Debug, Clone, Copy)]
pub struct PowerDistribution {
    a: f64,
    b: f64,
    c: f64,
}

impl PowerDistribution {
    pub fn new(a: f64, b: f64, c: f64) -> Self {
        PowerDistribution { a, b, c }
    }

    #[inline]
    pub fn value(&self, r: &mut JavaRandom) -> i32 {
        self.double(r) as i32
    }

    #[inline]
    pub fn double(&self, r: &mut JavaRandom) -> f64 {
        power_inverse(self.a, self.b, self.c, r.next_double())
    }
}

// ---------------------------------------------------------------- places

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaceType {
    City,
    Country,
    Continent,
}

impl PlaceType {
    pub fn as_str(self) -> &'static str {
        match self {
            PlaceType::City => "City",
            PlaceType::Country => "Country",
            PlaceType::Continent => "Continent",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Place {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
    pub population: i64,
    pub kind: PlaceType,
    pub zid: i32,
}

/// `PlaceDictionary`. Place ids are positions: countries, then cities, then
/// continents.
#[derive(Debug)]
pub struct Places {
    pub places: Vec<Place>,
    is_part_of: Vec<i32>,
    num_countries: usize,
    cities_by_country: Vec<Vec<i32>>,
    city_names: HashMap<String, i32>,
    country_names: HashMap<String, i32>,
    cumulative: Vec<f32>,
    sorted_place: Vec<i32>,
}

/// `Math.round(double)` as an `int`: nearest, ties towards +infinity.
fn java_round(a: f64) -> i32 {
    let f = a.floor();
    (if a - f >= 0.5 { f + 1.0 } else { f }) as i64 as i32
}

/// `ZOrder(8).getZValue(x, y)`: bits of `x` and `y` interleaved, `x` first.
fn z_value(x: i32, y: i32) -> i32 {
    let bx = format!("{:08b}", x);
    let by = format!("{:08b}", y);
    let (bx, by) = (bx.as_bytes(), by.as_bytes());
    let mut z = 0i32;
    for i in 0..bx.len() {
        z = (z << 1) | (bx[i] - b'0') as i32;
        z = (z << 1) | (by[i] - b'0') as i32;
    }
    z
}

impl Places {
    fn load(dir: &Path) -> io::Result<Self> {
        let countries_txt = read(dir, "dictionaries/dicLocations.txt")?;
        let mut p = Places {
            places: Vec::new(),
            is_part_of: Vec::new(),
            num_countries: 0,
            cities_by_country: Vec::new(),
            city_names: HashMap::new(),
            country_names: HashMap::new(),
            cumulative: Vec::new(),
            sorted_place: Vec::new(),
        };
        for line in lines(&countries_txt) {
            let d = split(line, " ");
            let id = p.places.len() as i32;
            p.places.push(Place {
                name: d[1].to_string(),
                lat: parse_f64(d[2]),
                lon: parse_f64(d[3]),
                population: parse_i32(d[4]) as i64,
                kind: PlaceType::Country,
                zid: 0,
            });
            p.country_names.insert(d[1].to_string(), id);
            p.cumulative.push(parse_f32(d[5]));
            p.cities_by_country.push(Vec::new());
        }
        p.num_countries = p.places.len();
        // orderByZ
        let mut z: Vec<(i32, i32)> = (0..p.num_countries)
            .map(|i| {
                let loc = &p.places[i];
                let zv = z_value(
                    (java_round(loc.lon) + 180) / 2,
                    (java_round(loc.lat) + 180) / 2,
                );
                (i as i32, zv)
            })
            .collect();
        z.sort_by_key(|&(_, zv)| zv); // stable, like Arrays.sort(Object[])
        for (rank, &(id, _)) in z.iter().enumerate() {
            p.places[id as usize].zid = rank as i32;
        }
        p.sorted_place = z.iter().map(|&(id, _)| id).collect();
        // cities
        let cities = read(dir, "dictionaries/citiesByCountry.txt")?;
        for line in lines(&cities) {
            let d = split(line, " ");
            if let Some(&country) = p.country_names.get(d[0])
                && !p.city_names.contains_key(d[1])
            {
                let id = p.places.len() as i32;
                let (lat, lon) = (
                    p.places[country as usize].lat,
                    p.places[country as usize].lon,
                );
                p.places.push(Place {
                    name: d[1].to_string(),
                    lat,
                    lon,
                    population: -1,
                    kind: PlaceType::City,
                    zid: 0,
                });
                set_part_of(&mut p.is_part_of, id, country);
                p.cities_by_country[country as usize].push(id);
                p.city_names.insert(d[1].to_string(), id);
            }
        }
        // continents
        let mut treated: HashMap<String, i32> = HashMap::new();
        for line in lines(&countries_txt) {
            let d = split(line, " ");
            let country = p.country_names[d[1]];
            let cont = match treated.get(d[0]) {
                Some(&c) => c,
                None => {
                    let id = p.places.len() as i32;
                    p.places.push(Place {
                        name: d[0].to_string(),
                        lat: parse_f64(d[2]),
                        lon: parse_f64(d[3]),
                        population: 0,
                        kind: PlaceType::Continent,
                        zid: 0,
                    });
                    treated.insert(d[0].to_string(), id);
                    id
                }
            };
            let pop = p.places[cont as usize].population + p.places[country as usize].population;
            p.places[cont as usize].population = pop;
            set_part_of(&mut p.is_part_of, country, cont);
        }
        p.is_part_of.resize(p.places.len(), -1);
        Ok(p)
    }

    pub fn num_countries(&self) -> usize {
        self.num_countries
    }

    pub fn name(&self, id: i32) -> &str {
        &self.places[id as usize].name
    }

    pub fn city_id(&self, name: &str) -> i32 {
        self.city_names.get(name).copied().unwrap_or(-1)
    }

    pub fn country_id(&self, name: &str) -> i32 {
        self.country_names.get(name).copied().unwrap_or(-1)
    }

    /// `belongsTo`: the parent place, or -1.
    pub fn belongs_to(&self, id: i32) -> i32 {
        self.is_part_of.get(id as usize).copied().unwrap_or(-1)
    }

    pub fn random_city(&self, r: &mut JavaRandom, country: i32) -> i32 {
        let c = &self.cities_by_country[country as usize];
        if c.is_empty() {
            return -1;
        }
        c[r.next_int_bound(c.len() as i32) as usize]
    }

    pub fn random_country_uniform(&self, r: &mut JavaRandom) -> i32 {
        r.next_int_bound(self.num_countries as i32)
    }

    pub fn country_for_person(&self, r: &mut JavaRandom) -> i32 {
        let pos = binary_search_f32(&self.cumulative, r.next_float());
        if pos >= 0 { pos } else { -(pos + 1) }
    }

    pub fn zorder(&self, id: i32) -> i32 {
        self.places[id as usize].zid
    }

    pub fn place_of_zorder(&self, z: i32) -> i32 {
        self.sorted_place[z as usize]
    }
}

fn set_part_of(v: &mut Vec<i32>, id: i32, parent: i32) {
    if v.len() <= id as usize {
        v.resize(id as usize + 1, -1);
    }
    v[id as usize] = parent;
}

// ------------------------------------------------------------------- IPs

/// An IPv4 address and its network mask (`IP`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ip {
    pub ip: i32,
    pub mask: i32,
}

impl Ip {
    pub fn write(self, out: &mut Vec<u8>) {
        let ip = self.ip as u32;
        for (k, shift) in [24, 16, 8, 0].into_iter().enumerate() {
            if k > 0 {
                out.push(b'.');
            }
            push_u64(out, ((ip >> shift) & 0xFF) as u64);
        }
    }
}

const DIGITS2: &[u8; 200] = b"0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// Append the decimal digits of `v` to `out`, without a call to `memcpy`:
/// 20 bytes are reserved with one fixed-size store, written back to front
/// two digits at a time, then cut to length.
#[inline]
pub(crate) fn push_u64(out: &mut Vec<u8>, mut v: u64) {
    let n = v.checked_ilog10().unwrap_or(0) as usize + 1;
    let l = out.len();
    out.extend_from_slice(&[0u8; 20]);
    let s = &mut out[l..l + n];
    let mut i = n;
    while v >= 100 {
        let d = (v % 100) as usize * 2;
        v /= 100;
        i -= 2;
        s[i..i + 2].copy_from_slice(&DIGITS2[d..d + 2]);
    }
    if v >= 10 {
        let d = v as usize * 2;
        s[..2].copy_from_slice(&DIGITS2[d..d + 2]);
    } else {
        s[0] = b'0' + v as u8;
    }
    out.truncate(l + n);
}

/// `IPAddressDictionary`.
#[derive(Debug)]
pub struct Ips {
    by_country: Vec<Vec<Ip>>,
}

impl Ips {
    fn load(dir: &Path, places: &Places) -> io::Result<Self> {
        let mapping = read(dir, "dictionaries/countryAbbrMapping.txt")?;
        let mut abbr: HashMap<String, String> = HashMap::new();
        for line in lines(&mapping) {
            let d = split(line, "   ");
            abbr.insert(trim(d[1]).replace(' ', "_"), d[0].to_string());
        }
        let mut by_country = Vec::with_capacity(places.num_countries());
        for c in 0..places.num_countries() {
            let file = &abbr[places.name(c as i32)];
            let zone = read(dir, &format!("ipaddrByCountries/{file}.zone"))?;
            let mut v = Vec::new();
            for line in lines(&zone).into_iter().take(100) {
                let d = split(line, ".");
                let m = split(d[3], "/");
                let b = [
                    parse_i32(d[0]),
                    parse_i32(d[1]),
                    parse_i32(d[2]),
                    parse_i32(m[0]),
                ];
                let bits = parse_i32(m[1]);
                let ip = ((b[0] & 0xFF) << 24)
                    | ((b[1] & 0xFF) << 16)
                    | ((b[2] & 0xFF) << 8)
                    | (b[3] & 0xFF);
                let mask = (-1i32).wrapping_shl((32 - bits) as u32);
                v.push(Ip { ip, mask });
            }
            by_country.push(v);
        }
        Ok(Ips { by_country })
    }

    pub fn ip(&self, r: &mut JavaRandom, country: i32) -> Ip {
        let v = &self.by_country[country as usize];
        let net = v[r.next_int_bound(v.len() as i32) as usize];
        let network = net.ip & net.mask;
        Ip {
            ip: network | (!net.mask & r.next_int()),
            mask: net.mask,
        }
    }
}

// ------------------------------------------------------------- languages

/// `LanguageDictionary`.
#[derive(Debug)]
pub struct Languages {
    pub names: Vec<String>,
    official: Vec<Vec<i32>>,
    by_country: Vec<Vec<i32>>,
    prob_english: f64,
    prob_second: f64,
    english: i32,
}

impl Languages {
    fn load(dir: &Path, places: &Places, p: &Params) -> io::Result<Self> {
        let txt = read(dir, "dictionaries/languagesByCountry.txt")?;
        let n = places.num_countries();
        let mut l = Languages {
            names: Vec::new(),
            official: vec![Vec::new(); n],
            by_country: vec![Vec::new(); n],
            prob_english: p.prob_english,
            prob_second: p.prob_second_lang,
            english: -1,
        };
        let mut index: HashMap<String, i32> = HashMap::new();
        for line in lines(&txt) {
            let d = split(line, "  ");
            let country = places.country_id(d[0]);
            if country == -1 {
                continue;
            }
            for item in &d[1..] {
                let ld = split(item, " ");
                let id = *index.entry(ld[0].to_string()).or_insert_with(|| {
                    l.names.push(ld[0].to_string());
                    l.names.len() as i32 - 1
                });
                if ld.len() == 3 {
                    l.official[country as usize].push(id);
                }
                l.by_country[country as usize].push(id);
            }
        }
        l.english = index.get("en").copied().unwrap_or(-1);
        Ok(l)
    }

    pub fn name(&self, id: i32) -> &str {
        if id < 0 || id as usize >= self.names.len() {
            return "";
        }
        &self.names[id as usize]
    }

    pub fn languages(&self, r: &mut JavaRandom, country: i32) -> Vec<i32> {
        let mut set = Vec::with_capacity(3);
        let off = &self.official[country as usize];
        let all = &self.by_country[country as usize];
        if !off.is_empty() {
            set.push(off[r.next_int_bound(off.len() as i32) as usize]);
        } else {
            set.push(all[r.next_int_bound(all.len() as i32) as usize]);
        }
        if r.next_double() < self.prob_second {
            let l = all[r.next_int_bound(all.len() as i32) as usize];
            if !set.contains(&l) {
                set.push(l);
            }
        }
        set
    }

    pub fn international(&self, r: &mut JavaRandom) -> i32 {
        if r.next_double() < self.prob_english {
            self.english
        } else {
            -1
        }
    }
}

// ----------------------------------------------------------------- names

/// `NamesDictionary`: names are indices into `pool`.
#[derive(Debug)]
pub struct Names {
    pub pool: Vec<String>,
    /// Email base of each given name in `pool` (diacritics stripped).
    pub email_base: Vec<String>,
    surnames: Vec<Vec<u32>>,
    /// `[period][male?][country]`
    given: [[Vec<Vec<u32>>; 2]; 2],
    vp: f64,
}

const GEOMETRIC_P: f64 = 0.2;
const TOP_N: i32 = 30;

/// `GeometricDist(p).inverseFInt(u)`.
fn geometric_inverse(p: f64, vp: f64, u: f64) -> i32 {
    if p >= 1.0 || u <= p {
        return 0;
    }
    if u >= 1.0 || p <= 0.0 {
        return i32::MAX;
    }
    floor_div(log1p(-u), vp)
}

impl Names {
    fn load(dir: &Path, places: &Places) -> io::Result<Self> {
        let n = places.num_countries();
        let mut pool = Vec::new();
        let mut surnames = vec![Vec::new(); n];
        let txt = read(dir, "dictionaries/surnameByCountryBirthPlace.txt.freq.sort")?;
        for line in lines(&txt) {
            let d = split(line, ",");
            let c = places.country_id(d[1]);
            if c != -1 {
                pool.push(trim(d[2]).to_string());
                surnames[c as usize].push(pool.len() as u32 - 1);
            }
        }
        let first_given = pool.len();
        let mut given: [[Vec<Vec<u32>>; 2]; 2] = Default::default();
        for period in &mut given {
            for g in period.iter_mut() {
                *g = vec![Vec::new(); n];
            }
        }
        let txt = read(
            dir,
            "dictionaries/givennameByCountryBirthPlace.txt.freq.full",
        )?;
        let mut seen: HashMap<String, u32> = HashMap::new();
        for line in lines(&txt) {
            let d = split(line, "  ");
            let gender = parse_i32(d[2]);
            let period = parse_i32(d[3]) as usize;
            let c = places.country_id(d[0]);
            if c != -1 {
                let name = trim(d[1]);
                let id = *seen.entry(name.to_string()).or_insert_with(|| {
                    pool.push(name.to_string());
                    pool.len() as u32 - 1
                });
                let male = usize::from(gender == 0);
                given[period][male][c as usize].push(id);
            }
        }
        let email_base = pool
            .iter()
            .enumerate()
            .map(|(i, s)| {
                if i >= first_given {
                    email_base(s)
                } else {
                    String::new()
                }
            })
            .collect();
        Ok(Names {
            pool,
            email_base,
            surnames,
            given,
            vp: log1p(-GEOMETRIC_P),
        })
    }

    fn geo_index(&self, r: &mut JavaRandom, num: i32) -> i32 {
        let rank = geometric_inverse(GEOMETRIC_P, self.vp, r.next_double());
        if rank < TOP_N {
            if num > rank {
                rank
            } else {
                r.next_int_bound(num)
            }
        } else if num > rank {
            TOP_N + r.next_int_bound(num - TOP_N)
        } else {
            r.next_int_bound(num)
        }
    }

    pub fn surname(&self, r: &mut JavaRandom, country: i32) -> u32 {
        let v = &self.surnames[country as usize];
        v[self.geo_index(r, v.len() as i32) as usize]
    }

    pub fn given_name(&self, r: &mut JavaRandom, country: i32, male: bool, birth_year: i64) -> u32 {
        let period = usize::from(birth_year >= 1985);
        let m = usize::from(male);
        let first = &self.given[0][m][country as usize];
        let id = self.geo_index(r, first.len() as i32);
        if id >= TOP_N {
            first[id as usize]
        } else {
            self.given[period][m][country as usize][id as usize]
        }
    }
}

/// `Normalizer.normalize(NFD)`, combining diacritical marks (U+0300..U+036F)
/// removed, spaces to dots, runs of dots collapsed.
fn email_base(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.nfd() {
        if ('\u{0300}'..='\u{036F}').contains(&c) {
            continue;
        }
        let c = if c == ' ' { '.' } else { c };
        if c == '.' && out.ends_with('.') {
            continue;
        }
        out.push(c);
    }
    out
}

// ------------------------------------------------------- browsers, emails

/// `BrowserDictionary`.
#[derive(Debug)]
pub struct Browsers {
    pub names: Vec<String>,
    cum: Vec<f64>,
    prob_another: f64,
}

impl Browsers {
    fn load(dir: &Path, p: &Params) -> io::Result<Self> {
        let txt = read(dir, "dictionaries/browsersDic.txt")?;
        let mut b = Browsers {
            names: Vec::new(),
            cum: Vec::new(),
            prob_another: p.prob_another_browser,
        };
        let mut c = 0.0;
        for line in lines(&txt) {
            let d = split(line, "  ");
            c += parse_f64(d[1]);
            b.names.push(d[0].to_string());
            b.cum.push(c);
        }
        Ok(b)
    }

    pub fn random(&self, r: &mut JavaRandom) -> i32 {
        let prob = r.next_double();
        let mut min = 0usize;
        let mut max = if prob < self.cum[0] {
            0
        } else {
            self.cum.len() - 1
        };
        while max - min > 1 {
            let mid = min + (max - min) / 2;
            if prob > self.cum[mid] {
                min = mid;
            } else {
                max = mid;
            }
        }
        max as i32
    }

    /// A message's browser: a random one, or `None` for its author's.
    pub fn post_browser(&self, diff: &mut Farm) -> Option<i32> {
        let prob = diff.get(Aspect::DiffBrowser).next_double();
        (prob < self.prob_another).then(|| self.random(diff.get(Aspect::Browser)))
    }
}

/// `EmailDictionary`.
#[derive(Debug)]
pub struct Emails {
    pub domains: Vec<String>,
    canonical: Vec<u16>,
    cum: Vec<f64>,
}

impl Emails {
    fn load(dir: &Path) -> io::Result<Self> {
        let txt = read(dir, "dictionaries/email.txt")?;
        let mut e = Emails {
            domains: Vec::new(),
            canonical: Vec::new(),
            cum: Vec::new(),
        };
        let mut c = 0.0;
        for line in lines(&txt) {
            let d = split(line, " ");
            e.domains.push(d[0].to_string());
            if d.len() == 2 {
                c += parse_f64(d[1]);
                e.cum.push(c);
            }
        }
        // the same domain may be listed twice: name each by its first index,
        // as the datagen compares the addresses as strings
        let mut first: HashMap<String, u16> = HashMap::new();
        e.canonical = e
            .domains
            .iter()
            .enumerate()
            .map(|(i, d)| *first.entry(d.clone()).or_insert(i as u16))
            .collect();
        Ok(e)
    }

    /// A random domain, by the index of its first listing.
    pub fn random(&self, farm: &mut Farm) -> u16 {
        self.canonical[self.random_index(farm) as usize]
    }

    fn random_index(&self, farm: &mut Farm) -> u16 {
        let mut min = 0usize;
        let mut max = self.cum.len() - 1;
        let prob = farm.get(Aspect::TopEmail).next_double();
        if prob > self.cum[max] {
            let n = (self.domains.len() - self.cum.len()) as i32;
            return (farm.get(Aspect::Email).next_int_bound(n) as usize + self.cum.len()) as u16;
        } else if prob < self.cum[min] {
            return min as u16;
        }
        while max - min > 1 {
            let mid = min + (max - min) / 2;
            if prob > self.cum[mid] {
                min = mid;
            } else {
                max = mid;
            }
        }
        max as u16
    }
}

// ------------------------------------------------------------------ tags

/// `TagDictionary`.
#[derive(Debug)]
pub struct Tags {
    pub names: Vec<String>,
    pub tag_class: Vec<i32>,
    class_name: HashMap<i32, String>,
    class_parent: HashMap<i32, i32>,
    by_country: Vec<Vec<i32>>,
    cum: Vec<Vec<f64>>,
    corr_prob: f64,
}

impl Tags {
    fn load(dir: &Path, num_countries: usize, p: &Params) -> io::Result<Self> {
        let mut t = Tags {
            names: Vec::new(),
            tag_class: Vec::new(),
            class_name: HashMap::new(),
            class_parent: HashMap::new(),
            by_country: vec![Vec::new(); num_countries],
            cum: vec![Vec::new(); num_countries],
            corr_prob: p.tag_country_corr_prob,
        };
        for line in lines(&read(dir, "dictionaries/tagClasses.txt")?) {
            let d = split(line, "\t");
            t.class_name.insert(parse_i32(d[0]), d[1].to_string());
        }
        for line in lines(&read(dir, "dictionaries/tagClassHierarchy.txt")?) {
            let d = split(line, "\t");
            t.class_parent.insert(parse_i32(d[0]), parse_i32(d[1]));
        }
        for line in lines(&read(dir, "dictionaries/tags.txt")?) {
            let d = split(line, "\t");
            let id = parse_i32(d[0]) as usize;
            if t.names.len() <= id {
                t.names.resize(id + 1, String::new());
                t.tag_class.resize(id + 1, -1);
            }
            t.tag_class[id] = parse_i32(d[1]);
            t.names[id] = d[2].to_string();
        }
        for line in lines(&read(dir, "dictionaries/popularTagByCountry.txt")?) {
            let d = split(line, " ");
            let c = parse_i32(d[0]) as usize;
            t.cum[c].push(parse_f64(d[2]));
            t.by_country[c].push(parse_i32(d[1]));
        }
        Ok(t)
    }

    pub fn class_name(&self, id: i32) -> &str {
        self.class_name.get(&id).map_or("", String::as_str)
    }

    pub fn class_parent(&self, id: i32) -> i32 {
        self.class_parent.get(&id).copied().unwrap_or(-1)
    }

    /// `getaTagByCountry`.
    pub fn tag_by_country(
        &self,
        other: &mut JavaRandom,
        prob: &mut JavaRandom,
        country: i32,
    ) -> i32 {
        let mut c = country as usize;
        if self.by_country[c].is_empty() || other.next_double() > self.corr_prob {
            loop {
                c = other.next_int_bound(self.by_country.len() as i32) as usize;
                if !self.by_country[c].is_empty() {
                    break;
                }
            }
        }
        let r = prob.next_double();
        let (mut lo, mut hi) = (0usize, self.by_country[c].len());
        let mut cur = (hi + lo) / 2;
        while hi > lo + 1 {
            if self.cum[c][cur] > r {
                hi = cur;
            } else {
                lo = cur;
            }
            cur = (hi + lo) / 2;
        }
        self.by_country[c][cur]
    }

    /// `getRandomTags`.
    fn random_tags(&self, r: &mut JavaRandom, num: usize) -> Vec<i32> {
        let mut out = Vec::with_capacity(num);
        while out.len() < num {
            let c = r.next_int_bound(self.by_country.len() as i32) as usize;
            let tags = &self.by_country[c];
            if !tags.is_empty() {
                out.push(tags[r.next_int_bound(tags.len() as i32) as usize]);
            }
        }
        out
    }
}

/// `TagMatrix`: related tags with cumulative probabilities.
#[derive(Debug)]
pub struct TagMatrix {
    /// `[start, end)` of each tag's related list, empty if none.
    ranges: Vec<(u32, u32)>,
    /// Related tag ids, as `u16` (there are ~16,000 tags): half the cache
    /// footprint of the hot random reads.
    related: Vec<u16>,
    cum: Vec<f64>,
    non_zero: Vec<i32>,
}

impl TagMatrix {
    fn load(dir: &Path) -> io::Result<Self> {
        let txt = read(dir, "dictionaries/tagMatrix.txt")?;
        // ~2 M lines: parsed in pieces on all cores, in file order
        let parts = parallel_lines(&txt, |line| {
            let mut it = line.split(' ');
            let a = parse_i32(it.next().expect("celebrity"));
            let b = parse_i32(it.next().expect("topic"));
            let c = parse_f64(it.next().expect("cumulative"));
            (a, b, c)
        });
        // celebrity ids appear in runs; a TreeMap groups them by id, each
        // id's rows in file order: a counting sort
        let rows = || parts.iter().flatten();
        let max = rows().map(|r| r.0).max().unwrap_or(0) as usize;
        let mut ranges = vec![(0u32, 0u32); max + 1];
        for r in rows() {
            ranges[r.0 as usize].1 += 1;
        }
        let mut at = 0u32;
        let mut non_zero = Vec::new();
        for (tag, r) in ranges.iter_mut().enumerate() {
            let n = r.1;
            *r = (at, at);
            if n > 0 {
                non_zero.push(tag as i32);
            }
            at += n;
        }
        let total = at as usize;
        let (mut related, mut cum) = (vec![0u16; total], vec![0f64; total]);
        for &(a, b, c) in rows() {
            let r = &mut ranges[a as usize];
            related[r.1 as usize] = u16::try_from(b).expect("tag ids fit in u16");
            cum[r.1 as usize] = c;
            r.1 += 1;
        }
        Ok(TagMatrix {
            ranges,
            related,
            cum,
            non_zero,
        })
    }

    fn range(&self, tag: i32) -> Option<(usize, usize)> {
        match self.ranges.get(tag as usize) {
            Some(&(s, e)) if e > s => Some((s as usize, e as usize)),
            _ => None,
        }
    }

    /// `getRandomRelated`.
    pub fn random_related(&self, r: &mut JavaRandom, tag: i32) -> i32 {
        self.related_at(self.random_related_index(r, tag))
    }

    /// The draws of [`Self::random_related`]: the related tag's position,
    /// for [`Self::related_at`].
    #[inline]
    pub fn random_related_index(&self, r: &mut JavaRandom, tag: i32) -> u32 {
        let (s, e) = match self.range(tag) {
            Some(x) => x,
            None => {
                let t = self.non_zero[r.next_int_bound(self.non_zero.len() as i32) as usize];
                self.range(t).expect("non-zero tag")
            }
        };
        (s + r.next_int_bound((e - s) as i32) as usize) as u32
    }

    #[inline]
    pub fn related_at(&self, k: u32) -> i32 {
        self.related[k as usize] as i32
    }

    /// `getSetofTags`: a sorted set of `num` tags around `popular`.
    pub fn set_of_tags(&self, r: &mut JavaRandom, popular: i32, num: usize) -> Vec<i32> {
        let mut set = vec![popular];
        while set.len() < num {
            let (s, e) = match self.range(popular) {
                Some(x) => x,
                None => {
                    let t = self.non_zero[r.next_int_bound(self.non_zero.len() as i32) as usize];
                    self.range(t).expect("non-zero tag")
                }
            };
            let rd = r.next_double();
            let (mut lo, mut hi) = (0usize, e - s);
            let mut mid = (hi + lo) / 2;
            while hi > lo + 1 {
                if self.cum[s + mid] > rd {
                    hi = mid;
                } else {
                    lo = mid;
                }
                mid = (hi + lo) / 2;
            }
            let t = self.related[s + mid] as i32;
            if let Err(pos) = set.binary_search(&t) {
                set.insert(pos, t);
            }
        }
        set
    }
}

/// `TagTextDictionary`: each tag's text as UTF-16.
#[derive(Debug)]
pub struct TagText {
    pub text: Vec<Vec<u16>>,
    /// "About <name>, " of each tag, UTF-16.
    pub prefix: Vec<Vec<u16>>,
    /// The lengths of each tag's text and prefix, packed (most draws need
    /// only these), and where the tag's bits start in `dots`.
    lens: Vec<(u32, u32, u32)>,
    /// One bit per character of every tag's text: is it a `.`? (A text's
    /// length needs only its last character's.)
    dots: Vec<u64>,
    reduced_ratio: f64,
}

impl TagText {
    fn load(dir: &Path, tags: &Tags, p: &Params) -> io::Result<Self> {
        let txt = read(dir, "dictionaries/tagText.txt")?;
        let mut text: Vec<Vec<u16>> = vec![Vec::new(); tags.names.len()];
        let parts = parallel_lines(&txt, |line| {
            let d = split(line, "  ");
            (
                parse_i32(d[0]) as usize,
                d[1].encode_utf16().collect::<Vec<u16>>(),
            )
        });
        for (id, t) in parts.into_iter().flatten() {
            if text.len() <= id {
                text.resize(id + 1, Vec::new());
            }
            text[id] = t;
        }
        let prefix: Vec<Vec<u16>> = tags
            .names
            .iter()
            .map(|n| {
                let n = n.replace('_', " ").replace('"', "\\\"");
                format!("About {n}, ").encode_utf16().collect()
            })
            .collect();
        let mut dots = Vec::new();
        let mut bits = 0usize;
        let lens = (0..text.len().max(prefix.len()))
            .map(|t| {
                let n = |v: &Vec<Vec<u16>>| v.get(t).map_or(0, |x| x.len() as u32);
                let at = u32::try_from(bits).expect("tag texts under 4 G characters");
                for &c in text.get(t).into_iter().flatten() {
                    if bits.is_multiple_of(64) {
                        dots.push(0u64);
                    }
                    if c == b'.' as u16 {
                        dots[bits / 64] |= 1 << (bits % 64);
                    }
                    bits += 1;
                }
                (n(&text), n(&prefix), at)
            })
            .collect();
        // every prefix ends in a space, so a piece ending in one is no dot
        debug_assert!(prefix.iter().all(|p| p.last() == Some(&(b' ' as u16))));
        Ok(TagText {
            text,
            prefix,
            lens,
            dots,
            reduced_ratio: p.ratio_reduce_text,
        })
    }

    pub fn random_text_size(&self, r: &mut JavaRandom, min: i32, max: i32) -> i32 {
        if r.next_double() > self.reduced_ratio {
            return r.next_int_bound(max - min) + min;
        }
        r.next_int_bound((max >> 1) - min) + min
    }

    pub fn random_large_text_size(&self, r: &mut JavaRandom, min: i32, max: i32) -> i32 {
        r.next_int_bound(max - min) + min
    }

    /// `generateText`, into `out` (cleared), as UTF-16 with `|` replaced.
    /// The length of the text [`Self::generate`] makes, with the same
    /// draws, without building it.
    pub fn generated_len(&self, r: &mut JavaRandom, tags: &[i32], size: i32) -> usize {
        let size = size as usize;
        let per_tag = (size as f64 / tags.len() as f64).ceil() as usize;
        // only the text's last character matters (is it a dot?): keep its
        // bit in `dots` (`None` for a prefix's last, a space)
        let (mut len, mut last): (usize, Option<Option<usize>>) = (0, None);
        while len < size {
            for &tag in tags {
                if len >= size {
                    break;
                }
                let (content, prefix, at) = self.lens[tag as usize];
                let (content, prefix, at) = (content as usize, prefix as usize, at as usize);
                let this = per_tag.min(size - len) + prefix;
                if this >= content {
                    len += content;
                    if content > 0 {
                        last = Some(Some(at + content - 1));
                    }
                } else {
                    let start = r.next_int_bound((content - this + prefix) as i32) as usize;
                    len += this;
                    // the piece's last character, or the prefix's if empty
                    last = Some((this > prefix).then(|| at + start + this - prefix - 1));
                }
            }
        }
        let dot = matches!(last, Some(Some(b)) if self.dots[b / 64] >> (b % 64) & 1 == 1);
        if !dot {
            len += 1;
        }
        if len + 1 < size {
            len += 1;
        }
        if len > size {
            len = size - 1;
        }
        len
    }

    pub fn generate(&self, r: &mut JavaRandom, tags: &[i32], size: i32, out: &mut Vec<u16>) {
        out.clear();
        let size = size as usize;
        let per_tag = (size as f64 / tags.len() as f64).ceil() as usize;
        while out.len() < size {
            for &tag in tags {
                if out.len() >= size {
                    break;
                }
                let content = &self.text[tag as usize];
                let prefix = &self.prefix[tag as usize];
                let this = per_tag.min(size - out.len()) + prefix.len();
                if this >= content.len() {
                    out.extend_from_slice(content);
                } else {
                    let start =
                        r.next_int_bound((content.len() - this + prefix.len()) as i32) as usize;
                    out.extend_from_slice(prefix);
                    out.extend_from_slice(&content[start..start + this - prefix.len()]);
                }
            }
        }
        if out.last() != Some(&(b'.' as u16)) {
            out.push(b'.' as u16);
        }
        if out.len() + 1 < size {
            out.push(b' ' as u16);
        }
        if out.len() > size {
            out.truncate(size - 1);
        }
        for c in out.iter_mut() {
            if *c == b'|' as u16 {
                *c = b' ' as u16;
            }
        }
    }
}

// ---------------------------------------------- universities, companies

/// `CompanyDictionary`.
#[derive(Debug)]
pub struct Companies {
    pub name: Vec<String>,
    pub country: Vec<i32>,
    by_country: Vec<Vec<i64>>,
    prob_uncorrelated: f64,
}

impl Companies {
    fn load(dir: &Path, places: &Places, p: &Params) -> io::Result<Self> {
        let mut c = Companies {
            name: Vec::new(),
            country: Vec::new(),
            by_country: vec![Vec::new(); places.num_countries()],
            prob_uncorrelated: p.prob_uncorrelated_company,
        };
        for line in lines(&read(dir, "dictionaries/companiesByCountry.txt")?) {
            let d = split(line, "  ");
            let loc = places.country_id(d[0]);
            if loc != -1 {
                let id = c.name.len() as i64;
                c.by_country[loc as usize].push(id);
                c.country.push(loc);
                c.name.push(trim(d[1]).to_string());
            }
        }
        Ok(c)
    }

    pub fn random(&self, farm: &mut Farm, country: i32) -> i64 {
        let n = self.by_country.len() as i32;
        let mut loc = country as usize;
        if farm.get(Aspect::UncorrelatedCompany).next_double() <= self.prob_uncorrelated {
            loc = farm
                .get(Aspect::UncorrelatedCompanyLocation)
                .next_int_bound(n) as usize;
        }
        while self.by_country[loc].is_empty() {
            loc = farm
                .get(Aspect::UncorrelatedCompanyLocation)
                .next_int_bound(n) as usize;
        }
        let v = &self.by_country[loc];
        v[farm.get(Aspect::Company).next_int_bound(v.len() as i32) as usize]
    }
}

/// `UniversityDictionary`. University ids start after the company ids.
#[derive(Debug)]
pub struct Universities {
    pub start: i64,
    pub name: Vec<String>,
    pub city: Vec<i32>,
    by_country: Vec<Vec<i64>>,
    prob_uncorrelated: f64,
    prob_top: f64,
}

impl Universities {
    fn load(dir: &Path, places: &Places, p: &Params, start: i64) -> io::Result<Self> {
        let mut u = Universities {
            start,
            name: Vec::new(),
            city: Vec::new(),
            by_country: vec![Vec::new(); places.num_countries()],
            prob_uncorrelated: p.prob_uncorrelated_organisation,
            prob_top: p.prob_top_univ,
        };
        for line in lines(&read(dir, "dictionaries/universities.txt")?) {
            let d = split(line, "  ");
            let country = places.country_id(d[0]);
            let city = places.city_id(d[2]);
            if country != -1 && city != -1 {
                let id = start + u.name.len() as i64;
                u.by_country[country as usize].push(id);
                u.city.push(city);
                u.name.push(trim(d[1]).to_string());
            }
        }
        Ok(u)
    }

    /// `getRandomUniversity`: a "university location" (z-order and index).
    pub fn random(&self, farm: &mut Farm, places: &Places, country: i32) -> i32 {
        let n = places.num_countries() as i32;
        let mut c = country as usize;
        let prob = farm.get(Aspect::UncorrelatedUniversity).next_double();
        if farm.get(Aspect::UncorrelatedUniversity).next_double() <= self.prob_uncorrelated {
            c = farm
                .get(Aspect::UncorrelatedUniversityLocation)
                .next_int_bound(n) as usize;
        }
        while self.by_country[c].is_empty() {
            c = farm
                .get(Aspect::UncorrelatedUniversityLocation)
                .next_int_bound(n) as usize;
        }
        let mut range = self.by_country[c].len() as i32;
        if prob > self.prob_uncorrelated
            && farm.get(Aspect::TopUniversity).next_double() < self.prob_top
        {
            range = range.min(10);
        }
        let idx = farm.get(Aspect::University).next_int_bound(range);
        (places.zorder(c as i32) << 24) | (idx << 12)
    }

    /// `getUniversityFromLocation`.
    pub fn at_location(&self, places: &Places, loc: i32) -> i64 {
        let z = loc >> 24;
        let idx = (loc >> 12) & 0x0FFF;
        self.by_country[places.place_of_zorder(z) as usize][idx as usize]
    }
}

// ------------------------------------------------------------- flashmobs

#[derive(Debug, Clone, Copy)]
pub struct FlashMobTag {
    pub level: i32,
    pub date: i64,
    pub prob: f64,
    pub tag: i32,
}

/// `FlashmobTagDictionary` (only its date-sorted distribution is used: the
/// datagen always passes an empty interest set).
#[derive(Debug)]
pub struct Flashmobs {
    pub cum_dist: Vec<FlashMobTag>,
    prob_random_per_level: f64,
}

impl Flashmobs {
    fn new(tags: &Tags, dates: &Dates, p: &Params) -> Self {
        let mut r = JavaRandom::new(0);
        let level = PowerDistribution::new(
            p.flashmob_tag_min_level,
            p.flashmob_tag_max_level,
            p.flashmob_tag_dist_exp,
        );
        let n = (p.flashmob_tags_per_month as f64 * dates.number_of_months(dates.start) as f64)
            as usize;
        let t = tags.random_tags(&mut r, n);
        let mut v = Vec::with_capacity(n);
        let mut sum = 0.0f64;
        for &tag in &t {
            let date = dates.random_date_from(&mut r, dates.start);
            let lv = level.value(&mut r);
            sum += lv as f64;
            v.push(FlashMobTag {
                level: lv,
                date,
                prob: 0.0,
                tag,
            });
        }
        v.sort_by_key(|f| f.date);
        let mut cur = 0.0;
        for f in &mut v {
            f.prob = cur;
            cur += f.level as f64 / sum;
        }
        Flashmobs {
            cum_dist: v,
            prob_random_per_level: p.prob_random_per_level,
        }
    }

    fn earliest(&self, from: i64) -> usize {
        let (mut lo, mut hi) = (0usize, self.cum_dist.len());
        let mut mid = (hi + lo) / 2;
        while hi > lo + 1 {
            if self.cum_dist[mid].date > from {
                hi = mid;
            } else {
                lo = mid;
            }
            mid = (hi + lo) / 2;
        }
        mid
    }

    /// `generateFlashmobTags(rand, {}, from)`.
    pub fn generate(&self, r: &mut JavaRandom, from: i64, out: &mut Vec<FlashMobTag>) {
        out.clear();
        for f in &self.cum_dist[self.earliest(from)..] {
            if r.next_double() > 1.0 - self.prob_random_per_level * f.level as f64 {
                out.push(*f);
            }
        }
    }
}

// ---------------------------------------------------------- distributions

/// A file of probabilities (`Distribution`, `PersonDeleteDistribution`,
/// `PowerLawActivityDeleteDistribution`).
fn read_doubles(dir: &Path, rel: &str) -> io::Result<Vec<f64>> {
    Ok(lines(&read(dir, rel)?).into_iter().map(parse_f64).collect())
}

/// `Distribution.nextDouble`: a position in a cumulative table, scaled to
/// `[0, 1)`.
#[derive(Debug)]
pub struct CumDistribution(Vec<f64>);

impl CumDistribution {
    pub fn next_double(&self, r: &mut JavaRandom) -> f64 {
        let p = r.next_double();
        let (mut lo, mut hi) = (0usize, self.0.len() - 1);
        let mut mid = (hi + lo) / 2;
        while hi > lo + 1 {
            if self.0[mid] > p {
                hi = mid;
            } else {
                lo = mid;
            }
            mid = (hi + lo) / 2;
        }
        mid as f64 / self.0.len() as f64
    }
}

/// Everything loaded from the resources directory.
#[derive(Debug)]
pub struct Dictionaries {
    pub places: Places,
    pub ips: Ips,
    pub languages: Languages,
    pub names: Names,
    /// Number of popular places of each country.
    pub popular_places: Vec<i32>,
    pub browsers: Browsers,
    pub emails: Emails,
    pub tags: Tags,
    pub tag_matrix: TagMatrix,
    pub tag_text: TagText,
    pub companies: Companies,
    pub universities: Universities,
    pub flashmobs: Flashmobs,
    pub person_delete: Vec<f64>,
    pub flashmob_dist: CumDistribution,
    pub fb_buckets: Vec<(f64, f64)>,
}

impl Dictionaries {
    /// Load every dictionary from `dir` (the datagen's `src/main/resources`).
    pub fn load(dir: &Path, p: &Params, dates: &Dates) -> io::Result<Self> {
        let (places, tag_matrix) = scope(|s| {
            let m = s.spawn(|| TagMatrix::load(dir));
            (Places::load(dir), m.join().expect("tag matrix"))
        });
        let places = places?;
        let tag_matrix = tag_matrix?;
        let tags = Tags::load(dir, places.num_countries(), p)?;
        // the rest depend only on places and tags: load them side by side
        let (tag_text, companies, ips, names, popular_places) = scope(|s| {
            let tag_text = s.spawn(|| TagText::load(dir, &tags, p));
            let companies = s.spawn(|| Companies::load(dir, &places, p));
            let ips = s.spawn(|| Ips::load(dir, &places));
            let names = s.spawn(|| Names::load(dir, &places));
            let popular = || -> io::Result<Vec<i32>> {
                let mut popular_places = vec![0; places.num_countries()];
                for line in lines(&read(dir, "dictionaries/popularPlacesByCountry.txt")?) {
                    let d = split(line, "  ");
                    let c = places.country_id(d[0]);
                    if c != -1 {
                        popular_places[c as usize] += 1;
                    }
                }
                Ok(popular_places)
            };
            let popular_places = popular();
            (
                tag_text.join().expect("tag text"),
                companies.join().expect("companies"),
                ips.join().expect("ips"),
                names.join().expect("names"),
                popular_places,
            )
        });
        let (tag_text, companies, popular_places) = (tag_text?, companies?, popular_places?);
        let universities = Universities::load(dir, &places, p, companies.name.len() as i64)?;
        let flashmobs = Flashmobs::new(&tags, dates, p);
        let fb_buckets = lines(&read(dir, "dictionaries/facebookBucket100.dat")?)
            .into_iter()
            .map(|l| {
                let d = split(l, " ");
                (parse_f32(d[0]) as f64, parse_f32(d[1]) as f64)
            })
            .collect();
        Ok(Dictionaries {
            ips: ips?,
            languages: Languages::load(dir, &places, p)?,
            names: names?,
            popular_places,
            browsers: Browsers::load(dir, p)?,
            emails: Emails::load(dir)?,
            tag_matrix,
            tag_text,
            companies,
            universities,
            flashmobs,
            person_delete: read_doubles(dir, "dictionaries/personDelete.txt")?,
            flashmob_dist: CumDistribution(read_doubles(dir, "dictionaries/flashmobDist.txt")?),
            fb_buckets,
            tags,
            places,
        })
    }

    /// `PopularPlacesDictionary.getPopularPlace(random, country)`.
    pub fn popular_place(&self, r: &mut JavaRandom, country: i32) -> i32 {
        let n = self.popular_places[country as usize];
        if n == 0 {
            return -1;
        }
        r.next_int_bound(n)
    }
}
