// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! `PersonGenerator`: persons, block by block.

use crate::dates::{self, Dates};
use crate::dict::{Dictionaries, Ip, PowerDistribution};
use crate::java::hashmap_long_order;
use crate::jrand::{Aspect, Farm, JavaRandom};
use crate::params::{ALPHA, Params};

/// A person, as the generator keeps it (`Person` minus its knows).
#[derive(Debug, Clone, Default)]
pub struct Person {
    pub account_id: i64,
    pub creation: i64,
    pub deletion: i64,
    pub max_knows: i64,
    pub explicitly_deleted: bool,
    pub message_deleter: bool,
    pub large_poster: bool,
    /// 1 = male, 0 = female.
    pub gender: u8,
    pub browser: i32,
    pub ip: Ip,
    pub country: i32,
    pub city: i32,
    /// Sorted ascending.
    pub interests: Vec<i32>,
    pub main_interest: i32,
    pub university_location: i32,
    pub birthday: i64,
    pub random_id: i64,
    /// Email domains (indices into the email dictionary).
    pub emails: Vec<u16>,
    pub languages: Vec<i32>,
    pub first_name: u32,
    pub last_name: u32,
    /// `(company, workFrom)` in `HashMap` iteration order.
    pub companies: Vec<(i64, i64)>,
    pub class_year: i64,
}

/// The fields of a [`Person`] the activity's draws read, packed (a
/// `Person` is ~200 bytes; activity reads these for random members).
#[derive(Debug, Clone, Copy)]
pub struct Hot {
    pub account_id: i64,
    pub creation: i64,
    pub deletion: i64,
    pub message_deleter: bool,
    pub large_poster: bool,
}

impl Hot {
    pub fn of(p: &Person) -> Self {
        Hot {
            account_id: p.account_id,
            creation: p.creation,
            deletion: p.deletion,
            message_deleter: p.message_deleter,
            large_poster: p.large_poster,
        }
    }
}

/// `FacebookDegreeDistribution`, a `BucketedDistribution`.
#[derive(Debug, Clone)]
pub struct DegreeDistribution {
    buckets: Vec<(f64, f64)>,
    degree: Vec<JavaRandom>,
    percentile: JavaRandom,
}

const FB_MEAN: f64 = 190.0;

impl DegreeDistribution {
    pub fn facebook(fb: &[(f64, f64)], num_persons: u64) -> Self {
        let n = num_persons as f64;
        let mean = n.powf(0.512 - 0.028 * n.log10()).round_ties_even_java() as i32 as f64;
        let buckets = fb
            .iter()
            .map(|&(min, max)| {
                let nmin = min * mean / FB_MEAN;
                let mut nmax = max * mean / FB_MEAN;
                if nmax < nmin {
                    nmax = nmin;
                }
                (nmin, nmax)
            })
            .collect::<Vec<_>>();
        DegreeDistribution {
            degree: vec![JavaRandom::new(0); buckets.len()],
            buckets,
            percentile: JavaRandom::new(0),
        }
    }

    pub fn reset(&mut self, seed: i64) {
        let mut s = JavaRandom::new(53_223_436i64.wrapping_add(1_234_567i64.wrapping_mul(seed)));
        for r in &mut self.degree {
            r.set_seed(s.next_long());
        }
        self.percentile.set_seed(s.next_long());
    }

    pub fn next_degree(&mut self) -> i64 {
        let idx = self.percentile.next_int_bound(self.buckets.len() as i32) as usize;
        let (min, mut max) = self.buckets[idx];
        if max < min {
            max = min;
        }
        (self.degree[idx].next_int_bound(max as i32 - min as i32 + 1) + min as i32) as i64
    }
}

trait JavaRound {
    fn round_ties_even_java(self) -> f64;
}

impl JavaRound for f64 {
    /// `Math.round(double)`: nearest, ties towards +infinity.
    fn round_ties_even_java(self) -> f64 {
        let f = self.floor();
        if self - f >= 0.5 { f + 1.0 } else { f }
    }
}

/// Generates the persons of one block.
pub struct PersonGenerator<'a> {
    p: &'a Params,
    d: &'a Dictionaries,
    dates: &'a Dates,
    degree: DegreeDistribution,
    tag_power: PowerDistribution,
    farm: Farm,
    next_id: i64,
}

impl<'a> PersonGenerator<'a> {
    pub fn new(p: &'a Params, d: &'a Dictionaries, dates: &'a Dates) -> Self {
        PersonGenerator {
            p,
            d,
            dates,
            degree: DegreeDistribution::facebook(&d.fb_buckets, p.num_persons),
            tag_power: PowerDistribution::new(
                p.min_num_tags_per_person as f64,
                (p.max_num_tags_per_person + 1) as f64,
                ALPHA,
            ),
            farm: Farm::default(),
            next_id: 0,
        }
    }

    /// The persons of block `block` (at most `block_size`, fewer in the last
    /// block).
    pub fn block(&mut self, block: u64, out: &mut Vec<Person>) {
        let bs = self.p.block_size as u64;
        self.degree.reset(block as i64);
        self.farm.reset(block as i64);
        self.next_id = (block * bs) as i64;
        let n = (self.p.num_persons - bs * block).min(bs);
        out.clear();
        out.reserve(n as usize);
        for _ in 0..n {
            let p = self.person();
            out.push(p);
        }
    }

    fn compose_id(&self, id: i64, date: i64) -> i64 {
        let id_mask = !((-1i64) << 41);
        let bucket = ((256 * (date - self.dates.start)) as f64 / self.dates.end as f64) as i64;
        (bucket << 41) | (id & id_mask)
    }

    fn person(&mut self) -> Person {
        let (p, d, dates) = (self.p, self.d, self.dates);
        let f = &mut self.farm;
        let creation = dates.person_creation(f.get(Aspect::Date));
        let country = d.places.country_for_person(f.get(Aspect::Country));
        let mut q = Person {
            creation,
            ..Default::default()
        };
        q.gender = u8::from(f.get(Aspect::Gender).next_double() > 0.5);
        q.birthday = dates.birthday(f.get(Aspect::BirthDay));
        q.browser = d.browsers.random(f.get(Aspect::Browser));
        q.country = country;
        q.city = d.places.random_city(f.get(Aspect::City), country);
        q.ip = d.ips.ip(f.get(Aspect::Ip), country);
        q.message_deleter = f.get(Aspect::Random).next_double() > 0.5;
        let max_knows = self.degree.next_degree().min(p.max_num_friends);
        q.max_knows = max_knows;
        let deleted =
            f.get(Aspect::DeletionPerson).next_double() < d.person_delete[max_knows as usize];
        if deleted {
            q.explicitly_deleted = true;
            q.deletion = dates.person_deletion(f.get(Aspect::Date), creation, dates.end);
        } else {
            q.deletion = dates.network_collapse();
        }
        q.account_id = self.compose_id(self.next_id, creation);
        self.next_id += 1;
        let f = &mut self.farm;
        {
            let [other, tag] = f.get_two(Aspect::TagOtherCountry, Aspect::Tag);
            q.main_interest = d.tags.tag_by_country(other, tag, q.country);
        }
        let num_tags = self.tag_power.value(f.get(Aspect::NumTag)) as i16;
        q.interests = d.tag_matrix.set_of_tags(
            f.get(Aspect::TagOtherCountry),
            q.main_interest,
            num_tags.max(0) as usize,
        );
        q.university_location = d.universities.random(f, &d.places, q.country);
        q.random_id = (f.get(Aspect::Random).next_int_bound(i32::MAX) % 100) as i64;
        q.first_name = d.names.given_name(
            f.get(Aspect::Name),
            q.country,
            q.gender == 1,
            dates::year(q.birthday),
        );
        q.last_name = d.names.surname(f.get(Aspect::Surname), q.country);
        let num_emails = f.get(Aspect::ExtraInfo).next_int_bound(p.max_emails) as usize + 1;
        while q.emails.len() < num_emails {
            let e = d.emails.random(f);
            // avoid duplicates (the address differs only in its domain)
            if !q.emails.contains(&e) {
                q.emails.push(e);
            }
        }
        let prob = f.get(Aspect::ExtraInfo).next_double();
        if prob < p.missing_ratio || q.university_location == -1 {
            q.class_year = -1;
        } else {
            q.class_year = dates.class_year(f.get(Aspect::Date), q.birthday);
        }
        let num_companies = f.get(Aspect::ExtraInfo).next_int_bound(p.max_companies) + 1;
        let prob = f.get(Aspect::ExtraInfo).next_double();
        if prob >= p.missing_ratio {
            for _ in 0..num_companies {
                let work = dates.work_from(f.get(Aspect::Date), q.class_year, q.birthday);
                let c = d.companies.random(f, q.country);
                match q.companies.iter_mut().find(|e| e.0 == c) {
                    Some(e) => e.1 = work,
                    None => q.companies.push((c, work)),
                }
            }
            hashmap_long_order(&mut q.companies);
        }
        let mut langs = d.languages.languages(f.get(Aspect::Language), q.country);
        let intl = d.languages.international(f.get(Aspect::Language));
        if intl != -1 && !langs.contains(&intl) {
            langs.push(intl);
        }
        q.languages = langs;
        q.large_poster = dates::month(q.birthday) == 1;
        q
    }
}
