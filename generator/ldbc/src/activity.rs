//! `PersonActivityGenerator` and the `PersonOutputStream` /
//! `ActivityOutputStream` serializers: one block of persons (sorted by
//! account id) to rows of the 14 dynamic tables.
//!
//! The Java generator builds lazy streams that the serializer consumes
//! depth-first, and every random draw comes from one farm shared by the
//! whole block, so the order of draws is the order of the serializer's
//! traversal. This port is that traversal written out eagerly: the parts
//! the Java evaluates when a stream is *created* (arguments of
//! `createPosts`, the eager prologue of `generateLikes`, ...) are drawn at
//! the same points.

use crate::dates::{self, Dates, SEVEN_DAYS};
use crate::dict::{Dictionaries, FlashMobTag, Ip, PowerDistribution, push_u64};
use crate::java::{clamp, push_utf16};
use crate::jrand::{Aspect, Farm};
use crate::knows::Knows;
use crate::params::{POST_MAPPING, Params};
use crate::person::{Hot, Person};
use crate::rows::{Rows, t};
use std::mem::{replace, take};
use std::ops::Range;

const SHORT_COMMENTS: [&str; 16] = [
    "ok", "good", "great", "cool", "thx", "fine", "LOL", "roflol", "no way!", "I see", "right",
    "yes", "no", "duh", "thanks", "maybe",
];

const FLASHMOB_SPAN: i64 = 72 * 60 * 60 * 1000;

/// Shared, read-only state of a run.
pub struct World<'a> {
    pub p: &'a Params,
    pub d: &'a Dictionaries,
    pub dates: &'a Dates,
    pub persons: &'a [Person],
    /// [`Hot`] of each person.
    pub hot: &'a [Hot],
    pub knows: &'a [Vec<Knows>],
    /// `SN` id bits for the block number.
    pub num_bits: u32,
}

impl World<'_> {
    /// `SN.composeId`.
    #[inline]
    fn compose_id(&self, id: i64, creation: i64) -> i64 {
        let bucket = ((256 * (creation - self.dates.start)) as f64 / self.dates.end as f64) as i64;
        let id_mask = !((-1i64) << 36);
        (bucket << 36) | (id & id_mask)
    }

    /// `SN.formId`.
    #[inline]
    fn form_id(&self, id: i64, block: i64) -> i64 {
        let low = id & 0x0F_FFFF;
        let machine = block << 20;
        let upper = (id >> 20) << (20 + self.num_bits);
        upper | machine | low
    }

    fn id(&self, counter: i64, creation: i64, block: i64) -> i64 {
        self.form_id(self.compose_id(counter, creation), block)
    }
}

#[derive(Debug, Clone, Copy)]
struct Membership {
    person: u32,
    creation: i64,
    deletion: i64,
    deleted: bool,
}

#[derive(Debug)]
struct Forum {
    id: i64,
    creation: i64,
    deletion: i64,
    deleted: bool,
    moderator: u32,
    title: String,
    tags: Vec<i32>,
    language: i32,
    place: i32,
    memberships: Vec<Membership>,
}

/// A possible parent of a comment.
#[derive(Debug, Clone)]
struct Parent {
    id: i64,
    creation: i64,
    deletion: i64,
    tags: Vec<i32>,
}

struct PostCore {
    tags: Vec<i32>,
    creation: i64,
    deletion: i64,
    deleted: bool,
}

/// Where a block's generation stands between two persons: all that one
/// person's rows depend on besides the pre-pass.
#[derive(Debug, Clone)]
pub struct Resume {
    farm: Farm,
    forum_counter: i64,
    message_counter: i64,
}

impl Resume {
    /// The state before block `block`'s first person.
    pub fn start(block: u64) -> Self {
        Resume {
            farm: Farm::new(block as i64),
            forum_counter: 0,
            message_counter: 0,
        }
    }
}

/// Generator state for one block.
pub struct BlockGen<'w, 'a> {
    w: &'w World<'a>,
    farm: Farm,
    block: i64,
    forum_counter: i64,
    message_counter: i64,
    flash_forum: i64,
    flash_tags: Vec<FlashMobTag>,
    flash_scratch: Vec<FlashMobTag>,
    text: Vec<u16>,
    /// Creation and deletion dates of forum `m_forum`'s memberships.
    m_forum: i64,
    m_creation: Vec<i64>,
    m_deletion: Vec<i64>,
    m_masks: Vec<u8>,
    /// Length of the last generated text (also kept when `dry`).
    text_len: usize,
    /// Only advance the state: no rows, no text.
    dry: bool,
    /// Scratch: positions of related tags in the tag matrix.
    related: Vec<u32>,
    /// Scratch for comments: parents, their members' masks, spare tag lists.
    parents: Vec<Parent>,
    cached: Vec<Option<(u32, usize)>>,
    tag_pool: Vec<Vec<i32>>,
    like_power: PowerDistribution,
    pub out: Vec<Rows>,
}

#[inline]
fn add_sorted(set: &mut Vec<i32>, x: i32) {
    if let Err(p) = set.binary_search(&x) {
        set.insert(p, x);
    }
}

impl<'w, 'a> BlockGen<'w, 'a> {
    pub fn new(w: &'w World<'a>, out: Vec<Rows>) -> Self {
        BlockGen {
            w,
            farm: Farm::default(),
            block: 0,
            forum_counter: 0,
            message_counter: 0,
            flash_forum: -1,
            flash_tags: Vec::new(),
            flash_scratch: Vec::new(),
            text: Vec::with_capacity(4096),
            text_len: 0,
            m_forum: -1,
            m_creation: Vec::new(),
            m_deletion: Vec::new(),
            m_masks: Vec::new(),
            dry: false,
            related: Vec::new(),
            parents: Vec::new(),
            cached: Vec::new(),
            tag_pool: Vec::new(),
            like_power: PowerDistribution::new(1.0, w.p.max_num_like as f64, 0.07),
            out,
        }
    }

    /// Generate persons `members[range]` of block `block` (`members`: the
    /// block's global indices, sorted by account id) into `self.out`,
    /// starting from `at`: [`Resume::start`] for the block's first person,
    /// else the [`Self::state`] after the person before `range`. Returns
    /// the state after the range.
    pub fn run(&mut self, block: u64, members: &[u32], range: Range<usize>, at: &Resume) -> Resume {
        self.resume(block, at);
        self.dry = false;
        for &m in &members[range] {
            self.person_rows(m);
            self.activity(m, members);
        }
        self.state()
    }

    /// Like [`Self::run`], but only to find the state after `range`: the
    /// same draws, without building rows or text.
    pub fn skip(
        &mut self,
        block: u64,
        members: &[u32],
        range: Range<usize>,
        at: &Resume,
    ) -> Resume {
        self.resume(block, at);
        self.dry = true;
        for &m in &members[range] {
            self.activity(m, members);
        }
        self.state()
    }

    fn resume(&mut self, block: u64, at: &Resume) {
        self.block = block as i64;
        self.farm = at.farm.clone();
        self.forum_counter = at.forum_counter;
        self.message_counter = at.message_counter;
        // forum ids only grow within a block, so no forum's flashmob tags
        // carry over from a person to the next
        self.flash_forum = -1;
        self.m_forum = -1;
    }

    fn state(&self) -> Resume {
        Resume {
            farm: self.farm.clone(),
            forum_counter: self.forum_counter,
            message_counter: self.message_counter,
        }
    }

    fn person(&self, i: u32) -> &'a Person {
        &self.w.persons[i as usize]
    }

    // ------------------------------------------------------ person rows

    fn person_rows(&mut self, i: u32) {
        let w = self.w;
        let (d, p) = (w.d, self.person(i));
        {
            let r = &mut self.out[t::PERSON];
            r.cols[0].i64(p.creation);
            r.cols[1].i64(p.deletion);
            r.cols[2].bool(p.explicitly_deleted);
            r.cols[3].i64(p.account_id);
            r.cols[4].str(d.names.pool[p.first_name as usize].as_bytes());
            r.cols[5].str(d.names.pool[p.last_name as usize].as_bytes());
            r.cols[6].str(if p.gender == 0 { b"female" } else { b"male" });
            r.cols[7].i64(p.birthday);
            p.ip.write(r.cols[8].str_buf());
            r.cols[8].end_str();
            r.cols[9].str(d.browsers.names[p.browser as usize].as_bytes());
            r.cols[10].i32(p.city);
            {
                let s = r.cols[11].str_buf();
                for (k, &l) in p.languages.iter().enumerate() {
                    if k > 0 {
                        s.push(b';');
                    }
                    s.extend_from_slice(d.languages.name(l).as_bytes());
                }
            }
            r.cols[11].end_str();
            {
                let s = r.cols[12].str_buf();
                let base = d.names.email_base[p.first_name as usize].as_bytes();
                for (k, &e) in p.emails.iter().enumerate() {
                    if k > 0 {
                        s.push(b';');
                    }
                    s.extend_from_slice(base);
                    push_u64(s, p.account_id as u64);
                    s.push(b'@');
                    s.extend_from_slice(d.emails.domains[e as usize].as_bytes());
                }
            }
            r.cols[12].end_str();
        }
        {
            let r = &mut self.out[t::HAS_INTEREST];
            for &tag in &p.interests {
                r.cols[0].i64(p.creation);
                r.cols[1].i64(p.deletion);
                r.cols[2].i64(p.account_id);
                r.cols[3].i32(tag);
            }
        }
        let uni = d.universities.at_location(&d.places, p.university_location);
        if uni != -1 && p.class_year != -1 {
            let r = &mut self.out[t::STUDY_AT];
            r.cols[0].i64(p.creation);
            r.cols[1].i64(p.deletion);
            r.cols[2].i64(p.account_id);
            r.cols[3].i64(uni);
            r.cols[4].i32(dates::year(p.class_year) as i32);
        }
        {
            let r = &mut self.out[t::WORK_AT];
            for &(c, from) in &p.companies {
                r.cols[0].i64(p.creation);
                r.cols[1].i64(p.deletion);
                r.cols[2].i64(p.account_id);
                r.cols[3].i64(c);
                r.cols[4].i32(dates::year(from) as i32);
            }
        }
        {
            let r = &mut self.out[t::KNOWS];
            for k in &w.knows[i as usize] {
                let to = w.persons[k.to as usize].account_id;
                if p.account_id < to {
                    r.cols[0].i64(k.creation);
                    r.cols[1].i64(k.deletion);
                    r.cols[2].bool(k.deleted);
                    r.cols[3].i64(p.account_id);
                    r.cols[4].i64(to);
                }
            }
        }
    }

    // ---------------------------------------------------------- activity

    fn activity(&mut self, i: u32, block: &[u32]) {
        let (w, p) = (self.w, self.person(i));
        // eager parts of generateWall, generateGroups and generateAlbums
        let forum_id = self.forum_counter;
        self.forum_counter += 1;
        let wall = self.create_wall(forum_id, i);
        let moderator = Membership {
            person: i,
            creation: wall.creation + w.p.delta,
            deletion: wall.deletion,
            deleted: false,
        };
        let n_uniform = self.num_posts(
            &wall,
            w.p.max_num_post_per_month,
            w.p.max_num_friends as i32,
        );
        let n_flash = self.num_posts(
            &wall,
            w.p.max_num_flashmob_post_per_month,
            w.p.max_num_friends as i32,
        );
        let moderator_prob = self.farm.get(Aspect::ForumModerator).next_double();
        let num_groups = self
            .farm
            .get(Aspect::NumForum)
            .next_int_bound(w.p.max_num_group_created_per_person)
            + 1;
        let months = w.dates.number_of_months(p.creation) as i32;
        let albums = self
            .farm
            .get(Aspect::NumPhotoAlbum)
            .next_int_bound(w.p.max_num_photo_albums_per_month + 1);
        let num_albums = if albums == 0 { 0 } else { months * albums };

        // the wall
        self.write_forum(&wall);
        let mods = [moderator];
        self.posts(&wall, &mods, n_uniform, false);
        self.posts(&wall, &mods, n_flash, true);

        // groups
        for _ in 0..num_groups {
            if moderator_prob >= w.p.group_moderator_prob {
                continue;
            }
            let id = self.forum_counter;
            self.forum_counter += 1;
            let group = self.create_group(id, i, block);
            let nu = self.num_posts(&group, w.p.max_num_group_post_per_month, w.p.max_group_size);
            let nf = self.num_posts(
                &group,
                w.p.max_num_group_flashmob_post_per_month,
                w.p.max_group_size,
            );
            self.write_forum(&group);
            self.posts(&group, &group.memberships, nu, false);
            self.posts(&group, &group.memberships, nf, true);
        }

        // albums
        for a in 0..num_albums {
            let id = self.forum_counter;
            self.forum_counter += 1;
            let Some(album) = self.create_album(id, i, a) else {
                continue;
            };
            let num_photos = self
                .farm
                .get(Aspect::NumPhoto)
                .next_int_bound(w.p.max_num_photo_per_albums + 1);
            // createPhotos' eager part: popular places (only the draws matter)
            let num_popular = self
                .farm
                .get(Aspect::NumPopular)
                .next_int_bound(w.p.max_num_popular_places + 1);
            for _ in 0..num_popular {
                w.d.popular_place(self.farm.get(Aspect::Popular), album.place);
            }
            self.write_forum(&album);
            self.photos(&album, num_photos);
        }
    }

    fn language_of(&mut self, p: &Person) -> i32 {
        let n = p.languages.len() as i32;
        p.languages[self.farm.get(Aspect::Language).next_int_bound(n) as usize]
    }

    fn create_wall(&mut self, forum_id: i64, i: u32) -> Forum {
        let (w, p) = (self.w, self.person(i));
        let language = self.language_of(p);
        let creation = p.creation + w.p.delta;
        let mut f = Forum {
            id: w.id(forum_id, creation, self.block),
            creation,
            deletion: p.deletion,
            deleted: false,
            moderator: i,
            // titles are only written, not drawn from
            title: if self.dry {
                String::new()
            } else {
                clamp(
                    &format!(
                        "Wall of {} {}",
                        w.d.names.pool[p.first_name as usize], w.d.names.pool[p.last_name as usize]
                    ),
                    256,
                )
            },
            tags: p.interests.clone(),
            language,
            place: p.city,
            memberships: Vec::new(),
        };
        for k in &w.knows[i as usize] {
            let c = k.creation + w.p.delta;
            let del = f.deletion.min(k.deletion);
            if del - c < 0 {
                continue;
            }
            f.memberships.push(Membership {
                person: k.to,
                creation: c,
                deletion: del,
                deleted: false,
            });
        }
        f
    }

    fn create_group(&mut self, forum_id: i64, i: u32, block: &[u32]) -> Forum {
        let w = self.w;
        let (p, d, dates) = (self.person(i), w.d, w.dates);
        let min_c = p.creation + w.p.delta;
        let max_c = p.deletion.min(dates.end);
        let creation = dates.random_date(self.farm.get(Aspect::Date), min_c, max_c);
        let (deleted, deletion) =
            if self.farm.get(Aspect::DeletionForum).next_double() < w.p.prob_forum_deleted {
                let d2 =
                    dates.random_date(self.farm.get(Aspect::Date), creation + w.p.delta, dates.end);
                (true, d2)
            } else {
                (false, dates.network_collapse())
            };
        let language = self.language_of(p);
        let idx = self
            .farm
            .get(Aspect::ForumInterest)
            .next_int_bound(p.interests.len() as i32);
        let interest = p.interests[idx as usize];
        let title = if self.dry {
            String::new()
        } else {
            clamp(
                &format!(
                    "Group for {} in {}",
                    d.tags.names[interest as usize].replace('"', "\\\""),
                    d.places.name(p.city)
                ),
                256,
            )
        };
        let mut f = Forum {
            id: w.id(forum_id, creation, self.block),
            creation,
            deletion,
            deleted,
            moderator: i,
            title,
            tags: vec![interest],
            language,
            place: p.city,
            memberships: Vec::new(),
        };
        let mut members: Vec<i64> = Vec::new(); // sorted account ids
        let friends = &w.knows[i as usize];
        let num_friends = friends.len() as i32;
        let group_size = self
            .farm
            .get(Aspect::NumUsersPerForum)
            .next_int_bound(w.p.max_group_size) as usize;
        let mut loops = 0u32;
        while f.memberships.len() < group_size && loops < w.p.block_size {
            let prob = self.farm.get(Aspect::KnowsLevel).next_double();
            if prob < 0.3 && num_friends > 0 {
                let k = friends[self
                    .farm
                    .get(Aspect::MembershipIndex)
                    .next_int_bound(num_friends) as usize];
                let to = &w.hot[k.to as usize];
                if members.binary_search(&to.account_id).is_err() {
                    let min_c = f.creation.max(to.creation) + w.p.delta;
                    let max_c = f.deletion.min(to.deletion).min(dates.end);
                    if max_c - min_c > 0 {
                        let c =
                            dates.random_date(self.farm.get(Aspect::MembershipIndex), min_c, max_c);
                        let (del_flag, del);
                        if self.farm.get(Aspect::DeletionMemb).next_double() < w.p.prob_memb_deleted
                        {
                            let min_d = c + w.p.delta;
                            let max_d = to.deletion.min(f.deletion).min(dates.end);
                            if max_d - min_d < 0 {
                                continue; // skips numLoop++, as the Java does
                            }
                            del_flag = true;
                            del = dates.random_date(
                                self.farm.get(Aspect::MembershipIndex),
                                min_d,
                                max_d,
                            );
                        } else {
                            del_flag = false;
                            del = to.deletion.min(f.deletion);
                        }
                        f.memberships.push(Membership {
                            person: k.to,
                            creation: c,
                            deletion: del,
                            deleted: del_flag,
                        });
                        add_sorted_i64(&mut members, to.account_id);
                    }
                }
            } else {
                let cand = block[self
                    .farm
                    .get(Aspect::MembershipIndex)
                    .next_int_bound(block.len() as i32) as usize];
                let m = &w.hot[cand as usize];
                let prob = self.farm.get(Aspect::Membership).next_double();
                if prob < 0.1 && members.binary_search(&m.account_id).is_err() {
                    let min_c = f.creation.max(m.creation) + w.p.delta;
                    let max_c = f.deletion.min(m.deletion).min(dates.end);
                    if max_c - min_c > 0 {
                        let c =
                            dates.random_date(self.farm.get(Aspect::MembershipIndex), min_c, max_c);
                        let (del_flag, del);
                        if self.farm.get(Aspect::DeletionMemb).next_double() < w.p.prob_memb_deleted
                        {
                            let min_d = c + w.p.delta;
                            let max_d = m.deletion.min(f.deletion).min(dates.end);
                            // the Java compares the *creation* bound here
                            if max_c - min_d < 0 {
                                continue;
                            }
                            del_flag = true;
                            del = dates.random_date(
                                self.farm.get(Aspect::MembershipIndex),
                                min_d,
                                max_d,
                            );
                        } else {
                            del_flag = false;
                            del = m.deletion.min(f.deletion);
                        }
                        f.memberships.push(Membership {
                            person: cand,
                            creation: c,
                            deletion: del,
                            deleted: del_flag,
                        });
                        add_sorted_i64(&mut members, m.account_id);
                    }
                }
            }
            loops += 1;
        }
        f
    }

    fn create_album(&mut self, forum_id: i64, i: u32, num: i32) -> Option<Forum> {
        let w = self.w;
        let (p, dates) = (self.person(i), w.dates);
        let min_c = p.creation + w.p.delta;
        let max_c = p.deletion.min(dates.end);
        let creation = dates.random_date(self.farm.get(Aspect::Date), min_c, max_c);
        let (deleted, deletion) =
            if self.farm.get(Aspect::DeletionForum).next_double() < w.p.prob_forum_deleted {
                let max_d = p.deletion.min(dates.end);
                if max_d - min_c < 0 {
                    return None;
                }
                (
                    true,
                    dates.random_date(self.farm.get(Aspect::Date), creation + w.p.delta, max_d),
                )
            } else {
                (false, p.deletion)
            };
        let language = self.language_of(p);
        let title = if self.dry {
            String::new()
        } else {
            clamp(
                &format!(
                    "Album {num} of {} {}",
                    w.d.names.pool[p.first_name as usize], w.d.names.pool[p.last_name as usize]
                ),
                256,
            )
        };
        let idx = self
            .farm
            .get(Aspect::ForumInterest)
            .next_int_bound(p.interests.len() as i32);
        let interest = p.interests[idx as usize];
        let place = self
            .farm
            .get(Aspect::Country)
            .next_int_bound(w.d.places.num_countries() as i32);
        let mut f = Forum {
            id: w.id(forum_id, creation, self.block),
            creation,
            deletion,
            deleted,
            moderator: i,
            title,
            tags: vec![interest],
            language,
            place,
            memberships: Vec::new(),
        };
        for k in &w.knows[i as usize] {
            let prob = self.farm.get(Aspect::AlbumMembership).next_double();
            if prob < 0.7 {
                let to = &w.hot[k.to as usize];
                let c = to.creation.max(f.creation) + w.p.delta;
                let del = to.deletion.min(f.deletion);
                if del - c > 0 {
                    f.memberships.push(Membership {
                        person: k.to,
                        creation: c,
                        deletion: del,
                        deleted: false,
                    });
                }
            }
        }
        Some(f)
    }

    /// `numPostsPerGroup`.
    fn num_posts(&mut self, f: &Forum, max_per_month: i32, max_members: i32) -> i64 {
        let months = self.w.dates.number_of_months(f.creation) as i32;
        let r = self.farm.get(Aspect::NumPost);
        let n = if months == 0 {
            r.next_int_bound(max_per_month + 1)
        } else {
            r.next_int_bound(max_per_month * months + 1)
        };
        ((n as i64 * f.memberships.len() as i64) as i32 / max_members) as i64
    }

    fn write_forum(&mut self, f: &Forum) {
        if self.dry {
            return;
        }
        let w = self.w;
        {
            let r = &mut self.out[t::FORUM];
            r.cols[0].i64(f.creation);
            r.cols[1].i64(f.deletion);
            r.cols[2].bool(f.deleted);
            r.cols[3].i64(f.id);
            r.cols[4].str(f.title.as_bytes());
            r.cols[5].i64(w.persons[f.moderator as usize].account_id);
        }
        {
            let r = &mut self.out[t::FORUM_TAG];
            for &tag in &f.tags {
                r.cols[0].i64(f.creation);
                r.cols[1].i64(f.deletion);
                r.cols[2].i64(f.id);
                r.cols[3].i32(tag);
            }
        }
        let r = &mut self.out[t::FORUM_MEMBER];
        for m in &f.memberships {
            r.cols[0].i64(m.creation);
            r.cols[1].i64(m.deletion);
            r.cols[2].bool(m.deleted);
            r.cols[3].i64(f.id);
            r.cols[4].i64(w.persons[m.person as usize].account_id);
        }
    }

    // ------------------------------------------------------------ posts

    /// `PostGenerator.createPosts`, consumed.
    fn posts(&mut self, f: &Forum, members: &[Membership], num: i64, flashmob: bool) {
        let w = self.w;
        for m in members {
            let mut per = num as f64 / members.len() as f64;
            if per < 1.0 {
                let prob = self.farm.get(Aspect::NumPost).next_double();
                if prob < per {
                    per = 1.0;
                }
            } else {
                per = per.ceil();
            }
            let per = per as i32;
            let num_comments = self
                .farm
                .get(Aspect::NumComment)
                .next_int_bound(w.p.max_num_comments + 1);
            for _ in 0..per {
                let core = if flashmob {
                    self.flashmob_post_info(f, m, num_comments)
                } else {
                    self.uniform_post_info(f, m, num_comments)
                };
                let Some(core) = core else { continue };
                self.post(f, m, core, num_comments);
            }
        }
    }

    fn delete_post(
        &mut self,
        m: &Membership,
        creation: i64,
        num_comments: i32,
    ) -> Option<(bool, i64)> {
        let w = self.w;
        let person = &w.hot[m.person as usize];
        let end = m.deletion.min(w.dates.end);
        if person.message_deleter
            && self.farm.get(Aspect::DeletionPost).next_double()
                < POST_MAPPING[num_comments as usize]
        {
            let min_d = creation + w.p.delta;
            if end - min_d < 0 {
                return None;
            }
            Some((
                true,
                w.dates
                    .power_law_delete_date(self.farm.get(Aspect::Date), min_d, end),
            ))
        } else {
            Some((false, end))
        }
    }

    fn uniform_post_info(
        &mut self,
        f: &Forum,
        m: &Membership,
        num_comments: i32,
    ) -> Option<PostCore> {
        let w = self.w;
        let min_c = m.creation + w.p.delta;
        let max_c = m.deletion.min(w.dates.end);
        if max_c - min_c < 0 {
            return None;
        }
        let creation = w
            .dates
            .random_date(self.farm.get(Aspect::Date), min_c, max_c);
        let (deleted, deletion) = self.delete_post(m, creation, num_comments)?;
        let mut tags = Vec::with_capacity(f.tags.len());
        for &v in &f.tags {
            if tags.is_empty() {
                tags.push(v);
            } else if self.farm.get(Aspect::Tag).next_double() < 0.05 {
                add_sorted(&mut tags, v);
            }
        }
        Some(PostCore {
            tags,
            creation,
            deletion,
            deleted,
        })
    }

    fn populate_flash_tags(&mut self, f: &Forum) {
        let w = self.w;
        let mut tmp = take(&mut self.flash_scratch);
        w.d.flashmobs
            .generate(self.farm.get(Aspect::Tag), f.creation, &mut tmp);
        let sum: i32 = tmp.iter().map(|t| t.level).sum();
        // already sorted by date (a stable sort keeps them)
        let mut cur = 0.0f64;
        for t in &mut tmp {
            t.prob = cur;
            cur += t.level as f64 / sum as f64;
        }
        self.flash_scratch = replace(&mut self.flash_tags, tmp);
    }

    fn flashmob_post_info(
        &mut self,
        f: &Forum,
        m: &Membership,
        num_comments: i32,
    ) -> Option<PostCore> {
        let w = self.w;
        if self.flash_forum != f.id {
            self.populate_flash_tags(f);
            self.flash_forum = f.id;
        }
        let tags = &self.flash_tags;
        if tags.is_empty() {
            return None;
        }
        // searchEarliest
        let from = m.creation + FLASHMOB_SPAN / 2 + w.p.delta;
        let (mut lo, mut hi) = (0usize, tags.len() - 1);
        let mut mid = (hi + lo) / 2;
        while hi > lo + 1 {
            if tags[mid].date > from {
                hi = mid;
            } else {
                lo = mid;
            }
            mid = (hi + lo) / 2;
        }
        if tags[mid].date < from {
            return None;
        }
        // selectRandomTag
        let (mut lo, mut hi) = (mid, tags.len() - 1);
        let r = self.farm.get(Aspect::Tag);
        let prob = r.next_double() * (tags[hi].prob - tags[lo].prob) + tags[lo].prob;
        let mut mid = (hi + lo) / 2;
        while hi > lo + 1 {
            if tags[mid].prob > prob {
                hi = mid;
            } else {
                lo = mid;
            }
            mid = (hi + lo) / 2;
        }
        let ft = tags[mid];
        let mut post_tags = vec![ft.tag];
        for _ in 0..w.p.max_num_tag_per_flashmob_post - 1 {
            let r = self.farm.get(Aspect::Tag);
            if r.next_double() < 0.05 {
                let t = w.d.tag_matrix.random_related(r, ft.tag);
                add_sorted(&mut post_tags, t);
            }
        }
        let min_c = m.creation + w.p.delta;
        let max_c = m.deletion.min(w.dates.end);
        if max_c - min_c < 0 {
            return None;
        }
        let prob = w.d.flashmob_dist.next_double(self.farm.get(Aspect::Date));
        let creation = ft.date - FLASHMOB_SPAN / 2 + (prob * FLASHMOB_SPAN as f64) as i64;
        if creation <= min_c || creation >= max_c {
            return None;
        }
        let (deleted, deletion) = self.delete_post(m, creation, num_comments)?;
        Some(PostCore {
            tags: post_tags,
            creation,
            deletion,
            deleted,
        })
    }

    /// `LdbcSnbTextGenerator.generateText` into `self.text`.
    fn gen_text(&mut self, author: u32, tags: &[i32], post: bool) {
        let w = self.w;
        let large = w.hot[author as usize].large_poster;
        let (ratio, lmin, lmax, min, max) = if post {
            (
                w.p.ratio_large_post,
                w.p.min_large_post_size,
                w.p.max_large_post_size,
                w.p.min_text_size,
                w.p.max_text_size,
            )
        } else {
            (
                w.p.ratio_large_comment,
                w.p.min_large_comment_size,
                w.p.max_large_comment_size,
                w.p.min_comment_size,
                w.p.max_comment_size,
            )
        };
        let r = self.farm.get(Aspect::LargeText);
        let size = if large && r.next_double() > (1.0 - ratio) {
            w.d.tag_text.random_large_text_size(r, lmin, lmax)
        } else {
            w.d.tag_text.random_text_size(r, min, max)
        };
        self.text_len = if self.dry {
            w.d.tag_text.generated_len(r, tags, size)
        } else {
            w.d.tag_text.generate(r, tags, size, &mut self.text);
            self.text.len()
        };
    }

    /// Country and IP of a message: a random country's when travelling,
    /// else `None` (the author's, looked up only when writing).
    fn location(&mut self, creation: i64) -> Option<(i32, Ip)> {
        let w = self.w;
        let prob = self.farm.get(Aspect::DiffIpForTraveler).next_double();
        let travel = dates::is_travel_season(creation);
        if (travel && prob < w.p.prob_diff_ip_in_travel_season)
            || (!travel && prob < w.p.prob_diff_ip_not_travel_season)
        {
            let c =
                w.d.places
                    .random_country_uniform(self.farm.get(Aspect::Country));
            let ip = w.d.ips.ip(self.farm.get(Aspect::Ip), c);
            Some((c, ip))
        } else {
            None
        }
    }

    fn post(&mut self, f: &Forum, m: &Membership, core: PostCore, num_comments: i32) {
        let w = self.w;
        self.gen_text(m.person, &core.tags, true);
        let loc = self.location(core.creation);
        let id = w.id(self.message_counter, core.creation, self.block);
        self.message_counter += 1;
        let browser = w.d.browsers.post_browser(&mut self.farm);
        let likes = if self.farm.get(Aspect::NumLike).next_double() <= 0.1 {
            Some(self.likes_prologue(f))
        } else {
            None
        };
        if !self.dry {
            let author = &w.persons[m.person as usize];
            let (country, ip) = loc.unwrap_or((author.country, author.ip));
            let browser = browser.unwrap_or(author.browser);
            let r = &mut self.out[t::POST];
            r.cols[0].i64(core.creation);
            r.cols[1].i64(core.deletion);
            r.cols[2].bool(core.deleted);
            r.cols[3].i64(id);
            r.cols[4].null_str();
            ip.write(r.cols[5].str_buf());
            r.cols[5].end_str();
            r.cols[6].str(w.d.browsers.names[browser as usize].as_bytes());
            r.cols[7].str(w.d.languages.name(f.language).as_bytes());
            push_utf16(r.cols[8].str_buf(), &self.text);
            r.cols[8].end_str();
            r.cols[9].i32(self.text.len() as i32);
            r.cols[10].i64(author.account_id);
            r.cols[11].i64(f.id);
            r.cols[12].i64(country as i64);
        }
        if !self.dry {
            let r = &mut self.out[t::POST_TAG];
            for &tag in &core.tags {
                r.cols[0].i64(core.creation);
                r.cols[1].i64(core.deletion);
                r.cols[2].i64(id);
                r.cols[3].i32(tag);
            }
        }
        let msg = Parent {
            id,
            creation: core.creation,
            deletion: core.deletion,
            tags: core.tags,
        };
        if let Some(l) = likes {
            self.likes(f, &msg, l, t::LIKES_POST);
        }
        self.comments(f, msg, num_comments);
    }

    /// `generateLikes`' eager part: `(numLikes, startIndex)`.
    fn likes_prologue(&mut self, f: &Forum) -> (i32, i32) {
        let n = f.memberships.len() as i32;
        let r = self.farm.get(Aspect::NumLike);
        let likes = self.like_power.value(r).min(n);
        let start = if likes < n {
            r.next_int_bound(n - likes)
        } else {
            0
        };
        (likes, start)
    }

    /// `generateLikes`' stream, consumed.
    fn likes(&mut self, f: &Forum, msg: &Parent, (num, start): (i32, i32), table: usize) {
        let w = self.w;
        for i in 0..num {
            let m = f.memberships[(start + i) as usize];
            let person = &w.hot[m.person as usize];
            let min_c = person.creation.max(msg.creation) + w.p.delta;
            let max_c = (msg.creation + SEVEN_DAYS)
                .min(person.deletion)
                .min(msg.deletion)
                .min(w.dates.end);
            if max_c <= min_c {
                continue;
            }
            let c = w
                .dates
                .random_date(self.farm.get(Aspect::NumLike), min_c, max_c);
            let (deleted, deletion);
            if person.message_deleter
                && self.farm.get(Aspect::DeletionLikes).next_double() < w.p.prob_like_deleted
            {
                let min_d = c + w.p.delta;
                let max_d = person.deletion.min(msg.deletion).min(w.dates.end);
                if max_d <= min_d {
                    continue;
                }
                deleted = true;
                deletion =
                    w.dates
                        .power_law_delete_date(self.farm.get(Aspect::NumLike), min_d, max_d);
            } else {
                deleted = false;
                deletion = person.deletion.min(msg.deletion);
            }
            if self.dry {
                continue;
            }
            let r = &mut self.out[table];
            r.cols[0].i64(c);
            r.cols[1].i64(deletion);
            r.cols[2].bool(deleted);
            r.cols[3].i64(person.account_id);
            r.cols[4].i64(msg.id);
        }
    }

    // --------------------------------------------------------- comments

    fn comments(&mut self, f: &Forum, post: Parent, num: i32) {
        let w = self.w;
        let post_id = post.id;
        // scratch buffers kept across calls (a block makes millions of
        // comments): the parents, their cached masks, and tag lists
        let mut parents = take(&mut self.parents);
        parents.clear();
        parents.push(post);
        let mut cached = take(&mut self.cached);
        cached.clear();
        self.m_masks.clear();
        let mut tags: Vec<i32> = self.tag_pool.pop().unwrap_or_default();
        tags.clear();
        if self.m_forum != f.id {
            self.m_forum = f.id;
            self.m_creation.clear();
            self.m_deletion.clear();
            self.m_creation
                .extend(f.memberships.iter().map(|m| m.creation));
            self.m_deletion
                .extend(f.memberships.iter().map(|m| m.deletion));
        }
        for _ in 0..num {
            let pi = self
                .farm
                .get(Aspect::ReplyTo)
                .next_int_bound(parents.len() as i32) as usize;
            // the members active at the parent's creation or deletion
            // (a parent is often drawn again: keep its members' masks)
            let stride = self.m_creation.len().div_ceil(8);
            if cached.len() <= pi {
                cached.resize(pi + 1, None);
            }
            let (count, start) = *cached[pi].get_or_insert_with(|| {
                let (pc, pd) = (parents[pi].creation, parents[pi].deletion);
                let start = self.m_masks.len();
                let n = active_masks(
                    &self.m_creation,
                    &self.m_deletion,
                    pc,
                    pd,
                    &mut self.m_masks,
                );
                (n, start)
            });
            if count == 0 {
                break;
            }
            let k = self
                .farm
                .get(Aspect::MembershipIndex)
                .next_int_bound(count as i32) as u32;
            let at = nth_set(&self.m_masks[start..start + stride], k);
            let m = f.memberships[at];
            let author = &w.hot[m.person as usize];
            tags.clear();
            let short;
            if self.farm.get(Aspect::ReducedText).next_double() > 0.6666 {
                short = None;
                let par_tags = &parents[pi].tags;
                for &tag in par_tags {
                    if self.farm.get(Aspect::Tag).next_double() > 0.5 {
                        add_sorted(&mut tags, tag);
                    }
                }
                let n = (par_tags.len() as f64 / 2.0).ceil() as usize;
                // the related tags' positions first, then their values: the
                // Tag and Topic draws are on generators of their own, so
                // this keeps every draw's order, and the (cache-missing)
                // reads of the related tags overlap
                self.related.clear();
                for _ in 0..n {
                    let [rt, rtop] = self.farm.get_two(Aspect::Tag, Aspect::Topic);
                    let t = par_tags[rt.next_int_bound(par_tags.len() as i32) as usize];
                    self.related
                        .push(w.d.tag_matrix.random_related_index(rtop, t));
                }
                for &k in &self.related {
                    add_sorted(&mut tags, w.d.tag_matrix.related_at(k));
                }
                let tags_now = take(&mut tags);
                self.gen_text(m.person, &tags_now, false);
                tags = tags_now;
            } else {
                let idx = self
                    .farm
                    .get(Aspect::TextSize)
                    .next_int_bound(SHORT_COMMENTS.len() as i32) as usize;
                short = Some(SHORT_COMMENTS[idx]);
            }
            let par = &parents[pi];
            let min_c = par.creation.max(m.creation) + w.p.delta;
            let max_c = m.deletion.min(par.deletion).min(w.dates.end);
            if max_c <= min_c {
                continue;
            }
            let creation = w
                .dates
                .power_law_comm_date_day(self.farm.get(Aspect::Date), min_c);
            if creation > max_c {
                continue;
            }
            let (deleted, deletion);
            if author.message_deleter
                && self.farm.get(Aspect::DeletionComm).next_double() < w.p.prob_comment_deleted
            {
                let min_d = creation + w.p.delta;
                let max_d = par.deletion.min(m.deletion).min(w.dates.end);
                if max_d <= min_d {
                    continue;
                }
                deleted = true;
                deletion = w
                    .dates
                    .power_law_delete_date(self.farm.get(Aspect::Date), min_d, max_d);
            } else {
                deleted = false;
                deletion = par.deletion.min(m.deletion);
            }
            let par_id = par.id;
            let loc = self.location(creation);
            let id = w.id(self.message_counter, creation, self.block);
            self.message_counter += 1;
            let browser = w.d.browsers.post_browser(&mut self.farm);
            let len = match short {
                Some(s) => s.len(),
                None => self.text_len,
            };
            if !self.dry {
                let author = &w.persons[m.person as usize];
                let (country, ip) = loc.unwrap_or((author.country, author.ip));
                let browser = browser.unwrap_or(author.browser);
                let r = &mut self.out[t::COMMENT];
                r.cols[0].i64(creation);
                r.cols[1].i64(deletion);
                r.cols[2].bool(deleted);
                r.cols[3].i64(id);
                ip.write(r.cols[4].str_buf());
                r.cols[4].end_str();
                r.cols[5].str(w.d.browsers.names[browser as usize].as_bytes());
                match short {
                    Some(s) => r.cols[6].str(s.as_bytes()),
                    None => {
                        push_utf16(r.cols[6].str_buf(), &self.text);
                        r.cols[6].end_str();
                    }
                }
                r.cols[7].i32(len as i32);
                r.cols[8].i64(author.account_id);
                r.cols[9].i32(country);
                if par_id == post_id {
                    r.cols[10].opt_i64(Some(par_id));
                    r.cols[11].opt_i64(None);
                } else {
                    r.cols[10].opt_i64(None);
                    r.cols[11].opt_i64(Some(par_id));
                }
            }
            if !self.dry {
                let r = &mut self.out[t::COMMENT_TAG];
                for &tag in &tags {
                    r.cols[0].i64(creation);
                    r.cols[1].i64(deletion);
                    r.cols[2].i64(id);
                    r.cols[3].i32(tag);
                }
            }
            let mut msg = Parent {
                id,
                creation,
                deletion,
                tags: Vec::new(),
            };
            let likes = if len > 10 && self.farm.get(Aspect::NumLike).next_double() <= 0.1 {
                Some(self.likes_prologue(f))
            } else {
                None
            };
            if let Some(l) = likes {
                self.likes(f, &msg, l, t::LIKES_COMMENT);
            }
            if short.is_none() {
                let mut fresh = self.tag_pool.pop().unwrap_or_default();
                fresh.clear();
                msg.tags = replace(&mut tags, fresh);
                parents.push(msg);
            }
        }
        // the comments' tag lists back to the pool, as many as were taken
        // (the post's own came from elsewhere), so the pool stays small
        self.tag_pool.push(tags);
        self.tag_pool
            .extend(parents.drain(..).skip(1).map(|p| p.tags));
        self.parents = parents;
        self.cached = cached;
    }

    // ----------------------------------------------------------- photos

    fn photos(&mut self, album: &Forum, num: i32) {
        let w = self.w;
        let moderator = &w.persons[album.moderator as usize];
        for i in 0..num as i64 {
            let creation = album.creation + w.p.delta + 1000 * (i + 1);
            if creation >= album.deletion {
                break;
            }
            let (deleted, deletion);
            if moderator.message_deleter
                && self.farm.get(Aspect::DeletionPost).next_double() < w.p.prob_photo_deleted
            {
                deleted = true;
                let min_d = creation + w.p.delta;
                let max_d = album.deletion.min(w.dates.end);
                deletion = w
                    .dates
                    .power_law_delete_date(self.farm.get(Aspect::Date), min_d, max_d);
            } else {
                deleted = false;
                deletion = album.deletion;
            }
            let (country, ip) = self
                .location(creation)
                .unwrap_or((moderator.country, moderator.ip));
            let id = w.id(self.message_counter, creation, self.block);
            self.message_counter += 1;
            let likes = if self.farm.get(Aspect::NumLike).next_double() <= 0.1 {
                Some(self.likes_prologue(album))
            } else {
                None
            };
            if !self.dry {
                let r = &mut self.out[t::POST];
                r.cols[0].i64(creation);
                r.cols[1].i64(deletion);
                r.cols[2].bool(deleted);
                r.cols[3].i64(id);
                {
                    let s = r.cols[4].str_buf();
                    s.extend_from_slice(b"photo");
                    push_u64(s, id as u64);
                    s.extend_from_slice(b".jpg");
                }
                r.cols[4].end_str();
                ip.write(r.cols[5].str_buf());
                r.cols[5].end_str();
                r.cols[6].str(w.d.browsers.names[moderator.browser as usize].as_bytes());
                r.cols[7].null_str();
                r.cols[8].null_str();
                r.cols[9].i32(0);
                r.cols[10].i64(moderator.account_id);
                r.cols[11].i64(album.id);
                r.cols[12].i64(country as i64);
            }
            if let Some(l) = likes {
                let msg = Parent {
                    id,
                    creation,
                    deletion,
                    tags: Vec::new(),
                };
                self.likes(album, &msg, l, t::LIKES_POST);
            }
        }
    }
}

/// Which members (by creation `c` and deletion `d`) are active at `pc`
/// or at `pd`: appends one bit per member to `masks` (8 per byte) and
/// returns how many. Branch-free over groups of 8, so it vectorises.
fn active_masks(c: &[i64], d: &[i64], pc: i64, pd: i64, masks: &mut Vec<u8>) -> u32 {
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: the CPU has AVX2
        return unsafe { active_masks_avx2(c, d, pc, pd, masks) };
    }
    active_masks_any(c, d, pc, pd, masks)
}

/// [`active_masks`] compiled for AVX2.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn active_masks_avx2(c: &[i64], d: &[i64], pc: i64, pd: i64, masks: &mut Vec<u8>) -> u32 {
    active_masks_any(c, d, pc, pd, masks)
}

#[inline(always)]
fn active_masks_any(c: &[i64], d: &[i64], pc: i64, pd: i64, masks: &mut Vec<u8>) -> u32 {
    let active = |c: i64, d: i64| ((c < pc) & (d > pc)) | ((c < pd) & (d > pd));
    let mut count = 0;
    let (cc, cr) = c.as_chunks::<8>();
    let (dc, dr) = d.as_chunks::<8>();
    for (c8, d8) in cc.iter().zip(dc) {
        let mut m = 0u8;
        for l in 0..8 {
            m |= (active(c8[l], d8[l]) as u8) << l;
        }
        count += m.count_ones();
        masks.push(m);
    }
    if !cr.is_empty() {
        let mut m = 0u8;
        for (l, (&c, &d)) in cr.iter().zip(dr).enumerate() {
            m |= (active(c, d) as u8) << l;
        }
        count += m.count_ones();
        masks.push(m);
    }
    count
}

/// The position of the `k`-th set bit (from 0) of `masks`.
fn nth_set(masks: &[u8], mut k: u32) -> usize {
    for (i, &m) in masks.iter().enumerate() {
        let n = m.count_ones();
        if k < n {
            let mut m = m;
            for _ in 0..k {
                m &= m - 1;
            }
            return i * 8 + m.trailing_zeros() as usize;
        }
        k -= n;
    }
    unreachable!("k < number of set bits")
}

fn add_sorted_i64(set: &mut Vec<i64>, x: i64) {
    if let Err(p) = set.binary_search(&x) {
        set.insert(p, x);
    }
}
