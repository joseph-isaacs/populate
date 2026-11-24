//! Friendships: three passes of `DistanceKnowsGenerator` over persons
//! sorted by university, main interest and random id, merged per person
//! (`SparkKnowsGenerator`, `SparkKnowsMerger`, `FriendshipMerger`).

use crate::dates::Dates;
use crate::dict::Dictionaries;
use crate::jrand::{Aspect, Farm};
use crate::params::Params;
use crate::person::Person;

/// A friendship, seen from one side.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Knows {
    /// Global index of the friend.
    pub to: u32,
    pub creation: i64,
    pub deletion: i64,
    pub deleted: bool,
}

/// Share of each person's degree filled by each pass.
pub const PERCENTAGES: [f32; 3] = [0.45, 0.45, 0.1];

/// `Knows.targetEdges`.
pub fn target_edges(max_knows: i64, step: usize) -> i64 {
    let mut generated: i32 = 0;
    for &pc in &PERCENTAGES[..step] {
        generated = (generated as f64 + (pc * max_knows as f32).ceil() as f64) as i32;
    }
    let max = max_knows as i32;
    generated = generated.min(max);
    (max - generated).min((PERCENTAGES[step] * max_knows as f32).ceil() as i32) as i64
}

/// The sort key of pass `step` (`byUni`, `byInterest`, `byRandomId`), then
/// the account id.
pub fn rank_key(p: &Person, step: usize) -> (i64, i64) {
    let k = match step {
        0 => p.university_location as i64,
        1 => p.main_interest as i64,
        _ => p.random_id,
    };
    (k, p.account_id)
}

/// Global indices of all persons sorted by the key of pass `step`.
pub fn ranked(persons: &[Person], step: usize) -> Vec<u32> {
    let mut keyed: Vec<((i64, i64), u32)> = persons
        .iter()
        .enumerate()
        .map(|(i, p)| (rank_key(p, step), i as u32))
        .collect();
    keyed.sort_unstable_by_key(|&(k, _)| k);
    keyed.into_iter().map(|(_, i)| i).collect()
}

/// `GeoDistanceSimilarity`.
fn similarity(d: &Dictionaries, a: &Person, b: &Person) -> f32 {
    let za = d.places.zorder(a.country);
    let zb = d.places.zorder(b.country);
    1.0f32 - ((za - zb).abs() as f32 / 256.0f32)
}

/// One block of one pass: `DistanceKnowsGenerator.generateKnows` over the
/// persons `members` (global indices, in rank order). Returns each member's
/// knows of this pass.
pub fn distance_block(
    p: &Params,
    d: &Dictionaries,
    dates: &Dates,
    persons: &[Person],
    members: &[u32],
    block: u64,
    step: usize,
) -> Vec<Vec<Knows>> {
    let n = members.len();
    let mut farm = Farm::new(block as i64);
    let mut knows: Vec<Vec<Knows>> = vec![Vec::new(); n];
    let target: Vec<i64> = members
        .iter()
        .map(|&m| target_edges(persons[m as usize].max_knows, step))
        .collect();
    for i in 0..n {
        let pa = &persons[members[i] as usize];
        let mut j = i + 1;
        while target[i] > knows[i].len() as i64 && j < n {
            // know()
            if (knows[i].len() as i64) < target[i] && (knows[j].len() as i64) < target[j] {
                let rand = farm.get(Aspect::Uniform).next_double();
                let prob = p.base_prob_correlated.powf((j - i) as f64);
                if rand < prob || rand < p.limit_pro_correlated {
                    let pb = &persons[members[j] as usize];
                    if let Some(k) = create_knows(p, d, dates, &mut farm, pa, pb) {
                        knows[j].push(Knows {
                            to: members[i],
                            ..k
                        });
                        knows[i].push(Knows {
                            to: members[j],
                            ..k
                        });
                    }
                }
            }
            j += 1;
        }
    }
    knows
}

/// `Knows.createKnow` (the variant without `ignore`): the friendship's
/// dates, or `None` if the two never overlap.
fn create_knows(
    p: &Params,
    d: &Dictionaries,
    dates: &Dates,
    farm: &mut Farm,
    a: &Person,
    b: &Person,
) -> Option<Knows> {
    if a.creation + p.delta > b.deletion || b.creation + p.delta > a.deletion {
        return None;
    }
    let sim = similarity(d, a, b);
    let ad = (a.creation, a.deletion);
    let bd = (b.creation, b.deletion);
    let creation = dates.knows_creation(farm.get(Aspect::Date), ad, bd);
    let delete_prob = if (sim as f64) < 0.9222521 {
        0.025
    } else {
        0.075
    };
    let (deleted, deletion) = if farm.get(Aspect::DeletionKnows).next_double() < delete_prob {
        (
            true,
            dates.knows_deletion(farm.get(Aspect::Date), ad, bd, creation),
        )
    } else {
        (false, a.deletion.min(b.deletion))
    };
    Some(Knows {
        to: 0,
        creation,
        deletion,
        deleted,
    })
}

/// `FriendshipMerger`: one person's knows of all passes, sorted by friend
/// and creation date, one per friend.
pub fn merge(persons: &[Person], mut all: Vec<Knows>) -> Vec<Knows> {
    // stable, like List.sort
    all.sort_by_key(|k| (persons[k.to as usize].account_id, k.creation));
    all.dedup_by_key(|k| persons[k.to as usize].account_id);
    all
}
