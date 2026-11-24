// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), joint_distribution.c. See populate-dsb/NOTICE.md.

//! DSB's attribute key maps: for each value of an attribute of a
//! dimension (an item's category, an address's state, ...), the keys of the
//! rows with that value. dsdgen writes them to `.meta` files after a
//! dimension is generated and reads them back when a later table draws
//! correlated or jointly distributed foreign keys. Here they are computed
//! from the dimensions' generators and cached per scale.

use super::uniform_int;
use crate::config::{Session, Table};
use crate::distribution::items_distributions::{category_count, colors_distribution};
use crate::distribution::{DemographicsDistributions, FipsCountyDistribution};
use crate::generator::{CustomerGeneratorColumn, GeneratorColumn};
use crate::random::RandomNumberStream;
use crate::row::{
    CustomerAddressRowGenerator, CustomerDemographicsRowGenerator,
    HouseholdDemographicsRowGenerator, ItemRowGenerator,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

/// `ATTRIBUTE_KEY_MAP`: keys grouped by attribute, ascending within each
/// attribute (the order dsdgen records and serialises them in).
#[derive(Debug)]
pub struct AttributeKeyMap {
    /// Keys of each attribute (`attributeKeyCount`).
    counts: Vec<i32>,
    /// Index into `keys` of each attribute's first key (`firstKeyIndex`).
    first: Vec<usize>,
    /// The keys (1-based), attribute by attribute.
    keys: Vec<i64>,
    /// The 1-based attribute of each key (`attribute`, by key - 1).
    attribute: Vec<i32>,
}

impl AttributeKeyMap {
    /// The map of `attribute_count` attributes where key `k` (1-based) has
    /// attribute `attributes[k - 1]` (1-based).
    pub fn from_attributes(attribute_count: usize, attributes: Vec<i32>) -> Self {
        let mut counts = vec![0i32; attribute_count];
        for &a in &attributes {
            counts[(a - 1) as usize] += 1;
        }
        let mut first = Vec::with_capacity(attribute_count);
        let mut at = 0usize;
        for &c in &counts {
            first.push(at);
            at += c as usize;
        }
        let mut next = first.clone();
        let mut keys = vec![0i64; attributes.len()];
        for (k, &a) in attributes.iter().enumerate() {
            let slot = &mut next[(a - 1) as usize];
            keys[*slot] = k as i64 + 1;
            *slot += 1;
        }
        AttributeKeyMap {
            counts,
            first,
            keys,
            attribute: attributes,
        }
    }

    /// `attributeCount`.
    pub fn attribute_count(&self) -> i32 {
        self.counts.len() as i32
    }

    /// `attributeKeyCount[attribute - 1]`.
    pub fn key_count(&self, attribute: i32) -> i32 {
        self.counts[(attribute - 1) as usize]
    }

    /// `attribute[key - 1]`: the 1-based attribute of `key`.
    pub fn attribute_of(&self, key: i64) -> i32 {
        self.attribute[(key - 1) as usize]
    }

    /// `genrandKeyWithAttribute(map, DIST_UNIFORM, attribute)`: a uniform
    /// key of those with `attribute`. One draw.
    pub fn key_with_attribute(&self, attribute: i32, stream: &mut RandomNumberStream) -> i64 {
        let a = (attribute - 1) as usize;
        let i = uniform_int(1, self.counts[a], stream);
        self.keys[self.first[a] + i as usize - 1]
    }
}

/// Ids for string attribute values in order of first appearance
/// (`searchAttributeId`).
#[derive(Default)]
struct FirstAppearance {
    ids: HashMap<String, i32>,
}

impl FirstAppearance {
    fn id(&mut self, value: &str) -> i32 {
        let next = self.ids.len() as i32 + 1;
        *self.ids.entry(value.to_string()).or_insert(next)
    }

    fn count(&self) -> usize {
        self.ids.len()
    }
}

/// The maps dsdgen serialises, by `.meta` name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MapName {
    /// `item_category`: item key to `i_category_id`.
    ItemCategory,
    /// `item_color`: item key to its `colors` id.
    ItemColor,
    /// `customer_address_county`: address key to its fips county.
    CustomerAddressCounty,
    /// `customer_address_state`: address key to its state (first
    /// appearance order).
    CustomerAddressState,
    /// `customer_demographics`: cdemo key to gender + education + marital
    /// status.
    CustomerDemographics,
    /// `household_demographics`: hdemo key to buy potential + dependants.
    HouseholdDemographics,
    /// `customer_county`: customer key to its current address's county.
    CustomerCounty,
    /// `customer_state`: customer key to its current address's state.
    CustomerState,
}

/// The maps one [`build`] makes.
type Built = Vec<(MapName, Arc<AttributeKeyMap>)>;

type Cache = Mutex<HashMap<(MapName, u64), Arc<OnceLock<Built>>>>;

/// The attribute key map `name` at `session`'s scale, built on first use.
/// Callers that ask at once wait for one build.
pub fn attribute_map(name: MapName, session: &Session) -> Arc<AttributeKeyMap> {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = (group(name), session.get_scaling().get_scale().to_bits());
    let cell = cache
        .lock()
        .expect("attribute map cache")
        .entry(key)
        .or_default()
        .clone();
    let built = cell.get_or_init(|| {
        build(name, session)
            .into_iter()
            .map(|(n, m)| (n, Arc::new(m)))
            .collect()
    });
    built
        .iter()
        .find(|(n, _)| *n == name)
        .expect("built with its group")
        .1
        .clone()
}

/// The first of the maps [`build`] makes together with `name`.
fn group(name: MapName) -> MapName {
    match name {
        MapName::ItemCategory | MapName::ItemColor => MapName::ItemCategory,
        MapName::CustomerAddressCounty | MapName::CustomerAddressState => {
            MapName::CustomerAddressCounty
        }
        MapName::CustomerCounty | MapName::CustomerState => MapName::CustomerCounty,
        n => n,
    }
}

fn rows(session: &Session, table: Table) -> u64 {
    session.get_scaling().get_row_count(table)
}

fn build(name: MapName, session: &Session) -> Vec<(MapName, AttributeKeyMap)> {
    use MapName::*;
    match name {
        ItemCategory | ItemColor => {
            let n = rows(session, Table::Item);
            let mut g = ItemRowGenerator::new(session.clone(), n);
            let (mut category, mut color) = (Vec::new(), Vec::new());
            while let Some(row) = g.next() {
                category.push(row.get_i_category_id() as i32);
                color.push(g.last_color_id());
            }
            vec![
                (
                    ItemCategory,
                    AttributeKeyMap::from_attributes(item_categories(), category),
                ),
                (
                    ItemColor,
                    AttributeKeyMap::from_attributes(item_colors(), color),
                ),
            ]
        }
        CustomerAddressCounty | CustomerAddressState => {
            let n = rows(session, Table::CustomerAddress);
            let mut g = CustomerAddressRowGenerator::new(session.clone(), n);
            let (mut county, mut state) = (Vec::new(), Vec::new());
            let mut states = FirstAppearance::default();
            while let Some(row) = g.next() {
                county.push(g.last_county_id().expect("customer_address is not small"));
                state.push(states.id(row.get_ca_address().get_state()));
            }
            vec![
                (
                    CustomerAddressCounty,
                    AttributeKeyMap::from_attributes(FipsCountyDistribution::size(), county),
                ),
                (
                    CustomerAddressState,
                    AttributeKeyMap::from_attributes(states.count(), state),
                ),
            ]
        }
        CustomerDemographics => {
            // Gender, marital status and education are the key's lowest
            // mixed-radix digits, so the attributes repeat with that period:
            // generate one period and repeat it.
            let n = rows(session, Table::CustomerDemographics) as usize;
            let period = (DemographicsDistributions::get_gender_size()
                * DemographicsDistributions::get_marital_status_size()
                * DemographicsDistributions::get_education_size())
            .min(n);
            let mut ids = FirstAppearance::default();
            let mut value = String::new();
            let one: Vec<i32> =
                CustomerDemographicsRowGenerator::new(session.clone(), period as u64)
                    .map(|r| {
                        value.clear();
                        value.push_str(r.get_cd_gender());
                        value.push_str(r.get_cd_education_status());
                        value.push_str(r.get_cd_marital_status());
                        ids.id(&value)
                    })
                    .collect();
            let attrs: Vec<i32> = (0..n).map(|k| one[k % period]).collect();
            vec![(name, AttributeKeyMap::from_attributes(ids.count(), attrs))]
        }
        HouseholdDemographics => {
            let n = rows(session, Table::HouseholdDemographics);
            let mut ids = FirstAppearance::default();
            let attrs: Vec<i32> = HouseholdDemographicsRowGenerator::new(session.clone(), n)
                .map(|r| {
                    ids.id(&format!(
                        "{}{}",
                        r.get_hd_buy_potential(),
                        r.get_hd_dep_count()
                    ))
                })
                .collect();
            vec![(name, AttributeKeyMap::from_attributes(ids.count(), attrs))]
        }
        CustomerCounty | CustomerState => {
            let county = attribute_map(CustomerAddressCounty, session);
            let state = attribute_map(CustomerAddressState, session);
            let addrs = customer_current_addresses(session);
            vec![
                (
                    CustomerCounty,
                    AttributeKeyMap::from_attributes(
                        county.attribute_count() as usize,
                        addrs.iter().map(|&a| county.attribute_of(a)).collect(),
                    ),
                ),
                (
                    CustomerState,
                    AttributeKeyMap::from_attributes(
                        state.attribute_count() as usize,
                        addrs.iter().map(|&a| state.attribute_of(a)).collect(),
                    ),
                ),
            ]
        }
    }
}

fn item_categories() -> usize {
    category_count()
}

fn item_colors() -> usize {
    colors_distribution().get_size()
}

/// Every customer's `c_current_addr_sk`: one exponential draw a row from
/// `C_CURRENT_ADDR_SK`, which never overruns its budget of one.
pub fn customer_current_addresses(session: &Session) -> Vec<i64> {
    let c = CustomerGeneratorColumn::CCurrentAddrSk;
    let mut s =
        RandomNumberStream::new_with_column(c.get_global_column_number(), c.get_seeds_per_row());
    let addresses = rows(session, Table::CustomerAddress) as i64;
    (0..rows(session, Table::Customer))
        .map(|_| super::exp_key(1, addresses, &mut s))
        .collect()
}

impl AttributeKeyMap {
    /// The map as dsdgen's `serializeAttributeKeyRecord` writes it.
    pub fn to_meta(&self) -> String {
        let mut out = format!("{} {}\n", self.counts.len(), self.attribute.len());
        for (a, &c) in self.counts.iter().enumerate() {
            out.push_str(&c.to_string());
            for k in &self.keys[self.first[a]..self.first[a] + c as usize] {
                out.push(' ');
                out.push_str(&k.to_string());
            }
            out.push('\n');
        }
        out
    }
}

/// `genrandSingleTableOneSidedCorrelatedJointDistribution`: a key of `map`
/// whose attribute lies exponentially near `drive_id / drive_count` of the
/// way through `map`'s (permuted) attributes, within `radius`. Two draws.
pub fn one_sided_correlated_key(
    drive_count: i32,
    drive_id: i32,
    map: &AttributeKeyMap,
    perm: &[i64],
    radius: f64,
    stream: &mut RandomNumberStream,
) -> i64 {
    let count = map.attribute_count();
    let ratio = 1.0 * (drive_id + 1) as f64 / drive_count as f64;
    let lo = ((count as f64 * ratio) as i32).max(1);
    let hi = ((count as f64 * (ratio + radius)) as i32).max(1);
    let t = super::exp_int(lo, hi, stream) % count + 1;
    let mut attribute = super::map_key_perm(t as i64, perm) as i32;
    // Roll over attributes with no keys (dsdgen's wrap can reach 0, which
    // it then reads out of bounds; the maps it is used with have no empty
    // attributes).
    while map.key_count(attribute) == 0 {
        attribute = (attribute + 1) % count;
    }
    map.key_with_attribute(attribute, stream)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compares the maps with the `.meta` files of a dsdgen SF 1 run in
    /// `$DSB_REF_DIR`.
    #[test]
    #[ignore = "needs DSB reference output in $DSB_REF_DIR"]
    fn maps_match_dsdgen_meta_files() {
        let dir = std::env::var("DSB_REF_DIR").expect("DSB_REF_DIR");
        let session = Session::default();
        for (name, file) in [
            (MapName::ItemCategory, "item_category"),
            (MapName::ItemColor, "item_color"),
            (MapName::CustomerAddressCounty, "customer_address_county"),
            (MapName::CustomerAddressState, "customer_address_state"),
            (MapName::CustomerDemographics, "customer_demographics"),
            (MapName::HouseholdDemographics, "household_demographics"),
            (MapName::CustomerCounty, "customer_county"),
            (MapName::CustomerState, "customer_state"),
        ] {
            let want = std::fs::read_to_string(format!("{dir}/{file}.meta")).expect("meta file");
            assert!(
                attribute_map(name, &session).to_meta() == want,
                "{file}.meta differs"
            );
        }
    }
}
