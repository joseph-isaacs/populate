// Ported from Microsoft DSB's dsdgen (MIT; Copyright (c) Microsoft
// Corporation), address.c. See populate-dsb/NOTICE.md.

//! DSB's `mk_address`: exponentially skewed, permuted street numbers,
//! street names and types, cities and counties.

use super::{exp_int, exp_pick_index, make_perm, map_perm, uniform_int};
use crate::config::Scaling;
use crate::distribution::address_distributions::{
    cities_distribution, street_names_distribution, street_types_distribution,
};
use crate::distribution::{pick_random_street_name, FipsCountyDistribution, StreetNamesWeights};
use crate::generator::{CallCenterGeneratorColumn, GeneratorColumn};
use crate::random::RandomNumberStream;
use crate::table::Table;
use crate::types::Address;
use std::sync::OnceLock;

/// The permutations `mk_address` makes on its first call.
///
/// dsdgen keeps them in statics, drawn from the stream of whichever column
/// calls `mk_address` first. In a full run that is `call_center`'s
/// `CC_ADDRESS`, in its first row, so every table's addresses use the
/// permutations drawn from that stream's start.
pub struct AddressPerms {
    street_number: Vec<i32>,
    cities: Vec<i32>,
    counties: Vec<i32>,
    street_type: Vec<i32>,
    street_names: Vec<i32>,
    /// Draws taken from the `CC_ADDRESS` stream.
    pub draws: u64,
}

impl AddressPerms {
    fn new(stream: &mut RandomNumberStream) -> Self {
        let n_cities = cities_distribution().get_size() as i32;
        let n_counties = FipsCountyDistribution::size() as i32;
        let n_types = street_types_distribution().get_size() as i32;
        let n_names = street_names_distribution().get_size() as i32;
        AddressPerms {
            street_number: make_perm(1000, stream),
            cities: make_perm(n_cities, stream),
            counties: make_perm(n_counties, stream),
            street_type: make_perm(n_types, stream),
            street_names: make_perm(n_names, stream),
            draws: (1000 + n_cities + n_counties + n_types + n_names) as u64,
        }
    }

    /// The process-wide permutations (drawn from the start of
    /// `CC_ADDRESS`'s stream).
    pub fn get() -> &'static AddressPerms {
        static PERMS: OnceLock<AddressPerms> = OnceLock::new();
        PERMS.get_or_init(|| {
            let c = CallCenterGeneratorColumn::CcAddress;
            let mut stream = RandomNumberStream::new_with_column(
                c.get_global_column_number(),
                c.get_seeds_per_row(),
            );
            AddressPerms::new(&mut stream)
        })
    }
}

/// `mk_address` for a column of `table`. Returns the address and the
/// 1-based county id (`g_address_county_id`, set only for tables that are
/// not small: `None` otherwise).
pub fn make_address(
    table: Table,
    stream: &mut RandomNumberStream,
    scaling: &Scaling,
) -> (Address, Option<i32>) {
    let perms = AddressPerms::get();
    let names = street_names_distribution();
    let types = street_types_distribution();
    let cities = cities_distribution();
    let n_cities = cities.get_size() as i32;
    let n_counties = FipsCountyDistribution::size() as i32;

    let street_number = map_perm(exp_int(1, 1000, stream), &perms.street_number);
    let street_name1 = names
        .get_value_at_index(
            0,
            exp_pick_index(names.get_size() as i32, Some(&perms.street_names), stream),
        )
        .to_string();
    let street_name2 = pick_random_street_name(StreetNamesWeights::HalfEmpty, stream).to_string();
    let street_type = types
        .get_value_at_index(
            0,
            exp_pick_index(types.get_size() as i32, Some(&perms.street_type), stream),
        )
        .to_string();

    let i = uniform_int(1, 100, stream);
    let suite_number = if i & 1 == 1 {
        format!("Suite {}", (i >> 1) * 10)
    } else {
        format!("Suite {}", (((i >> 1) % 25) as u8 + b'A') as char)
    };

    let small = table.is_small();
    let rows = scaling.get_row_count(table) as i32;
    let city = if small {
        let i = uniform_int(1, if n_cities > rows { rows } else { n_cities }, stream);
        cities.get_value_at_index(0, (i - 1) as usize)
    } else {
        // pick_distribution_permutation_with_count: the count is the
        // whole distribution here.
        cities.get_value_at_index(0, exp_pick_index(n_cities, Some(&perms.cities), stream))
    }
    .to_string();

    let (region, county_id) = if small {
        let r = uniform_int(1, if n_counties > rows { rows } else { n_counties }, stream);
        (r, None)
    } else {
        let r = exp_pick_index(n_counties, Some(&perms.counties), stream) as i32 + 1;
        (r, Some(r))
    };
    let idx = (region - 1) as usize;
    let county = FipsCountyDistribution::get_county_at_index(idx);
    let state = FipsCountyDistribution::get_state_abbreviation_at_index(idx);

    let mut zip = Address::compute_city_hash(&city);
    let zip_prefix = FipsCountyDistribution::get_zip_prefix_at_index(idx);
    if zip_prefix == 0 && zip < 9400 {
        zip += 600;
    }
    zip += zip_prefix * 10000;
    let gmt_offset = FipsCountyDistribution::get_gmt_offset_at_index(idx);

    (
        Address::new(
            suite_number,
            street_number,
            street_name1,
            street_name2,
            street_type,
            city,
            Some(county.to_string()),
            state.to_string(),
            "United States".to_string(),
            zip,
            gmt_offset,
        ),
        county_id,
    )
}
