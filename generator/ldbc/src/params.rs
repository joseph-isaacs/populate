// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! Generator parameters: `params_default.ini`, the scale factors of
//! `scale_factors.xml`, and `DatagenParams.readConf` (bugs included: several
//! settings read the `maxEmails` key).

/// Scale factors of the datagen's `scale_factors.xml`: `(name, persons)`.
/// All start in 2010 and span 3 years with the Facebook degree distribution.
pub const SCALE_FACTORS: &[(&str, u64)] = &[
    ("0.003", 50),
    ("0.1", 1_700),
    ("0.3", 3_900),
    ("1", 10_620),
    ("3", 25_870),
    ("10", 70_800),
    ("30", 175_950),
    ("100", 487_700),
    ("300", 1_230_500),
    ("1000", 3_505_000),
    ("3000", 9_232_000),
    ("10000", 27_200_000),
    ("30000", 77_000_000),
];

/// Persons of a named scale factor (`"0.1"`, `"1"`, `"10"`, ...).
pub fn persons_for_scale_factor(sf: &str) -> Option<u64> {
    let want: f64 = sf.parse().ok()?;
    SCALE_FACTORS
        .iter()
        .find(|(n, _)| n.parse::<f64>().ok() == Some(want))
        .map(|&(_, p)| p)
}

/// Everything `DatagenParams` holds, as the generator sees it.
#[derive(Debug, Clone)]
pub struct Params {
    pub num_persons: u64,
    pub block_size: u32,
    pub start_year: i32,
    pub num_years: i32,
    pub delta: i64,

    pub base_prob_correlated: f64,
    pub limit_pro_correlated: f64,
    pub flashmob_tag_dist_exp: f64,
    pub flashmob_tag_max_level: f64,
    pub flashmob_tag_min_level: f64,
    pub flashmob_tags_per_month: i32,
    pub group_moderator_prob: f64,
    pub missing_ratio: f64,
    pub prob_another_browser: f64,
    pub prob_diff_ip_in_travel_season: f64,
    pub prob_diff_ip_not_travel_season: f64,
    pub prob_english: f64,
    pub prob_interest_flashmob_tag: f64,
    pub prob_random_per_level: f64,
    pub prob_second_lang: f64,
    pub prob_top_univ: f64,
    pub prob_uncorrelated_company: f64,
    pub prob_uncorrelated_organisation: f64,
    pub ratio_large_comment: f64,
    pub ratio_large_post: f64,
    pub ratio_reduce_text: f64,
    pub tag_country_corr_prob: f64,
    pub max_comment_size: i32,
    pub max_companies: i32,
    pub max_emails: i32,
    pub max_large_comment_size: i32,
    pub max_large_post_size: i32,
    pub max_num_comments: i32,
    pub max_num_flashmob_post_per_month: i32,
    pub max_num_friends: i64,
    pub max_num_group_created_per_person: i32,
    pub max_num_group_flashmob_post_per_month: i32,
    pub max_num_group_post_per_month: i32,
    pub max_group_size: i32,
    pub max_num_like: i32,
    pub max_num_photo_albums_per_month: i32,
    pub max_num_photo_per_albums: i32,
    pub max_num_popular_places: i32,
    pub max_num_post_per_month: i32,
    pub max_num_tag_per_flashmob_post: i32,
    pub max_num_tags_per_person: i32,
    pub max_text_size: i32,
    pub min_comment_size: i32,
    pub min_large_comment_size: i32,
    pub min_large_post_size: i32,
    pub min_num_tags_per_person: i32,
    pub min_text_size: i32,
    pub prob_forum_deleted: f64,
    pub prob_photo_deleted: f64,
    pub prob_comment_deleted: f64,
    pub prob_knows_deleted: f64,
    pub prob_memb_deleted: f64,
    pub prob_like_deleted: f64,
}

/// `DatagenParams.postMapping`: probability that a post with `n` comments
/// is deleted.
pub const POST_MAPPING: [f64; 21] = [
    0.18, 0.152, 0.142, 0.1, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05, 0.01, 0.01, 0.01, 0.01, 0.01, 0.01,
    0.01, 0.01, 0.01, 0.01, 0.01,
];

/// `DatagenParams.alpha`, the power law of person tags and dates.
pub const ALPHA: f64 = 0.4;

impl Params {
    /// `params_default.ini` with `num_persons` persons.
    pub fn new(num_persons: u64) -> Self {
        let max_emails = 5;
        Params {
            num_persons,
            block_size: 10_000,
            start_year: 2010,
            num_years: 3,
            delta: 10_000,
            base_prob_correlated: 0.95,
            limit_pro_correlated: 0.2,
            flashmob_tag_dist_exp: 0.4,
            flashmob_tag_max_level: 20.0,
            flashmob_tag_min_level: 1.0,
            flashmob_tags_per_month: 5,
            group_moderator_prob: 0.05,
            missing_ratio: 0.2,
            prob_another_browser: 0.01,
            prob_diff_ip_in_travel_season: 0.1,
            prob_diff_ip_not_travel_season: 0.02,
            // readConf reads these three from `maxEmails`
            prob_english: max_emails as f64,
            prob_second_lang: max_emails as f64,
            max_companies: max_emails,
            prob_interest_flashmob_tag: 0.8,
            prob_random_per_level: 0.005,
            prob_top_univ: 0.9,
            prob_uncorrelated_company: 0.05,
            prob_uncorrelated_organisation: 0.005,
            ratio_large_comment: 0.001,
            ratio_large_post: 0.001,
            ratio_reduce_text: 0.8,
            tag_country_corr_prob: 0.5,
            max_comment_size: 185,
            max_emails,
            max_large_comment_size: 2000,
            max_large_post_size: 2000,
            max_num_comments: 20,
            max_num_flashmob_post_per_month: 30,
            max_num_friends: 1000,
            max_num_group_created_per_person: 4,
            max_num_group_flashmob_post_per_month: 10,
            max_num_group_post_per_month: 10,
            max_group_size: 100,
            max_num_like: 1000,
            max_num_photo_albums_per_month: 1,
            max_num_photo_per_albums: 20,
            max_num_popular_places: 2,
            max_num_post_per_month: 30,
            max_num_tag_per_flashmob_post: 80,
            max_num_tags_per_person: 80,
            max_text_size: 250,
            min_comment_size: 75,
            min_large_comment_size: 700,
            min_large_post_size: 700,
            min_num_tags_per_person: 1,
            min_text_size: 85,
            prob_forum_deleted: 0.01,
            prob_photo_deleted: 0.054,
            prob_comment_deleted: 0.036,
            prob_knows_deleted: 0.05,
            prob_memb_deleted: 0.05,
            prob_like_deleted: 0.048,
        }
    }

    /// Number of person blocks.
    pub fn num_blocks(&self) -> u64 {
        self.num_persons.div_ceil(self.block_size as u64)
    }
}
