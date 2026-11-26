// Portions adapted from LDBC SNB datagen Spark (a2210c5), Apache-2.0.
// Modified for Rust column buffers and chunked generation.
// See ../NOTICE.md for upstream copyright and ../LICENSE-Apache-2.0.txt.

//! `java.util.Random`, bit for bit, and the datagen's `RandomGeneratorFarm`.

const MULT: u64 = 0x5_DEEC_E66D;
const ADD: u64 = 0xB;
const MASK: u64 = (1 << 48) - 1;

/// `java.util.Random`: a 48-bit LCG.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JavaRandom {
    seed: u64,
}

impl JavaRandom {
    /// `new Random(seed)`.
    #[inline]
    pub fn new(seed: i64) -> Self {
        JavaRandom {
            seed: (seed as u64 ^ MULT) & MASK,
        }
    }

    /// `setSeed(seed)`.
    #[inline]
    pub fn set_seed(&mut self, seed: i64) {
        self.seed = (seed as u64 ^ MULT) & MASK;
    }

    #[inline(always)]
    fn next(&mut self, bits: u32) -> i32 {
        self.seed = (self.seed.wrapping_mul(MULT).wrapping_add(ADD)) & MASK;
        (self.seed >> (48 - bits)) as i32
    }

    /// `nextInt()`.
    #[inline]
    pub fn next_int(&mut self) -> i32 {
        self.next(32)
    }

    /// `nextInt(bound)`. Panics if `bound <= 0`, as Java throws.
    #[inline]
    pub fn next_int_bound(&mut self, bound: i32) -> i32 {
        assert!(bound > 0, "bound must be positive");
        let mut r = self.next(31);
        let m = bound - 1;
        if bound & m == 0 {
            ((bound as i64 * r as i64) >> 31) as i32
        } else {
            let mut u = r;
            loop {
                r = u % bound;
                if u.wrapping_sub(r).wrapping_add(m) >= 0 {
                    break;
                }
                u = self.next(31);
            }
            r
        }
    }

    /// `nextLong()`.
    #[inline]
    pub fn next_long(&mut self) -> i64 {
        let hi = self.next(32) as i64;
        let lo = self.next(32) as i64;
        (hi << 32).wrapping_add(lo)
    }

    /// `nextDouble()`.
    #[inline]
    pub fn next_double(&mut self) -> f64 {
        let hi = self.next(26) as i64;
        let lo = self.next(27) as i64;
        ((hi << 27) + lo) as f64 * (1.0 / (1u64 << 53) as f64)
    }

    /// `nextFloat()`.
    #[inline]
    pub fn next_float(&mut self) -> f32 {
        self.next(24) as f32 / (1u32 << 24) as f32
    }
}

/// The datagen's random aspects, one `Random` each, in enum order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
#[allow(dead_code, clippy::upper_case_acronyms)]
pub enum Aspect {
    Date,
    BirthDay,
    KnowsRequest,
    Initiator,
    Uniform,
    NumInterest,
    NumTag,
    NumKnows,
    NumComment,
    NumPhotoAlbum,
    NumPhoto,
    NumForum,
    NumUsersPerForum,
    NumPopular,
    NumLike,
    NumPost,
    Knows,
    KnowsLevel,
    Gender,
    Random,
    Membership,
    MembershipIndex,
    Forum,
    ForumModerator,
    ForumInterest,
    ExtraInfo,
    ExactLongLat,
    Status,
    HaveStatus,
    StatusSingle,
    UserAgent,
    UserAgentSent,
    FileSelect,
    Ip,
    DiffIpForTraveler,
    DiffIp,
    Browser,
    DiffBrowser,
    City,
    Country,
    Tag,
    University,
    UncorrelatedUniversity,
    UncorrelatedUniversityLocation,
    TopUniversity,
    Popular,
    Email,
    TopEmail,
    Company,
    UncorrelatedCompany,
    UncorrelatedCompanyLocation,
    Language,
    Album,
    AlbumMembership,
    Name,
    Surname,
    TagOtherCountry,
    SetOfTag,
    TextSize,
    ReducedText,
    LargeText,
    MembershipPostCreator,
    ReplyTo,
    Topic,
    DeletionPerson,
    DeletionKnows,
    DeletionForum,
    DeletionMemb,
    DeletionPost,
    DeletionComm,
    DeletionLikes,
    /// Java's `NUM_ASPECT`, the last constant (a generator of its own).
    Count,
}

/// Number of generators in a farm (`Aspect.values().length`, `NUM_ASPECT`
/// included).
pub const NUM_ASPECTS: usize = Aspect::Count as usize + 1;

/// `RandomGeneratorFarm`: one `Random` per [`Aspect`], all reseeded from one
/// seed.
#[derive(Debug, Clone)]
pub struct Farm {
    r: [JavaRandom; NUM_ASPECTS],
}

impl Default for Farm {
    fn default() -> Self {
        Farm {
            r: [JavaRandom::new(0); NUM_ASPECTS],
        }
    }
}

impl Farm {
    /// A farm reset with `seed`.
    pub fn new(seed: i64) -> Self {
        let mut f = Farm::default();
        f.reset(seed);
        f
    }

    /// `resetRandomGenerators(seed)`.
    pub fn reset(&mut self, seed: i64) {
        let mut s = JavaRandom::new(53_223_436i64.wrapping_add(1_234_567i64.wrapping_mul(seed)));
        for r in &mut self.r {
            r.set_seed(s.next_long());
        }
    }

    /// The generator of one aspect.
    #[inline(always)]
    pub fn get(&mut self, a: Aspect) -> &mut JavaRandom {
        &mut self.r[a as usize]
    }

    /// Two different aspects' generators at once.
    pub fn get_two(&mut self, a: Aspect, b: Aspect) -> [&mut JavaRandom; 2] {
        self.r
            .get_disjoint_mut([a as usize, b as usize])
            .expect("different aspects")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_java() {
        // values from OpenJDK: new Random(42)
        let mut r = JavaRandom::new(42);
        assert_eq!(r.next_int(), -1170105035);
        assert_eq!(r.next_int_bound(100), 63);
        let mut r = JavaRandom::new(42);
        assert_eq!(r.next_long(), -5025562857975149833);
        let mut r = JavaRandom::new(42);
        assert_eq!(r.next_double(), 0.7275636800328681);
        let mut r = JavaRandom::new(42);
        assert_eq!(r.next_float(), 0.7275637);
    }
}
