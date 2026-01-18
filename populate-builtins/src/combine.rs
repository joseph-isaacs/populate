use std::ops::Deref;

/// Holds the precomputed cartesian product of string slices.
/// Use with `pick_one` to select random elements.
///
/// # Example
/// ```ignore
/// let types = combine3(&SYLLABLE3, &SYLLABLE2, &SYLLABLE1, " ");
/// seeded(TpchRng::new(seed), pick_one(range(0, types.len()), &types))
/// ```
pub struct Combine {
    values: Vec<String>,
}

impl Combine {
    /// Create from two string slices (A × B).
    pub fn new2(a: &[&str], b: &[&str], sep: &str) -> Self {
        let values: Vec<String> = a
            .iter()
            .flat_map(|x| b.iter().map(move |y| format!("{}{}{}", x, sep, y)))
            .collect();
        Self { values }
    }

    /// Create from three string slices (A × B × C).
    pub fn new3(a: &[&str], b: &[&str], c: &[&str], sep: &str) -> Self {
        let values: Vec<String> = a
            .iter()
            .flat_map(|x| {
                b.iter()
                    .flat_map(move |y| c.iter().map(move |z| format!("{}{}{}{}{}", x, sep, y, sep, z)))
            })
            .collect();
        Self { values }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl Deref for Combine {
    type Target = [String];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

/// Create the cartesian product of two string slices.
pub fn combine2(a: &[&str], b: &[&str], sep: &str) -> Combine {
    Combine::new2(a, b, sep)
}

/// Create the cartesian product of three string slices.
pub fn combine3(a: &[&str], b: &[&str], c: &[&str], sep: &str) -> Combine {
    Combine::new3(a, b, c, sep)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combine2() {
        let sizes = ["SM", "LG"];
        let types = ["BOX", "BAG"];
        let combined = combine2(&sizes, &types, " ");

        assert_eq!(combined.len(), 4);
        assert!(combined.iter().any(|s| s == "SM BOX"));
        assert!(combined.iter().any(|s| s == "SM BAG"));
        assert!(combined.iter().any(|s| s == "LG BOX"));
        assert!(combined.iter().any(|s| s == "LG BAG"));
    }

    #[test]
    fn test_combine3() {
        let a = ["A", "B"];
        let b = ["1", "2"];
        let c = ["x", "y"];
        let combined = combine3(&a, &b, &c, "-");

        assert_eq!(combined.len(), 8);
        assert!(combined.iter().any(|s| s == "A-1-x"));
        assert!(combined.iter().any(|s| s == "B-2-y"));
    }

    #[test]
    fn test_deref_to_slice() {
        let combined = combine2(&["A"], &["B"], "-");
        let slice: &[String] = &combined;
        assert_eq!(slice.len(), 1);
        assert_eq!(slice[0], "A-B");
    }
}
