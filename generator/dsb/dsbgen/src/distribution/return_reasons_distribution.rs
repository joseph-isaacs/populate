// Modified from tpcdsgen 4.0.0 for the populate DSB variant.
// Licensed under Apache-2.0; see the crate LICENSE and generator/dsb/NOTICE.md.

use crate::config::CompatMode;
use crate::distribution::FileBasedStringValuesDistribution;
use std::sync::OnceLock;

/// Distribution for return reasons (ReturnReasons)
pub struct ReturnReasonsDistribution;

impl ReturnReasonsDistribution {
    fn get_distribution() -> &'static FileBasedStringValuesDistribution {
        static DISTRIBUTION: OnceLock<FileBasedStringValuesDistribution> = OnceLock::new();
        DISTRIBUTION.get_or_init(|| {
            FileBasedStringValuesDistribution::build_string_values_distribution(
                "return_reasons_trino.dst",
                1,
                6,
            )
            .expect("Failed to load return_reasons_trino.dst")
        })
    }

    pub fn get_return_reason_at_index(index: usize, compat: CompatMode) -> &'static str {
        // DSB's tpcds.dst repeats "reason 31" (row 30), as the Java port's
        // file does; tpcgen-cli's C mode file has "reason 30".
        let _ = compat;
        Self::get_distribution().get_value_at_index(0, index)
    }

    pub fn get_size() -> usize {
        Self::get_distribution().get_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_return_reasons_distribution() {
        // Test that we can load the distribution
        let size = ReturnReasonsDistribution::get_size();
        assert!(
            size > 0,
            "Return reasons distribution should have at least one entry"
        );

        // Test that we can get values at valid indices
        for i in 0..size.min(5) {
            let value = ReturnReasonsDistribution::get_return_reason_at_index(i, CompatMode::Trino);
            assert!(!value.is_empty(), "Value at index {i} should not be empty");
        }
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_return_reasons_out_of_bounds() {
        let size = ReturnReasonsDistribution::get_size();
        ReturnReasonsDistribution::get_return_reason_at_index(size + 100, CompatMode::Trino);
    }
}
