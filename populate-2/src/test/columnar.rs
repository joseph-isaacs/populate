// ========================================================================
// ColumnarVecBackend tests — Arrow-like columnar export
//
// Demonstrates that list<(u32, u64)> decomposes into three flat vectors:
//   col1: Vec<u32>, col2: Vec<u64>, offsets: Vec<usize>
// Just like Arrow's ListArray<StructArray<[u32, u64]>>
// ========================================================================

use crate::dist::{constant, list, range};
use crate::traits::{ColumnarVecBackend, Distribution, DistributionExt, DistributionScalar};
use rand::rng;

// Helper: construct distributions with explicit ColumnarVecBackend

fn scalar_tuple() -> impl Distribution<ColumnarVecBackend, Value = (i32, i64)> {
    (constant(1i32), range(0i64, 10))
}

fn list_scalar() -> impl Distribution<ColumnarVecBackend, Value = Vec<i64>> {
    list(constant(3usize), range(0i64, 1))
}

fn list_of_struct() -> impl Distribution<ColumnarVecBackend, Value = Vec<(u32, u64)>> {
    list(constant(2usize), (range(0u32, 1), range(100u64, 10)))
}

fn list_of_struct_in_tuple() -> impl Distribution<ColumnarVecBackend, Value = (i32, Vec<(u32, u64)>)>
{
    (
        constant(42i32),
        list(constant(2usize), (range(10u32, 1), range(200u64, 100))),
    )
}

fn variable_length_list() -> impl Distribution<ColumnarVecBackend, Value = Vec<u32>> {
    list(range(0usize, 1), range(0u32, 1))
}

// ========================================================================
// Scalar columns — same as VecBackend
// ========================================================================

#[test]
fn test_columnar_scalar() {
    let mut dist = scalar_tuple();
    let (c1, c2) = dist.collect_flat(&mut rng(), 3);
    assert_eq!(c1, vec![1, 1, 1]);
    assert_eq!(c2, vec![0, 10, 20]);
}

// ========================================================================
// list<i64> — single flat buffer + offsets
// ========================================================================

#[test]
fn test_columnar_list_scalar() {
    let mut dist = list_scalar();
    let ((values,), offsets) = dist.collect_col(&mut rng(), 2).into_parts();

    // 2 lists of 3 elements each: [0,1,2], [0,1,2]
    assert_eq!(values, vec![0, 1, 2, 0, 1, 2]);
    assert_eq!(offsets, vec![0, 3, 6]);
}

// ========================================================================
// list<(u32, u64)> — Arrow-like: two value buffers + shared offsets
// ========================================================================

#[test]
fn test_columnar_list_of_struct() {
    let mut dist = list_of_struct();
    let ((col1, col2), offsets) = dist.collect_col(&mut rng(), 3).into_parts();

    // col1 (u32): all first-fields flattened across all lists
    assert_eq!(col1, vec![0, 1, 0, 1, 0, 1]);
    // col2 (u64): all second-fields flattened across all lists
    assert_eq!(col2, vec![100, 110, 100, 110, 100, 110]);
    // offsets: shared across both columns, list boundaries
    assert_eq!(offsets, vec![0, 2, 4, 6]);

    // Reconstruct list[i] from the flat columns:
    for i in 0..3 {
        let start = offsets[i];
        let end = offsets[i + 1];
        assert_eq!(&col1[start..end], &[0, 1]);
        assert_eq!(&col2[start..end], &[100, 110]);
    }
}

// ========================================================================
// list<(u32, u64)> inside a tuple — col-by-col at the top level
// ========================================================================

#[test]
fn test_columnar_struct_in_list_in_tuple() {
    let mut dist = list_of_struct_in_tuple();
    let (scalars, lists) = dist.collect_flat(&mut rng(), 3);
    let ((col1, col2), offsets) = lists.into_parts();

    assert_eq!(scalars, vec![42, 42, 42]);
    assert_eq!(col1, vec![10, 11, 10, 11, 10, 11]);
    assert_eq!(col2, vec![200, 300, 200, 300, 200, 300]);
    assert_eq!(offsets, vec![0, 2, 4, 6]);
}

// ========================================================================
// Variable-length lists — offsets correctly track different sizes
// ========================================================================

#[test]
fn test_columnar_variable_length_lists() {
    // Lists with lengths 0, 1, 2, 3
    let mut dist = variable_length_list();
    let ((values,), offsets) = dist.collect_col(&mut rng(), 4).into_parts();

    // Lists: [], [0], [0,1], [0,1,2]
    assert_eq!(values, vec![0, 0, 1, 0, 1, 2]);
    assert_eq!(offsets, vec![0, 0, 1, 3, 6]);
}
