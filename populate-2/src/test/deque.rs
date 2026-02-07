// ========================================================================
// Dual-backend tests: same generic helpers, two backends
// ========================================================================

#[cfg(test)]
mod test {
    use crate::test::dist::{make_constant, make_range, make_tuple};
    use crate::traits::{DequeBackend, DistributionExt};
    use rand::rng;
    use std::collections::VecDeque;

    #[test]
    fn test_constant_deque_backend() {
        let mut dist = make_constant::<DequeBackend>();
        let (col,) = dist.collect_flat_into((VecDeque::<i32>::new(),), &mut rng(), 5);
        assert_eq!(col, VecDeque::from([42, 42, 42, 42, 42]));
    }

    #[test]
    fn test_range_deque_backend() {
        let mut dist = make_range::<DequeBackend>();
        let (col,) = dist.collect_flat_into((VecDeque::<i64>::new(),), &mut rng(), 5);
        assert_eq!(col, VecDeque::from([0, 1, 2, 3, 4]));
    }

    #[test]
    fn test_tuple_deque_backend() {
        let mut dist = make_tuple::<DequeBackend>();
        let (c1, c2) = dist.collect_flat_into(
            (VecDeque::<i32>::new(), VecDeque::<i64>::new()),
            &mut rng(),
            4,
        );
        assert_eq!(c1, VecDeque::from([1, 1, 1, 1]));
        assert_eq!(c2, VecDeque::from([0, 10, 20, 30]));
    }
}
