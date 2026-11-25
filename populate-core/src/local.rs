use std::mem::ManuallyDrop;
use std::ptr;

/// Run `f` on `*slot` moved into a local variable, then move it back.
///
/// For loops that sample a distribution once per value. Through a `&mut`
/// the compiler has to assume that every value written to a column might
/// overwrite the distribution's state, so it stores and reloads that state
/// on every turn of the loop. A local whose address goes nowhere else has no
/// such problem: its fields stay in registers and the loop can vectorise.
/// The default [`collect_into`](crate::Distribution::collect_into) runs its
/// loop in here.
#[inline(always)]
pub fn in_local<D, R>(slot: &mut D, f: impl FnOnce(&mut D) -> R) -> R {
    /// Moves `local` back into `slot` when dropped, which also covers `f`
    /// panicking.
    struct Restore<'a, D> {
        slot: &'a mut D,
        local: ManuallyDrop<D>,
    }

    impl<D> Drop for Restore<'_, D> {
        #[inline(always)]
        fn drop(&mut self) {
            // SAFETY: `local` is taken exactly once, here. `*slot` still
            // holds the stale bytes `local` was read from; they are
            // overwritten without being dropped, so the value is never
            // dropped twice.
            unsafe { ptr::write(self.slot, ManuallyDrop::take(&mut self.local)) }
        }
    }

    // SAFETY: `slot` is valid for reads. Until `Restore` is dropped nothing
    // reads `*slot` (it is borrowed mutably by `Restore`, which never
    // touches it before the write-back), so the value exists only in
    // `local` as far as any code can observe.
    let local = ManuallyDrop::new(unsafe { ptr::read(slot) });
    let mut restore = Restore { slot, local };
    f(&mut restore.local)
}

#[cfg(test)]
mod tests {
    use super::in_local;
    use std::panic::{AssertUnwindSafe, catch_unwind};
    use std::rc::Rc;

    #[test]
    fn changes_are_moved_back() {
        let mut v = vec![1, 2];
        let len = in_local(&mut v, |v| {
            v.push(3);
            v.len()
        });
        assert_eq!((len, v), (3, vec![1, 2, 3]));
    }

    #[test]
    fn moved_back_once_after_a_panic() {
        let rc = Rc::new(());
        let mut held = vec![Rc::clone(&rc)];
        let r = catch_unwind(AssertUnwindSafe(|| {
            in_local(&mut held, |h| {
                h.push(Rc::clone(&rc));
                panic!("boom");
            })
        }));
        assert!(r.is_err());
        // the push before the panic is there, and nothing was dropped twice
        assert_eq!((held.len(), Rc::strong_count(&rc)), (2, 3));
        drop(held);
        assert_eq!(Rc::strong_count(&rc), 1);
    }
}
