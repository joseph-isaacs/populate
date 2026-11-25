use crate::sink::Sink;

/// A row type that can be stored column-wise.
///
/// [`Columns`](Self::Columns) holds any number of `Self` values: one buffer
/// for a scalar, one set of columns per field for a tuple. The table in the
/// [crate docs](crate) lists every implementation.
pub trait Columnar: Sized {
    type Columns: Sink<Self> + Default;
}

/// The columns that hold values of type `T`.
pub type Columns<T> = <T as Columnar>::Columns;

macro_rules! columnar_scalar {
    ($($t:ty),*) => {
        $(impl Columnar for $t {
            type Columns = Vec<$t>;
        })*
    };
}

columnar_scalar!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool, usize);
