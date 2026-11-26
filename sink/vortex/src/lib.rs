//! Collect Populate distributions as native Vortex arrays.
use populate_core::{Columns, Distribution, ListCol, Nullable, StrCol};
use rand::Rng;
use vortex_array::arrays::{
    BoolArray, FixedSizeListArray, ListArray, PrimitiveArray, StructArray, VarBinArray,
};
use vortex_array::dtype::{DType, FieldNames};
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray};
use vortex_buffer::{BitBuffer, Buffer};
use vortex_error::{VortexResult, vortex_err};

/// A set of Populate columns that can be handed to a native Vortex constructor.
pub trait IntoVortexArray {
    fn into_vortex_array(
        self,
        ctx: &mut ExecutionCtx,
        validity: Validity,
        names: &[String],
    ) -> VortexResult<ArrayRef>;
}

/// Collect the next batch as a type-erased native array.
pub trait DistributionVortex: Distribution {
    fn collect_vortex(
        &mut self,
        rng: &mut impl Rng,
        n: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef>
    where
        Columns<Self::Item>: IntoVortexArray,
    {
        let mut names = Vec::new();
        self.column_names(&mut names);
        self.collect(rng, n)
            .into_vortex_array(ctx, Validity::NonNullable, &names)
    }
}
impl<D: Distribution> DistributionVortex for D {}

macro_rules! primitive {
    ($($t:ty),+) => {$(
        impl IntoVortexArray for Vec<$t> {
            fn into_vortex_array(self, _: &mut ExecutionCtx, validity: Validity, _: &[String]) -> VortexResult<ArrayRef> {
                Ok(PrimitiveArray::new(Buffer::from(self), validity).into_array())
            }
        }
    )+};
}
primitive!(i8, u8, i16, u16, i32, u32, i64, u64, f32, f64);

impl IntoVortexArray for Vec<bool> {
    fn into_vortex_array(
        self,
        _: &mut ExecutionCtx,
        validity: Validity,
        _: &[String],
    ) -> VortexResult<ArrayRef> {
        Ok(BoolArray::new(BitBuffer::from(self), validity).into_array())
    }
}

impl IntoVortexArray for StrCol {
    fn into_vortex_array(
        self,
        ctx: &mut ExecutionCtx,
        validity: Validity,
        _: &[String],
    ) -> VortexResult<ArrayRef> {
        let (offsets, bytes) = self.into_parts();
        let dtype = DType::Utf8(validity.nullability());
        Ok(VarBinArray::try_new(
            Buffer::from(offsets).into_array(),
            Buffer::from(bytes),
            dtype,
            validity,
            ctx,
        )?
        .into_array())
    }
}

impl<C: IntoVortexArray> IntoVortexArray for ListCol<C> {
    fn into_vortex_array(
        self,
        ctx: &mut ExecutionCtx,
        validity: Validity,
        _: &[String],
    ) -> VortexResult<ArrayRef> {
        let (offsets, values) = self.into_parts();
        let elements = values.into_vortex_array(ctx, Validity::NonNullable, &[])?;
        Ok(
            ListArray::try_new(elements, Buffer::from(offsets).into_array(), validity, ctx)?
                .into_array(),
        )
    }
}

impl<T, const N: usize> IntoVortexArray for Vec<[T; N]>
where
    Vec<T>: IntoVortexArray,
{
    fn into_vortex_array(
        self,
        ctx: &mut ExecutionCtx,
        validity: Validity,
        _: &[String],
    ) -> VortexResult<ArrayRef> {
        let len = self.len();
        let width = u32::try_from(N).map_err(|_| vortex_err!("fixed list width exceeds u32"))?;
        let elements = self
            .into_flattened()
            .into_vortex_array(ctx, Validity::NonNullable, &[])?;
        Ok(FixedSizeListArray::try_new(elements, width, validity, len)?.into_array())
    }
}

impl<C: IntoVortexArray> IntoVortexArray for Nullable<C> {
    fn into_vortex_array(
        self,
        ctx: &mut ExecutionCtx,
        validity: Validity,
        names: &[String],
    ) -> VortexResult<ArrayRef> {
        let (mut words, len) = self.valid.into_parts();
        if cfg!(target_endian = "big") {
            for word in &mut words {
                *word = word.to_le();
            }
        }
        let bits = BitBuffer::new(Buffer::from(words).into_byte_buffer(), len);
        self.values
            .into_vortex_array(ctx, validity.and(Validity::from(bits))?, names)
    }
}

fn struct_array(
    fields: Vec<ArrayRef>,
    names: &[String],
    validity: Validity,
) -> VortexResult<ArrayRef> {
    let names: FieldNames = if names.is_empty() {
        (0..fields.len()).map(|i| format!("c{i}")).collect()
    } else if names.len() == fields.len() {
        names.iter().cloned().collect()
    } else {
        return Err(vortex_err!(
            "expected {} struct field names, got {}",
            fields.len(),
            names.len()
        ));
    };
    let len = fields[0].len();
    Ok(StructArray::try_new(names, fields, len, validity)?.into_array())
}

macro_rules! tuples {
    ($first:ident $idx:tt $(, $t:ident $i:tt)*) => {
        impl<$first: IntoVortexArray, $($t: IntoVortexArray),*> IntoVortexArray for ($first, $($t,)*) {
            fn into_vortex_array(self, ctx: &mut ExecutionCtx, validity: Validity, names: &[String]) -> VortexResult<ArrayRef> {
                let fields = vec![self.$idx.into_vortex_array(ctx, Validity::NonNullable, &[])?,
                    $(self.$i.into_vortex_array(ctx, Validity::NonNullable, &[])?,)*];
                struct_array(fields, names, validity)
            }
        }
    };
}
tuples!(A 0, B 1);
tuples!(A 0, B 1, C 2);
tuples!(A 0, B 1, C 2, D 3);
tuples!(A 0, B 1, C 2, D 3, E 4);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10);
tuples!(A 0, B 1, C 2, D 3, E 4, F 5, G 6, H 7, I 8, J 9, K 10, L 11);

#[cfg(test)]
mod tests {
    use super::*;
    use populate_builtins::{constant, fixed_size_list, indexed, list, named, nullable, range};
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::bool::BoolArrayExt;
    use vortex_array::arrays::struct_::StructArrayExt;
    use vortex_array::arrays::varbin::VarBinArrayExt;
    use vortex_array::arrays::{Bool, FixedSizeList, List, Primitive, Struct, VarBin};
    use vortex_array::session::ArraySession;
    use vortex_session::VortexSession;

    #[test]
    fn native_columns_keep_names_values_and_nulls() -> VortexResult<()> {
        let mut ctx = VortexSession::empty()
            .with::<ArraySession>()
            .create_execution_ctx();
        let mut rng = rand::rng();
        let mut rows = (
            named("id", nullable(range(10i32, 1), indexed(|i| i % 2 == 0))),
            named("text", constant("abc")),
            named("items", list(constant(2i32), range(0u32, 1))),
            named("vector", fixed_size_list::<3, _>(constant(1.5f32))),
        );
        let empty = rows.collect_vortex(&mut rng, 0, &mut ctx)?;
        let arr = rows.collect_vortex(&mut rng, 67, &mut ctx)?;
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.dtype(), arr.dtype());
        let fields = arr.as_::<Struct>();
        let ids = fields.unmasked_field_by_name("id")?;
        assert_eq!(
            ids.as_::<Primitive>().to_buffer::<i32>().as_slice(),
            &(10..77).collect::<Vec<_>>()
        );
        let Validity::Array(bits) = ids.validity()? else {
            panic!("expected mixed validity")
        };
        assert!(
            bits.as_::<Bool>()
                .to_bit_buffer()
                .iter()
                .eq((0..67).map(|i| i % 2 == 0))
        );
        assert_eq!(
            fields
                .unmasked_field_by_name("text")?
                .as_::<VarBin>()
                .bytes_at(66, &mut ctx)
                .as_slice(),
            b"abc"
        );
        assert_eq!(
            fields.unmasked_field_by_name("items")?.as_::<List>().len(),
            67
        );
        assert_eq!(
            fields
                .unmasked_field_by_name("vector")?
                .as_::<FixedSizeList>()
                .len(),
            67
        );
        let next = rows.collect_vortex(&mut rng, 1, &mut ctx)?;
        let fields = next.as_::<Struct>();
        let id = fields.unmasked_field_by_name("id")?;
        assert!(matches!(id.validity()?, Validity::AllInvalid));
        Ok(())
    }
}
