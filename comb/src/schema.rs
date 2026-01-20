//! Schema types - compile-time type tags for the type system.
//!
//! These marker types encode the schema at compile time, enabling
//! type-safe generation while allowing conversion to runtime TypeTags.

use crate::value::TypeTag;

/// A schema type that can be used in generators and sinks.
pub trait Schema: 'static {
    /// The runtime value type for this schema.
    type Value;

    /// Get the runtime type tag for this schema.
    fn type_tag() -> TypeTag;
}

/// Schema for 64-bit signed integers.
#[derive(Debug, Clone, Copy)]
pub struct SInt64;

impl Schema for SInt64 {
    type Value = i64;

    fn type_tag() -> TypeTag {
        TypeTag::Int64
    }
}

/// Schema for 32-bit signed integers.
#[derive(Debug, Clone, Copy)]
pub struct SInt32;

impl Schema for SInt32 {
    type Value = i32;

    fn type_tag() -> TypeTag {
        TypeTag::Int32
    }
}

/// Schema for 64-bit floating point numbers.
#[derive(Debug, Clone, Copy)]
pub struct SFloat64;

impl Schema for SFloat64 {
    type Value = f64;

    fn type_tag() -> TypeTag {
        TypeTag::Float64
    }
}

/// Schema for 32-bit floating point numbers.
#[derive(Debug, Clone, Copy)]
pub struct SFloat32;

impl Schema for SFloat32 {
    type Value = f32;

    fn type_tag() -> TypeTag {
        TypeTag::Float32
    }
}

/// Schema for booleans.
#[derive(Debug, Clone, Copy)]
pub struct SBool;

impl Schema for SBool {
    type Value = bool;

    fn type_tag() -> TypeTag {
        TypeTag::Bool
    }
}

/// Schema for owned strings.
#[derive(Debug, Clone, Copy)]
pub struct SString;

impl Schema for SString {
    type Value = String;

    fn type_tag() -> TypeTag {
        TypeTag::String
    }
}

/// Schema for lists with element type E.
#[derive(Debug, Clone, Copy)]
pub struct SList<E: Schema>(std::marker::PhantomData<E>);

impl<E: Schema> Schema for SList<E> {
    type Value = Vec<E::Value>;

    fn type_tag() -> TypeTag {
        TypeTag::List(Box::new(E::type_tag()))
    }
}

/// Schema for optional values.
#[derive(Debug, Clone, Copy)]
pub struct SOption<S: Schema>(std::marker::PhantomData<S>);

impl<S: Schema> Schema for SOption<S> {
    type Value = Option<S::Value>;

    fn type_tag() -> TypeTag {
        TypeTag::Option(Box::new(S::type_tag()))
    }
}

/// Schema for 2-tuples (structs with 2 fields).
#[derive(Debug, Clone, Copy)]
pub struct STuple2<A: Schema, B: Schema>(std::marker::PhantomData<(A, B)>);

impl<A: Schema, B: Schema> Schema for STuple2<A, B> {
    type Value = (A::Value, B::Value);

    fn type_tag() -> TypeTag {
        TypeTag::Struct(vec![A::type_tag(), B::type_tag()])
    }
}

/// Schema for 3-tuples (structs with 3 fields).
#[derive(Debug, Clone, Copy)]
pub struct STuple3<A: Schema, B: Schema, C: Schema>(std::marker::PhantomData<(A, B, C)>);

impl<A: Schema, B: Schema, C: Schema> Schema for STuple3<A, B, C> {
    type Value = (A::Value, B::Value, C::Value);

    fn type_tag() -> TypeTag {
        TypeTag::Struct(vec![A::type_tag(), B::type_tag(), C::type_tag()])
    }
}
