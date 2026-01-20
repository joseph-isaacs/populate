//! Runtime type tags and erased values for Tier 2 (dynamic) operations.

use crate::Schema;

/// Runtime type tag - mirrors Schema types at runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeTag {
    Int32,
    Int64,
    Float32,
    Float64,
    Bool,
    String,
    List(Box<TypeTag>),
    Option(Box<TypeTag>),
    Struct(Vec<TypeTag>),
}

impl TypeTag {
    /// Check if this type tag matches another.
    pub fn matches(&self, other: &TypeTag) -> bool {
        self == other
    }

    /// Human-readable type name.
    pub fn name(&self) -> String {
        match self {
            TypeTag::Int32 => "i32".to_string(),
            TypeTag::Int64 => "i64".to_string(),
            TypeTag::Float32 => "f32".to_string(),
            TypeTag::Float64 => "f64".to_string(),
            TypeTag::Bool => "bool".to_string(),
            TypeTag::String => "String".to_string(),
            TypeTag::List(e) => format!("List<{}>", e.name()),
            TypeTag::Option(s) => format!("Option<{}>", s.name()),
            TypeTag::Struct(fields) => {
                let field_names: Vec<_> = fields.iter().map(|f| f.name()).collect();
                format!("({})", field_names.join(", "))
            }
        }
    }
}

/// Type-erased value carrier for Tier 2 operations.
#[derive(Debug, Clone)]
pub enum ErasedValue {
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    String(String),
    List(Vec<ErasedValue>),
    Option(Option<Box<ErasedValue>>),
    Struct(Vec<ErasedValue>),
}

impl ErasedValue {
    /// Get the type tag for this value.
    pub fn type_tag(&self) -> TypeTag {
        match self {
            ErasedValue::Int32(_) => TypeTag::Int32,
            ErasedValue::Int64(_) => TypeTag::Int64,
            ErasedValue::Float32(_) => TypeTag::Float32,
            ErasedValue::Float64(_) => TypeTag::Float64,
            ErasedValue::Bool(_) => TypeTag::Bool,
            ErasedValue::String(_) => TypeTag::String,
            ErasedValue::List(elems) => {
                let elem_tag = elems
                    .first()
                    .map(|e| e.type_tag())
                    .unwrap_or(TypeTag::Int64);
                TypeTag::List(Box::new(elem_tag))
            }
            ErasedValue::Option(inner) => {
                let inner_tag = inner
                    .as_ref()
                    .map(|v| v.type_tag())
                    .unwrap_or(TypeTag::Int64);
                TypeTag::Option(Box::new(inner_tag))
            }
            ErasedValue::Struct(fields) => {
                TypeTag::Struct(fields.iter().map(|f| f.type_tag()).collect())
            }
        }
    }

    /// Try to extract an i64 value.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ErasedValue::Int64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract an i32 value.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            ErasedValue::Int32(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract an f64 value.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ErasedValue::Float64(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract an f32 value.
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            ErasedValue::Float32(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract a bool value.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ErasedValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Try to extract a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ErasedValue::String(v) => Some(v),
            _ => None,
        }
    }

    /// Try to extract a list reference.
    pub fn as_list(&self) -> Option<&[ErasedValue]> {
        match self {
            ErasedValue::List(v) => Some(v),
            _ => None,
        }
    }
}

/// Trait for converting typed values to erased values.
pub trait IntoErased {
    fn into_erased(self) -> ErasedValue;
}

impl IntoErased for i32 {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Int32(self)
    }
}

impl IntoErased for i64 {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Int64(self)
    }
}

impl IntoErased for f32 {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Float32(self)
    }
}

impl IntoErased for f64 {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Float64(self)
    }
}

impl IntoErased for bool {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Bool(self)
    }
}

impl IntoErased for String {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::String(self)
    }
}

impl<T: IntoErased> IntoErased for Vec<T> {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::List(self.into_iter().map(|v| v.into_erased()).collect())
    }
}

impl<T: IntoErased> IntoErased for Option<T> {
    fn into_erased(self) -> ErasedValue {
        ErasedValue::Option(self.map(|v| Box::new(v.into_erased())))
    }
}

/// Trait for converting erased values back to typed values.
pub trait FromErased: Sized {
    fn from_erased(value: ErasedValue) -> Option<Self>;
}

impl FromErased for i32 {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        value.as_i32()
    }
}

impl FromErased for i64 {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        value.as_i64()
    }
}

impl FromErased for f32 {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        value.as_f32()
    }
}

impl FromErased for f64 {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        value.as_f64()
    }
}

impl FromErased for bool {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        value.as_bool()
    }
}

impl FromErased for String {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        match value {
            ErasedValue::String(s) => Some(s),
            _ => None,
        }
    }
}

impl<T: FromErased> FromErased for Vec<T> {
    fn from_erased(value: ErasedValue) -> Option<Self> {
        match value {
            ErasedValue::List(elems) => elems.into_iter().map(T::from_erased).collect(),
            _ => None,
        }
    }
}

/// Check that a schema type matches a type tag at runtime.
#[allow(dead_code)]
pub fn check_type<S: Schema>(tag: &TypeTag) -> bool {
    S::type_tag() == *tag
}
