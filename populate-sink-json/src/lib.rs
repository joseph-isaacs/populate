use populate_builtins::List;
use populate_core::{Collector, Distribution, SampleIter, Sampler};
use serde_json::{Value, json};

/// Marker for collecting distributions into a JSON array of objects.
pub struct JsonRows;

/// Trait for types that can be converted to JSON values.
pub trait ToJsonValue {
    fn to_json_value(&self) -> Value;
}

impl ToJsonValue for i8 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for i16 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for i32 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for i64 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for u8 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for u16 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for u32 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for u64 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for f32 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for f64 {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for bool {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl ToJsonValue for String {
    fn to_json_value(&self) -> Value {
        json!(self)
    }
}

impl ToJsonValue for &str {
    fn to_json_value(&self) -> Value {
        json!(*self)
    }
}

impl<T: ToJsonValue> ToJsonValue for Option<T> {
    fn to_json_value(&self) -> Value {
        match self {
            Some(v) => v.to_json_value(),
            None => Value::Null,
        }
    }
}

impl<T: ToJsonValue> ToJsonValue for Vec<T> {
    fn to_json_value(&self) -> Value {
        Value::Array(self.iter().map(|v| v.to_json_value()).collect())
    }
}

/// Collect 2-tuple distribution into JSON array
impl<T1, T2, D1, D2> Collector<(D1, D2), (T1, T2)> for JsonRows
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    T1: ToJsonValue,
    T2: ToJsonValue,
{
    type Output = Value;

    fn collect(dist: &mut (D1, D2), rng: &mut impl Sampler, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let rows: Vec<Value> = (0..n)
            .map(|_| {
                json!({
                    "c0": iter.0.next(rng).to_json_value(),
                    "c1": iter.1.next(rng).to_json_value(),
                })
            })
            .collect();
        Value::Array(rows)
    }
}

/// Collect 3-tuple distribution into JSON array
impl<T1, T2, T3, D1, D2, D3> Collector<(D1, D2, D3), (T1, T2, T3)> for JsonRows
where
    D1: Distribution<T1>,
    D2: Distribution<T2>,
    D3: Distribution<T3>,
    T1: ToJsonValue,
    T2: ToJsonValue,
    T3: ToJsonValue,
{
    type Output = Value;

    fn collect(dist: &mut (D1, D2, D3), rng: &mut impl Sampler, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let rows: Vec<Value> = (0..n)
            .map(|_| {
                json!({
                    "c0": iter.0.next(rng).to_json_value(),
                    "c1": iter.1.next(rng).to_json_value(),
                    "c2": iter.2.next(rng).to_json_value(),
                })
            })
            .collect();
        Value::Array(rows)
    }
}

/// Collect List distribution into JSON array of arrays
impl<T, L, E> Collector<List<L, E>, Vec<T>> for JsonRows
where
    L: Distribution<usize>,
    E: Distribution<T>,
    T: ToJsonValue,
{
    type Output = Value;

    fn collect(dist: &mut List<L, E>, rng: &mut impl Sampler, n: usize) -> Self::Output {
        let mut iter = dist.sample();
        let rows: Vec<Value> = (0..n)
            .map(|_| {
                let len = iter.lengths.next(rng);
                let elements: Vec<Value> = (0..len)
                    .map(|_| iter.elements.next(rng).to_json_value())
                    .collect();
                Value::Array(elements)
            })
            .collect();
        Value::Array(rows)
    }
}

/// Pretty print a JSON value
pub fn pretty_print(value: &Value) -> String {
    serde_json::to_string_pretty(value).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use populate_builtins::{Constant, Range};
    use rand::SeedableRng;
    use rand::rngs::StdRng;

    #[test]
    fn test_json_2tuple() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (Range::new(0i32, 100), Constant::new(true));
        let json = <JsonRows as Collector<_, _>>::collect(&mut dist, &mut rng, 2);

        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_json_3tuple() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = (
            Range::new(0i32, 100),
            Range::new(0.0f64, 1.0),
            Constant::new("hello"),
        );
        let json = <JsonRows as Collector<_, _>>::collect(&mut dist, &mut rng, 3);

        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 3);
    }

    #[test]
    fn test_json_list() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut dist = List::new(Range::new(1usize, 4), Range::new(0i32, 100));
        let json = <JsonRows as Collector<_, _>>::collect(&mut dist, &mut rng, 3);

        assert!(json.is_array());
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        // Each element should be an array
        assert!(arr[0].is_array());
    }
}
