# Struct Derive Design

Derives `Distribution` support for user-defined structs via `#[derive(ValueType)]`.

---

## Goal

Allow structs as value types with columnar storage, builder-pattern construction, and full composition with tuples, lists, and maps.

```rust
#[derive(ValueType)]
struct LineItem {
    order_key: i32,
    quantity: i64,
    price: f64,
    shipped: bool,
}

let dist = LineItem::dist()
    .with_order_key(range(0, 1000))
    .with_quantity(range(1, 100))
    .with_price(range(0.0, 999.99))
    .with_shipped(constant(true));

let sink = dist.collect(&mut rng, 1_000_000);
```

---

## What `#[derive(ValueType)]` generates

For a struct with N fields:

```rust
#[derive(ValueType)]
struct LineItem {
    order_key: i32,
    quantity: i64,
    price: f64,
    shipped: bool,
}
```

### 1. `SinkFor<B>` — maps struct to flat HList sink

```rust
impl<B: Backend> SinkFor<B> for LineItem
where
    B::Prim<i32>: HAppend<HCons<B::Prim<i64>, HNil>>,
    // ... chained HAppend bounds
{
    type Sink = HCons<B::Prim<i32>, HCons<B::Prim<i64>,
                HCons<B::Prim<f64>, HCons<B::Prim<bool>, HNil>>>>;
}
```

Same flat HList as `(i32, i64, f64, bool)`. Each field maps to one HList element via its own `SinkFor`.

### 2. `Sink<LineItem>` for the HList — destructure and push

```rust
impl<S1: Sink<i32>, S2: Sink<i64>, S3: Sink<f64>, S4: Sink<bool>>
    Sink<LineItem> for HCons<S1, HCons<S2, HCons<S3, HCons<S4, HNil>>>>
{
    fn push(&mut self, item: LineItem) {
        self.0.push(item.order_key);
        self.1.0.push(item.quantity);
        self.1.1.0.push(item.price);
        self.1.1.1.0.push(item.shipped);
    }
}
```

### 3. `ValueHList` — struct to HList for flattening in tuples

```rust
impl ValueHList for LineItem {
    type HList = HCons<i32, HCons<i64, HCons<f64, HCons<bool, HNil>>>>;

    fn into_hlist(self) -> Self::HList {
        HCons(self.order_key, HCons(self.quantity,
            HCons(self.price, HCons(self.shipped, HNil))))
    }
}
```

### 4. `LineItemDist` — distribution struct with builder

```rust
struct LineItemDist<D1 = (), D2 = (), D3 = (), D4 = ()> {
    order_key: D1,
    quantity: D2,
    price: D3,
    shipped: D4,
}
```

`()` defaults for unset fields — compile error if any field is missing when used as a Distribution.

### 5. `LineItem::dist()` — entry point

```rust
impl LineItem {
    fn dist() -> LineItemDist {
        LineItemDist {
            order_key: (),
            quantity: (),
            price: (),
            shipped: (),
        }
    }
}
```

### 6. `.with_<field>()` — builder methods

```rust
impl<D1, D2, D3, D4> LineItemDist<D1, D2, D3, D4> {
    fn with_order_key<D>(self, d: D) -> LineItemDist<D, D2, D3, D4> {
        LineItemDist {
            order_key: d,
            quantity: self.quantity,
            price: self.price,
            shipped: self.shipped,
        }
    }

    fn with_quantity<D>(self, d: D) -> LineItemDist<D1, D, D3, D4> {
        LineItemDist {
            order_key: self.order_key,
            quantity: d,
            price: self.price,
            shipped: self.shipped,
        }
    }

    fn with_price<D>(self, d: D) -> LineItemDist<D1, D2, D, D4> {
        LineItemDist {
            order_key: self.order_key,
            quantity: self.quantity,
            price: d,
            shipped: self.shipped,
        }
    }

    fn with_shipped<D>(self, d: D) -> LineItemDist<D1, D2, D3, D> {
        LineItemDist {
            order_key: self.order_key,
            quantity: self.quantity,
            price: self.price,
            shipped: d,
        }
    }
}
```

Each method replaces one type parameter. The others carry through unchanged.

### 7. `Distribution<B>` for `LineItemDist`

```rust
impl<B: Backend, D1, D2, D3, D4> Distribution<B> for LineItemDist<D1, D2, D3, D4>
where
    D1: Distribution<B, Value = i32>,
    D2: Distribution<B, Value = i64>,
    D3: Distribution<B, Value = f64>,
    D4: Distribution<B, Value = bool>,
    D1::Sink: HAppend<D2::Sink>,
    <D1::Sink as HAppend<D2::Sink>>::Output: HAppend<D3::Sink>,
    <<D1::Sink as HAppend<D2::Sink>>::Output as HAppend<D3::Sink>>::Output: HAppend<D4::Sink>,
{
    type Value = LineItem;
    type Sink = <<<D1::Sink as HAppend<D2::Sink>>::Output
        as HAppend<D3::Sink>>::Output
        as HAppend<D4::Sink>>::Output;
    type Iter = LineItemIter<D1::Iter, D2::Iter, D3::Iter, D4::Iter>;

    fn default_sink(&self) -> Self::Sink {
        self.order_key.default_sink()
            .append(self.quantity.default_sink())
            .append(self.price.default_sink())
            .append(self.shipped.default_sink())
    }

    fn collect_into(&mut self, sink: Self::Sink, rng: &mut impl Rng, n: usize) -> Self::Sink {
        let (s4, rest) = <<D1::Sink as HAppend<D2::Sink>>::Output
            as HAppend<D3::Sink>>::Output::unappend_right(sink);
        let (s3, rest) = <D1::Sink as HAppend<D2::Sink>>::Output::unappend_right(rest);
        let (s2, s1) = D1::Sink::unappend_right(rest);

        let s1 = self.order_key.collect_into(s1, rng, n);
        let s2 = self.quantity.collect_into(s2, rng, n);
        let s3 = self.price.collect_into(s3, rng, n);
        let s4 = self.shipped.collect_into(s4, rng, n);

        s1.append(s2).append(s3).append(s4)
    }

    fn row_iter(&mut self) -> Self::Iter {
        LineItemIter {
            order_key: self.order_key.row_iter(),
            quantity: self.quantity.row_iter(),
            price: self.price.row_iter(),
            shipped: self.shipped.row_iter(),
        }
    }
}
```

### 8. `LineItemIter` — row iterator producing structs

```rust
struct LineItemIter<I1, I2, I3, I4> {
    order_key: I1,
    quantity: I2,
    price: I3,
    shipped: I4,
}

impl<I1, I2, I3, I4> SampleIter for LineItemIter<I1, I2, I3, I4>
where
    I1: SampleIter<Item = i32>,
    I2: SampleIter<Item = i64>,
    I3: SampleIter<Item = f64>,
    I4: SampleIter<Item = bool>,
{
    type Item = LineItem;

    fn next(&mut self, rng: &mut impl Rng) -> LineItem {
        LineItem {
            order_key: self.order_key.next(rng),
            quantity: self.quantity.next(rng),
            price: self.price.next(rng),
            shipped: self.shipped.next(rng),
        }
    }
}
```

---

## Composition

Struct distributions compose exactly like tuples because they produce the same flat HList sinks.

### In a tuple

```rust
let dist = (
    LineItem::dist()
        .with_order_key(range(0, 1000))
        .with_quantity(range(1, 100))
        .with_price(range(0.0, 999.99))
        .with_shipped(constant(true)),
    range(0i32, 100),  // extra column
);
// Value = (LineItem, i32) — but auto-flattens to (i32, i64, f64, bool, i32)
// Sink  = LineItem's 4-element HList ++ HCons<Vec<i32>, HNil> = 5-element HList
```

ValueHList on LineItem produces `HCons<i32, HCons<i64, HCons<f64, HCons<bool, HNil>>>>`, so the tuple auto-flattens the struct fields alongside other columns.

### Inside a list

```rust
let dist = list(
    range(1usize, 5),
    LineItem::dist()
        .with_order_key(range(0, 1000))
        .with_quantity(range(1, 100))
        .with_price(range(0.0, 999.99))
        .with_shipped(constant(true)),
);
// Value = Vec<LineItem>
// Sink  = HCons<ColumnarList<HCons<Vec<i32>, HCons<Vec<i64>,
//           HCons<Vec<f64>, HCons<Vec<bool>, HNil>>>>>, HNil>
//
// Layout: offsets + 4 flat column buffers. Columnar struct-inside-list.
```

### Inside a map

```rust
let dist = map(
    (range(0i32, 100), range(0i64, 50)),
    |(key, qty)| LineItem {
        order_key: key,
        quantity: qty,
        price: qty as f64 * 9.99,
        shipped: key > 50,
    },
);
// Value = LineItem
// Sink  = <LineItem as SinkFor<B>>::Sink (the flat 4-element HList)
// Collects row-by-row via Sink<LineItem> for HCons<..>
```

---

## Nested structs

```rust
#[derive(ValueType)]
struct Order {
    order_key: i32,
    status: bool,
}

#[derive(ValueType)]
struct LineItem {
    order: Order,     // nested struct
    quantity: i64,
    price: f64,
}
```

`SinkFor` for `LineItem` recurses into `Order`'s `SinkFor`:

```
Order::SinkFor     = HCons<Vec<i32>, HCons<Vec<bool>, HNil>>
LineItem::SinkFor  = HCons<Vec<i32>, HCons<Vec<bool>, HCons<Vec<i64>, HCons<Vec<f64>, HNil>>>>
```

Flat — the nested struct's fields are inlined into the parent's HList. Same as `(i32, bool, i64, f64)`.

The derive for LineItem generates:

```rust
impl<B: Backend> SinkFor<B> for LineItem
where
    <Order as SinkFor<B>>::Sink: HAppend<<i64 as SinkFor<B>>::Sink>,
    // ...
{
    type Sink = <<Order as SinkFor<B>>::Sink
        as HAppend<<i64 as SinkFor<B>>::Sink>>::Output
        as HAppend<<f64 as SinkFor<B>>::Sink>>::Output;
}
```

The builder uses the nested struct's distribution:

```rust
let dist = LineItem::dist()
    .with_order(
        Order::dist()
            .with_order_key(range(0, 1000))
            .with_status(constant(true))
    )
    .with_quantity(range(1, 100))
    .with_price(range(0.0, 999.99));
```

The `.with_order()` method accepts any `D: Distribution<B, Value = Order>`.

---

## Struct fields with lists

```rust
#[derive(ValueType)]
struct Document {
    id: i32,
    tags: Vec<String>,
}
```

`SinkFor`:

```
i32          → HCons<Vec<i32>, HNil>
Vec<String>  → HCons<ColumnarList<HCons<ColumnarList<HCons<Vec<u8>, HNil>>, HNil>>, HNil>
Document     → HCons<Vec<i32>, HCons<ColumnarList<...>, HNil>>
```

The builder:

```rust
let dist = Document::dist()
    .with_id(range(0, 10000))
    .with_tags(list(
        range(1usize, 5),
        pick_one(&["rust", "data", "fast"]),
    ));
```

---

## Missing fields — compile-time error

```rust
let dist = LineItem::dist()
    .with_order_key(range(0, 1000))
    .with_quantity(range(1, 100));
    // forgot price and shipped

dist.collect(&mut rng, 100);
// ERROR: `()` does not implement `Distribution<_, Value = f64>`
//   in `LineItemDist<Range<i32>, Range<i64>, (), ()>`
```

The `()` default for unset fields causes a clear compile error pointing at the missing field's type parameter.

---

## What the derive macro generates (summary)

| Generated item | Purpose |
|---|---|
| `SinkFor<B> for Struct` | Maps struct to flat HList sink |
| `Sink<Struct> for HCons<..>` | Push struct into HList sink (field-by-field) |
| `ValueHList for Struct` | Struct → value HList (for tuple auto-flatten) |
| `StructDist<D1, ..., DN>` | Distribution struct with one type param per field |
| `Struct::dist()` | Returns `StructDist<(), ..., ()>` |
| `.with_<field>(d)` | Builder method, replaces one type param |
| `Distribution<B> for StructDist` | col-by-col collection, split/delegate/reappend |
| `StructIter<I1, ..., IN>` | Row iterator producing struct values |
| `SampleIter for StructIter` | Combines sub-iterators into struct |
