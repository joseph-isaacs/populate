# Implementation Plan: Missing TPC-H Tables

This document details the implementation plan for the 5 missing TPC-H tables.

## Architecture Summary

From analyzing tpchgen, each column is **completely independent**:
- Each column has its own seeded RNG (`Seeded<TpchRng, Distribution>`)
- "Foreign keys" are computed from indices, not cross-column dependencies
- `seeds_per_row` handles variable-count child rows (1-7 lines per order)

No `ForeignKey` combinator is needed. We need:
1. **`Repeat<N>`** - repeat each value N times (fixed count)
2. **`RepeatVar`** - repeat based on a length distribution (variable count)
3. **`SeedsPerRow`** - advance RNG to row boundary after variable iterations

---

## New Combinators Required

### 1. `Repeat<N>` (for PartSupp)

Repeats each value from the inner distribution N times before advancing.

```rust
pub struct Repeat<D, const N: usize> {
    inner: D,
}

pub struct RepeatIter<I: SampleIter, const N: usize> {
    inner: I,
    current: Option<I::Item>,
    count: usize,
}

impl<I: SampleIter, const N: usize> SampleIter for RepeatIter<I, N>
where
    I::Item: Clone,
{
    type Item = I::Item;

    fn next(&mut self, rng: &mut impl Rng) -> Self::Item {
        if self.count == 0 {
            self.current = Some(self.inner.next(rng));
            self.count = N;
        }
        self.count -= 1;
        self.current.clone().unwrap()
    }
}
```

**Usage:**
```rust
// ps_partkey: 1,1,1,1, 2,2,2,2, 3,3,3,3, ...
let part_key = Repeat::<_, 4>::new(inc(1i64));
```

### 2. `Cycle<N>` enhancement

Already exists, but verify it works for:
```rust
// supplier_num: 0,1,2,3, 0,1,2,3, ...
let supplier_num = Cycle::new([0i64, 1, 2, 3]);
```

### 3. `RepeatVar` (for LineItem)

Repeats each value a variable number of times based on a length distribution.

```rust
pub struct RepeatVar<D, L> {
    inner: D,
    lengths: L,
}

pub struct RepeatVarIter<I, LI> {
    inner: I,
    lengths: LI,
    current: Option<I::Item>,
    remaining: usize,
}
```

### 4. `SeedsPerRow<N>` wrapper (for LineItem)

Wraps a distribution to consume exactly N seeds per logical row, advancing to row boundary.

```rust
pub struct SeedsPerRow<D, const N: usize> {
    inner: D,
}

pub struct SeedsPerRowIter<I, const N: usize> {
    inner: I,
    used_this_row: usize,
}

impl<I: SampleIter, const N: usize> SeedsPerRowIter<I, N> {
    /// Called when logical row ends - advances RNG to consume remaining seeds
    fn row_finished(&mut self, rng: &mut impl Rng) {
        for _ in self.used_this_row..N {
            // Advance RNG by calling inner.next() or directly on rng
        }
        self.used_this_row = 0;
    }
}
```

### 5. Text Generation Combinators

#### `RandomAlphaNumeric`
Variable-length alphanumeric strings (for addresses).

#### `RandomText` / `TextPool`
TPC-H text generation from word distributions.

#### `PhoneNumber`
Nation-aware phone: `(nation + 10)-XXX-XXX-XXXX`

---

## Table Implementations

### 1. SUPPLIER (10,000 × SF rows)

**Columns:**
| Column | Seed | Generation |
|--------|------|------------|
| s_suppkey | N/A | `inc(1i64)` |
| s_name | N/A | `s_suppkey.map(\|k\| format!("Supplier#{:09}", k))` |
| s_address | 706178559 | `seeded(rng, RandomAlphaNumeric::new(10, 40))` |
| s_nationkey | 110356601 | `seeded(rng, range(0, 25))` |
| s_phone | 884434366 | `seeded(rng, PhoneNumber)` paired with s_nationkey |
| s_acctbal | 962338209 | `seeded(rng, range(-99999, 999999))` |
| s_comment | 1341315363 | `seeded(rng, RandomText::new(25, 100))` with BBB injection |

**Phone number note:** Phone depends on nation. Options:
1. Generate nation first, pass to phone via Map on tuple
2. Generate phone digits independently, format with nation

**BBB injection:** 10 suppliers (per 10K) get "Customer Complaints" or "Customer Recommends" injected. Handle via Map with index check.

**Implementation:**
```rust
pub fn suppliers() -> impl Distribution<Supplier> {
    (
        inc(1i64),                                           // s_suppkey
        seeded(rng(706178559), RandomAlphaNumeric::new(25)), // s_address
        seeded(rng(110356601), range(0i64, 25)),             // s_nationkey
        seeded(rng(884434366), PhoneDigits),                 // phone digits
        seeded(rng(962338209), range(-99999i64, 1000000)),   // s_acctbal
        seeded(rng(1341315363), RandomText::new(63)),        // s_comment
    ).map(|(key, addr, nation, phone_digits, acctbal, comment)| {
        Supplier {
            s_suppkey: key,
            s_name: format!("Supplier#{:09}", key),
            s_address: addr,
            s_nationkey: nation,
            s_phone: format_phone(nation, phone_digits),
            s_acctbal: acctbal,
            s_comment: maybe_inject_bbb(key, comment),
        }
    })
}
```

---

### 2. CUSTOMER (150,000 × SF rows)

Nearly identical to Supplier.

**Columns:**
| Column | Seed | Generation |
|--------|------|------------|
| c_custkey | N/A | `inc(1i64)` |
| c_name | N/A | `format!("Customer#{:09}", key)` |
| c_address | 881155353 | `RandomAlphaNumeric` |
| c_nationkey | 1489529863 | `range(0, 25)` |
| c_phone | 1521138112 | `PhoneNumber` (with nation) |
| c_acctbal | 298370230 | `range(-99999, 999999)` |
| c_mktsegment | 1140279430 | `PickOneRef` from 5 segments |
| c_comment | 1335826707 | `RandomText::new(73)` |

**Market segments:** `["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"]`

---

### 3. PARTSUPP (800,000 × SF rows = 4 per Part)

This is the first table requiring `Repeat<4>`.

**Columns:**
| Column | Seed | Generation |
|--------|------|------------|
| ps_partkey | N/A | `Repeat::<_, 4>::new(inc(1i64))` → 1,1,1,1, 2,2,2,2, ... |
| ps_suppkey | N/A | Computed from (part_key, supplier_num) |
| ps_availqty | 1671059989 | `range(1, 9999)` with seeds_per_row=4 |
| ps_supplycost | 1051288424 | `range(100, 100000)` with seeds_per_row=4 |
| ps_comment | 1961692154 | `RandomText::new(124)` with seeds_per_row=4 |

**Supplier selection formula:**
```rust
fn select_supplier(part_key: i64, supplier_num: i64, supplier_count: i64) -> i64 {
    ((part_key + (supplier_num * ((supplier_count / 4) + ((part_key - 1) / supplier_count))))
        % supplier_count) + 1
}
```

**Implementation sketch:**
```rust
pub fn partsupps(scale_factor: f64) -> impl Distribution<PartSupp> {
    let supplier_count = (10_000.0 * scale_factor) as i64;

    (
        Repeat::<_, 4>::new(inc(1i64)),           // ps_partkey: 1,1,1,1, 2,2,2,2, ...
        Cycle::new([0i64, 1, 2, 3]),              // supplier_num: 0,1,2,3, 0,1,2,3, ...
        seeded(rng(1671059989), range(1, 10000)), // ps_availqty
        seeded(rng(1051288424), range(100, 100001)), // ps_supplycost (cents)
        seeded(rng(1961692154), RandomText::new(124)), // ps_comment
    ).map(move |(part_key, supp_num, availqty, cost, comment)| {
        PartSupp {
            ps_partkey: part_key,
            ps_suppkey: select_supplier(part_key, supp_num, supplier_count),
            ps_availqty: availqty as i32,
            ps_supplycost: cost,
            ps_comment: comment,
        }
    })
}
```

**Seed accounting:** The RNGs for availqty, supplycost, comment need `seeds_per_row=4`. They generate one value per supplier, then advance to next row's seed block after 4 values.

---

### 4. ORDERS (1,500,000 × SF rows)

**Columns:**
| Column | Seed | Generation |
|--------|------|------------|
| o_orderkey | N/A | `inc(1i64).map(sparse_encode)` |
| o_custkey | 851767375 | `range(1, max_customers).map(apply_mortality)` |
| o_orderstatus | N/A | Computed (requires simulating line items) |
| o_totalprice | N/A | Computed (requires simulating line items) |
| o_orderdate | 1066728069 | `range(92, 2466)` (days from epoch) |
| o_orderpriority | 591449447 | `PickOneRef` from 5 priorities |
| o_clerk | 1171034773 | `range(1, max_clerk).map(format_clerk)` |
| o_shippriority | N/A | `Constant::new(0i32)` |
| o_comment | 276090261 | `RandomText::new(49)` |

**Sparse orderkey encoding:**
```rust
fn sparse_encode(index: i64) -> i64 {
    let low_bits = index & 0b111;
    let high = (index >> 3) << 2;
    (high << 3) + low_bits
}
```

**Customer mortality (1 in 3 have no orders):**
```rust
fn apply_mortality(cust_key: i64, order_index: i64) -> i64 {
    if cust_key % 3 == 0 {
        if (order_index / 3) % 2 == 0 { cust_key + 1 } else { cust_key - 1 }
    } else {
        cust_key
    }
}
```

**Order status/totalprice:** These require knowing all line items for the order. Options:
1. **Simulate line items inline** - generate line item data just to compute status/price
2. **Generate Orders + LineItems together** - return (Order, Vec<LineItem>) and derive status
3. **Post-process** - generate with placeholder, fix up in a second pass

Recommend option 1 (inline simulation) to match tpchgen's approach.

**Priorities:** `["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]`

---

### 5. LINEITEM (~6,000,000 × SF rows, 1-7 per Order)

This is the most complex table.

**Columns:**
| Column | Seed | seeds_per_row | Generation |
|--------|------|---------------|------------|
| l_orderkey | N/A | 1 | From order index via sparse encoding |
| l_partkey | 1808217256 | 7 | `range(1, max_parts)` |
| l_suppkey | 2095021727 | 7 | Computed from (part_key, supplier_num) |
| l_linenumber | N/A | N/A | Counter 1..=line_count within order |
| l_quantity | 209208115 | 7 | `range(1, 50)` |
| l_extendedprice | N/A | N/A | `quantity * part_price(part_key)` |
| l_discount | 554590007 | 7 | `range(0, 10)` (represents 0.00-0.10) |
| l_tax | 721958466 | 7 | `range(0, 8)` (represents 0.00-0.08) |
| l_returnflag | 717419739 | 7 | Conditional on receipt_date |
| l_linestatus | N/A | N/A | "F" if ship_date < today else "O" |
| l_shipdate | 1769349045 | 7 | `order_date + range(1, 121)` |
| l_commitdate | 904914315 | 7 | `order_date + range(30, 90)` |
| l_receiptdate | 373135028 | 7 | `ship_date + range(1, 30)` |
| l_shipinstruct | 1371272478 | 7 | PickOneRef from 4 values |
| l_shipmode | 675466456 | 7 | PickOneRef from 7 values |
| l_comment | 1095462486 | 7 | `RandomText::new(27)` |

**Line count per order:**
- Seed: 1095462486 (same as comment? need to verify)
- Range: 1-7
- This determines how many LineItems each Order produces

**Return flag logic:**
```rust
let return_flag = if receipt_date.is_past() {
    pick_from_distribution()  // "R", "A", or "N"
} else {
    "N"
};
```

**Ship instructions:** `["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]`

**Ship modes:** `["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]`

---

## Implementation Order

1. **New combinators first:**
   - `Repeat<N>` - simple, enables PartSupp
   - `RandomAlphaNumeric` - needed for addresses
   - `PhoneNumber` - needed for phone fields
   - `RandomText` - needed for comments

2. **Tables in dependency order:**
   1. Supplier (no deps except new combinators)
   2. Customer (same pattern as Supplier)
   3. PartSupp (needs Repeat<4>, depends on Part + Supplier counts)
   4. Orders (needs mortality logic, line item simulation for status)
   5. LineItem (most complex, needs RepeatVar + SeedsPerRow)

---

## Testing Strategy

Each table should be validated against tpchgen:
```rust
#[test]
fn test_supplier_matches_tpchgen() {
    let our_suppliers: Vec<_> = suppliers().sample().take(100).collect();
    let tpchgen_suppliers: Vec<_> = SupplierGenerator::new(1.0, 1, 1).take(100).collect();

    for (ours, theirs) in our_suppliers.iter().zip(tpchgen_suppliers.iter()) {
        assert_eq!(ours.s_suppkey, theirs.s_suppkey);
        assert_eq!(ours.s_name, theirs.s_name.to_string());
        // ... etc
    }
}
```

---

## Open Questions

1. **BBB injection in Supplier comments** - How to inject "Customer Complaints" at specific indices? Need stateful Map or separate combinator.

2. **Order status/totalprice** - Inline line item simulation vs. generate together? Inline matches tpchgen but adds complexity.

3. **seeds_per_row accounting** - Should this be explicit in our API or handled implicitly by grouping generators?

4. **Scale factor** - Should it be a parameter to each table function or global config?
