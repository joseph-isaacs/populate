# populate-builtins

populate's built-in distributions: `constant`, `range`, `map`, `list`,
`uniform`, `choose`, `weighted`, `text`, `chars` and the other leaves and
combinators the generators are built from.

Part of [populate](https://github.com/joseph-isaacs/populate); most users depend on
the `populate` crate, which re-exports this one. API docs: <https://docs.rs/populate-builtins>.

For synthetic benchmark inputs, `indexed` computes a value from the row
number, `runs` expands (value, length) pairs, and `shuffled` makes a
permutation that chunks can read independently. Use `sorted` for ordered
values and `sorted_unique` for distinct patch or selection indices. Use `sample` with a rand
distribution, `sample_range(bounds)` to reproduce rand’s per-row range draws,
or `bernoulli(p)` for a boolean mask. `nullable(values, present)`
consumes a value for every row. `draw_if(present, values)` checks each mask row
first and consumes a value only when present. The combinator `fixed_size_list::<N, _>(elements)` groups
successive scalar values into arrays of width N. `from_iter` accepts an
existing iterator, including one that owns a seeded RNG.

Use `collect_as` when a benchmark wants a specific scalar collection.
For example, `let indices: Buffer<u32> = indexed(|i| (i * 4) as u32).collect_as(rng, n);`
fills a Vortex buffer directly. The return type selects the destination.
