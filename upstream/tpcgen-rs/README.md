# Upstream patches for tpcgen-rs (tpchgen / tpcdsgen)

Two independent PRs for `datafusion-contrib/tpcgen-rs`, as `git format-patch` files against `main` @ `d2ccc58`. Descriptions are in `PR-1-tpchgen.md` and `PR-2-tpcdsgen.md`.

## Open the PRs (from a fork of tpcgen-rs)

```bash
git checkout -b perf/tpchgen-rng origin/main && git am 0001-*.patch
git checkout -b perf/tpcdsgen-rng origin/main && git am 0002-*.patch
```

## Reproduce the validation

`validate/` compares the patched crates with the released 4.0.0 crates, field by field. It expects two checkouts next to `data-comb`, each with its patch applied and the workspace version bumped locally to `5.0.0`, so Cargo can link both versions:

```bash
git clone https://github.com/datafusion-contrib/tpcgen-rs ../tpcgen-rs-tpch
git -C ../tpcgen-rs-tpch am "$PWD"/upstream/tpcgen-rs/0001-*.patch
sed -i '0,/^version = "4.0.0"/s//version = "5.0.0"/' ../tpcgen-rs-tpch/Cargo.toml
```

Repeat for `../tpcgen-rs-tpcds` with `0002-*.patch`, then:

```bash
cd upstream/tpcgen-rs/validate && cargo run --release            # SF 1 full + SF 100 partitions
cargo run --release -- sweep                                       # SF 0.01 .. 300000
cargo run --release -- bench                                       # speed, released vs patched
```

Logs from this session: `validate/full-sf1-and-sf100.log`, `validate/sf-sweep.log`.
