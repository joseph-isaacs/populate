<!-- Modified from tpcdsgen 4.0.0 for the populate DSB variant. -->

# dsbgen

tpcdsgen 4.0 (the Apache-2.0 Rust port of TPC-DS dsdgen from
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs)) with
Microsoft [DSB](https://github.com/microsoft/dsb)'s changes to dsdgen
ported in. Its row generators produce DSB dsdgen's rows; `populate-gen-dsb`
runs them in parallel chunks. See `../NOTICE.md` and `../README.md`.
