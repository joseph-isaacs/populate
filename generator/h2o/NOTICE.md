# Notices

`populate-gen-h2o` is licensed as a whole under `GPL-2.0-or-later AND
MPL-2.0`, because it ports code from R and from data.table. The rest of
the populate workspace is MIT; depending on this crate brings these terms
into whatever links it.

## R

`src/rng.rs` ports R 4.x's Mersenne Twister seeding, `unif_rand()`,
`R_unif_index()` and `sample()` (`src/main/RNG.c`, `src/main/random.c`),
and `src/cols.rs` reproduces the first-appearance order of `unique()`
(`src/main/unique.c`); `round6` in `src/fwrite.rs` follows R's `fround`
(`src/nmath/fround.c`). Together they make the output byte-identical to the H2O
db-benchmark datagen scripts run under `set.seed(108)`.

R is copyright The R Core Team and the other authors named in its source
files, and is licensed under the GNU General Public License, version 2 or
(at your option) any later version. The GPL-2.0 text is in
[LICENSE-GPL-2.0.txt](LICENSE-GPL-2.0.txt).

## data.table

`src/fwrite.rs` ports `writeFloat64` from data.table's `src/fwrite.c`, and
`src/fwrite_tables.rs` is generated from its `src/fwriteLookups.h`, so that
numbers are written exactly as `fwrite` writes them (data.table 1.14.10).

data.table is copyright its authors and contributors and is licensed under
the Mozilla Public License 2.0. The MPL-2.0 text is in
[LICENSE-MPL-2.0.txt](LICENSE-MPL-2.0.txt); the source of the original
files is at <https://github.com/Rdatatable/data.table>.
