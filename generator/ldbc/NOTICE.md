# Notices

Original code in this crate is covered by [the MIT licence](LICENSE-MIT.txt).
The ported components below retain their upstream terms. The package's SPDX
expression is `MIT AND Apache-2.0 AND SunPro`; these are cumulative
requirements for different components, not alternative licences.

## LDBC SNB datagen

The generator adapts
[ldbc_snb_datagen_spark at a2210c52ddeceeeafbbdd4d4d166d0d0da82f739](https://github.com/ldbc/ldbc_snb_datagen_spark/tree/a2210c52ddeceeeafbbdd4d4d166d0d0da82f739).
The Rust implementation replaces the Java/Spark execution model with
column buffers, chunked generation and Arrow/Parquet output.
The generation, dictionary loading, parameters, table layouts and
transformations are adapted in `src/activity.rs`, `src/bi.rs`,
`src/dates.rs`, `src/dict.rs`, `src/factors.rs`, `src/jrand.rs`,
`src/knows.rs`, `src/layout.rs`, `src/lib.rs`, `src/params.rs`,
`src/person.rs` and `src/rows.rs`.

The upstream Apache-2.0 licence is reproduced in
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt). Its NOTICE.txt reads:

    Copyright [2020-]2026 Linked Data Benchmark Council

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

           http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing, software
       distributed under the License is distributed on an "AS IS" BASIS,
       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
       See the License for the specific language governing permissions and
       limitations under the License.

Dictionaries are fetched separately by `scripts/fetch-ldbc-resources.sh`;
the upstream checkout retains its licence and notices.

## fdlibm

`log1p` in `src/fdlibm.rs` adapts
[fdlibm's s_log1p.c](https://netlib.org/fdlibm/s_log1p.c) to Rust,
using explicit IEEE-754 bit operations. Its SunPro notice is preserved
in that file:

    Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.

    Developed at SunSoft, a Sun Microsystems, Inc. business.
    Permission to use, copy, modify, and distribute this
    software is freely granted, provided that this notice
    is preserved.
