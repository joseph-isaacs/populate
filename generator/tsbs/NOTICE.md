# Notices

Original code in this crate is covered by [the MIT licence](LICENSE-MIT.txt).
The ported components below retain their upstream terms. The package's SPDX
expression is `MIT AND BSD-3-Clause AND Apache-2.0`; these are cumulative
requirements for different components, not alternative licences.

## TSBS

The simulations and serializers in `src/config.rs`, `src/devops.rs`,
`src/iot.rs`, `src/model.rs`, `src/sim.rs`, `src/text.rs` and
`src/binary.rs` adapt [TSBS](https://github.com/timescale/tsbs).
The Rust implementation uses column buffers and resumable chunks for
parallel generation. TSBS is MIT-licensed; its complete copyright and
permission notice is in [LICENSE-TSBS.txt](LICENSE-TSBS.txt).

## Go

`src/gorand.rs` and `src/gotables.rs` port Go 1.24's
[`math/rand`](https://github.com/golang/go/tree/go1.24.0/src/math/rand)
(`rng.go`, `rand.go`, `normal.go` and `zipf.go`).
The uniform generator's algorithm is by DP Mitchell and JA Reeds.
`go_exp` and `go_log` port
[`exp_amd64.s`](https://github.com/golang/go/blob/go1.24.0/src/math/exp_amd64.s)
and [`log_amd64.s`](https://github.com/golang/go/blob/go1.24.0/src/math/log_amd64.s).

Copyright 2009 The Go Authors. All rights reserved.
Copyright 2010 The Go Authors. All rights reserved.

These Rust adaptations preserve the output sequence and floating-point
evaluation order. Go's BSD-3-Clause terms are reproduced in full in
[LICENSE-Go.txt](LICENSE-Go.txt).

Go's exponential implementation credits Naoki Shibata's “Efficient
evaluation methods of elementary functions suitable for SIMD computation”
and the original SLEEF implementation, which its source identifies as
public domain.

## FlatBuffers

The `Builder` in `src/binary.rs` is a Rust adaptation of
[FlatBuffers v1.11.0's Go builder](https://github.com/google/flatbuffers/blob/v1.11.0/go/builder.go).
It implements the operations needed by TSBS's MongoDB serializer.
FlatBuffers is licensed under Apache-2.0; the complete licence is in
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt).
