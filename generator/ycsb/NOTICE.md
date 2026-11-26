# Notices

Original code in this crate is covered by [the MIT licence](LICENSE-MIT.txt).
The ported components below retain their upstream terms. The package's SPDX
expression is `MIT AND Apache-2.0`; these are cumulative requirements for
different components, not alternative licences.

## YCSB

`src/ycsb.rs` and `src/ycsb_run.rs` adapt YCSB 0.17.0's
[`CoreWorkload`](https://github.com/brianfrankcooper/YCSB/blob/0.17.0/core/src/main/java/site/ycsb/workloads/CoreWorkload.java),
`Utils`, byte iterators and
[request generators](https://github.com/brianfrankcooper/YCSB/tree/0.17.0/core/src/main/java/site/ycsb/generator).
The Rust implementation emits columns and operation streams using
counter-based random access and parallel chunks.

Copyright (c) 2010 Yahoo! Inc., Copyright (c) 2016-2017 YCSB contributors. All rights reserved.
Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors. All rights reserved.

The Apache-2.0 licence is reproduced in
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt). The upstream NOTICE.txt reads:

    =========================================================================
    NOTICE file for use with, and corresponding to Section 4 of,
    the Apache License, Version 2.0,
    in this case for the YCSB project.
    =========================================================================

       This product includes software developed by
       Yahoo! Inc. (www.yahoo.com)
       Copyright (c) 2010 Yahoo! Inc.  All rights reserved.

       This product includes software developed by
       Google Inc. (www.google.com)
       Copyright (c) 2015 Google Inc.  All rights reserved.
