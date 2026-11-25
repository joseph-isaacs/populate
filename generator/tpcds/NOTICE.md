# Notices

Original code in this crate is covered by [the MIT licence](LICENSE-MIT.txt).
The ported components below retain their upstream terms. The package's SPDX
expression is `MIT AND Apache-2.0`; these are cumulative requirements for
different components, not alternative licences.

## tpcdsgen

The sales and returns loops in `src/channels.rs` adapt the store, catalog
and web sales/returns row generators from tpcdsgen 4.0.0 in
[tpcgen-rs](https://github.com/datafusion-contrib/tpcgen-rs/tree/2a9b8868aa1006aded4176abff45318a9e808ba4/tpcdsgen/src/row/tables).
They write paired sales and returns columns directly and share the random
streams and order-level computations.

tpcdsgen is licensed under Apache-2.0. The complete licence is in
[LICENSE-Apache-2.0.txt](LICENSE-Apache-2.0.txt).
The tpcdsgen 4.0.0 source package contains no separate NOTICE file.

TPC-DS is a trademark of the Transaction Processing Performance Council.
Data generated here is not valid for TPC-DS result publication.
