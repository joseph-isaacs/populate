//! A sales table and its returns from one pass.
//!
//! dsbgen's returns generators replay every sales line item, so generating
//! `store_sales` and `store_returns` separately makes the sales twice. When
//! both are asked for with the same chunks, [`Channel::run`] drives the
//! returns generator's [`next_with_sale`](StoreReturnsRowGenerator::next_with_sale)
//! once and converts both tables' rows.

use crate::TableName;
use arrow_array::RecordBatch;
use dsbgen::config::Session;
use dsbgen::row::{CatalogReturnsRowGenerator, StoreReturnsRowGenerator, WebReturnsRowGenerator};
use dsbgen_arrow::{
    CatalogReturnsArrow, CatalogSalesArrow, StoreReturnsArrow, StoreSalesArrow, WebReturnsArrow,
    WebSalesArrow,
};

/// A sales channel: a sales table and its returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Store,
    Catalog,
    Web,
}

macro_rules! run_channel {
    ($gen:ident, $sales:ident, $returns:ident, $session:expr, $source_rows:expr,
     $first:expr, $last:expr, $batch:expr, $emit:expr) => {{
        let mut g = $gen::new($session.clone(), $source_rows);
        g.set_source_row_range($first, $last);
        let (mut sc, mut rc) = ($sales::converter(), $returns::converter());
        let (mut sales, mut returns) = (Vec::with_capacity($batch), Vec::with_capacity($batch));
        while let Some((sale, ret)) = g.next_with_sale() {
            sales.push(sale);
            if sales.len() == $batch {
                $emit(0, sc.batch_from_rows(&sales).expect("sales batch"));
                sales.clear();
            }
            if let Some(r) = ret {
                returns.push(r);
                if returns.len() == $batch {
                    $emit(1, rc.batch_from_rows(&returns).expect("returns batch"));
                    returns.clear();
                }
            }
        }
        if !sales.is_empty() {
            $emit(0, sc.batch_from_rows(&sales).expect("sales batch"));
        }
        if !returns.is_empty() {
            $emit(1, rc.batch_from_rows(&returns).expect("returns batch"));
        }
    }};
}

impl Channel {
    /// The channel of a sales or returns table, and whether it is the
    /// sales one.
    pub(crate) fn of(t: TableName) -> Option<(Channel, bool)> {
        use TableName::*;
        Some(match t {
            StoreSales => (Channel::Store, true),
            StoreReturns => (Channel::Store, false),
            CatalogSales => (Channel::Catalog, true),
            CatalogReturns => (Channel::Catalog, false),
            WebSales => (Channel::Web, true),
            WebReturns => (Channel::Web, false),
            _ => return None,
        })
    }

    /// Sales (output 0) and returns (output 1) of source rows
    /// `first..=last` (1-based) of `source_rows`, in batches of up to
    /// `batch` rows.
    pub(crate) fn run(
        self,
        session: &Session,
        source_rows: u64,
        first: u64,
        last: u64,
        batch: usize,
        emit: &mut dyn FnMut(usize, RecordBatch),
    ) {
        match self {
            Channel::Store => run_channel!(
                StoreReturnsRowGenerator,
                StoreSalesArrow,
                StoreReturnsArrow,
                session,
                source_rows,
                first,
                last,
                batch,
                emit
            ),
            Channel::Catalog => run_channel!(
                CatalogReturnsRowGenerator,
                CatalogSalesArrow,
                CatalogReturnsArrow,
                session,
                source_rows,
                first,
                last,
                batch,
                emit
            ),
            Channel::Web => run_channel!(
                WebReturnsRowGenerator,
                WebSalesArrow,
                WebReturnsArrow,
                session,
                source_rows,
                first,
                last,
                batch,
                emit
            ),
        }
    }
}
