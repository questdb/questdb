mod column_chunk;
mod compression;
mod file;
pub mod footer_cache;
pub mod indexes;
pub(crate) mod page;
mod row_group;
pub mod statistics;

#[cfg(feature = "async")]
mod stream;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::FileStreamer;

mod dyn_iter;
pub use dyn_iter::{DynIter, DynStreamingIterator};

pub use compression::{compress, Compressor};

pub use file::{
    end_file, start_file, write_metadata_sidecar, CopiedColumnIndex, FileWriter, ParquetFile,
};

pub use row_group::{write_row_group, ColumnOffsetsMetadata};

use parquet_format_safe::{ColumnOrder, TypeDefinedOrder};

use crate::page::CompressedPage;

pub type RowGroupIter<'a, E> =
    DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>;

/// Builds the file-level `column_orders` list: one `TypeDefinedOrder` per leaf
/// column, in schema order. The Parquet spec requires this whenever the modern
/// `min_value`/`max_value` statistics are written; without it, conformant readers
/// (pyarrow, Spark, DuckDB, Trino, Iceberg) treat those bounds as undefined and
/// ignore them, losing row-group skipping and predicate pushdown.
pub(crate) fn type_defined_column_orders(num_leaf_columns: usize) -> Vec<ColumnOrder> {
    vec![ColumnOrder::TYPEORDER(TypeDefinedOrder {}); num_leaf_columns]
}

/// Write options of different interfaces on this crate
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct WriteOptions {
    /// Whether to write statistics, including indexes
    pub write_statistics: bool,
    /// Which Parquet version to use
    pub version: Version,
    /// False positive probability for bloom filters
    pub bloom_filter_fpp: f64,
}

/// The parquet version to use
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Version {
    V1,
    V2,
}

/// Used to recall the state of the parquet writer - whether sync or async.
#[derive(PartialEq)]
enum State {
    Initialised,
    Started,
    Finished,
}

impl From<Version> for i32 {
    fn from(version: Version) -> Self {
        match version {
            Version::V1 => 1,
            Version::V2 => 2,
        }
    }
}
