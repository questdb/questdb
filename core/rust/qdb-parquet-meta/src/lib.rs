//! `_pm` parquet partition metadata file format.
//!
//! Readers and writers for the binary metadata file that accompanies each
//! `data.parquet` partition file. The format stores column descriptors,
//! per-row-group column chunk metadata (byte ranges, encodings, statistics),
//! and a footer whose offset is tracked in `_txn`.
//!
//! The format specification lives in `docs/parquet-metadata.md`.

pub mod column_chunk;
pub mod error;
pub mod footer;
pub mod header;
pub mod reader;
pub mod row_group;
pub mod types;
pub mod writer;

pub use column_chunk::ColumnChunkRaw;
pub use error::{ParquetMetaError, ParquetMetaErrorKind, ParquetMetaResult};
pub use footer::{Footer, FooterBuilder};
pub use header::{ColumnDescriptorRaw, FileHeader, FileHeaderBuilder};
pub use reader::ParquetMetaReader;
pub use row_group::{RowGroupBlockBuilder, RowGroupBlockReader};
pub use types::*;
pub use writer::{ParquetMetaUpdateWriter, ParquetMetaWriter};
