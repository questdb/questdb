//! `_pm` parquet partition metadata file format.
//!
//! Readers and writers for the binary metadata file that accompanies each
//! `data.parquet` partition file. The format stores column descriptors,
//! per-row-group column chunk metadata (byte ranges, encodings, statistics),
//! and a footer whose offset is tracked in `_txn`.
//!
//! The format specification lives in `docs/parquet-metadata.md`.

pub mod column_chunk;
pub mod convert;
pub mod error;
pub mod footer;
pub mod header;
pub mod infer;
pub mod qdb_meta;
pub mod reader;
pub mod row_group;
pub mod types;
pub mod writer;

pub use column_chunk::ColumnChunkRaw;
pub use convert::{
    build_row_group_block, convert_from_parquet, detect_designated_timestamp,
    extract_sorting_columns, generate_parquet_metadata, physical_type_to_u8,
    resolve_sorting_columns, validate_file_paths, BloomFilterSource, NoBloomFilterSource,
    ParquetMetaColumnInfo, SliceBloomFilterSource, SortingCol, TsStatsBackfill,
};
pub use error::{ParquetMetaError, ParquetMetaErrorKind, ParquetMetaResult};
pub use footer::{Footer, FooterBuilder};
pub use header::{ColumnDescriptorRaw, FileHeader, FileHeaderBuilder};
pub use infer::infer_column_type;
pub use qdb_meta::{
    extract_qdb_meta, ParquetFieldId, QdbMeta, QdbMetaCol, QdbMetaColFormat, QdbMetaSchema,
    QdbMetaV1, QDB_META_KEY,
};
pub use reader::ParquetMetaReader;
pub use row_group::{RowGroupBlockBuilder, RowGroupBlockReader};
pub use types::*;
pub use writer::{ParquetMetaUpdateWriter, ParquetMetaWriter};
