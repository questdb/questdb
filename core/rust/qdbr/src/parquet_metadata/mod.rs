/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! `_pm` parquet partition metadata.
//!
//! Format primitives (readers, writers, footer/header/row-group/column-chunk
//! types) live in the shared `qdb-parquet-meta` crate so that both `qdbr`
//! (OSS) and `qdb-ent` (enterprise) can parse `_pm` independently. This
//! module retains qdbr-only pieces: write-path conversion helpers
//! (`convert`), JNI thunks (`jni`), and row-group filter pushdown that
//! delegates to `crate::parquet_read::ParquetDecoder` (`skip`).
//!
//! The format specification lives in `docs/parquet-metadata.md`.

pub mod convert;
pub mod jni;
pub mod skip;

pub use qdb_parquet_meta::{
    column_chunk, error, footer, header, reader, row_group, types, writer, ColumnChunkRaw,
    ColumnDescriptorRaw, FileHeader, FileHeaderBuilder, Footer, FooterBuilder, ParquetMetaReader,
    ParquetMetaUpdateWriter, ParquetMetaWriter, RowGroupBlockBuilder, RowGroupBlockReader,
};

pub use convert::{
    generate_parquet_metadata, physical_type_to_u8, update_parquet_metadata, ParquetMetaColumnInfo,
    ParquetMetaUpdateResult,
};
