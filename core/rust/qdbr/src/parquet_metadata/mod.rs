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

//! `_pm` parquet partition metadata file format.
//!
//! This module implements readers and writers for the binary metadata file
//! that accompanies each `data.parquet` partition file. The format stores
//! column descriptors, per-row-group column chunk metadata (byte ranges,
//! encodings, statistics), and a footer whose offset is tracked in `_txn`.

pub mod column_chunk;
pub mod convert;
pub mod error;
pub mod footer;
pub mod header;
pub mod jni;
pub mod reader;
pub mod row_group;
pub mod types;
pub mod writer;

pub use column_chunk::ColumnChunkRaw;
pub use convert::{
    generate_parquet_metadata, physical_type_to_u8, update_parquet_metadata, ParquetMetaColumnInfo,
    ParquetMetaUpdateResult,
};
pub use footer::{Footer, FooterBuilder};
pub use header::{ColumnDescriptorRaw, FileHeader, FileHeaderBuilder};
pub use reader::ParquetMetaReader;
pub use row_group::{RowGroupBlockBuilder, RowGroupBlockReader};
pub use types::*;
pub use writer::{ParquetMetaUpdateWriter, ParquetMetaWriter};
