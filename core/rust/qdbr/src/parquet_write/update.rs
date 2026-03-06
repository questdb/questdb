/*******************************************************************************
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
use crate::allocator::QdbAllocator;
use crate::parquet::error::{
    fmt_err, ParquetError, ParquetErrorExt, ParquetErrorReason, ParquetResult,
};
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_write::file::{create_row_group, WriteOptions};
use crate::parquet_write::schema::{to_encodings, Partition};
use parquet2::compression::CompressionOptions;
use parquet2::metadata::{FileMetaData, KeyValue, SortingColumn};
use parquet2::read::{read_metadata_with_footer_bytes, read_metadata_with_size};
use parquet2::write;
use parquet2::write::footer_cache::FooterCache;
use parquet2::write::{ParquetFile, Version};
use std::fs::File;
use std::io::{Read as _, Seek, SeekFrom};

#[repr(C)]
pub struct ParquetUpdater {
    allocator: QdbAllocator,
    reader: File,
    read_file_size: u64,
    parquet_file: ParquetFile<File>,
    compression_options: CompressionOptions,
    row_group_size: Option<usize>,
    data_page_size: Option<usize>,
    raw_array_encoding: bool,
    file_metadata: FileMetaData,
    accumulated_unused_bytes: u64,
    old_footer_size: u64,
    is_rewrite: bool,
    result_file_size: u64,
    result_unused_bytes: u64,
}

impl ParquetUpdater {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allocator: QdbAllocator,
        mut reader: File,
        read_file_size: u64,
        writer: File,
        write_file_size: u64,
        sorting_columns: Option<Vec<SortingColumn>>,
        write_statistics: bool,
        raw_array_encoding: bool,
        compression_options: CompressionOptions,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
    ) -> ParquetResult<Self> {
        fn version_from(value: i32) -> ParquetResult<Version> {
            match value {
                1 => Ok(Version::V1),
                2 => Ok(Version::V2),
                _ => Err(fmt_err!(
                    InvalidLayout,
                    "invalid parquet version number: {value}"
                )),
            }
        }

        let is_rewrite = write_file_size == 0;

        let (metadata, footer_cache, old_footer_size) = if is_rewrite {
            let metadata = read_metadata_with_size(&mut reader, read_file_size)?;
            (metadata, None, 0u64)
        } else {
            // In update mode, also capture raw footer bytes for incremental serialization.
            let (metadata, raw_footer_bytes, footer_size) =
                read_metadata_with_footer_bytes(&mut reader, read_file_size)?;
            let cache = FooterCache::from_footer_bytes(raw_footer_bytes).map_err(|e| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(parquet2::error::Error::oos(e.to_string())),
                    "could not build footer cache",
                )
            })?;
            (metadata, Some(cache), footer_size)
        };

        let file_metadata = metadata.clone();

        // Validate that the file was written by QuestDB and has consistent metadata.
        // O3 merge relies on QuestDB-specific metadata (column types, symbol tables,
        // unused_bytes tracking) that external Parquet writers don't produce.
        let num_parquet_cols = metadata.schema_descr.columns().len();
        let qdb_meta = metadata.key_value_metadata.as_ref().and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == QDB_META_KEY)
                .and_then(|kv| kv.value.as_ref())
        });
        match qdb_meta {
            None => {
                return Err(fmt_err!(
                    InvalidLayout,
                    "parquet file lacks '{}' metadata key; O3 merge requires files written by QuestDB",
                    QDB_META_KEY
                ));
            }
            Some(raw) => {
                let meta = QdbMeta::deserialize(raw)?;
                if meta.schema.len() != num_parquet_cols {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "QuestDB metadata schema has {} columns but parquet schema has {}",
                        meta.schema.len(),
                        num_parquet_cols
                    ));
                }
            }
        }

        let version = version_from(metadata.version)?;
        let created_by = metadata.created_by.clone();
        let schema = metadata.schema_descr.clone();
        let options = write::WriteOptions { write_statistics, version };

        let (parquet_file, accumulated_unused_bytes) = if is_rewrite {
            // Rewrite mode: write to a fresh file
            let pf = ParquetFile::with_sorting_columns(
                writer,
                schema,
                options,
                created_by,
                sorting_columns,
            );
            (pf, 0u64)
        } else {
            // Update mode: append to existing file.
            // The upfront guard already validated QDB metadata exists and parses,
            // so unwrap_or(0) only covers the default for missing unused_bytes field.
            let accumulated_unused_bytes = metadata
                .key_value_metadata
                .as_ref()
                .and_then(|kvs| {
                    kvs.iter()
                        .find(|kv| kv.key == QDB_META_KEY)
                        .and_then(|kv| kv.value.as_ref())
                })
                .map(|raw| QdbMeta::deserialize(raw))
                .transpose()?
                .map(|m| m.unused_bytes)
                .unwrap_or(0);

            // Seek writer to end of file so new data is appended after existing content.
            // The reader and writer are separate fds; reading metadata only moves the reader cursor.
            let mut writer = writer;
            writer.seek(SeekFrom::Start(write_file_size))?;

            let pf = ParquetFile::new_updater(
                writer,
                write_file_size,
                schema,
                options,
                created_by,
                sorting_columns,
                metadata.into_thrift(),
                footer_cache,
            );
            (pf, accumulated_unused_bytes)
        };

        Ok(ParquetUpdater {
            allocator,
            reader,
            read_file_size,
            parquet_file,
            compression_options,
            raw_array_encoding,
            row_group_size,
            data_page_size,
            file_metadata,
            accumulated_unused_bytes,
            old_footer_size,
            is_rewrite,
            result_file_size: 0,
            result_unused_bytes: 0,
        })
    }

    pub fn replace_row_group(
        &mut self,
        partition: &Partition<'_>,
        row_group_id: i16,
    ) -> ParquetResult<()> {
        let options = self.row_group_options();
        let row_group = create_row_group(
            partition,
            0,
            partition.columns[0].row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            false,
        )?;

        if self.is_rewrite {
            self.parquet_file.write(row_group).with_context(|_| {
                format!("Failed to write row group {row_group_id} in rewrite mode")
            })
        } else {
            // Track the old row group's bytes that will become dead space.
            let rg_idx = row_group_id as usize;
            if rg_idx < self.file_metadata.row_groups.len() {
                let old_rg = &self.file_metadata.row_groups[rg_idx];
                self.accumulated_unused_bytes += old_rg.compressed_size() as u64;
                for col in old_rg.columns() {
                    if let Some(len) = col.column_index_length() {
                        self.accumulated_unused_bytes += len as u64;
                    }
                    if let Some(len) = col.offset_index_length() {
                        self.accumulated_unused_bytes += len as u64;
                    }
                }
            }

            self.parquet_file
                .replace(row_group, Some(row_group_id))
                .with_context(|_| format!("Failed to replace row group {row_group_id}"))
        }
    }

    pub fn insert_row_group(
        &mut self,
        partition: &Partition<'_>,
        position: i16,
    ) -> ParquetResult<()> {
        let options = self.row_group_options();
        let row_group = create_row_group(
            partition,
            0,
            partition.columns[0].row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            false,
        )?;

        if self.is_rewrite {
            self.parquet_file.write(row_group).with_context(|_| {
                format!("Failed to write row group at position {position} in rewrite mode")
            })
        } else {
            self.parquet_file
                .insert(row_group, position)
                .with_context(|_| format!("Failed to insert row group at position {position}"))
        }
    }

    pub fn copy_row_group(&mut self, rg_index: i16) -> ParquetResult<()> {
        let rg_idx = rg_index as usize;
        if rg_idx >= self.file_metadata.row_groups.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group: row group index {} out of range [0,{})",
                rg_idx,
                self.file_metadata.row_groups.len()
            ));
        }

        let old_rg = &self.file_metadata.row_groups[rg_idx];
        let columns_meta = old_rg.columns();

        // Determine the byte range covering column chunk data in this row group.
        // Column/offset indexes are stored separately (typically after all row groups)
        // and must not be included here, as that would copy data from other row groups.
        let mut rg_start = u64::MAX;
        let mut rg_end = 0u64;
        for col in columns_meta {
            let (start, len) = col.byte_range();
            rg_start = rg_start.min(start);
            rg_end = rg_end.max(start + len);
        }

        if rg_start >= rg_end {
            return Err(fmt_err!(
                InvalidLayout,
                "copy_row_group: empty byte range for row group {}",
                rg_idx
            ));
        }

        // Read the raw bytes from the reader file.
        let raw_len = (rg_end - rg_start) as usize;
        let mut raw_bytes = vec![0u8; raw_len];
        self.reader.seek(SeekFrom::Start(rg_start))?;
        self.reader.read_exact(&mut raw_bytes)?;

        // Ensure the PAR1 file header is written before computing offsets.
        // Without this, current_offset() returns 0, but write_raw_row_group()
        // would then write the 4-byte PAR1 header first, making the actual data
        // start at offset 4 while metadata offsets point to 0.
        self.parquet_file.ensure_started().map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "Failed to write file header before raw copy",
            )
        })?;

        // Build adjusted thrift metadata with offsets shifted to the new file position.
        let new_offset = self.parquet_file.current_offset();
        let offset_delta = new_offset as i64 - rg_start as i64;

        // Clone the row group metadata and convert to thrift, then adjust offsets.
        let mut thrift_rg = old_rg.clone().into_thrift();
        for col_chunk in &mut thrift_rg.columns {
            if let Some(ref mut meta) = col_chunk.meta_data {
                meta.data_page_offset += offset_delta;
                if let Some(ref mut dict_offset) = meta.dictionary_page_offset {
                    *dict_offset += offset_delta;
                }
                if let Some(ref mut idx_offset) = meta.index_page_offset {
                    *idx_offset += offset_delta;
                }
            }
            // Column/offset indexes are not copied with the row group data,
            // so clear their references to avoid dangling pointers into the old file.
            col_chunk.column_index_offset = None;
            col_chunk.column_index_length = None;
            col_chunk.offset_index_offset = None;
            col_chunk.offset_index_length = None;
        }
        if let Some(ref mut fo) = thrift_rg.file_offset {
            *fo += offset_delta;
        }

        self.parquet_file
            .write_raw_row_group(&raw_bytes, thrift_rg)
            .map_err(|s| {
                ParquetError::with_descr(
                    ParquetErrorReason::Parquet2(s),
                    format!("Failed to raw-copy row group {rg_idx}"),
                )
            })
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> ParquetResult<u64> {
        // Build updated QDB metadata with unused_bytes and pass it as KV metadata.
        let mut qdb_meta = self
            .file_metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| {
                kvs.iter()
                    .find(|kv| kv.key == QDB_META_KEY)
                    .and_then(|kv| kv.value.as_ref())
                    .and_then(|v| QdbMeta::deserialize(v).ok())
            })
            .unwrap_or_else(|| QdbMeta::new(0));

        if self.is_rewrite {
            qdb_meta.unused_bytes = 0;
        } else {
            // The old footer is now dead space.
            self.accumulated_unused_bytes += self.old_footer_size;
            qdb_meta.unused_bytes = self.accumulated_unused_bytes;
        }

        // After an O3 merge, row group sizes change but the file-level
        // column_top values remain stale from the original file.  The decoder
        // uses column_top together with the *new* accumulated row group sizes
        // to decide whether a row group is entirely before the column top and
        // can be skipped (returning null ptr).  Stale column_top values may
        // cause the decoder to incorrectly skip row groups that now contain
        // actual data (from merged O3 rows).
        //
        // All merged/inserted row groups already embed null sentinels in their
        // data with column_top=0, and copied row groups preserve their original
        // null definitions in the page data.  Zeroing the file-level column_top
        // is therefore safe: the decoder will read the (null) pages instead of
        // skipping them, which is correct albeit slightly less optimal.
        for col in &mut qdb_meta.schema {
            col.column_top = 0;
        }

        let qdb_meta_json = qdb_meta.serialize()?;
        let qdb_kv = KeyValue {
            key: QDB_META_KEY.to_string(),
            value: Some(qdb_meta_json),
        };

        self.result_unused_bytes = qdb_meta.unused_bytes;

        let mut kv = key_value_metadata.unwrap_or_default();
        kv.push(qdb_kv);

        let file_size = self.parquet_file.end(Some(kv)).map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "could not update parquet file",
            )
        })?;
        self.result_file_size = file_size;
        Ok(file_size)
    }

    pub fn result_unused_bytes(&self) -> u64 {
        self.result_unused_bytes
    }

    fn row_group_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.parquet_file.options().write_statistics,
            compression: self.compression_options,
            version: self.parquet_file.options().version,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
            raw_array_encoding: self.raw_array_encoding,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parquet::tests::ColumnTypeTagExt;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::compression::CompressionOptions;
    use parquet2::write::{ParquetFile, Version};
    use std::env;
    use std::error::Error;
    use std::fs::File;
    use std::io::Cursor;
    use std::io::Write;
    use std::ptr::null;

    use crate::parquet_write::file::{create_row_group, ParquetWriter, WriteOptions};
    use crate::parquet_write::schema::{to_encodings, to_parquet_schema, Column, Partition};

    use arrow::datatypes::ToByteSlice;
    use num_traits::float::FloatCore;
    use parquet2::read::read_metadata_with_size;
    use parquet2::write;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};

    fn save_to_file(bytes: &Bytes) {
        if let Ok(path) = env::var("OUT_PARQUET_FILE") {
            let mut file = File::create(path).expect("file create failed");
            file.write_all(bytes.to_byte_slice())
                .expect("file write failed");
        };
    }

    fn make_column<T>(name: &'static str, col_type: ColumnType, values: &[T]) -> Column<'static> {
        Column::from_raw_data(
            0,
            name,
            col_type.code(),
            0,
            values.len(),
            values.as_ptr() as *const u8,
            std::mem::size_of_val(values),
            null(),
            0,
            null(),
            0,
            false,
            false,
        )
        .unwrap()
    }

    #[test]
    fn append_replace_row_group() -> Result<(), Box<dyn Error>> {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i32, 2, i32::MIN, 3];
        let _expected1 = [Some(1i32), Some(2), None, Some(3)];
        let col2 = [0.5f32, 0.001, f32::nan(), 3.15];
        let _expected2 = [Some(0.5f32), Some(0.001), None, Some(3.15)];

        let col1_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1);
        let col2_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2);

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w, col2_w].to_vec(),
        };

        ParquetWriter::new(&mut buf)
            .finish(partition)
            .expect("parquet writer");

        let col1_extra = [4, 5, i32::MIN];
        let extra_expected1 = [Some(4i32), Some(5), None];
        let col2_extra = [f32::nan(), 3.13, std::f32::consts::PI];
        let extra_expected2 = [None, Some(3.13), Some(std::f32::consts::PI)];

        let col1_extra_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1_extra);
        let col2_extra_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2_extra);

        let new_partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_extra_w, col2_extra_w].to_vec(),
        };

        let orig_offset = buf.position();
        let metadata = read_metadata_with_size(&mut buf, orig_offset)?;

        let (schema, _) = to_parquet_schema(&new_partition, false)?;

        let foptions = WriteOptions {
            write_statistics: true,
            compression: CompressionOptions::Uncompressed,
            version: Version::V1,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
        };

        let options = write::WriteOptions { write_statistics: true, version: Version::V1 };

        let row_group = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions,
            false,
        )?;

        let replace_row_group = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions,
            false,
        )?;

        let orig_offset = buf.position();
        let metadata = read_metadata_with_size(&mut buf, orig_offset)?;
        let created_by = metadata.created_by.clone();

        let mut parquet_file = ParquetFile::new_updater(
            &mut buf,
            orig_offset,
            schema,
            options,
            created_by,
            None,
            metadata.into_thrift(),
            None,
        );
        parquet_file.replace(row_group, None)?;
        parquet_file.replace(replace_row_group, Some(0))?;
        parquet_file.end(None)?;

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        save_to_file(&bytes);
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let i32array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("Failed to downcast");
            let collected: Vec<_> = i32array.iter().collect();
            assert_eq!(
                &collected,
                &extra_expected1
                    .iter()
                    .chain(extra_expected1.iter())
                    .cloned()
                    .collect::<Vec<_>>()
            );
            let f32array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("Failed to downcast");
            let collected: Vec<_> = f32array.iter().collect();
            assert_eq!(
                &collected,
                &extra_expected2
                    .iter()
                    .chain(extra_expected2.iter())
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
        Ok(())
    }
}
