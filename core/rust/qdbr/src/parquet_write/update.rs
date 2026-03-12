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
use crate::allocator::QdbAllocator;
use crate::parquet::error::{ParquetError, ParquetErrorExt, ParquetErrorReason, ParquetResult};
use crate::parquet_write::file::{create_row_group, WriteOptions};
use crate::parquet_write::schema::to_compressions;
use crate::parquet_write::schema::{to_encodings, Partition};
use parquet2::compression::CompressionOptions;
use parquet2::metadata::{KeyValue, SortingColumn};
use parquet2::read::read_metadata_with_size;
use parquet2::write;
use parquet2::write::{ParquetFile, Version};
use std::collections::HashSet;
use std::fs::File;

#[repr(C)]
pub struct ParquetUpdater {
    allocator: QdbAllocator,
    parquet_file: ParquetFile<File>,
    compression_options: CompressionOptions,
    row_group_size: Option<usize>,
    data_page_size: Option<usize>,
    raw_array_encoding: bool,
    bloom_filter_columns: HashSet<usize>,
    min_compression_ratio: f64,
}

impl ParquetUpdater {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        allocator: QdbAllocator,
        mut reader: File,
        file_size: u64,
        sorting_columns: Option<Vec<SortingColumn>>,
        write_statistics: bool,
        raw_array_encoding: bool,
        compression_options: CompressionOptions,
        row_group_size: Option<usize>,
        data_page_size: Option<usize>,
        bloom_filter_fpp: f64,
        min_compression_ratio: f64,
    ) -> ParquetResult<Self> {
        fn version_from_i32(value: i32) -> ParquetResult<Version> {
            match value {
                1 => Ok(Version::V1),
                2 => Ok(Version::V2),
                _ => Err(ParquetError::with_descr(
                    ParquetErrorReason::Unsupported,
                    format!("unsupported parquet version number: {value}"),
                )),
            }
        }

        let metadata = read_metadata_with_size(&mut reader, file_size)?;
        let version = version_from_i32(metadata.version)?;
        let created_by = metadata.created_by.clone();
        let schema = metadata.schema_descr.clone();

        let bloom_filter_columns = if let Some(first_rg) = metadata.row_groups.first() {
            first_rg
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, col)| {
                    if col.metadata().bloom_filter_offset.is_some() {
                        Some(i)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            HashSet::new()
        };

        let options = write::WriteOptions { write_statistics, version, bloom_filter_fpp };
        let parquet_file = ParquetFile::new_updater(
            reader,
            file_size,
            schema,
            options,
            created_by,
            sorting_columns,
            metadata.into_thrift(),
        );

        Ok(ParquetUpdater {
            allocator,
            parquet_file,
            compression_options,
            raw_array_encoding,
            row_group_size,
            data_page_size,
            bloom_filter_columns,
            min_compression_ratio,
        })
    }

    pub fn replace_row_group(
        &mut self,
        partition: &Partition,
        row_group_id: i16,
    ) -> ParquetResult<()> {
        let options = self.row_group_options();
        let (row_group, bloom_hashes) = create_row_group(
            partition,
            0,
            partition.columns[0].row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            &to_compressions(partition),
            false,
        )?;

        self.parquet_file
            .replace(row_group, Some(row_group_id), &bloom_hashes)
            .with_context(|_| format!("Failed to replace row group {row_group_id}"))
    }

    pub fn append_row_group(&mut self, partition: &Partition) -> ParquetResult<()> {
        let options = self.row_group_options();
        let (row_group, bloom_hashes) = create_row_group(
            partition,
            0,
            partition.columns[0].row_count,
            self.parquet_file.schema().fields(),
            &to_encodings(partition),
            options,
            &to_compressions(partition),
            false,
        )?;

        self.parquet_file
            .append(row_group, &bloom_hashes)
            .context("Failed to append row group")
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> ParquetResult<u64> {
        self.parquet_file.end(key_value_metadata).map_err(|s| {
            ParquetError::with_descr(
                ParquetErrorReason::Parquet2(s),
                "could not update parquet file",
            )
        })
    }

    fn row_group_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.parquet_file.options().write_statistics,
            compression: self.compression_options,
            version: self.parquet_file.options().version,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
            raw_array_encoding: self.raw_array_encoding,
            bloom_filter_columns: self.bloom_filter_columns.clone(),
            bloom_filter_fpp: self.parquet_file.options().bloom_filter_fpp,
            min_compression_ratio: self.min_compression_ratio,
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
    use std::collections::HashSet;
    use std::env;
    use std::error::Error;
    use std::fs::File;
    use std::io::Cursor;
    use std::io::Write;
    use std::ptr::null;

    use crate::parquet_write::file::DEFAULT_BLOOM_FILTER_FPP;
    use crate::parquet_write::file::{create_row_group, ParquetWriter, WriteOptions};
    use crate::parquet_write::schema::{
        to_compressions, to_encodings, to_parquet_schema, Column, Partition,
    };

    use arrow::datatypes::ToByteSlice;
    use num_traits::float::FloatCore;
    use parquet2::compression::Compression;
    use parquet2::read::read_metadata_with_size;
    use parquet2::write;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use tempfile::NamedTempFile;

    fn save_to_file(bytes: &Bytes) {
        if let Ok(path) = env::var("OUT_PARQUET_FILE") {
            let mut file = File::create(path).expect("file create failed");
            file.write_all(bytes.to_byte_slice())
                .expect("file write failed");
        };
    }

    fn make_column<T>(name: &'static str, col_type: ColumnType, values: &[T]) -> Column {
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
            0,
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
            bloom_filter_columns: HashSet::new(),
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            min_compression_ratio: 0.0,
        };

        let options = write::WriteOptions {
            write_statistics: true,
            version: Version::V1,
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
        };

        let (row_group, bloom_hashes) = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions.clone(),
            &to_compressions(&new_partition),
            false,
        )?;

        let (replace_row_group, replace_bloom_hashes) = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions,
            &to_compressions(&new_partition),
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
        );
        parquet_file.append(row_group, &bloom_hashes)?;
        parquet_file.replace(replace_row_group, Some(0), &replace_bloom_hashes)?;
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

    /// Write an initial compressed parquet file to a temp file and return it.
    fn write_initial_zstd_file() -> Result<(NamedTempFile, Partition), Box<dyn Error>> {
        let col1 = [1i32, 2, i32::MIN, 3];
        let col2 = [0.5f32, 0.001, f32::nan(), 3.15];
        let col1_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1);
        let col2_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2);
        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w, col2_w].to_vec(),
        };

        let tmp = NamedTempFile::new()?;
        let file = tmp.reopen()?;
        ParquetWriter::new(file)
            .with_compression(CompressionOptions::Zstd(None))
            .finish(partition)?;

        // Build the partition for appending (same schema, fresh data).
        let col1_extra = [4, 5, i32::MIN];
        let col2_extra = [f32::nan(), 3.13, std::f32::consts::PI];
        let col1_extra_w = make_column("col1", ColumnTypeTag::Int.into_type(), &col1_extra);
        let col2_extra_w = make_column("col2", ColumnTypeTag::Float.into_type(), &col2_extra);
        let new_partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_extra_w, col2_extra_w].to_vec(),
        };

        Ok((tmp, new_partition))
    }

    #[test]
    fn test_updater_with_min_compression_ratio() -> Result<(), Box<dyn Error>> {
        use crate::allocator::TestAllocatorState;

        // --- Case 1: very high min_compression_ratio forces fallback to uncompressed ---
        {
            let (tmp, new_partition) = write_initial_zstd_file()?;
            let file_len = tmp.as_file().metadata()?.len();
            let reader = tmp.reopen()?;
            let alloc_state = TestAllocatorState::new();

            let mut updater = super::ParquetUpdater::new(
                alloc_state.allocator(),
                reader,
                file_len,
                None,                                   // sorting_columns
                true,                                    // write_statistics
                false,                                   // raw_array_encoding
                CompressionOptions::Zstd(None),          // compression
                None,                                    // row_group_size
                None,                                    // data_page_size
                DEFAULT_BLOOM_FILTER_FPP,                // bloom_filter_fpp
                100.0,                                   // min_compression_ratio (impossibly high)
            )?;

            updater.append_row_group(&new_partition)?;
            updater.end(None)?;

            // Read back metadata and check the appended row group (index 1).
            let verify_file = tmp.reopen()?;
            let verify_len = verify_file.metadata()?.len();
            let metadata = read_metadata_with_size(&mut &verify_file, verify_len)?;
            assert_eq!(metadata.row_groups.len(), 2, "expected 2 row groups");

            // The appended row group should have fallen back to Uncompressed
            // because the ratio threshold (100.0) is impossibly high.
            let appended_rg = &metadata.row_groups[1];
            for col in appended_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Uncompressed,
                    "expected uncompressed fallback for column {:?}",
                    col.descriptor().path_in_schema,
                );
            }

            // Original row group should still be Zstd (it was written before
            // the updater applied its ratio check).
            let original_rg = &metadata.row_groups[0];
            for col in original_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Zstd,
                    "original row group column should remain Zstd",
                );
            }
        }

        // --- Case 2: low min_compression_ratio keeps compressed output ---
        {
            let (tmp, new_partition) = write_initial_zstd_file()?;
            let file_len = tmp.as_file().metadata()?.len();
            let reader = tmp.reopen()?;
            let alloc_state = TestAllocatorState::new();

            let mut updater = super::ParquetUpdater::new(
                alloc_state.allocator(),
                reader,
                file_len,
                None,
                true,
                false,
                CompressionOptions::Zstd(None),
                None,
                None,
                DEFAULT_BLOOM_FILTER_FPP,
                0.5, // min_compression_ratio: ratio check active but easily met
            )?;

            updater.append_row_group(&new_partition)?;
            updater.end(None)?;

            let verify_file = tmp.reopen()?;
            let verify_len = verify_file.metadata()?.len();
            let metadata = read_metadata_with_size(&mut &verify_file, verify_len)?;
            assert_eq!(metadata.row_groups.len(), 2);

            // The appended row group should keep Zstd because the ratio
            // threshold (0.5) is trivially satisfied — it only requires
            // uncompressed/compressed >= 0.5.
            let appended_rg = &metadata.row_groups[1];
            for col in appended_rg.columns() {
                assert_eq!(
                    col.compression(),
                    Compression::Zstd,
                    "expected Zstd compression to be kept for column {:?}",
                    col.descriptor().path_in_schema,
                );
            }
        }

        Ok(())
    }
}
