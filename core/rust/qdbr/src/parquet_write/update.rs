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
use crate::parquet::error::{ParquetError, ParquetErrorExt, ParquetErrorReason, ParquetResult};
use crate::parquet_write::file::{create_row_group, WriteOptions};
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
    ) -> ParquetResult<Self> {
        fn from(value: i32) -> Version {
            match value {
                1 => Version::V1,
                2 => Version::V2,
                _ => panic!("Invalid version number: {value}"),
            }
        }

        let metadata = read_metadata_with_size(&mut reader, file_size)?;
        let version = from(metadata.version);
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
            bloom_filter_fpp: 0.01,
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
            bloom_filter_fpp: 0.01,
        };

        let options = write::WriteOptions {
            write_statistics: true,
            version: Version::V1,
            bloom_filter_fpp: 0.01,
        };

        let (row_group, bloom_hashes) = create_row_group(
            &new_partition,
            0,
            col1_extra.len(),
            metadata.schema_descr.fields(),
            &to_encodings(&new_partition),
            foptions.clone(),
            false,
        )?;

        let (replace_row_group, replace_bloom_hashes) = create_row_group(
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
}
