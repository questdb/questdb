use num_traits::AsPrimitive;
use qdb_core::col_type::nulls;

pub mod array;
mod binary;
mod boolean;
pub(crate) mod decimal;
pub(crate) mod file;
pub use file::ParquetWriter;
mod fixed_len_bytes;
mod jni;
mod primitive;
pub mod schema;
pub mod simd;
mod string;
mod symbol;
mod update;
mod util;
pub mod varchar;

#[doc(hidden)]
pub mod bench {
    pub use super::array::array_to_raw_page;
    pub use super::binary::{binary_to_dict_pages, binary_to_page};
    pub use super::boolean::slice_to_page as boolean_to_page;
    pub use super::file::WriteOptions;
    pub use super::fixed_len_bytes::{bytes_to_dict_pages, bytes_to_page};
    pub use super::primitive::{
        decimal_slice_to_dict_pages, int_slice_to_dict_pages_notnull,
        int_slice_to_dict_pages_nullable, int_slice_to_page_notnull, int_slice_to_page_nullable,
        slice_to_dict_pages_simd, slice_to_page_simd,
    };
    pub use super::string::{string_to_dict_pages, string_to_page};
    pub use super::symbol::symbol_to_pages;
    pub use super::varchar::{varchar_to_dict_pages, varchar_to_page};
}

/// Trait for detecting QuestDB's in-band null sentinel values in fixed-size columns.
///
/// QuestDB does not use a separate validity bitmap for columnar data. Instead, each
/// fixed-size type reserves a specific sentinel value to represent NULL. The canonical
/// definitions live in [`qdb_core::col_type::nulls`].
///
/// ## Sentinel values by type
///
/// | QuestDB type    | Rust type | Sentinel                   |
/// |-----------------|-----------|----------------------------|
/// | Int             | `i32`     | `i32::MIN`                 |
/// | Long / Timestamp| `i64`     | `i64::MIN`                 |
/// | Float           | `f32`     | `NaN`                      |
/// | Double          | `f64`     | `NaN`                      |
/// | GeoHash(byte)   | `i8`      | `-1`                       |
/// | GeoHash(short)  | `i16`     | `-1`                       |
/// | GeoHash(int)    | `i32`     | `-1`                       |
/// | GeoHash(long)   | `i64`     | `-1`                       |
/// | IPv4            | `i32`     | `0`                        |
/// | Decimal128      | `(i64, u64)` | `(i64::MIN, 0)`        |
/// | Decimal256      | 4×i64     | `(i64::MIN, 0, 0, 0)`     |
///
/// **Non-nullable types** (Boolean, Byte, Short, Char) use `Repetition::Required`
/// in the Parquet schema and do not implement this trait.
///
/// **Variable-size types** (String, Binary, Varchar) detect nulls via length-based
/// checks rather than sentinel values, so they also do not implement this trait.
pub trait Nullable {
    fn is_null(&self) -> bool;
}

impl Nullable for i32 {
    fn is_null(&self) -> bool {
        *self == nulls::INT
    }
}

impl Nullable for i64 {
    fn is_null(&self) -> bool {
        *self == nulls::LONG
    }
}

impl Nullable for f32 {
    fn is_null(&self) -> bool {
        self.is_nan()
    }
}

impl Nullable for f64 {
    fn is_null(&self) -> bool {
        self.is_nan()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GeoByte(i8);

impl Nullable for GeoByte {
    fn is_null(&self) -> bool {
        self.0 == nulls::GEOHASH_BYTE
    }
}

impl AsPrimitive<i32> for GeoByte {
    fn as_(self) -> i32 {
        self.0 as i32
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GeoShort(i16);

impl Nullable for GeoShort {
    fn is_null(&self) -> bool {
        self.0 == nulls::GEOHASH_SHORT
    }
}

impl AsPrimitive<i32> for GeoShort {
    fn as_(self) -> i32 {
        self.0 as i32
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GeoInt(i32);

impl Nullable for GeoInt {
    fn is_null(&self) -> bool {
        self.0 == nulls::GEOHASH_INT
    }
}

impl AsPrimitive<i32> for GeoInt {
    fn as_(self) -> i32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GeoLong(i64);

impl Nullable for GeoLong {
    fn is_null(&self) -> bool {
        self.0 == nulls::GEOHASH_LONG
    }
}

impl AsPrimitive<i64> for GeoLong {
    fn as_(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IPv4(i32);

impl Nullable for IPv4 {
    fn is_null(&self) -> bool {
        self.0 == nulls::IPV4
    }
}

impl AsPrimitive<i32> for IPv4 {
    fn as_(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema;
    use crate::parquet_write::schema::{Column, Partition};
    use arrow::array::Array;
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use num_traits::Float;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::deserialize::{HybridEncoded, HybridRleIter};
    use parquet2::encoding::{hybrid_rle, uleb128};
    use parquet2::metadata::SortingColumn;
    use parquet2::page::CompressedPage;
    use parquet2::types;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use std::env;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::ptr::null;
    use std::time::Instant;

    #[test]
    fn test_write_parquet_2m_rows() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 1_000_000;
        let fix_col_count = 3;

        let buffers: Vec<Vec<i32>> = (0..fix_col_count)
            .map(|_| (0..row_count).collect())
            .collect();

        let columns: Vec<Column> = buffers
            .iter()
            .enumerate()
            .map(|(i, buffer)| {
                let column_name = format!("col{}", i);
                let name: &'static str = Box::leak(column_name.into_boxed_str());
                Column::from_raw_data(
                    i as i32,
                    name,
                    ColumnTypeTag::Int.into_type().code(),
                    0,
                    row_count as usize,
                    buffer.as_ptr() as *const u8,
                    buffer.len() * size_of::<i32>(),
                    null(),
                    0,
                    null(),
                    0,
                    false,
                    false,
                    0,
                )
                .expect("column")
            })
            .collect();

        let partition = Partition { table: "test_table".to_string(), columns };

        // Measure the start time
        let start = Instant::now();
        ParquetWriter::new(&mut buf)
            .with_statistics(false)
            .with_row_group_size(Some(1048576))
            .with_data_page_size(Some(1048576))
            .finish(partition)
            .expect("parquet writer");

        // Measure the end time
        let duration = start.elapsed();
        println!("finished writing in: {:?}", duration);

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        save_to_file(bytes);
    }

    #[test]
    fn test_write_parquet_with_symbol_column() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = vec![0, 1, i32::MIN, 2, 4];
        let (col_chars, offsets) =
            serialize_as_symbols(vec!["foo", "bar", "baz", "notused", "plus"]);

        serialize_to_parquet(&mut buf, col1, col_chars, offsets);
        let expected = vec![Some("foo"), Some("bar"), None, Some("baz"), Some("plus")];

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let symbol_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("Failed to downcast");
            let collected: Vec<_> = symbol_array.iter().collect();
            assert_eq!(collected, expected);
        }

        save_to_file(bytes);
    }

    #[test]
    fn test_write_parquet_with_symbol_column_all_values_nulls() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = vec![i32::MIN, i32::MIN, i32::MIN, i32::MIN, i32::MIN];
        let (col_chars, offsets) = serialize_as_symbols(vec![]);

        serialize_to_parquet(&mut buf, col1, col_chars, offsets);

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let symbol_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("Failed to downcast");
            let collected: Vec<_> = symbol_array.iter().collect();
            let expected = vec![None, None, None, None, None];
            assert_eq!(collected, expected);
        }

        save_to_file(bytes);
    }

    fn serialize_to_parquet(
        mut buf: &mut Cursor<Vec<u8>>,
        col1: Vec<i32>,
        col_chars: Vec<u8>,
        offsets: Vec<u64>,
    ) {
        assert_eq!(
            ColumnTypeTag::Symbol,
            ColumnType::try_from(12).expect("fail").tag()
        );
        let col1_w = Column::from_raw_data(
            0,
            "col1",
            12,
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            col_chars.as_ptr(),
            col_chars.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w].to_vec(),
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(false)
            .finish(partition)
            .expect("parquet writer");
    }

    fn save_to_file(bytes: Bytes) {
        if let Ok(path) = env::var("OUT_PARQUET_FILE") {
            let mut file = File::create(path).expect("file create failed");
            file.write_all(bytes.to_byte_slice())
                .expect("file write failed");
        };
    }

    fn serialize_as_symbols(symbol_chars: Vec<&str>) -> (Vec<u8>, Vec<u64>) {
        let mut chars = vec![];
        let mut offsets = vec![];

        for s in symbol_chars {
            let sym_chars: Vec<_> = s.encode_utf16().collect();
            let len = sym_chars.len();
            offsets.push(chars.len() as u64);
            chars.extend_from_slice(&(len as u32).to_le_bytes());
            // SAFETY: Reinterprets a contiguous `&[u16]` as raw bytes for testing.
            // Valid because slices are contiguous and `u8` has no alignment requirement.
            let encoded: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    sym_chars.as_ptr() as *const u8,
                    sym_chars.len() * size_of::<u16>(),
                )
            };
            chars.extend_from_slice(encoded);
        }

        (chars, offsets)
    }

    #[test]
    fn test_write_parquet_with_fixed_sized_columns() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i32, 2, i32::MIN, 3];
        let expected1 = [Some(1i32), Some(2), None, Some(3)];
        let col2 = [0.5f32, 0.001, f32::nan(), 3.15];
        let expected2 = [Some(0.5f32), Some(0.001), None, Some(3.15)];

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            5,
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let col2_w = Column::from_raw_data(
            0,
            "col2",
            9,
            0,
            col2.len(),
            col2.as_ptr() as *const u8,
            col2.len() * size_of::<f32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w, col2_w].to_vec(),
        };

        ParquetWriter::new(&mut buf)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
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
            assert_eq!(collected, expected1);
            let f32array = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("Failed to downcast");
            let collected: Vec<_> = f32array.iter().collect();
            assert_eq!(collected, expected2);
        }
    }

    #[test]
    fn test_write_parquet_row_group_size_data_page_size() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 100_000usize;
        let row_group_size = 500usize;
        let page_size_bytes = 256usize;
        let col1: Vec<i64> = (0..row_count).map(|v| v as i64).collect();
        let col1_w = Column::from_raw_data(
            0,
            "col1",
            6,
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w].to_vec(),
        };

        ParquetWriter::new(&mut buf)
            .with_row_group_size(Some(row_group_size))
            .with_data_page_size(Some(page_size_bytes))
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut reader = Cursor::new(bytes.clone());
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        let mut expected = 0;
        for batch in parquet_reader.flatten() {
            let i64array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("Failed to downcast");
            for v in i64array.iter() {
                assert!(v.is_some());
                let v = v.unwrap();
                assert_eq!(expected, v);
                expected += 1;
            }
        }
        assert_eq!(expected, row_count as i64);

        let meta = parquet2::read::read_metadata(&mut reader).expect("metadata");
        assert_eq!(row_count, meta.num_rows);
        assert_eq!(row_count / row_group_size, meta.row_groups.len());
        assert_eq!(row_group_size, meta.row_groups[0].num_rows());

        let chunk_meta = &meta.row_groups[0].columns()[0];
        let max_page_size = page_size_bytes + 20;
        let pages =
            parquet2::read::get_page_iterator(chunk_meta, reader, None, vec![], max_page_size)
                .expect("pages iter");
        for page in pages {
            let page = page.expect("page");
            match page {
                CompressedPage::Data(data) => {
                    let uncompressed = data.uncompressed_size();
                    assert!(uncompressed <= max_page_size);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn encode_column_tops() {
        let def_level_count: usize = 113_000_000;
        let mut buffer = vec![];
        let mut bb = [0u8; 10];
        let len = uleb128::encode((def_level_count << 1) as u64, &mut bb);
        buffer.extend_from_slice(&bb[..len]);
        buffer.extend_from_slice(&[1u8]);

        // assert!(encode_iter(&mut buffer, std::iter::repeat(true).take(def_level_count), Version::V1).is_ok());

        let iter = hybrid_rle::Decoder::new(buffer.as_slice(), 1);
        let iter = HybridRleIter::new(iter, def_level_count);
        for el in iter {
            assert!(el.is_ok());
            let he = el.unwrap();
            match he {
                HybridEncoded::Repeated(val, len) => {
                    assert!(val);
                    assert_eq!(len, def_level_count);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn decode_len() {
        let data = [
            1u8, 0, 0, 0, 65, 0, 1, 0, 0, 0, 67, 0, 1, 0, 0, 0, 67, 0, 1, 0, 0, 0, 65, 0, 1, 0, 0,
            0, 67, 0, 1, 0, 0, 0, 65, 0, 1, 0, 0, 0, 65, 0, 1, 0, 0, 0, 65, 0, 1, 0, 0, 0, 66, 0,
            1, 0, 0, 0, 65, 0,
        ];
        let len = types::decode::<i32>(&data[0..4]);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_write_parquet_with_designated_timestamp_descending() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let timestamps: Vec<i64> = vec![1000, 2000, 3000, 4000, 5000];
        let row_count = timestamps.len();
        let ts_col = Column::from_raw_data(
            0,
            "timestamp",
            ColumnTypeTag::Timestamp.into_type().code(),
            0,
            row_count,
            timestamps.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            true,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![ts_col],
        };

        let sorting_columns = Some(vec![SortingColumn::new(0, true, false)]); // descending=true
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_sorting_columns(sorting_columns)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut reader = Cursor::new(bytes);
        let meta = parquet2::read::read_metadata(&mut reader).expect("metadata");

        assert!(!meta.row_groups.is_empty());
        let sorting_cols = meta.row_groups[0].sorting_columns();
        assert!(
            sorting_cols.is_some(),
            "Expected sorting columns in metadata"
        );
        let sorting_cols = sorting_cols.as_ref().unwrap();
        assert_eq!(sorting_cols.len(), 1);
        assert_eq!(sorting_cols[0].column_idx, 0);
        assert!(
            sorting_cols[0].descending,
            "Expected descending=true for timestamp column"
        );
        assert!(!sorting_cols[0].nulls_first);
    }

    #[test]
    fn test_write_parquet_with_designated_timestamp_ascending() {
        use parquet2::metadata::SortingColumn;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let timestamps: Vec<i64> = vec![1000, 2000, 3000, 4000, 5000];
        let row_count = timestamps.len();

        let ts_col = Column::from_raw_data(
            0,
            "timestamp",
            ColumnTypeTag::Timestamp.into_type().code(),
            0,
            row_count,
            timestamps.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            true,
            true,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![ts_col],
        };

        let sorting_columns = Some(vec![SortingColumn::new(0, false, false)]); // descending=false
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_sorting_columns(sorting_columns)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut reader = Cursor::new(bytes);
        let meta = parquet2::read::read_metadata(&mut reader).expect("metadata");

        assert!(!meta.row_groups.is_empty());
        let sorting_cols = meta.row_groups[0].sorting_columns();
        assert!(
            sorting_cols.is_some(),
            "Expected sorting columns in metadata"
        );
        let sorting_cols = sorting_cols.as_ref().unwrap();
        assert_eq!(sorting_cols.len(), 1);
        assert_eq!(sorting_cols[0].column_idx, 0);
        assert!(
            !sorting_cols[0].descending,
            "Expected descending=false for ascending timestamp"
        );
        assert!(!sorting_cols[0].nulls_first);
    }

    #[test]
    fn test_write_row_group_from_partitions_symbol_non_parallel() {
        test_write_row_group_from_partitions_symbol(false);
    }

    #[test]
    fn test_write_row_group_from_partitions_symbol_parallel() {
        test_write_row_group_from_partitions_symbol(true);
    }

    fn test_write_row_group_from_partitions_symbol(parallel: bool) {
        use crate::parquet_write::schema::{to_encodings, to_parquet_schema};
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let (col_chars, offsets) =
            serialize_as_symbols(vec!["apple", "banana", "cherry", "date", "elderberry"]);

        // Partition 1: keys [0, 1, 2] -> "apple", "banana", "cherry"
        let keys1 = [0i32, 1, 2];
        // Partition 2: keys [null, 3, 4] -> null, "date", "elderberry"
        let keys2 = [i32::MIN, 3, 4];
        // Partition 3: keys [1, null, 0] -> "banana", null, "apple"
        let keys3 = [1i32, i32::MIN, 0];
        let col1 = Column::from_raw_data(
            0,
            "sym",
            12,
            0,
            keys1.len(),
            keys1.as_ptr() as *const u8,
            keys1.len() * size_of::<i32>(),
            col_chars.as_ptr(),
            col_chars.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
            0,
        )
        .unwrap();

        let col2 = Column::from_raw_data(
            0,
            "sym",
            12,
            0,
            keys2.len(),
            keys2.as_ptr() as *const u8,
            keys2.len() * size_of::<i32>(),
            col_chars.as_ptr(),
            col_chars.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
            0,
        )
        .unwrap();

        let col3 = Column::from_raw_data(
            0,
            "sym",
            12,
            0,
            keys3.len(),
            keys3.as_ptr() as *const u8,
            keys3.len() * size_of::<i32>(),
            col_chars.as_ptr(),
            col_chars.len(),
            offsets.as_ptr(),
            offsets.len(),
            false,
            false,
            0,
        )
        .unwrap();

        let partition1 = Partition {
            table: "test_table".to_string(),
            columns: vec![col1],
        };
        let partition2 = Partition {
            table: "test_table".to_string(),
            columns: vec![col2],
        };
        let partition3 = Partition {
            table: "test_table".to_string(),
            columns: vec![col3],
        };

        let (schema, additional_meta) = to_parquet_schema(&partition1, false, -1).unwrap();
        let encodings = to_encodings(&partition1);

        let mut chunked = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_parallel(parallel)
            .chunked(schema, encodings)
            .unwrap();

        let partitions: Vec<&Partition> = vec![&partition1, &partition2, &partition3];
        chunked
            .write_row_group_from_partitions(&partitions, 0, keys3.len())
            .unwrap();
        chunked.finish(additional_meta).unwrap();

        // Verify output
        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        let expected = vec![
            Some("apple"),
            Some("banana"),
            Some("cherry"),
            None,
            Some("date"),
            Some("elderberry"),
            Some("banana"),
            None,
            Some("apple"),
        ];

        for batch in parquet_reader.flatten() {
            let symbol_array = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .expect("Failed to downcast");
            let collected: Vec<_> = symbol_array.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_per_column_encoding_config() {
        // Create two int columns: one with default encoding (Plain), one with DeltaBinaryPacked
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1_data: Vec<i32> = (0..100).collect();
        let col2_data: Vec<i32> = (0..100).collect();

        // Pack config: encoding=4 (DeltaBinaryPacked), compression=0, level=0, explicit flag set
        let delta_binary_config = schema::ParquetEncodingConfig::new(4, 0, 0).raw();

        let col1 = Column::from_raw_data(
            0,
            "col_plain",
            ColumnTypeTag::Int.into_type().code(),
            0,
            100,
            col1_data.as_ptr() as *const u8,
            col1_data.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0, // default encoding
        )
        .unwrap();

        let col2 = Column::from_raw_data(
            1,
            "col_delta",
            ColumnTypeTag::Int.into_type().code(),
            0,
            100,
            col2_data.as_ptr() as *const u8,
            col2_data.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            delta_binary_config,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1, col2],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Read back and verify encodings
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");

        let row_group = &metadata.row_groups[0];

        // col_plain should use Plain encoding (default for Int)
        let col0_chunk = &row_group.columns()[0];
        let col0_encoding = col0_chunk.column_encoding();
        // parquet_format_safe::Encoding uses i32 constants: PLAIN=0, DELTA_BINARY_PACKED=5
        assert!(
            col0_encoding.iter().any(|e| e.0 == 0), // PLAIN = 0
            "col_plain should use Plain encoding, got: {:?}",
            col0_encoding
        );

        // col_delta should use DeltaBinaryPacked encoding
        let col1_chunk = &row_group.columns()[1];
        let col1_encoding = col1_chunk.column_encoding();
        assert!(
            col1_encoding.iter().any(|e| e.0 == 5), // DELTA_BINARY_PACKED = 5
            "col_delta should use DeltaBinaryPacked encoding, got: {:?}",
            col1_encoding
        );

        // Verify data is correct by reading with arrow
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr0 = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("downcast");
            let collected: Vec<_> = arr0.iter().collect();
            let expected: Vec<_> = (0..100).map(Some).collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_per_column_compression_config() {
        // Create two double columns: one uncompressed, one with Snappy compression
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1_data: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();
        let col2_data: Vec<f64> = (0..1000).map(|i| i as f64 * 0.1).collect();

        // Pack config: encoding=0 (default), compression=2 (Snappy), level=0, explicit flag set
        let snappy_config = schema::ParquetEncodingConfig::new(0, 2, 0).raw();

        let col1 = Column::from_raw_data(
            0,
            "col_uncompressed",
            ColumnTypeTag::Double.into_type().code(),
            0,
            1000,
            col1_data.as_ptr() as *const u8,
            col1_data.len() * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0, // default (uncompressed, since global is uncompressed)
        )
        .unwrap();

        let col2 = Column::from_raw_data(
            1,
            "col_snappy",
            ColumnTypeTag::Double.into_type().code(),
            0,
            1000,
            col2_data.as_ptr() as *const u8,
            col2_data.len() * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            snappy_config,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1, col2],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Read metadata and verify compression
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");

        let row_group = &metadata.row_groups[0];

        // col_uncompressed should use no compression
        let col0_compression = row_group.columns()[0].compression();
        assert_eq!(
            col0_compression,
            parquet2::compression::Compression::Uncompressed,
            "col_uncompressed should be uncompressed"
        );

        // col_snappy should use Snappy compression
        let col1_compression = row_group.columns()[1].compression();
        assert_eq!(
            col1_compression,
            parquet2::compression::Compression::Snappy,
            "col_snappy should use Snappy compression"
        );

        // Verify data is correct
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr0 = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("downcast");
            assert_eq!(arr0.len(), 1000);
            // Check first few values
            for i in 0..10 {
                assert!((arr0.value(i) - i as f64 * 0.1).abs() < 1e-10);
            }
        }
    }

    /// Helper: pack RleDictionary encoding config (encoding=2, explicit flag set)
    fn rle_dict_config() -> i32 {
        2 | (1 << 24)
    }

    #[test]
    fn test_dict_encoding_int_column() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i32, 2, i32::MIN, 3, 1, 2, 3, i32::MIN, 1, 2];
        let expected: Vec<Option<i32>> = col1
            .iter()
            .map(|&v| if v == i32::MIN { None } else { Some(v) })
            .collect();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Int.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify encoding is RLE_DICTIONARY
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let col_encoding = metadata.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_encoding.iter().any(|e| e.0 == 8), // RLE_DICTIONARY = 8
            "expected RLE_DICTIONARY encoding, got: {:?}",
            col_encoding
        );

        // Verify data is correct
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_dict_encoding_long_column() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [100i64, 200, i64::MIN, 300, 100, 200];
        let expected: Vec<Option<i64>> = col1
            .iter()
            .map(|&v| if v == i64::MIN { None } else { Some(v) })
            .collect();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Long.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_dict_encoding_double_column() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1.5f64, 2.5, f64::NAN, 3.5, 1.5, 2.5];
        let expected_some = [Some(1.5), Some(2.5), None, Some(3.5), Some(1.5), Some(2.5)];

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Double.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .expect("downcast");
            for (i, expected) in expected_some.iter().enumerate() {
                match expected {
                    Some(v) => {
                        assert!(!arr.is_null(i));
                        assert!((arr.value(i) - v).abs() < 1e-10);
                    }
                    None => {
                        assert!(arr.is_null(i));
                    }
                }
            }
        }
    }

    #[test]
    fn test_dict_encoding_byte_notnull_column() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i8> = vec![1, 2, 3, 1, 2, 3, 4, 5];
        let expected: Vec<Option<i8>> = col1.iter().map(|&v| Some(v)).collect();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Byte.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i8>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_dict_encoding_all_nulls() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [i32::MIN, i32::MIN, i32::MIN, i32::MIN];

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Int.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, vec![None, None, None, None]);
        }
    }

    #[test]
    fn test_bloom_filter_roundtrip_i64() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnTypeTag;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i64> = (100..110).collect();
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        // Read it back with parquet2 and verify bloom filter works
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // Values NOT in the data: should skip
        let absent_vals: Vec<i64> = vec![0, 1, 50, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        // Values IN the data: should not skip
        let present_vals: Vec<i64> = vec![0, 105, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is in the row group");
    }

    #[test]
    fn test_bloom_filter_roundtrip_i32() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnTypeTag;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i32> = (200..210).collect();
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Int.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // Verify metadata has bloom filter offset
        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_meta = meta.row_groups[0].columns()[0].metadata();
        let bf_offset = col_meta.bloom_filter_offset;
        assert!(
            bf_offset.is_some(),
            "bloom filter offset should be present in metadata"
        );
        let bf_length = col_meta.bloom_filter_length;
        assert!(
            bf_length.is_some(),
            "bloom filter length should be present in metadata"
        );
        assert!(
            bf_length.unwrap() > 0,
            "bloom filter length should be positive"
        );

        // Values not in the data
        let absent_vals: Vec<i64> = vec![0, 1, 50, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals: Vec<i64> = vec![200, 205];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            !can_skip,
            "should NOT skip: filter values are present in the row group"
        );
    }

    #[test]
    fn test_bloom_filter_not_written_for_unselected_columns() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i64> = (0..10).collect();
        let col2: Vec<i32> = (0..10).collect();
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let col2_w = Column::from_raw_data(
            1,
            "col2",
            ColumnTypeTag::Int.into_type().code(),
            0,
            row_count,
            col2.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w, col2_w],
        };

        // Only enable bloom filter for column 0
        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let bf_offset_col0 = meta.row_groups[0].columns()[0]
            .metadata()
            .bloom_filter_offset;
        let bf_offset_col1 = meta.row_groups[0].columns()[1]
            .metadata()
            .bloom_filter_offset;

        assert!(bf_offset_col0.is_some(), "col0 should have bloom filter");
        assert!(
            bf_offset_col1.is_none(),
            "col1 should not have bloom filter"
        );
    }

    fn i64_to_be_bytes_vec(values: &[i64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_be_bytes()).collect()
    }

    fn i32_to_be_bytes_vec(values: &[i32]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_be_bytes()).collect()
    }

    fn i16_to_be_bytes_vec(values: &[i16]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_be_bytes()).collect()
    }

    fn i8_to_be_bytes_vec(values: &[i8]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_be_bytes()).collect()
    }

    #[test]
    fn test_bloom_filter_roundtrip_decimal8() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i8> = (10i8..20).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(2, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i8>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals_be = i8_to_be_bytes_vec(&[0, 1, 5, 99]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: absent_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals_be = i8_to_be_bytes_vec(&[0, 15, 99]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: present_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 15 is in the row group");
    }

    #[test]
    fn test_bloom_filter_roundtrip_decimal16() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i16> = (100i16..110).collect();
        let row_count = col1.len();

        // Decimal16 requires precision 3-4
        let decimal_type = ColumnType::new_decimal(4, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i16>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals_be = i16_to_be_bytes_vec(&[0, 1, 50, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: absent_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals_be = i16_to_be_bytes_vec(&[0, 105, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: present_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is in the row group");
    }

    #[test]
    fn test_bloom_filter_roundtrip_decimal32() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i32> = (10000i32..10010).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(9, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals_be = i32_to_be_bytes_vec(&[0, 1, 5000, 99999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: absent_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals_be = i32_to_be_bytes_vec(&[0, 10005, 99999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: present_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 10005 is in the row group");
    }

    #[test]
    fn test_bloom_filter_roundtrip_decimal64() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i64> = (100i64..110).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(18, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals_be = i64_to_be_bytes_vec(&[0, 1, 50, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: absent_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals_be = i64_to_be_bytes_vec(&[0, 105, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: present_vals_be.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is in the row group");
    }

    // Decimal128 memory layout: (hi: i64, lo: u64) in native byte order
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct TestDecimal128(i64, u64);

    #[test]
    fn test_bloom_filter_roundtrip_decimal128() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<TestDecimal128> = (100u64..110).map(|v| TestDecimal128(0, v)).collect();
        let row_count = col1.len();

        let decimal_type = ColumnType::new_decimal(38, 0).unwrap();
        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 16,
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // Helper to create 128-bit big-endian bytes for filter values
        // This matches Decimal128.to_bytes() which outputs hi.to_be_bytes() + lo.to_be_bytes()
        let to_be_128 = |hi: i64, lo: u64| -> [u8; 16] {
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&hi.to_be_bytes());
            bytes[8..16].copy_from_slice(&lo.to_be_bytes());
            bytes
        };

        let absent_vals: Vec<[u8; 16]> = [0u64, 1, 50, 999]
            .iter()
            .map(|&v| to_be_128(0, v))
            .collect();
        let absent_vals_flat: Vec<u8> =
            absent_vals.iter().flat_map(|b| b.iter().copied()).collect();
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: absent_vals_flat.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );

        let present_vals: Vec<[u8; 16]> =
            [0u64, 105, 999].iter().map(|&v| to_be_128(0, v)).collect();
        let present_vals_flat: Vec<u8> = present_vals
            .iter()
            .flat_map(|b| b.iter().copied())
            .collect();
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: present_vals_flat.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is in the row group");
    }

    #[test]
    fn test_min_max_skip_decimal8() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Data range: 10..20 (min=10, max=19)
        let col1: Vec<i8> = (10i8..20).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(2, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i8>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        // No bloom filter, only statistics
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // All values outside [10, 19] - should skip
        let outside_vals = i8_to_be_bytes_vec(&[0, 5, 25, 99]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: outside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(can_skip, "should skip: all values outside [10, 19]");

        // One value inside [10, 19] - should not skip
        let inside_vals = i8_to_be_bytes_vec(&[0, 15, 99]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: inside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 15 is inside [10, 19]");
    }

    #[test]
    fn test_min_max_skip_decimal16() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i16> = (100i16..110).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(4, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i16>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // All values outside [100, 109]
        let outside_vals = i16_to_be_bytes_vec(&[0, 50, 200, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: outside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(can_skip, "should skip: all values outside [100, 109]");

        // One value inside [100, 109]
        let inside_vals = i16_to_be_bytes_vec(&[0, 105, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: inside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is inside [100, 109]");
    }

    #[test]
    fn test_min_max_skip_decimal32() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i32> = (10000i32..10010).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(9, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // All values outside [10000, 10009]
        let outside_vals = i32_to_be_bytes_vec(&[0, 5000, 20000, 99999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: outside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(can_skip, "should skip: all values outside [10000, 10009]");

        // One value inside [10000, 10009]
        let inside_vals = i32_to_be_bytes_vec(&[0, 10005, 99999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: inside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 10005 is inside [10000, 10009]");
    }

    #[test]
    fn test_min_max_skip_decimal64() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i64> = (100i64..110).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(18, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        // All values outside [100, 109]
        let outside_vals = i64_to_be_bytes_vec(&[0, 50, 200, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: outside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(can_skip, "should skip: all values outside [100, 109]");

        // One value inside [100, 109]
        let inside_vals = i64_to_be_bytes_vec(&[0, 105, 999]);
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: inside_vals.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is inside [100, 109]");
    }

    #[test]
    fn test_min_max_skip_decimal128() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use qdb_core::col_type::ColumnType;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Data range: 100..110 (hi=0, lo=100..110)
        let col1: Vec<TestDecimal128> = (100u64..110).map(|v| TestDecimal128(0, v)).collect();
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(38, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 16,
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .expect("column");

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let data = buf.into_inner();

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let to_be_128 = |hi: i64, lo: u64| -> [u8; 16] {
            let mut bytes = [0u8; 16];
            bytes[0..8].copy_from_slice(&hi.to_be_bytes());
            bytes[8..16].copy_from_slice(&lo.to_be_bytes());
            bytes
        };

        // All values outside [100, 109]
        let outside_vals: Vec<[u8; 16]> = [0u64, 50, 200, 999]
            .iter()
            .map(|&v| to_be_128(0, v))
            .collect();
        let outside_vals_flat: Vec<u8> = outside_vals
            .iter()
            .flat_map(|b| b.iter().copied())
            .collect();
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 4u64 << 32,
            ptr: outside_vals_flat.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(can_skip, "should skip: all values outside [100, 109]");

        // One value inside [100, 109]
        let inside_vals: Vec<[u8; 16]> =
            [0u64, 105, 999].iter().map(|&v| to_be_128(0, v)).collect();
        let inside_vals_flat: Vec<u8> =
            inside_vals.iter().flat_map(|b| b.iter().copied()).collect();
        let filters = [ColumnFilterPacked {
            col_idx_and_count: 3u64 << 32,
            ptr: inside_vals_flat.as_ptr() as u64,
            column_type: 0,
        }];
        let can_skip = decoder
            .can_skip_row_group(0, &data, &filters, u64::MAX)
            .expect("can_skip");
        assert!(!can_skip, "should not skip: 105 is inside [100, 109]");
    }

    #[test]
    fn test_multi_partition_dict_fallback_non_symbol() {
        // When writing multiple partitions, non-Symbol columns configured with
        // RleDictionary should fall back to their default encoding (Plain for Int).
        // This covers the multi-partition dict fallback in file.rs (lines 487-491).
        use crate::parquet_write::schema::{to_encodings, to_parquet_schema};

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let data1: Vec<i32> = vec![1, 2, 3];
        let data2: Vec<i32> = vec![4, 5, 6];

        let col1 = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Int.into_type().code(),
            0,
            data1.len(),
            data1.as_ptr() as *const u8,
            data1.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let col2 = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Int.into_type().code(),
            0,
            data2.len(),
            data2.as_ptr() as *const u8,
            data2.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition1 = Partition {
            table: "test_table".to_string(),
            columns: vec![col1],
        };
        let partition2 = Partition {
            table: "test_table".to_string(),
            columns: vec![col2],
        };

        let (schema, additional_meta) = to_parquet_schema(&partition1, false, -1).unwrap();
        let encodings = to_encodings(&partition1);
        // Encoding should be RleDictionary since we requested it
        assert_eq!(encodings[0], parquet2::encoding::Encoding::RleDictionary);

        let mut chunked = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .chunked(schema, encodings)
            .unwrap();

        let partitions: Vec<&Partition> = vec![&partition1, &partition2];
        chunked
            .write_row_group_from_partitions(&partitions, 0, data2.len())
            .unwrap();
        chunked.finish(additional_meta).unwrap();

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify the encoding fell back to Plain (not RLE_DICTIONARY)
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");

        let col_encoding = metadata.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_encoding.iter().any(|e| e.0 == 0), // PLAIN = 0
            "expected Plain encoding after fallback, got: {:?}",
            col_encoding
        );
        assert!(
            !col_encoding.iter().any(|e| e.0 == 8), // RLE_DICTIONARY = 8
            "should not use RLE_DICTIONARY for non-symbol multi-partition, got: {:?}",
            col_encoding
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        let expected: Vec<Option<i32>> = vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)];
        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_per_column_compression_multi_partition() {
        // Write a multi-partition file with per-column compression overrides.
        // Covers column_compression() in multi-partition context (file.rs line 481-482).
        use crate::parquet_write::schema::{to_compressions, to_encodings, to_parquet_schema};

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        // Two columns: col_uncompressed (default) and col_snappy (Snappy)
        let snappy_config = schema::ParquetEncodingConfig::new(0, 2, 0).raw();

        let data1_a: Vec<i64> = (0..50).collect();
        let data1_b: Vec<i64> = (50..100).collect();
        let data2_a: Vec<f64> = (0..50).map(|i| i as f64 * 0.5).collect();
        let data2_b: Vec<f64> = (50..100).map(|i| i as f64 * 0.5).collect();

        let col1_a = Column::from_raw_data(
            0,
            "col_uncompressed",
            ColumnTypeTag::Long.into_type().code(),
            0,
            data1_a.len(),
            data1_a.as_ptr() as *const u8,
            data1_a.len() * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let col2_a = Column::from_raw_data(
            1,
            "col_snappy",
            ColumnTypeTag::Double.into_type().code(),
            0,
            data2_a.len(),
            data2_a.as_ptr() as *const u8,
            data2_a.len() * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            snappy_config,
        )
        .unwrap();

        let col1_b = Column::from_raw_data(
            0,
            "col_uncompressed",
            ColumnTypeTag::Long.into_type().code(),
            0,
            data1_b.len(),
            data1_b.as_ptr() as *const u8,
            data1_b.len() * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let col2_b = Column::from_raw_data(
            1,
            "col_snappy",
            ColumnTypeTag::Double.into_type().code(),
            0,
            data2_b.len(),
            data2_b.as_ptr() as *const u8,
            data2_b.len() * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            snappy_config,
        )
        .unwrap();

        let partition_a = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_a, col2_a],
        };
        let partition_b = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_b, col2_b],
        };

        let (schema, additional_meta) = to_parquet_schema(&partition_a, false, -1).unwrap();
        let encodings = to_encodings(&partition_a);
        let compressions = to_compressions(&partition_a);

        let mut chunked = ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .chunked_with_compressions(schema, encodings, compressions)
            .unwrap();

        let partitions: Vec<&Partition> = vec![&partition_a, &partition_b];
        chunked
            .write_row_group_from_partitions(&partitions, 0, data1_b.len())
            .unwrap();
        chunked.finish(additional_meta).unwrap();

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");

        let row_group = &metadata.row_groups[0];
        let col0_compression = row_group.columns()[0].compression();
        assert_eq!(
            col0_compression,
            parquet2::compression::Compression::Uncompressed,
            "col_uncompressed should be uncompressed in multi-partition"
        );

        let col1_compression = row_group.columns()[1].compression();
        assert_eq!(
            col1_compression,
            parquet2::compression::Compression::Snappy,
            "col_snappy should use Snappy in multi-partition"
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr0 = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr0.len(), 100);
            for i in 0..100 {
                assert_eq!(arr0.value(i), i as i64);
            }
        }
    }

    #[test]
    fn test_write_with_min_compression_ratio() {
        // Verify with_min_compression_ratio() flows through correctly.
        // With a very high ratio and Snappy compression, pages that don't compress well
        // are stored uncompressed. The file should still be readable.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        // Random-ish data that compresses poorly
        let col_data: Vec<i64> = (0..500).map(|i| i * 7 + (i % 13) * 1000).collect();
        let row_count = col_data.len();

        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col_data.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(parquet2::compression::CompressionOptions::Snappy)
            .with_min_compression_ratio(100.0) // very high: force fallback to uncompressed
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify metadata: compression should have fallen back to Uncompressed
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let row_group = &metadata.row_groups[0];
        assert_eq!(
            row_group.columns()[0].compression(),
            parquet2::compression::Compression::Uncompressed,
            "high min_compression_ratio should force fallback to uncompressed"
        );

        // Verify the file is readable and data is correct
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr.len(), 500);
            for i in 0..500 {
                let expected = i as i64 * 7 + (i as i64 % 13) * 1000;
                assert_eq!(arr.value(i), expected);
            }
        }
    }

    #[test]
    fn test_min_compression_ratio_met() {
        // All-zeros data compresses very well with Snappy.
        // With a modest ratio threshold, compression should be kept.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let col_data: Vec<i64> = vec![0i64; 500];
        let row_count = col_data.len();

        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col_data.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(parquet2::compression::CompressionOptions::Snappy)
            .with_min_compression_ratio(1.2) // modest ratio, easily met by all-zeros
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let row_group = &metadata.row_groups[0];
        assert_eq!(
            row_group.columns()[0].compression(),
            parquet2::compression::Compression::Snappy,
            "compression should be kept when ratio threshold is met"
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr.len(), 500);
            for i in 0..500 {
                assert_eq!(arr.value(i), 0);
            }
        }
    }

    #[test]
    fn test_min_compression_ratio_boundary_one() {
        // ratio=1.0 is trivially met for any compressible data.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let col_data: Vec<i64> = vec![0i64; 500];
        let row_count = col_data.len();

        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col_data.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(parquet2::compression::CompressionOptions::Snappy)
            .with_min_compression_ratio(1.0) // boundary: ratio=1.0
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let row_group = &metadata.row_groups[0];
        assert_eq!(
            row_group.columns()[0].compression(),
            parquet2::compression::Compression::Snappy,
            "ratio=1.0 should be trivially met for compressible data"
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr.len(), 500);
            for i in 0..500 {
                assert_eq!(arr.value(i), 0);
            }
        }
    }

    #[test]
    fn test_min_compression_ratio_zero_disabled() {
        // ratio=0.0 disables the check entirely (streaming mode).
        // Compression should always be kept regardless of data compressibility.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let col_data: Vec<i64> = (0..500).map(|i| i * 7 + (i % 13) * 1000).collect();
        let row_count = col_data.len();

        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col_data.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(parquet2::compression::CompressionOptions::Snappy)
            .with_min_compression_ratio(0.0) // disabled
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let row_group = &metadata.row_groups[0];
        assert_eq!(
            row_group.columns()[0].compression(),
            parquet2::compression::Compression::Snappy,
            "ratio=0.0 should disable the check, keeping compression"
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr.len(), 500);
            for i in 0..500 {
                let expected = i as i64 * 7 + (i as i64 % 13) * 1000;
                assert_eq!(arr.value(i), expected);
            }
        }
    }

    #[test]
    fn test_min_compression_ratio_negative() {
        // Negative ratio should be treated the same as disabled (0.0).
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());

        let col_data: Vec<i64> = (0..500).map(|i| i * 7 + (i % 13) * 1000).collect();
        let row_count = col_data.len();

        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col_data.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_compression(parquet2::compression::CompressionOptions::Snappy)
            .with_min_compression_ratio(-1.0) // negative: treated as disabled
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let row_group = &metadata.row_groups[0];
        assert_eq!(
            row_group.columns()[0].compression(),
            parquet2::compression::Compression::Snappy,
            "negative ratio should be treated as disabled, keeping compression"
        );

        // Verify data correctness
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .expect("downcast");
            assert_eq!(arr.len(), 500);
            for i in 0..500 {
                let expected = i as i64 * 7 + (i as i64 % 13) * 1000;
                assert_eq!(arr.value(i), expected);
            }
        }
    }

    #[test]
    fn test_dict_encoding_short_notnull_column() {
        // Dict encode a Short (non-nullable) column via int_slice_to_dict_pages_notnull.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i16> = vec![10, 20, 30, 10, 20, 30, 40, 50];
        let expected: Vec<Option<i16>> = col1.iter().map(|&v| Some(v)).collect();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Short.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i16>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify encoding is RLE_DICTIONARY
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let col_encoding = metadata.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_encoding.iter().any(|e| e.0 == 8), // RLE_DICTIONARY = 8
            "expected RLE_DICTIONARY encoding for Short, got: {:?}",
            col_encoding
        );

        // Verify data (Short has Int16 logical type, Arrow reads as Int16Array)
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    #[test]
    fn test_dict_encoding_float_column() {
        // Dict encode a Float column via slice_to_dict_pages_simd for f32.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1.5f32, 2.5, f32::NAN, 3.5, 1.5, 2.5];
        let expected_some = [
            Some(1.5f32),
            Some(2.5),
            None,
            Some(3.5),
            Some(1.5),
            Some(2.5),
        ];

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::Float.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<f32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify encoding is RLE_DICTIONARY
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let col_encoding = metadata.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_encoding.iter().any(|e| e.0 == 8), // RLE_DICTIONARY = 8
            "expected RLE_DICTIONARY encoding for Float, got: {:?}",
            col_encoding
        );

        // Verify data
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .expect("downcast");
            for (i, expected) in expected_some.iter().enumerate() {
                match expected {
                    Some(v) => {
                        assert!(!arr.is_null(i));
                        assert!((arr.value(i) - v).abs() < 1e-6);
                    }
                    None => {
                        assert!(arr.is_null(i));
                    }
                }
            }
        }
    }

    #[test]
    fn test_dict_encoding_ipv4_column() {
        // Dict encode an IPv4 column via int_slice_to_dict_pages_nullable.
        // IPv4 null sentinel is 0.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1: Vec<i32> = vec![
            0x0A000001i32, // 10.0.0.1
            0x0A000002i32, // 10.0.0.2
            0,             // null
            0x0A000001i32, // 10.0.0.1
            0x0A000003i32, // 10.0.0.3
            0,             // null
        ];
        // IPv4 has UInt32 logical type in Parquet; Arrow reads as UInt32Array
        let expected: Vec<Option<u32>> = col1
            .iter()
            .map(|&v| if v == 0 { None } else { Some(v as u32) })
            .collect();

        let col1_w = Column::from_raw_data(
            0,
            "col1",
            ColumnTypeTag::IPv4.into_type().code(),
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();

        // Verify encoding is RLE_DICTIONARY
        let metadata = parquet2::read::read_metadata_with_size(
            &mut std::io::Cursor::new(bytes.to_byte_slice()),
            bytes.len() as u64,
        )
        .expect("read metadata");
        let col_encoding = metadata.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_encoding.iter().any(|e| e.0 == 8), // RLE_DICTIONARY = 8
            "expected RLE_DICTIONARY encoding for IPv4, got: {:?}",
            col_encoding
        );

        // Verify data (IPv4 has UInt32 logical type, Arrow reads as UInt32Array)
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("reader")
            .with_batch_size(8192)
            .build()
            .expect("builder");

        for batch in parquet_reader.flatten() {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .expect("downcast");
            let collected: Vec<_> = arr.iter().collect();
            assert_eq!(collected, expected);
        }
    }

    // =========================================================================
    // Bloom filter + RleDictionary encoding tests
    // =========================================================================

    #[test]
    fn test_bloom_filter_dict_int() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i32, 2, 3, 1, 2, 3, i32::MIN, 1, 2, 3];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Int.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        // Verify encoding is RLE_DICTIONARY
        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding, got: {:?}",
            col_enc
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals: Vec<i64> = vec![0, 10, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should skip: none of the filter values are in the row group"
        );

        let present_vals: Vec<i64> = vec![0, 2, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            !decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should not skip: 2 is in the row group"
        );
    }

    #[test]
    fn test_bloom_filter_dict_long() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [100i64, 200, 300, 100, 200, i64::MIN, 300];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals: Vec<i64> = vec![0, 10, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should skip"
        );

        let present_vals: Vec<i64> = vec![0, 200, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            !decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should not skip: 200 is in the row group"
        );
    }

    #[test]
    fn test_bloom_filter_dict_float() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1.0f32, 2.0, 3.0, 1.0, 2.0, f32::NAN, 3.0];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Float.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<f32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding"
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_double() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1.5f64, 2.5, 3.5, 1.5, 2.5, f64::NAN, 3.5];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Double.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<f64>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals: Vec<i64> = vec![
            i64::from_le_bytes(10.0f64.to_le_bytes()),
            i64::from_le_bytes(20.0f64.to_le_bytes()),
        ];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should skip"
        );

        let present_vals: Vec<i64> = vec![i64::from_le_bytes(2.5f64.to_le_bytes())];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            !decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should not skip: 2.5 is in the row group"
        );
    }

    #[test]
    fn test_bloom_filter_dict_byte_notnull() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [1i8, 2, 3, 1, 2, 3, 1, 2];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Byte.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i8>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding"
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_short_notnull() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = [10i16, 20, 30, 10, 20, 30];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Short.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i16>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_ipv4() {
        use crate::allocator::TestAllocatorState;
        use crate::parquet_read::{ColumnFilterPacked, ParquetDecoder};
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // IPv4 null sentinel is i32::MIN
        let col1 = [100i32, 200, 300, 100, 200, i32::MIN, 300];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::IPv4.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );

        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(data.as_slice());
        let decoder =
            ParquetDecoder::read(allocator, &mut reader, data.len() as u64).expect("decoder");

        let absent_vals: Vec<i64> = vec![0, 10, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (absent_vals.len() as u64) << 32,
            ptr: absent_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should skip"
        );

        let present_vals: Vec<i64> = vec![0, 200, 999];
        let filters = [ColumnFilterPacked {
            col_idx_and_count: (present_vals.len() as u64) << 32,
            ptr: present_vals.as_ptr() as u64,
            column_type: 0,
        }];
        assert!(
            !decoder
                .can_skip_row_group(0, &data, &filters, u64::MAX)
                .expect("can_skip"),
            "should not skip: 200 is in the row group"
        );
    }

    /// Helper: build QuestDB string column data (UTF-16 with i64 offset array).
    /// Returns (primary_data, aux_offsets_as_bytes).
    fn serialize_as_questdb_strings(strings: &[Option<&str>]) -> (Vec<u8>, Vec<u8>) {
        let mut primary = Vec::new();
        let mut offsets = Vec::new();

        for s in strings {
            let offset = primary.len() as i64;
            offsets.extend_from_slice(&offset.to_le_bytes());
            match s {
                Some(text) => {
                    let utf16: Vec<u16> = text.encode_utf16().collect();
                    let len = utf16.len() as i32;
                    primary.extend_from_slice(&len.to_le_bytes());
                    for ch in &utf16 {
                        primary.extend_from_slice(&ch.to_le_bytes());
                    }
                }
                None => {
                    let null_marker: i32 = -1;
                    primary.extend_from_slice(&null_marker.to_le_bytes());
                }
            }
        }

        (primary, offsets)
    }

    #[test]
    fn test_bloom_filter_dict_string() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let strings: Vec<Option<&str>> = vec![
            Some("hello"),
            Some("world"),
            None,
            Some("hello"),
            Some("world"),
            Some("hello"),
        ];
        let (primary, aux) = serialize_as_questdb_strings(&strings);
        let row_count = strings.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::String.into_type().code(),
            0,
            row_count,
            primary.as_ptr(),
            primary.len(),
            aux.as_ptr(),
            aux.len(),
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding"
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    /// Helper: build QuestDB binary column data (i64 length header + raw bytes).
    /// Returns (primary_data, aux_offsets_as_bytes).
    fn serialize_as_questdb_binaries(entries: &[Option<&[u8]>]) -> (Vec<u8>, Vec<u8>) {
        let mut primary = Vec::new();
        let mut offsets = Vec::new();

        for entry in entries {
            let offset = primary.len() as i64;
            offsets.extend_from_slice(&offset.to_le_bytes());
            match entry {
                Some(bytes) => {
                    let len = bytes.len() as i64;
                    primary.extend_from_slice(&len.to_le_bytes());
                    primary.extend_from_slice(bytes);
                }
                None => {
                    let null_marker: i64 = -1;
                    primary.extend_from_slice(&null_marker.to_le_bytes());
                }
            }
        }

        (primary, offsets)
    }

    #[test]
    fn test_bloom_filter_dict_binary() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let entries: Vec<Option<&[u8]>> = vec![
            Some(b"abc"),
            Some(b"def"),
            None,
            Some(b"abc"),
            Some(b"def"),
            Some(b"abc"),
        ];
        let (primary, aux) = serialize_as_questdb_binaries(&entries);
        let row_count = entries.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Binary.into_type().code(),
            0,
            row_count,
            primary.as_ptr(),
            primary.len(),
            aux.as_ptr(),
            aux.len(),
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_uuid() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let null_val = {
            let mut v = [0u8; 16];
            let long_bytes = i64::MIN.to_le_bytes();
            for i in 0..16 {
                v[i] = long_bytes[i % 8];
            }
            v
        };
        let val1 = [1u8; 16];
        let val2 = [2u8; 16];
        let col1: Vec<[u8; 16]> = vec![val1, val2, null_val, val1, val2, val1];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Uuid.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 16,
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding"
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_long256() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let null_val = {
            let mut v = [0u8; 32];
            let long_bytes = i64::MIN.to_le_bytes();
            for i in 0..32 {
                v[i] = long_bytes[i % 8];
            }
            v
        };
        let val1 = [1u8; 32];
        let val2 = [2u8; 32];
        let col1: Vec<[u8; 32]> = vec![val1, val2, null_val, val1, val2, val1];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long256.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 32,
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_bloom_filter_dict_decimal8() {
        use std::collections::HashSet;

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        // Decimal8 values with repeats (natural for dict encoding).
        // Decimal8 null sentinel is i8::MIN (-128).
        let col1 = [10i8, 20, 30, 10, 20, 30, 10];
        let row_count = col1.len();
        let decimal_type = ColumnType::new_decimal(2, 0).unwrap();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            decimal_type.code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * size_of::<i8>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        let mut bloom_cols = HashSet::new();
        bloom_cols.insert(0);

        // Decimal dict encoding has a known issue with statistics serialization
        // (PrimitiveStatistics vs FixedLenStatistics mismatch), so disable statistics.
        ParquetWriter::new(&mut buf)
            .with_statistics(false)
            .with_bloom_filter_columns(bloom_cols)
            .with_bloom_filter_fpp(0.01)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();

        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");
        let col_enc = meta.row_groups[0].columns()[0].column_encoding();
        assert!(
            col_enc.iter().any(|e| e.0 == 8),
            "expected RLE_DICTIONARY encoding"
        );
        assert!(
            meta.row_groups[0].columns()[0]
                .metadata()
                .bloom_filter_offset
                .is_some(),
            "bloom filter offset should be present"
        );
    }

    #[test]
    fn test_dict_stats_fixed_len_bytes_non_reversed() {
        // Long256 uses reverse=false: stats should match the raw byte values.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let null_val = {
            let mut v = [0u8; 32];
            let long_bytes = i64::MIN.to_le_bytes();
            for i in 0..32 {
                v[i] = long_bytes[i % 8];
            }
            v
        };
        // min = [0x01; 32], max = [0x03; 32]
        let val_min = [0x01u8; 32];
        let val_mid = [0x02u8; 32];
        let val_max = [0x03u8; 32];
        let col1: Vec<[u8; 32]> = vec![
            val_mid, val_max, null_val, val_min, val_mid, val_max, val_min,
        ];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Long256.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 32,
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();
        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");

        let stats_arc = meta.row_groups[0].columns()[0]
            .statistics()
            .expect("statistics present")
            .expect("statistics ok");
        let stats = stats_arc
            .as_any()
            .downcast_ref::<parquet2::statistics::FixedLenStatistics>()
            .expect("FixedLenStatistics");

        assert_eq!(stats.min_value.as_deref(), Some(val_min.as_slice()));
        assert_eq!(stats.max_value.as_deref(), Some(val_max.as_slice()));
        assert_eq!(stats.null_count, Some(1));
    }

    #[test]
    fn test_dict_stats_fixed_len_bytes_reversed() {
        // UUID uses reverse=true: stats should be computed on the reversed
        // (big-endian / Parquet) representation, not the original LE bytes.
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let null_val = {
            let mut v = [0u8; 16];
            let long_bytes = i64::MIN.to_le_bytes();
            for i in 0..16 {
                v[i] = long_bytes[i % 8];
            }
            v
        };

        // In-memory LE values.  After reversal the byte ordering flips.
        // val_a LE = [0x01, 0x00, ..., 0x00] → reversed = [0x00, ..., 0x00, 0x01]
        // val_b LE = [0x00, 0x00, ..., 0x02] → reversed = [0x02, 0x00, ..., 0x00]
        // Lexicographic comparison on the reversed form: val_a_rev < val_b_rev
        // because 0x00 < 0x02 at byte[0].
        let mut val_a = [0u8; 16];
        val_a[0] = 0x01; // LE low byte set
        let mut val_b = [0u8; 16];
        val_b[15] = 0x02; // LE high byte set

        let mut val_a_rev = val_a;
        val_a_rev.reverse();
        let mut val_b_rev = val_b;
        val_b_rev.reverse();
        // Sanity: val_a_rev < val_b_rev lexicographically
        assert!(val_a_rev < val_b_rev);

        let col1: Vec<[u8; 16]> = vec![val_b, null_val, val_a, val_b, val_a];
        let row_count = col1.len();

        let col1_w = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Uuid.into_type().code(),
            0,
            row_count,
            col1.as_ptr() as *const u8,
            row_count * 16,
            null(),
            0,
            null(),
            0,
            false,
            false,
            rle_dict_config(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: vec![col1_w],
        };

        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .finish(partition)
            .expect("parquet writer");

        let data = buf.into_inner();
        let meta =
            parquet2::read::read_metadata(&mut Cursor::new(data.as_slice())).expect("metadata");

        let stats_arc = meta.row_groups[0].columns()[0]
            .statistics()
            .expect("statistics present")
            .expect("statistics ok");
        let stats = stats_arc
            .as_any()
            .downcast_ref::<parquet2::statistics::FixedLenStatistics>()
            .expect("FixedLenStatistics");

        // Stats must be on the reversed (Parquet) representation.
        assert_eq!(
            stats.min_value.as_deref(),
            Some(val_a_rev.as_slice()),
            "min should be val_a reversed"
        );
        assert_eq!(
            stats.max_value.as_deref(),
            Some(val_b_rev.as_slice()),
            "max should be val_b reversed"
        );
        assert_eq!(stats.null_count, Some(1));
    }
}
