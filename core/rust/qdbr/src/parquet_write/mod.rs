use num_traits::AsPrimitive;

mod binary;
mod boolean;
pub(crate) mod file;
mod fixed_len_bytes;
mod jni;
mod primitive;
pub mod schema;
mod string;
mod symbol;
mod update;
mod util;
pub mod varchar;

pub trait Nullable {
    fn is_null(&self) -> bool;
}

impl Nullable for i32 {
    fn is_null(&self) -> bool {
        *self == i32::MIN
    }
}

impl Nullable for i64 {
    fn is_null(&self) -> bool {
        *self == i64::MIN
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
        self.0 == -1
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
        self.0 == -1
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
        self.0 == -1
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
        self.0 == -1
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
        self.0 == 0
    }
}

impl AsPrimitive<i32> for IPv4 {
    fn as_(self) -> i32 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use crate::parquet::col_type::{ColumnType, ColumnTypeTag};
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use arrow::array::Array;
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use num_traits::Float;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::deserialize::{HybridEncoded, HybridRleIter};
    use parquet2::encoding::{hybrid_rle, uleb128};
    use parquet2::page::CompressedPage;
    use parquet2::types;
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
}
