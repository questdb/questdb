mod binary;
mod boolean;
pub(crate) mod file;
mod fixed_len_bytes;
mod jni;
mod primitive;
mod schema;
mod string;
mod symbol;
mod util;
mod varchar;

pub trait Nullable {
    fn is_null(&self) -> bool;
}

impl Nullable for i8 {
    fn is_null(&self) -> bool {
        false
    }
}

impl Nullable for bool {
    fn is_null(&self) -> bool {
        false
    }
}

impl Nullable for i16 {
    fn is_null(&self) -> bool {
        false
    }
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

#[cfg(test)]
mod tests {
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{ColumnImpl, Partition};
    use arrow::array::Array;
    use bytes::Bytes;
    use num_traits::Float;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::deserialize::{HybridEncoded, HybridRleIter};
    use parquet2::encoding::{hybrid_rle, uleb128};
    use parquet2::page::CompressedPage;
    use parquet2::types;
    use std::io::Cursor;
    use std::mem::size_of;
    use std::ptr::null;
    use std::sync::Arc;

    #[test]
    fn test_write_parquet_with_fixed_sized_columns() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = vec![1i32, 2, i32::MIN, 3];
        let expected1 = vec![Some(1i32), Some(2), None, Some(3)];
        let col2 = vec![0.5f32, 0.001, f32::nan(), 3.14];
        let expected2 = vec![Some(0.5f32), Some(0.001), None, Some(3.14)];

        let col1_w = Arc::new(
            ColumnImpl::from_raw_data(
                "col1",
                5,
                col1.len(),
                col1.as_ptr() as *const u8,
                col1.len() * size_of::<i32>(),
                null(),
                0,
                null(),
                0,
            )
            .unwrap(),
        );
        let col2_w = Arc::new(
            ColumnImpl::from_raw_data(
                "col2",
                9,
                col2.len(),
                col2.as_ptr() as *const u8,
                col2.len() * size_of::<f32>(),
                null(),
                0,
                null(),
                0,
            )
            .unwrap(),
        );

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

        for batch in parquet_reader {
            if let Ok(batch) = batch {
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
    }
    #[test]
    fn test_write_parquet_row_group_size_data_page_size() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 100_000usize;
        let row_group_size = 500usize;
        let page_size_bytes = 256usize;
        let col1: Vec<i64> = (0..row_count).into_iter().map(|v| v as i64).collect();
        let col1_w = Arc::new(
            ColumnImpl::from_raw_data(
                "col1",
                6,
                col1.len(),
                col1.as_ptr() as *const u8,
                col1.len() * size_of::<i64>(),
                null(),
                0,
                null(),
                0,
            )
            .unwrap(),
        );

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
        for batch in parquet_reader {
            if let Ok(batch) = batch {
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
        }
        assert_eq!(expected, row_count as i64);

        let meta = parquet2::read::read_metadata(&mut reader).expect("metadata");
        assert_eq!(row_count, meta.num_rows);
        assert_eq!(row_count / row_group_size, meta.row_groups.len());
        assert_eq!(row_group_size, meta.row_groups[0].num_rows());

        let chunk_meta = &meta.row_groups[0].columns()[0];
        let pages =
            parquet2::read::get_page_iterator(chunk_meta, reader, None, vec![], page_size_bytes)
                .expect("pages iter");
        for page in pages {
            let page = page.expect("page");
            match page {
                CompressedPage::Data(data) => {
                    let uncompressed = data.uncompressed_size();
                    assert!(uncompressed <= page_size_bytes);
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
                    assert_eq!(val, true);
                    assert_eq!(len, def_level_count);
                }
                _ => assert!(false),
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
