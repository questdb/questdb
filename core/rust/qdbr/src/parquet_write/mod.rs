mod boolean;
pub(crate) mod file;
mod primitive;
mod schema;
mod binary;
mod string;
mod fixed_len_bytes;
mod varchar;
mod symbol;

use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;
use parquet2::write::DynIter;

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

pub fn column_chunk_to_pages(
    column_type: PrimitiveType,
    slice: &[i32],
    data_pagesize_limit: Option<usize>,
    _encoding: Encoding,
) -> DynIter<'_, parquet2::error::Result<Page>> {
    let number_of_rows = slice.len();
    let max_page_size = data_pagesize_limit.unwrap_or(10);
    let max_page_size = max_page_size.min(2usize.pow(31) - 2usize.pow(25));
    let rows_per_page = (max_page_size / (std::mem::size_of::<i32>() + 1)).max(1);

    let rows = (0..number_of_rows)
        .step_by(rows_per_page)
        .map(move |offset| {
            let length = if offset + rows_per_page > number_of_rows {
                number_of_rows - offset
            } else {
                rows_per_page
            };
            (offset, length)
        });

    let pages = rows.map(move |(offset, length)| {
        Ok(slice_to_page(
            column_type.clone(),
            &slice[offset..offset + length],
        ))
    });

    DynIter::new(pages)
}

pub fn slice_to_page(column_type: PrimitiveType, chunk: &[i32]) -> Page {
    let mut values = vec![];
    let mut null_count = 0;
    let mut max = i32::MIN;
    let mut min = i32::MAX;
    let nulls_iter = chunk.iter().map(|value| {
        max = max.max(*value);
        min = min.min(*value);
        if i32::MIN == *value {
            null_count += 1;
            false
        } else {
            // encode() values here
            values.extend_from_slice(value.to_le_bytes().as_ref());
            true
        }
    });

    let mut buffer = vec![];
    // encode_iter(&mut buffer, nulls_iter, Version::V1).expect("nulls encoding");
    let _definition_levels_byte_length = buffer.len();

    buffer.extend_from_slice(&values);

    let statistics = if true {
        Some(serialize_statistics(&PrimitiveStatistics::<i32> {
            primitive_type: column_type.clone(),
            null_count: Some(null_count as i64),
            distinct_count: None,
            min_value: Some(min),
            max_value: Some(max),
        }))
    } else {
        None
    };

    Page::Data(build_page_v1(
        buffer,
        chunk.len(),
        chunk.len(),
        statistics,
        column_type.clone(),
        Encoding::Plain,
    ))
}

#[allow(clippy::too_many_arguments)]
pub fn build_page_v1(
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> DataPage {
    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: num_values as i32,
        encoding: encoding.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics,
    });

    DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn _build_page_v2(
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    null_count: usize,
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> DataPage {
    let header = DataPageHeader::V2(DataPageHeaderV2 {
        num_values: num_values as i32,
        encoding: encoding.into(),
        num_nulls: null_count as i32,
        num_rows: num_rows as i32,
        definition_levels_byte_length: definition_levels_byte_length as i32,
        repetition_levels_byte_length: repetition_levels_byte_length as i32,
        is_compressed: Some(false),
        statistics,
    });

    DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    )
}

#[cfg(test)]
mod tests {
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{ColumnImpl, Partition};
    use bytes::Bytes;
    use num_traits::Float;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet2::deserialize::{HybridEncoded, HybridRleIter};
    use parquet2::encoding::{hybrid_rle, uleb128};
    use std::io::Cursor;
    use std::ptr::null;
    use std::sync::Arc;
    use arrow::array::Array;

    #[test]
    fn test_write_parquet_with_fixed_sized_columns() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let col1 = vec![1i32, 2, i32::MIN, 3];
        let expected1 = vec![Some(1i32), Some(2), None, Some(3)];
        let col2 = vec![0.5f32, 0.001, f32::nan(), 3.14];
        let expected2 = vec![Some(0.5f32), Some(0.001), None, Some(3.14)];

        let col1_w = Arc::new(
            ColumnImpl::from_raw_data("col1", 5, col1.len(), col1.as_ptr() as *const u8, null(), 0)
                .unwrap(),
        );
        let col2_w = Arc::new(
            ColumnImpl::from_raw_data("col2", 9, col2.len(), col2.as_ptr() as *const u8, null(), 0)
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
            ColumnImpl::from_raw_data("col1", 6, col1.len(), col1.as_ptr() as *const u8, null(), 0)
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
        let meta = parquet2::read::read_metadata(&mut buf).expect("metadata");
        assert_eq!(row_count, meta.num_rows);
        assert_eq!(row_count / row_group_size, meta.row_groups.len());
        assert_eq!(row_group_size, meta.row_groups[0].num_rows());
        // assert!(meta.row_groups[0].columns()[0].metadata().total_uncompressed_size < page_size_bytes as i64);
        //TODO: data page metadata?
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
}
