use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::qdb_metadata::QdbMeta;
use nonmax::NonMaxU32;
use parquet2::metadata::FileMetaData;
use qdb_core::col_type::ColumnType;

mod column_sink;
mod decode;
mod jni;
mod meta;
mod slicer;

// The metadata fields are accessed from Java.
// This struct contains only immutable metadata.
// The reader is passed as a parameter to decode methods.
#[repr(C)]
pub struct ParquetDecoder {
    pub allocator: QdbAllocator,
    pub col_count: u32,
    pub row_count: usize,
    pub row_group_count: u32,
    pub row_group_sizes_ptr: *const u32,
    pub row_group_sizes: AcVec<u32>,
    // None (stored as zero, which is equal to ~u32::MAX) means no designated timestamp
    pub timestamp_index: Option<NonMaxU32>,
    pub columns_ptr: *const ColumnMeta,
    pub columns: AcVec<ColumnMeta>,
    pub(crate) metadata: FileMetaData,
    pub(crate) qdb_meta: Option<QdbMeta>,
    pub(crate) row_group_sizes_acc: AcVec<usize>,
}

#[repr(C)]
pub struct DecodeContext {
    pub(crate) file_ptr: *const u8,
    pub(crate) file_size: u64,
    pub(crate) decompress_buffer: Vec<u8>,
}

impl DecodeContext {
    pub fn new(file_ptr: *const u8, file_size: u64) -> Self {
        Self { file_ptr, file_size, decompress_buffer: Vec::new() }
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ColumnFilterPacked {
    pub col_idx_and_count: u64,
    pub ptr: u64,
}

impl ColumnFilterPacked {
    #[inline]
    pub fn column_index(&self) -> u32 {
        (self.col_idx_and_count & 0xFFFFFFFF) as u32
    }

    #[inline]
    pub fn count(&self) -> u32 {
        (self.col_idx_and_count >> 32) as u32
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ColumnFilterValues {
    pub count: u32,
    pub ptr: u64,
}

#[repr(C)]
#[derive(Debug)]
pub struct ColumnMeta {
    // None (zero) means unsupported column type
    pub column_type: Option<ColumnType>,
    pub id: i32,
    pub name_size: i32,
    pub name_ptr: *const u16,
    pub name_vec: AcVec<u16>,
}

// The fields are accessed from Java.
#[repr(C)]
pub struct RowGroupBuffers {
    column_bufs_ptr: *const ColumnChunkBuffers,
    column_bufs: AcVec<ColumnChunkBuffers>,
}

impl RowGroupBuffers {
    pub fn column_buffers(&self) -> &AcVec<ColumnChunkBuffers> {
        &self.column_bufs
    }
}

#[repr(C)]
pub struct RowGroupStatBuffers {
    column_chunk_stats_ptr: *const ColumnChunkStats,
    column_chunk_stats: AcVec<ColumnChunkStats>,
}

/// QuestDB-format Column Data
///
/// The memory is owned by the Rust code, read by Java.
/// The `(data|aux)_vec` are vectors since these are grown dynamically.
/// After each growth the `_size` and `_ptr` fields are updated
/// and then accessed from Java via base pointer + field offset via `Unsafe`.
#[repr(C)]
pub struct ColumnChunkBuffers {
    pub data_size: usize,
    pub data_ptr: *mut u8,
    pub data_vec: AcVec<u8>,

    pub aux_size: usize,
    pub aux_ptr: *mut u8,
    pub aux_vec: AcVec<u8>,
}

#[repr(C)]
pub struct ColumnChunkStats {
    pub min_value_ptr: *mut u8,
    pub min_value_size: usize,
    pub min_value: AcVec<u8>,
    pub max_value_ptr: *mut u8,
    pub max_value_size: usize,
    pub max_value: AcVec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::allocator::TestAllocatorState;
    use crate::parquet::error::ParquetResult;
    use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_read::decode::ParquetColumnIndex;
    use crate::parquet_read::{ColumnFilterValues, DecodeContext, ParquetDecoder, RowGroupBuffers};
    use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::{BloomFilterPosition, WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use qdb_core::col_type::ColumnTypeTag;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn fn_load_symbol_without_local_is_global_format_meta() -> ParquetResult<()> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut qdb_meta = QdbMeta::new(1);
        qdb_meta.schema.insert(
            0,
            QdbMetaCol {
                column_type: ColumnTypeTag::Symbol.into_type(),
                column_top: 0,
                format: None, // It should error because this is missing.
            },
        );

        let (buf, row_count) = gen_test_symbol_parquet(Some(qdb_meta.serialize()?))?;
        let buf_len = buf.len() as u64;

        let mut reader = Cursor::new(&buf);
        let parquet_decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)?;
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);
        let res = parquet_decoder.decode_row_group(
            &mut ctx,
            &mut rgb,
            &[(0, ColumnTypeTag::Symbol.into_type())],
            0,
            0,
            row_count as u32,
        );
        let err = res.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("could not decode page for column \"sym\" in row group 0"));
        assert!(msg.contains("only special LocalKeyIsGlobal-encoded symbol columns are supported"));

        Ok(())
    }

    fn gen_test_symbol_parquet(qdb_metadata: Option<String>) -> ParquetResult<(Vec<u8>, usize)> {
        let symbol_col_data: Vec<ByteArray> = vec![
            ByteArray::from("abc"),
            ByteArray::from("defg"),
            ByteArray::from("hijkl"),
            ByteArray::from("mn"),
            ByteArray::from(""),
            ByteArray::from("o"),
        ];

        let def_levels = vec![1, 1, 0, 0, 1, 1, 0, 1, 0, 1];

        let col_type = Arc::new(
            Type::primitive_type_builder("sym", PhysicalType::BYTE_ARRAY)
                // We need parquet fields to have an assigned ID
                // This gets matched up against the QuestDB column type in the JSON metadata.
                .with_id(Some(0))
                .with_converted_type(ConvertedType::UTF8)
                .with_logical_type(Some(LogicalType::String))
                .build()?,
        );

        let schema_type = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col_type])
                .build()?,
        );

        let kv_metadata = qdb_metadata
            .map(|qdb_meta_string| vec![KeyValue::new("questdb".to_string(), qdb_meta_string)]);

        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(true)
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_key_value_metadata(kv_metadata)
                .build(),
        );

        let mut cursor = Cursor::new(Vec::new());
        let mut file_writer = SerializedFileWriter::new(&mut cursor, schema_type, props)?;

        let mut row_group_writer = file_writer.next_row_group()?;
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_writer = col_writer.typed::<ByteArrayType>();
            typed_writer.write_batch(&symbol_col_data, Some(&def_levels), None)?;
            col_writer.close()?;
        }
        row_group_writer.close()?;
        file_writer.close()?;

        Ok((cursor.into_inner(), symbol_col_data.len()))
    }

    // --- Helper: build a parquet file with a single Int64 column and bloom filters ---
    fn gen_i64_parquet_with_bloom(values: &[i64]) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::INT64)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_ndv(values.len() as u64)
                .set_bloom_filter_fpp(0.01)
                .set_bloom_filter_position(BloomFilterPosition::AfterRowGroup)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int64Type>();
            tw.write_batch(values, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    fn gen_string_parquet_with_bloom(values: &[&str]) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .with_logical_type(Some(LogicalType::String))
                .with_converted_type(ConvertedType::UTF8)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(false)
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_ndv(values.len() as u64)
                .set_bloom_filter_fpp(0.01)
                .set_bloom_filter_position(BloomFilterPosition::AfterRowGroup)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<ByteArrayType>();
            let ba: Vec<ByteArray> = values.iter().map(|s| ByteArray::from(*s)).collect();
            tw.write_batch(&ba, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    fn make_i64_filter(values: &[i64]) -> (ParquetColumnIndex, ColumnFilterValues) {
        (
            0,
            ColumnFilterValues {
                count: values.len() as u32,
                ptr: values.as_ptr() as u64,
            },
        )
    }

    fn make_string_filter_buf(values: &[&str]) -> Vec<u8> {
        let mut buf = Vec::new();
        for s in values {
            buf.extend_from_slice(&(s.len() as i32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        buf
    }

    fn read_decoder(buf: &[u8]) -> ParquetResult<ParquetDecoder> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut reader = Cursor::new(buf);
        ParquetDecoder::read(allocator, &mut reader, buf.len() as u64)
    }

    #[test]
    fn skip_row_group_bloom_filter_i64_all_absent() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;
        let filter_vals: Vec<i64> = vec![0, 1, 2, 999];
        let filters = [make_i64_filter(&filter_vals)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(
            can_skip,
            "should skip: none of the filter values are in the row group"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_bloom_filter_i64_some_present() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;
        let filter_vals: Vec<i64> = vec![0, 105, 999];
        let filters = [make_i64_filter(&filter_vals)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(!can_skip, "should not skip: 105 is in the row group");
        Ok(())
    }

    #[test]
    fn skip_row_group_min_max_i64() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::INT64)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_bloom_filter_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int64Type>();
            tw.write_batch(&data, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        let buf = cursor.into_inner();
        let decoder = read_decoder(&buf)?;

        // All filter values are outside [100, 109].
        let filter_vals: Vec<i64> = vec![0, 50, 200];
        let filters = [make_i64_filter(&filter_vals)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(can_skip);

        // One filter value is inside [100, 109].
        let filter_vals: Vec<i64> = vec![0, 105, 200];
        let filters = [make_i64_filter(&filter_vals)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(!can_skip);

        Ok(())
    }

    #[test]
    fn skip_row_group_bloom_filter_string_all_absent() -> ParquetResult<()> {
        let data = vec!["alice", "bob", "charlie", "diana"];
        let buf = gen_string_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;

        let filter_buf = make_string_filter_buf(&["xyz", "unknown"]);
        let filters = [(
            0 as ParquetColumnIndex,
            ColumnFilterValues { count: 2, ptr: filter_buf.as_ptr() as u64 },
        )];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(
            can_skip,
            "should skip: none of the filter strings are in the row group"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_bloom_filter_string_some_present() -> ParquetResult<()> {
        let data = vec!["alice", "bob", "charlie", "diana"];
        let buf = gen_string_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;

        let filter_buf = make_string_filter_buf(&["xyz", "bob"]);
        let filters = [(
            0 as ParquetColumnIndex,
            ColumnFilterValues { count: 2, ptr: filter_buf.as_ptr() as u64 },
        )];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(!can_skip, "should not skip: 'bob' is in the row group");
        Ok(())
    }

    #[test]
    fn skip_row_group_and_filters_any_column_skips() -> ParquetResult<()> {
        // if one column's filter proves no match, the row group is skipped.
        let col_a = Arc::new(
            Type::primitive_type_builder("a", PhysicalType::INT64)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let col_b = Arc::new(
            Type::primitive_type_builder("b", PhysicalType::INT64)
                .with_id(Some(1))
                .with_repetition(parquet::basic::Repetition::REQUIRED)
                .build()
                .unwrap(),
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col_a, col_b])
                .build()
                .unwrap(),
        );
        let props = Arc::new(
            WriterProperties::builder()
                .set_bloom_filter_enabled(true)
                .set_bloom_filter_ndv(10)
                .set_bloom_filter_fpp(0.01)
                .set_bloom_filter_position(BloomFilterPosition::AfterRowGroup)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        // column a: 100..110
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int64Type>();
            let vals_a: Vec<i64> = (100..110).collect();
            tw.write_batch(&vals_a, None, None)?;
            cw.close()?;
        }
        // column b: 200..210
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int64Type>();
            let vals_b: Vec<i64> = (200..210).collect();
            tw.write_batch(&vals_b, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        let buf = cursor.into_inner();
        let decoder = read_decoder(&buf)?;

        // Filter on column a: value 105 IS present.
        // Filter on column b: value 999 is NOT present.
        // AND semantics: column b proves no match → skip.
        let fa: Vec<i64> = vec![105];
        let fb: Vec<i64> = vec![999];
        let filters = [
            (
                0 as ParquetColumnIndex,
                ColumnFilterValues { count: fa.len() as u32, ptr: fa.as_ptr() as u64 },
            ),
            (
                1 as ParquetColumnIndex,
                ColumnFilterValues { count: fb.len() as u32, ptr: fb.as_ptr() as u64 },
            ),
        ];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters)?;
        assert!(can_skip);

        // Both columns have matching values → cannot skip.
        let fa2: Vec<i64> = vec![105];
        let fb2: Vec<i64> = vec![205];
        let filters2 = [
            (
                0 as ParquetColumnIndex,
                ColumnFilterValues { count: fa2.len() as u32, ptr: fa2.as_ptr() as u64 },
            ),
            (
                1 as ParquetColumnIndex,
                ColumnFilterValues { count: fb2.len() as u32, ptr: fb2.as_ptr() as u64 },
            ),
        ];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters2)?;
        assert!(!can_skip,);
        Ok(())
    }

    #[test]
    fn skip_row_group_invalid_index_returns_error() {
        let data: Vec<i64> = (0..10).collect();
        let buf = gen_i64_parquet_with_bloom(&data).unwrap();
        let decoder = read_decoder(&buf).unwrap();

        let filter_vals: Vec<i64> = vec![0];
        let filters = [make_i64_filter(&filter_vals)];
        let result = decoder.can_skip_row_group(99, &buf, &filters);
        assert!(result.is_err(), "should error on invalid row group index");
    }
}
