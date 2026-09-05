use crate::allocator::AcVec;
use qdb_core::col_type::ColumnType;

pub mod column_sink;
pub mod decode;
pub mod decode_column;
pub mod decoders;
pub mod jni;
pub mod meta;
pub mod page;
pub mod parquet_meta_decode;
pub mod row_groups;
pub mod slicer;

pub use row_groups::{ParquetDecoder, RowGroupBuffers};

#[repr(C)]
pub struct DecodeContext {
    pub file_ptr: *const u8,
    pub file_size: u64,
    pub dict_decompress_buffer: Vec<u8>,
    pub decompress_buffer: Vec<u8>,
    pub varchar_slice_buf_pool: Vec<Vec<u8>>,
    /// Scratch outer-vec for varchar_slice data-page buffers, hoisted out of the
    /// per-column-chunk decode loop so the heap-allocated outer storage is reused
    /// across calls. The inner `Vec<u8>` buffers are still moved into
    /// `ColumnChunkBuffers::page_buffers` at the end of the column-chunk decode.
    pub varchar_slice_page_bufs_scratch: Vec<Vec<u8>>,
    /// Scratch outer-vec for varchar_slice dict-page buffers, hoisted for the same
    /// reason as `varchar_slice_page_bufs_scratch`.
    pub varchar_slice_dict_bufs_scratch: Vec<Vec<u8>>,
}

impl DecodeContext {
    pub fn new(file_ptr: *const u8, file_size: u64) -> Self {
        Self {
            file_ptr,
            file_size,
            dict_decompress_buffer: Vec::new(),
            decompress_buffer: Vec::new(),
            varchar_slice_buf_pool: Vec::new(),
            varchar_slice_page_bufs_scratch: Vec::new(),
            varchar_slice_dict_bufs_scratch: Vec::new(),
        }
    }
}

/// Scope guard that empties the VarcharSlice reuse pool and scratch vecs when it
/// drops, so a row-group decode releases them on success and error paths alike.
///
/// During an untracked decode the column loop parks a column's old
/// `page_buffers` in `varchar_slice_buf_pool` so the next column can reuse them.
/// A tracker-bound decode drops old buffers before crediting their physical
/// capacity, preserving the hard ceiling. Both modes stage in-flight page/dict
/// buffers in the scratch vecs. On a successful return the
/// scratch vecs have already drained into `ColumnChunkBuffers::page_buffers`
/// (counted by the Java byte budget via `page_buffers_size`) and only unused
/// spares remain in the pool, so the drop is equivalent to the previous explicit
/// end-of-decode pool clear. On an error return the parked and in-flight buffers
/// are still in the context; without the guard they would survive as RSS
/// invisible to the cache budget until the decoder is destroyed, because Java
/// only closes the `RowGroupBuffers` shell when a decode fails — the configured
/// cache budget could read zero while the context still held varchar page bytes.
///
/// Clearing the scratch vecs on an error frees buffers that the failed chunk's
/// aux entries may still point into. That is safe because no caller reads a
/// chunk after a failed decode (the Java cache evicts and closes the shell),
/// and the next decode would free those buffers anyway when it clears the
/// scratch vecs at the start of each column chunk.
///
/// Same-slot reuse is preserved for untracked decodes: the next decode re-parks
/// the live `page_buffers` it drains from `ColumnChunkBuffers`.
pub struct VarcharSliceBufGuard<'a> {
    ctx: &'a mut DecodeContext,
}

impl<'a> VarcharSliceBufGuard<'a> {
    pub fn new(ctx: &'a mut DecodeContext) -> Self {
        Self { ctx }
    }

    /// Reborrows the guarded context for the decode body.
    pub fn ctx(&mut self) -> &mut DecodeContext {
        self.ctx
    }
}

impl Drop for VarcharSliceBufGuard<'_> {
    fn drop(&mut self) {
        self.ctx.varchar_slice_buf_pool.clear();
        self.ctx.varchar_slice_page_bufs_scratch.clear();
        self.ctx.varchar_slice_dict_bufs_scratch.clear();
    }
}

pub const FILTER_OP_EQ: u8 = 0;
pub const FILTER_OP_LT: u8 = 1;
pub const FILTER_OP_LE: u8 = 2;
pub const FILTER_OP_GT: u8 = 3;
pub const FILTER_OP_GE: u8 = 4;
pub const FILTER_OP_IS_NULL: u8 = 5;
pub const FILTER_OP_IS_NOT_NULL: u8 = 6;
pub const FILTER_OP_BETWEEN: u8 = 7;

/// Milliseconds per day, used to convert QuestDB DATE (millis) to Parquet DATE (days).
pub(crate) const MILLIS_PER_DAY: i64 = 86_400_000;

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ColumnFilterPacked {
    pub col_idx_and_count: u64,
    pub ptr: u64,
    pub column_type: u64,
}

impl ColumnFilterPacked {
    #[inline]
    pub fn column_index(&self) -> u32 {
        (self.col_idx_and_count & 0xFFFFFFFF) as u32
    }

    #[inline]
    pub fn count(&self) -> u32 {
        ((self.col_idx_and_count >> 32) & 0x00FFFFFF) as u32
    }

    #[inline]
    pub fn operation_type(&self) -> u8 {
        ((self.col_idx_and_count >> 56) & 0xFF) as u8
    }

    #[inline]
    pub fn qdb_column_type(&self) -> i32 {
        self.column_type as i32
    }
}

#[repr(C)]
#[derive(Copy, Clone)]
pub struct ColumnFilterValues {
    pub count: u32,
    pub ptr: u64,
    pub buf_end: u64,
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

    /// Total `len()` of the buffers retained in `page_buffers`. For VarcharSlice
    /// columns under the Plain / DeltaLengthByteArray / dictionary encodings the
    /// decoded string bytes live in these retained pages (the aux entries hold
    /// pointers into them) while `data_vec` stays empty, so this is the decode's
    /// heap footprint beyond `data_size` + `aux_size`. Java's decode-cache byte
    /// budget adds it so VarcharSlice frames are not undercounted. Refreshed by
    /// `refresh_ptrs` at the end of each column-chunk decode; 0 for every other
    /// column type (and for uncompressed pages borrowed from the mmap).
    pub page_buffers_size: usize,
    pub page_buffers: Vec<Vec<u8>>,

    /// Number of leading column-top rows in the decoded chunk. For a source type with no
    /// in-band null sentinel (BYTE/SHORT/CHAR) these are its only nulls, so Java consults
    /// this count to surface NULL for column-top rows during a lazy fixed->var conversion
    /// (the in-band decoded value is 0, indistinguishable from a real 0). Read from Java via
    /// the `chunkColumnTopOffset` field offset. 0 when there is no column top.
    pub column_top: usize,

    /// Allocated capacity of `page_buffers` last published by `refresh_ptrs`.
    /// This is tracked separately from the exposed logical byte size so spare
    /// system-allocator capacity cannot escape a hard query-memory ceiling.
    /// Not read from Java.
    page_buffers_capacity: usize,

    /// Bytes of `page_buffers` currently charged against the per-query memory
    /// tracker. The payload is backed by the system allocator, not the
    /// tracker-aware `AcVec`, so VarcharSlice decompression reserves it explicitly
    /// before allowing a buffer to grow. `refresh_ptrs` reconciles the published
    /// payload, and `reset` / `Drop` credit it back. Always equals the amount this
    /// chunk added to the tracker; 0 when nothing is charged. Not read from Java.
    page_buffers_charged: usize,

    /// Count of `page_buffers` entries already summed into `page_buffers_size`.
    /// `refresh_ptrs` adds only the buffers appended since the last refresh
    /// instead of re-summing the whole vector, keeping a multi-row-group decode
    /// (decode_row_group_range concatenates into the same buffers without
    /// resetting between groups) linear rather than quadratic. Entries below
    /// this index stay immutable once retained -- aux entries hold raw pointers
    /// into them -- so the partial sum is exact; a shrink (len below this index)
    /// falls back to a full recompute, and `reset` zeroes it. Not read from Java.
    page_buffers_counted: usize,
}

#[cfg(test)]
mod tests {
    use crate::allocator::TestAllocatorState;
    use crate::parquet::error::ParquetResult;
    use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_read::{
        ColumnFilterPacked, DecodeContext, ParquetDecoder, RowGroupBuffers, FILTER_OP_BETWEEN,
        FILTER_OP_EQ, FILTER_OP_GE, FILTER_OP_GT, FILTER_OP_IS_NOT_NULL, FILTER_OP_IS_NULL,
        FILTER_OP_LE, FILTER_OP_LT,
    };
    use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::{BloomFilterPosition, WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use parquet2::metadata::{ColumnChunkMetaData, RowGroupMetaData};
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
                ascii: None,
                id: None,
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

    #[test]
    fn decode_row_group_error_releases_varchar_slice_bufs() -> ParquetResult<()> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut qdb_meta = QdbMeta::new(1);
        qdb_meta.schema.insert(
            0,
            QdbMetaCol {
                column_type: ColumnTypeTag::Symbol.into_type(),
                column_top: 0,
                format: None, // Missing format makes the page decode fail mid-chunk.
                ascii: None,
                id: None,
            },
        );

        let (buf, row_count) = gen_test_symbol_parquet(Some(qdb_meta.serialize()?))?;
        let buf_len = buf.len() as u64;

        let mut reader = Cursor::new(&buf);
        let parquet_decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)?;
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);

        // Simulate buffers parked or staged by an in-flight decode.
        ctx.varchar_slice_buf_pool.push(vec![0u8; 4096]);
        ctx.varchar_slice_page_bufs_scratch.push(vec![0u8; 1024]);
        ctx.varchar_slice_dict_bufs_scratch.push(vec![0u8; 1024]);

        let res = parquet_decoder.decode_row_group(
            &mut ctx,
            &mut rgb,
            &[(0, ColumnTypeTag::Symbol.into_type())],
            0,
            0,
            row_count as u32,
        );
        assert!(res.is_err());
        assert!(
            ctx.varchar_slice_buf_pool.is_empty(),
            "a failed row-group decode must release the varchar-slice reuse pool"
        );
        assert!(
            ctx.varchar_slice_page_bufs_scratch.is_empty(),
            "a failed row-group decode must release the page-buffer scratch"
        );
        assert!(
            ctx.varchar_slice_dict_bufs_scratch.is_empty(),
            "a failed row-group decode must release the dict-buffer scratch"
        );

        Ok(())
    }

    #[test]
    fn decode_row_group_filtered_error_releases_varchar_slice_bufs() -> ParquetResult<()> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut qdb_meta = QdbMeta::new(1);
        qdb_meta.schema.insert(
            0,
            QdbMetaCol {
                column_type: ColumnTypeTag::Symbol.into_type(),
                column_top: 0,
                format: None, // Missing format makes the page decode fail mid-chunk.
                ascii: None,
                id: None,
            },
        );

        let (buf, row_count) = gen_test_symbol_parquet(Some(qdb_meta.serialize()?))?;
        let buf_len = buf.len() as u64;

        let mut reader = Cursor::new(&buf);
        let parquet_decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)?;
        let mut rgb = RowGroupBuffers::new(allocator);
        let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);

        // Simulate buffers parked or staged by an in-flight decode.
        ctx.varchar_slice_buf_pool.push(vec![0u8; 4096]);
        ctx.varchar_slice_page_bufs_scratch.push(vec![0u8; 1024]);
        ctx.varchar_slice_dict_bufs_scratch.push(vec![0u8; 1024]);

        let res = parquet_decoder.decode_row_group_filtered::<false>(
            &mut ctx,
            &mut rgb,
            0,
            &[(0, ColumnTypeTag::Symbol.into_type())],
            0,
            0,
            row_count as u32,
            &[0, 1],
        );
        assert!(res.is_err());
        assert!(
            ctx.varchar_slice_buf_pool.is_empty(),
            "a failed filtered decode must release the varchar-slice reuse pool"
        );
        assert!(
            ctx.varchar_slice_page_bufs_scratch.is_empty(),
            "a failed filtered decode must release the page-buffer scratch"
        );
        assert!(
            ctx.varchar_slice_dict_bufs_scratch.is_empty(),
            "a failed filtered decode must release the dict-buffer scratch"
        );

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

    fn make_filter(column_index: u32, count: usize, ptr: u64) -> ColumnFilterPacked {
        make_filter_with_op(column_index, count, ptr, FILTER_OP_EQ)
    }

    fn make_filter_with_op(
        column_index: u32,
        count: usize,
        ptr: u64,
        op: u8,
    ) -> ColumnFilterPacked {
        ColumnFilterPacked {
            col_idx_and_count: (column_index as u64)
                | (((count as u64) & 0x00FFFFFF) << 32)
                | ((op as u64) << 56),
            ptr,
            column_type: 0,
        }
    }

    fn make_i64_filter(values: &[i64]) -> ColumnFilterPacked {
        make_filter(0, values.len(), values.as_ptr() as u64)
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
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
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
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
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
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
        assert!(can_skip);

        // One filter value is inside [100, 109].
        let filter_vals: Vec<i64> = vec![0, 105, 200];
        let filters = [make_i64_filter(&filter_vals)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
        assert!(!can_skip);

        Ok(())
    }

    #[test]
    fn skip_row_group_bloom_filter_string_all_absent() -> ParquetResult<()> {
        let data = vec!["alice", "bob", "charlie", "diana"];
        let buf = gen_string_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;

        let filter_buf = make_string_filter_buf(&["xyz", "unknown"]);
        let filters = [make_filter(0, 2, filter_buf.as_ptr() as u64)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
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
        let filters = [make_filter(0, 2, filter_buf.as_ptr() as u64)];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
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
            make_filter(0, fa.len(), fa.as_ptr() as u64),
            make_filter(1, fb.len(), fb.as_ptr() as u64),
        ];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?;
        assert!(can_skip);

        // Both columns have matching values → cannot skip.
        let fa2: Vec<i64> = vec![105];
        let fb2: Vec<i64> = vec![205];
        let filters2 = [
            make_filter(0, fa2.len(), fa2.as_ptr() as u64),
            make_filter(1, fb2.len(), fb2.as_ptr() as u64),
        ];
        let can_skip = decoder.can_skip_row_group(0, &buf, &filters2, u64::MAX)?;
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
        let result = decoder.can_skip_row_group(99, &buf, &filters, u64::MAX);
        assert!(result.is_err(), "should error on invalid row group index");
    }

    fn gen_i64_parquet_with_stats(values: &[i64]) -> ParquetResult<Vec<u8>> {
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
                .set_bloom_filter_enabled(false)
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

    fn gen_nullable_i64_parquet(values: &[i64], def_levels: &[i16]) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::INT64)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
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
            tw.write_batch(values, Some(def_levels), None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    #[test]
    fn skip_row_group_range_lt_below_min() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col < 100: min=100, so min >= val → skip
        let filter_vals: Vec<i64> = vec![100];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_LT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col < 99: min=100 >= 99 → skip
        let filter_vals: Vec<i64> = vec![99];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_LT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col < 101: min=100 < 101 → cannot skip
        let filter_vals: Vec<i64> = vec![101];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_LT,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_le_below_min() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col <= 99: min=100 > 99 → skip
        let filter_vals: Vec<i64> = vec![99];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_LE,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col <= 100: min=100 > 100 is false → cannot skip
        let filter_vals: Vec<i64> = vec![100];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_LE,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_gt_above_max() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col > 109: max=109, so max <= val → skip
        let filter_vals: Vec<i64> = vec![109];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_GT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > 110: max=109 <= 110 → skip
        let filter_vals: Vec<i64> = vec![110];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_GT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > 108: max=109 <= 108 is false → cannot skip
        let filter_vals: Vec<i64> = vec![108];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_GT,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_ge_above_max() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col >= 110: max=109 < 110 → skip
        let filter_vals: Vec<i64> = vec![110];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_GE,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col >= 109: max=109 < 109 is false → cannot skip
        let filter_vals: Vec<i64> = vec![109];
        let filters = [make_filter_with_op(
            0,
            1,
            filter_vals.as_ptr() as u64,
            FILTER_OP_GE,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_is_null_no_nulls() -> ParquetResult<()> {
        // All non-null: null_count=0 → IS NULL should skip
        let data: Vec<i64> = (100..110).collect();
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        let filters = [make_filter_with_op(0, 0, 0, FILTER_OP_IS_NULL)];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NULL should skip when null_count=0"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_is_null_has_nulls() -> ParquetResult<()> {
        // Some nulls present: IS NULL should not skip
        let data: Vec<i64> = vec![100, 200];
        let def_levels: Vec<i16> = vec![1, 0, 1]; // 3 rows: non-null, null, non-null
        let buf = gen_nullable_i64_parquet(&data, &def_levels)?;
        let decoder = read_decoder(&buf)?;

        let filters = [make_filter_with_op(0, 0, 0, FILTER_OP_IS_NULL)];
        assert!(
            !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NULL should not skip when nulls exist"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_is_null_char_with_stored_zero() -> ParquetResult<()> {
        // A CHAR NULL is (char) 0, an in-domain value the writer stores at definition level 1,
        // so null_count stays 0 while the row IS null to QuestDB. Skipping on that count drops
        // every row native storage returns. A REQUIRED column reproduces it exactly: no row can
        // reach definition level 0, so null_count is Some(0) whatever the values are.
        //
        // This covers ParquetDecoder::can_skip_row_group, the arm read_parquet() takes. Its twin in
        // parquet_metadata::skip is what a CONVERT PARTITION TO PARQUET table reads, and the two
        // must agree - see writer_undercounts_nulls.
        let buf = gen_i32_parquet_with_stats(&[0, 97, 98])?;
        let decoder = read_decoder(&buf)?;

        let filters = [make_filter_with_op_and_type(
            0,
            0,
            0,
            FILTER_OP_IS_NULL,
            ColumnTypeTag::Char as i32,
        )];
        assert!(
            !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NULL must not skip a CHAR group: null_count cannot see a stored (char) 0"
        );

        // An INT group of the same shape has no such hidden null, so it still skips - the gate is
        // per type, not a blanket surrender of the IS NULL skip.
        let filters = [make_filter_with_op_and_type(
            0,
            0,
            0,
            FILTER_OP_IS_NULL,
            ColumnTypeTag::Int as i32,
        )];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NULL should still skip an INT group whose null_count is 0"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_eq_null_double_with_infinity() -> ParquetResult<()> {
        // Numbers.isNull(double) is an exponent-bits test, so QuestDB calls +/-Infinity NULL and
        // Numbers.equals calls it EQUAL to a NaN bound - `d = null::double` matches that row
        // natively. The writer counts only is_nan(), so null_count stays 0 and the EQ arm, which
        // keeps the group only `if has_nulls`, pruned the row away.
        //
        // Same division as the CHAR test above: this is the read_parquet() arm.
        let buf = gen_f64_parquet_with_stats(&[1.0, f64::INFINITY])?;
        let decoder = read_decoder(&buf)?;

        let v: Vec<f64> = vec![f64::NAN];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Double as i32,
        )];
        assert!(
            !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "EQ on a NULL bound must not skip a DOUBLE group: an infinity is an uncounted null"
        );

        // A finite bound outside the range still prunes, so the group did not simply become
        // unprunable.
        let v: Vec<f64> = vec![-5.0];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Double as i32,
        )];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "a finite bound outside [min, max] should still skip"
        );
        Ok(())
    }

    #[test]
    fn skip_row_group_is_not_null_all_nulls() -> ParquetResult<()> {
        // All nulls: null_count == num_values → IS NOT NULL should skip
        let data: Vec<i64> = vec![];
        let def_levels: Vec<i16> = vec![0, 0, 0]; // 3 rows, all null
        let buf = gen_nullable_i64_parquet(&data, &def_levels)?;
        let decoder = read_decoder(&buf)?;

        let filters = [make_filter_with_op(0, 0, 0, FILTER_OP_IS_NOT_NULL)];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NOT NULL should skip when all values are null"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_is_not_null_has_non_nulls() -> ParquetResult<()> {
        // Some non-null values: IS NOT NULL should not skip
        let data: Vec<i64> = vec![100];
        let def_levels: Vec<i16> = vec![1, 0, 0]; // 3 rows: non-null, null, null
        let buf = gen_nullable_i64_parquet(&data, &def_levels)?;
        let decoder = read_decoder(&buf)?;

        let filters = [make_filter_with_op(0, 0, 0, FILTER_OP_IS_NOT_NULL)];
        assert!(
            !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "IS NOT NULL should not skip when some non-nulls exist"
        );
        Ok(())
    }

    #[test]
    fn no_skip_row_group_is_not_null_over_column_top_of_null_free_type() -> ParquetResult<()> {
        // BOOLEAN, BYTE and SHORT carry no NULL sentinel, so `IS NOT NULL` is a constant TRUE over
        // them: a column-top row decodes back as 0/false and native storage returns it. The writer
        // still records that row at definition level 0, so a row group lying wholly inside a column
        // top reports null_count == num_values, which this arm otherwise reads as "every value is
        // null" and skips. See ParquetDecoder::is_null_free_type.
        //
        // This test is the guard's only coverage on this arm - ParquetDecoder::can_skip_row_group,
        // the one read_parquet() takes. Its twin in parquet_metadata::skip, which a CONVERT
        // PARTITION TO PARQUET table reads, carries its own guard coverage in
        // parquet_metadata::skip::tests::no_skip_is_not_null_over_column_top_of_null_free_type.
        // PushdownFilterExtractor.isNullOpPushable refuses a null op for these three types, so no
        // SQL query reaches the guard - see its doc comment for why opening that gate would recover
        // nothing.
        let data: Vec<i64> = vec![];
        let def_levels: Vec<i16> = vec![0, 0, 0]; // 3 rows, all column top
        let buf = gen_nullable_i64_parquet(&data, &def_levels)?;
        let decoder = read_decoder(&buf)?;

        // The arm answers from the null count alone and returns before it reads the physical type,
        // so an INT64 file carries these narrower QuestDB types faithfully for this branch.
        for tag in [
            ColumnTypeTag::Boolean,
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
        ] {
            let filters = [make_filter_with_op_and_type(
                0,
                0,
                0,
                FILTER_OP_IS_NOT_NULL,
                tag as i32,
            )];
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{tag:?}: IS NOT NULL must not skip a group lying wholly inside a column top"
            );
        }

        // CHAR shares the zero decoding, but `(char) 0` genuinely IS its NULL, and every other type
        // reaches definition level 0 only for a real NULL. The skip stays right for all of them, so
        // the guard costs only the pruning that was wrong.
        for tag in [ColumnTypeTag::Char, ColumnTypeTag::Int, ColumnTypeTag::Long] {
            let filters = [make_filter_with_op_and_type(
                0,
                0,
                0,
                FILTER_OP_IS_NOT_NULL,
                tag as i32,
            )];
            assert!(
                decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{tag:?}: an all-null group should still skip for IS NOT NULL"
            );
        }
        Ok(())
    }

    #[test]
    fn skip_row_group_string_range() -> ParquetResult<()> {
        let data = vec!["alice", "bob", "charlie"];
        let buf = gen_string_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;

        // col > "zzz": max is "charlie", "charlie" <= "zzz" → skip
        let filter_buf = make_string_filter_buf(&["zzz"]);
        let filters = [make_filter_with_op(
            0,
            1,
            filter_buf.as_ptr() as u64,
            FILTER_OP_GT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col < "aaa": min is "alice", "alice" >= "aaa" → skip
        let filter_buf = make_string_filter_buf(&["aaa"]);
        let filters = [make_filter_with_op(
            0,
            1,
            filter_buf.as_ptr() as u64,
            FILTER_OP_LT,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > "bob": max is "charlie", "charlie" > "bob" → cannot skip
        let filter_buf = make_string_filter_buf(&["bob"]);
        let filters = [make_filter_with_op(
            0,
            1,
            filter_buf.as_ptr() as u64,
            FILTER_OP_GT,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    // Helper functions for additional type coverage
    fn gen_i32_parquet_with_stats(values: &[i32]) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::INT32)
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
                .set_bloom_filter_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int32Type>();
            tw.write_batch(values, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    fn gen_f64_parquet_with_stats(values: &[f64]) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::DOUBLE)
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
                .set_bloom_filter_enabled(false)
                .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk)
                .build(),
        );
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, props)?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::DoubleType>();
            tw.write_batch(values, None, None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    fn make_filter_with_op_and_type(
        column_index: u32,
        count: usize,
        ptr: u64,
        op: u8,
        column_type: i32,
    ) -> ColumnFilterPacked {
        ColumnFilterPacked {
            col_idx_and_count: (column_index as u64)
                | (((count as u64) & 0x00FFFFFF) << 32)
                | ((op as u64) << 56),
            ptr,
            column_type: column_type as u64,
        }
    }

    /// Writes a nullable INT32 column. A `def_levels` entry of 0 is the definition level a QuestDB
    /// column top is stored at, so the row group carries both real values and rows a BYTE, SHORT or
    /// CHAR reader decodes back as 0.
    fn gen_nullable_i32_parquet(
        values: &[i32],
        def_levels: &[i16],
        with_bloom: bool,
    ) -> ParquetResult<Vec<u8>> {
        let col = Arc::new(
            Type::primitive_type_builder("val", PhysicalType::INT32)
                .with_id(Some(0))
                .with_repetition(parquet::basic::Repetition::OPTIONAL)
                .build()?,
        );
        let schema = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col])
                .build()?,
        );
        let mut props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_bloom_filter_enabled(with_bloom)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Chunk);
        if with_bloom {
            props = props
                .set_bloom_filter_ndv(values.len() as u64)
                .set_bloom_filter_fpp(0.001)
                .set_bloom_filter_position(BloomFilterPosition::AfterRowGroup);
        }
        let mut cursor = Cursor::new(Vec::new());
        let mut fw = SerializedFileWriter::new(&mut cursor, schema, Arc::new(props.build()))?;
        let mut rg = fw.next_row_group()?;
        if let Some(mut cw) = rg.next_column()? {
            let tw = cw.typed::<parquet::data_type::Int32Type>();
            tw.write_batch(values, Some(def_levels), None)?;
            cw.close()?;
        }
        rg.close()?;
        fw.close()?;
        Ok(cursor.into_inner())
    }

    /// The tags [`crate::parquet_read::ParquetDecoder::has_matchable_zero_nulls`] reports: their
    /// column tops sit at definition level 0 yet decode back as 0, so 0 is a value the row group
    /// can match even though no statistic and no bloom entry mentions it.
    fn zero_decoding_tags() -> [(ColumnTypeTag, &'static str); 3] {
        [
            (ColumnTypeTag::Byte, "BYTE"),
            (ColumnTypeTag::Short, "SHORT"),
            (ColumnTypeTag::Char, "CHAR"),
        ]
    }

    /// Covers the min/max half of the column-top zero guard on the `read_parquet()` arm, the one
    /// `widen_int32_stats_to_include_zero` implements.
    ///
    /// A BYTE, SHORT or CHAR column top is written at definition level 0 and decodes back as 0, but
    /// it never reaches the statistics: a row group holding `[5, 6]` plus a column top reports
    /// min=5. `WHERE c = 0` and `WHERE c < 1` both match those rows natively, so pruning on the raw
    /// statistics dropped rows native storage returns.
    ///
    /// Its twin in `parquet_metadata::skip` calls the same widening helper for the
    /// CONVERT PARTITION TO PARQUET path, and the two must agree.
    #[test]
    fn no_skip_row_group_zero_matching_filter_over_column_top() -> ParquetResult<()> {
        // 5 rows: two values, three column-top rows. Statistics report min=5, max=6, null_count=3.
        let buf = gen_nullable_i32_parquet(&[5, 6], &[0, 1, 1, 0, 0], false)?;
        let decoder = read_decoder(&buf)?;

        for (tag, name) in zero_decoding_tags() {
            // c = 0 matches every column-top row, so the group must survive.
            let v: Vec<i32> = vec![0];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_EQ,
                tag as i32,
            )];
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: EQ 0 must not skip a group whose column top decodes as 0"
            );

            // c < 1 matches them too, and a raw min of 5 says otherwise.
            let v: Vec<i32> = vec![1];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_LT,
                tag as i32,
            )];
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: LT 1 must not skip a group whose column top decodes as 0"
            );

            // Widening reaches 0 and stops there: a bound outside [0, 6] still prunes, so the
            // guard costs only the pruning that was wrong.
            let v: Vec<i32> = vec![7];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_EQ,
                tag as i32,
            )];
            assert!(
                decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: EQ 7 lies outside the widened [0, 6] and should still skip"
            );

            let v: Vec<i32> = vec![6];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_GT,
                tag as i32,
            )];
            assert!(
                decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: GT 6 lies above the widened max of 6 and should still skip"
            );
        }

        // An INT column top decodes as i32::MIN, a genuine NULL, so nothing is widened and EQ 0
        // still prunes. The guard is per type, not a blanket surrender of the skip.
        let v: Vec<i32> = vec![0];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Int as i32,
        )];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "EQ 0 should still skip an INT group: its column top is not a zero"
        );
        Ok(())
    }

    /// Covers the bloom half of the same guard on the `read_parquet()` arm: the
    /// `has_implicit_zeros` arm of `all_values_absent_from_bloom`.
    ///
    /// A bloom filter stores exact hashes, so widening the statistics cannot express the implicit
    /// zero. The column-top rows were never hashed into the set, a 0 probe therefore reports
    /// absent, and the EQ arm skips on that answer before it ever consults min/max.
    #[test]
    fn no_skip_row_group_bloom_zero_probe_over_column_top() -> ParquetResult<()> {
        let values: Vec<i32> = vec![5, 6, 7, 9, 11, 13, 15, 17, 19, 21];
        let mut def_levels: Vec<i16> = vec![1; values.len()];
        def_levels.extend_from_slice(&[0, 0, 0]);
        let buf = gen_nullable_i32_parquet(&values, &def_levels, true)?;
        let decoder = read_decoder(&buf)?;

        // The bloom filter is live and doing the pruning: 8 lies inside [5, 21], so min/max cannot
        // prune it, yet the set reports it absent and the group is skipped on that alone. Without
        // this the assertions below would pass on an empty bitset and prove nothing.
        let v: Vec<i32> = vec![8];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Int as i32,
        )];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "the bloom filter must prune 8, which lies inside [5, 21]"
        );

        for (tag, name) in zero_decoding_tags() {
            // 0 is absent from the set, yet three column-top rows decode as 0.
            let v: Vec<i32> = vec![0];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_EQ,
                tag as i32,
            )];
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: a 0 probe must not prune on a bloom set the column top never entered"
            );

            // Only the 0 probe is spared: any other absent value still prunes on the bloom answer.
            let v: Vec<i32> = vec![8];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_EQ,
                tag as i32,
            )];
            assert!(
                decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: EQ 8 is absent from the set and should still skip"
            );

            // A value the set does hold keeps the group, as it always did.
            let v: Vec<i32> = vec![13];
            let filters = [make_filter_with_op_and_type(
                0,
                1,
                v.as_ptr() as u64,
                FILTER_OP_EQ,
                tag as i32,
            )];
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "{name}: EQ 13 is in the row group and must not skip"
            );
        }

        // INT once more: its column top is a genuine NULL, so a 0 probe prunes on the bloom answer.
        let v: Vec<i32> = vec![0];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Int as i32,
        )];
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "EQ 0 should still skip an INT group: no column-top zero was left out of the set"
        );
        Ok(())
    }

    #[test]
    fn skip_row_group_range_i32() -> ParquetResult<()> {
        let data: Vec<i32> = (10..20).collect(); // min=10, max=19
        let buf = gen_i32_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col < 10: min=10 >= 10 → skip
        let v: Vec<i32> = vec![10];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_LT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        let v: Vec<i32> = vec![19];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        let v: Vec<i32> = vec![20];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GE)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        let v: Vec<i32> = vec![9];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_LE)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        let v: Vec<i32> = vec![15];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GT)];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_f64() -> ParquetResult<()> {
        let data: Vec<f64> = vec![1.0, 2.5, 5.0, 7.5, 10.0]; // min=1.0, max=10.0
        let buf = gen_f64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col < 1.0: min=1.0 >= 1.0 → skip
        let v: Vec<f64> = vec![1.0];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_LT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > 10.0: max=10.0 <= 10.0 → skip
        let v: Vec<f64> = vec![10.0];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col >= 5.0: max=10.0 >= 5.0 → cannot skip
        let v: Vec<f64> = vec![5.0];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GE)];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_date_millis_to_days() -> ParquetResult<()> {
        // Parquet stores DATE as INT32 days. Filter values arrive as i64 millis.
        let day_values: Vec<i32> = vec![100, 150, 200]; // min=100, max=200 days
        let buf = gen_i32_parquet_with_stats(&day_values)?;
        let decoder = read_decoder(&buf)?;

        let millis_per_day: i64 = 86_400_000;

        // col > day 200: max=200 <= 200 → skip
        let v: Vec<i64> = vec![200 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_GT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col < day 100: min=100 >= 100 → skip
        let v: Vec<i64> = vec![100 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_LT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col >= day 150: max=200 >= 150 → cannot skip
        let v: Vec<i64> = vec![150 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_GE,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN day 250 AND day 300: [250,300] doesn't overlap [100,200] → skip
        let v: Vec<i64> = vec![250 * millis_per_day, 300 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN day 120 AND day 180: overlaps [100,200] → cannot skip
        let v: Vec<i64> = vec![120 * millis_per_day, 180 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // Sub-day precision: col < (day 100 - 1ms). The filter value
        // 100*86_400_000 - 1 = day 99 end-of-day. min_stat = day 100 =
        // 100*86_400_000 ms > filter value → skip is correct.
        let v: Vec<i64> = vec![100 * millis_per_day - 1];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_LT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // Sub-day precision: col < (day 100 + 1ms). The filter value is
        // just past midnight of day 100. min_stat = day 100 = 100*86_400_000 ms
        // which is < filter value → cannot skip (some rows may match).
        let v: Vec<i64> = vec![100 * millis_per_day + 1];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_LT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // Sub-day precision: col > (day 200 + half day).
        // max_stat = day 200 = 200*86_400_000 ms < filter value → skip.
        let v: Vec<i64> = vec![200 * millis_per_day + millis_per_day / 2];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_GT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // Sub-day BETWEEN: BETWEEN (day 201 - 1ms) AND (day 300).
        // Filter lo = 200*86_400_000 + 86_399_999 ms. Stat max = day 200 =
        // 200*86_400_000 ms < lo → skip.
        let v: Vec<i64> = vec![201 * millis_per_day - 1, 300 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_date_pre_epoch() -> ParquetResult<()> {
        // Pre-epoch dates: day -3, -2, -1 (i.e. 1969-12-29, 30, 31)
        let day_values: Vec<i32> = vec![-3, -2, -1];
        let buf = gen_i32_parquet_with_stats(&day_values)?;
        let decoder = read_decoder(&buf)?;

        let millis_per_day: i64 = 86_400_000;

        // col < -1ms: min_stat = day -3 = -3*86_400_000 ms, filter = -1.
        // -259_200_000 >= -1 is false → cannot skip.
        let v: Vec<i64> = vec![-1];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_LT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > day -1: max_stat = day -1 = -86_400_000 ms, filter = -1*86_400_000.
        // -86_400_000 <= -86_400_000 → skip.
        let v: Vec<i64> = vec![-millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_GT,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col >= day -2: max_stat = day -1 = -86_400_000 ms, filter = -2*86_400_000.
        // -86_400_000 < -172_800_000 is false → cannot skip.
        let v: Vec<i64> = vec![-2 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            v.as_ptr() as u64,
            FILTER_OP_GE,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN day -10 AND day -5: [-10,-5] doesn't overlap [-3,-1] → skip.
        let v: Vec<i64> = vec![-10 * millis_per_day, -5 * millis_per_day];
        let filters = [make_filter_with_op_and_type(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
            ColumnTypeTag::Date as i32,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN day -2 AND day 0: overlaps [-3,-1] → cannot skip.
        let v: Vec<i64> = vec![-2 * millis_per_day, 0];
        let filters = [make_filter_with_op_and_type(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
            ColumnTypeTag::Date as i32,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_between_i64() -> ParquetResult<()> {
        let data: Vec<i64> = (100..110).collect(); // min=100, max=109
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // BETWEEN 200 AND 300: [200,300] doesn't overlap [100,109] → skip
        let v: Vec<i64> = vec![200, 300];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 50 AND 80: [50,80] doesn't overlap [100,109] → skip
        let v: Vec<i64> = vec![50, 80];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 95 AND 105: overlaps [100,109] → cannot skip
        let v: Vec<i64> = vec![95, 105];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_between_reversed_bounds() -> ParquetResult<()> {
        // Reversed bounds (lo > hi): auto-swap must still produce correct results.
        let data: Vec<i64> = (100..110).collect(); // min=100, max=109
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // BETWEEN 300 AND 200 (reversed): swaps to [200,300] → skip
        let v: Vec<i64> = vec![300, 200];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 80 AND 50 (reversed): swaps to [50,80] → skip
        let v: Vec<i64> = vec![80, 50];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 105 AND 95 (reversed): swaps to [95,105] → overlaps → cannot skip
        let v: Vec<i64> = vec![105, 95];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_range_all_equal_boundary() -> ParquetResult<()> {
        // All values equal: min=max=42
        let data: Vec<i64> = vec![42, 42, 42, 42];
        let buf = gen_i64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // col < 42: min=42 >= 42 → skip
        let v: Vec<i64> = vec![42];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_LT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col > 42: max=42 <= 42 → skip
        let v: Vec<i64> = vec![42];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GT)];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col <= 42: min=42 > 42 is false → cannot skip (42 matches)
        let v: Vec<i64> = vec![42];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_LE)];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // col >= 42: max=42 < 42 is false → cannot skip (42 matches)
        let v: Vec<i64> = vec![42];
        let filters = [make_filter_with_op(0, 1, v.as_ptr() as u64, FILTER_OP_GE)];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 42 AND 42: [42,42] overlaps [42,42] → cannot skip
        let v: Vec<i64> = vec![42, 42];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 43 AND 50: [43,50] doesn't overlap [42,42] → skip
        let v: Vec<i64> = vec![43, 50];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_between_string() -> ParquetResult<()> {
        let data = vec!["alice", "bob", "charlie"]; // min="alice", max="charlie"
        let buf = gen_string_parquet_with_bloom(&data)?;
        let decoder = read_decoder(&buf)?;

        // BETWEEN "dog" AND "zebra": ["dog","zebra"] doesn't overlap ["alice","charlie"] → skip
        let filter_buf = make_string_filter_buf(&["dog", "zebra"]);
        let filters = [make_filter_with_op(
            0,
            2,
            filter_buf.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN "aaa" AND "bbb": overlaps ["alice","charlie"] → cannot skip
        let filter_buf = make_string_filter_buf(&["aaa", "bbb"]);
        let filters = [make_filter_with_op(
            0,
            2,
            filter_buf.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN "zebra" AND "dog" (reversed): swaps to ["dog","zebra"] → skip
        let filter_buf = make_string_filter_buf(&["zebra", "dog"]);
        let filters = [make_filter_with_op(
            0,
            2,
            filter_buf.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_between_f64() -> ParquetResult<()> {
        let data: Vec<f64> = vec![1.0, 2.5, 5.0, 7.5, 10.0]; // min=1.0, max=10.0
        let buf = gen_f64_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // BETWEEN 20.0 AND 30.0: [20,30] doesn't overlap [1,10] → skip
        let v: Vec<f64> = vec![20.0, 30.0];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 30.0 AND 20.0 (reversed): swaps to [20,30] → skip
        let v: Vec<f64> = vec![30.0, 20.0];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 3.0 AND 8.0: overlaps [1,10] → cannot skip
        let v: Vec<f64> = vec![3.0, 8.0];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    #[test]
    fn skip_row_group_between_i32() -> ParquetResult<()> {
        let data: Vec<i32> = (10..20).collect(); // min=10, max=19
        let buf = gen_i32_parquet_with_stats(&data)?;
        let decoder = read_decoder(&buf)?;

        // BETWEEN 30 AND 50: [30,50] doesn't overlap [10,19] → skip
        let v: Vec<i32> = vec![30, 50];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 50 AND 30 (reversed): swaps to [30,50] → skip
        let v: Vec<i32> = vec![50, 30];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // BETWEEN 5 AND 15: overlaps [10,19] → cannot skip
        let v: Vec<i32> = vec![5, 15];
        let filters = [make_filter_with_op(
            0,
            2,
            v.as_ptr() as u64,
            FILTER_OP_BETWEEN,
        )];
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        Ok(())
    }

    // ── Group 5: bloom filter read_from_slice_at_offset tests ───────

    #[test]
    fn bloom_at_offset_matches_column_metadata_path() -> ParquetResult<()> {
        use parquet2::bloom_filter::{read_from_slice, read_from_slice_at_offset};
        use parquet2::read::read_metadata_with_size;

        let data: Vec<i64> = (100..200).collect();
        let buf = gen_i64_parquet_with_bloom(&data)?;

        // Read metadata via parquet2.
        let mut cursor = Cursor::new(&buf);
        let metadata = read_metadata_with_size(&mut cursor, buf.len() as u64)
            .map_err(|e| crate::parquet::error::fmt_err!(Unsupported, "metadata: {}", e))?;

        let chunk_meta = &metadata.row_groups[0].columns()[0];

        // Path A: read bloom via column metadata reference.
        let bitset_a = read_from_slice(chunk_meta, &buf)
            .map_err(|e| crate::parquet::error::fmt_err!(Unsupported, "bloom read: {}", e))?;

        // Path B: read bloom at raw offset (the _pm path).
        let offset = chunk_meta
            .metadata()
            .bloom_filter_offset
            .expect("bloom filter offset should be present");
        let bitset_b = read_from_slice_at_offset(offset as u64, &buf).map_err(|e| {
            crate::parquet::error::fmt_err!(Unsupported, "bloom offset read: {}", e)
        })?;

        assert!(
            !bitset_a.is_empty(),
            "bloom filter bitset should not be empty"
        );
        assert_eq!(
            bitset_a, bitset_b,
            "read_from_slice and read_from_slice_at_offset should return identical bitsets"
        );
        Ok(())
    }

    #[test]
    fn bloom_at_offset_out_of_bounds() {
        use parquet2::bloom_filter::read_from_slice_at_offset;
        let data = vec![0u8; 100];
        let result = read_from_slice_at_offset(200, &data);
        assert!(result.is_err(), "should error on out-of-bounds offset");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("exceeds"),
            "error should mention 'exceeds': {err_msg}"
        );
    }

    /// Rewrite the `null_count` statistic of row group 0, column 0. Parquet stores the count
    /// signed and QuestDB reads files other tools wrote, so a negative count is something the
    /// decoder has to survive; nothing in the writer can produce one.
    fn override_null_count(decoder: &mut ParquetDecoder, null_count: Option<i64>) {
        let row_group = &mut decoder.metadata.row_groups[0];
        let num_rows = row_group.num_rows();
        let total_byte_size = row_group.total_byte_size();
        let mut columns = row_group.take_columns();
        let column = columns.remove(0);
        let descriptor = column.descriptor().clone();
        let mut column_chunk = column.into_thrift();
        let statistics = column_chunk
            .meta_data
            .as_mut()
            .expect("column chunk has metadata")
            .statistics
            .as_mut()
            .expect("column chunk has statistics");
        statistics.null_count = null_count;
        columns.insert(0, ColumnChunkMetaData::new(column_chunk, descriptor));
        *row_group = RowGroupMetaData::_new(columns, num_rows, total_byte_size);
    }

    #[test]
    fn no_skip_eq_null_when_parquet_null_count_is_negative() -> ParquetResult<()> {
        // Ten values, every other one null, so the row group genuinely holds nulls.
        let values: Vec<i64> = (100..110).collect();
        let def_levels: Vec<i16> = (0..10).map(|i| i % 2).collect();
        let buf = gen_nullable_i64_parquet(&values, &def_levels)?;

        let sentinel: Vec<i64> = vec![i64::MIN]; // the LONG null sentinel
        let filters = [make_filter_with_op_and_type(
            0,
            1,
            sentinel.as_ptr() as u64,
            FILTER_OP_EQ,
            ColumnTypeTag::Long as i32,
        )];

        // A negative count carries no information, but reading it as "no nulls" let the sentinel
        // be pruned against min/max - the sentinel is below every real value, so the row group
        // and its null rows disappeared.
        for null_count in [Some(-1i64), Some(-5), Some(i64::MIN)] {
            let mut decoder = read_decoder(&buf)?;
            override_null_count(&mut decoder, null_count);
            assert!(
                !decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
                "a negative null count must not read as null-free [null_count={null_count:?}]"
            );
        }

        // An absent count was already conservative; keep it that way.
        let mut decoder = read_decoder(&buf)?;
        override_null_count(&mut decoder, None);
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // The writer's own count says nulls are present, so this cannot prune either.
        let decoder = read_decoder(&buf)?;
        assert!(!decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?);

        // Control: an honest zero count really is null-free, so the sentinel still prunes. Without
        // this the assertions above would pass on a decoder that never prunes anything.
        let mut decoder = read_decoder(&buf)?;
        override_null_count(&mut decoder, Some(0));
        assert!(
            decoder.can_skip_row_group(0, &buf, &filters, u64::MAX)?,
            "a zero null count must keep pruning the NULL sentinel"
        );
        Ok(())
    }

    #[test]
    fn bloom_probe_declines_structurally_invalid_bitset() -> ParquetResult<()> {
        use crate::parquet_read::ColumnFilterValues;
        use parquet2::schema::types::PhysicalType;

        // `is_in_set` indexes a whole 32-byte block without bounds checks, so anything that is
        // not a whole number of blocks either panics - aborting the JVM through JNI - or probes a
        // set the writer never filled that way. Both callers are supposed to have rejected such a
        // bitset before this point; the probe declines it as well rather than trust them.
        let value: i64 = 42;
        let filter_desc = ColumnFilterValues {
            count: 1,
            ptr: &value as *const i64 as u64,
            buf_end: unsafe { (&value as *const i64).add(1) } as u64,
        };
        for len in [0usize, 1, 31, 33, 63] {
            let bitset = vec![0u8; len];
            assert!(
                !ParquetDecoder::all_values_absent_from_bloom(
                    &bitset,
                    &PhysicalType::Int64,
                    &filter_desc,
                    false,
                    false,
                    false,
                    ColumnTypeTag::Long as i32,
                )?,
                "a bitset of {len} bytes must not answer 'absent'"
            );
        }
        // Control: an all-zero WHOLE block really does answer "absent", so the guard above is
        // rejecting the invalid lengths rather than the probe having no teeth.
        let bitset = vec![0u8; 32];
        assert!(ParquetDecoder::all_values_absent_from_bloom(
            &bitset,
            &PhysicalType::Int64,
            &filter_desc,
            false,
            false,
            false,
            ColumnTypeTag::Long as i32,
        )?);
        Ok(())
    }

    #[test]
    fn bloom_at_offset_truncated_data() {
        use parquet2::bloom_filter::read_from_slice_at_offset;
        // A valid bloom filter header but truncated data (only 10 bytes total,
        // not enough for header + bitset).
        let data = vec![0u8; 10];
        let result = read_from_slice_at_offset(0, &data);
        // Should error or return empty because the data is too short to parse
        // a valid bloom filter header + bitset.
        assert!(
            result.is_err() || result.unwrap().is_empty(),
            "should error or return empty on truncated data"
        );
    }
}
