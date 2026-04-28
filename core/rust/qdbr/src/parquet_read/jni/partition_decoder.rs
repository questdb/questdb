use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_metadata::jni::reader::JniParquetMetaReader;
use crate::parquet_metadata::reader::ParquetMetaReader;
use crate::parquet_read::jni::validate_jni_column_types;
use crate::parquet_read::row_groups::ParquetColumnIndex;
use crate::parquet_read::{DecodeContext, RowGroupBuffers};
use jni::objects::JClass;
use jni::JNIEnv;
use qdb_core::col_type::ColumnType;

/// Decode a row group using metadata from a `_pm` sidecar file.
///
/// Column types, byte ranges, codecs, and descriptors are read from the
/// `_pm` binary format via [`ParquetMetaReader`]. The `columns` array
/// uses the same `[parquet_column_index, column_type]` pair format as
/// `PartitionDecoder` for compatibility with `PageFrameMemoryPool`.
/// The `column_type` from Java is used for Symbol→Varchar and
/// Varchar→VarcharSlice overrides; the base type comes from `_pm`.
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetPartitionDecoder_decodeRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    row_group_bufs: *mut RowGroupBuffers,
    columns: *const (ParquetColumnIndex, ColumnType), // [index, type] pairs
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
) -> u32 {
    let env = &mut env;
    let res = parquet_meta_decode_row_group_impl(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_reader_ptr,
        row_group_bufs,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
    );
    match res {
        Ok(count) => count as u32,
        Err(mut err) => {
            err.add_context("error in ParquetPartitionDecoder.decodeRowGroup");
            err.into_cairo_exception().throw(env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetPartitionDecoder_decodeRowGroupWithRowFilter(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: u32,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_size: i64,
) {
    let env = &mut env;
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
    let res = parquet_meta_decode_row_group_filtered_impl::<false>(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_reader_ptr,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_count,
    );
    if let Err(mut err) = res {
        err.add_context("error in ParquetPartitionDecoder.decodeRowGroupWithRowFilter");
        let _: () = err.into_cairo_exception().throw(env);
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetPartitionDecoder_decodeRowGroupWithRowFilterFillNulls(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: u32,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_size: i64,
) {
    let env = &mut env;
    let filtered_rows_count = if filtered_rows_size < 0 {
        0usize
    } else {
        filtered_rows_size as usize
    };
    let res = parquet_meta_decode_row_group_filtered_impl::<true>(
        allocator,
        ctx,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_reader_ptr,
        row_group_bufs,
        column_offset as usize,
        columns,
        column_count,
        row_group_index,
        row_group_lo,
        row_group_hi,
        filtered_rows_ptr,
        filtered_rows_count,
    );
    if let Err(mut err) = res {
        err.add_context(
            "error in ParquetPartitionDecoder.decodeRowGroupWithRowFilterFillNulls",
        );
        let _: () = err.into_cairo_exception().throw(env);
    }
}

#[allow(clippy::too_many_arguments)]
fn parquet_meta_decode_row_group_filtered_impl<const FILL_NULLS: bool>(
    _allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    row_group_bufs: *mut RowGroupBuffers,
    column_offset: usize,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
    filtered_rows_ptr: *const i64,
    filtered_rows_count: usize,
) -> ParquetResult<usize> {
    if ctx.is_null() {
        return Err(fmt_err!(InvalidType, "decode context pointer is null"));
    }
    if row_group_bufs.is_null() {
        return Err(fmt_err!(InvalidType, "row group buffers pointer is null"));
    }
    if columns.is_null() && column_count > 0 {
        return Err(fmt_err!(InvalidType, "columns pointer is null"));
    }
    if filtered_rows_ptr.is_null() && filtered_rows_count > 0 {
        return Err(fmt_err!(InvalidType, "filtered rows pointer is null"));
    }
    if parquet_meta_reader_ptr.is_null() {
        return Err(fmt_err!(
            InvalidType,
            "JniParquetMetaReader pointer is null"
        ));
    }
    if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "parquet file pointer is null or size is zero"
        ));
    }

    let parquet_meta_reader: &ParquetMetaReader = unsafe { &*parquet_meta_reader_ptr }.reader();
    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let ctx = unsafe { &mut *ctx };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let col_pairs = if columns.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(columns, column_count as usize) }
    };
    let filtered_rows = if filtered_rows_ptr.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(filtered_rows_ptr, filtered_rows_count) }
    };

    crate::parquet_read::parquet_meta_decode::decode_row_group_filtered::<FILL_NULLS>(
        ctx,
        row_group_bufs,
        crate::parquet_read::parquet_meta_decode::ColumnChunkSource::File(file_data),
        parquet_meta_reader,
        column_offset,
        col_pairs,
        row_group_index as usize,
        row_group_lo as usize,
        row_group_hi as usize,
        filtered_rows,
    )
}

#[allow(clippy::too_many_arguments)]
fn parquet_meta_decode_row_group_impl(
    _allocator: *const QdbAllocator,
    ctx: *mut DecodeContext,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    row_group_bufs: *mut RowGroupBuffers,
    columns: *const (ParquetColumnIndex, ColumnType),
    column_count: u32,
    row_group_index: u32,
    row_group_lo: u32,
    row_group_hi: u32,
) -> ParquetResult<usize> {
    if ctx.is_null() {
        return Err(fmt_err!(InvalidType, "decode context pointer is null"));
    }
    if row_group_bufs.is_null() {
        return Err(fmt_err!(InvalidType, "row group buffers pointer is null"));
    }
    if columns.is_null() && column_count > 0 {
        return Err(fmt_err!(InvalidType, "columns pointer is null"));
    }
    if column_count > 0 {
        let col_pairs = unsafe { slice::from_raw_parts(columns, column_count as usize) };
        validate_jni_column_types(col_pairs)?;
    }
    if parquet_meta_reader_ptr.is_null() {
        return Err(fmt_err!(
            InvalidType,
            "JniParquetMetaReader pointer is null"
        ));
    }
    if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "parquet file pointer is null or size is zero"
        ));
    }

    let parquet_meta_reader: &ParquetMetaReader = unsafe { &*parquet_meta_reader_ptr }.reader();
    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let ctx = unsafe { &mut *ctx };
    let row_group_bufs = unsafe { &mut *row_group_bufs };
    let col_pairs = if columns.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(columns, column_count as usize) }
    };

    crate::parquet_read::parquet_meta_decode::decode_row_group(
        ctx,
        row_group_bufs,
        crate::parquet_read::parquet_meta_decode::ColumnChunkSource::File(file_data),
        parquet_meta_reader,
        col_pairs,
        row_group_index as usize,
        row_group_lo as usize,
        row_group_hi as usize,
    )
}

/// Find the row group containing the given timestamp using `_pm` metadata.
///
/// Reads min/max timestamp stats directly from the `_pm` file's column chunks.
/// Falls back to actual decode if stats are unavailable (should not happen for
/// QDB-written partitions).
#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetPartitionDecoder_findRowGroupByTimestamp(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    timestamp: i64,
    row_lo: i64,
    row_hi: i64,
    timestamp_column_index: i32,
) -> i64 {
    let env = &mut env;
    let res = parquet_meta_find_row_group_by_timestamp_impl(
        allocator,
        parquet_file_ptr,
        parquet_file_size,
        parquet_meta_reader_ptr,
        timestamp,
        row_lo as usize,
        row_hi as usize,
        timestamp_column_index as usize,
    );
    match res {
        Ok(val) => val as i64,
        Err(mut err) => {
            err.add_context("error in ParquetPartitionDecoder.findRowGroupByTimestamp");
            err.into_cairo_exception().throw(env)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn parquet_meta_find_row_group_by_timestamp_impl(
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader_ptr: *const JniParquetMetaReader,
    timestamp: i64,
    row_lo: usize,
    row_hi: usize,
    ts_col: usize,
) -> ParquetResult<u64> {
    if parquet_meta_reader_ptr.is_null() {
        return Err(fmt_err!(
            InvalidType,
            "JniParquetMetaReader pointer is null"
        ));
    }

    if parquet_file_ptr.is_null() || parquet_file_size == 0 {
        return Err(fmt_err!(
            InvalidType,
            "parquet file pointer is null or size is zero"
        ));
    }

    let parquet_meta_reader: &ParquetMetaReader = unsafe { &*parquet_meta_reader_ptr }.reader();

    let decode_ts = |rg_idx, ts_col, row_lo, row_hi| {
        decode_single_ts_from_pm(
            allocator,
            parquet_file_ptr,
            parquet_file_size,
            parquet_meta_reader,
            rg_idx,
            ts_col,
            row_lo,
            row_hi,
        )
    };

    crate::parquet_read::parquet_meta_decode::find_row_group_by_timestamp(
        parquet_meta_reader,
        timestamp,
        row_lo,
        row_hi,
        ts_col,
        decode_ts,
    )
}

/// Decode a single timestamp value from the parquet file using `_pm` metadata.
#[allow(clippy::too_many_arguments)]
pub(crate) fn decode_single_ts_from_pm(
    allocator: *const QdbAllocator,
    parquet_file_ptr: *const u8,
    parquet_file_size: u64,
    parquet_meta_reader: &ParquetMetaReader,
    rg_idx: usize,
    ts_col: usize,
    row_lo: usize,
    row_hi: usize,
) -> ParquetResult<i64> {
    use crate::parquet_read::decode_column::{
        decode_single_timestamp_value, reconstruct_descriptor,
    };

    let file_data = unsafe { slice::from_raw_parts(parquet_file_ptr, parquet_file_size as usize) };
    let col_desc = parquet_meta_reader.column_descriptor(ts_col)?;
    let rg_block = parquet_meta_reader.row_group(rg_idx)?;
    let chunk = rg_block.column_chunk(ts_col)?;

    let compression: parquet2::compression::Compression = chunk
        .codec()
        .map_err(|e| fmt_err!(InvalidType, "invalid codec: {}", e))?
        .into();
    let flags = crate::parquet_metadata::types::ColumnFlags(col_desc.flags);
    let field_rep = flags
        .repetition()
        .unwrap_or(crate::parquet_metadata::types::FieldRepetition::Required);
    let column_name = parquet_meta_reader.column_name(ts_col).unwrap_or("<ts>");
    let descriptor = reconstruct_descriptor(
        col_desc.physical_type,
        col_desc.fixed_byte_len,
        col_desc.max_rep_level,
        col_desc.max_def_level,
        column_name,
        field_rep.into(),
    );

    decode_single_timestamp_value(
        allocator,
        file_data,
        chunk.byte_range_start as usize,
        chunk.total_compressed as usize,
        compression,
        descriptor,
        chunk.num_values as i64,
        column_name,
        rg_idx,
        row_lo,
        row_hi,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::TestAllocatorState;
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_metadata::column_chunk::ColumnChunkRaw;
    use crate::parquet_metadata::reader::ParquetMetaReader;
    use crate::parquet_metadata::row_group::RowGroupBlockBuilder;
    use crate::parquet_metadata::types::{Codec, ColumnFlags, FieldRepetition};
    use crate::parquet_metadata::writer::ParquetMetaWriter;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ParquetEncodingConfig, Partition};
    use parquet2::compression::CompressionOptions;
    use parquet2::read::read_metadata_with_size;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnTypeTag;
    use std::io::Cursor;

    /// Parquet physical type ordinal for Int64 in the `_pm` format.
    const PHYS_INT64: u8 = 2;

    /// Writes a parquet file with a single i64 "ts" column and `with_statistics(false)`.
    /// The column is marked `designated_timestamp: false` so the override at
    /// `parquet_write/file.rs:419-420,540-541` does not force-enable statistics —
    /// the resulting column chunk carries no min/max in the parquet footer.
    ///
    /// The matching `_pm` is then built manually via `ParquetMetaWriter` so its
    /// column descriptor correctly declares the ts column as the designated
    /// timestamp at index 0, while each row group's column chunk points at the
    /// real byte range from the parquet metadata with `stat_flags = 0`. This is
    /// precisely the state `decode_single_ts_from_pm` is meant to handle.
    ///
    /// Returns `(parquet_bytes, parquet_meta_bytes, parquet_meta_file_size)`.
    fn build_parquet_and_parquet_meta_without_ts_stats(
        num_row_groups: usize,
        rows_per_group: usize,
    ) -> (Vec<u8>, Vec<u8>, u64) {
        let total_rows = num_row_groups * rows_per_group;
        let ts_values: Vec<i64> = (0..total_rows as i64).collect();
        let data_bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(ts_values.as_ptr() as *const u8, ts_values.len() * 8)
        };
        let data_static: &'static [u8] = Box::leak(data_bytes.to_vec().into_boxed_slice());

        let col = Column {
            id: 0,
            name: "ts",
            data_type: ColumnTypeTag::Timestamp.into_type(),
            row_count: total_rows,
            primary_data: data_static,
            secondary_data: &[],
            symbol_offsets: &[],
            column_top: 0,
            designated_timestamp: false,
            not_null_hint: false,
            designated_timestamp_ascending: true,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(0),
        };
        let partition = Partition { table: "test".to_string(), columns: vec![col] };

        let mut parquet_buf = Vec::new();
        ParquetWriter::new(&mut parquet_buf)
            .with_statistics(false)
            .with_compression(CompressionOptions::Uncompressed)
            .with_version(Version::V1)
            .with_row_group_size(Some(rows_per_group))
            .finish(partition)
            .unwrap();

        let mut cursor = Cursor::new(&parquet_buf);
        let metadata = read_metadata_with_size(&mut cursor, parquet_buf.len() as u64).unwrap();
        assert_eq!(metadata.row_groups.len(), num_row_groups);

        let col_desc = &metadata.schema_descr.columns()[0];
        let max_rep_level: u8 = col_desc.descriptor.max_rep_level.try_into().unwrap();
        let max_def_level: u8 = col_desc.descriptor.max_def_level.try_into().unwrap();
        let repetition = FieldRepetition::from(col_desc.base_type.get_field_info().repetition);

        let mut writer = ParquetMetaWriter::new();
        writer
            .designated_timestamp(0)
            .add_column(
                "ts",
                0,
                ColumnTypeTag::Timestamp as i32,
                ColumnFlags::new().with_repetition(repetition),
                0,
                PHYS_INT64,
                max_rep_level,
                max_def_level,
            )
            .parquet_footer(0, 0);

        for rg_idx in 0..num_row_groups {
            let rg_meta = &metadata.row_groups[rg_idx];
            let col_chunk = &rg_meta.columns()[0];
            let (byte_range_start, total_compressed) = col_chunk.byte_range();
            let num_values = col_chunk.num_values().max(0) as u64;

            let mut chunk = ColumnChunkRaw::zeroed();
            chunk.codec = Codec::Uncompressed as u8;
            chunk.num_values = num_values;
            chunk.byte_range_start = byte_range_start;
            chunk.total_compressed = total_compressed;
            // stat_flags intentionally left at 0 so the fallback decoder is used.

            let mut rg = RowGroupBlockBuilder::new(1);
            rg.set_num_rows(rows_per_group as u64);
            rg.set_column_chunk(0, chunk).unwrap();
            writer.add_row_group(rg);
        }

        let (parquet_meta_bytes, parquet_meta_file_size) = writer.finish().unwrap();
        (parquet_buf, parquet_meta_bytes, parquet_meta_file_size)
    }

    /// Decode a timestamp via `decode_single_ts_from_pm`. Builds the allocator
    /// pointer the same way production callers do through the JNI boundary.
    fn decode_ts(
        parquet_bytes: &[u8],
        reader: &ParquetMetaReader,
        rg_idx: usize,
        ts_col: usize,
        row_lo: usize,
        row_hi: usize,
    ) -> ParquetResult<i64> {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let allocator_ptr: *const QdbAllocator = &allocator;
        decode_single_ts_from_pm(
            allocator_ptr,
            parquet_bytes.as_ptr(),
            parquet_bytes.len() as u64,
            reader,
            rg_idx,
            ts_col,
            row_lo,
            row_hi,
        )
    }

    #[test]
    fn decode_first_and_last_timestamps_in_row_group_0() {
        let (parquet_bytes, parquet_meta_bytes, parquet_meta_file_size) =
            build_parquet_and_parquet_meta_without_ts_stats(1, 100);
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(decode_ts(&parquet_bytes, &reader, 0, 0, 0, 1).unwrap(), 0);
        assert_eq!(
            decode_ts(&parquet_bytes, &reader, 0, 0, 99, 100).unwrap(),
            99
        );
    }

    #[test]
    fn decode_crosses_row_group_boundary() {
        let (parquet_bytes, parquet_meta_bytes, parquet_meta_file_size) =
            build_parquet_and_parquet_meta_without_ts_stats(2, 50);
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert_eq!(decode_ts(&parquet_bytes, &reader, 1, 0, 0, 1).unwrap(), 50);
    }

    #[test]
    fn out_of_range_indices_error() {
        let (parquet_bytes, parquet_meta_bytes, parquet_meta_file_size) =
            build_parquet_and_parquet_meta_without_ts_stats(1, 10);
        let reader =
            ParquetMetaReader::from_file_size(&parquet_meta_bytes, parquet_meta_file_size).unwrap();
        assert!(decode_ts(&parquet_bytes, &reader, 1, 0, 0, 1).is_err());
        assert!(decode_ts(&parquet_bytes, &reader, 0, 99, 0, 1).is_err());
    }
}
