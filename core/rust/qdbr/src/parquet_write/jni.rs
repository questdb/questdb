use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet_write::file::{ChunkedWriter, ParquetWriter, DEFAULT_ROW_GROUP_SIZE};
use crate::parquet_write::schema::{Column, Partition};
use crate::parquet_write::update::ParquetUpdater;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::ops::Sub;
use std::path::Path;
use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::io::FromRawFdI32Ext;
use jni::objects::JClass;
use jni::sys::{jboolean, jdouble, jint, jlong, jshort};
use jni::JNIEnv;
use parquet2::compression::{BrotliLevel, CompressionOptions, GzipLevel, ZstdLevel};
use parquet2::metadata::{KeyValue, SortingColumn};
use parquet2::write::Version;

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_create(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    src_path_len: u32,
    src_path_ptr: *const u8,
    raw_fd: i32,
    file_size: u64,
    timestamp_index: jint,
    compression_codec: jlong,
    statistics_enabled: jboolean,
    raw_array_encoding: jboolean,
    row_group_size: jlong,
    data_page_size: jlong,
    bloom_filter_fpp: jdouble,
) -> *mut ParquetUpdater {
    let create = || -> ParquetResult<ParquetUpdater> {
        let compression_options =
            compression_from_i64(compression_codec).context("CompressionCodec")?;
        let allocator = unsafe { &*allocator }.clone();

        let statistics_enabled = statistics_enabled != 0;
        let raw_array_encoding = raw_array_encoding != 0;

        let row_group_size = if row_group_size > 0 {
            Some(row_group_size as usize)
        } else {
            None
        };

        let data_page_size = if data_page_size > 0 {
            Some(data_page_size as usize)
        } else {
            None
        };

        let sorting_columns = if timestamp_index != -1 {
            Some(vec![SortingColumn::new(timestamp_index, false, false)])
        } else {
            None
        };

        ParquetUpdater::new(
            allocator,
            unsafe { File::from_raw_fd_i32(raw_fd) },
            file_size,
            sorting_columns,
            statistics_enabled,
            raw_array_encoding,
            compression_options,
            row_group_size,
            data_page_size,
            bloom_filter_fpp,
        )
    };

    match create() {
        Ok(updater) => Box::into_raw(Box::new(updater)),
        Err(mut err) => {
            let src_path = unsafe { slice::from_raw_parts(src_path_ptr, src_path_len as usize) };
            let src_path = std::str::from_utf8(src_path).unwrap_or("!!invalid path utf8!!");
            err.add_context(format!(
                "could not open parquet file for update from path {src_path}"
            ));
            err.add_context("error in PartitionUpdater.create");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_destroy(
    _env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
) {
    if updater.is_null() {
        panic!("ParquetUpdater pointer is null");
    }

    let _ = unsafe { Box::from_raw(updater) };
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_updateFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
) {
    if updater.is_null() {
        panic!("ParquetUpdater pointer is null");
    }

    let parquet_updater = unsafe { &mut *updater };
    match parquet_updater.end(None) {
        Ok(_) => (),
        Err(mut err) => {
            err.add_context("could not update partition metadata");
            err.add_context("error in PartitionUpdater.updateFileMetadata");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_updateRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    parquet_updater: *mut ParquetUpdater,
    table_name_len: u32,
    table_name_ptr: *const u8,
    row_group_id: jshort,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    timestamp_index: jint,
    row_count: jlong,
) {
    let orig_row_group_id = row_group_id;
    let row_group_id = Some(row_group_id);

    assert!(
        !parquet_updater.is_null(),
        "parquet_updater pointer is null"
    );
    let parquet_updater = unsafe { &mut *parquet_updater };

    let mut update = || -> ParquetResult<()> {
        let table_name = "update";
        let partition = create_partition_descriptor(
            table_name.as_ptr(),
            table_name.len() as i32,
            col_count,
            col_names_ptr,
            col_names_len,
            col_data_ptr,
            col_data_len,
            row_count,
            timestamp_index,
        )?;
        if let Some(row_group_id) = row_group_id {
            parquet_updater.replace_row_group(&partition, row_group_id)
        } else {
            parquet_updater.append_row_group(&partition)
        }
    };

    match update() {
        Ok(_) => (),
        Err(mut err) => {
            let table_name =
                unsafe { slice::from_raw_parts(table_name_ptr, table_name_len as usize) };
            let table_name =
                std::str::from_utf8(table_name).unwrap_or("!!invalid table_dir_name utf8!!");
            err.add_context(format!(
                "could not update row group {orig_row_group_id} for table {table_name}"
            ));
            err.add_context("error in PartitionUpdater.updateRowGroup");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_encodePartition(
    mut env: JNIEnv,
    _class: JClass,
    table_name_ptr: *const u8,
    table_name_size: jint,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    timestamp_index: jint,
    row_count: jlong,
    dest_path: *const u8,
    dest_path_len: i32,
    compression_codec: jlong,
    statistics_enabled: jboolean,
    raw_array_encoding: jboolean,
    row_group_size: jlong,
    data_page_size: jlong,
    version: jint,
    bloom_filter_column_indexes: *const jint,
    bloom_filter_column_count: jint,
    bloom_filter_fpp: jdouble,
) {
    let encode = || -> ParquetResult<()> {
        let partition = create_partition_descriptor(
            table_name_ptr,
            table_name_size,
            col_count,
            col_names_ptr,
            col_names_len,
            col_data_ptr,
            col_data_len,
            row_count,
            timestamp_index,
        )?;

        let dest_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(dest_path, dest_path_len as usize))
        };

        let dest_path = Path::new(&dest_path);
        let compression_options =
            compression_from_i64(compression_codec).context("CompressionCodec")?;

        let statistics_enabled = statistics_enabled != 0;
        let raw_array_encoding = raw_array_encoding != 0;
        let row_group_size = if row_group_size > 0 {
            Some(row_group_size as usize)
        } else {
            None
        };
        let data_page_size = if data_page_size > 0 {
            Some(data_page_size as usize)
        } else {
            None
        };
        let version = version_from_i32(version)?;

        let bloom_filter_cols =
            build_bloom_filter_set(bloom_filter_column_indexes, bloom_filter_column_count);
        let bloom_fpp = if bloom_filter_fpp > 0.0 {
            bloom_filter_fpp
        } else {
            0.01
        };

        let file: ParquetResult<_> = File::create(dest_path).map_err(|e| e.into());
        let mut file = file.with_context(|_| {
            format!("Could not create parquet file for {}", dest_path.display())
        })?;

        let local_timestamp_index = partition.columns.iter().enumerate().find_map(|(i, c)| {
            if c.designated_timestamp {
                Some(i as i32)
            } else {
                None
            }
        });
        let sorting_columns =
            local_timestamp_index.map(|i| vec![SortingColumn::new(i, false, false)]);

        ParquetWriter::new(&mut file)
            .with_version(version)
            .with_compression(compression_options)
            .with_statistics(statistics_enabled)
            .with_raw_array_encoding(raw_array_encoding)
            .with_row_group_size(row_group_size)
            .with_data_page_size(data_page_size)
            .with_sorting_columns(sorting_columns)
            .with_bloom_filter_columns(bloom_filter_cols)
            .with_bloom_filter_fpp(bloom_fpp)
            .finish(partition)
            .map(|_| ())
            .context("ParquetWriter::finish failed")
    };

    match encode() {
        Ok(_) => (),
        Err(mut err) => {
            let table_name = unsafe {
                std::str::from_utf8(slice::from_raw_parts(
                    table_name_ptr,
                    table_name_size as usize,
                ))
                .expect("invalid table name utf8")
            };
            err.add_context(format!(
                "could not encode partition for table {table_name} and timestamp index {timestamp_index}"
            ));
            err.add_context("error in PartitionEncoder.encodePartition");
            err.into_cairo_exception().throw(&mut env)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn create_partition_descriptor(
    table_name_ptr: *const u8,
    table_name_size: jint,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    row_count: jlong,
    timestamp_index: jint,
) -> ParquetResult<Partition> {
    let col_count = col_count as usize;
    let col_names_len = col_names_len as usize;
    let col_data_len = col_data_len as usize;
    const COL_DATA_ENTRY_SIZE: usize = 9;
    assert_eq!(col_data_len % COL_DATA_ENTRY_SIZE, 0);

    let mut col_names = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(col_names_ptr, col_names_len))
    };
    let col_data = unsafe { slice::from_raw_parts(col_data_ptr, col_data_len) };

    let row_count = row_count as usize;
    let mut columns = vec![];
    for col_idx in 0..col_count {
        let raw_idx = col_idx * COL_DATA_ENTRY_SIZE;

        let col_name_size = col_data[raw_idx];
        let (col_name, tail) = col_names.split_at(col_name_size as usize);
        col_names = tail;

        let packed = col_data[raw_idx + 1];
        let col_id = (packed >> 32) as i32;
        let col_type = (packed & 0xFFFFFFFF) as i32;

        let col_top = col_data[raw_idx + 2];

        let primary_col_addr = col_data[raw_idx + 3];
        let primary_col_size = col_data[raw_idx + 4];

        let secondary_col_addr = col_data[raw_idx + 5];
        let secondary_col_size = col_data[raw_idx + 6];

        let symbol_offsets_addr = col_data[raw_idx + 7];
        let symbol_offsets_size = col_data[raw_idx + 8];

        let designated_timestamp = col_id == timestamp_index;

        let column = Column::from_raw_data(
            col_id,
            col_name,
            col_type,
            col_top,
            row_count,
            primary_col_addr as *const u8,
            primary_col_size as usize,
            secondary_col_addr as *const u8,
            secondary_col_size as usize,
            symbol_offsets_addr as *const u64,
            symbol_offsets_size as usize,
            designated_timestamp,
            true,
        )?;

        columns.push(column);
    }

    let table = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(
            table_name_ptr,
            table_name_size as usize,
        ))
    }
    .to_string();

    let partition = Partition { table, columns };
    Ok(partition)
}

fn build_bloom_filter_set(indexes_ptr: *const jint, count: jint) -> HashSet<usize> {
    if indexes_ptr.is_null() || count <= 0 {
        return HashSet::new();
    }
    let indexes = unsafe { slice::from_raw_parts(indexes_ptr, count as usize) };
    indexes.iter().map(|&i| i as usize).collect()
}

fn version_from_i32(value: i32) -> Result<Version, ParquetError> {
    match value {
        1 => Ok(Version::V1),
        2 => Ok(Version::V2),
        _ => Err(fmt_err!(Unsupported, "unsupported parquet version {value}")),
    }
}

/// The `i64` value is expected to encode two `i32` values:
/// - The higher 32 bits represent the `level_id`.
/// - The lower 32 bits represent the optional `codec_id`.
///   `let value: i64 = (3 << 32) | 2;` Gzip with level 3.
fn compression_from_i64(value: i64) -> Result<CompressionOptions, ParquetError> {
    let codec_id = value as i32;
    let level_id = (value >> 32) as i32;
    match codec_id {
        0 => Ok(CompressionOptions::Uncompressed),
        1 => Ok(CompressionOptions::Snappy),
        2 => Ok(CompressionOptions::Gzip(Some(GzipLevel::try_new(
            level_id as u8,
        )?))),
        3 => Ok(CompressionOptions::Lzo),
        4 => Ok(CompressionOptions::Brotli(Some(BrotliLevel::try_new(
            level_id as u32,
        )?))),
        5 => Ok(CompressionOptions::Lz4),
        6 => Ok(CompressionOptions::Zstd(Some(ZstdLevel::try_new(
            level_id,
        )?))),
        7 => Ok(CompressionOptions::Lz4Raw),
        _ => Err(fmt_err!(
            Unsupported,
            "unsupported compression codec id: {codec_id}"
        )),
    }
}

struct BufferWriter {
    buffer: *mut Vec<u8, QdbAllocator>,
    offset: usize,
    init_offset: usize,
}

impl BufferWriter {
    unsafe fn new_with_offset(buffer: *mut Vec<u8, QdbAllocator>, offset: usize) -> Self {
        Self { buffer, offset, init_offset: offset }
    }
}

impl Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            let buffer_ref = &mut *self.buffer;
            if buffer_ref.len() <= self.init_offset {
                buffer_ref.set_len(self.init_offset);
                self.offset = self.init_offset;
            }

            let total_size = self.offset + buf.len();
            if buffer_ref.capacity() < total_size {
                let growth = (total_size - buffer_ref.len()).max(8192);
                buffer_ref.reserve(growth);
            }
            std::ptr::copy_nonoverlapping(
                buf.as_ptr(),
                buffer_ref.as_mut_ptr().add(self.offset),
                buf.len(),
            );
            self.offset += buf.len();
            buffer_ref.set_len(self.offset);
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct StreamingParquetWriter {
    partition: Partition,
    // We need Box<Vec<u8>> here to ensure the Vec itself has a stable heap address
    #[allow(clippy::box_collection)]
    current_buffer: Box<Vec<u8, QdbAllocator>>,
    chunked_writer: ChunkedWriter<BufferWriter>,
    additional_data: Vec<KeyValue>,

    // Fields for accumulating partitions across multiple writeChunk calls
    row_group_size: usize,
    pending_partitions: Vec<Partition>,
    first_partition_start: usize,
    accumulated_rows: usize,
    // Keep RowGroupBuffers alive while partitions reference their data.
    // Used by writeStreamingParquetChunkFromRowGroup to hold decoded parquet data.
    // Index corresponds to pending_partitions: Some(_) for FromRowGroup, None for writeChunk.
    pending_row_group_buffers: Vec<Option<crate::parquet_read::RowGroupBuffers>>,
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_createStreamingParquetWriter(
    mut env: JNIEnv,
    _class: JClass,
    allocator_ptr: *const QdbAllocator,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_meta_data: *const i64,
    timestamp_index: jint,
    timestamp_descending: jboolean,
    compression_codec: jlong,
    statistics_enabled: jboolean,
    raw_array_encoding: jboolean,
    row_group_size: jlong,
    data_page_size: jlong,
    version: jint,
    bloom_filter_column_indexes: *const jint,
    bloom_filter_column_count: jint,
    bloom_filter_fpp: jdouble,
) -> *mut StreamingParquetWriter {
    let create = || -> ParquetResult<StreamingParquetWriter> {
        let partition_template = create_partition_template(
            col_count,
            col_names_ptr,
            col_names_len,
            col_meta_data,
            timestamp_index,
            timestamp_descending,
        )?;
        let compression_options =
            compression_from_i64(compression_codec).context("CompressionCodec")?;
        let row_group_size_opt = if row_group_size > 0 {
            Some(row_group_size as usize)
        } else {
            None
        };
        let data_page_size_opt = if data_page_size > 0 {
            Some(data_page_size as usize)
        } else {
            None
        };
        let local_timestamp_index =
            partition_template
                .columns
                .iter()
                .enumerate()
                .find_map(|(i, c)| {
                    if c.designated_timestamp {
                        Some(i as i32)
                    } else {
                        None
                    }
                });
        let sorting_columns = local_timestamp_index
            .map(|i| vec![SortingColumn::new(i, timestamp_descending != 0, false)]);

        let (parquet_schema, additional_data) = crate::parquet_write::schema::to_parquet_schema(
            &partition_template,
            raw_array_encoding != 0,
        )?;
        let allocator = unsafe { &*allocator_ptr };
        let encodings = crate::parquet_write::schema::to_encodings(&partition_template);
        let mut current_buffer = Box::new(Vec::with_capacity_in(8192, allocator.clone()));
        let buffer_writer = unsafe {
            BufferWriter::new_with_offset(&mut *current_buffer as *mut Vec<u8, QdbAllocator>, 8)
        };
        let bloom_filter_cols =
            build_bloom_filter_set(bloom_filter_column_indexes, bloom_filter_column_count);
        let bloom_fpp = if bloom_filter_fpp > 0.0 {
            bloom_filter_fpp
        } else {
            0.01
        };

        let parquet_writer = ParquetWriter::new(buffer_writer)
            .with_version(version_from_i32(version)?)
            .with_compression(compression_options)
            .with_statistics(statistics_enabled != 0)
            .with_raw_array_encoding(raw_array_encoding != 0)
            .with_row_group_size(row_group_size_opt)
            .with_data_page_size(data_page_size_opt)
            .with_sorting_columns(sorting_columns.clone())
            .with_bloom_filter_columns(bloom_filter_cols)
            .with_bloom_filter_fpp(bloom_fpp);
        let chunked_writer = parquet_writer.chunked(parquet_schema, encodings)?;

        let effective_row_group_size = row_group_size_opt.unwrap_or(DEFAULT_ROW_GROUP_SIZE);

        Ok(StreamingParquetWriter {
            partition: partition_template,
            current_buffer,
            chunked_writer,
            additional_data,
            row_group_size: effective_row_group_size,
            pending_partitions: Vec::new(),
            first_partition_start: 0,
            accumulated_rows: 0,
            pending_row_group_buffers: Vec::new(),
        })
    };

    match create() {
        Ok(encoder) => Box::into_raw(Box::new(encoder)),
        Err(mut err) => {
            err.add_context("error in createStreamingParquetWriter");
            err.into_cairo_exception()
                .throw::<*mut StreamingParquetWriter>(&mut env)
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_writeStreamingParquetChunk(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
    col_data_ptr: *const i64,
    row_count: jlong,
) -> *const u8 {
    if encoder.is_null() {
        let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
        err.add_context("error in StreamingPartitionEncoder.writeChunk");
        err.into_cairo_exception().throw::<*const u8>(&mut env);
        return std::ptr::null();
    }
    let encoder = unsafe { &mut *encoder };
    let mut write_chunk = || -> ParquetResult<*const u8> {
        let row_count = row_count as usize;
        if row_count > 0 {
            let mut new_partition = Partition {
                table: String::new(),
                columns: encoder.partition.columns.clone(),
            };
            update_partition_data(&mut new_partition, col_data_ptr, row_count)?;
            encoder.pending_partitions.push(new_partition);
            encoder.pending_row_group_buffers.push(None);
            encoder.accumulated_rows += row_count;
        }
        flush_pending_partitions(encoder)
    };

    match write_chunk() {
        Ok(ptr) => ptr,
        Err(mut err) => {
            err.add_context("error in StreamingPartitionEncoder.writeChunk");
            err.into_cairo_exception().throw::<*const u8>(&mut env);
            std::ptr::null()
        }
    }
}

fn flush_pending_partitions(encoder: &mut StreamingParquetWriter) -> ParquetResult<*const u8> {
    if encoder.accumulated_rows >= encoder.row_group_size {
        unsafe {
            encoder.current_buffer.set_len(0);
        }
        write_pending_row_group(encoder)?;
        let data_len = (encoder.current_buffer.len() - 8) as u64;
        encoder.current_buffer[0..8].copy_from_slice(&data_len.to_le_bytes());
        Ok(encoder.current_buffer.as_ptr())
    } else {
        Ok(std::ptr::null())
    }
}

fn write_pending_row_group(encoder: &mut StreamingParquetWriter) -> ParquetResult<()> {
    let row_group_size = encoder.row_group_size;
    let first_start = encoder.first_partition_start;
    let mut rows_needed = row_group_size;
    let mut last_partition_idx = 0;
    let mut last_partition_end = 0;

    for (idx, partition) in encoder.pending_partitions.iter().enumerate() {
        let partition_rows = partition.columns[0].row_count;
        let available_rows = if idx == 0 {
            partition_rows.sub(first_start)
        } else {
            partition_rows
        };

        if available_rows >= rows_needed {
            last_partition_idx = idx;
            last_partition_end = if idx == 0 {
                first_start + rows_needed
            } else {
                rows_needed
            };
            break;
        } else {
            rows_needed -= available_rows;
            last_partition_idx = idx;
            last_partition_end = partition_rows;
        }
    }

    let partitions: Vec<&Partition> = encoder.pending_partitions[..=last_partition_idx]
        .iter()
        .collect();
    encoder.chunked_writer.write_row_group_from_partitions(
        &partitions,
        first_start,
        last_partition_end,
    )?;
    let last_partition_rows = encoder.pending_partitions[last_partition_idx].columns[0].row_count;

    if last_partition_end >= last_partition_rows {
        encoder.pending_partitions.drain(..=last_partition_idx);
        encoder
            .pending_row_group_buffers
            .drain(..=last_partition_idx);
        encoder.first_partition_start = 0;
    } else {
        encoder.pending_partitions.drain(..last_partition_idx);
        encoder
            .pending_row_group_buffers
            .drain(..last_partition_idx);
        encoder.first_partition_start = last_partition_end;
    }

    encoder.accumulated_rows = encoder
        .pending_partitions
        .iter()
        .enumerate()
        .map(|(idx, p)| {
            let rows = p.columns[0].row_count;
            if idx == 0 {
                rows.sub(encoder.first_partition_start)
            } else {
                rows
            }
        })
        .sum();

    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_finishStreamingParquetWrite(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
) -> *const u8 {
    if encoder.is_null() {
        let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
        err.add_context("error in StreamingPartitionEncoder.finish");
        err.into_cairo_exception().throw::<*const u8>(&mut env);
        return std::ptr::null();
    }

    let encoder = unsafe { &mut *encoder };
    let mut finish = || -> ParquetResult<*const u8> {
        unsafe {
            encoder.current_buffer.set_len(0);
        }

        if !encoder.pending_partitions.is_empty() && encoder.accumulated_rows > 0 {
            let last_idx = encoder.pending_partitions.len() - 1;
            let last_partition_end = encoder.pending_partitions[last_idx].columns[0].row_count;

            let partitions: Vec<&Partition> = encoder.pending_partitions.iter().collect();
            encoder.chunked_writer.write_row_group_from_partitions(
                &partitions,
                encoder.first_partition_start,
                last_partition_end,
            )?;
        }

        encoder
            .chunked_writer
            .finish(encoder.additional_data.clone())?;
        let data_len = (encoder.current_buffer.len() - 8) as u64;
        encoder.current_buffer[0..8].copy_from_slice(&data_len.to_le_bytes());
        Ok(encoder.current_buffer.as_ptr())
    };

    match finish() {
        Ok(ptr) => ptr,
        Err(mut err) => {
            err.add_context("error in StreamingPartitionEncoder.finish");
            err.into_cairo_exception().throw::<*const u8>(&mut env);
            std::ptr::null()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_closeStreamingParquetWriter(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
) {
    if encoder.is_null() {
        let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
        err.add_context("error in StreamingPartitionEncoder.destroy");
        return err.into_cairo_exception().throw::<()>(&mut env);
    }

    let _ = unsafe { Box::from_raw(encoder) };
}

fn create_partition_template(
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_meta_data_ptr: *const i64,
    timestamp_index: jint,
    timestamp_descending: jboolean,
) -> ParquetResult<Partition> {
    let col_count = col_count as usize;
    let col_names_len = col_names_len as usize;

    let mut col_names = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(col_names_ptr, col_names_len))
    };
    let col_meta_datas = unsafe { slice::from_raw_parts(col_meta_data_ptr, col_count * 2) };
    let mut columns = vec![];

    for col_idx in 0..col_count {
        let raw_idx = col_idx * 2;
        let col_name_size = col_meta_datas[raw_idx];
        let (col_name, tail) = col_names.split_at(col_name_size as usize);
        col_names = tail;

        let packed = col_meta_datas[raw_idx + 1];
        let col_id = (packed >> 32) as i32;
        let col_type = (packed & 0xFFFFFFFF) as i32;
        let designated_timestamp = col_idx as i32 == timestamp_index;
        let column = Column::from_raw_data(
            col_id,
            col_name,
            col_type,
            0,
            0,
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            designated_timestamp,
            timestamp_descending == 0,
        )?;

        columns.push(column);
    }

    Ok(Partition { table: String::new(), columns })
}

fn update_partition_data(
    partition: &mut Partition,
    col_data_ptr: *const i64,
    row_count: usize,
) -> ParquetResult<()> {
    const COL_DATA_ENTRY_SIZE: usize = 7;
    let col_data = unsafe {
        slice::from_raw_parts(col_data_ptr, partition.columns.len() * COL_DATA_ENTRY_SIZE)
    };

    for (col_idx, column) in partition.columns.iter_mut().enumerate() {
        let raw_idx = col_idx * COL_DATA_ENTRY_SIZE;
        let col_top = col_data[raw_idx];
        let primary_col_addr = col_data[raw_idx + 1];
        let primary_col_size = col_data[raw_idx + 2];
        let secondary_col_addr = col_data[raw_idx + 3];
        let secondary_col_size = col_data[raw_idx + 4];
        let symbol_offsets_addr = col_data[raw_idx + 5];
        let symbol_offsets_size = col_data[raw_idx + 6];
        let primary_ptr = primary_col_addr as *const u8;
        let secondary_ptr = secondary_col_addr as *const u8;
        let symbol_offsets_ptr = symbol_offsets_addr as *const u64;

        column.column_top = col_top as usize;
        column.row_count = row_count;
        column.primary_data = if primary_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(primary_ptr, primary_col_size as usize) }
        };
        column.secondary_data = if secondary_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(secondary_ptr, secondary_col_size as usize) }
        };
        column.symbol_offsets = if symbol_offsets_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(symbol_offsets_ptr, symbol_offsets_size as usize) }
        };
    }

    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_writeStreamingParquetChunkFromRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
    allocator_ptr: *const QdbAllocator,
    symbol_data_ptr: jlong,
    source_parquet_addr: jlong,
    source_parquet_size: jlong,
    row_group_index: jint,
    row_group_lo: jint,
    row_group_hi: jint,
) -> *const u8 {
    if encoder.is_null() {
        let mut err = fmt_err!(InvalidType, "StreamingParquetWriter pointer is null");
        err.add_context("error in writeStreamingParquetChunkFromParquet");
        err.into_cairo_exception().throw::<*const u8>(&mut env);
        return std::ptr::null();
    }

    let encoder = unsafe { &mut *encoder };
    let mut write_chunk = || -> ParquetResult<*const u8> {
        let row_count = (row_group_hi - row_group_lo) as usize;
        if row_count > 0 {
            use crate::parquet_read::{DecodeContext, ParquetDecoder, RowGroupBuffers};
            use std::io::Cursor;
            let source_data = unsafe {
                slice::from_raw_parts(
                    source_parquet_addr as *const u8,
                    source_parquet_size as usize,
                )
            };
            let mut reader = Cursor::new(source_data);
            let allocator = unsafe { &*allocator_ptr }.clone();
            let decoder =
                ParquetDecoder::read(allocator.clone(), &mut reader, source_parquet_size as u64)?;
            let mut row_group_bufs = RowGroupBuffers::new(allocator);
            let mut ctx =
                DecodeContext::new(source_parquet_addr as *const u8, source_parquet_size as u64);
            let columns: Vec<(i32, qdb_core::col_type::ColumnType)> = encoder
                .partition
                .columns
                .iter()
                .enumerate()
                .map(|(i, col)| (i as i32, col.data_type))
                .collect();

            decoder.decode_row_group(
                &mut ctx,
                &mut row_group_bufs,
                &columns,
                row_group_index as u32,
                row_group_lo as u32,
                row_group_hi as u32,
            )?;

            let partition = convert_row_group_buffers_to_partition(
                &encoder.partition,
                &row_group_bufs,
                row_count,
                symbol_data_ptr,
            )?;
            encoder.pending_partitions.push(partition);
            encoder.pending_row_group_buffers.push(Some(row_group_bufs));
            encoder.accumulated_rows += row_count;
        }

        flush_pending_partitions(encoder)
    };

    match write_chunk() {
        Ok(ptr) => ptr,
        Err(mut err) => {
            err.add_context("error in writeStreamingParquetChunkFromRowGroup");
            err.into_cairo_exception().throw::<*const u8>(&mut env);
            std::ptr::null()
        }
    }
}

fn convert_row_group_buffers_to_partition(
    partition_template: &Partition,
    row_group_bufs: &crate::parquet_read::RowGroupBuffers,
    row_count: usize,
    symbol_data_ptr: jlong,
) -> ParquetResult<Partition> {
    use qdb_core::col_type::ColumnTypeTag;

    let mut new_partition = Partition {
        table: String::new(),
        columns: Vec::with_capacity(partition_template.columns.len()),
    };
    let column_bufs = row_group_bufs.column_buffers();

    // For each Symbol column: [values_ptr (i64), values_size (i64), offsets_ptr (i64), symbol_count (i64)]
    let symbol_data = if symbol_data_ptr != 0 {
        let symbol_count = partition_template
            .columns
            .iter()
            .filter(|col| col.data_type.tag() == ColumnTypeTag::Symbol)
            .count();

        if symbol_count > 0 {
            unsafe { slice::from_raw_parts(symbol_data_ptr as *const i64, symbol_count * 4) }
        } else {
            &[]
        }
    } else {
        &[]
    };

    let mut symbol_data_idx = 0;

    for (i, column_template) in partition_template.columns.iter().enumerate() {
        let col_buf = &column_bufs[i];
        let mut column = *column_template;
        column.row_count = row_count;
        column.column_top = 0;
        column.primary_data =
            unsafe { slice::from_raw_parts(col_buf.data_ptr as *const u8, col_buf.data_size) };

        if column.data_type.tag() == ColumnTypeTag::Symbol {
            let values_ptr = symbol_data[symbol_data_idx] as *const u8;
            let values_size = symbol_data[symbol_data_idx + 1] as usize;
            let offsets_ptr = symbol_data[symbol_data_idx + 2] as *const u64;
            let symbol_count = symbol_data[symbol_data_idx + 3] as usize;
            symbol_data_idx += 4;
            if !values_ptr.is_null() && values_size > 0 {
                column.secondary_data = unsafe { slice::from_raw_parts(values_ptr, values_size) };
            } else {
                column.secondary_data = &[];
            }

            if !offsets_ptr.is_null() && symbol_count > 0 {
                column.symbol_offsets = unsafe { slice::from_raw_parts(offsets_ptr, symbol_count) };
            } else {
                column.symbol_offsets = &[];
            }
        } else {
            column.secondary_data =
                unsafe { slice::from_raw_parts(col_buf.aux_ptr as *const u8, col_buf.aux_size) };
            column.symbol_offsets = &[];
        }

        new_partition.columns.push(column);
    }
    Ok(new_partition)
}
