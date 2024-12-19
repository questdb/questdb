use std::fs::File;
use std::path::Path;
use std::slice;

use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::schema::{Column, Partition};
use crate::parquet_write::update::ParquetUpdater;

use crate::allocator::QdbAllocator;
use crate::parquet::io::FromRawFdI32Ext;
use jni::objects::JClass;
use jni::sys::{jboolean, jint, jlong, jshort};
use jni::JNIEnv;
use parquet2::compression::{BrotliLevel, CompressionOptions, GzipLevel, ZstdLevel};
use parquet2::metadata::SortingColumn;
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
    row_group_size: jlong,
    data_page_size: jlong,
) -> *mut ParquetUpdater {
    let create = || -> ParquetResult<ParquetUpdater> {
        let compression_options =
            compression_from_i64(compression_codec).context("CompressionCodec")?;
        let allocator = unsafe { &*allocator }.clone();

        let statistics_enabled = statistics_enabled != 0;

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
            compression_options,
            row_group_size,
            data_page_size,
        )
    };

    match create() {
        Ok(updater) => Box::into_raw(Box::new(updater)),
        Err(mut err) => {
            let src_path = unsafe { slice::from_raw_parts(src_path_ptr, src_path_len as usize) };
            let src_path = std::str::from_utf8(src_path).unwrap_or("!!invalid path utf8!!");
            err.add_context(format!(
                "could not open parquet file for update from path {}",
                src_path
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
    row_group_size: jlong,
    data_page_size: jlong,
    version: jint,
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
        )?;

        let dest_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(dest_path, dest_path_len as usize))
        };

        let dest_path = Path::new(&dest_path);
        let compression_options =
            compression_from_i64(compression_codec).context("CompressionCodec")?;

        let statistics_enabled = statistics_enabled != 0;
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

        let file: ParquetResult<_> = File::create(dest_path).map_err(|e| e.into());
        let mut file = file.with_context(|_| {
            format!("Could not create parquet file for {}", dest_path.display())
        })?;

        let sorting_columns = if timestamp_index != -1 {
            Some(vec![SortingColumn::new(timestamp_index, false, false)])
        } else {
            None
        };

        ParquetWriter::new(&mut file)
            .with_version(version)
            .with_compression(compression_options)
            .with_statistics(statistics_enabled)
            .with_row_group_size(row_group_size)
            .with_data_page_size(data_page_size)
            .with_sorting_columns(sorting_columns)
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
                "could not encode partition for table {} and timestamp index {}",
                table_name, timestamp_index
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
