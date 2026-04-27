use crate::ffi_panic_guard::{ffi_guard, ffi_guard_void};
use crate::parquet::error::{fmt_err, ParquetError, ParquetErrorExt, ParquetResult};
use crate::parquet_write::file::{
    ChunkedWriter, ParquetWriter, DEFAULT_BLOOM_FILTER_FPP, DEFAULT_ROW_GROUP_SIZE,
};
use crate::parquet_write::schema::{Column, Partition};
use crate::parquet_write::update::ParquetUpdater;
use qdb_core::col_type::ColumnType;
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::slice;

use crate::allocator::QdbAllocator;
use crate::parquet::io::FromRawFdI32Ext;
use jni::objects::JClass;
use jni::sys::{jboolean, jdouble, jint, jlong};
use jni::JNIEnv;
use parquet2::compression::{BrotliLevel, CompressionOptions, GzipLevel, ZstdLevel};
use parquet2::metadata::{KeyValue, SortingColumn};
use parquet2::write::Version;

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_copyRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
    rg_index: jint,
) {
    ffi_guard_void("PartitionUpdater.copyRowGroup", || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.copyRowGroup");
            return err.into_cairo_exception().throw(&mut env);
        }

        let parquet_updater = unsafe { &mut *updater };
        match parquet_updater.copy_row_group(rg_index) {
            Ok(_) => (),
            Err(mut err) => {
                err.add_context(format!("could not copy row group {rg_index}"));
                err.add_context("error in PartitionUpdater.copyRowGroup");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_setTargetSchema(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
    table_name_ptr: *const u8,
    table_name_len: jint,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    timestamp_index: jint,
) {
    ffi_guard_void("PartitionUpdater.setTargetSchema", || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.setTargetSchema");
            return err.into_cairo_exception().throw(&mut env);
        }

        let parquet_updater = unsafe { &mut *updater };

        let mut set_schema = || -> ParquetResult<()> {
            // Reuse the standard partition descriptor format (9 entries per column)
            // but only the name length (offset 0) and packed id/type (offset 1) are
            // read. Data pointers at offsets 3-8 are ignored for schema building
            // because from_raw_data accepts null pointers with zero sizes.
            let partition = create_partition_descriptor(
                table_name_ptr,
                table_name_len,
                col_count,
                col_names_ptr,
                col_names_len,
                col_data_ptr,
                col_data_len,
                0, // row_count = 0 for schema-only
                timestamp_index,
            )?;
            parquet_updater.set_target_schema(&partition)
        };

        match set_schema() {
            Ok(_) => (),
            Err(mut err) => {
                err.add_context("could not set target schema");
                err.add_context("error in PartitionUpdater.setTargetSchema");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_copyRowGroupWithNullColumns(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
    rg_index: jint,
    null_col_desc_ptr: *const i64,
    null_col_count: jint,
) {
    ffi_guard_void("PartitionUpdater.copyRowGroupWithNullColumns", || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.copyRowGroupWithNullColumns");
            return err.into_cairo_exception().throw(&mut env);
        }

        let parquet_updater = unsafe { &mut *updater };

        let mut copy = || -> ParquetResult<()> {
            let null_columns: Vec<(usize, ColumnType)> =
                if null_col_count > 0 && !null_col_desc_ptr.is_null() {
                    let desc = unsafe {
                        slice::from_raw_parts(null_col_desc_ptr, (null_col_count as usize) * 2)
                    };
                    (0..null_col_count as usize)
                        .map(|i| {
                            let target_pos = desc[i * 2] as usize;
                            let col_type_raw = desc[i * 2 + 1] as i32;
                            let col_type: ColumnType =
                                (col_type_raw & 0x7FFFFFFF).try_into().map_err(|_| {
                                    fmt_err!(InvalidType, "invalid column type {}", col_type_raw)
                                })?;
                            Ok((target_pos, col_type))
                        })
                        .collect::<ParquetResult<Vec<_>>>()?
                } else {
                    vec![]
                };
            parquet_updater.copy_row_group_with_null_columns(rg_index, &null_columns)
        };

        match copy() {
            Ok(_) => (),
            Err(mut err) => {
                err.add_context(format!(
                    "could not copy row group {rg_index} with null columns"
                ));
                err.add_context("error in PartitionUpdater.copyRowGroupWithNullColumns");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_create(
    mut env: JNIEnv,
    _class: JClass,
    allocator: *const QdbAllocator,
    src_path_len: u32,
    src_path_ptr: *const u8,
    reader_fd: i32,
    read_file_size: u64,
    writer_fd: i32,
    write_file_size: u64,
    timestamp_index: jint,
    compression_codec: jlong,
    statistics_enabled: jboolean,
    raw_array_encoding: jboolean,
    row_group_size: jlong,
    data_page_size: jlong,
    bloom_filter_fpp: jdouble,
    min_compression_ratio: jdouble,
    parquet_meta_fd: jint,
    parquet_meta_file_size: jlong,
    existing_parquet_meta_file_size: jlong,
) -> *mut ParquetUpdater {
    ffi_guard("PartitionUpdater.create", std::ptr::null_mut(), || {
        let create = || -> ParquetResult<ParquetUpdater> {
            let (reader_file, writer_file, parquet_meta_fd_handle) =
                take_partition_updater_fds(reader_fd, writer_fd, parquet_meta_fd)?;

            let compression_options =
                compression_from_i64(compression_codec).context("CompressionCodec")?;
            // SAFETY: Pointer was passed from Java and points to a valid allocator for the JNI call duration.
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
                reader_file,
                read_file_size,
                writer_file,
                write_file_size,
                sorting_columns,
                statistics_enabled,
                raw_array_encoding,
                compression_options,
                row_group_size,
                data_page_size,
                bloom_filter_fpp,
                min_compression_ratio,
                parquet_meta_fd_handle,
                parquet_meta_file_size as u64,
                existing_parquet_meta_file_size,
            )
        };

        match create() {
            Ok(updater) => Box::into_raw(Box::new(updater)),
            Err(mut err) => {
                // SAFETY: JNI caller guarantees a valid pointer to `src_path_len` bytes of path data.
                // The memory remains valid for the JNI call duration.
                let src_path =
                    unsafe { slice::from_raw_parts(src_path_ptr, src_path_len as usize) };
                let src_path = std::str::from_utf8(src_path).unwrap_or("!!invalid path utf8!!");
                err.add_context(format!(
                    "could not open parquet file for update from path {src_path}"
                ));
                err.add_context("error in PartitionUpdater.create");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_destroy(
    _env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
) {
    ffi_guard_void("PartitionUpdater.destroy", || {
        if updater.is_null() {
            return;
        }

        // SAFETY: Pointer was created by `Box::into_raw` in the corresponding create function.
        // Java guarantees a single destroy call and no further use after destroy.
        let _ = unsafe { Box::from_raw(updater) };
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_updateFileMetadata(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
) -> jlong {
    ffi_guard("PartitionUpdater.updateFileMetadata", -1, || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.updateFileMetadata");
            return err.into_cairo_exception().throw::<jlong>(&mut env);
        }

        // SAFETY: Pointer was created by `Box::into_raw` in the create function.
        // Single-threaded JNI access guarantees no aliasing.
        let parquet_updater = unsafe { &mut *updater };
        match parquet_updater.end(None) {
            Ok(file_size) => file_size as jlong,
            Err(mut err) => {
                err.add_context("could not update partition metadata");
                err.add_context("error in PartitionUpdater.updateFileMetadata");
                err.into_cairo_exception().throw::<jlong>(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_getResultUnusedBytes(
    mut env: JNIEnv,
    _class: JClass,
    updater: *const ParquetUpdater,
) -> jlong {
    ffi_guard("PartitionUpdater.getResultUnusedBytes", -1, || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.getResultUnusedBytes");
            return err.into_cairo_exception().throw::<jlong>(&mut env);
        }

        let parquet_updater = unsafe { &*updater };
        parquet_updater.result_unused_bytes() as jlong
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_getResultParquetMetaFileSize(
    mut env: JNIEnv,
    _class: JClass,
    updater: *const ParquetUpdater,
) -> jlong {
    ffi_guard("PartitionUpdater.getResultParquetMetaFileSize", -1, || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.getResultParquetMetaFileSize");
            return err.into_cairo_exception().throw::<jlong>(&mut env);
        }

        let parquet_updater = unsafe { &*updater };
        parquet_updater.result_parquet_meta_size()
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_syncParquetMeta(
    mut env: JNIEnv,
    _class: JClass,
    updater: *mut ParquetUpdater,
) {
    ffi_guard_void("PartitionUpdater.syncParquetMeta", || {
        if updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.syncParquetMeta");
            err.into_cairo_exception().throw::<()>(&mut env);
            return;
        }

        let parquet_updater = unsafe { &mut *updater };
        if let Err(mut err) = parquet_updater.sync_parquet_meta() {
            err.add_context("error in PartitionUpdater.syncParquetMeta");
            err.into_cairo_exception().throw::<()>(&mut env);
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_updateRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    parquet_updater: *mut ParquetUpdater,
    table_name_len: u32,
    table_name_ptr: *const u8,
    row_group_id: jint,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    timestamp_index: jint,
    row_count: jlong,
) {
    ffi_guard_void("PartitionUpdater.updateRowGroup", || {
        if parquet_updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.updateRowGroup");
            return err.into_cairo_exception().throw(&mut env);
        }

        // SAFETY: Pointer was created by `Box::into_raw` in the create function.
        // Single-threaded JNI access guarantees no aliasing.
        let parquet_updater = unsafe { &mut *parquet_updater };

        let mut update = || -> ParquetResult<()> {
            let partition = create_partition_descriptor(
                table_name_ptr,
                table_name_len as i32,
                col_count,
                col_names_ptr,
                col_names_len,
                col_data_ptr,
                col_data_len,
                row_count,
                timestamp_index,
            )?;
            parquet_updater.replace_row_group(&partition, row_group_id)
        };

        match update() {
            Ok(_) => (),
            Err(mut err) => {
                // SAFETY: JNI caller guarantees a valid pointer to `table_name_len` bytes of table name data.
                // The memory remains valid for the JNI call duration.
                let table_name =
                    unsafe { slice::from_raw_parts(table_name_ptr, table_name_len as usize) };
                let table_name =
                    std::str::from_utf8(table_name).unwrap_or("!!invalid table_dir_name utf8!!");
                err.add_context(format!(
                    "could not update row group {row_group_id} for table {table_name}"
                ));
                err.add_context("error in PartitionUpdater.updateRowGroup");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionUpdater_insertRowGroup(
    mut env: JNIEnv,
    _class: JClass,
    parquet_updater: *mut ParquetUpdater,
    table_name_len: u32,
    table_name_ptr: *const u8,
    position: jint,
    col_count: jint,
    col_names_ptr: *const u8,
    col_names_len: jint,
    col_data_ptr: *const i64,
    col_data_len: jlong,
    timestamp_index: jint,
    row_count: jlong,
) {
    ffi_guard_void("PartitionUpdater.insertRowGroup", || {
        if parquet_updater.is_null() {
            let mut err = fmt_err!(InvalidType, "ParquetUpdater pointer is null");
            err.add_context("error in PartitionUpdater.insertRowGroup");
            return err.into_cairo_exception().throw(&mut env);
        }
        let parquet_updater = unsafe { &mut *parquet_updater };

        let mut insert = || -> ParquetResult<()> {
            let partition = create_partition_descriptor(
                table_name_ptr,
                table_name_len as i32,
                col_count,
                col_names_ptr,
                col_names_len,
                col_data_ptr,
                col_data_len,
                row_count,
                timestamp_index,
            )?;
            parquet_updater.insert_row_group(&partition, position)
        };

        match insert() {
            Ok(_) => (),
            Err(mut err) => {
                let table_name =
                    unsafe { slice::from_raw_parts(table_name_ptr, table_name_len as usize) };
                let table_name =
                    std::str::from_utf8(table_name).unwrap_or("!!invalid table_dir_name utf8!!");
                err.add_context(format!(
                    "could not insert row group at position {position} for table {table_name}"
                ));
                err.add_context("error in PartitionUpdater.insertRowGroup");
                err.into_cairo_exception().throw(&mut env)
            }
        }
    })
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
    min_compression_ratio: jdouble,
    parquet_meta_fd: jint,
    squash_tracker: jlong,
) -> jlong {
    ffi_guard("PartitionEncoder.encodePartition", -1, || {
        let encode = || -> ParquetResult<i64> {
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

            // SAFETY: JNI caller guarantees a valid pointer to `dest_path_len` bytes of path data.
            // The memory is backed by Java and remains valid for the JNI call duration.
            // Java guarantees valid UTF-8 for file paths (validated on creation).
            let dest_path = unsafe {
                std::str::from_utf8_unchecked(slice::from_raw_parts(
                    dest_path,
                    dest_path_len as usize,
                ))
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

            let bloom_filter_cols = build_bloom_filter_set(
                bloom_filter_column_indexes,
                bloom_filter_column_count,
                partition.columns.len(),
            )?;

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

            // Break apart ParquetWriter::finish() to access row groups after writing.
            let writer = ParquetWriter::new(&mut file)
                .with_version(version)
                .with_compression(compression_options)
                .with_statistics(statistics_enabled)
                .with_raw_array_encoding(raw_array_encoding)
                .with_row_group_size(row_group_size)
                .with_data_page_size(data_page_size)
                .with_sorting_columns(sorting_columns.clone())
                .with_bloom_filter_columns(bloom_filter_cols)
                .with_bloom_filter_fpp(bloom_filter_fpp)
                .with_min_compression_ratio(min_compression_ratio)
                .with_squash_tracker(squash_tracker);

            let (schema, additional_meta) = crate::parquet_write::schema::to_parquet_schema(
                &partition,
                raw_array_encoding,
                squash_tracker,
            )?;
            let encodings = crate::parquet_write::schema::to_encodings(&partition);
            let compressions = crate::parquet_write::schema::to_compressions(&partition);
            let mut chunked = writer.chunked_with_compressions(schema, encodings, compressions)?;
            chunked.write_chunk(&partition)?;
            let parquet_file_size = chunked
                .finish(additional_meta)
                .context("ParquetWriter::finish failed")?;

            // Generate _pm from the in-memory thrift row groups.
            if parquet_meta_fd < 0 {
                return Ok(parquet_file_size as i64);
            }

            let footer_offset = chunked.parquet_footer_offset();
            let footer_length = parquet_file_size
                .checked_sub(footer_offset)
                .and_then(|v| v.checked_sub(8))
                .ok_or_else(|| {
                    crate::parquet::error::fmt_err!(
                        InvalidLayout,
                        "parquet footer offset {} exceeds file size {}",
                        footer_offset,
                        parquet_file_size
                    )
                })? as u32;
            let col_infos = build_column_infos_from_partition(
                &partition,
                chunked.schema().columns(),
                sorting_columns.as_deref(),
            );
            let sorting_col_indices: Vec<u32> = sorting_columns
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|sc| sc.column_idx as u32)
                .collect();

            let designated_ts = partition
                .columns
                .iter()
                .position(|c| c.designated_timestamp)
                .map(|i| i as i32)
                .unwrap_or(-1);

            let (parquet_meta_bytes, _parquet_meta_footer_offset) =
                crate::parquet_metadata::generate_parquet_metadata(
                    &col_infos,
                    chunked.schema().columns(),
                    chunked.row_groups(),
                    designated_ts,
                    &sorting_col_indices,
                    footer_offset,
                    footer_length,
                    chunked.bloom_bitsets(),
                    0, // unused_bytes: new file, no dead space
                    squash_tracker,
                )
                .context("generate_parquet_metadata failed")?;

            // Write to the parquet metadata fd. ManuallyDrop ensures Rust never
            // closes the fd — Java owns it.
            let mut parquet_meta_file: std::mem::ManuallyDrop<File> =
                std::mem::ManuallyDrop::new(unsafe {
                    crate::parquet::io::FromRawFdI32Ext::from_raw_fd_i32(parquet_meta_fd)
                });
            parquet_meta_file
                .write_all(&parquet_meta_bytes)
                .map_err(crate::parquet::error::ParquetError::from)
                .context("could not write _pm file")?;

            Ok(parquet_file_size as i64)
        };

        match encode() {
            Ok(size) => size,
            Err(mut err) => {
                // SAFETY: JNI caller guarantees a valid pointer to `table_name_size` bytes of table name data.
                // The memory remains valid for the JNI call duration.
                let table_name = unsafe {
                    std::str::from_utf8(slice::from_raw_parts(
                        table_name_ptr,
                        table_name_size as usize,
                    ))
                    .unwrap_or("!!invalid table name utf8!!")
                };
                err.add_context(format!(
                "could not encode partition for table {table_name} and timestamp index {timestamp_index}"
            ));
                err.add_context("error in PartitionEncoder.encodePartition");
                err.into_cairo_exception().throw::<i64>(&mut env)
            }
        }
    })
}

/// Takes ownership of the raw fds passed by Java, returning (reader, writer,
/// optional parquet meta) `File` wrappers. Both `reader_fd` and `writer_fd` must be
/// distinct OS file descriptors; aliased fds would otherwise be closed twice
/// on Drop.
///
/// On any error, the fds that have already been wrapped are dropped here, so
/// the caller does not need to clean them up. `parquet_meta_fd < 0` means
/// "no parquet meta file" (the Some/None toggle on the returned tuple element).
fn take_partition_updater_fds(
    reader_fd: i32,
    writer_fd: i32,
    parquet_meta_fd: i32,
) -> ParquetResult<(File, File, Option<File>)> {
    // Wrap in File FIRST so Drop closes every fd on any error path; Java has
    // already detached these fds from its side. Avoid wrapping writer_fd when
    // it aliases reader_fd, because dropping two File wrappers over the same
    // fd would cause a double-close.
    let reader_file = unsafe { File::from_raw_fd_i32(reader_fd) };
    let writer_file_opt = if reader_fd == writer_fd {
        None
    } else {
        Some(unsafe { File::from_raw_fd_i32(writer_fd) })
    };
    let parquet_meta_fd_handle = if parquet_meta_fd >= 0 {
        Some(unsafe { FromRawFdI32Ext::from_raw_fd_i32(parquet_meta_fd) })
    } else {
        None
    };

    match writer_file_opt {
        Some(writer_file) => Ok((reader_file, writer_file, parquet_meta_fd_handle)),
        // reader_file and parquet_meta_fd_handle drop here, closing their fds exactly once.
        None => Err(fmt_err!(
            InvalidLayout,
            "reader_fd and writer_fd must be different file descriptors, got {}",
            reader_fd
        )),
    }
}

fn build_column_infos_from_partition<'a>(
    partition: &'a crate::parquet_write::schema::Partition,
    schema_columns: &[parquet2::metadata::ColumnDescriptor],
    sorting_columns: Option<&[parquet2::metadata::SortingColumn]>,
) -> Vec<crate::parquet_metadata::ParquetMetaColumnInfo<'a>> {
    partition
        .columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let col_type = if col.designated_timestamp {
                col.data_type
                    .into_designated_with_order(col.designated_timestamp_ascending)
                    .unwrap_or(col.data_type)
            } else {
                col.data_type
            };

            let mut flags = crate::parquet_metadata::types::ColumnFlags::new();
            let repetition = if col.not_null_hint {
                crate::parquet_metadata::types::FieldRepetition::Required
            } else {
                crate::parquet_metadata::types::FieldRepetition::Optional
            };
            flags = flags.with_repetition(repetition);

            if col.data_type.tag() == qdb_core::col_type::ColumnTypeTag::Symbol {
                flags = flags.with_local_key_is_global();
            }

            if let Some(scs) = sorting_columns {
                if let Some(sc) = scs.iter().find(|sc| sc.column_idx == i as i32) {
                    if sc.descending {
                        flags = flags.with_descending();
                    }
                }
            }

            let (physical_type, fixed_byte_len, max_rep_level, max_def_level) =
                if let Some(col_desc) = schema_columns.get(i) {
                    let phys_type = col_desc.descriptor.primitive_type.physical_type;
                    let fbl = match phys_type {
                        parquet2::schema::types::PhysicalType::FixedLenByteArray(len) => len as i32,
                        _ => 0,
                    };
                    (
                        crate::parquet_metadata::physical_type_to_u8(phys_type),
                        fbl,
                        col_desc.descriptor.max_rep_level as u8,
                        col_desc.descriptor.max_def_level as u8,
                    )
                } else {
                    (0, 0, 0, 0)
                };

            crate::parquet_metadata::ParquetMetaColumnInfo {
                name: col.name,
                col_type_code: col_type.code(),
                col_type_tag: Some(col.data_type.tag()),
                id: col.id,
                flags,
                fixed_byte_len,
                physical_type,
                max_rep_level,
                max_def_level,
            }
        })
        .collect()
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

    const COL_DATA_ENTRY_SIZE: usize = 10;
    if !col_data_len.is_multiple_of(COL_DATA_ENTRY_SIZE) {
        return Err(fmt_err!(
            Layout,
            "col_data_len {} is not a multiple of {}",
            col_data_len,
            COL_DATA_ENTRY_SIZE
        ));
    }

    // SAFETY: JNI caller guarantees a valid pointer to `col_names_len` bytes of column name data.
    // The memory is backed by Java and remains valid for the JNI call duration.
    // Java guarantees valid UTF-8 for column names (validated on creation).
    let mut col_names = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(col_names_ptr, col_names_len))
    };
    // SAFETY: JNI caller guarantees a valid pointer to `col_data_len` elements of column metadata.
    // The memory is backed by Java and remains valid for the JNI call duration.
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
        let symbol_offsets_count = col_data[raw_idx + 8];

        let parquet_encoding_config = col_data[raw_idx + 9] as i32;

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
            symbol_offsets_count as usize,
            designated_timestamp,
            true,
            parquet_encoding_config,
        )?;

        columns.push(column);
    }

    // SAFETY: JNI caller guarantees a valid pointer to `table_name_size` bytes of table name data.
    // The memory is backed by Java and remains valid for the JNI call duration.
    // Java guarantees valid UTF-8 for table names (validated on creation).
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

fn build_bloom_filter_set(
    indexes_ptr: *const jint,
    count: jint,
    max_columns: usize,
) -> ParquetResult<HashSet<usize>> {
    if indexes_ptr.is_null() || count <= 0 {
        return Ok(HashSet::new());
    }
    let indexes = unsafe { slice::from_raw_parts(indexes_ptr, count as usize) };
    let mut out = HashSet::with_capacity(indexes.len());
    for &i in indexes {
        if i < 0 || (i as usize) >= max_columns {
            return Err(fmt_err!(
                InvalidType,
                "invalid bloom filter column index: {i}"
            ));
        }
        out.insert(i as usize);
    }
    Ok(out)
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
        // SAFETY: `self.buffer` points to a heap-allocated Vec created in `new_with_offset`.
        // `reserve` guarantees sufficient capacity. `copy_nonoverlapping` initializes the bytes
        // up to the new length.
        unsafe {
            let buffer_ref = &mut *self.buffer;
            if buffer_ref.len() <= self.init_offset {
                buffer_ref.resize(self.init_offset, 0);
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
    // Cumulative count of rows that have been fully written to row groups.
    // Used by Java to determine when partition memory can be safely released.
    rows_written_to_row_groups: usize,
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
    min_compression_ratio: jdouble,
) -> *mut StreamingParquetWriter {
    ffi_guard(
        "PartitionEncoder.createStreamingParquetWriter",
        std::ptr::null_mut(),
        || {
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

                let (parquet_schema, additional_data) =
                    crate::parquet_write::schema::to_parquet_schema(
                        &partition_template,
                        raw_array_encoding != 0,
                        -1,
                    )?;
                // SAFETY: Pointer was passed from Java and points to a valid allocator for the JNI call duration.
                let allocator = unsafe { &*allocator_ptr };
                let encodings = crate::parquet_write::schema::to_encodings(&partition_template);
                let per_column_compressions =
                    crate::parquet_write::schema::to_compressions(&partition_template);
                let mut current_buffer = Box::new(Vec::with_capacity_in(8192, allocator.clone()));
                // Reserve 16 bytes for header: [8 bytes data_len][8 bytes rows_written_to_row_groups]
                // SAFETY: `current_buffer` is a Box, so `&mut *current_buffer` yields a valid reference.
                // The raw pointer is stable because Box guarantees a fixed heap address.
                let buffer_writer = unsafe {
                    BufferWriter::new_with_offset(
                        &mut *current_buffer as *mut Vec<u8, QdbAllocator>,
                        16,
                    )
                };
                let bloom_filter_cols = build_bloom_filter_set(
                    bloom_filter_column_indexes,
                    bloom_filter_column_count,
                    partition_template.columns.len(),
                )?;
                let bloom_fpp = if bloom_filter_fpp > 0.0 && bloom_filter_fpp < 1.0 {
                    bloom_filter_fpp
                } else {
                    DEFAULT_BLOOM_FILTER_FPP
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
                    .with_bloom_filter_fpp(bloom_fpp)
                    .with_min_compression_ratio(min_compression_ratio);
                let chunked_writer = parquet_writer.chunked_with_compressions(
                    parquet_schema,
                    encodings,
                    per_column_compressions,
                )?;

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
                    rows_written_to_row_groups: 0,
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
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_writeStreamingParquetChunk(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
    col_data_ptr: *const i64,
    row_count: jlong,
) -> *const u8 {
    ffi_guard(
        "PartitionEncoder.writeStreamingParquetChunk",
        std::ptr::null(),
        || {
            if encoder.is_null() {
                let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
                err.add_context("error in StreamingPartitionEncoder.writeChunk");
                err.into_cairo_exception().throw::<*const u8>(&mut env);
                return std::ptr::null();
            }
            // SAFETY: Pointer was created by `Box::into_raw` in the create function.
            // Single-threaded JNI access guarantees no aliasing.
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
        },
    )
}

fn flush_pending_partitions(encoder: &mut StreamingParquetWriter) -> ParquetResult<*const u8> {
    if encoder.accumulated_rows >= encoder.row_group_size {
        // SAFETY: Truncating to zero is always valid.
        unsafe {
            encoder.current_buffer.set_len(0);
        }
        write_pending_row_group(encoder)?;
        // Buffer layout: [8 bytes data_len][8 bytes rows_written_to_row_groups][data...]
        debug_assert!(
            encoder.current_buffer.len() >= 16,
            "streaming parquet writer must produce at least a 16-byte header",
        );
        let data_len = encoder.current_buffer.len().saturating_sub(16) as u64;
        encoder.current_buffer[0..8].copy_from_slice(&data_len.to_le_bytes());
        encoder.current_buffer[8..16]
            .copy_from_slice(&(encoder.rows_written_to_row_groups as u64).to_le_bytes());
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
            partition_rows.saturating_sub(first_start)
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

    // Track rows written to row groups (always row_group_size for intermediate flushes)
    encoder.rows_written_to_row_groups += row_group_size;

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
                rows.saturating_sub(encoder.first_partition_start)
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
    ffi_guard(
        "PartitionEncoder.finishStreamingParquetWrite",
        std::ptr::null(),
        || {
            if encoder.is_null() {
                let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
                err.add_context("error in StreamingPartitionEncoder.finish");
                err.into_cairo_exception().throw::<*const u8>(&mut env);
                return std::ptr::null();
            }

            // SAFETY: Pointer was created by `Box::into_raw` in the create function.
            // Single-threaded JNI access guarantees no aliasing.
            let encoder = unsafe { &mut *encoder };
            let mut finish = || -> ParquetResult<*const u8> {
                // SAFETY: Truncating to zero is always valid.
                unsafe {
                    encoder.current_buffer.set_len(0);
                }

                if !encoder.pending_partitions.is_empty() && encoder.accumulated_rows > 0 {
                    let last_idx = encoder.pending_partitions.len() - 1;
                    let last_partition_end =
                        encoder.pending_partitions[last_idx].columns[0].row_count;

                    let partitions: Vec<&Partition> = encoder.pending_partitions.iter().collect();
                    encoder.chunked_writer.write_row_group_from_partitions(
                        &partitions,
                        encoder.first_partition_start,
                        last_partition_end,
                    )?;

                    // Track remaining rows written in the final row group
                    encoder.rows_written_to_row_groups += encoder.accumulated_rows;
                }

                encoder
                    .chunked_writer
                    .finish(encoder.additional_data.clone())?;
                // Buffer layout: [8 bytes data_len][8 bytes rows_written_to_row_groups][data...]
                debug_assert!(
                    encoder.current_buffer.len() >= 16,
                    "streaming parquet writer must produce at least a 16-byte header",
                );
                let data_len = encoder.current_buffer.len().saturating_sub(16) as u64;
                encoder.current_buffer[0..8].copy_from_slice(&data_len.to_le_bytes());
                encoder.current_buffer[8..16]
                    .copy_from_slice(&(encoder.rows_written_to_row_groups as u64).to_le_bytes());
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
        },
    )
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_closeStreamingParquetWriter(
    mut env: JNIEnv,
    _class: JClass,
    encoder: *mut StreamingParquetWriter,
) {
    ffi_guard_void("PartitionEncoder.closeStreamingParquetWriter", || {
        if encoder.is_null() {
            let mut err = fmt_err!(InvalidType, "StreamingParquetEncoder pointer is null");
            err.add_context("error in StreamingPartitionEncoder.destroy");
            return err.into_cairo_exception().throw::<()>(&mut env);
        }

        // SAFETY: Pointer was created by `Box::into_raw` in the corresponding create function.
        // Java guarantees a single destroy call and no further use after destroy.
        let _ = unsafe { Box::from_raw(encoder) };
    })
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_ParquetEncoding_isEncodingValid0(
    _env: JNIEnv,
    _class: JClass,
    encoding_id: jint,
    col_type_tag: jint,
) -> jboolean {
    ffi_guard("ParquetEncoding.isEncodingValid0", 0, || {
        if crate::parquet_write::schema::is_encoding_valid_for_column_tag(encoding_id, col_type_tag)
        {
            1
        } else {
            0
        }
    })
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

    // SAFETY: JNI caller guarantees a valid pointer to `col_names_len` bytes of column name data.
    // The memory is backed by Java and remains valid for the JNI call duration.
    // Java guarantees valid UTF-8 for column names (validated on creation).
    let mut col_names = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(col_names_ptr, col_names_len))
    };
    // SAFETY: JNI caller guarantees a valid pointer to `col_count * 3` elements of column metadata
    // (name length, packed writer-index/type, parquet encoding config). The memory is backed by
    // Java and remains valid for the JNI call duration.
    let col_meta_datas = unsafe { slice::from_raw_parts(col_meta_data_ptr, col_count * 3) };
    let mut columns = vec![];
    let mut max_id: i32 = 0;

    for col_idx in 0..col_count {
        let raw_idx = col_idx * 3;
        let col_name_size = col_meta_datas[raw_idx] as usize;
        if col_name_size > col_names.len() {
            return Err(fmt_err!(
                InvalidLayout,
                "column name size {} exceeds remaining name buffer length {}",
                col_name_size,
                col_names.len()
            ));
        }
        let (col_name, tail) = col_names.split_at(col_name_size);
        col_names = tail;

        let packed = col_meta_datas[raw_idx + 1];
        let col_id = (packed >> 32) as i32;
        let col_type = (packed & 0xFFFFFFFF) as i32;
        let parquet_encoding_config = col_meta_datas[raw_idx + 2] as i32;
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
            parquet_encoding_config,
        )?;

        if col_id < 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "column '{}' (index {}) has invalid field_id {}",
                col_name,
                col_idx,
                col_id
            ));
        }

        if col_id > max_id {
            max_id = col_id;
        }

        columns.push(column);
    }

    // Check for duplicate field_ids (ids are dense non-negative).
    let mut seen = vec![false; (max_id + 1) as usize];
    for (i, c) in columns.iter().enumerate() {
        let id = c.id as usize;
        if seen[id] {
            return Err(fmt_err!(
                InvalidLayout,
                "duplicate field_id {} at column '{}' (index {})",
                c.id,
                c.name,
                i
            ));
        }
        seen[id] = true;
    }

    Ok(Partition { table: String::new(), columns })
}

fn update_partition_data(
    partition: &mut Partition,
    col_data_ptr: *const i64,
    row_count: usize,
) -> ParquetResult<()> {
    const COL_DATA_ENTRY_SIZE: usize = 7;
    // SAFETY: JNI caller guarantees a valid pointer to `columns.len() * COL_DATA_ENTRY_SIZE`
    // elements of column data. The memory is backed by Java and remains valid for the JNI call duration.
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
        let symbol_offsets_count = col_data[raw_idx + 6];
        let primary_ptr = primary_col_addr as *const u8;
        let secondary_ptr = secondary_col_addr as *const u8;
        let symbol_offsets_ptr = symbol_offsets_addr as *const u64;

        column.column_top = col_top as usize;
        column.row_count = row_count;
        column.primary_data = if primary_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer to `primary_col_size` bytes of column data.
            // The memory is backed by Java memory-mapped files and remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(primary_ptr, primary_col_size as usize) }
        };
        column.secondary_data = if secondary_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer to `secondary_col_size` bytes of column data.
            // The memory is backed by Java memory-mapped files and remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(secondary_ptr, secondary_col_size as usize) }
        };
        column.symbol_offsets = if symbol_offsets_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer to `symbol_offsets_size` elements of symbol offset data.
            // The memory is backed by Java memory-mapped files and remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(symbol_offsets_ptr, symbol_offsets_count as usize) }
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
    ffi_guard(
        "PartitionEncoder.writeStreamingParquetChunkFromRowGroup",
        std::ptr::null(),
        || {
            if encoder.is_null() {
                let mut err = fmt_err!(InvalidType, "StreamingParquetWriter pointer is null");
                err.add_context("error in writeStreamingParquetChunkFromParquet");
                err.into_cairo_exception().throw::<*const u8>(&mut env);
                return std::ptr::null();
            }

            // SAFETY: Pointer was created by `Box::into_raw` in the create function.
            // Single-threaded JNI access guarantees no aliasing.
            let encoder = unsafe { &mut *encoder };
            let mut write_chunk = || -> ParquetResult<*const u8> {
                let row_count = (row_group_hi - row_group_lo) as usize;
                if row_count > 0 {
                    use crate::parquet_read::{DecodeContext, ParquetDecoder, RowGroupBuffers};
                    use std::io::Cursor;
                    // SAFETY: JNI caller guarantees a valid pointer to `source_parquet_size` bytes
                    // of source parquet data. The memory remains valid for the JNI call duration.
                    let source_data = unsafe {
                        slice::from_raw_parts(
                            source_parquet_addr as *const u8,
                            source_parquet_size as usize,
                        )
                    };
                    let mut reader = Cursor::new(source_data);
                    // SAFETY: Pointer was passed from Java and points to a valid allocator for the JNI call duration.
                    let allocator = unsafe { &*allocator_ptr }.clone();
                    let decoder = ParquetDecoder::read(
                        allocator.clone(),
                        &mut reader,
                        source_parquet_size as u64,
                    )?;
                    let mut row_group_bufs = RowGroupBuffers::new(allocator);
                    let mut ctx = DecodeContext::new(
                        source_parquet_addr as *const u8,
                        source_parquet_size as u64,
                    );
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
        },
    )
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
            // SAFETY: JNI caller guarantees a valid pointer to `symbol_count * 4` elements of
            // symbol metadata. The memory is backed by Java and remains valid for the JNI call duration.
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
        // SAFETY: `col_buf.data_ptr` points to decoded row group data owned by `row_group_bufs`,
        // which remains alive as long as the partition references it.
        column.primary_data =
            unsafe { slice::from_raw_parts(col_buf.data_ptr as *const u8, col_buf.data_size) };

        if column.data_type.tag() == ColumnTypeTag::Symbol {
            let values_ptr = symbol_data[symbol_data_idx] as *const u8;
            let values_size = symbol_data[symbol_data_idx + 1] as usize;
            let offsets_ptr = symbol_data[symbol_data_idx + 2] as *const u64;
            let symbol_count = symbol_data[symbol_data_idx + 3] as usize;
            symbol_data_idx += 4;
            if !values_ptr.is_null() && values_size > 0 {
                // SAFETY: JNI caller guarantees a valid pointer to `values_size` bytes of symbol value data.
                // The memory is backed by Java and remains valid for the JNI call duration.
                column.secondary_data = unsafe { slice::from_raw_parts(values_ptr, values_size) };
            } else {
                column.secondary_data = &[];
            }

            if !offsets_ptr.is_null() && symbol_count > 0 {
                // SAFETY: JNI caller guarantees a valid pointer to `symbol_count` elements of symbol offset data.
                // The memory is backed by Java and remains valid for the JNI call duration.
                column.symbol_offsets = unsafe { slice::from_raw_parts(offsets_ptr, symbol_count) };
            } else {
                column.symbol_offsets = &[];
            }
        } else {
            // SAFETY: `col_buf.aux_ptr` points to decoded row group auxiliary data owned by
            // `row_group_bufs`, which remains alive as long as the partition references it.
            column.secondary_data =
                unsafe { slice::from_raw_parts(col_buf.aux_ptr as *const u8, col_buf.aux_size) };
            column.symbol_offsets = &[];
        }

        new_partition.columns.push(column);
    }
    Ok(new_partition)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::io::AsRawFd;
    use tempfile::NamedTempFile;

    /// Returns true iff `fd` still refers to an open file descriptor in this
    /// process.
    fn fd_is_open(fd: i32) -> bool {
        // SAFETY: fcntl(F_GETFD) returns -1 and sets errno=EBADF for a closed
        // fd; the call has no side effects on still-open fds.
        unsafe { libc::fcntl(fd, libc::F_GETFD) != -1 }
    }

    #[test]
    fn reader_writer_fd_aliased_returns_err_and_closes_fd() {
        let tmp = NamedTempFile::new().unwrap();
        let fd = tmp.as_file().as_raw_fd();
        // Leak the temp-file handle so we retain the fd after close detection;
        // the Rust side is expected to close it on the error path.
        let _file_retained = tmp.into_file();
        std::mem::forget(_file_retained);

        let result = take_partition_updater_fds(fd, fd, -1);
        assert!(result.is_err(), "expected Err for aliased fds");
        assert!(
            !fd_is_open(fd),
            "aliased reader/writer fd was not closed by the error path"
        );
    }

    #[test]
    fn reader_writer_fd_aliased_also_closes_parquet_meta_fd() {
        let tmp1 = NamedTempFile::new().unwrap();
        let tmp2 = NamedTempFile::new().unwrap();
        let shared_fd = tmp1.as_file().as_raw_fd();
        let parquet_meta_fd_handle = tmp2.as_file().as_raw_fd();
        // Retain both handles as owned fds and detach them.
        std::mem::forget(tmp1.into_file());
        std::mem::forget(tmp2.into_file());

        let result = take_partition_updater_fds(shared_fd, shared_fd, parquet_meta_fd_handle);
        assert!(
            result.is_err(),
            "expected Err for aliased reader/writer fds"
        );
        assert!(!fd_is_open(shared_fd), "shared fd was not closed");
        assert!(
            !fd_is_open(parquet_meta_fd_handle),
            "parquet_meta fd was not closed on the error path"
        );
    }

    #[test]
    fn distinct_fds_return_ok_and_retain_ownership() {
        let tmp_reader = NamedTempFile::new().unwrap();
        let tmp_writer = NamedTempFile::new().unwrap();
        let reader_fd = tmp_reader.as_file().as_raw_fd();
        let writer_fd = tmp_writer.as_file().as_raw_fd();
        std::mem::forget(tmp_reader.into_file());
        std::mem::forget(tmp_writer.into_file());

        let result = take_partition_updater_fds(reader_fd, writer_fd, -1);
        let (reader_file, writer_file, parquet_meta_fd_handle) = result.expect("distinct fds must succeed");
        assert!(parquet_meta_fd_handle.is_none());
        // Drop the returned Files to close the fds; the helper must have
        // given us ownership.
        drop(reader_file);
        drop(writer_file);
        assert!(!fd_is_open(reader_fd));
        assert!(!fd_is_open(writer_fd));
    }
}
