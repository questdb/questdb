use std::fs::File;
use std::path::Path;
use std::slice;

use anyhow::Context;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use parquet2::metadata::SortingColumn;

use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::schema::{Column, Partition};

fn read_utf8_encoded_string_list(
    count: usize,
    strings_sink: *const u8,
    strings_len: usize,
    sizes: *const i32,
) -> Vec<&'static str> {
    let mut strings = Vec::new();
    let mut utf8_sink =
        unsafe { std::str::from_utf8_unchecked(slice::from_raw_parts(strings_sink, strings_len)) };

    let sizes = unsafe { slice::from_raw_parts(sizes, count) };
    for size in sizes {
        let (s, tail) = utf8_sink.split_at(*size as usize);
        strings.push(s);
        utf8_sink = tail;
    }

    strings
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_encodePartition(
    mut env: JNIEnv,
    _class: JClass,
    table_name_ptr: *const u8,
    table_name_size: jint,
    col_count: jint,
    col_names: *const u8,
    col_names_len: i32,
    col_name_sizes_ptr: *const i32,
    col_types_ptr: *const i32,
    col_ids_ptr: *const i32,
    timestamp_index: jint,
    col_tops_ptr: *const i64,
    primary_col_addrs_ptr: *const *const u8,
    primary_col_sizes_ptr: *const i64,
    secondary_col_addrs_ptr: *const *const u8,
    secondary_col_sizes_ptr: *const i64,
    symbol_offsets_addrs_ptr: *const *const u64,
    symbol_offsets_sizes_ptr: *const i64,
    row_count: jlong,
    dest_path: *const u8,
    dest_path_len: i32,
) {
    let encode = || -> anyhow::Result<()> {
        let col_count = col_count as usize;
        let col_names = read_utf8_encoded_string_list(
            col_count,
            col_names,
            col_names_len as usize,
            col_name_sizes_ptr,
        );
        let col_types = unsafe { slice::from_raw_parts(col_types_ptr, col_count) };
        let col_tops = unsafe { slice::from_raw_parts(col_tops_ptr, col_count) };
        let col_ids = unsafe { slice::from_raw_parts(col_ids_ptr, col_count) };

        let primary_col_addrs_slice =
            unsafe { slice::from_raw_parts(primary_col_addrs_ptr, col_count) };
        let primary_col_sizes_slice =
            unsafe { slice::from_raw_parts(primary_col_sizes_ptr, col_count) };

        let secondary_col_addrs_slice =
            unsafe { slice::from_raw_parts(secondary_col_addrs_ptr, col_count) };
        let secondary_col_sizes_slice =
            unsafe { slice::from_raw_parts(secondary_col_sizes_ptr, col_count) };

        let symbol_offsets_addrs_slice =
            unsafe { slice::from_raw_parts(symbol_offsets_addrs_ptr, col_count) };
        let symbol_offsets_sizes_slice =
            unsafe { slice::from_raw_parts(symbol_offsets_sizes_ptr, col_count) };

        let dest_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(dest_path, dest_path_len as usize))
        };

        let dest_path = Path::new(&dest_path);

        let row_count = row_count as usize;

        let mut columns = vec![];
        for i in 0..col_count {
            let col_id = col_ids[i];
            let col_name = col_names[i];
            let col_type = col_types[i];
            let col_top = col_tops[i];

            let primary_col_addr = primary_col_addrs_slice[i];
            let primary_col_size = primary_col_sizes_slice[i];

            let secondary_col_addr = secondary_col_addrs_slice[i];
            let secondary_col_size = secondary_col_sizes_slice[i];

            let symbol_offsets_addr = symbol_offsets_addrs_slice[i];
            let symbol_offsets_size = symbol_offsets_sizes_slice[i];

            let column = Column::from_raw_data(
                col_id,
                col_name,
                col_type,
                col_top,
                row_count,
                primary_col_addr,
                primary_col_size as usize,
                secondary_col_addr,
                secondary_col_size as usize,
                symbol_offsets_addr,
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

        let mut file = File::create(dest_path).with_context(|| {
            format!(
                "Could not send create parquet file for {}",
                dest_path.display()
            )
        })?;

        let sorting_columns = if timestamp_index != -1 {
            Some(vec![SortingColumn::new(timestamp_index, false, false)])
        } else {
            None
        };

        ParquetWriter::new(&mut file)
            .with_sorting_columns(sorting_columns)
            .finish(partition)
            .map(|_| ())
            .context("")
    };

    match encode() {
        Ok(_) => (),
        Err(err) => {
            if let Some(jni_err) = err.downcast_ref::<jni::errors::Error>() {
                match jni_err {
                    jni::errors::Error::JavaException => {
                        // Already thrown.
                    }
                    _ => {
                        let msg = format!("Failed to encode partition: {:?}", jni_err);
                        env.throw_new("java/lang/RuntimeException", msg)
                            .expect("failed to throw exception");
                    }
                }
            } else {
                let msg = format!("Failed to encode partition: {:?}", err);
                env.throw_new("java/lang/RuntimeException", msg)
                    .expect("failed to throw exception");
            }
        }
    }
}
