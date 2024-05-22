use std::fs::File;
use std::path::Path;
use std::ptr::null;
use std::slice;
use std::sync::Arc;
use anyhow::Context;
use jni::JNIEnv;
use jni::objects::JClass;
use jni::sys::{jint, jlong};
use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::schema::{ColumnImpl, Partition};

fn read_utf8_encoded_string_list<'a>(count: usize, strings_sink: *const u8, strings_len: usize, lengths: *const i32) -> Vec<&'a str> {
    let mut strings: Vec<&str> = Vec::new();
    let mut utf8_sink = unsafe {
        std::str::from_utf8_unchecked(slice::from_raw_parts(strings_sink, strings_len))
    };

    let lengths = unsafe { slice::from_raw_parts(lengths, count) };
    for len in lengths {
        let (s, tail) = utf8_sink.split_at(*len as usize);
        strings.push(s);
        utf8_sink = tail;
    }

    strings
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionEncoder_encodePartition(
    mut env: JNIEnv,
    _class: JClass,
    col_count: jint,
    col_names: *const u8,
    col_names_len: i32,
    col_name_lengths_ptr: *const i32,
    col_types_ptr: *const i32,
    col_ids_ptr: *const i32,
    timestamp_index: jint,
    _col_tops_ptr: *const i64,
    col_addrs_ptr: *const *const u8,
    _col_secondary_addrs_ptr: *const *const u8,
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
            col_name_lengths_ptr
        );
        let col_types = unsafe { slice::from_raw_parts(col_types_ptr, col_count) };
        let _col_ids = unsafe { slice::from_raw_parts(col_ids_ptr, col_count) };
        let col_addrs = unsafe { slice::from_raw_parts(col_addrs_ptr, col_count) };

        let dest_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(dest_path, dest_path_len as usize))
        };
        let dest_path = Path::new(&dest_path);

        let row_count = row_count as usize;

        let mut columns = vec![];
        for i in 0..col_count {
            let col_name = col_names[i];
            let col_type = col_types[i];
            let col_addr_fixed = col_addrs[i];
            let column = ColumnImpl::from_raw_data(col_name, col_type, row_count, col_addr_fixed, null(), 0)?;
            columns.push(Arc::new(column));
        }

        let partition = Partition {
            table: "test_table".to_string(),
            columns,
        };

        let mut file = File::create(dest_path).with_context(|| {
            format!(
                "Could not send create parquet file for {}",
                dest_path.display()
            )
        })?;

        ParquetWriter::new(&mut file).finish(partition).map(|_| ()).context("")
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