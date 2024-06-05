use std::fs::File;
use std::path::Path;
use std::slice;

use anyhow::anyhow;
use jni::objects::JClass;
use jni::JNIEnv;

use parquet2::encoding::Encoding;
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator, read_metadata};
use parquet2::schema::types::PhysicalType;

// The metadata fields are accessed from Java.
#[repr(C, packed)]
pub struct ParquetDecoder {
    column_count: usize,
    row_count: usize,
    row_group_count: usize,
    reader: File,
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_create(
    mut env: JNIEnv,
    _class: JClass,
    src_path: *const u8,
    src_path_len: i32,
) -> *mut ParquetDecoder {
    let init = || -> anyhow::Result<ParquetDecoder> {
        let src_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(src_path, src_path_len as usize))
        };
        let src_path = Path::new(&src_path);

        let mut reader = File::open(src_path)?;
        let metadata = read_metadata(&mut reader)?;

        let decoder = ParquetDecoder{
            column_count: if metadata.row_groups.len() > 0 { // TODO: check fields instead
                metadata.row_groups[0].columns().len()
            } else {
                0
            },
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len(),
            reader,
        };

        return Ok(decoder);
    };

    match init() {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(e) => throw_state_ex(&mut env, "create_parquet_decoder", e, std::ptr::null_mut()),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_destroy(
    _env: JNIEnv,
    _class: JClass,
    decoder: *mut ParquetDecoder,
) {
    assert!(!decoder.is_null(), "decoder pointer is null");

    unsafe {
        drop(Box::from_raw(decoder));
    }
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_decodeColumnChunk(
    mut env: JNIEnv,
    _class: JClass,
    src_path: *const u8,
    src_path_len: i32,
    row_group: usize,
    column: usize,
    _column_type: i32,
    // [data_ptr: jlong, aux_ptr: jlong]
    buffers_ptr: *mut i64,
) {
    let read = || -> anyhow::Result<()> {
        let src_path = unsafe {
            std::str::from_utf8_unchecked(slice::from_raw_parts(src_path, src_path_len as usize))
        };
        let src_path = Path::new(&src_path);

        // TODO: we shouldn't reopen the file for each column/row group
        let mut reader = std::fs::File::open(src_path)?;
        let metadata = read_metadata(&mut reader)?;

        let columns = metadata.row_groups[row_group].columns();
        let column_metadata = &columns[column];

        // TODO: avoid hardcoding page size
        let pages = get_page_iterator(column_metadata, &mut reader, None, vec![], 1024 * 1024)?;

        let mut decompress_buffer = vec![];
        let mut data_buffer = vec![];
        let mut aux_buffer = vec![];
        let mut dict = None;
        for maybe_page in pages {
            let page = maybe_page?;
            let page = decompress(page, &mut decompress_buffer)?;

            match page {
                Page::Dict(page) => {
                    // TODO: the first page may be a dictionary page, which needs to be deserialized
                    dict = Some(page);
                }
                Page::Data(mut page) => {
                    decode(&mut page, dict.as_ref(), &mut data_buffer, &mut aux_buffer)?;
                }
            }
        }

        unsafe {
            *buffers_ptr = Box::into_raw(Box::new(data_buffer)) as i64;
            *(buffers_ptr.add(1)) = if aux_buffer.len() > 0 {
                Box::into_raw(Box::new(aux_buffer)) as i64
            } else {
                0
            }
        }

        return Ok(());
    };

    match read() {
        Ok(_) => (),
        Err(err) => {
            if let Some(jni_err) = err.downcast_ref::<jni::errors::Error>() {
                match jni_err {
                    jni::errors::Error::JavaException => {
                        // Already thrown.
                    }
                    _ => {
                        let msg = format!("Failed to read partition metadata: {:?}", jni_err);
                        env.throw_new("java/lang/RuntimeException", msg)
                            .expect("failed to throw exception");
                    }
                }
            } else {
                let msg = format!("Failed to read partition metadata: {:?}", err);
                env.throw_new("java/lang/RuntimeException", msg)
                    .expect("failed to throw exception");
            }
        }
    }
}

fn decode(
    page: &mut DataPage,
    dict: Option<&DictPage>,
    data_buffer: &mut Vec<u8>,
    aux_buffer: &mut Vec<u8>,
) -> anyhow::Result<()> {
    let (_rep_levels, _def_levels, _values_buffer) = split_buffer(page)?;

    match (
        page.descriptor.primitive_type.physical_type,
        page.encoding(),
        dict,
    ) {
        (PhysicalType::Int32, Encoding::Plain, None) => {
            data_buffer.append(page.buffer_mut());
            Ok(())
        }
        _ => return Err(anyhow!("deserialization not supported")),
    }
}

fn throw_state_ex<T>(env: &mut JNIEnv, method_name: &str, err: anyhow::Error, def: T) -> T {
    if let Some(jni_err) = err.downcast_ref::<jni::errors::Error>() {
        match jni_err {
            jni::errors::Error::JavaException => {
                // Already thrown.
            }
            _ => {
                let msg = format!("error while {}: {:?}", method_name, jni_err);
                env.throw_new("java/lang/RuntimeException", msg)
                    .expect("failed to throw exception");
            }
        }
    } else {
        let msg = format!("error while {}: {:?}", method_name, err);
        env.throw_new("java/lang/RuntimeException", msg)
            .expect("failed to throw exception");
    }
    def
}
