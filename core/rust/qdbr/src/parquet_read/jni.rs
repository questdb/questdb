use std::fs::File;
use std::path::Path;
use std::slice;

use anyhow::anyhow;
use jni::objects::JClass;
use jni::JNIEnv;

use parquet2::encoding::Encoding;
use parquet2::metadata::FileMetaData;
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator, read_metadata};
use parquet2::schema::types::ParquetType::{GroupType, PrimitiveType};
use parquet2::schema::types::PhysicalType;

// These constants should match constants in Java's PartitionDecoder.
pub const BOOLEAN: u32 = 0;
pub const INT32: u32 = 1;
pub const INT64: u32 = 2;
pub const INT96: u32 = 3;
pub const FLOAT: u32 = 4;
pub const DOUBLE: u32 = 5;
pub const BYTE_ARRAY: u32 = 6;
pub const FIXED_LEN_BYTE_ARRAY: u32 = 7;

// The metadata fields are accessed from Java.
#[repr(C, align(8))]
pub struct ParquetDecoder {
    col_count: i32,
    row_count: usize,
    row_group_count: i32,
    col_names_ptr: *const u16,        // col_names' pointer exposed to Java
    col_name_lengths_ptr: *const i32, // col_name_lengths' pointer exposed to Java
    col_ids_ptr: *const i32,          // col_ids' pointer exposed to Java
    col_physical_types_ptr: *const i64, // col_physical_types' pointer exposed to Java
    reader: File,
    metadata: FileMetaData,
    decompress_buffer: Vec<u8>,
    col_names: Vec<u16>, // UTF-16 encoded
    col_name_lengths: Vec<i32>,
    col_ids: Vec<i32>,
    col_physical_types: Vec<i64>,
}

impl ParquetDecoder {
    pub fn decode_column_chunk(
        &mut self,
        row_group: usize,
        column: usize,
        _column_type: i32,
        buffers: &mut ColumnChunkBuffers,
    ) -> anyhow::Result<()> {
        // TODO: support input buffers
        assert!(buffers.aux_ptr.is_null());
        assert!(buffers.data_ptr.is_null());

        let columns = self.metadata.row_groups[row_group].columns();
        let column_metadata = &columns[column];

        // TODO: avoid hardcoding page size
        let page_reader =
            get_page_iterator(column_metadata, &mut self.reader, None, vec![], 1024 * 1024)?;

        let mut data_buffer = Vec::with_capacity(column_metadata.uncompressed_size() as usize);
        let mut aux_buffer = vec![];
        let mut dict = None;
        for maybe_page in page_reader {
            let page = maybe_page?;
            let page = decompress(page, &mut self.decompress_buffer)?;

            match page {
                Page::Dict(page) => {
                    // TODO: the first page may be a dictionary page, which needs to be deserialized
                    dict = Some(page);
                }
                Page::Data(mut page) => {
                    decode(&mut page, dict.as_ref(), &mut data_buffer, &mut aux_buffer)?;
                }
            }

            buffers.data_size = data_buffer.len();
            let data_buffer: &mut [u8] = &mut data_buffer;
            buffers.data_ptr = data_buffer.as_mut_ptr();

            buffers.aux_size = aux_buffer.len();
            let aux_buffer: &mut [u8] = &mut aux_buffer;
            buffers.aux_ptr = aux_buffer.as_mut_ptr();
        }

        return Ok(());
    }
}

fn decode(
    page: &mut DataPage,
    dict: Option<&DictPage>,
    data_buffer: &mut Vec<u8>,
    _aux_buffer: &mut Vec<u8>,
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

// Always created from Java.
#[repr(C, align(8))]
pub struct ColumnChunkBuffers {
    data_ptr: *mut u8,
    data_size: usize,
    aux_ptr: *mut u8,
    aux_size: usize,
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

        let col_count = metadata.schema().fields().len() as i32;

        let mut col_names: Vec<u16> = vec![];
        let mut col_name_lengths: Vec<i32> = vec![];
        let mut col_ids: Vec<i32> = vec![];
        let mut col_physical_types: Vec<i64> = vec![];

        for (_i, f) in metadata.schema().fields().iter().enumerate() {
            let physical_type = match f {
                PrimitiveType(f) => Ok(f.physical_type),
                GroupType {
                    field_info: f,
                    logical_type: _,
                    converted_type: _,
                    fields: _,
                } => Err(anyhow!("group types not supported: {}", f.name)),
            }?;
            let physical_type = match physical_type {
                PhysicalType::Boolean => BOOLEAN as i64,
                PhysicalType::Int32 => INT32 as i64,
                PhysicalType::Int64 => INT64 as i64,
                PhysicalType::Int96 => INT96 as i64,
                PhysicalType::Float => FLOAT as i64,
                PhysicalType::Double => DOUBLE as i64,
                PhysicalType::ByteArray => BYTE_ARRAY as i64,
                PhysicalType::FixedLenByteArray(length) => {
                    // Should match Numbers#encodeLowHighInts().
                    (i64::overflowing_shl(length as i64, 32).0 | (FIXED_LEN_BYTE_ARRAY as i64))
                        as i64
                }
            };
            col_physical_types.push(physical_type);

            let info = f.get_field_info();
            let name: Vec<u16> = info.name.encode_utf16().collect();
            col_names.extend_from_slice(&name);
            col_name_lengths.push(name.len() as i32);

            col_ids.push(info.id.unwrap_or(-1));
        }

        // TODO: add some validation

        let decoder = ParquetDecoder {
            col_count,
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len() as i32,
            reader,
            metadata,
            decompress_buffer: vec![],
            col_names_ptr: col_names.as_ptr(),
            col_names,
            col_name_lengths_ptr: col_name_lengths.as_ptr(),
            col_name_lengths,
            col_ids_ptr: col_ids.as_ptr(),
            col_ids,
            col_physical_types_ptr: col_physical_types.as_ptr(),
            col_physical_types,
        };

        return Ok(decoder);
    };

    match init() {
        Ok(decoder) => Box::into_raw(Box::new(decoder)),
        Err(err) => throw_state_ex(
            &mut env,
            "create_parquet_decoder",
            err,
            std::ptr::null_mut(),
        ),
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
    decoder: *mut ParquetDecoder,
    row_group: usize,
    column: usize,
    column_type: i32,
    buffers: *mut ColumnChunkBuffers,
) {
    assert!(!decoder.is_null(), "decoder pointer is null");
    assert!(!buffers.is_null(), "buffers pointer is null");

    let (decoder, buffers) = unsafe { (&mut *decoder, &mut *buffers) };

    match decoder.decode_column_chunk(row_group, column, column_type, buffers) {
        Ok(_) => (),
        Err(err) => throw_state_ex(&mut env, "decode_column_chunk", err, ()),
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
