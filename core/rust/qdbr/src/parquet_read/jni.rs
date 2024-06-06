use std::fs::File;
use std::ops::Sub;
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

const MAX_ALLOC_SIZE: usize = i32::MAX as usize;

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
        let columns = self.metadata.row_groups[row_group].columns();
        let column_metadata = &columns[column];

        // TODO: avoid hardcoding page size
        let page_reader =
            get_page_iterator(column_metadata, &mut self.reader, None, vec![], 1024 * 1024)?;

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
                    decode(&mut page, dict.as_ref(), buffers)?;
                }
            };
        }

        return Ok(());
    }
}

fn decode(
    page: &DataPage,
    dict: Option<&DictPage>,
    buffers: &mut ColumnChunkBuffers,
) -> anyhow::Result<()> {
    let (_rep_levels, _def_levels, values_buffer) = split_buffer(page)?;

    match (
        page.descriptor.primitive_type.physical_type,
        page.encoding(),
        dict,
    ) {
        (PhysicalType::Int32, Encoding::Plain, None) => {
            let prev_pos = buffers.book_data(values_buffer.len())?;
            unsafe {
                libc::memcpy(
                    buffers.data_ptr.add(prev_pos) as *mut libc::c_void,
                    values_buffer.as_ptr() as *mut libc::c_void,
                    values_buffer.len(),
                )
            };
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
    data_pos: usize,
    aux_ptr: *mut u8,
    aux_size: usize,
    aux_pos: usize,
}

impl ColumnChunkBuffers {
    pub fn avail_aux(&self) -> usize {
        if self.aux_ptr.is_null() {
            0
        } else {
            self.aux_size - self.aux_pos
        }
    }

    pub fn avail_data(&self) -> usize {
        if self.data_ptr.is_null() {
            0
        } else {
            self.data_size - self.data_pos
        }
    }

    // returns old aux_pos value
    pub fn book_aux(&mut self, wanted: usize) -> anyhow::Result<usize> {
        let prev_aux_pos = self.aux_pos;
        let avail = self.avail_aux();
        if avail >= wanted {
            self.aux_pos += wanted;
            return Ok(prev_aux_pos);
        }
        let needed = wanted - avail;

        let new_size = (self.aux_size + needed)
            .next_power_of_two()
            .min(MAX_ALLOC_SIZE);
        if (new_size == MAX_ALLOC_SIZE) && (new_size < (self.aux_size + needed)) {
            return Err(anyhow!("aux buffer overflowed 2GiB limit"));
        }

        let new_ptr = if self.aux_ptr.is_null() {
            unsafe { libc::malloc(new_size) }
        } else {
            unsafe { libc::realloc(self.aux_ptr as *mut libc::c_void, new_size) }
        } as *mut u8;
        if new_ptr.is_null() {
            return Err(anyhow!("aux alloc failed (out of memory)"));
        }

        self.aux_ptr = new_ptr;
        self.aux_size = new_size;
        self.aux_pos += wanted;
        Ok(prev_aux_pos)
    }

    // returns old data_pos value
    pub fn book_data(&mut self, wanted: usize) -> anyhow::Result<usize> {
        let prev_data_pos = self.data_pos;
        let avail = self.avail_data();
        if avail >= wanted {
            self.data_pos += wanted;
            return Ok(prev_data_pos);
        }
        let needed = wanted - avail;

        let new_size = (self.data_size + needed)
            .next_power_of_two()
            .min(MAX_ALLOC_SIZE);
        if (new_size == MAX_ALLOC_SIZE) && (new_size < (self.data_size + needed)) {
            return Err(anyhow!("data buffer overflowed 2GiB limit"));
        }

        let new_ptr = if self.data_ptr.is_null() {
            unsafe { libc::malloc(new_size) }
        } else {
            unsafe { libc::realloc(self.data_ptr as *mut libc::c_void, new_size) }
        } as *mut u8;
        if new_ptr.is_null() {
            return Err(anyhow!("data alloc failed (out of memory)"));
        }

        self.data_ptr = new_ptr;
        self.data_size = new_size;
        self.data_pos += wanted;
        Ok(prev_data_pos)
    }
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
