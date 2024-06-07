use std::fs::File;
use std::mem::{offset_of, size_of};
use std::ops::Sub;
use std::os::fd::FromRawFd;
use std::path::Path;
use std::slice;

use anyhow::anyhow;
use jni::objects::JClass;
use jni::JNIEnv;

use crate::parquet_write::schema::ColumnType;
use parquet2::encoding::Encoding;
use parquet2::metadata::{Descriptor, FileMetaData};
use parquet2::page::{split_buffer, DataPage, DictPage, Page};
use parquet2::read::{decompress, get_page_iterator, read_metadata};
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{IntegerType, PhysicalType, PrimitiveLogicalType, TimeUnit};

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
#[repr(C)]
pub struct ParquetDecoder {
    col_count: i32,
    row_count: usize,
    row_group_count: i32,
    columns_ptr: *const ColumnMeta,
    columns: Vec<ColumnMeta>,
    file: File,
    metadata: FileMetaData,
    decompress_buffer: Vec<u8>,
}

#[repr(C)]
#[derive(Debug)]
pub struct ColumnMeta {
    typ: ColumnType,
    id: i32,
    physical_type: i64,
    name_size: u32,
    name_ptr: *const u16,
    name_vec: Vec<u16>,
}

impl ParquetDecoder {
    pub fn read(mut reader: File) -> anyhow::Result<Self> {
        let metadata = read_metadata(&mut reader)?;

        let col_count = metadata.schema().fields().len() as i32;
        let mut columns = vec![];

        for (_i, f) in metadata.schema_descr.columns().iter().enumerate() {
            // Some types are not supported, this will skip them.
            to_column_type(&f.descriptor).map(|typ| {
                let physical_type = match f.descriptor.primitive_type.physical_type {
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

                let info = &f.descriptor.primitive_type.field_info;
                let name: Vec<u16> = info.name.encode_utf16().collect();

                columns.push(ColumnMeta {
                    typ,
                    id: info.id.unwrap_or(-1),
                    physical_type,
                    name_size: name.len() as u32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                });
            });
        }

        // TODO: add some validation

        let decoder = ParquetDecoder {
            col_count,
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len() as i32,
            file: reader,
            metadata,
            decompress_buffer: vec![],
            columns_ptr: columns.as_ptr(),
            columns,
        };

        return Ok(decoder);
    }

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
            get_page_iterator(column_metadata, &mut self.file, None, vec![], 1024 * 1024)?;

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
    raw_fd: i32,
) -> *mut ParquetDecoder {
    let init = || -> anyhow::Result<ParquetDecoder> {
        let file = unsafe { File::from_raw_fd(raw_fd) };
        ParquetDecoder::read(file)
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

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnCountOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, col_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowCountOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, row_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_rowGroupCountOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, row_group_count)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnsPtrOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ParquetDecoder, columns_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordTypeOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, typ)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordPhysicalTypeOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, physical_type)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNamePtrOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, name_ptr)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordNameSizeOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, name_size)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnIdsOffset(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    offset_of!(ColumnMeta, id)
}

#[no_mangle]
pub extern "system" fn Java_io_questdb_griffin_engine_table_parquet_PartitionDecoder_columnRecordSize(
    env: JNIEnv,
    _class: JClass,
) -> usize {
    size_of::<ColumnMeta>()
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

fn to_column_type(des: &Descriptor) -> Option<ColumnType> {
    match (
        des.primitive_type.physical_type,
        des.primitive_type.logical_type,
    ) {
        (
            PhysicalType::Int64,
            Some(Timestamp {
                unit: TimeUnit::Microseconds,
                is_adjusted_to_utc: true,
            }),
        ) => Some(ColumnType::Timestamp),
        (
            PhysicalType::Int64,
            Some(Timestamp {
                unit: TimeUnit::Milliseconds,
                is_adjusted_to_utc: true,
            }),
        ) => Some(ColumnType::Date),
        (PhysicalType::Int64, None) => Some(ColumnType::Long),
        (PhysicalType::Int64, Some(PrimitiveLogicalType::Integer(IntegerType::Int64))) => {
            Some(ColumnType::Long)
        }
        (PhysicalType::Int32, None) => Some(ColumnType::Int),
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32))) => {
            Some(ColumnType::Int)
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int16))) => {
            Some(ColumnType::Short)
        }
        (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int8))) => {
            Some(ColumnType::Byte)
        }
        (PhysicalType::Boolean, None) => Some(ColumnType::Boolean),
        (PhysicalType::Double, None) => Some(ColumnType::Double),
        (PhysicalType::Float, None) => Some(ColumnType::Float),
        (PhysicalType::FixedLenByteArray(16), Some(Uuid)) => Some(ColumnType::Uuid),
        (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String)) => Some(ColumnType::Varchar),
        (PhysicalType::FixedLenByteArray(32), None) => Some(ColumnType::Long256),
        (PhysicalType::ByteArray, None) => Some(ColumnType::Binary),
        (_, _) => None,
    }
}

#[cfg(test)]
mod tests {
    use crate::parquet_read::jni::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, ColumnType, Partition};
    use arrow::array::Array;
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;
    use std::{env, mem};
    use tempfile::NamedTempFile;

    #[test]
    fn test_decode_column_type_fixed() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 10;
        let mut columns = Vec::new();

        let cols = vec![
            (ColumnType::Long256, size_of::<i64>() * 4, "col_long256"),
            (ColumnType::Timestamp, size_of::<i64>(), "col_ts"),
            (ColumnType::Int, size_of::<i32>(), "col_int"),
            (ColumnType::Long, size_of::<i64>(), "col_long"),
            (ColumnType::Uuid, size_of::<i64>() * 2, "col_uuid"),
            (ColumnType::Boolean, size_of::<bool>(), "col_bool"),
            (ColumnType::Date, size_of::<i64>(), "col_date"),
            (ColumnType::Byte, size_of::<u8>(), "col_byte"),
            (ColumnType::Short, size_of::<i16>(), "col_short"),
            (ColumnType::Double, size_of::<f64>(), "col_double"),
            (ColumnType::Float, size_of::<f32>(), "col_float"),
            (ColumnType::GeoInt, size_of::<f32>(), "col_geo_int"),
            (ColumnType::GeoShort, size_of::<u16>(), "col_geo_short"),
            (ColumnType::GeoByte, size_of::<u8>(), "col_geo_byte"),
            (ColumnType::GeoLong, size_of::<i64>(), "col_geo_long"),
            (ColumnType::IPv4, size_of::<i32>(), "col_geo_ipv4"),
        ];

        for (col_type, value_size, name) in cols.iter() {
            columns.push(create_fix_column(row_count, *col_type, *value_size, name));
        }

        let column_count = columns.len();
        let partition = Partition { table: "test_table".to_string(), columns: columns };
        let parquet_writer = ParquetWriter::new(&mut buf)
            .with_statistics(false)
            .with_row_group_size(Some(1048576))
            .with_data_page_size(Some(1048576))
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
        temp_file
            .write_all(bytes.to_byte_slice())
            .expect("Failed to write to temp file");

        let path = temp_file.path().to_str().unwrap();
        let file = File::open(Path::new(path)).unwrap();
        let meta = ParquetDecoder::read(file).unwrap();

        assert_eq!(meta.columns.len(), column_count);
        assert_eq!(meta.row_count, row_count);

        for (i, col) in meta.columns.iter().enumerate() {
            let (col_type, _, name) = cols[i];
            assert_eq!(col.typ, to_storage_type(col_type));
            let actual_name: String = String::from_utf16(&col.name_vec).unwrap();
            assert_eq!(actual_name, name);
        }

        temp_file.close().expect("Failed to delete temp file");
    }

    fn to_storage_type(column_type: ColumnType) -> ColumnType {
        match column_type {
            ColumnType::GeoInt | ColumnType::IPv4 => ColumnType::Int,
            ColumnType::GeoShort => ColumnType::Short,
            ColumnType::GeoByte => ColumnType::Byte,
            ColumnType::GeoLong => ColumnType::Long,
            other => other,
        }
    }

    fn create_fix_column(
        row_count: usize,
        col_type: ColumnType,
        value_size: usize,
        name: &'static str,
    ) -> Column {
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..row_count {
            let value = i as u8;
            let offset = i * value_size;
            buff[offset..offset + 1].copy_from_slice(&value.to_le_bytes());
        }
        let col_type_i32 = col_type as i32;
        assert_eq!(
            col_type,
            ColumnType::try_from(col_type_i32).expect("invalid colum type")
        );

        Column::from_raw_data(
            0,
            name,
            col_type as i32,
            0,
            row_count,
            buff.as_ptr(),
            buff.len(),
            null(),
            0,
            null(),
            0,
        )
        .unwrap()
    }

    fn serialize_to_parquet(
        mut buf: &mut Cursor<Vec<u8>>,
        col1: Vec<i32>,
        col_chars: Vec<u8>,
        offsets: Vec<u64>,
    ) {
        assert_eq!(ColumnType::Symbol, ColumnType::try_from(12).expect("fail"));
        let col1_w = Column::from_raw_data(
            0,
            "col1",
            12,
            0,
            col1.len(),
            col1.as_ptr() as *const u8,
            col1.len() * size_of::<i32>(),
            col_chars.as_ptr(),
            col_chars.len(),
            offsets.as_ptr(),
            offsets.len(),
        )
        .unwrap();

        let partition = Partition {
            table: "test_table".to_string(),
            columns: [col1_w].to_vec(),
        };

        let parquet_writer = ParquetWriter::new(&mut buf)
            .with_statistics(false)
            .finish(partition)
            .expect("parquet writer");
    }

    fn serialize_as_symbols(symbol_chars: Vec<&str>) -> (Vec<u8>, Vec<u64>) {
        let mut chars = vec![];
        let mut offsets = vec![];

        for s in symbol_chars {
            let sym_chars: Vec<_> = s.encode_utf16().collect();
            let len = sym_chars.len();
            offsets.push(chars.len() as u64);
            chars.extend_from_slice(&(len as u32).to_le_bytes());
            let encoded: &[u8] = unsafe {
                std::slice::from_raw_parts(
                    sym_chars.as_ptr() as *const u8,
                    sym_chars.len() * mem::size_of::<u16>(),
                )
            };
            chars.extend_from_slice(encoded);
        }

        (chars, offsets)
    }
}
