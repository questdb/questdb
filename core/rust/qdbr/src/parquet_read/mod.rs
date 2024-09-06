use crate::parquet_write::schema::ColumnType;
use parquet2::metadata::FileMetaData;
use std::fs::File;
use std::mem::ManuallyDrop;

mod column_sink;
mod decode;
mod jni;
mod meta;
mod slicer;
mod symbols;

// The metadata fields are accessed from Java.
#[repr(C)]
pub struct ParquetDecoder {
    pub col_count: i32,
    pub row_count: usize,
    pub row_group_count: i32,
    pub row_group_sizes_ptr: *const i32,
    pub row_group_sizes: Vec<i32>,
    pub columns_ptr: *const ColumnMeta,
    pub columns: Vec<ColumnMeta>,
    file: ManuallyDrop<File>, // the fd is managed by Java code
    metadata: FileMetaData,
    decompress_buf: Vec<u8>,
}

#[repr(C)]
#[derive(Debug)]
pub struct ColumnMeta {
    pub typ: ColumnType,
    pub column_type: i32,
    pub id: i32,
    pub name_size: i32,
    pub name_ptr: *const u16,
    pub name_vec: Vec<u16>,
}

// The fields are accessed from Java.
#[repr(C)]
pub struct RowGroupBuffers {
    pub column_bufs_ptr: *const ColumnChunkBuffers,
    pub column_bufs: Vec<ColumnChunkBuffers>,
}

// The metadata fields are accessed from Java.
#[repr(C)]
pub struct ColumnChunkBuffers {
    pub data_size: usize,
    pub data_ptr: *mut u8,
    pub data_vec: Vec<u8>,
    pub aux_size: usize,
    pub aux_ptr: *mut u8,
    pub aux_vec: Vec<u8>,
}
