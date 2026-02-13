#![allow(dead_code)]

use std::{
    io::Cursor,
    sync::{atomic::AtomicUsize, Arc},
};

use parquet::{
    basic::{Encoding as ParquetEncoding, LogicalType, Repetition, Type as PhysicalType},
    data_type::{ByteArrayType, DataType},
    file::{
        properties::{WriterProperties, WriterVersion},
        writer::SerializedFileWriter,
    },
    format::KeyValue,
    schema::types::Type,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::{
    allocator::{MemTracking, QdbAllocator},
    parquet_read::{DecodeContext, ParquetDecoder, RowGroupBuffers},
};

pub const COUNT: usize = 4096;

pub const VERSIONS: [WriterVersion; 2] = [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0];

#[derive(Debug, Clone, Copy)]
pub enum Encoding {
    Plain,
    RleDictionary,
    DeltaBinaryPacked,
    DeltaLengthByteArray,
    DeltaByteArray,
}

#[derive(Debug, Clone, Copy)]
pub enum Null {
    None,
    Sparse,
    Dense,
}

pub const ALL_NULLS: [Null; 3] = [Null::None, Null::Dense, Null::Sparse];

pub fn generate_nulls(count: usize, null: Null) -> Vec<bool> {
    match null {
        Null::Dense => (0..count).map(|i| i % 2 == 0).collect(),
        Null::None => vec![false; count],
        Null::Sparse => (0..count).map(|i| i % 10 == 0).collect(),
    }
}

pub fn def_levels_from_nulls(nulls: &[bool]) -> Vec<i16> {
    nulls
        .iter()
        .map(|&is_null| if is_null { 0 } else { 1 })
        .collect()
}

pub fn non_null_only<T: Clone>(all: &[T], nulls: &[bool]) -> Vec<T> {
    all.iter()
        .zip(nulls)
        .filter(|(_, &is_null)| !is_null)
        .map(|(v, _)| v.clone())
        .collect()
}

struct TestAlloc {
    _mem_tracking: Box<MemTracking>,
    _tagged_used: Box<AtomicUsize>,
    allocator: QdbAllocator,
}

impl TestAlloc {
    fn new() -> Self {
        let mem_tracking = Box::new(MemTracking::new());
        let tagged_used = Box::new(AtomicUsize::new(0));
        let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);
        Self {
            _mem_tracking: mem_tracking,
            _tagged_used: tagged_used,
            allocator,
        }
    }

    fn allocator(&self) -> QdbAllocator {
        self.allocator.clone()
    }
}

/// Write a single-column Parquet file with the given typed data and return the bytes.
pub fn write_parquet_column<T: DataType>(
    col_name: &str,
    schema_type: Type,
    data: &[T::T],
    def_levels: Option<&[i16]>,
    props: Arc<WriterProperties>,
) -> Vec<u8> {
    let mut cursor = Cursor::new(Vec::new());
    let mut file_writer = SerializedFileWriter::new(&mut cursor, Arc::new(schema_type), props)
        .expect("create file writer");

    let mut row_group_writer = file_writer.next_row_group().expect("next row group");
    if let Some(mut col_writer) = row_group_writer.next_column().expect("next column") {
        let typed = col_writer.typed::<T>();
        typed
            .write_batch(data, def_levels, None)
            .unwrap_or_else(|e| panic!("write_batch for {col_name}: {e}"));
        col_writer.close().expect("close column writer");
    }
    row_group_writer.close().expect("close row group writer");
    file_writer.close().expect("close file writer");

    cursor.into_inner()
}

/// Decode a single column from Parquet bytes using QuestDB's decoder.
/// Returns (data_vec bytes, aux_vec bytes).
pub fn decode_file(buf: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let ta = TestAlloc::new();
    let allocator = ta.allocator();

    let buf_len = buf.len() as u64;
    let mut reader = Cursor::new(buf);
    let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)
        .expect("ParquetDecoder::read");

    assert_eq!(decoder.col_count, 1, "expected single column");
    let col_type = decoder.columns[0]
        .column_type
        .expect("column type should be recognized");

    let row_group_count = decoder.row_group_count;

    let mut rgb = RowGroupBuffers::new(allocator);
    let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);

    let columns = vec![(0i32, col_type)];

    let mut all_data = Vec::new();
    let mut all_aux = Vec::new();

    for rg_idx in 0..row_group_count {
        let rg_size = decoder.row_group_sizes[rg_idx as usize] as u32;
        decoder
            .decode_row_group(&mut ctx, &mut rgb, &columns, rg_idx, 0, rg_size)
            .unwrap_or_else(|e| panic!("decode row group {rg_idx}: {e}"));

        let bufs = &rgb.column_buffers()[0];
        all_data.extend_from_slice(&bufs.data_vec);
        all_aux.extend_from_slice(&bufs.aux_vec);
    }

    (all_data, all_aux)
}

/// Build QDB metadata JSON for a column type tag.
pub fn qdb_meta(tag: ColumnTypeTag) -> String {
    format!(
        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0}}]}}"#,
        tag as u8
    )
}

/// Build QDB metadata JSON for a column type with format field (e.g. Symbol).
pub fn qdb_meta_with_format(col_type: ColumnType, format: u8) -> String {
    format!(
        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0,"format":{}}}]}}"#,
        col_type.code(),
        format
    )
}

/// Build writer properties with QDB metadata for a full column type and encoding.
pub fn qdb_props_col_type(col_type: ColumnType, version: WriterVersion, encoding: Encoding) -> WriterProperties {
    let qdb_json = format!(
        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0}}]}}"#,
        col_type.code()
    );
    qdb_props_with_json(qdb_json, version, encoding)
}

/// Build writer properties with QDB metadata for a column type tag and encoding.
pub fn qdb_props(tag: ColumnTypeTag, version: WriterVersion, encoding: Encoding) -> WriterProperties {
    let qdb_json = qdb_meta(tag);
    qdb_props_with_json(qdb_json, version, encoding)
}

fn qdb_props_with_json(qdb_json: String, version: WriterVersion, encoding: Encoding) -> WriterProperties {
    let props = WriterProperties::builder()
        .set_writer_version(version)
        .set_key_value_metadata(Some(vec![KeyValue::new("questdb".to_string(), qdb_json)]));
    match encoding {
        Encoding::Plain => props
            .set_dictionary_enabled(false)
            .set_encoding(ParquetEncoding::PLAIN),
        Encoding::DeltaBinaryPacked => props
            .set_dictionary_enabled(false)
            .set_encoding(ParquetEncoding::DELTA_BINARY_PACKED),
        Encoding::DeltaLengthByteArray => props
            .set_dictionary_enabled(false)
            .set_encoding(ParquetEncoding::DELTA_LENGTH_BYTE_ARRAY),
        Encoding::DeltaByteArray => props
            .set_dictionary_enabled(false)
            .set_encoding(ParquetEncoding::DELTA_BYTE_ARRAY),
        Encoding::RleDictionary => props.set_dictionary_enabled(true),
    }
    .build()
}

/// Build an optional ByteArray schema with an optional logical type.
pub fn optional_byte_array_schema(col_name: &str, logical: Option<LogicalType>) -> Type {
    let mut builder = parquet::schema::types::Type::primitive_type_builder(col_name, PhysicalType::BYTE_ARRAY);
    if let Some(lt) = logical {
        builder = builder.with_logical_type(Some(lt));
    }
    Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(builder.build().unwrap())])
        .build()
        .unwrap()
}

/// Build a required ByteArray schema with an optional logical type.
pub fn required_byte_array_schema(col_name: &str, logical: Option<LogicalType>) -> Type {
    let mut builder = parquet::schema::types::Type::primitive_type_builder(col_name, PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED);
    if let Some(lt) = logical {
        builder = builder.with_logical_type(Some(lt));
    }
    Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(builder.build().unwrap())])
        .build()
        .unwrap()
}

/// Encode + decode a ByteArray column with the given values, nulls, schema, and props.
pub fn encode_decode_byte_array(
    values: &[<ByteArrayType as DataType>::T],
    nulls: &[bool],
    schema: Type,
    props: WriterProperties,
) -> (Vec<u8>, Vec<u8>) {
    let non_null_values = non_null_only(values, nulls);
    let def_levels = def_levels_from_nulls(nulls);
    let buf = write_parquet_column::<ByteArrayType>(
        "col",
        schema,
        &non_null_values,
        Some(&def_levels),
        Arc::new(props),
    );
    decode_file(&buf)
}
