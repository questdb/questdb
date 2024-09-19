use crate::parquet::col_type::ColumnType;
use crate::parquet::qdb_metadata::QdbMeta;
use parquet2::metadata::FileMetaData;
use std::io::{Read, Seek};

mod column_sink;
mod decode;
mod io;
mod jni;
mod meta;
mod slicer;

// The metadata fields are accessed from Java.
#[repr(C)]
pub struct ParquetDecoder<R>
where
    R: Read + Seek,
{
    pub col_count: u32,
    pub row_count: usize,
    pub row_group_count: u32,
    pub row_group_sizes_ptr: *const i32,
    pub row_group_sizes: Vec<i32>,
    pub columns_ptr: *const ColumnMeta,
    pub columns: Vec<ColumnMeta>,
    reader: R,
    metadata: FileMetaData,
    qdb_meta: Option<QdbMeta>,
    decompress_buf: Vec<u8>,
}

#[repr(C)]
#[derive(Debug)]
pub struct ColumnMeta {
    pub column_type: ColumnType,
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

/// QuestDB-format Column Data
///
/// The memory is owned by the Rust code, read by Java.
/// The `(data|aux)_vec` are vectors since these are grown dynamically.
/// After each growth the `_size` and `_ptr` fields are updated
/// and then accessed from Java via base pointer + field offset via `Unsafe`.
#[repr(C)]
pub struct ColumnChunkBuffers {
    pub data_size: usize,
    pub data_ptr: *mut u8,
    pub data_vec: Vec<u8>,

    pub aux_size: usize,
    pub aux_ptr: *mut u8,
    pub aux_vec: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::parquet::col_type::ColumnTypeTag;
    use crate::parquet::error::ParquetResult;
    use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol};
    use crate::parquet_read::{ParquetDecoder, RowGroupBuffers};
    use parquet::basic::{ConvertedType, LogicalType, Type as PhysicalType};
    use parquet::data_type::{ByteArray, ByteArrayType};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn fn_load_symbol_without_local_is_global_handling_meta() -> ParquetResult<()> {
        let mut qdb_meta = QdbMeta::new();
        qdb_meta.schema.columns.insert(
            0,
            QdbMetaCol {
                column_type: ColumnTypeTag::Symbol.into_type(),
                handling: None, // It should error because this is missing.
            },
        );

        let buf = gen_test_symbol_parquet(Some(qdb_meta.serialize()?))?;

        let reader = Cursor::new(buf);
        let mut parquet_decoder = ParquetDecoder::read(reader)?;
        let mut rgb = RowGroupBuffers::new();
        let res = parquet_decoder.decode_row_group(
            &mut rgb,
            &[Some(ColumnTypeTag::Symbol.into_type())],
            0,
        );
        let err = res.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("could not decode page for column \"sym\" in row group 0"));
        assert!(msg.contains("only special LocalKeyIsGlobal-encoded symbol columns are supported"));

        Ok(())
    }

    fn gen_test_symbol_parquet(qdb_metadata: Option<String>) -> ParquetResult<Vec<u8>> {
        let symbol_col_data: Vec<ByteArray> = vec![
            ByteArray::from("abc"),
            ByteArray::from("defg"),
            ByteArray::from("hijkl"),
            ByteArray::from("mn"),
            ByteArray::from(""),
            ByteArray::from("o"),
        ];

        let def_levels = vec![1, 1, 0, 0, 1, 1, 0, 1, 0, 1];

        let col_type = Arc::new(
            Type::primitive_type_builder("sym", PhysicalType::BYTE_ARRAY)
                // We need parquet fields to have an assigned ID
                // This gets matched up against the QuestDB column type in the JSON metadata.
                .with_id(Some(0))
                .with_converted_type(ConvertedType::UTF8)
                .with_logical_type(Some(LogicalType::String))
                .build()?,
        );

        let schema_type = Arc::new(
            Type::group_type_builder("schema")
                .with_fields(vec![col_type])
                .build()?,
        );

        let kv_metadata = qdb_metadata
            .map(|qdb_meta_string| vec![KeyValue::new("questdb".to_string(), qdb_meta_string)]);

        let props = Arc::new(
            WriterProperties::builder()
                .set_dictionary_enabled(true)
                .set_writer_version(WriterVersion::PARQUET_1_0)
                .set_key_value_metadata(kv_metadata)
                .build(),
        );

        let mut cursor = Cursor::new(Vec::new());
        let mut file_writer = SerializedFileWriter::new(&mut cursor, schema_type, props)?;

        let mut row_group_writer = file_writer.next_row_group()?;
        if let Some(mut col_writer) = row_group_writer.next_column()? {
            let typed_writer = col_writer.typed::<ByteArrayType>();
            typed_writer.write_batch(&symbol_col_data, Some(&def_levels), None)?;
            col_writer.close()?;
        }
        row_group_writer.close()?;
        file_writer.close()?;

        Ok(cursor.into_inner())
    }
}
