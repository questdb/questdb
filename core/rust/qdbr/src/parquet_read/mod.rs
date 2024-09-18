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
    use crate::parquet::error::ParquetResult;
    use crate::parquet_read::{ParquetDecoder, RowGroupBuffers};
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use std::io::Cursor;
    use std::sync::Arc;
    use crate::parquet::col_type::ColumnTypeTag;

    #[test]
    fn fn_load_symbol_without_local_is_global_handling_meta() -> ParquetResult<()> {
        let buf = gen_test_symbol_parquet()?;

        eprintln!("buf: {:?}", buf);
        let reader = Cursor::new(buf);
        let mut parquet_decoder = ParquetDecoder::read(reader)?;
        let mut rgb = RowGroupBuffers::new();
        parquet_decoder.decode_row_group(
            &mut rgb,
            &[Some(ColumnTypeTag::Symbol.into_type())],
            0
        )?;

        Ok(())
    }

    fn gen_test_symbol_parquet() -> ParquetResult<Vec<u8>> {
        let symbol_col_data = StringArray::from(vec![
            Some("abc"),
            None,
            Some("defg"),
            Some("hijkl"),
            None,
            None,
            Some("mn"),
            Some(""),
            Some("o"),
        ]);

        let schema = Schema::new(vec![Field::new(
            "sym",
            arrow::datatypes::DataType::Utf8,
            true,
        )]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(symbol_col_data)])?;

        let props = WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .build();

        let mut buf: Vec<u8> = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;
        Ok(buf)
    }
    // #[test]
    // fn test_write_parquet_with_symbol_column() {
    //     let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
    //     let col1 = vec![0, 1, i32::MIN, 2, 4];
    //     let (col_chars, offsets) =
    //         crate::parquet_write::tests::serialize_as_symbols(vec!["foo", "bar", "baz", "notused", "plus"]);
    //
    //     crate::parquet_write::tests::serialize_to_parquet(&mut buf, col1, col_chars, offsets);
    //     let expected = vec![Some("foo"), Some("bar"), None, Some("baz"), Some("plus")];
    //
    //     buf.set_position(0);
    //     let bytes: Bytes = buf.into_inner().into();
    //     let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
    //         .expect("reader")
    //         .with_batch_size(8192)
    //         .build()
    //         .expect("builder");
    //
    //     for batch in parquet_reader.flatten() {
    //         let symbol_array = batch
    //             .column(0)
    //             .as_any()
    //             .downcast_ref::<arrow::array::StringArray>()
    //             .expect("Failed to downcast");
    //         let collected: Vec<_> = symbol_array.iter().collect();
    //         assert_eq!(collected, expected);
    //     }
    //
    //     crate::parquet_write::tests::save_to_file(bytes);
    // }
}
