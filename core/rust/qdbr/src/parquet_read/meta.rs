use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::col_type::{ColumnType, ColumnTypeTag};
use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_read::{ColumnMeta, ParquetDecoder};
use parquet2::metadata::{Descriptor, FileMetaData};
use parquet2::read::read_metadata_with_size;
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{
    IntegerType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use std::io::{Read, Seek};

/// Extract the questdb-specific metadata from the parquet file metadata.
/// Error if the JSON is not valid or the version is not supported.
/// Returns `None` if the metadata is not present.
fn extract_qdb_meta(file_metadata: &FileMetaData) -> ParquetResult<Option<QdbMeta>> {
    let Some(key_value_meta) = file_metadata.key_value_metadata.as_ref() else {
        return Ok(None);
    };
    let Some(questdb_key_value) = key_value_meta.iter().find(|kv| kv.key == QDB_META_KEY) else {
        return Ok(None);
    };
    let Some(json) = questdb_key_value.value.as_deref() else {
        return Ok(None);
    };
    let qdb_meta = QdbMeta::deserialize(json)?;
    Ok(Some(qdb_meta))
}

impl<R: Read + Seek> ParquetDecoder<R> {
    pub fn read(allocator: QdbAllocator, mut reader: R, file_size: u64) -> ParquetResult<Self> {
        let metadata = read_metadata_with_size(&mut reader, file_size)?;
        let col_len = metadata.schema_descr.columns().len();
        let qdb_meta = extract_qdb_meta(&metadata)?;
        let mut row_group_sizes: AcVec<i32> =
            AcVec::with_capacity_in(metadata.row_groups.len(), allocator.clone())?;
        let mut row_group_sizes_acc: AcVec<usize> =
            AcVec::with_capacity_in(metadata.row_groups.len(), allocator.clone())?;
        let mut columns = AcVec::with_capacity_in(col_len, allocator.clone())?;

        let mut accumulated_size = 0;
        for row_group in metadata.row_groups.iter() {
            row_group_sizes_acc.push(accumulated_size)?;
            let row_group_size = row_group.num_rows();
            row_group_sizes.push(row_group_size as i32)?;
            accumulated_size += row_group_size;
        }

        assert_eq!(accumulated_size, metadata.num_rows);

        for (column_id, f) in metadata.schema_descr.columns().iter().enumerate() {
            // Some types are not supported, this will skip them.
            if let Some(column_type) =
                Self::descriptor_to_column_type(&f.descriptor, column_id, qdb_meta.as_ref())
            {
                let name_str = &f.descriptor.primitive_type.field_info.name;
                let mut name = AcVec::with_capacity_in(name_str.len() * 2, allocator.clone())?;
                name.extend(name_str.encode_utf16())?;

                columns.push(ColumnMeta {
                    column_type,
                    id: column_id as i32,
                    name_size: name.len() as i32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                })?;
            }
        }

        // TODO: add some validation
        Ok(Self {
            allocator,
            col_count: columns.len() as u32,
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len() as u32,
            row_group_sizes_ptr: row_group_sizes.as_ptr(),
            row_group_sizes,
            reader,
            metadata,
            qdb_meta,
            decompress_buffer: vec![],
            columns_ptr: columns.as_ptr(),
            columns,
            row_group_sizes_acc,
        })
    }

    fn extract_column_type_from_qdb_meta(
        qdb_meta: Option<&QdbMeta>,
        column_id: usize,
    ) -> Option<ColumnType> {
        let col_meta = qdb_meta?.schema.get(column_id)?;
        Some(col_meta.column_type)
    }

    fn descriptor_to_column_type(
        des: &Descriptor,
        column_id: usize,
        qdb_meta: Option<&QdbMeta>,
    ) -> Option<ColumnType> {
        if let Some(col_type) = Self::extract_column_type_from_qdb_meta(qdb_meta, column_id) {
            return Some(col_type);
        }

        let column_type_tag = match (
            des.primitive_type.physical_type,
            des.primitive_type.logical_type,
            des.primitive_type.converted_type,
        ) {
            (
                PhysicalType::Int64,
                Some(Timestamp {
                    unit: TimeUnit::Microseconds,
                    is_adjusted_to_utc: _,
                })
                | Some(Timestamp { unit: TimeUnit::Nanoseconds, is_adjusted_to_utc: _ }),
                _,
            ) => Some(ColumnTypeTag::Timestamp),
            (
                PhysicalType::Int64,
                Some(Timestamp {
                    unit: TimeUnit::Milliseconds,
                    is_adjusted_to_utc: _,
                }),
                _,
            ) => Some(ColumnTypeTag::Date),
            (PhysicalType::Int64, None, _) => Some(ColumnTypeTag::Long),
            (PhysicalType::Int64, Some(PrimitiveLogicalType::Integer(IntegerType::Int64)), _) => {
                Some(ColumnTypeTag::Long)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32)), _) => {
                Some(ColumnTypeTag::Int)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Decimal(_, _)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Decimal(_, _))) => {
                Some(ColumnTypeTag::Double)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int16)), _) => {
                Some(ColumnTypeTag::Short)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::UInt16)), _) => {
                Some(ColumnTypeTag::Int)
            }
            (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int16)) => {
                Some(ColumnTypeTag::Short)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int8)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int8)) => {
                Some(ColumnTypeTag::Byte)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Date), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Date)) => {
                Some(ColumnTypeTag::Date)
            }
            (PhysicalType::Int32, None, _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int32)) => {
                Some(ColumnTypeTag::Int)
            }
            (PhysicalType::Boolean, None, _) => Some(ColumnTypeTag::Boolean),
            (PhysicalType::Double, None, _) => Some(ColumnTypeTag::Double),
            (PhysicalType::Float, None, _) => Some(ColumnTypeTag::Float),
            (PhysicalType::FixedLenByteArray(16), Some(Uuid), _) => Some(ColumnTypeTag::Uuid),
            (PhysicalType::FixedLenByteArray(16), None, None) => Some(ColumnTypeTag::Long128),
            (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _) => {
                Some(ColumnTypeTag::Varchar)
            }
            (PhysicalType::FixedLenByteArray(32), None, _) => Some(ColumnTypeTag::Long256),
            (PhysicalType::ByteArray, None, Some(PrimitiveConvertedType::Utf8)) => {
                Some(ColumnTypeTag::Varchar)
            }
            (PhysicalType::ByteArray, None, _) => Some(ColumnTypeTag::Binary),
            (PhysicalType::Int96, None, None) => Some(ColumnTypeTag::Timestamp),
            (_, _, _) => None,
        };
        column_type_tag.map(|tag| ColumnType::new(tag, 0))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;

    use crate::allocator::TestAllocatorState;
    use crate::parquet::col_type::{ColumnType, ColumnTypeTag};
    use crate::parquet_read::meta::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use parquet::file::reader::Length;
    use tempfile::NamedTempFile;

    #[test]
    fn test_decode_column_type_fixed() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 10;
        let mut buffers_columns = Vec::new();
        let mut columns = Vec::new();

        let cols: Vec<_> = ([
            (ColumnTypeTag::Long128, size_of::<i64>() * 2, "col_long128"),
            (ColumnTypeTag::Long256, size_of::<i64>() * 4, "col_long256"),
            (ColumnTypeTag::Timestamp, size_of::<i64>(), "col_ts"),
            (ColumnTypeTag::Int, size_of::<i32>(), "col_int"),
            (ColumnTypeTag::Long, size_of::<i64>(), "col_long"),
            (ColumnTypeTag::Uuid, size_of::<i64>() * 2, "col_uuid"),
            (ColumnTypeTag::Boolean, size_of::<bool>(), "col_bool"),
            (ColumnTypeTag::Date, size_of::<i64>(), "col_date"),
            (ColumnTypeTag::Byte, size_of::<u8>(), "col_byte"),
            (ColumnTypeTag::Short, size_of::<i16>(), "col_short"),
            (ColumnTypeTag::Double, size_of::<f64>(), "col_double"),
            (ColumnTypeTag::Float, size_of::<f32>(), "col_float"),
            (ColumnTypeTag::GeoInt, size_of::<f32>(), "col_geo_int"),
            (ColumnTypeTag::GeoShort, size_of::<u16>(), "col_geo_short"),
            (ColumnTypeTag::GeoByte, size_of::<u8>(), "col_geo_byte"),
            (ColumnTypeTag::GeoLong, size_of::<i64>(), "col_geo_long"),
            (ColumnTypeTag::IPv4, size_of::<i32>(), "col_geo_ipv4"),
            (ColumnTypeTag::Char, size_of::<u16>(), "col_char"),
        ])
        .iter()
        .map(|(tag, value_size, name)| (ColumnType::new(*tag, 0), *value_size, *name))
        .collect();

        for (col_id, (col_type, value_size, name)) in cols.iter().enumerate() {
            let (buff, column) =
                create_fix_column(col_id as i32, row_count, *col_type, *value_size, name);
            columns.push(column);
            buffers_columns.push(buff);
        }

        let column_count = columns.len();
        let partition = Partition { table: "test_table".to_string(), columns };
        ParquetWriter::new(&mut buf)
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
        let file_len = file.len();
        let meta = ParquetDecoder::read(allocator, file, file_len).unwrap();

        assert_eq!(meta.columns.len(), column_count);
        assert_eq!(meta.row_count, row_count);

        for (i, col) in meta.columns.iter().enumerate() {
            let (col_type, _, name) = cols[i];
            assert_eq!(col.column_type, col_type);
            let actual_name: String = String::from_utf16(&col.name_vec).unwrap();
            assert_eq!(actual_name, name);
        }

        temp_file.close().expect("Failed to delete temp file");

        // make sure buffer live until the end of the test
        assert_eq!(buffers_columns.len(), column_count);
    }

    fn create_fix_column(
        id: i32,
        row_count: usize,
        col_type: ColumnType,
        value_size: usize,
        name: &'static str,
    ) -> (Vec<u8>, Column) {
        let mut buff = vec![0u8; row_count * value_size];
        for i in 0..row_count {
            let value = i as u8;
            let offset = i * value_size;
            buff[offset..offset + 1].copy_from_slice(&value.to_le_bytes());
        }
        let col_type_i32 = col_type.code();
        assert_eq!(
            col_type,
            ColumnType::try_from(col_type_i32).expect("invalid colum type")
        );

        let ptr = buff.as_ptr();
        let data_size = buff.len();
        (
            buff,
            Column::from_raw_data(
                id,
                name,
                col_type.code(),
                0,
                row_count,
                ptr,
                data_size,
                null(),
                0,
                null(),
                0,
            )
            .unwrap(),
        )
    }
}
