use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_read::{ColumnMeta, ParquetDecoder};
use parquet2::metadata::{ColumnDescriptor, FileMetaData};
use parquet2::read::read_metadata_with_size;
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{GroupConvertedType, GroupLogicalType, ParquetType};
use parquet2::schema::types::{
    IntegerType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;
use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
use std::io::{Read, Seek};

const QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG: i32 = 1 << 10;

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

        for (index, column) in metadata.schema_descr.columns().iter().enumerate() {
            // Arrays have column name and id stored in the base group type.
            // Primitive type fields have the same primitive type as the base type.
            // That's why we're using the base type.

            let base_field = column.base_type.get_field_info();
            let name_str = &base_field.name;
            let mut name = AcVec::with_capacity_in(name_str.len() * 2, allocator.clone())?;
            name.extend(name_str.encode_utf16())?;

            if let Some(column_type) =
                Self::descriptor_to_column_type(column, index, qdb_meta.as_ref())
            {
                columns.push(ColumnMeta {
                    column_type,
                    id: base_field.id.unwrap_or(-1_i32),
                    name_size: name.len() as i32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                })?;
            } else {
                // The type is not supported, Java code will have to skip it.
                columns.push(ColumnMeta {
                    column_type: ColumnType::new(ColumnTypeTag::Undefined, 0),
                    id: -1,
                    name_size: name.len() as i32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                })?;
            }
        }

        // TODO(eugenels): add some validation
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
        column_index: usize,
    ) -> Option<ColumnType> {
        let col_meta = qdb_meta?.schema.get(column_index)?;
        Some(col_meta.column_type)
    }

    fn descriptor_to_column_type(
        column: &ColumnDescriptor,
        column_index: usize,
        qdb_meta: Option<&QdbMeta>,
    ) -> Option<ColumnType> {
        if let Some(col_type) = Self::extract_column_type_from_qdb_meta(qdb_meta, column_index) {
            return Some(col_type);
        }

        match (
            column.descriptor.primitive_type.physical_type,
            column.descriptor.primitive_type.logical_type,
            column.descriptor.primitive_type.converted_type,
        ) {
            (
                PhysicalType::Int64,
                Some(Timestamp {
                    unit: TimeUnit::Microseconds,
                    is_adjusted_to_utc: _,
                }),
                _,
            ) => Some(ColumnType::new(ColumnTypeTag::Timestamp, 0)),
            (
                PhysicalType::Int64,
                Some(Timestamp { unit: TimeUnit::Nanoseconds, is_adjusted_to_utc: _ }),
                _,
            ) => Some(ColumnType::new(
                ColumnTypeTag::Timestamp,
                QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
            )),
            (
                PhysicalType::Int64,
                Some(Timestamp {
                    unit: TimeUnit::Milliseconds,
                    is_adjusted_to_utc: _,
                }),
                _,
            ) => Some(ColumnType::new(ColumnTypeTag::Date, 0)),
            (PhysicalType::Int64, _, _) => Some(ColumnType::new(ColumnTypeTag::Long, 0)),
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32)), _) => {
                Some(ColumnType::new(ColumnTypeTag::Int, 0))
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Decimal(_, _)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Decimal(_, _))) => {
                Some(ColumnType::new(ColumnTypeTag::Double, 0))
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int16)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int16)) => {
                Some(ColumnType::new(ColumnTypeTag::Short, 0))
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int8)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int8)) => {
                Some(ColumnType::new(ColumnTypeTag::Byte, 0))
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Date), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Date)) => {
                Some(ColumnType::new(ColumnTypeTag::Date, 0))
            }
            (PhysicalType::Int32, _, _) => Some(ColumnType::new(ColumnTypeTag::Int, 0)),
            (PhysicalType::Boolean, _, _) => Some(ColumnType::new(ColumnTypeTag::Boolean, 0)),
            (PhysicalType::Double, _, _) => match array_column_type(&column.base_type) {
                Some(array_type) => Some(array_type),
                None => Some(ColumnType::new(ColumnTypeTag::Double, 0)),
            },
            (PhysicalType::Float, _, _) => Some(ColumnType::new(ColumnTypeTag::Float, 0)),
            (PhysicalType::FixedLenByteArray(16), Some(Uuid), _) => {
                Some(ColumnType::new(ColumnTypeTag::Uuid, 0))
            }
            (PhysicalType::FixedLenByteArray(16), _, _) => {
                Some(ColumnType::new(ColumnTypeTag::Long128, 0))
            }
            (PhysicalType::FixedLenByteArray(32), _, _) => {
                Some(ColumnType::new(ColumnTypeTag::Long256, 0))
            }
            (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _)
            | (PhysicalType::ByteArray, _, Some(PrimitiveConvertedType::Utf8)) => {
                Some(ColumnType::new(ColumnTypeTag::Varchar, 0))
            }
            (PhysicalType::ByteArray, _, _) => Some(ColumnType::new(ColumnTypeTag::Binary, 0)),
            (PhysicalType::Int96, _, None) => Some(ColumnType::new(
                ColumnTypeTag::Timestamp,
                QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
            )),
            (_, _, _) => None,
        }
    }
}

// The expected layout is described here:
// https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
// Yet, some software derives from the above layout, so the actual check can't be strict.
// Known to work with DuckDB's list of doubles.
fn array_column_type(base_type: &ParquetType) -> Option<ColumnType> {
    let mut cur_type;
    // First check the root field.
    match base_type {
        ParquetType::GroupType {
            field_info: _,
            logical_type,
            converted_type,
            fields,
        } => {
            let is_list = *converted_type == Some(GroupConvertedType::List)
                || *logical_type == Some(GroupLogicalType::List);
            if !is_list || fields.len() != 1 {
                return None;
            }
            cur_type = &fields[0];
        }
        ParquetType::PrimitiveType(_) => {
            return None;
        }
    };

    // Next, count repeated LIST sub-types.
    let mut dim = 0;
    loop {
        match cur_type {
            ParquetType::PrimitiveType(_) => {
                break;
            }
            ParquetType::GroupType {
                field_info,
                logical_type: _,
                converted_type: _,
                fields,
            } => {
                if fields.len() != 1 {
                    return None;
                }
                if field_info.repetition == Repetition::Repeated {
                    dim += 1;
                }
                cur_type = &fields[0];
            }
        }
    }

    encode_array_type(ColumnTypeTag::Double, dim).ok()
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;

    use crate::allocator::TestAllocatorState;
    use crate::parquet_read::meta::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use parquet::file::reader::Length;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};
    use tempfile::NamedTempFile;

    #[test]
    fn test_decode_column_type_fixed() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 10;
        let mut buffers_columns = Vec::new();
        let mut columns = Vec::new();

        let cols: Vec<_> = [
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
        ]
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
            ColumnType::try_from(col_type_i32).expect("invalid column type")
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
                false,
            )
            .unwrap(),
        )
    }
}
