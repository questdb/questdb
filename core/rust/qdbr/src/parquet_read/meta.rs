use crate::parquet_read::{ColumnChunkBuffers, ColumnMeta, ParquetDecoder};
use crate::parquet_write::schema::ColumnType;
use crate::parquet_write::QDB_TYPE_META_PREFIX;
use parquet2::metadata::Descriptor;
use parquet2::read::read_metadata;
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{
    IntegerType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use std::collections::HashMap;
use std::fs::File;

impl ParquetDecoder {
    pub fn read(mut reader: File) -> anyhow::Result<Self> {
        let metadata = read_metadata(&mut reader)?;
        let col_len = metadata.schema_descr.columns().len();
        let additional_meta: Option<HashMap<String, Option<String>>> =
            metadata.key_value_metadata.as_ref().map(|vec| {
                vec.iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_owned()))
                    .collect()
            });
        let mut columns = Vec::with_capacity(col_len);
        let mut column_buffers = Vec::with_capacity(col_len);

        for (column_id, f) in metadata.schema_descr.columns().iter().enumerate() {
            // Some types are not supported, this will skip them.
            if let Some((data_type, column_type)) =
                Self::descriptor_to_column_type(&f.descriptor, &additional_meta)
            {
                let name_str = &f.descriptor.primitive_type.field_info.name;
                let name: Vec<u16> = name_str.encode_utf16().collect();

                columns.push(ColumnMeta {
                    typ: data_type,
                    column_type,
                    id: column_id as i32,
                    name_size: name.len() as u32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                });

                column_buffers.push(ColumnChunkBuffers::new());
            }
        }

        // TODO: add some validation
        let decoder = ParquetDecoder {
            col_count: columns.len() as i32,
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len() as i32,
            file: reader,
            metadata,
            decompress_buffer: vec![],
            columns_ptr: columns.as_ptr(),
            columns,
            column_buffers,
        };

        Ok(decoder)
    }

    fn descriptor_to_column_type(
        des: &Descriptor,
        additional_meta: &Option<HashMap<String, Option<String>>>,
    ) -> Option<(ColumnType, i32)> {
        if let Some(col_id) = des.primitive_type.field_info.id {
            let meta_kv_key = format!("{}{}", QDB_TYPE_META_PREFIX, col_id);
            let result = additional_meta
                .as_ref()
                .and_then(|hashmap| hashmap.get(&meta_kv_key));
            if let Some(Some(val)) = result {
                let full_col_type_int = match val.parse::<i32>() {
                    Ok(col_type_int) => match ColumnType::try_from(col_type_int) {
                        Ok(col_type) => return Some((col_type, col_type_int)),
                        _ => None,
                    },
                    _ => None,
                };
                if full_col_type_int.is_some() {
                    return full_col_type_int;
                }
            }
        }

        let column_type = match (
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
            ) => Some(ColumnType::Timestamp),
            (
                PhysicalType::Int64,
                Some(Timestamp {
                    unit: TimeUnit::Milliseconds,
                    is_adjusted_to_utc: _,
                }),
                _,
            ) => Some(ColumnType::Date),
            (PhysicalType::Int64, None, _) => Some(ColumnType::Long),
            (PhysicalType::Int64, Some(PrimitiveLogicalType::Integer(IntegerType::Int64)), _) => {
                Some(ColumnType::Long)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32)), _) => {
                Some(ColumnType::Int)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Decimal(_, _)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Decimal(_, _))) => {
                Some(ColumnType::Double)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int16)), _) => {
                Some(ColumnType::Short)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::UInt16)), _) => {
                Some(ColumnType::Int)
            }
            (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int16)) => {
                Some(ColumnType::Short)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int8)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int8)) => {
                Some(ColumnType::Byte)
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Date), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Date)) => {
                Some(ColumnType::Date)
            }
            (PhysicalType::Int32, None, _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Int32)) => {
                Some(ColumnType::Int)
            }
            (PhysicalType::Boolean, None, _) => Some(ColumnType::Boolean),
            (PhysicalType::Double, None, _) => Some(ColumnType::Double),
            (PhysicalType::Float, None, _) => Some(ColumnType::Float),
            (PhysicalType::FixedLenByteArray(16), Some(Uuid), _) => Some(ColumnType::Uuid),
            (PhysicalType::FixedLenByteArray(16), None, None) => Some(ColumnType::Long128),
            (PhysicalType::ByteArray, Some(PrimitiveLogicalType::String), _) => {
                Some(ColumnType::Varchar)
            }
            (PhysicalType::FixedLenByteArray(32), None, _) => Some(ColumnType::Long256),
            (PhysicalType::ByteArray, None, Some(PrimitiveConvertedType::Utf8)) => {
                Some(ColumnType::Varchar)
            }
            (PhysicalType::ByteArray, None, _) => Some(ColumnType::Binary),
            (PhysicalType::Int96, None, None) => Some(ColumnType::Timestamp),
            (_, _, _) => None,
        };

        column_type.map(|ct| (ct, ct as i32))
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{Cursor, Write};
    use std::mem::size_of;
    use std::path::Path;
    use std::ptr::null;

    use crate::parquet_read::meta::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use tempfile::NamedTempFile;

    use crate::parquet_write::schema::{Column, ColumnType, Partition};

    #[test]
    fn test_decode_column_type_fixed() {
        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        let row_count = 10;
        let mut buffers_columns = Vec::new();
        let mut columns = Vec::new();

        let cols = vec![
            (ColumnType::Long128, size_of::<i64>() * 2, "col_long128"),
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
            (ColumnType::Char, size_of::<u16>(), "col_char"),
        ];

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
        let meta = ParquetDecoder::read(file).unwrap();

        assert_eq!(meta.columns.len(), column_count);
        assert_eq!(meta.row_count, row_count);

        for (i, col) in meta.columns.iter().enumerate() {
            let (col_type, _, name) = cols[i];
            assert_eq!(col.typ, col_type);
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
        let col_type_i32 = col_type as i32;
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
                col_type as i32,
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
