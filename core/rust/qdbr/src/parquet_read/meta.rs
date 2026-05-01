use crate::allocator::{AcVec, QdbAllocator};
use crate::parquet::error::{ParquetError, ParquetErrorReason, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_read::{ColumnMeta, ParquetDecoder};
use nonmax::NonMaxU32;
use parquet2::metadata::{ColumnDescriptor, FileMetaData};
use parquet2::read::read_metadata_with_size;
use parquet2::schema::types::PrimitiveLogicalType::{Timestamp, Uuid};
use parquet2::schema::types::{GroupConvertedType, GroupLogicalType, ParquetType};
use parquet2::schema::types::{
    IntegerType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;
use qdb_core::col_type::{
    encode_array_type, ColumnType, ColumnTypeTag, QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG,
};
use std::io::{Read, Seek};

/// Extract the questdb-specific metadata from the parquet file metadata.
/// Error if the JSON is not valid or the version is not supported.
/// Returns `None` if the metadata is not present.
pub(crate) fn extract_qdb_meta(file_metadata: &FileMetaData) -> ParquetResult<Option<QdbMeta>> {
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

impl ParquetDecoder {
    pub fn read<R: Read + Seek>(
        allocator: QdbAllocator,
        reader: &mut R,
        file_size: u64,
    ) -> ParquetResult<Self> {
        let metadata = read_metadata_with_size(reader, file_size)?;
        let col_len = metadata.schema_descr.columns().len();
        let qdb_meta = extract_qdb_meta(&metadata)?;

        // Discard QDB metadata when the schema length doesn't match the Parquet
        // column count. This happens when an external tool rewrites the file
        // (e.g. drops partition columns) but preserves the original key-value
        // metadata. Positional lookups into a stale schema return wrong types.
        //
        // Note: col_len is the number of leaf (primitive) columns from
        // schema_descr.columns(). For all column types QuestDB currently writes
        // (including arrays encoded as LIST groups), each top-level field
        // produces exactly one leaf column, so this matches qdb_meta.schema.len().
        let qdb_meta = qdb_meta.filter(|m| m.schema.len() == col_len);
        let mut row_group_sizes: AcVec<u32> =
            AcVec::with_capacity_in(metadata.row_groups.len(), allocator.clone())?;
        let mut row_group_sizes_acc: AcVec<usize> =
            AcVec::with_capacity_in(metadata.row_groups.len(), allocator.clone())?;
        let mut columns = AcVec::with_capacity_in(col_len, allocator.clone())?;

        let mut accumulated_size = 0;
        for row_group in metadata.row_groups.iter() {
            row_group_sizes_acc.push(accumulated_size)?;
            let row_group_size = row_group.num_rows();
            row_group_sizes.push(row_group_size as u32)?;
            accumulated_size += row_group_size;
        }

        if accumulated_size != metadata.num_rows {
            let mut err = ParquetError::new(ParquetErrorReason::Layout);
            err.add_context("row group sizes do not sum to total row count");
            return Err(err);
        }

        let mut timestamp_index: Option<NonMaxU32> = None;

        for (index, column) in metadata.schema_descr.columns().iter().enumerate() {
            // Arrays have column name and id stored in the base group type.
            // Primitive type fields have the same primitive type as the base type.
            // That's why we're using the base type.

            let base_field = column.base_type.get_field_info();
            let name_str = &base_field.name;
            let mut name = AcVec::with_capacity_in(name_str.len() * 2, allocator.clone())?;
            name.extend(name_str.encode_utf16())?;

            if let Some(mut column_type) =
                Self::descriptor_to_column_type(column, index, qdb_meta.as_ref())
            {
                if column_type.is_designated() {
                    if column_type.is_designated_timestamp_ascending() {
                        timestamp_index = NonMaxU32::new(index as u32);
                    }
                    // Clear the bit as designated timestamp is reported in timestamp_index.
                    column_type = column_type.into_non_designated()?;
                }
                columns.push(ColumnMeta {
                    column_type: Some(column_type),
                    id: base_field.id.unwrap_or(-1_i32),
                    name_size: name.len() as i32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                })?;
            } else {
                // The column is not supported, Java code will have to skip it.
                columns.push(ColumnMeta {
                    column_type: None,
                    id: -1,
                    name_size: name.len() as i32,
                    name_ptr: name.as_ptr(),
                    name_vec: name,
                })?;
            }
        }

        if timestamp_index.is_none() {
            // There was no designated timestamp flag in the metadata, so let's detect it.
            // First, find the first ASC order column, if it's the same across all row groups.
            let mut asc_column_index = -1_i32;
            for row_group in &metadata.row_groups {
                if let Some(sorting_columns) = row_group.sorting_columns() {
                    if let Some(sorting_column) = sorting_columns.first() {
                        if !sorting_column.descending
                            && (asc_column_index == -1
                                || asc_column_index == sorting_column.column_idx)
                        {
                            asc_column_index = sorting_column.column_idx;
                            continue;
                        }
                    }
                }
                asc_column_index = -1;
                break;
            }
            if asc_column_index > -1 {
                // We have a candidate, let's check its type and nullability.
                if let Some(column) = columns.get(asc_column_index as usize) {
                    if let Some(column_descr) = metadata
                        .schema_descr
                        .columns()
                        .get(asc_column_index as usize)
                    {
                        if let Some(column_type) = column.column_type {
                            if column_type.tag() == ColumnTypeTag::Timestamp
                                && column_descr.descriptor.primitive_type.field_info.repetition
                                    == Repetition::Required
                            {
                                timestamp_index = NonMaxU32::new(asc_column_index as u32);
                            }
                        }
                    }
                }
            }
        }

        let unused_bytes = qdb_meta.as_ref().map(|m| m.unused_bytes).unwrap_or(0);

        // TODO(eugenels): add some validation
        Ok(Self {
            allocator,
            col_count: columns.len() as u32,
            row_count: metadata.num_rows,
            row_group_count: metadata.row_groups.len() as u32,
            row_group_sizes_ptr: row_group_sizes.as_ptr(),
            row_group_sizes,
            timestamp_index,
            metadata,
            qdb_meta,
            columns_ptr: columns.as_ptr(),
            columns,
            row_group_sizes_acc,
            unused_bytes,
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
            (PhysicalType::Int64, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
            | (PhysicalType::Int64, _, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
                ColumnType::new_decimal(precision as u8, scale as u8)
            }
            (PhysicalType::Int64, _, _) => Some(ColumnType::new(ColumnTypeTag::Long, 0)),
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Integer(IntegerType::Int32)), _) => {
                Some(ColumnType::new(ColumnTypeTag::Int, 0))
            }
            (PhysicalType::Int32, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
            | (PhysicalType::Int32, _, Some(PrimitiveConvertedType::Decimal(precision, scale))) => {
                ColumnType::new_decimal(precision as u8, scale as u8)
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
            (
                PhysicalType::FixedLenByteArray(_),
                Some(PrimitiveLogicalType::Decimal(precision, scale)),
                _,
            )
            | (
                PhysicalType::FixedLenByteArray(_),
                _,
                Some(PrimitiveConvertedType::Decimal(precision, scale)),
            ) => ColumnType::new_decimal(precision as u8, scale as u8),
            (PhysicalType::ByteArray, Some(PrimitiveLogicalType::Decimal(precision, scale)), _)
            | (
                PhysicalType::ByteArray,
                _,
                Some(PrimitiveConvertedType::Decimal(precision, scale)),
            ) => ColumnType::new_decimal(precision as u8, scale as u8),
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
    use crate::parquet::tests::ColumnTypeTagExt;
    use crate::parquet_read::meta::ParquetDecoder;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{Column, Partition};
    use arrow::datatypes::ToByteSlice;
    use bytes::Bytes;
    use parquet::file::reader::Length;
    use parquet2::metadata::{ColumnDescriptor, Descriptor};
    use parquet2::schema::types::{
        FieldInfo, ParquetType, PhysicalType, PrimitiveLogicalType, PrimitiveType,
    };
    use parquet2::schema::Repetition;
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
        let mut file = File::open(Path::new(path)).unwrap();
        let file_len = file.len();
        let meta = ParquetDecoder::read(allocator, &mut file, file_len).unwrap();

        assert_eq!(meta.columns.len(), column_count);
        assert_eq!(meta.row_count, row_count);

        for (i, col) in meta.columns.iter().enumerate() {
            let (col_type, _, name) = cols[i];
            assert!(col.column_type.is_some());
            assert_eq!(col.column_type.unwrap(), col_type);
            let actual_name: String = String::from_utf16(&col.name_vec).unwrap();
            assert_eq!(actual_name, name);
        }

        temp_file.close().expect("Failed to delete temp file");

        // make sure buffer live until the end of the test
        assert_eq!(buffers_columns.len(), column_count);
    }

    #[test]
    fn test_descriptor_to_column_type_byte_array_decimal() {
        for (logical_type, converted_type) in [
            (Some(PrimitiveLogicalType::Decimal(20, 4)), None),
            (
                None,
                Some(parquet2::schema::types::PrimitiveConvertedType::Decimal(
                    20, 4,
                )),
            ),
        ] {
            let primitive_type = PrimitiveType {
                field_info: FieldInfo {
                    name: "dec_ba".to_string(),
                    repetition: Repetition::Required,
                    id: None,
                },
                logical_type,
                converted_type,
                physical_type: PhysicalType::ByteArray,
            };
            let descriptor = ColumnDescriptor::new(
                Descriptor {
                    primitive_type: primitive_type.clone(),
                    max_def_level: 0,
                    max_rep_level: 0,
                },
                vec!["dec_ba".to_string()],
                ParquetType::PrimitiveType(primitive_type),
            );

            let column_type = ParquetDecoder::descriptor_to_column_type(&descriptor, 0, None)
                .expect("decimal type should be inferred from BYTE_ARRAY");

            assert_eq!(
                column_type,
                ColumnType::new_decimal(20, 4).expect("valid QuestDB decimal type")
            );
        }
    }

    /// Parquet thrift Encoding enum bytes that the writer emits and the
    /// `row_group_column_has_encoding` reader compares against.
    /// PLAIN=0, RLE=3, RLE_DICTIONARY=8, PLAIN_DICTIONARY=2.
    const PARQUET_ENCODING_PLAIN: i32 = 0;
    const PARQUET_ENCODING_RLE_DICTIONARY: i32 = 8;

    /// Build a 100-row Int column. `parquet_encoding` follows the
    /// `parquet_encoding_override_round_trip_representative_types` convention:
    /// the low byte is a QuestDB encoding id (1 = Plain, 2 = RleDictionary)
    /// and bit 24 marks the override as user-supplied.
    fn build_int_column_with_encoding(encoding_id: i32) -> (Vec<i32>, Column) {
        let data: Vec<i32> = (0..100i32).collect();
        let parquet_encoding = if encoding_id == 0 {
            0
        } else {
            encoding_id | (1 << 24)
        };
        let col = Column::from_raw_data(
            0,
            "val",
            ColumnTypeTag::Int.into_type().code(),
            0,
            data.len(),
            data.as_ptr() as *const u8,
            data.len() * size_of::<i32>(),
            null(),
            0,
            null(),
            0,
            false,
            false,
            parquet_encoding,
        )
        .unwrap();
        (data, col)
    }

    /// Write a single-column single-row-group parquet file to a temp file and
    /// open it through `ParquetDecoder::read`. Returns the decoder, the temp
    /// file (kept alive so the path stays valid), and the original buffer of
    /// column data so the caller can keep that alive too.
    fn round_trip_int_column(
        encoding_id: i32,
    ) -> (
        ParquetDecoder,
        NamedTempFile,
        Vec<i32>,
        crate::allocator::TestAllocatorState,
    ) {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let (data, col) = build_int_column_with_encoding(encoding_id);
        let partition = Partition { table: "t".to_string(), columns: vec![col] };

        let mut buf: Cursor<Vec<u8>> = Cursor::new(Vec::new());
        ParquetWriter::new(&mut buf)
            .with_statistics(true)
            .with_row_group_size(Some(1_048_576))
            .with_data_page_size(Some(1_048_576))
            .finish(partition)
            .expect("parquet writer");

        buf.set_position(0);
        let bytes: Bytes = buf.into_inner().into();
        let mut temp_file = NamedTempFile::new().expect("temp file");
        temp_file
            .write_all(bytes.to_byte_slice())
            .expect("write parquet bytes");

        let path = temp_file.path().to_str().unwrap();
        let mut file = File::open(Path::new(path)).unwrap();
        let file_len = file.len();
        let decoder = ParquetDecoder::read(allocator, &mut file, file_len).unwrap();
        (decoder, temp_file, data, tas)
    }

    #[test]
    fn row_group_column_has_encoding_finds_plain() {
        let (decoder, _temp_file, _data, _tas) = round_trip_int_column(0);

        // Sanity-check the file shape.
        assert_eq!(decoder.row_group_count, 1);
        assert_eq!(decoder.col_count, 1);

        let has_plain = decoder
            .row_group_column_has_encoding(0, 0, PARQUET_ENCODING_PLAIN)
            .expect("plain lookup");
        assert!(has_plain, "default-encoded column should report PLAIN");

        let has_dict = decoder
            .row_group_column_has_encoding(0, 0, PARQUET_ENCODING_RLE_DICTIONARY)
            .expect("dict lookup");
        assert!(
            !has_dict,
            "default-encoded column should not report RLE_DICTIONARY"
        );
    }

    #[test]
    fn row_group_column_has_encoding_finds_rle_dictionary() {
        // QuestDB encoding id 2 = RleDictionary; the writer should emit
        // RLE_DICTIONARY=8 in the column chunk's encoding list.
        let (decoder, _temp_file, _data, _tas) = round_trip_int_column(2);

        let has_dict = decoder
            .row_group_column_has_encoding(0, 0, PARQUET_ENCODING_RLE_DICTIONARY)
            .expect("dict lookup");
        assert!(
            has_dict,
            "RleDictionary-overridden column should report RLE_DICTIONARY"
        );
    }

    #[test]
    fn row_group_column_has_encoding_row_group_index_out_of_range() {
        let (decoder, _temp_file, _data, _tas) = round_trip_int_column(0);

        let err = decoder
            .row_group_column_has_encoding(5, 0, PARQUET_ENCODING_PLAIN)
            .expect_err("expected out-of-range row group error");
        assert!(
            matches!(
                err.reason(),
                crate::parquet::error::ParquetErrorReason::InvalidLayout
            ),
            "expected InvalidLayout, got {:?}",
            err.reason()
        );
        let msg = err.to_string();
        assert!(
            msg.contains("row group index"),
            "error should mention 'row group index', got: {msg}"
        );
        assert!(
            msg.contains("out of range"),
            "error should mention 'out of range', got: {msg}"
        );
    }

    #[test]
    fn row_group_column_has_encoding_column_index_out_of_range() {
        let (decoder, _temp_file, _data, _tas) = round_trip_int_column(0);

        let bad_col = decoder.col_count + 1;
        let err = decoder
            .row_group_column_has_encoding(0, bad_col, PARQUET_ENCODING_PLAIN)
            .expect_err("expected out-of-range column error");
        assert!(
            matches!(
                err.reason(),
                crate::parquet::error::ParquetErrorReason::InvalidLayout
            ),
            "expected InvalidLayout, got {:?}",
            err.reason()
        );
        let msg = err.to_string();
        assert!(
            msg.contains("column index"),
            "error should mention 'column index', got: {msg}"
        );
        assert!(
            msg.contains("out of range"),
            "error should mention 'out of range', got: {msg}"
        );
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
                false,
                0,
            )
            .unwrap(),
        )
    }
}
