use std::slice;

use crate::parquet::error::fmt_err;
use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol, QdbMetaColFormat, QDB_META_KEY};
use parquet2::encoding::Encoding;
use parquet2::metadata::KeyValue;
use parquet2::metadata::SchemaDescriptor;
use parquet2::schema::types::GroupConvertedType;
use parquet2::schema::types::GroupLogicalType;
use parquet2::schema::types::{
    IntegerType, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;
use qdb_core::col_type::{ColumnType, ColumnTypeTag, QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG};

pub fn column_type_to_parquet_type(
    column_id: i32,
    column_name: &str,
    column_type: ColumnType,
    designated_timestamp: bool,
    required: bool,
    raw_array_encoding: bool,
) -> ParquetResult<ParquetType> {
    let name = column_name.to_string();
    // Types that don't have null values in QuestDB always use Required repetition
    let is_notnull_type = matches!(
        column_type.tag(),
        ColumnTypeTag::Boolean | ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Char
    );
    let repetition = if designated_timestamp || required || is_notnull_type {
        Repetition::Required
    } else {
        Repetition::Optional
    };

    match column_type.tag() {
        ColumnTypeTag::Boolean => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Boolean,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Byte => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int8),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int8)),
            Some(column_id),
        )?),
        ColumnTypeTag::Short => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int16),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int16)),
            Some(column_id),
        )?),
        ColumnTypeTag::Char => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int16),
            Some(PrimitiveLogicalType::Integer(IntegerType::UInt16)),
            Some(column_id),
        )?),
        ColumnTypeTag::Int => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Long => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Date => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            Some(PrimitiveConvertedType::TimestampMillis),
            Some(PrimitiveLogicalType::Timestamp {
                unit: TimeUnit::Milliseconds,
                is_adjusted_to_utc: true,
            }),
            Some(column_id),
        )?),
        ColumnTypeTag::Timestamp => {
            if column_type.has_flag(QDB_TIMESTAMP_NS_COLUMN_TYPE_FLAG) {
                Ok(ParquetType::try_from_primitive(
                    name,
                    PhysicalType::Int64,
                    repetition,
                    None,
                    Some(PrimitiveLogicalType::Timestamp {
                        unit: TimeUnit::Nanoseconds,
                        is_adjusted_to_utc: true,
                    }),
                    Some(column_id),
                )?)
            } else {
                Ok(ParquetType::try_from_primitive(
                    name,
                    PhysicalType::Int64,
                    repetition,
                    Some(PrimitiveConvertedType::TimestampMicros),
                    Some(PrimitiveLogicalType::Timestamp {
                        unit: TimeUnit::Microseconds,
                        is_adjusted_to_utc: true,
                    }),
                    Some(column_id),
                )?)
            }
        }
        ColumnTypeTag::Float => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Float,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Double => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Double,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Binary => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::ByteArray,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::String | ColumnTypeTag::Symbol | ColumnTypeTag::Varchar => {
            Ok(ParquetType::try_from_primitive(
                name,
                PhysicalType::ByteArray,
                repetition,
                Some(PrimitiveConvertedType::Utf8),
                Some(PrimitiveLogicalType::String),
                Some(column_id),
            )?)
        }
        ColumnTypeTag::Array => {
            if raw_array_encoding {
                // encode in native QDB array format
                Ok(ParquetType::try_from_primitive(
                    name,
                    PhysicalType::ByteArray,
                    repetition,
                    None,
                    None,
                    Some(column_id),
                )?)
            } else {
                // encode as nested lists
                // see https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
                let elem_type = column_type.array_element_type()?;
                if elem_type != ColumnTypeTag::Double {
                    return Err(fmt_err!(
                        InvalidType,
                        "unsupported array element type {}",
                        elem_type.name()
                    ));
                }
                let elem_type = ParquetType::try_from_primitive(
                    "element".to_string(),
                    PhysicalType::Double,
                    Repetition::Optional,
                    None,
                    None,
                    None,
                )?;
                let dim = column_type.array_dimensionality()?;
                let mut root_type = elem_type;
                for i in 0..dim {
                    let list = ParquetType::from_group(
                        "list".to_string(),
                        Repetition::Repeated,
                        None,
                        None,
                        vec![root_type],
                        None,
                    );
                    if i < dim - 1 {
                        root_type = ParquetType::from_group(
                            "list".to_string(),
                            Repetition::Required,
                            Some(GroupConvertedType::List),
                            Some(GroupLogicalType::List),
                            vec![list],
                            None,
                        );
                    } else {
                        // top field has to be nullable, hence optional repetition
                        root_type = ParquetType::from_group(
                            name.clone(),
                            Repetition::Optional,
                            Some(GroupConvertedType::List),
                            Some(GroupLogicalType::List),
                            vec![list],
                            Some(column_id),
                        );
                    }
                }
                Ok(root_type)
            }
        }
        ColumnTypeTag::Long256 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(32),
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::GeoByte => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int8),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int8)),
            Some(column_id),
        )?),
        ColumnTypeTag::GeoShort => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int16),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int16)),
            Some(column_id),
        )?),
        ColumnTypeTag::GeoInt => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            Some(PrimitiveConvertedType::Int32),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int32)),
            Some(column_id),
        )?),
        ColumnTypeTag::GeoLong => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            repetition,
            Some(PrimitiveConvertedType::Int64),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int64)),
            Some(column_id),
        )?),
        ColumnTypeTag::Long128 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(16),
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Uuid => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(16),
            repetition,
            None,
            Some(PrimitiveLogicalType::Uuid),
            Some(column_id),
        )?),
        ColumnTypeTag::IPv4 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            repetition,
            None,
            None,
            Some(column_id),
        )?),
        ColumnTypeTag::Decimal8
        | ColumnTypeTag::Decimal16
        | ColumnTypeTag::Decimal32
        | ColumnTypeTag::Decimal64
        | ColumnTypeTag::Decimal128
        | ColumnTypeTag::Decimal256 => {
            let size = match column_type.tag() {
                ColumnTypeTag::Decimal8 => 1,
                ColumnTypeTag::Decimal16 => 2,
                ColumnTypeTag::Decimal32 => 4,
                ColumnTypeTag::Decimal64 => 8,
                ColumnTypeTag::Decimal128 => 16,
                ColumnTypeTag::Decimal256 => 32,
                _ => unreachable!(),
            };
            let scale = column_type.decimal_scale() as usize;
            let precision = column_type.decimal_precision() as usize;
            Ok(ParquetType::try_from_primitive(
                name,
                PhysicalType::FixedLenByteArray(size),
                repetition,
                Some(PrimitiveConvertedType::Decimal(precision, scale)),
                Some(PrimitiveLogicalType::Decimal(precision, scale)),
                Some(column_id),
            )?)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Column {
    pub id: i32,
    pub name: &'static str,
    pub data_type: ColumnType,
    pub row_count: usize,
    pub column_top: usize,
    pub primary_data: &'static [u8],
    pub secondary_data: &'static [u8],
    pub symbol_offsets: &'static [u64],
    pub designated_timestamp: bool,
    /// Passed by QuestDB during writes to indicate that the column contains no null values.
    /// Currently only Symbol dataType columns support this flag.
    pub required: bool,
    pub designated_timestamp_ascending: bool,
}

impl Column {
    #[allow(clippy::too_many_arguments)]
    pub fn from_raw_data(
        id: i32,
        name: &'static str,
        column_type: i32,
        column_top: i64,
        row_count: usize,
        primary_data_ptr: *const u8,
        primary_data_size: usize,
        secondary_data_ptr: *const u8,
        secondary_data_size: usize,
        symbol_offsets_ptr: *const u64,
        symbol_offsets_size: usize,
        designated_timestamp: bool,
        designated_timestamp_ascending: bool,
    ) -> ParquetResult<Self> {
        assert!(
            !primary_data_ptr.is_null() || primary_data_size == 0,
            "primary_data_ptr inconsistent with primary_data_size"
        );
        assert!(
            !secondary_data_ptr.is_null() || secondary_data_size == 0,
            "secondary_data_ptr inconsistent with secondary_data_size"
        );
        assert!(
            !symbol_offsets_ptr.is_null() || symbol_offsets_size == 0,
            "symbol_offsets_ptr inconsistent with symbol_offsets_size"
        );

        let required = column_type < 0;
        let column_type: ColumnType = (column_type & 0x7FFFFFFF).try_into()?;

        let primary_data = if primary_data_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(primary_data_ptr, primary_data_size) }
        };
        let secondary_data = if secondary_data_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(secondary_data_ptr, secondary_data_size) }
        };
        let symbol_offsets = if symbol_offsets_ptr.is_null() {
            &[]
        } else {
            unsafe { slice::from_raw_parts(symbol_offsets_ptr, symbol_offsets_size) }
        };

        Ok(Column {
            id,
            name,
            data_type: column_type,
            column_top: column_top as usize,
            row_count,
            primary_data,
            secondary_data,
            symbol_offsets,
            designated_timestamp,
            required,
            designated_timestamp_ascending,
        })
    }
}

pub struct Partition {
    pub table: String,
    pub columns: Vec<Column>,
}

pub fn to_parquet_schema(
    partition: &Partition,
    raw_array_encoding: bool,
) -> ParquetResult<(SchemaDescriptor, Vec<KeyValue>)> {
    let parquet_types = partition
        .columns
        .iter()
        .map(|c| {
            column_type_to_parquet_type(
                c.id,
                c.name,
                c.data_type,
                c.designated_timestamp,
                c.required,
                raw_array_encoding,
            )
        })
        .collect::<ParquetResult<Vec<_>>>()?;

    let mut qdb_meta = QdbMeta::new(partition.columns.len());
    for column in partition.columns.iter() {
        let format = if column.data_type.tag() == ColumnTypeTag::Symbol {
            Some(QdbMetaColFormat::LocalKeyIsGlobal)
        } else {
            None
        };

        let column_type = if column.designated_timestamp {
            column
                .data_type
                .into_designated_with_order(column.designated_timestamp_ascending)?
        } else {
            column.data_type
        };
        qdb_meta
            .schema
            .push(QdbMetaCol { column_type, column_top: column.column_top, format });
    }

    let encoded_qdb_meta = qdb_meta.serialize()?;
    let questdb_keyval = KeyValue::new(QDB_META_KEY.to_string(), encoded_qdb_meta);

    Ok((
        SchemaDescriptor::new(partition.table.clone(), parquet_types),
        vec![questdb_keyval],
    ))
}

pub fn to_encodings(partition: &Partition) -> Vec<Encoding> {
    partition
        .columns
        .iter()
        .map(|c| encoding_map(c.data_type))
        .collect()
}

fn encoding_map(data_type: ColumnType) -> Encoding {
    match data_type.tag() {
        ColumnTypeTag::Symbol => Encoding::RleDictionary,
        ColumnTypeTag::Binary => Encoding::DeltaLengthByteArray,
        ColumnTypeTag::String => Encoding::DeltaLengthByteArray,
        ColumnTypeTag::Varchar => Encoding::DeltaLengthByteArray,
        _ => Encoding::Plain,
    }
}
