use std::slice;

use parquet2::encoding::Encoding;
use parquet2::metadata::SchemaDescriptor;
use parquet2::schema::types::{
    IntegerType, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;

use crate::parquet_write::{ParquetError, ParquetResult};

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ColumnType {
    Boolean,
    Byte,
    Short,
    Char,
    Int,
    Long,
    Date,
    Timestamp,
    Float,
    Double,
    String,
    Symbol,
    Long256,
    GeoByte,
    GeoShort,
    GeoInt,
    GeoLong,
    Binary,
    Uuid,
    Long128,
    IPv4,
    Varchar,
}

impl TryFrom<i32> for ColumnType {
    type Error = String;

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        // Start with removing geohash size bits. See ColumnType#tagOf().
        let col_tag = v & 0xFF;
        match col_tag {
            1 => Ok(ColumnType::Boolean),
            2 => Ok(ColumnType::Byte),
            3 => Ok(ColumnType::Short),
            4 => Ok(ColumnType::Char),
            5 => Ok(ColumnType::Int),
            6 => Ok(ColumnType::Long),
            7 => Ok(ColumnType::Date),
            8 => Ok(ColumnType::Timestamp),
            9 => Ok(ColumnType::Float),
            10 => Ok(ColumnType::Double),
            11 => Ok(ColumnType::String),
            12 => Ok(ColumnType::Symbol),
            13 => Ok(ColumnType::Long256),
            14 => Ok(ColumnType::GeoByte),
            15 => Ok(ColumnType::GeoShort),
            16 => Ok(ColumnType::GeoInt),
            17 => Ok(ColumnType::GeoLong),
            18 => Ok(ColumnType::Binary),
            19 => Ok(ColumnType::Uuid),
            24 => Ok(ColumnType::Long128),
            25 => Ok(ColumnType::IPv4),
            26 => Ok(ColumnType::Varchar),
            _ => Err(format!("unknown column type: {}", v)),
        }
    }
}

pub fn column_type_to_parquet_type(
    column_name: &str,
    column_type: ColumnType,
) -> ParquetResult<ParquetType> {
    let name = column_name.to_string();

    match column_type {
        ColumnType::Boolean => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Boolean,
            Repetition::Required,
            None,
            None,
            None,
        )?),
        ColumnType::Byte => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Required,
            Some(PrimitiveConvertedType::Int8),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int8)),
            None,
        )?),
        ColumnType::Short | ColumnType::Char => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Required,
            Some(PrimitiveConvertedType::Int16),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int16)),
            None,
        )?),
        ColumnType::Int => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Optional,
            None,
            None,
            None,
        )?),
        ColumnType::Long => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            Repetition::Optional,
            None,
            None,
            None,
        )?),
        ColumnType::Date | ColumnType::Timestamp => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            Repetition::Optional,
            Some(PrimitiveConvertedType::TimestampMicros),
            Some(PrimitiveLogicalType::Timestamp {
                unit: TimeUnit::Microseconds,
                is_adjusted_to_utc: true,
            }),
            None,
        )?),
        ColumnType::Float => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Float,
            Repetition::Optional,
            None,
            None,
            None,
        )?),
        ColumnType::Double => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Double,
            Repetition::Optional,
            None,
            None,
            None,
        )?),
        ColumnType::String | ColumnType::Symbol | ColumnType::Varchar => {
            Ok(ParquetType::try_from_primitive(
                name,
                PhysicalType::ByteArray,
                Repetition::Optional,
                Some(PrimitiveConvertedType::Utf8),
                Some(PrimitiveLogicalType::String),
                None,
            )?)
        }
        ColumnType::Long256 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(32),
            Repetition::Optional,
            None,
            None,
            None,
        )?),
        ColumnType::GeoByte => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Optional,
            Some(PrimitiveConvertedType::Int8),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int8)),
            None,
        )?),
        ColumnType::GeoShort => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Optional,
            Some(PrimitiveConvertedType::Int16),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int16)),
            None,
        )?),
        ColumnType::GeoInt => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Optional,
            Some(PrimitiveConvertedType::Int32),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int32)),
            None,
        )?),
        ColumnType::GeoLong => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int64,
            Repetition::Optional,
            Some(PrimitiveConvertedType::Int64),
            Some(PrimitiveLogicalType::Integer(IntegerType::Int64)),
            None,
        )?),
        ColumnType::Binary => {
            Ok(ParquetType::try_from_primitive(
                name,
                PhysicalType::ByteArray,
                Repetition::Required, //TODO: check for nullability
                None,
                None,
                None,
            )?)
        }
        ColumnType::Uuid | ColumnType::Long128 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::FixedLenByteArray(16),
            Repetition::Required, //TODO: check for nullability
            None,
            None,
            None,
        )?),
        ColumnType::IPv4 => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::Int32,
            Repetition::Optional,
            None,
            None,
            None,
        )?),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Column {
    pub name: &'static str,
    pub data_type: ColumnType,
    pub row_count: usize,
    pub primary_data: &'static [u8],
    pub secondary_data: Option<&'static [u8]>,
    pub symbol_offsets: Option<&'static [u64]>,
}

impl Column {
    pub fn from_raw_data(
        name: &'static str,
        column_type: i32,
        row_count: usize,
        primary_data_ptr: *const u8,
        primary_data_size: usize,
        secondary_data_ptr: *const u8,
        secondary_data_size: usize,
        symbol_offsets_ptr: *const u64,
        symbol_offsets_size: usize,
    ) -> ParquetResult<Self> {
        assert!(row_count > 0);
        let column_type: ColumnType = column_type
            .try_into()
            .map_err(ParquetError::InvalidParameter)?;
        assert!(!primary_data_ptr.is_null());

        let primary_data = unsafe { slice::from_raw_parts(primary_data_ptr, primary_data_size) };
        let secondary_data = if secondary_data_ptr.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(secondary_data_ptr, secondary_data_size) })
        };
        let symbol_offsets = if symbol_offsets_ptr.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(symbol_offsets_ptr, symbol_offsets_size) })
        };

        Ok(Column {
            name,
            data_type: column_type,
            row_count,
            primary_data,
            secondary_data,
            symbol_offsets,
        })
    }
}

pub struct Partition {
    pub table: String,
    pub columns: Vec<Column>,
}

pub fn to_parquet_schema(partition: &Partition) -> ParquetResult<SchemaDescriptor> {
    let parquet_types = partition
        .columns
        .iter()
        .map(|c| column_type_to_parquet_type(&c.name, c.data_type))
        .collect::<ParquetResult<Vec<_>>>()?;
    Ok(SchemaDescriptor::new(
        partition.table.clone(),
        parquet_types,
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
    match data_type {
        ColumnType::Float | ColumnType::Double => Encoding::Plain,
        ColumnType::Symbol => Encoding::RleDictionary,
        ColumnType::Binary => Encoding::DeltaLengthByteArray,
        // _ => Encoding::DeltaBinaryPacked, //TODO: for tests only
        _ => Encoding::Plain, //TODO: for tests only
                              //_ => Encoding::RleDictionary,
    }
}
