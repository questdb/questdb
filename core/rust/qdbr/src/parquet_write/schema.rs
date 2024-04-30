use parquet2::schema::types::{
    IntegerType, ParquetType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, TimeUnit,
};
use parquet2::schema::Repetition;

#[derive(Debug)]
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
            _ => Err(format!("unknown column type: {}", v)),
        }
    }
}
pub fn qdb_to_parquet_type(
    column_name: &str,
    column_type: ColumnType,
) -> parquet2::error::Result<ParquetType> {
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
        // TODO: add varchar
        ColumnType::String | ColumnType::Symbol => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::ByteArray,
            Repetition::Optional,
            Some(PrimitiveConvertedType::Utf8),
            Some(PrimitiveLogicalType::String),
            None,
        )?),
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
