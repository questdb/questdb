use std::slice;

use crate::parquet::error::fmt_err;
use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QdbMetaCol, QdbMetaColFormat, QDB_META_KEY};
use parquet2::compression::CompressionOptions;
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
        ColumnTypeTag::String
        | ColumnTypeTag::Symbol
        | ColumnTypeTag::Varchar
        | ColumnTypeTag::VarcharSlice => Ok(ParquetType::try_from_primitive(
            name,
            PhysicalType::ByteArray,
            repetition,
            Some(PrimitiveConvertedType::Utf8),
            Some(PrimitiveLogicalType::String),
            Some(column_id),
        )?),
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
            Some(PrimitiveLogicalType::Integer(IntegerType::UInt32)),
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
    /// Packed per-column parquet encoding config from Java.
    /// Bits 0-7: encoding enum, bits 8-15: compression codec, bits 16-23: compression level, bit 24: isExplicitlySet
    pub parquet_encoding_config: i32,
}

impl Column {
    #[allow(clippy::too_many_arguments, clippy::not_unsafe_ptr_arg_deref)]
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
        parquet_encoding_config: i32,
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
            parquet_encoding_config,
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

        let ascii = if column.data_type.tag() == ColumnTypeTag::Varchar
            && !column.secondary_data.is_empty()
        {
            let aux: &[[u8; 16]] = unsafe { super::util::transmute_slice(column.secondary_data) };
            Some(super::varchar::is_column_ascii(aux))
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
        qdb_meta.schema.push(QdbMetaCol {
            column_type,
            column_top: column.column_top,
            format,
            ascii,
        });
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
        .map(|c| {
            if let Some(enc) = encoding_from_config(c.parquet_encoding_config) {
                validate_encoding(c.data_type, enc)
            } else {
                encoding_map(c.data_type)
            }
        })
        .collect()
}

/// Check whether the given encoding_id is valid for the column type tag.
/// encoding_id values: 0=DEFAULT, 1=PLAIN, 2=RLE_DICTIONARY,
/// 3=DELTA_LENGTH_BYTE_ARRAY, 4=DELTA_BINARY_PACKED, 5=BYTE_STREAM_SPLIT.
/// Returns true if encoding_id 0 (DEFAULT), or if the encoding is accepted
/// by `validate_encoding` for the given column type tag.
pub fn is_encoding_valid_for_column_tag(encoding_id: i32, col_type_tag: i32) -> bool {
    if encoding_id == 0 {
        return true;
    }
    let encoding = match encoding_id {
        1 => Encoding::Plain,
        2 => Encoding::RleDictionary,
        3 => Encoding::DeltaLengthByteArray,
        4 => Encoding::DeltaBinaryPacked,
        5 => Encoding::ByteStreamSplit,
        _ => return false,
    };
    let tag = match ColumnTypeTag::try_from(col_type_tag as u8) {
        Ok(t) => t,
        Err(_) => return false,
    };
    let col_type = ColumnType::new(tag, 0);
    validate_encoding(col_type, encoding) == encoding
}

/// Validate that the given encoding is supported for the column type.
/// Falls back to the default encoding if the combination is unsupported.
fn validate_encoding(data_type: ColumnType, encoding: Encoding) -> Encoding {
    let valid = match encoding {
        Encoding::Plain => !matches!(
            data_type.tag(),
            ColumnTypeTag::Symbol | ColumnTypeTag::Varchar
        ),
        Encoding::RleDictionary => !matches!(
            data_type.tag(),
            ColumnTypeTag::Boolean | ColumnTypeTag::Array
        ),
        Encoding::DeltaLengthByteArray => matches!(
            data_type.tag(),
            ColumnTypeTag::String | ColumnTypeTag::Binary | ColumnTypeTag::Varchar
        ),
        Encoding::DeltaBinaryPacked => matches!(
            data_type.tag(),
            ColumnTypeTag::Byte
                | ColumnTypeTag::Short
                | ColumnTypeTag::Char
                | ColumnTypeTag::Int
                | ColumnTypeTag::Long
                | ColumnTypeTag::Date
                | ColumnTypeTag::Timestamp
                | ColumnTypeTag::IPv4
                | ColumnTypeTag::GeoByte
                | ColumnTypeTag::GeoShort
                | ColumnTypeTag::GeoInt
                | ColumnTypeTag::GeoLong
        ),
        _ => false,
    };
    if valid {
        encoding
    } else {
        encoding_map(data_type)
    }
}

pub fn to_compressions(partition: &Partition) -> Vec<Option<CompressionOptions>> {
    partition
        .columns
        .iter()
        .map(|c| compression_from_config(c.parquet_encoding_config))
        .collect()
}

/// Extract per-column encoding override from packed config.
/// Returns None if the config uses default encoding (encoding byte == 0 or not explicitly set).
fn encoding_from_config(config: i32) -> Option<Encoding> {
    let is_explicit = (config & (1 << 24)) != 0;
    if !is_explicit {
        return None;
    }
    let encoding_id = config & 0xFF;
    match encoding_id {
        0 => None, // default
        1 => Some(Encoding::Plain),
        2 => Some(Encoding::RleDictionary),
        3 => Some(Encoding::DeltaLengthByteArray),
        4 => Some(Encoding::DeltaBinaryPacked),
        5 => Some(Encoding::ByteStreamSplit),
        _ => None,
    }
}

/// Extract per-column compression override from packed config.
/// Returns None if the config uses default/global compression.
fn compression_from_config(config: i32) -> Option<CompressionOptions> {
    let is_explicit = (config & (1 << 24)) != 0;
    if !is_explicit {
        return None;
    }
    let codec_id = (config >> 8) & 0xFF;
    let level = (config >> 16) & 0xFF;
    match codec_id {
        0 => None, // use global default
        1 => Some(CompressionOptions::Uncompressed),
        2 => Some(CompressionOptions::Snappy),
        3 => Some(CompressionOptions::Gzip(
            parquet2::compression::GzipLevel::try_new(if level > 0 { level as u8 } else { 6 }).ok(),
        )),
        4 => Some(CompressionOptions::Brotli(
            parquet2::compression::BrotliLevel::try_new(if level > 0 { level as u32 } else { 1 })
                .ok(),
        )),
        5 => Some(CompressionOptions::Zstd(
            parquet2::compression::ZstdLevel::try_new(if level > 0 { level } else { 1 }).ok(),
        )),
        6 => Some(CompressionOptions::Lz4Raw),
        _ => None,
    }
}

pub(crate) fn encoding_map(data_type: ColumnType) -> Encoding {
    match data_type.tag() {
        ColumnTypeTag::Symbol => Encoding::RleDictionary,
        ColumnTypeTag::Binary => Encoding::DeltaLengthByteArray,
        ColumnTypeTag::String => Encoding::DeltaLengthByteArray,
        ColumnTypeTag::Varchar => Encoding::RleDictionary,
        _ => Encoding::Plain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack_config(encoding: i32, compression: i32, level: i32) -> i32 {
        (encoding & 0xFF) | ((compression & 0xFF) << 8) | ((level & 0xFF) << 16) | (1 << 24)
    }

    #[test]
    fn test_encoding_from_config_default() {
        assert_eq!(encoding_from_config(0), None);
    }

    #[test]
    fn test_encoding_from_config_not_explicit() {
        // encoding byte set but explicit flag not set
        assert_eq!(encoding_from_config(1), None);
    }

    #[test]
    fn test_encoding_from_config_plain() {
        assert_eq!(
            encoding_from_config(pack_config(1, 0, 0)),
            Some(Encoding::Plain)
        );
    }

    #[test]
    fn test_encoding_from_config_rle_dictionary() {
        assert_eq!(
            encoding_from_config(pack_config(2, 0, 0)),
            Some(Encoding::RleDictionary)
        );
    }

    #[test]
    fn test_encoding_from_config_delta_length_byte_array() {
        assert_eq!(
            encoding_from_config(pack_config(3, 0, 0)),
            Some(Encoding::DeltaLengthByteArray)
        );
    }

    #[test]
    fn test_encoding_from_config_delta_binary_packed() {
        assert_eq!(
            encoding_from_config(pack_config(4, 0, 0)),
            Some(Encoding::DeltaBinaryPacked)
        );
    }

    #[test]
    fn test_encoding_from_config_byte_stream_split() {
        assert_eq!(
            encoding_from_config(pack_config(5, 0, 0)),
            Some(Encoding::ByteStreamSplit)
        );
    }

    #[test]
    fn test_encoding_from_config_encoding_zero_explicit() {
        // explicit flag set but encoding is 0 -> use default
        assert_eq!(encoding_from_config(pack_config(0, 0, 0)), None);
    }

    #[test]
    fn test_encoding_from_config_unknown_id() {
        assert_eq!(encoding_from_config(pack_config(99, 0, 0)), None);
    }

    #[test]
    fn test_compression_from_config_default() {
        assert_eq!(compression_from_config(0), None);
    }

    #[test]
    fn test_compression_from_config_not_explicit() {
        assert_eq!(compression_from_config(1 << 8), None);
    }

    #[test]
    fn test_compression_from_config_uncompressed() {
        let c = compression_from_config(pack_config(0, 1, 0));
        assert_eq!(c, Some(CompressionOptions::Uncompressed));
    }

    #[test]
    fn test_compression_from_config_snappy() {
        let c = compression_from_config(pack_config(0, 2, 0));
        assert_eq!(c, Some(CompressionOptions::Snappy));
    }

    #[test]
    fn test_compression_from_config_lz4_raw() {
        let c = compression_from_config(pack_config(0, 6, 0));
        assert_eq!(c, Some(CompressionOptions::Lz4Raw));
    }

    #[test]
    fn test_compression_from_config_zstd_default_level() {
        let c = compression_from_config(pack_config(0, 5, 0));
        match c {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_from_config_zstd_custom_level() {
        let c = compression_from_config(pack_config(0, 5, 3));
        match c {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_from_config_gzip() {
        let c = compression_from_config(pack_config(0, 3, 0));
        match c {
            Some(CompressionOptions::Gzip(_)) => {}
            other => panic!("expected Gzip, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_from_config_brotli() {
        let c = compression_from_config(pack_config(0, 4, 0));
        match c {
            Some(CompressionOptions::Brotli(_)) => {}
            other => panic!("expected Brotli, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_from_config_unknown_codec() {
        assert_eq!(compression_from_config(pack_config(0, 99, 0)), None);
    }

    #[test]
    fn test_combined_encoding_and_compression() {
        let config = pack_config(5, 5, 3); // BYTE_STREAM_SPLIT + ZSTD level 3
        assert_eq!(
            encoding_from_config(config),
            Some(Encoding::ByteStreamSplit)
        );
        match compression_from_config(config) {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_from_config_codec_zero_explicit() {
        // explicit flag set, compression codec is 0 -> use global default
        assert_eq!(compression_from_config(pack_config(1, 0, 0)), None);
    }

    fn col_type(tag: ColumnTypeTag) -> ColumnType {
        ColumnType::new(tag, 0)
    }

    #[test]
    fn test_validate_encoding_plain_valid() {
        for tag in [
            ColumnTypeTag::Boolean,
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
            ColumnTypeTag::Char,
            ColumnTypeTag::Int,
            ColumnTypeTag::Long,
            ColumnTypeTag::Date,
            ColumnTypeTag::Timestamp,
            ColumnTypeTag::Float,
            ColumnTypeTag::Double,
            ColumnTypeTag::String,
            ColumnTypeTag::Binary,
            ColumnTypeTag::Long128,
            ColumnTypeTag::Uuid,
            ColumnTypeTag::Long256,
            ColumnTypeTag::IPv4,
            ColumnTypeTag::GeoByte,
            ColumnTypeTag::GeoShort,
            ColumnTypeTag::GeoInt,
            ColumnTypeTag::GeoLong,
        ] {
            assert_eq!(
                validate_encoding(col_type(tag), Encoding::Plain),
                Encoding::Plain,
                "Plain should be valid for {:?}",
                tag
            );
        }
    }

    #[test]
    fn test_validate_encoding_plain_invalid() {
        // SYMBOL should fall back to RleDictionary
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Symbol), Encoding::Plain),
            Encoding::RleDictionary
        );
        // VARCHAR should fall back to RleDictionary
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Varchar), Encoding::Plain),
            Encoding::RleDictionary
        );
    }

    #[test]
    fn test_validate_encoding_rle_dictionary_valid() {
        for tag in [
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
            ColumnTypeTag::Char,
            ColumnTypeTag::Int,
            ColumnTypeTag::Long,
            ColumnTypeTag::Date,
            ColumnTypeTag::Timestamp,
            ColumnTypeTag::Float,
            ColumnTypeTag::Double,
            ColumnTypeTag::String,
            ColumnTypeTag::Symbol,
            ColumnTypeTag::Binary,
            ColumnTypeTag::Varchar,
            ColumnTypeTag::Long128,
            ColumnTypeTag::Uuid,
            ColumnTypeTag::Long256,
            ColumnTypeTag::IPv4,
            ColumnTypeTag::GeoByte,
            ColumnTypeTag::GeoShort,
            ColumnTypeTag::GeoInt,
            ColumnTypeTag::GeoLong,
            ColumnTypeTag::Decimal8,
            ColumnTypeTag::Decimal16,
            ColumnTypeTag::Decimal32,
            ColumnTypeTag::Decimal64,
            ColumnTypeTag::Decimal128,
            ColumnTypeTag::Decimal256,
        ] {
            assert_eq!(
                validate_encoding(col_type(tag), Encoding::RleDictionary),
                Encoding::RleDictionary,
                "RleDictionary should be valid for {:?}",
                tag
            );
        }
    }

    #[test]
    fn test_validate_encoding_rle_dictionary_invalid() {
        // Boolean should fall back to Plain
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Boolean), Encoding::RleDictionary),
            Encoding::Plain
        );
        // Array should fall back to Plain
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Array), Encoding::RleDictionary),
            Encoding::Plain
        );
    }

    #[test]
    fn test_validate_encoding_delta_length_byte_array_valid() {
        for tag in [
            ColumnTypeTag::String,
            ColumnTypeTag::Binary,
            ColumnTypeTag::Varchar,
        ] {
            assert_eq!(
                validate_encoding(col_type(tag), Encoding::DeltaLengthByteArray),
                Encoding::DeltaLengthByteArray,
                "DeltaLengthByteArray should be valid for {:?}",
                tag
            );
        }
    }

    #[test]
    fn test_validate_encoding_delta_length_byte_array_invalid() {
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Int), Encoding::DeltaLengthByteArray),
            Encoding::Plain
        );
        assert_eq!(
            validate_encoding(
                col_type(ColumnTypeTag::Float),
                Encoding::DeltaLengthByteArray
            ),
            Encoding::Plain
        );
    }

    #[test]
    fn test_validate_encoding_delta_binary_packed_valid() {
        for tag in [
            ColumnTypeTag::Byte,
            ColumnTypeTag::Short,
            ColumnTypeTag::Char,
            ColumnTypeTag::Int,
            ColumnTypeTag::Long,
            ColumnTypeTag::Date,
            ColumnTypeTag::Timestamp,
            ColumnTypeTag::IPv4,
            ColumnTypeTag::GeoByte,
            ColumnTypeTag::GeoShort,
            ColumnTypeTag::GeoInt,
            ColumnTypeTag::GeoLong,
        ] {
            assert_eq!(
                validate_encoding(col_type(tag), Encoding::DeltaBinaryPacked),
                Encoding::DeltaBinaryPacked,
                "DeltaBinaryPacked should be valid for {:?}",
                tag
            );
        }
    }

    #[test]
    fn test_validate_encoding_delta_binary_packed_invalid() {
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Float), Encoding::DeltaBinaryPacked),
            Encoding::Plain
        );
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Double), Encoding::DeltaBinaryPacked),
            Encoding::Plain
        );
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::String), Encoding::DeltaBinaryPacked),
            Encoding::DeltaLengthByteArray
        );
    }

    #[test]
    fn test_validate_encoding_byte_stream_split_falls_back() {
        // ByteStreamSplit is not supported for any type, should fall back
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Float), Encoding::ByteStreamSplit),
            Encoding::Plain
        );
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Int), Encoding::ByteStreamSplit),
            Encoding::Plain
        );
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Symbol), Encoding::ByteStreamSplit),
            Encoding::RleDictionary
        );
    }

    const ALL_TAGS: [ColumnTypeTag; 30] = [
        ColumnTypeTag::Boolean,
        ColumnTypeTag::Byte,
        ColumnTypeTag::Short,
        ColumnTypeTag::Char,
        ColumnTypeTag::Int,
        ColumnTypeTag::Long,
        ColumnTypeTag::Date,
        ColumnTypeTag::Timestamp,
        ColumnTypeTag::Float,
        ColumnTypeTag::Double,
        ColumnTypeTag::String,
        ColumnTypeTag::Symbol,
        ColumnTypeTag::Long256,
        ColumnTypeTag::GeoByte,
        ColumnTypeTag::GeoShort,
        ColumnTypeTag::GeoInt,
        ColumnTypeTag::GeoLong,
        ColumnTypeTag::Binary,
        ColumnTypeTag::Uuid,
        ColumnTypeTag::Long128,
        ColumnTypeTag::IPv4,
        ColumnTypeTag::Varchar,
        ColumnTypeTag::Array,
        ColumnTypeTag::Decimal8,
        ColumnTypeTag::Decimal16,
        ColumnTypeTag::Decimal32,
        ColumnTypeTag::Decimal64,
        ColumnTypeTag::Decimal128,
        ColumnTypeTag::Decimal256,
        ColumnTypeTag::VarcharSlice,
    ];

    #[test]
    fn test_is_encoding_valid_default_always_true() {
        for tag in ALL_TAGS {
            assert!(
                is_encoding_valid_for_column_tag(0, tag as i32),
                "DEFAULT should be valid for {:?}",
                tag
            );
        }
    }

    #[test]
    fn test_is_encoding_valid_unknown_encoding() {
        assert!(!is_encoding_valid_for_column_tag(
            99,
            ColumnTypeTag::Int as i32
        ));
        assert!(!is_encoding_valid_for_column_tag(
            -1,
            ColumnTypeTag::Int as i32
        ));
    }

    #[test]
    fn test_is_encoding_valid_unknown_column_tag() {
        assert!(!is_encoding_valid_for_column_tag(1, 255));
        assert!(!is_encoding_valid_for_column_tag(1, 0));
    }

    #[test]
    fn test_is_encoding_valid_matches_validate_encoding() {
        let encodings = [
            (1, Encoding::Plain),
            (2, Encoding::RleDictionary),
            (3, Encoding::DeltaLengthByteArray),
            (4, Encoding::DeltaBinaryPacked),
            (5, Encoding::ByteStreamSplit),
        ];
        for tag in ALL_TAGS {
            let ct = col_type(tag);
            for &(enc_id, enc) in &encodings {
                let expected = validate_encoding(ct, enc) == enc;
                assert_eq!(
                    is_encoding_valid_for_column_tag(enc_id, tag as i32),
                    expected,
                    "mismatch for encoding_id={} tag={:?}",
                    enc_id,
                    tag
                );
            }
        }
    }
}
