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
    raw_array_encoding: bool,
) -> ParquetResult<ParquetType> {
    let name = column_name.to_string();
    // Types that don't have null values in QuestDB always use Required repetition.
    // All other types — including Symbol — use Optional so the file-level schema
    // is stable across O3 merges. This avoids a REQUIRED→OPTIONAL transition that
    // would break copy_row_group (raw-copied pages already have def levels encoded).
    //
    // Symbol columns are always Optional even when Column::not_null_hint is true (no
    // nulls). The `not_null_hint` flag is only a write-time hint that lets the encoder
    // emit a fast all-ones RLE run for definition levels instead of computing
    // per-row values. See encoders::symbol::encode().
    let is_notnull_type = matches!(
        column_type.tag(),
        ColumnTypeTag::Boolean | ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Char
    );
    let repetition = if designated_timestamp || is_notnull_type {
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
            Some(PrimitiveConvertedType::Uint16),
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
    /// Hint from QuestDB indicating that the column is NOT NULL and currently
    /// contains no null values. Used by Symbol and all primitive type encoders.
    /// It does NOT affect the parquet schema Repetition (columns stay Optional
    /// for O3 merge stability) — it only lets the encoder take a fast path that
    /// writes an all-ones RLE run for definition levels and skips null filtering
    /// when `column_top == 0`.
    pub not_null_hint: bool,
    pub designated_timestamp_ascending: bool,
    pub parquet_encoding_config: ParquetEncodingConfig,
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
        symbol_offsets_count: usize,
        designated_timestamp: bool,
        designated_timestamp_ascending: bool,
        parquet_encoding_config: i32,
    ) -> ParquetResult<Self> {
        if primary_data_ptr.is_null() && primary_data_size != 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "from_raw_data: null primary_data_ptr with non-zero size {}",
                primary_data_size
            ));
        }
        if secondary_data_ptr.is_null() && secondary_data_size != 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "from_raw_data: null secondary_data_ptr with non-zero size {}",
                secondary_data_size
            ));
        }
        if symbol_offsets_ptr.is_null() && symbol_offsets_count != 0 {
            return Err(fmt_err!(
                InvalidLayout,
                "from_raw_data: null symbol_offsets_ptr with non-zero count {}",
                symbol_offsets_count
            ));
        }

        let not_null_hint = column_type < 0;
        let column_type: ColumnType = (column_type & 0x7FFFFFFF).try_into()?;

        let primary_data = if primary_data_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer and length. Memory remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(primary_data_ptr, primary_data_size) }
        };
        let secondary_data = if secondary_data_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer and length. Memory remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(secondary_data_ptr, secondary_data_size) }
        };
        let symbol_offsets = if symbol_offsets_ptr.is_null() {
            &[]
        } else {
            // SAFETY: JNI caller guarantees a valid pointer and length. Memory remains valid for the JNI call duration.
            unsafe { slice::from_raw_parts(symbol_offsets_ptr, symbol_offsets_count) }
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
            not_null_hint,
            designated_timestamp_ascending,
            parquet_encoding_config: ParquetEncodingConfig::from_raw(parquet_encoding_config),
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
    squash_tracker: i64,
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
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is page-aligned.
            // The byte content represents valid values of the target type.
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
            not_null: column.not_null_hint,
        });
    }

    qdb_meta.squash_tracker = squash_tracker;

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
            if let Some(enc) = c.parquet_encoding_config.encoding() {
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
        .map(|c| c.parquet_encoding_config.compression())
        .collect()
}

/// Packed per-column parquet encoding config from Java.
///
/// Bit layout (i32):
/// - Bits 0-7: encoding id (matches ParquetEncoding.ENCODING_* on the Java side)
/// - Bits 8-15: compression codec id (matches ParquetCompression constants, offset +1)
/// - Bits 16-23: compression level
/// - Bit 24: explicit flag (1 = user-specified override, 0 = use defaults)
/// - Bit 25: bloom filter flag (1 = column should have a bloom filter)
#[derive(Clone, Copy, Debug, Default)]
pub struct ParquetEncodingConfig(i32);

const ENCODING_MASK: u32 = 0xFF;
const COMPRESSION_SHIFT: u32 = 8;
const COMPRESSION_MASK: u32 = 0xFF;
const LEVEL_SHIFT: u32 = 16;
const LEVEL_MASK: u32 = 0xFF;
const EXPLICIT_FLAG: u32 = 1 << 24;

impl ParquetEncodingConfig {
    /// Create a config from the raw packed i32 received from JNI.
    pub fn from_raw(raw: i32) -> Self {
        Self(raw)
    }

    /// Build an explicit config from encoding, compression, and semantic level.
    /// The level is packed with +1 offset (0 = not set sentinel, 1 = level 0, etc.),
    /// matching the Java packing convention. Pass -1 for "no level specified".
    #[cfg(test)]
    pub fn new(encoding: i32, compression: i32, level: i32) -> Self {
        let packed_level = if level >= 0 { (level + 1) as u32 } else { 0u32 };
        Self(
            ((encoding as u32 & ENCODING_MASK)
                | ((compression as u32 & COMPRESSION_MASK) << COMPRESSION_SHIFT)
                | ((packed_level & LEVEL_MASK) << LEVEL_SHIFT)
                | EXPLICIT_FLAG) as i32,
        )
    }

    /// Return the raw packed i32 value.
    pub fn raw(self) -> i32 {
        self.0
    }

    /// Whether the config was explicitly set by the user.
    pub fn is_explicit(self) -> bool {
        (self.0 as u32 & EXPLICIT_FLAG) != 0
    }

    /// Extract per-column encoding override.
    /// Returns None if the config uses default encoding (encoding byte == 0 or not explicitly set).
    pub fn encoding(self) -> Option<Encoding> {
        if !self.is_explicit() {
            return None;
        }
        let encoding_id = self.0 as u32 & ENCODING_MASK;
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

    /// Extract per-column compression override.
    /// Returns None if the config uses default/global compression.
    pub fn compression(self) -> Option<CompressionOptions> {
        if !self.is_explicit() {
            return None;
        }
        let codec_id = (self.0 as u32 >> COMPRESSION_SHIFT) & COMPRESSION_MASK;
        let level = (self.0 as u32 >> LEVEL_SHIFT) & LEVEL_MASK;
        match codec_id {
            0 => None, // use global default
            1 => Some(CompressionOptions::Uncompressed),
            2 => Some(CompressionOptions::Snappy),
            3 => Some(CompressionOptions::Gzip(
                parquet2::compression::GzipLevel::try_new(if level > 0 {
                    (level - 1) as u8
                } else {
                    6
                })
                .ok(),
            )),
            4 => Some(CompressionOptions::Brotli(
                parquet2::compression::BrotliLevel::try_new(if level > 0 { level - 1 } else { 1 })
                    .ok(),
            )),
            5 => Some(CompressionOptions::Zstd(
                parquet2::compression::ZstdLevel::try_new(if level > 0 {
                    (level - 1) as i32
                } else {
                    1
                })
                .ok(),
            )),
            6 => Some(CompressionOptions::Lz4Raw),
            _ => None,
        }
    }
}

impl From<i32> for ParquetEncodingConfig {
    fn from(raw: i32) -> Self {
        Self::from_raw(raw)
    }
}

pub(crate) fn encoding_map(data_type: ColumnType) -> Encoding {
    match data_type.tag() {
        ColumnTypeTag::Symbol => Encoding::RleDictionary,
        ColumnTypeTag::Binary | ColumnTypeTag::Varchar | ColumnTypeTag::String => {
            Encoding::DeltaLengthByteArray
        }
        _ => Encoding::Plain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoding_default() {
        assert_eq!(ParquetEncodingConfig::from_raw(0).encoding(), None);
    }

    #[test]
    fn test_encoding_not_explicit() {
        // encoding byte set but explicit flag not set
        assert_eq!(ParquetEncodingConfig::from_raw(1).encoding(), None);
    }

    #[test]
    fn test_encoding_plain() {
        assert_eq!(
            ParquetEncodingConfig::new(1, 0, -1).encoding(),
            Some(Encoding::Plain)
        );
    }

    #[test]
    fn test_encoding_rle_dictionary() {
        assert_eq!(
            ParquetEncodingConfig::new(2, 0, -1).encoding(),
            Some(Encoding::RleDictionary)
        );
    }

    #[test]
    fn test_encoding_delta_length_byte_array() {
        assert_eq!(
            ParquetEncodingConfig::new(3, 0, -1).encoding(),
            Some(Encoding::DeltaLengthByteArray)
        );
    }

    #[test]
    fn test_encoding_delta_binary_packed() {
        assert_eq!(
            ParquetEncodingConfig::new(4, 0, -1).encoding(),
            Some(Encoding::DeltaBinaryPacked)
        );
    }

    #[test]
    fn test_encoding_byte_stream_split() {
        assert_eq!(
            ParquetEncodingConfig::new(5, 0, -1).encoding(),
            Some(Encoding::ByteStreamSplit)
        );
    }

    #[test]
    fn test_encoding_zero_explicit() {
        // explicit flag set but encoding is 0 -> use default
        assert_eq!(ParquetEncodingConfig::new(0, 0, -1).encoding(), None);
    }

    #[test]
    fn test_encoding_unknown_id() {
        assert_eq!(ParquetEncodingConfig::new(99, 0, -1).encoding(), None);
    }

    #[test]
    fn test_compression_default() {
        assert_eq!(ParquetEncodingConfig::from_raw(0).compression(), None);
    }

    #[test]
    fn test_compression_not_explicit() {
        assert_eq!(ParquetEncodingConfig::from_raw(1 << 8).compression(), None);
    }

    #[test]
    fn test_compression_uncompressed() {
        let c = ParquetEncodingConfig::new(0, 1, -1).compression();
        assert_eq!(c, Some(CompressionOptions::Uncompressed));
    }

    #[test]
    fn test_compression_snappy() {
        let c = ParquetEncodingConfig::new(0, 2, -1).compression();
        assert_eq!(c, Some(CompressionOptions::Snappy));
    }

    #[test]
    fn test_compression_lz4_raw() {
        let c = ParquetEncodingConfig::new(0, 6, -1).compression();
        assert_eq!(c, Some(CompressionOptions::Lz4Raw));
    }

    #[test]
    fn test_compression_zstd_default_level() {
        let c = ParquetEncodingConfig::new(0, 5, -1).compression();
        match c {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_zstd_custom_level() {
        let c = ParquetEncodingConfig::new(0, 5, 3).compression();
        match c {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_gzip() {
        let c = ParquetEncodingConfig::new(0, 3, -1).compression();
        match c {
            Some(CompressionOptions::Gzip(_)) => {}
            other => panic!("expected Gzip, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_brotli() {
        let c = ParquetEncodingConfig::new(0, 4, -1).compression();
        match c {
            Some(CompressionOptions::Brotli(_)) => {}
            other => panic!("expected Brotli, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_gzip_level_zero() {
        // level 0 = "store without compression", must not become default (6)
        let c = ParquetEncodingConfig::new(0, 3, 0).compression();
        match c {
            Some(CompressionOptions::Gzip(Some(level))) => {
                assert_eq!(level, parquet2::compression::GzipLevel::try_new(0).unwrap());
            }
            other => panic!("expected Gzip(Some(GzipLevel(0))), got {:?}", other),
        }
    }

    #[test]
    fn test_compression_brotli_level_zero() {
        // level 0 is a valid Brotli level, must not become default (1)
        let c = ParquetEncodingConfig::new(0, 4, 0).compression();
        match c {
            Some(CompressionOptions::Brotli(Some(level))) => {
                assert_eq!(
                    level,
                    parquet2::compression::BrotliLevel::try_new(0).unwrap()
                );
            }
            other => panic!("expected Brotli(Some(BrotliLevel(0))), got {:?}", other),
        }
    }

    #[test]
    fn test_compression_unknown_codec() {
        assert_eq!(ParquetEncodingConfig::new(0, 99, -1).compression(), None);
    }

    #[test]
    fn test_combined_encoding_and_compression() {
        let config = ParquetEncodingConfig::new(5, 5, 3); // BYTE_STREAM_SPLIT + ZSTD level 3
        assert_eq!(config.encoding(), Some(Encoding::ByteStreamSplit));
        match config.compression() {
            Some(CompressionOptions::Zstd(_)) => {}
            other => panic!("expected Zstd, got {:?}", other),
        }
    }

    #[test]
    fn test_compression_codec_zero_explicit() {
        // explicit flag set, compression codec is 0 -> use global default
        assert_eq!(ParquetEncodingConfig::new(1, 0, -1).compression(), None);
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
        // VARCHAR should fall back to DeltaLengthByteArray
        assert_eq!(
            validate_encoding(col_type(ColumnTypeTag::Varchar), Encoding::Plain),
            Encoding::DeltaLengthByteArray
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
