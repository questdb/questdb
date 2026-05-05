/// Incremental footer serialization for Parquet files.
///
/// When updating a Parquet file, most row groups remain unchanged. This module
/// captures the raw Thrift-serialized bytes of each row group from the original
/// footer and allows `end()` to write unchanged row groups as raw bytes instead
/// of re-serializing them, which is orders of magnitude faster for files with
/// thousands of row groups.
use crate::error::{Error, Result};

/// Cached footer data from the original Parquet file.
/// Stores the raw Thrift-serialized bytes and the byte offsets of each
/// individual RowGroup entry within those bytes.
pub struct FooterCache {
    /// Raw Thrift-serialized footer bytes (the metadata portion, not the
    /// trailing 8-byte length+magic).
    raw_bytes: Vec<u8>,
    /// Byte offsets `(start, end)` within `raw_bytes` for each RowGroup struct.
    /// These delimit the complete Thrift-serialized bytes for each row group.
    row_group_offsets: Vec<(usize, usize)>,
}

impl FooterCache {
    /// Returns the raw Thrift-serialized bytes for row group at the given index.
    pub fn row_group_bytes(&self, index: usize) -> &[u8] {
        let (start, end) = self.row_group_offsets[index];
        &self.raw_bytes[start..end]
    }

    /// Returns the number of row groups in the cache.
    pub fn row_group_count(&self) -> usize {
        self.row_group_offsets.len()
    }

    /// Scans the raw Thrift-serialized footer bytes to find the byte boundaries
    /// of each RowGroup entry within the `row_groups` list (field 4 of
    /// FileMetaData).
    ///
    /// Uses a minimal Thrift compact protocol parser that skips over struct
    /// fields until it finds the row_groups list, then records the start/end
    /// position of each element.
    pub fn from_footer_bytes(raw_bytes: Vec<u8>) -> Result<Self> {
        let offsets = scan_row_group_offsets(&raw_bytes)?;
        Ok(FooterCache {
            raw_bytes,
            row_group_offsets: offsets,
        })
    }
}

/// Scans a Thrift compact protocol-serialized FileMetaData to find the byte
/// offsets of each RowGroup entry in the `row_groups` list (field 4).
fn scan_row_group_offsets(data: &[u8]) -> Result<Vec<(usize, usize)>> {
    let mut pos = 0;
    let mut prev_field_id: i16 = 0;

    // Parse FileMetaData struct fields
    loop {
        if pos >= data.len() {
            return Err(Error::oos(
                "Unexpected end of footer while scanning for row_groups",
            ));
        }

        let byte = data[pos];
        pos += 1;

        if byte == 0 {
            // STOP - end of struct, didn't find row_groups
            break;
        }

        let field_type = byte & 0x0F;
        let delta = (byte >> 4) & 0x0F;
        let field_id = if delta != 0 {
            prev_field_id + delta as i16
        } else {
            let (val, consumed) = read_varint(&data[pos..])?;
            pos += consumed;
            zigzag_decode_i16(val)
        };
        prev_field_id = field_id;

        if field_id == 4 {
            // row_groups: list<RowGroup>
            // field_type should be 12 (LIST) in compact encoding = type 12
            // But the field type for list in compact protocol header is 12
            let (count, _elem_type, consumed) = read_list_header(&data[pos..])?;
            pos += consumed;

            let mut offsets = Vec::with_capacity(count);
            for _ in 0..count {
                let start = pos;
                pos = skip_thrift_struct(data, pos)?;
                offsets.push((start, pos));
            }
            return Ok(offsets);
        } else {
            pos = skip_thrift_value(data, pos, field_type)?;
        }
    }

    Ok(vec![])
}

// --- Thrift compact protocol primitives ---

/// Reads a varint (unsigned, variable-length integer).
/// Returns (value, bytes_consumed).
fn read_varint(data: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for i in 0..10 {
        if i >= data.len() {
            return Err(Error::oos("Unexpected end of data reading varint"));
        }
        let byte = data[i] as u64;
        result |= (byte & 0x7F) << shift;
        shift += 7;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
    }
    Err(Error::oos("Varint too long"))
}

/// Zigzag-decodes a varint value to a signed i16.
fn zigzag_decode_i16(val: u64) -> i16 {
    ((val >> 1) as i16) ^ (-((val & 1) as i16))
}

/// Reads a Thrift compact protocol list/set header.
/// Returns (element_count, element_type, bytes_consumed).
fn read_list_header(data: &[u8]) -> Result<(usize, u8, usize)> {
    if data.is_empty() {
        return Err(Error::oos("Unexpected end of data reading list header"));
    }
    let byte = data[0];
    let size_and_type = byte;
    let elem_type = size_and_type & 0x0F;
    let size_high = (size_and_type >> 4) & 0x0F;

    if size_high == 0x0F {
        // Large list: size as varint follows
        let (size, consumed) = read_varint(&data[1..])?;
        Ok((size as usize, elem_type, 1 + consumed))
    } else {
        Ok((size_high as usize, elem_type, 1))
    }
}

/// Skips over a complete Thrift compact protocol struct (including nested structs).
/// Returns the new position after the struct's STOP byte.
fn skip_thrift_struct(data: &[u8], mut pos: usize) -> Result<usize> {
    let mut _prev_field_id: i16 = 0;
    loop {
        if pos >= data.len() {
            return Err(Error::oos("Unexpected end of data skipping struct"));
        }
        let byte = data[pos];
        pos += 1;

        if byte == 0 {
            return Ok(pos); // STOP byte
        }

        let field_type = byte & 0x0F;
        let delta = (byte >> 4) & 0x0F;
        if delta != 0 {
            _prev_field_id += delta as i16;
        } else {
            let (val, consumed) = read_varint(&data[pos..])?;
            pos += consumed;
            _prev_field_id = zigzag_decode_i16(val);
        }

        pos = skip_thrift_value(data, pos, field_type)?;
    }
}

/// Skips over a single Thrift compact protocol value of the given type.
/// Returns the new position after the value.
fn skip_thrift_value(data: &[u8], pos: usize, type_id: u8) -> Result<usize> {
    match type_id {
        1 | 2 => Ok(pos), // BOOLEAN_TRUE / BOOLEAN_FALSE
        3 => Ok(pos + 1), // I8 (BYTE)
        4 | 5 | 6 => {
            // I16, I32, I64 (varint)
            let (_, consumed) = read_varint(&data[pos..])?;
            Ok(pos + consumed)
        }
        7 => Ok(pos + 8), // DOUBLE (fixed 8 bytes)
        8 => {
            // BINARY (varint length + bytes)
            let (len, consumed) = read_varint(&data[pos..])?;
            Ok(pos + consumed + len as usize)
        }
        9 | 10 => {
            // LIST / SET
            let (count, elem_type, consumed) = read_list_header(&data[pos..])?;
            let mut p = pos + consumed;
            for _ in 0..count {
                p = skip_thrift_value(data, p, elem_type)?;
            }
            Ok(p)
        }
        11 => {
            // MAP
            let (size, consumed) = read_varint(&data[pos..])?;
            let mut p = pos + consumed;
            if size > 0 {
                if p >= data.len() {
                    return Err(Error::oos("Unexpected end of data reading map types"));
                }
                let types = data[p];
                p += 1;
                let key_type = (types >> 4) & 0x0F;
                let val_type = types & 0x0F;
                for _ in 0..size {
                    p = skip_thrift_value(data, p, key_type)?;
                    p = skip_thrift_value(data, p, val_type)?;
                }
            }
            Ok(p)
        }
        12 => {
            // STRUCT
            skip_thrift_struct(data, pos)
        }
        _ => Err(Error::oos(format!(
            "Unknown Thrift compact type: {type_id}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet_format_safe::thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
    use parquet_format_safe::{
        ColumnChunk, ColumnMetaData, CompressionCodec, Encoding, FieldRepetitionType, FileMetaData,
        KeyValue, RowGroup, SchemaElement, Statistics, Type,
    };

    /// Helper: creates a root SchemaElement with the given number of children.
    fn root_schema(name: &str, num_children: i32) -> SchemaElement {
        SchemaElement::new(
            None::<Type>,                               // type_
            None::<i32>,                                // type_length
            None::<FieldRepetitionType>,                // repetition_type
            name.to_string(),                           // name
            num_children,                               // num_children
            None::<parquet_format_safe::ConvertedType>, // converted_type
            None::<i32>,                                // scale
            None::<i32>,                                // precision
            None::<i32>,                                // field_id
            None::<parquet_format_safe::LogicalType>,   // logical_type
        )
    }

    /// Helper: creates a leaf SchemaElement for a column.
    fn leaf_schema(name: &str, type_: Type) -> SchemaElement {
        SchemaElement::new(
            type_,                         // type_
            None::<i32>,                   // type_length
            FieldRepetitionType::OPTIONAL, // repetition_type
            name.to_string(),              // name
            None::<i32>,                   // num_children (leaf)
            None::<parquet_format_safe::ConvertedType>,
            None::<i32>,
            None::<i32>,
            None::<i32>,
            None::<parquet_format_safe::LogicalType>,
        )
    }

    /// Helper: creates a simple RowGroup with no columns.
    fn empty_row_group(num_rows: i64, ordinal: i16) -> RowGroup {
        RowGroup::new(
            vec![],   // columns
            100,      // total_byte_size
            num_rows, // num_rows
            None::<Vec<parquet_format_safe::SortingColumn>>,
            None::<i64>, // file_offset
            None::<i64>, // total_compressed_size
            ordinal,     // ordinal
        )
    }

    /// Helper: creates a ColumnChunk with realistic ColumnMetaData.
    fn make_column_chunk(col_name: &str, type_: Type, num_values: i64) -> ColumnChunk {
        let meta = ColumnMetaData::new(
            type_,
            vec![Encoding::PLAIN, Encoding::RLE],
            vec![col_name.to_string()],
            CompressionCodec::SNAPPY,
            num_values,
            num_values * 8, // total_uncompressed_size
            num_values * 6, // total_compressed_size
            None::<Vec<KeyValue>>,
            0,                  // data_page_offset
            None::<i64>,        // index_page_offset
            None::<i64>,        // dictionary_page_offset
            None::<Statistics>, // statistics
            None::<Vec<parquet_format_safe::PageEncodingStats>>,
            None::<i64>, // bloom_filter_offset
            None::<i32>, // bloom_filter_length
        );
        ColumnChunk::new(
            None::<String>, // file_path
            0,              // file_offset
            meta,           // meta_data
            None::<i64>,    // offset_index_offset
            None::<i32>,    // offset_index_length
            None::<i64>,    // column_index_offset
            None::<i32>,    // column_index_length
            None::<parquet_format_safe::ColumnCryptoMetaData>,
            None::<Vec<u8>>, // encrypted_column_metadata
        )
    }

    /// Serializes FileMetaData to Thrift compact protocol bytes.
    fn serialize(metadata: &FileMetaData) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut prot = TCompactOutputProtocol::new(&mut buf);
        metadata.write_to_out_protocol(&mut prot).unwrap();
        buf
    }

    /// Deserializes a RowGroup from Thrift compact protocol bytes.
    fn deserialize_row_group(bytes: &[u8]) -> RowGroup {
        let mut cursor = std::io::Cursor::new(bytes);
        let mut prot = TCompactInputProtocol::new(&mut cursor, usize::MAX);
        RowGroup::read_from_in_protocol(&mut prot).unwrap()
    }

    // ---------------------------------------------------------------
    // 1. Original test, fixed to use correct constructors
    // ---------------------------------------------------------------

    #[test]
    fn test_scan_finds_row_group_boundaries() {
        let rgs: Vec<RowGroup> = (0..3)
            .map(|i| empty_row_group(1000 + i, i as i16))
            .collect();

        let metadata = FileMetaData::new(
            1,
            vec![root_schema("root", 0)],
            3000,
            rgs,
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 3);

        for i in 0..3 {
            let bytes = cache.row_group_bytes(i);
            assert!(!bytes.is_empty(), "row group {i} should have bytes");
        }
    }

    // ---------------------------------------------------------------
    // 2. Row groups with actual ColumnChunk/ColumnMetaData (nested structs)
    // ---------------------------------------------------------------

    #[test]
    fn test_row_groups_with_column_chunks() {
        let mut rgs = vec![];
        for i in 0..3 {
            let cols = vec![
                make_column_chunk("timestamp", Type::INT64, 1000),
                make_column_chunk("price", Type::DOUBLE, 1000),
                make_column_chunk("symbol", Type::BYTE_ARRAY, 1000),
            ];
            rgs.push(RowGroup::new(
                cols,
                24_000,
                1000,
                None::<Vec<parquet_format_safe::SortingColumn>>,
                None::<i64>,
                Some(18_000i64),
                i as i16,
            ));
        }

        let schema = vec![
            root_schema("trades", 3),
            leaf_schema("timestamp", Type::INT64),
            leaf_schema("price", Type::DOUBLE),
            leaf_schema("symbol", Type::BYTE_ARRAY),
        ];

        let metadata = FileMetaData::new(
            2,
            schema,
            3000,
            rgs,
            None::<Vec<KeyValue>>,
            Some("questdb".to_string()),
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 3);

        // Verify each cached row group deserializes back to a valid RowGroup
        // with the correct number of columns.
        for i in 0..3 {
            let rg = deserialize_row_group(cache.row_group_bytes(i));
            assert_eq!(rg.columns.len(), 3, "row group {i} should have 3 columns");
            assert_eq!(rg.num_rows, 1000);
            let meta = rg.columns[0].meta_data.as_ref().unwrap();
            assert_eq!(meta.type_, Type::INT64);
        }
    }

    // ---------------------------------------------------------------
    // 3. Column metadata with Statistics (deeper nesting)
    // ---------------------------------------------------------------

    #[test]
    fn test_row_groups_with_statistics() {
        let stats = Statistics::new(
            Some(vec![0xFF, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), // max
            Some(vec![0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), // min
            Some(42i64),                                                // null_count
            Some(999i64),                                               // distinct_count
            None::<Vec<u8>>,                                            // max_value
            None::<Vec<u8>>,                                            // min_value
        );

        let meta = ColumnMetaData::new(
            Type::INT64,
            vec![Encoding::PLAIN, Encoding::RLE],
            vec!["value".to_string()],
            CompressionCodec::ZSTD,
            5000,
            40_000,
            30_000,
            None::<Vec<KeyValue>>,
            0,
            None::<i64>,
            None::<i64>,
            stats,
            None::<Vec<parquet_format_safe::PageEncodingStats>>,
            None::<i64>,
            None::<i32>,
        );
        let cc = ColumnChunk::new(
            None::<String>,
            0,
            meta,
            None::<i64>,
            None::<i32>,
            None::<i64>,
            None::<i32>,
            None::<parquet_format_safe::ColumnCryptoMetaData>,
            None::<Vec<u8>>,
        );

        let rg = RowGroup::new(
            vec![cc],
            40_000,
            5000,
            None::<Vec<parquet_format_safe::SortingColumn>>,
            None::<i64>,
            Some(30_000i64),
            0i16,
        );

        let metadata = FileMetaData::new(
            2,
            vec![
                root_schema("stats_table", 1),
                leaf_schema("value", Type::INT64),
            ],
            5000,
            vec![rg],
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 1);

        let rg = deserialize_row_group(cache.row_group_bytes(0));
        let stats = rg.columns[0]
            .meta_data
            .as_ref()
            .unwrap()
            .statistics
            .as_ref()
            .unwrap();
        assert_eq!(stats.null_count, Some(42));
        assert_eq!(stats.distinct_count, Some(999));
    }

    // ---------------------------------------------------------------
    // 4. Key-value metadata (MAP field) before the row_groups list
    // ---------------------------------------------------------------

    #[test]
    fn test_key_value_metadata_before_row_groups() {
        let kvs = vec![
            KeyValue::new("questdb.meta".to_string(), Some("binary_blob".to_string())),
            KeyValue::new("created_by".to_string(), Some("questdb-test".to_string())),
            KeyValue::new(
                "long_value".to_string(),
                Some("x".repeat(300)), // value longer than 255 to exercise varint in BINARY
            ),
        ];

        let rgs: Vec<RowGroup> = (0..2).map(|i| empty_row_group(500, i as i16)).collect();

        let metadata = FileMetaData::new(
            1,
            vec![root_schema("kv_test", 0)],
            1000,
            rgs,
            kvs, // key_value_metadata is field 5 which comes AFTER row_groups (field 4)
            Some("questdb-test-suite".to_string()),
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 2);

        for i in 0..2 {
            let rg = deserialize_row_group(cache.row_group_bytes(i));
            assert_eq!(rg.num_rows, 500);
        }
    }

    // ---------------------------------------------------------------
    // 5. Large list: >= 15 row groups (exercises the large-list varint path)
    // ---------------------------------------------------------------

    #[test]
    fn test_large_list_many_row_groups() {
        // Thrift compact protocol encodes list size inline (4 bits) for size < 15,
        // and as a separate varint for size >= 15. 20 row groups exercises that path.
        let count = 20;
        let rgs: Vec<RowGroup> = (0..count)
            .map(|i| {
                let cols = vec![make_column_chunk("ts", Type::INT64, 100)];
                RowGroup::new(
                    cols,
                    800,
                    100,
                    None::<Vec<parquet_format_safe::SortingColumn>>,
                    None::<i64>,
                    Some(600i64),
                    i as i16,
                )
            })
            .collect();

        let metadata = FileMetaData::new(
            1,
            vec![root_schema("big", 1), leaf_schema("ts", Type::INT64)],
            count as i64 * 100,
            rgs,
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), count);

        for i in 0..count {
            let rg = deserialize_row_group(cache.row_group_bytes(i));
            assert_eq!(rg.num_rows, 100, "row group {i}");
            assert_eq!(rg.ordinal, Some(i as i16), "row group {i} ordinal");
        }
    }

    // ---------------------------------------------------------------
    // 6. Large field IDs: delta=0 case in field header
    // ---------------------------------------------------------------

    #[test]
    fn test_large_field_id_delta_zero() {
        // The Thrift compact protocol uses delta encoding for field IDs.
        // When the delta exceeds 15, it falls back to writing the full field ID
        // as a zigzag-encoded varint (the delta=0 case).
        //
        // We hand-craft a valid FileMetaData payload where the RowGroup struct
        // contains a delta=0 field header to exercise that code path in
        // skip_thrift_struct.
        //
        // Compact protocol type IDs:
        //   4=I16, 5=I32, 6=I64, 8=BINARY, 9=LIST, 12=STRUCT
        let mut data = Vec::new();

        // FileMetaData field 1 (version, I32): delta=1, type=5
        data.push(0x15); // (1 << 4) | 5
        data.push(0x02); // zigzag(1) = 2

        // FileMetaData field 2 (schema, list<SchemaElement>): delta=1, type=9 (LIST)
        data.push(0x19); // (1 << 4) | 9
                         // List header: 1 element, elem_type=12 (STRUCT)
        data.push(0x1C); // (1 << 4) | 12
                         // Minimal SchemaElement: just field 4 (name), then STOP
        data.push(0x48); // field 4, delta=4, type=8 (BINARY)
        data.push(0x04); // varint: string length = 4
        data.extend_from_slice(b"root");
        data.push(0x00); // SchemaElement STOP

        // FileMetaData field 3 (num_rows, I64): delta=1, type=6
        data.push(0x16); // (1 << 4) | 6
        data.push(0x00); // zigzag(0)

        // FileMetaData field 4 (row_groups, list<RowGroup>): delta=1, type=9 (LIST)
        data.push(0x19); // (1 << 4) | 9
                         // List header: 1 element, elem_type=12 (STRUCT)
        data.push(0x1C); // (1 << 4) | 12

        // --- RowGroup struct ---
        // Field 1 (columns, list<ColumnChunk>): delta=1, type=9 (LIST)
        data.push(0x19); // (1 << 4) | 9
                         // Empty list: 0 elements, elem_type=12 (STRUCT)
        data.push(0x0C); // (0 << 4) | 12

        // Field 2 (total_byte_size, I64): delta=1, type=6
        data.push(0x16); // (1 << 4) | 6
        data.extend_from_slice(&[0xC8, 0x01]); // zigzag(100) = 200, varint

        // Field 3 (num_rows, I64): delta=1, type=6
        data.push(0x16); // (1 << 4) | 6
        data.extend_from_slice(&[0xD0, 0x0F]); // zigzag(1000) = 2000, varint

        // Field 7 (ordinal, I16) via delta=0 (force the non-delta path):
        // Header: (0 << 4) | 4 = 0x04  (delta=0, type=I16)
        data.push(0x04);
        // Field ID as zigzag varint: zigzag(7) = 14 = 0x0E
        data.push(0x0E);
        // Value: zigzag(42) = 84 = 0x54
        data.push(0x54);

        // RowGroup STOP
        data.push(0x00);

        // FileMetaData STOP
        data.push(0x00);

        let cache = FooterCache::from_footer_bytes(data).unwrap();
        assert_eq!(cache.row_group_count(), 1);

        let rg = deserialize_row_group(cache.row_group_bytes(0));
        assert_eq!(rg.num_rows, 1000);
        assert_eq!(rg.ordinal, Some(42));
    }

    // ---------------------------------------------------------------
    // 7. Column-level key_value_metadata (MAP within ColumnMetaData)
    // ---------------------------------------------------------------

    #[test]
    fn test_column_metadata_with_key_value() {
        let col_kvs = vec![
            KeyValue::new("encoding.hint".to_string(), Some("dict".to_string())),
            KeyValue::new("custom.meta".to_string(), Some("value123".to_string())),
        ];

        let meta = ColumnMetaData::new(
            Type::BYTE_ARRAY,
            vec![Encoding::PLAIN, Encoding::RLE_DICTIONARY],
            vec!["name".to_string()],
            CompressionCodec::GZIP,
            2000,
            16_000,
            8_000,
            col_kvs, // key_value_metadata on the column
            0,
            None::<i64>,
            Some(100i64), // dictionary_page_offset
            None::<Statistics>,
            None::<Vec<parquet_format_safe::PageEncodingStats>>,
            None::<i64>,
            None::<i32>,
        );
        let cc = ColumnChunk::new(
            None::<String>,
            0,
            meta,
            None::<i64>,
            None::<i32>,
            None::<i64>,
            None::<i32>,
            None::<parquet_format_safe::ColumnCryptoMetaData>,
            None::<Vec<u8>>,
        );

        let rg = RowGroup::new(
            vec![cc],
            16_000,
            2000,
            None::<Vec<parquet_format_safe::SortingColumn>>,
            None::<i64>,
            Some(8_000i64),
            0i16,
        );

        let metadata = FileMetaData::new(
            2,
            vec![
                root_schema("kv_col_test", 1),
                leaf_schema("name", Type::BYTE_ARRAY),
            ],
            2000,
            vec![rg],
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 1);

        let rg = deserialize_row_group(cache.row_group_bytes(0));
        let meta = rg.columns[0].meta_data.as_ref().unwrap();
        let kvs = meta.key_value_metadata.as_ref().unwrap();
        assert_eq!(kvs.len(), 2);
        assert_eq!(kvs[0].key, "encoding.hint");
        assert_eq!(kvs[1].value, Some("value123".to_string()));
    }

    // ---------------------------------------------------------------
    // 8. Zero row groups
    // ---------------------------------------------------------------

    #[test]
    fn test_zero_row_groups() {
        let metadata = FileMetaData::new(
            1,
            vec![root_schema("empty", 0)],
            0,
            vec![], // no row groups
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 0);
    }

    // ---------------------------------------------------------------
    // 9. Round-trip fidelity: cached bytes re-serialize identically
    // ---------------------------------------------------------------

    #[test]
    fn test_cached_bytes_match_original_serialization() {
        // Serialize a non-trivial row group, then verify that the cached bytes
        // are byte-for-byte identical to what TCompactOutputProtocol produces
        // for each individual RowGroup.
        let rgs: Vec<RowGroup> = (0..3)
            .map(|i| {
                let cols = vec![
                    make_column_chunk("ts", Type::INT64, 500),
                    make_column_chunk("val", Type::DOUBLE, 500),
                ];
                RowGroup::new(
                    cols,
                    8000,
                    500,
                    None::<Vec<parquet_format_safe::SortingColumn>>,
                    Some(i as i64 * 8000),
                    Some(6000i64),
                    i as i16,
                )
            })
            .collect();

        // Serialize each RowGroup individually for reference
        let mut reference_bytes: Vec<Vec<u8>> = vec![];
        for rg in &rgs {
            let mut buf = Vec::new();
            let mut prot = TCompactOutputProtocol::new(&mut buf);
            rg.write_to_out_protocol(&mut prot).unwrap();
            reference_bytes.push(buf);
        }

        let metadata = FileMetaData::new(
            2,
            vec![
                root_schema("fidelity_test", 2),
                leaf_schema("ts", Type::INT64),
                leaf_schema("val", Type::DOUBLE),
            ],
            1500,
            rgs,
            None::<Vec<KeyValue>>,
            Some("questdb".to_string()),
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 3);

        for i in 0..3 {
            assert_eq!(
                cache.row_group_bytes(i),
                &reference_bytes[i][..],
                "row group {i} cached bytes differ from individual serialization"
            );
        }
    }

    // ---------------------------------------------------------------
    // 10. Truncated footer (error handling)
    // ---------------------------------------------------------------

    #[test]
    fn test_truncated_footer_returns_error() {
        let rg = RowGroup::new(
            vec![make_column_chunk("x", Type::INT32, 10)],
            80,
            10,
            None::<Vec<parquet_format_safe::SortingColumn>>,
            None::<i64>,
            None::<i64>,
            0i16,
        );
        let metadata = FileMetaData::new(
            1,
            vec![root_schema("trunc", 1), leaf_schema("x", Type::INT32)],
            10,
            vec![rg],
            None::<Vec<KeyValue>>,
            None::<String>,
            None::<Vec<parquet_format_safe::ColumnOrder>>,
            None::<parquet_format_safe::EncryptionAlgorithm>,
            None::<Vec<u8>>,
        );

        let buf = serialize(&metadata);
        // Truncate to half the bytes
        let truncated = buf[..buf.len() / 2].to_vec();
        let result = FooterCache::from_footer_bytes(truncated);
        assert!(result.is_err(), "truncated footer should produce an error");
    }

    // ---------------------------------------------------------------
    // 11. Varint edge cases via unit tests on primitives
    // ---------------------------------------------------------------

    #[test]
    fn test_read_varint_single_byte() {
        let data = [0x05]; // 5, no continuation
        let (val, consumed) = read_varint(&data).unwrap();
        assert_eq!(val, 5);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_read_varint_multi_byte() {
        // Encode 300: 300 = 0b100101100
        // byte 0: 0b10101100 = 0xAC (continuation set)
        // byte 1: 0b00000010 = 0x02
        let data = [0xAC, 0x02];
        let (val, consumed) = read_varint(&data).unwrap();
        assert_eq!(val, 300);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_read_varint_large_value() {
        // Encode 2^32 = 4294967296
        // This needs 5 bytes in varint encoding.
        let n: u64 = 1 << 32;
        let mut data = Vec::new();
        let mut v = n;
        loop {
            let mut byte = (v & 0x7F) as u8;
            v >>= 7;
            if v > 0 {
                byte |= 0x80;
            }
            data.push(byte);
            if v == 0 {
                break;
            }
        }
        let (val, consumed) = read_varint(&data).unwrap();
        assert_eq!(val, n);
        assert_eq!(consumed, data.len());
    }

    #[test]
    fn test_read_varint_empty_data() {
        let result = read_varint(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_zigzag_decode() {
        assert_eq!(zigzag_decode_i16(0), 0);
        assert_eq!(zigzag_decode_i16(1), -1);
        assert_eq!(zigzag_decode_i16(2), 1);
        assert_eq!(zigzag_decode_i16(3), -2);
        assert_eq!(zigzag_decode_i16(4), 2);
        // Field ID 4 -> zigzag(4) = 8, decode: 8 -> 4
        assert_eq!(zigzag_decode_i16(8), 4);
    }

    #[test]
    fn test_read_list_header_small() {
        // size=3, elem_type=12 (STRUCT) -> (3 << 4) | 12 = 0x3C
        let data = [0x3C];
        let (count, elem_type, consumed) = read_list_header(&data).unwrap();
        assert_eq!(count, 3);
        assert_eq!(elem_type, 12);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn test_read_list_header_large() {
        // size >= 15: header byte = (0xF << 4) | elem_type, then varint size
        // elem_type=12, size=20
        // header: (15 << 4) | 12 = 0xFC
        // varint(20) = 0x14
        let data = [0xFC, 0x14];
        let (count, elem_type, consumed) = read_list_header(&data).unwrap();
        assert_eq!(count, 20);
        assert_eq!(elem_type, 12);
        assert_eq!(consumed, 2);
    }

    #[test]
    fn test_read_list_header_large_varint_size() {
        // size=300, elem_type=12
        // header: 0xFC
        // varint(300) = [0xAC, 0x02]
        let data = [0xFC, 0xAC, 0x02];
        let (count, elem_type, consumed) = read_list_header(&data).unwrap();
        assert_eq!(count, 300);
        assert_eq!(elem_type, 12);
        assert_eq!(consumed, 3);
    }
}
