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
    use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
    use parquet_format_safe::FileMetaData;

    #[test]
    fn test_scan_finds_row_group_boundaries() {
        // Create a minimal FileMetaData with 3 row groups and serialize it.
        let mut rgs = vec![];
        for i in 0..3 {
            let mut rg = parquet_format_safe::RowGroup::new(vec![], 1000 + i, i as i64);
            rg.ordinal = Some(i as i16);
            rgs.push(rg);
        }

        let metadata = FileMetaData::new(
            1, // version
            vec![parquet_format_safe::SchemaElement::new(
                "root".to_string(),
                0,
            )],
            3, // num_rows
            rgs,
            None,
            None,
            None,
            None,
            None,
        );

        // Serialize to bytes
        let mut buf = Vec::new();
        {
            let mut prot = TCompactOutputProtocol::new(&mut buf);
            metadata.write_to_out_protocol(&mut prot).unwrap();
        }

        let cache = FooterCache::from_footer_bytes(buf).unwrap();
        assert_eq!(cache.row_group_count(), 3);

        // Each row group should have non-zero bytes
        for i in 0..3 {
            let bytes = cache.row_group_bytes(i);
            assert!(!bytes.is_empty(), "row group {i} should have bytes");
        }
    }
}
