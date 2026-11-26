/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/
//! Sortable dedup-key bytes, using the same physical comparisons as WAL apply.
//!
//! WAL compares fixed-width values as signed integer containers (including float
//! bit patterns), with every FLOAT or DOUBLE NaN as the one NaN, which SQL reads as
//! NULL. Variable values compare length first, then their native bytes.
//! In particular this is not SQL's lexical STRING/VARCHAR ordering.
//!
//! The `_pm` footer stores the first and last dedup key of a row group in this
//! encoding: the non-timestamp keys in column id order, one after another.

use crate::col_type::{ColumnType, ColumnTypeTag, nulls};
use crate::error::{CoreResult, fmt_err};

/// Appends the key bytes of `row` of a column in its native layout: `data`, and `aux` for a
/// variable type.
pub fn append_key_from(
    out: &mut Vec<u8>,
    ty: ColumnType,
    data: &[u8],
    aux: Option<&[u8]>,
    row: usize,
) -> CoreResult<()> {
    if let Some(width) = ty.tag().fixed_size() {
        let bytes = range(data, row.checked_mul(width), width)?;
        append_fixed(out, ty, bytes);
        return Ok(());
    }
    let aux = aux.ok_or_else(|| fmt_err!(InvalidLayout, "dedup key has no auxiliary buffer"))?;
    let (length, bytes) = match ty.tag() {
        ColumnTypeTag::Varchar => {
            let entry = range(aux, row.checked_mul(16), 16)?;
            let header = u32::from_le_bytes([entry[0], entry[1], entry[2], entry[3]]);
            if header & 4 != 0 {
                (-1, &[][..])
            } else if header & 1 != 0 {
                let length = (entry[0] >> 4) as usize;
                if length > 9 {
                    return Err(fmt_err!(
                        InvalidLayout,
                        "invalid inline dedup VARCHAR length {length}"
                    ));
                }
                (length as i64, &entry[1..1 + length])
            } else {
                let length = (header >> 4) as usize;
                let mut offset = [0u8; 8];
                offset.copy_from_slice(&entry[8..]);
                let offset = u64::from_le_bytes(offset) >> 16;
                (
                    length as i64,
                    range(data, usize::try_from(offset).ok(), length)?,
                )
            }
        }
        ColumnTypeTag::String | ColumnTypeTag::Binary => {
            let mut offset = [0u8; 8];
            offset.copy_from_slice(range(aux, row.checked_mul(8), 8)?);
            let offset = usize::try_from(u64::from_le_bytes(offset))
                .map_err(|_| fmt_err!(InvalidLayout, "dedup key offset out of range"))?;
            let (header_bytes, item_bytes) = if ty.tag() == ColumnTypeTag::String {
                (4, 2)
            } else {
                (8, 1)
            };
            let header = range(data, Some(offset), header_bytes)?;
            let length = if header_bytes == 4 {
                i32::from_le_bytes([header[0], header[1], header[2], header[3]]) as i64
            } else {
                let mut length = [0u8; 8];
                length.copy_from_slice(header);
                i64::from_le_bytes(length)
            };
            if length < -1 {
                return Err(fmt_err!(
                    InvalidLayout,
                    "invalid variable dedup key length {length}"
                ));
            }
            let bytes = usize::try_from(length.max(0))
                .ok()
                .and_then(|n| n.checked_mul(item_bytes))
                .ok_or_else(|| fmt_err!(InvalidLayout, "variable dedup key length overflow"))?;
            (
                length,
                range(data, offset.checked_add(header_bytes), bytes)?,
            )
        }
        _ => {
            return Err(fmt_err!(
                InvalidType,
                "unsupported dedup key type {}",
                ty.code()
            ));
        }
    };
    append_variable(out, length, bytes);
    Ok(())
}

/// Appends the key bytes of a NULL, as a column top reads.
pub fn append_null_key(out: &mut Vec<u8>, ty: ColumnType) -> CoreResult<()> {
    let mut buf = [0u8; 32];
    let null: &[u8] = match ty.tag() {
        ColumnTypeTag::Boolean => &[0],
        ColumnTypeTag::Byte => &nulls::BYTE.to_le_bytes(),
        ColumnTypeTag::Short => &nulls::SHORT.to_le_bytes(),
        ColumnTypeTag::Char => &0u16.to_le_bytes(),
        ColumnTypeTag::Int => &nulls::INT.to_le_bytes(),
        ColumnTypeTag::Long => &nulls::LONG.to_le_bytes(),
        ColumnTypeTag::Date => &i64::MIN.to_le_bytes(),
        ColumnTypeTag::Timestamp => &nulls::TIMESTAMP.to_le_bytes(),
        ColumnTypeTag::Float => &nulls::FLOAT.to_bits().to_le_bytes(),
        ColumnTypeTag::Double => &nulls::DOUBLE.to_bits().to_le_bytes(),
        ColumnTypeTag::Symbol => &nulls::SYMBOL.to_le_bytes(),
        ColumnTypeTag::IPv4 => &nulls::IPV4.to_le_bytes(),
        ColumnTypeTag::GeoByte => &nulls::GEOHASH_BYTE.to_le_bytes(),
        ColumnTypeTag::GeoShort => &nulls::GEOHASH_SHORT.to_le_bytes(),
        ColumnTypeTag::GeoInt => &nulls::GEOHASH_INT.to_le_bytes(),
        ColumnTypeTag::GeoLong => &nulls::GEOHASH_LONG.to_le_bytes(),
        ColumnTypeTag::Uuid | ColumnTypeTag::Long128 => &nulls::UUID.to_le_bytes(),
        ColumnTypeTag::Long256 => {
            for chunk in buf.chunks_exact_mut(8) {
                chunk.copy_from_slice(&i64::MIN.to_le_bytes());
            }
            &buf
        }
        ColumnTypeTag::Decimal8 => &nulls::DECIMAL8.to_le_bytes(),
        ColumnTypeTag::Decimal16 => &nulls::DECIMAL16.to_le_bytes(),
        ColumnTypeTag::Decimal32 => &nulls::DECIMAL32.to_le_bytes(),
        ColumnTypeTag::Decimal64 => &nulls::DECIMAL64.to_le_bytes(),
        ColumnTypeTag::Decimal128 => &nulls::DECIMAL128.to_le_bytes(),
        ColumnTypeTag::Decimal256 => {
            buf[..16].copy_from_slice(&nulls::DECIMAL256[0].to_le_bytes());
            buf[16..].copy_from_slice(&nulls::DECIMAL256[1].to_le_bytes());
            &buf
        }
        ColumnTypeTag::String | ColumnTypeTag::Binary | ColumnTypeTag::Varchar => {
            append_variable(out, -1, &[]);
            return Ok(());
        }
        _ => {
            return Err(fmt_err!(
                InvalidType,
                "unsupported dedup key type {}",
                ty.code()
            ));
        }
    };
    append_fixed(out, ty, null);
    Ok(())
}

fn append_fixed(out: &mut Vec<u8>, ty: ColumnType, bytes: &[u8]) {
    match ty.tag() {
        // WAL's int256 compares two signed 128-bit halves, high then low.
        _ if bytes.len() == 32 => {
            append_signed(out, &bytes[16..]);
            append_signed(out, &bytes[..16]);
        }
        ColumnTypeTag::Float => {
            let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
            let value = if value.is_nan() { f32::NAN } else { value };
            append_signed(out, &value.to_bits().to_le_bytes());
        }
        ColumnTypeTag::Double => {
            let mut value = [0u8; 8];
            value.copy_from_slice(bytes);
            let value = f64::from_le_bytes(value);
            let value = if value.is_nan() { f64::NAN } else { value };
            append_signed(out, &value.to_bits().to_le_bytes());
        }
        _ => append_signed(out, bytes),
    }
}

fn append_variable(out: &mut Vec<u8>, length: i64, bytes: &[u8]) {
    out.extend_from_slice(&((length as u64) ^ (1_u64 << 63)).to_be_bytes());
    out.extend_from_slice(bytes);
}

fn append_signed(out: &mut Vec<u8>, bytes: &[u8]) {
    let start = out.len();
    out.extend(bytes.iter().rev().copied());
    out[start] ^= 0x80;
}

fn range(bytes: &[u8], offset: Option<usize>, length: usize) -> CoreResult<&[u8]> {
    offset
        .and_then(|offset| bytes.get(offset..))
        .and_then(|tail| tail.get(..length))
        .ok_or_else(|| fmt_err!(InvalidLayout, "dedup key exceeds its column"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ty(tag: ColumnTypeTag) -> ColumnType {
        ColumnType::new(tag, 0)
    }

    #[test]
    fn null_keys_match_stored_nulls() {
        for (tag, null) in [
            (ColumnTypeTag::Int, nulls::INT.to_le_bytes().to_vec()),
            (ColumnTypeTag::Symbol, nulls::SYMBOL.to_le_bytes().to_vec()),
            (ColumnTypeTag::Long, nulls::LONG.to_le_bytes().to_vec()),
            (
                ColumnTypeTag::Double,
                nulls::DOUBLE.to_bits().to_le_bytes().to_vec(),
            ),
            (ColumnTypeTag::Uuid, nulls::UUID.to_le_bytes().to_vec()),
        ] {
            let mut stored = Vec::new();
            append_key_from(&mut stored, ty(tag), &null, None, 0).unwrap();
            let mut top = Vec::new();
            append_null_key(&mut top, ty(tag)).unwrap();
            assert_eq!(stored, top, "{tag:?}");
        }
        let mut aux = vec![0u8; 16];
        aux[0] = 4;
        let mut stored = Vec::new();
        append_key_from(&mut stored, ty(ColumnTypeTag::Varchar), &[], Some(&aux), 0).unwrap();
        let mut top = Vec::new();
        append_null_key(&mut top, ty(ColumnTypeTag::Varchar)).unwrap();
        assert_eq!(stored, top);
    }

    #[test]
    fn null_sorts_below_values() {
        let mut null = Vec::new();
        append_null_key(&mut null, ty(ColumnTypeTag::Int)).unwrap();
        let mut zero = Vec::new();
        append_key_from(
            &mut zero,
            ty(ColumnTypeTag::Int),
            &0i32.to_le_bytes(),
            None,
            0,
        )
        .unwrap();
        assert!(null < zero);
    }
}
