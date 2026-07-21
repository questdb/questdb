//! Varchar row-building helpers shared by the writer and reader sides.
//!
//! The encoder functions (`varchar_to_page`, `varchar_to_dict_pages`) live in
//! `parquet_write/encoders/plain.rs`. This module retains the row-builder
//! functions that the decoder side imports (`append_varchar*`,
//! `is_column_ascii`, `SLICE_NULL_HEADER`) plus the constants they share.

use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};

const HEADER_FLAG_INLINED: u8 = 1 << 0;
const HEADER_FLAG_ASCII: u8 = 1 << 1;
const HEADER_FLAG_ASCII_32: u32 = 1 << 1;
const HEADER_FLAG_NULL: u8 = 1 << 2;
const HEADER_FLAGS_WIDTH: u32 = 4;
const VARCHAR_MAX_BYTES_FULLY_INLINED: usize = 9;
const VARCHAR_INLINED_PREFIX_BYTES: usize = 6;
const LENGTH_LIMIT_BYTES: usize = 1usize << 28;
const VARCHAR_MAX_COLUMN_SIZE: usize = 1usize << 48;
const VARCHAR_HEADER_FLAG_NULL: [u8; 10] = [
    HEADER_FLAG_NULL,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
    0u8,
];

pub fn is_column_ascii(aux: &[[u8; 16]]) -> bool {
    for entry in aux.iter() {
        let header = entry[0];
        if header & HEADER_FLAG_NULL != 0 {
            continue;
        }
        if header & HEADER_FLAG_ASCII == 0 {
            return false;
        }
    }
    true
}

pub fn append_varchar(
    aux_mem: &mut AcVec<u8>,
    data_mem: &mut AcVec<u8>,
    value: &[u8],
) -> ParquetResult<()> {
    let value_size = value.len();
    if value_size <= VARCHAR_MAX_BYTES_FULLY_INLINED {
        let flags = HEADER_FLAG_INLINED | is_ascii_inlined(value);
        let header = (value_size << HEADER_FLAGS_WIDTH) as u8 | flags;
        aux_mem.push(header)?;
        let len_before_value = aux_mem.len();
        aux_mem.extend_from_slice(value)?;
        // Add zeroes to align to 16 bytes.
        aux_mem.resize(len_before_value + VARCHAR_MAX_BYTES_FULLY_INLINED, 0u8)?;
        append_offset(aux_mem, data_mem.len())
    } else {
        if value_size > LENGTH_LIMIT_BYTES {
            return Err(fmt_err!(
                Layout,
                "VARCHAR value size {} exceeds limit {}",
                value_size,
                LENGTH_LIMIT_BYTES
            ));
        }
        let header = ((value_size as u32) << HEADER_FLAGS_WIDTH) | is_ascii(value);
        aux_mem.extend_from_slice(&header.to_le_bytes())?;
        aux_mem.extend_from_slice(&value[0..VARCHAR_INLINED_PREFIX_BYTES])?;
        data_mem.extend_from_slice(value)?;
        append_offset(aux_mem, data_mem.len() - value_size)
    }
}

fn is_ascii_inlined(value: &[u8]) -> u8 {
    for &c in value {
        if c > 127 {
            return 0u8;
        }
    }
    HEADER_FLAG_ASCII
}

fn is_ascii(value: &[u8]) -> u32 {
    for &c in value {
        if c > 127 {
            return 0u32;
        }
    }
    HEADER_FLAG_ASCII_32
}

pub fn append_varchar_null(aux_mem: &mut AcVec<u8>, data_mem: &[u8]) -> ParquetResult<()> {
    aux_mem.extend_from_slice(&VARCHAR_HEADER_FLAG_NULL)?;
    append_offset(aux_mem, data_mem.len())?;
    Ok(())
}

fn append_offset(aux_mem: &mut AcVec<u8>, offset: usize) -> ParquetResult<()> {
    if offset >= VARCHAR_MAX_COLUMN_SIZE {
        return Err(fmt_err!(
            Layout,
            "VARCHAR column offset {} exceeds maximum size {}",
            offset,
            VARCHAR_MAX_COLUMN_SIZE
        ));
    }
    aux_mem.extend_from_slice(&(offset as u16).to_le_bytes())?;
    aux_mem.extend_from_slice(&((offset >> 16) as u32).to_le_bytes())?;
    Ok(())
}

/// Writes a VarcharSlice aux entry using a VARCHAR-compatible header format.
///
/// Header (bytes 0-3, i32):
///   Non-null: (len << 4) | (ascii ? 0b11 : 0b01)
///     bit 0 = 1 (always set for non-null, guarantees header is odd, never == 4)
///     bit 1 = ASCII flag
///     bit 2 = 0 (null flag, never set for non-null)
///     bits 4-31 = string byte length
///   Null: see `append_varchar_slice_null`
///
/// Bytes 4-7 (u32): reserved, always 0
/// Bytes 8-15 (u64): pointer to string data
pub fn append_varchar_slice(
    aux_mem: &mut AcVec<u8>,
    value: &[u8],
    ascii: bool,
) -> ParquetResult<()> {
    let len = value.len();
    if len >= LENGTH_LIMIT_BYTES {
        return Err(fmt_err!(
            Layout,
            "varchar_slice value length {} exceeds 28-bit header capacity",
            len
        ));
    }
    let len = len as u32;
    let header: u32 = (len << 4) | if ascii || len == 0 { 3 } else { 1 };
    aux_mem.reserve(16)?;
    // SAFETY: `reserve` guarantees capacity for 16 new bytes. `write_unaligned` initializes
    // the memory (header + pointer) before `set_len` makes it accessible.
    unsafe {
        let addr = aux_mem.as_mut_ptr().add(aux_mem.len()).cast::<u64>();
        // Write header (bytes 0-3) + reserved zero (bytes 4-7) as a single u64.
        std::ptr::write_unaligned(addr, header as u64);
        std::ptr::write_unaligned(addr.add(1), value.as_ptr() as u64);
        aux_mem.set_len(aux_mem.len() + 16);
    }
    Ok(())
}

/// Writes a null VarcharSlice aux entry: header=4 (VARCHAR_HEADER_FLAG_NULL), ptr=0.
pub fn append_varchar_slice_null(aux_mem: &mut AcVec<u8>) -> ParquetResult<()> {
    aux_mem.reserve(16)?;
    // SAFETY: `reserve` guarantees capacity for 16 new bytes. `write_unaligned` initializes
    // the null header and zero pointer before `set_len` makes them accessible.
    unsafe {
        let addr = aux_mem.as_mut_ptr().add(aux_mem.len()).cast::<u64>();
        // Bytes 0-3: header = 4 (null flag, bit 2 set), bytes 4-7: 0.
        std::ptr::write_unaligned(addr, SLICE_NULL_HEADER as u64);
        std::ptr::write_unaligned(addr.add(1), 0u64);
        aux_mem.set_len(aux_mem.len() + 16);
    }
    Ok(())
}

/// VARCHAR_HEADER_FLAG_NULL: null sentinel for VarcharSlice header (bit 2 set).
/// Matches the VARCHAR null check in the JIT: i64(bytes 0-7) == 4.
pub const SLICE_NULL_HEADER: u32 = 4;

/// Writes count null VarcharSlice aux entries (header=4, ptr=0 each).
pub fn append_varchar_slice_nulls(aux_mem: &mut AcVec<u8>, count: usize) -> ParquetResult<()> {
    let len = count
        .checked_mul(16)
        .ok_or_else(|| fmt_err!(Layout, "append_varchar_slice_nulls overflow"))?;
    aux_mem.reserve(len)?;
    // SAFETY: `reserve` guarantees capacity for `count * 16` new bytes. `write_unaligned`
    // initializes each 16-byte null entry before `set_len` makes them accessible.
    unsafe {
        let addr = aux_mem.as_mut_ptr().add(aux_mem.len()).cast::<u64>();
        for i in 0..count {
            std::ptr::write_unaligned(addr.add(i * 2), SLICE_NULL_HEADER as u64);
            std::ptr::write_unaligned(addr.add(i * 2 + 1), 0u64);
        }
        aux_mem.set_len(aux_mem.len() + len);
    }
    Ok(())
}

pub fn append_varchar_nulls(
    aux_mem: &mut AcVec<u8>,
    data_mem: &[u8],
    count: usize,
) -> ParquetResult<()> {
    match count {
        0 => Ok(()),
        1 => append_varchar_null(aux_mem, data_mem),
        2 => {
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)
        }
        3 => {
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)
        }
        4 => {
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)?;
            append_varchar_null(aux_mem, data_mem)
        }
        _ => {
            const ENTRY_SIZE: usize = 16; // 10 bytes header + 6 bytes offset
            let offset = data_mem.len();
            if offset >= VARCHAR_MAX_COLUMN_SIZE {
                return Err(fmt_err!(
                    Layout,
                    "VARCHAR column offset {} exceeds maximum size {}",
                    offset,
                    VARCHAR_MAX_COLUMN_SIZE
                ));
            }

            let mut null_entry = [0u8; ENTRY_SIZE];
            null_entry[..10].copy_from_slice(&VARCHAR_HEADER_FLAG_NULL);
            null_entry[10..12].copy_from_slice(&(offset as u16).to_le_bytes());
            null_entry[12..16].copy_from_slice(&((offset >> 16) as u32).to_le_bytes());

            let base = aux_mem.len();
            let total_bytes = count
                .checked_mul(ENTRY_SIZE)
                .ok_or_else(|| fmt_err!(Layout, "append_varchar_nulls overflow"))?;
            let new_len = base
                .checked_add(total_bytes)
                .ok_or_else(|| fmt_err!(Layout, "append_varchar_nulls overflow"))?;

            aux_mem.reserve(total_bytes)?;
            // SAFETY: `reserve` guarantees capacity for `count * ENTRY_SIZE` new bytes.
            // `copy_nonoverlapping` initializes each 16-byte null entry before `set_len`
            // makes them accessible. Source and destination do not overlap.
            unsafe {
                let ptr = aux_mem.as_mut_ptr().add(base);
                for i in 0..count {
                    std::ptr::copy_nonoverlapping(
                        null_entry.as_ptr(),
                        ptr.add(i * ENTRY_SIZE),
                        ENTRY_SIZE,
                    );
                }
                aux_mem.set_len(new_len);
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::TestAllocatorState;

    fn test_allocator() -> (TestAllocatorState, crate::allocator::QdbAllocator) {
        let state = TestAllocatorState::new();
        let alloc = state.allocator();
        (state, alloc)
    }

    /// Build a 16-byte inlined aux entry for a short ASCII string (≤9 bytes).
    fn make_inlined_entry(value: &[u8]) -> [u8; 16] {
        assert!(value.len() <= VARCHAR_MAX_BYTES_FULLY_INLINED);
        let mut entry = [0u8; 16];
        let flags = HEADER_FLAG_INLINED | HEADER_FLAG_ASCII;
        entry[0] = ((value.len() as u8) << HEADER_FLAGS_WIDTH) | flags;
        entry[1..1 + value.len()].copy_from_slice(value);
        // offset bytes (10..16) left as zero
        entry
    }

    /// Build a 16-byte null aux entry.
    fn make_null_entry() -> [u8; 16] {
        let mut entry = [0u8; 16];
        entry[0] = HEADER_FLAG_NULL;
        entry
    }

    #[test]
    fn test_append_varchar_nulls_bulk_matches_individual() {
        let (_state, alloc) = test_allocator();
        let data_mem: Vec<u8> = vec![1, 2, 3]; // non-empty so offset is non-zero

        let count = 10;

        // Bulk path (count >= 5)
        let mut bulk_aux = AcVec::new_in(alloc.clone());
        append_varchar_nulls(&mut bulk_aux, &data_mem, count).unwrap();

        // Individual path
        let mut ind_aux = AcVec::new_in(alloc.clone());
        for _ in 0..count {
            append_varchar_null(&mut ind_aux, &data_mem).unwrap();
        }

        assert_eq!(bulk_aux.len(), ind_aux.len());
        assert_eq!(bulk_aux.as_slice(), ind_aux.as_slice());
    }

    #[test]
    fn test_is_column_ascii_false_for_non_ascii() {
        // Entry with INLINED flag set but ASCII flag clear => non-ASCII, non-null
        let mut entry = [0u8; 16];
        entry[0] = (3u8 << HEADER_FLAGS_WIDTH) | HEADER_FLAG_INLINED; // 3 chars, inlined, NOT ascii
        entry[1] = 0xC3; // UTF-8 byte > 127
        entry[2] = 0xA9;
        entry[3] = 0x21;
        let aux = vec![entry];
        assert!(!is_column_ascii(&aux));
    }

    #[test]
    fn test_is_column_ascii_true_for_ascii_entries() {
        let aux = vec![make_inlined_entry(b"hello"), make_inlined_entry(b"world")];
        assert!(is_column_ascii(&aux));
    }

    #[test]
    fn test_is_column_ascii_true_for_all_nulls() {
        let aux = vec![make_null_entry(), make_null_entry(), make_null_entry()];
        assert!(is_column_ascii(&aux));
    }

    #[test]
    fn test_is_column_ascii_true_for_empty() {
        let aux: Vec<[u8; 16]> = vec![];
        assert!(is_column_ascii(&aux));
    }

    #[test]
    fn test_append_varchar_split_path() {
        let (_state, alloc) = test_allocator();
        let mut aux_mem = AcVec::new_in(alloc.clone());
        let mut data_mem = AcVec::new_in(alloc.clone());

        let value = b"hello world!!"; // 13 bytes, > 9
        append_varchar(&mut aux_mem, &mut data_mem, value).unwrap();

        // aux_mem should be exactly 16 bytes
        assert_eq!(aux_mem.len(), 16);

        // Parse back as split entry
        let header = u32::from_le_bytes([aux_mem[0], aux_mem[1], aux_mem[2], aux_mem[3]]);
        let size = (header >> HEADER_FLAGS_WIDTH) as usize;
        assert_eq!(size, 13);

        // INLINED flag should NOT be set (bit 0 of the u8 view)
        assert_eq!(header & (HEADER_FLAG_INLINED as u32), 0);

        // ASCII flag should be set
        assert_ne!(header & HEADER_FLAG_ASCII_32, 0);

        // Prefix bytes (4..10) should match first 6 bytes of value
        assert_eq!(&aux_mem[4..10], &value[..6]);

        // data_mem should contain the full value
        assert_eq!(data_mem.as_slice(), value);

        // Offset should point to 0 (start of data_mem)
        let offset_lo = u16::from_le_bytes([aux_mem[10], aux_mem[11]]) as usize;
        let offset_hi =
            u32::from_le_bytes([aux_mem[12], aux_mem[13], aux_mem[14], aux_mem[15]]) as usize;
        let offset = offset_lo | (offset_hi << 16);
        assert_eq!(offset, 0);
    }

    #[test]
    fn test_append_varchar_inlined_path() {
        let (_state, alloc) = test_allocator();
        let mut aux_mem = AcVec::new_in(alloc.clone());
        let mut data_mem = AcVec::new_in(alloc.clone());

        let value = b"hi"; // 2 bytes, ≤ 9
        append_varchar(&mut aux_mem, &mut data_mem, value).unwrap();

        assert_eq!(aux_mem.len(), 16);

        let header = aux_mem[0];
        let size = (header >> HEADER_FLAGS_WIDTH) as usize;
        assert_eq!(size, 2);

        // INLINED flag should be set
        assert_ne!(header & HEADER_FLAG_INLINED, 0);
        // ASCII flag should be set
        assert_ne!(header & HEADER_FLAG_ASCII, 0);

        // chars at bytes 1..3 should be "hi"
        assert_eq!(&aux_mem[1..3], b"hi");

        // data_mem should be empty (inlined, nothing written to data)
        assert!(data_mem.is_empty());
    }
}
