use std::mem;

use super::util::ExactSizedIter;
use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_dict_rle_pages, encode_primitive_def_levels, BinaryMaxMinStats,
};
use parquet2::encoding::{delta_bitpacked, Encoding};
use parquet2::page::Page;
use parquet2::schema::types::PrimitiveType;
use parquet2::write::DynIter;
use rapidhash::RapidHashMap;

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

#[repr(C, packed)]
struct AuxEntryInlined {
    header: u8,
    chars: [u8; 9],
    _offset: [u8; 6],
}

#[repr(C, packed)]
struct AuxEntrySplit {
    header: u32,
    chars: [u8; 6],
    offset_lo: u16,
    offset_hi: u32,
}

pub fn varchar_to_page(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> ParquetResult<Page> {
    assert!(
        mem::size_of::<AuxEntryInlined>() == 16 && mem::size_of::<AuxEntrySplit>() == 16,
        "size_of(AuxEntryInlined) or size_of(AuxEntrySplit) is not 16"
    );

    let num_rows = column_top + aux.len();
    let mut buffer = vec![];
    let mut null_count = 0usize;

    // SAFETY: `AuxEntryInlined` is `#[repr(C, packed)]` and exactly 16 bytes (asserted above).
    // The source `&[[u8; 16]]` has compatible size and alignment 1.
    let aux: &[AuxEntryInlined] = unsafe { mem::transmute(aux) };

    let utf8_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            if is_null(entry.header) {
                null_count += 1;
                None
            } else if is_inlined(entry.header) {
                let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
                Some(&entry.chars[..size])
            } else {
                // SAFETY: Both `AuxEntryInlined` and `AuxEntrySplit` are `#[repr(C, packed)]` and 16 bytes.
                // The header flag check above determines which interpretation is valid.
                let entry: &AuxEntrySplit = unsafe { mem::transmute(entry) };
                let header = entry.header;
                let size = (header >> HEADER_FLAGS_WIDTH) as usize;
                let offset = entry.offset_lo as usize | ((entry.offset_hi as usize) << 16);
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in VARCHAR column"
                );
                Some(&data[offset..][..size])
            }
        })
        .collect();

    let deflevels_iter =
        (0..num_rows).map(|i| i >= column_top && utf8_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut buffer, deflevels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();

    let mut stats = BinaryMaxMinStats::new(&primitive_type);

    match encoding {
        Encoding::Plain => {
            encode_plain(&utf8_slices, &mut buffer, &mut stats);
        }
        Encoding::DeltaLengthByteArray => {
            encode_delta(&utf8_slices, null_count, &mut buffer, &mut stats);
        }
        _ => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {encoding:?} while writing a varchar column"
            ));
        }
    };

    let null_count = column_top + null_count;
    build_plain_page(
        buffer,
        num_rows,
        null_count,
        definition_levels_byte_length,
        if options.write_statistics {
            Some(stats.into_parquet_stats(null_count))
        } else {
            None
        },
        primitive_type,
        options,
        encoding,
        false,
    )
    .map(Page::Data)
}

pub fn varchar_to_dict_pages(
    aux: &[[u8; 16]],
    data: &[u8],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let num_rows = column_top + aux.len();
    // SAFETY: `AuxEntryInlined` is `#[repr(C, packed)]` and exactly 16 bytes (asserted above in `varchar_to_page`).
    // The source `&[[u8; 16]]` has compatible size and alignment 1.
    let aux: &[AuxEntryInlined] = unsafe { mem::transmute(aux) };

    // Pass 1: decode aux entries into contiguous slice pointers.
    let mut null_count = 0usize;
    let utf8_slices: Vec<Option<&[u8]>> = aux
        .iter()
        .map(|entry| {
            if is_null(entry.header) {
                null_count += 1;
                None
            } else if is_inlined(entry.header) {
                let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
                Some(&entry.chars[..size])
            } else {
                // SAFETY: Both `AuxEntryInlined` and `AuxEntrySplit` are `#[repr(C, packed)]` and 16 bytes.
                // The header flag check above determines which interpretation is valid.
                let entry: &AuxEntrySplit = unsafe { mem::transmute(entry) };
                let size = (entry.header >> HEADER_FLAGS_WIDTH) as usize;
                let offset = entry.offset_lo as usize | ((entry.offset_hi as usize) << 16);
                assert!(
                    offset + size <= data.len(),
                    "Data corruption in VARCHAR column"
                );
                Some(&data[offset..][..size])
            }
        })
        .collect();

    // Pass 2: deduplicate strings into dictionary.
    let mut dict_map: RapidHashMap<&[u8], u32> = RapidHashMap::default();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut keys = Vec::with_capacity(utf8_slices.len() - null_count);
    let mut total_keys_bytes = 0usize;
    for s in utf8_slices.iter().flatten() {
        let next_id = dict_entries.len() as u32;
        let key = *dict_map.entry(s).or_insert_with(|| {
            dict_entries.push(s);
            total_keys_bytes += 4 + s.len(); // 4 bytes for length prefix
            next_id
        });
        keys.push(key);
    }

    // Build dict buffer (length-prefixed UTF-8)
    let mut dict_buffer = Vec::with_capacity(total_keys_bytes);
    let mut stats = if options.write_statistics {
        Some(BinaryMaxMinStats::new(&primitive_type))
    } else {
        None
    };
    for &entry in &dict_entries {
        dict_buffer.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        dict_buffer.extend_from_slice(entry);
        if let Some(ref mut s) = stats {
            s.update(entry);
        }
    }

    // Encode data page: def levels + RLE-encoded keys
    let mut data_buffer = Vec::with_capacity(num_rows / 4);
    let total_null_count = column_top + null_count;

    let def_levels =
        (0..num_rows).map(|i| i >= column_top && utf8_slices[i - column_top].is_some());
    encode_primitive_def_levels(&mut data_buffer, def_levels, num_rows, options.version)?;
    let definition_levels_byte_length = data_buffer.len();

    let non_null_len = aux.len() - null_count;
    let statistics = stats.map(|s| s.into_parquet_stats(total_null_count));

    encode_dict_rle_pages(
        dict_buffer,
        dict_entries.len(),
        keys,
        non_null_len,
        data_buffer,
        definition_levels_byte_length,
        num_rows,
        total_null_count,
        statistics,
        primitive_type,
        options,
        false,
    )
}

fn encode_plain(
    utf8_slices: &[Option<&[u8]>],
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        let len = (utf8.len() as u32).to_le_bytes();
        buffer.extend_from_slice(&len);
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
    }
}

fn encode_delta(
    utf8_slices: &[Option<&[u8]>],
    null_count: usize,
    buffer: &mut Vec<u8>,
    stats: &mut BinaryMaxMinStats,
) {
    let lengths = utf8_slices
        .iter()
        .filter_map(|&option| option)
        .map(|utf8| utf8.len() as i64);
    let lengths = ExactSizedIter::new(lengths, utf8_slices.len() - null_count);
    delta_bitpacked::encode(lengths, buffer);
    for utf8 in utf8_slices.iter().filter_map(|&option| option) {
        buffer.extend_from_slice(utf8);
        stats.update(utf8);
    }
}

#[inline(always)]
fn is_null(header: u8) -> bool {
    (header & HEADER_FLAG_NULL) == HEADER_FLAG_NULL
}

#[inline(always)]
fn is_inlined(header: u8) -> bool {
    (header & HEADER_FLAG_INLINED) == HEADER_FLAG_INLINED
}

/// Check if all non-null varchar values in the aux vector have the ASCII flag set.
/// Uses the per-value flags already stored in QuestDB's internal varchar format.
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
        assert!(value_size <= LENGTH_LIMIT_BYTES);
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
    assert!(offset < VARCHAR_MAX_COLUMN_SIZE);
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
            assert!(offset < VARCHAR_MAX_COLUMN_SIZE);

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
    use parquet2::schema::types::PhysicalType;

    fn test_allocator() -> (TestAllocatorState, crate::allocator::QdbAllocator) {
        let state = TestAllocatorState::new();
        let alloc = state.allocator();
        (state, alloc)
    }

    fn test_write_options() -> WriteOptions {
        WriteOptions {
            write_statistics: true,
            version: parquet2::write::Version::V2,
            compression: parquet2::compression::CompressionOptions::Uncompressed,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            min_compression_ratio: 0.0,
        }
    }

    fn test_primitive_type() -> PrimitiveType {
        PrimitiveType::from_physical("test_col".to_string(), PhysicalType::ByteArray)
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

    /// Build a 16-byte split aux entry for a longer string (>9 bytes).
    /// `offset` is the byte offset into the data buffer where the full string starts.
    fn make_split_entry(value: &[u8], offset: usize) -> [u8; 16] {
        assert!(value.len() > VARCHAR_MAX_BYTES_FULLY_INLINED);
        let mut entry = [0u8; 16];
        let header: u32 = ((value.len() as u32) << HEADER_FLAGS_WIDTH) | HEADER_FLAG_ASCII_32;
        entry[0..4].copy_from_slice(&header.to_le_bytes());
        entry[4..10].copy_from_slice(&value[..VARCHAR_INLINED_PREFIX_BYTES]);
        entry[10..12].copy_from_slice(&(offset as u16).to_le_bytes());
        entry[12..16].copy_from_slice(&((offset >> 16) as u32).to_le_bytes());
        entry
    }

    /// Build a 16-byte null aux entry.
    fn make_null_entry() -> [u8; 16] {
        let mut entry = [0u8; 16];
        entry[0] = HEADER_FLAG_NULL;
        entry
    }

    #[test]
    fn test_varchar_to_dict_pages() {
        let short1 = b"hi";
        let short2 = b"bye";
        let long1 = b"hello world!!";

        let mut data = Vec::new();
        let long1_offset = 0usize;
        data.extend_from_slice(long1);

        let aux = vec![
            make_inlined_entry(short1),
            make_null_entry(),
            make_split_entry(long1, long1_offset),
            make_inlined_entry(short2),
            make_inlined_entry(short1), // duplicate of short1
        ];

        let options = test_write_options();
        let pt = test_primitive_type();

        let result = varchar_to_dict_pages(&aux, &data, 0, options, pt);
        assert!(result.is_ok());

        let pages: Vec<_> = result.unwrap().collect();
        assert_eq!(pages.len(), 2);
        assert!(matches!(&pages[0], Ok(Page::Dict(_))));
        assert!(matches!(&pages[1], Ok(Page::Data(_))));
    }

    #[test]
    fn test_unsupported_encoding_error() {
        let aux = vec![make_inlined_entry(b"abc")];
        let data = vec![];
        let options = test_write_options();
        let pt = test_primitive_type();

        let result = varchar_to_page(&aux, &data, 0, options, pt, Encoding::BitPacked);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("unsupported encoding"),
            "expected 'unsupported encoding' in: {err_msg}"
        );
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
