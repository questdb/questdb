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

//! Constants, enums, and bitfield types for the `.qdbp` metadata file format.

#[cfg(not(target_endian = "little"))]
compile_error!("qdbp metadata format requires a little-endian target");

use crate::parquet::error::ParquetResult;
use crate::parquet_metadata::error::{qdbp_err, QdbpErrorKind};

// ── File format constants ──────────────────────────────────────────────

/// Current file format version.
pub const FILE_FORMAT_VERSION: u32 = 1;

/// Fixed portion of the file header (version + designated_ts + sorting_col_count + col_count).
pub const HEADER_FIXED_SIZE: usize = 16;

/// Size of a single column descriptor in the header.
pub const COLUMN_DESCRIPTOR_SIZE: usize = 32;

/// Size of a single column chunk inside a row group block.
pub const COLUMN_CHUNK_SIZE: usize = 64;

/// Fixed portion of the footer (parquet_footer_offset + parquet_footer_length + row_group_count).
pub const FOOTER_FIXED_SIZE: usize = 16;

/// Size of the CRC32 checksum appended after the footer entries.
pub const FOOTER_CHECKSUM_SIZE: usize = 4;

/// Size of a single row group entry in the footer.
pub const ROW_GROUP_ENTRY_SIZE: usize = 4;

/// Row group block offsets are stored right-shifted by this amount.
/// Actual byte offset = stored_value << BLOCK_ALIGNMENT_SHIFT.
pub const BLOCK_ALIGNMENT_SHIFT: u32 = 3;

/// The alignment requirement for row group blocks (1 << BLOCK_ALIGNMENT_SHIFT).
pub const BLOCK_ALIGNMENT: usize = 1 << BLOCK_ALIGNMENT_SHIFT;

// ── BlockAlignedOffset ────────────────────────────────────────────────

/// A byte offset stored right-shifted by [`BLOCK_ALIGNMENT_SHIFT`].
///
/// The on-disk u32 value is `actual_byte_offset >> 3`. Use [`byte_offset()`](Self::byte_offset)
/// to recover the real offset. A zero value means "not present".
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[repr(transparent)]
pub struct BlockAlignedOffset(pub u32);

impl BlockAlignedOffset {
    pub const ZERO: Self = Self(0);

    /// Returns the actual byte offset (`stored << BLOCK_ALIGNMENT_SHIFT`).
    pub const fn byte_offset(self) -> u64 {
        (self.0 as u64) << BLOCK_ALIGNMENT_SHIFT
    }

    /// Creates from an absolute byte offset. The offset must be block-aligned.
    pub fn from_byte_offset(offset: u64) -> ParquetResult<Self> {
        if !offset.is_multiple_of(BLOCK_ALIGNMENT as u64) {
            return Err(qdbp_err!(
                QdbpErrorKind::Alignment,
                "offset {} is not {}-byte aligned",
                offset,
                BLOCK_ALIGNMENT
            ));
        }
        let shifted = offset >> BLOCK_ALIGNMENT_SHIFT;
        if shifted > u32::MAX as u64 {
            return Err(qdbp_err!(
                QdbpErrorKind::Alignment,
                "offset {} exceeds maximum representable block-aligned offset",
                offset
            ));
        }
        Ok(Self(shifted as u32))
    }
}

// ── Codec ──────────────────────────────────────────────────────────────

/// Parquet compression codec, stored as a single byte in each column chunk.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lzo = 3,
    Brotli = 4,
    Lz4 = 5,
    Zstd = 6,
    Lz4Raw = 7,
}

impl TryFrom<u8> for Codec {
    type Error = crate::parquet::error::ParquetError;

    fn try_from(value: u8) -> ParquetResult<Self> {
        match value {
            0 => Ok(Codec::Uncompressed),
            1 => Ok(Codec::Snappy),
            2 => Ok(Codec::Gzip),
            3 => Ok(Codec::Lzo),
            4 => Ok(Codec::Brotli),
            5 => Ok(Codec::Lz4),
            6 => Ok(Codec::Zstd),
            7 => Ok(Codec::Lz4Raw),
            _ => Err(qdbp_err!(
                QdbpErrorKind::InvalidValue,
                "invalid codec value: {}",
                value
            )),
        }
    }
}

impl From<parquet2::compression::Compression> for Codec {
    fn from(c: parquet2::compression::Compression) -> Self {
        use parquet2::compression::Compression;
        match c {
            Compression::Uncompressed => Codec::Uncompressed,
            Compression::Snappy => Codec::Snappy,
            Compression::Gzip => Codec::Gzip,
            Compression::Lzo => Codec::Lzo,
            Compression::Brotli => Codec::Brotli,
            Compression::Lz4 => Codec::Lz4,
            Compression::Zstd => Codec::Zstd,
            Compression::Lz4Raw => Codec::Lz4Raw,
        }
    }
}

// ── EncodingMask ───────────────────────────────────────────────────────

/// Bitmask of parquet encodings used in a column chunk.
///
/// Each bit corresponds to an encoding type per the `.qdbp` spec:
/// - bit 0: PLAIN
/// - bit 1: RLE_DICTIONARY
/// - bit 2: DELTA_BINARY_PACKED
/// - bit 3: DELTA_LENGTH_BYTE_ARRAY
/// - bit 4: DELTA_BYTE_ARRAY
/// - bit 5: BYTE_STREAM_SPLIT
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct EncodingMask(pub u8);

impl EncodingMask {
    pub const PLAIN: u8 = 1 << 0;
    pub const RLE_DICTIONARY: u8 = 1 << 1;
    pub const DELTA_BINARY_PACKED: u8 = 1 << 2;
    pub const DELTA_LENGTH_BYTE_ARRAY: u8 = 1 << 3;
    pub const DELTA_BYTE_ARRAY: u8 = 1 << 4;
    pub const BYTE_STREAM_SPLIT: u8 = 1 << 5;

    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn has_plain(self) -> bool {
        self.0 & Self::PLAIN != 0
    }

    pub const fn has_rle_dictionary(self) -> bool {
        self.0 & Self::RLE_DICTIONARY != 0
    }

    pub const fn has_delta_binary_packed(self) -> bool {
        self.0 & Self::DELTA_BINARY_PACKED != 0
    }

    pub const fn has_delta_length_byte_array(self) -> bool {
        self.0 & Self::DELTA_LENGTH_BYTE_ARRAY != 0
    }

    pub const fn has_delta_byte_array(self) -> bool {
        self.0 & Self::DELTA_BYTE_ARRAY != 0
    }

    pub const fn has_byte_stream_split(self) -> bool {
        self.0 & Self::BYTE_STREAM_SPLIT != 0
    }
}

impl From<&[parquet2::encoding::Encoding]> for EncodingMask {
    fn from(encodings: &[parquet2::encoding::Encoding]) -> Self {
        use parquet2::encoding::Encoding;
        let mut mask = 0u8;
        for enc in encodings {
            mask |= match enc {
                Encoding::Plain => Self::PLAIN,
                Encoding::PlainDictionary | Encoding::RleDictionary => Self::RLE_DICTIONARY,
                Encoding::DeltaBinaryPacked => Self::DELTA_BINARY_PACKED,
                Encoding::DeltaLengthByteArray => Self::DELTA_LENGTH_BYTE_ARRAY,
                Encoding::DeltaByteArray => Self::DELTA_BYTE_ARRAY,
                Encoding::ByteStreamSplit => Self::BYTE_STREAM_SPLIT,
                // RLE and BitPacked are used for definition/repetition levels,
                // not data encodings tracked in the qdbp mask.
                Encoding::Rle | Encoding::BitPacked => 0,
            };
        }
        Self(mask)
    }
}

// ── StatFlags ──────────────────────────────────────────────────────────

/// Bitmask describing which statistics are present in a column chunk.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct StatFlags(pub u8);

impl StatFlags {
    pub const MIN_PRESENT: u8 = 1 << 0;
    pub const MIN_INLINED: u8 = 1 << 1;
    pub const MIN_EXACT: u8 = 1 << 2;
    pub const MAX_PRESENT: u8 = 1 << 3;
    pub const MAX_INLINED: u8 = 1 << 4;
    pub const MAX_EXACT: u8 = 1 << 5;
    pub const DISTINCT_COUNT_PRESENT: u8 = 1 << 6;
    pub const NULL_COUNT_PRESENT: u8 = 1 << 7;

    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn has_min_stat(self) -> bool {
        self.0 & Self::MIN_PRESENT != 0
    }

    pub const fn is_min_inlined(self) -> bool {
        self.0 & Self::MIN_INLINED != 0
    }

    pub const fn is_min_exact(self) -> bool {
        self.0 & Self::MIN_EXACT != 0
    }

    pub const fn has_max_stat(self) -> bool {
        self.0 & Self::MAX_PRESENT != 0
    }

    pub const fn is_max_inlined(self) -> bool {
        self.0 & Self::MAX_INLINED != 0
    }

    pub const fn is_max_exact(self) -> bool {
        self.0 & Self::MAX_EXACT != 0
    }

    pub const fn has_distinct_count(self) -> bool {
        self.0 & Self::DISTINCT_COUNT_PRESENT != 0
    }

    pub const fn has_null_count(self) -> bool {
        self.0 & Self::NULL_COUNT_PRESENT != 0
    }

    pub const fn with_min(self, is_inlined: bool, is_exact: bool) -> Self {
        let mut bits = self.0 | Self::MIN_PRESENT;
        if is_inlined {
            bits |= Self::MIN_INLINED;
        }
        if is_exact {
            bits |= Self::MIN_EXACT;
        }
        Self(bits)
    }

    pub const fn with_max(self, is_inlined: bool, is_exact: bool) -> Self {
        let mut bits = self.0 | Self::MAX_PRESENT;
        if is_inlined {
            bits |= Self::MAX_INLINED;
        }
        if is_exact {
            bits |= Self::MAX_EXACT;
        }
        Self(bits)
    }

    pub const fn with_null_count(self) -> Self {
        Self(self.0 | Self::NULL_COUNT_PRESENT)
    }

    pub const fn with_distinct_count(self) -> Self {
        Self(self.0 | Self::DISTINCT_COUNT_PRESENT)
    }
}

// ── FieldRepetition ────────────────────────────────────────────────────

/// Field repetition level, stored in bits 2-3 of [`ColumnFlags`].
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldRepetition {
    Required = 0,
    Optional = 1,
    Repeated = 2,
}

impl From<parquet2::schema::Repetition> for FieldRepetition {
    fn from(r: parquet2::schema::Repetition) -> Self {
        use parquet2::schema::Repetition;
        match r {
            Repetition::Required => FieldRepetition::Required,
            Repetition::Optional => FieldRepetition::Optional,
            Repetition::Repeated => FieldRepetition::Repeated,
        }
    }
}

impl TryFrom<u8> for FieldRepetition {
    type Error = crate::parquet::error::ParquetError;

    fn try_from(value: u8) -> ParquetResult<Self> {
        match value {
            0 => Ok(FieldRepetition::Required),
            1 => Ok(FieldRepetition::Optional),
            2 => Ok(FieldRepetition::Repeated),
            _ => Err(qdbp_err!(
                QdbpErrorKind::InvalidValue,
                "invalid field repetition: {}",
                value
            )),
        }
    }
}

// ── ColumnFlags ────────────────────────────────────────────────────────

/// Bitfield stored in each column descriptor's FLAGS field.
///
/// Layout:
/// - bit 0: LOCAL_KEY_IS_GLOBAL (symbol columns)
/// - bit 1: IS_ASCII (varchar columns)
/// - bits 2-3: FIELD_REPETITION (2 bits)
/// - bit 4: DESCENDING (for sorted columns)
/// - bits 5-31: reserved
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct ColumnFlags(pub i32);

impl ColumnFlags {
    const LOCAL_KEY_IS_GLOBAL_BIT: i32 = 1 << 0;
    const IS_ASCII_BIT: i32 = 1 << 1;
    const REPETITION_SHIFT: u32 = 2;
    const REPETITION_MASK: i32 = 0b11 << Self::REPETITION_SHIFT;
    const DESCENDING_BIT: i32 = 1 << 4;

    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn is_local_key_global(self) -> bool {
        self.0 & Self::LOCAL_KEY_IS_GLOBAL_BIT != 0
    }

    pub const fn is_ascii(self) -> bool {
        self.0 & Self::IS_ASCII_BIT != 0
    }

    pub fn repetition(self) -> ParquetResult<FieldRepetition> {
        let val = ((self.0 & Self::REPETITION_MASK) >> Self::REPETITION_SHIFT) as u8;
        FieldRepetition::try_from(val)
    }

    pub const fn is_descending(self) -> bool {
        self.0 & Self::DESCENDING_BIT != 0
    }

    pub const fn with_local_key_is_global(self) -> Self {
        Self(self.0 | Self::LOCAL_KEY_IS_GLOBAL_BIT)
    }

    pub const fn with_ascii(self) -> Self {
        Self(self.0 | Self::IS_ASCII_BIT)
    }

    pub const fn with_repetition(self, rep: FieldRepetition) -> Self {
        let cleared = self.0 & !Self::REPETITION_MASK;
        Self(cleared | ((rep as i32) << Self::REPETITION_SHIFT))
    }

    pub const fn with_descending(self) -> Self {
        Self(self.0 | Self::DESCENDING_BIT)
    }
}

/// Encodes min/max stat byte sizes into the STAT_SIZES nibble byte.
/// Low nibble = min size, high nibble = max size.
pub const fn encode_stat_sizes(min_size: u8, max_size: u8) -> u8 {
    (min_size & 0x0F) | ((max_size & 0x0F) << 4)
}

/// Decodes STAT_SIZES nibble byte into (min_size, max_size).
pub const fn decode_stat_sizes(stat_sizes: u8) -> (u8, u8) {
    (stat_sizes & 0x0F, stat_sizes >> 4)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Codec tests ────────────────────────────────────────────────────

    #[test]
    fn codec_round_trip_all_variants() {
        for val in 0..=7u8 {
            let codec = Codec::try_from(val).unwrap();
            assert_eq!(codec as u8, val);
        }
    }

    #[test]
    fn codec_invalid_value() {
        assert!(Codec::try_from(8u8).is_err());
        assert!(Codec::try_from(255u8).is_err());
    }

    #[test]
    fn codec_from_parquet2_compression() {
        use parquet2::compression::Compression;
        assert_eq!(Codec::from(Compression::Uncompressed), Codec::Uncompressed);
        assert_eq!(Codec::from(Compression::Snappy), Codec::Snappy);
        assert_eq!(Codec::from(Compression::Gzip), Codec::Gzip);
        assert_eq!(Codec::from(Compression::Lzo), Codec::Lzo);
        assert_eq!(Codec::from(Compression::Brotli), Codec::Brotli);
        assert_eq!(Codec::from(Compression::Lz4), Codec::Lz4);
        assert_eq!(Codec::from(Compression::Zstd), Codec::Zstd);
        assert_eq!(Codec::from(Compression::Lz4Raw), Codec::Lz4Raw);
    }

    // ── EncodingMask tests ─────────────────────────────────────────────

    #[test]
    fn encoding_mask_individual_bits() {
        use parquet2::encoding::Encoding;

        let m = EncodingMask::from([Encoding::Plain].as_slice());
        assert!(m.has_plain());
        assert!(!m.has_rle_dictionary());

        let m = EncodingMask::from([Encoding::RleDictionary].as_slice());
        assert!(m.has_rle_dictionary());
        assert!(!m.has_plain());

        let m = EncodingMask::from([Encoding::PlainDictionary].as_slice());
        assert!(m.has_rle_dictionary()); // PlainDictionary maps to same bit

        let m = EncodingMask::from([Encoding::DeltaBinaryPacked].as_slice());
        assert!(m.has_delta_binary_packed());

        let m = EncodingMask::from([Encoding::DeltaLengthByteArray].as_slice());
        assert!(m.has_delta_length_byte_array());

        let m = EncodingMask::from([Encoding::DeltaByteArray].as_slice());
        assert!(m.has_delta_byte_array());

        let m = EncodingMask::from([Encoding::ByteStreamSplit].as_slice());
        assert!(m.has_byte_stream_split());
    }

    #[test]
    fn encoding_mask_combined() {
        use parquet2::encoding::Encoding;
        let m = EncodingMask::from(
            [Encoding::Plain, Encoding::RleDictionary, Encoding::Rle].as_slice(),
        );
        assert!(m.has_plain());
        assert!(m.has_rle_dictionary());
        assert!(!m.has_delta_binary_packed());
        assert_eq!(m.0, 0b00000011);
    }

    #[test]
    fn encoding_mask_rle_bitpacked_ignored() {
        use parquet2::encoding::Encoding;
        let m = EncodingMask::from([Encoding::Rle, Encoding::BitPacked].as_slice());
        assert_eq!(m.0, 0);
    }

    // ── StatFlags tests ────────────────────────────────────────────────

    #[test]
    fn stat_flags_individual_bits() {
        let f = StatFlags::new().with_min(true, true).with_null_count();
        assert!(f.has_min_stat());
        assert!(f.is_min_inlined());
        assert!(f.is_min_exact());
        assert!(!f.has_max_stat());
        assert!(f.has_null_count());
        assert!(!f.has_distinct_count());
    }

    #[test]
    fn stat_flags_out_of_line_min() {
        let f = StatFlags::new().with_min(false, true);
        assert!(f.has_min_stat());
        assert!(!f.is_min_inlined());
        assert!(f.is_min_exact());
    }

    #[test]
    fn stat_flags_all_set() {
        let f = StatFlags::new()
            .with_min(true, true)
            .with_max(true, true)
            .with_null_count()
            .with_distinct_count();
        assert_eq!(f.0, 0xFF);
    }

    // ── FieldRepetition tests ──────────────────────────────────────────

    #[test]
    fn field_repetition_round_trip() {
        for val in 0..=2u8 {
            let rep = FieldRepetition::try_from(val).unwrap();
            assert_eq!(rep as u8, val);
        }
    }

    #[test]
    fn field_repetition_invalid() {
        assert!(FieldRepetition::try_from(3u8).is_err());
    }

    #[test]
    fn field_repetition_from_parquet2() {
        use parquet2::schema::Repetition;
        assert_eq!(
            FieldRepetition::from(Repetition::Required),
            FieldRepetition::Required
        );
        assert_eq!(
            FieldRepetition::from(Repetition::Optional),
            FieldRepetition::Optional
        );
        assert_eq!(
            FieldRepetition::from(Repetition::Repeated),
            FieldRepetition::Repeated
        );
    }

    // ── ColumnFlags tests ──────────────────────────────────────────────

    #[test]
    fn column_flags_default_empty() {
        let f = ColumnFlags::new();
        assert!(!f.is_local_key_global());
        assert!(!f.is_ascii());
        assert_eq!(f.repetition().unwrap(), FieldRepetition::Required);
        assert!(!f.is_descending());
        assert_eq!(f.0, 0);
    }

    #[test]
    fn column_flags_individual_setters() {
        let f = ColumnFlags::new().with_local_key_is_global();
        assert!(f.is_local_key_global());
        assert_eq!(f.0, 1);

        let f = ColumnFlags::new().with_ascii();
        assert!(f.is_ascii());
        assert_eq!(f.0, 2);

        let f = ColumnFlags::new().with_repetition(FieldRepetition::Optional);
        assert_eq!(f.repetition().unwrap(), FieldRepetition::Optional);
        assert_eq!(f.0, 1 << 2);

        let f = ColumnFlags::new().with_repetition(FieldRepetition::Repeated);
        assert_eq!(f.repetition().unwrap(), FieldRepetition::Repeated);
        assert_eq!(f.0, 2 << 2);

        let f = ColumnFlags::new().with_descending();
        assert!(f.is_descending());
        assert_eq!(f.0, 1 << 4);
    }

    #[test]
    fn column_flags_combined() {
        let f = ColumnFlags::new()
            .with_local_key_is_global()
            .with_repetition(FieldRepetition::Optional)
            .with_descending();
        assert!(f.is_local_key_global());
        assert!(!f.is_ascii());
        assert_eq!(f.repetition().unwrap(), FieldRepetition::Optional);
        assert!(f.is_descending());
    }

    #[test]
    fn column_flags_i32_round_trip() {
        let f = ColumnFlags::new()
            .with_ascii()
            .with_repetition(FieldRepetition::Repeated)
            .with_descending();
        let raw = f.0;
        let f2 = ColumnFlags(raw);
        assert_eq!(f, f2);
    }

    // ── stat_sizes tests ───────────────────────────────────────────────

    #[test]
    fn stat_sizes_encode_decode() {
        assert_eq!(decode_stat_sizes(encode_stat_sizes(0, 0)), (0, 0));
        assert_eq!(decode_stat_sizes(encode_stat_sizes(4, 8)), (4, 8));
        assert_eq!(decode_stat_sizes(encode_stat_sizes(1, 15)), (1, 15));
        assert_eq!(decode_stat_sizes(encode_stat_sizes(8, 8)), (8, 8));
    }

    // ── BlockAlignedOffset tests ──────────────────────────────────────

    #[test]
    fn block_aligned_offset_valid() {
        let o = BlockAlignedOffset::from_byte_offset(64).unwrap();
        assert_eq!(o.byte_offset(), 64);
    }

    #[test]
    fn block_aligned_offset_misaligned() {
        assert!(BlockAlignedOffset::from_byte_offset(7).is_err());
        assert!(BlockAlignedOffset::from_byte_offset(1).is_err());
    }

    #[test]
    fn block_aligned_offset_overflow() {
        // (u32::MAX as u64 + 1) << 3 exceeds u32 shifted range.
        let huge = ((u32::MAX as u64) + 1) << BLOCK_ALIGNMENT_SHIFT;
        assert!(BlockAlignedOffset::from_byte_offset(huge).is_err());
    }

    // ── StatFlags extra coverage ──────────────────────────────────────

    #[test]
    fn stat_flags_max_exact() {
        let f = StatFlags::new().with_max(true, true);
        assert!(f.is_max_exact());
        let f = StatFlags::new().with_max(true, false);
        assert!(!f.is_max_exact());
    }
}
