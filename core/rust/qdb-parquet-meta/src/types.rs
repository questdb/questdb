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

//! Constants, enums, and bitfield types for the `_pm` metadata file format.

#[cfg(not(target_endian = "little"))]
compile_error!("pm metadata format requires a little-endian target");

use crate::error::ParquetMetaErrorKind;
use crate::error::ParquetMetaResult;
use crate::parquet_meta_err;

// ── File format constants ──────────────────────────────────────────────

/// Fixed portion of the file header (parquet_meta_file_size(8) +
/// feature_flags(8) + designated_ts(4) + sorting_col_count(4) +
/// col_count(4) + reserved(4)).
pub const HEADER_FIXED_SIZE: usize = 32;

/// Byte offset within the header where `parquet_meta_file_size` is stored.
/// This field holds the total committed size of the `_pm` file and is
/// patched last by the writer, acting as the MVCC commit signal.
pub const HEADER_PARQUET_META_FILE_SIZE_OFF: usize = 0;

/// Byte offset where the CRC-covered area begins (right after
/// `parquet_meta_file_size`).
pub const HEADER_CRC_AREA_OFF: usize = HEADER_PARQUET_META_FILE_SIZE_OFF + 8;

/// Size of a single column descriptor in the header.
pub const COLUMN_DESCRIPTOR_SIZE: usize = 32;

/// Size of a single column chunk inside a row group block.
pub const COLUMN_CHUNK_SIZE: usize = 64;

/// Fixed portion of the footer (parquet_footer_offset(8) + parquet_footer_length(4)
/// + row_group_count(4) + unused_bytes(8) + prev_parquet_meta_file_size(8)
/// + footer_feature_flags(8)).
pub const FOOTER_FIXED_SIZE: usize = 40;

/// Byte offset within the footer where `prev_parquet_meta_file_size` is
/// stored. This is the committed `_pm` file size at the time of the
/// previous snapshot; the previous footer is located by reading the trailer
/// at `prev_parquet_meta_file_size - 4`, mirroring how the header locates
/// the latest footer.
pub const FOOTER_PREV_PARQUET_META_FILE_SIZE_OFF: usize = 24;

/// Byte offset within the footer where footer_feature_flags is stored.
pub const FOOTER_FEATURE_FLAGS_OFF: usize = 32;

// ── Feature flags ─────────────────────────────────────────────────────

/// Required flag bits occupy the upper 32 bits (bits 32-63).
/// The reader rejects the file if any unknown required bits are set.
const REQUIRED_FLAG_MASK: u64 = 0xFFFF_FFFF_0000_0000;

/// Returns the unknown required bits given the raw flags and a mask of
/// known required bits. Non-zero means the reader must reject the file.
const fn unknown_required(raw: u64, known_required: u64) -> u64 {
    raw & REQUIRED_FLAG_MASK & !known_required
}

// ── HeaderFeatureFlags ───────────────────────────────────────────────

/// Feature flags stored in the file header.
///
/// Applies file-wide: these flags describe capabilities of the whole `_pm`
/// file and cover every footer in the MVCC chain. Features that need to vary
/// between footers live in [`FooterFeatureFlags`] instead.
///
/// Each feature's spec defines whether it adds a header section, a footer
/// section, or both. Header-gated footer sections are attached to every
/// footer in the file.
///
/// Bits 0-31 are optional (reader ignores unknown bits).
/// Bits 32-63 are required (reader rejects the file if unknown bits are set).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct HeaderFeatureFlags(pub u64);

impl HeaderFeatureFlags {
    /// Bloom filter feature is present. Adds a header section (column indices)
    /// and a footer section (per-row-group offset table).
    pub const BLOOM_FILTERS_BIT: u64 = 1 << 0;

    /// Bloom filter bitsets are stored in the parquet file (external) rather
    /// than inlined in `_pm`. Only meaningful when `BLOOM_FILTERS_BIT` is set.
    pub const BLOOM_FILTERS_EXTERNAL_BIT: u64 = 1 << 1;

    /// Sorting order is implicitly [designated_timestamp] ascending.
    /// When set, on-disk `SORTING_COLUMN_COUNT` is 0 and the sorting columns
    /// section is absent, but readers treat the partition as sorted by
    /// `[DESIGNATED_TIMESTAMP]` ascending. Only valid when `DESIGNATED_TIMESTAMP >= 0`.
    pub const SORTING_IS_DTS_ASC_BIT: u64 = 1 << 2;

    /// Partition squash tracker is stored in the header feature sections as a
    /// single `i64`. Omitted when the tracker value would be the unset
    /// sentinel (-1). Consumed by the enterprise build.
    pub const SQUASH_TRACKER_BIT: u64 = 1 << 3;

    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    /// Returns the unknown required bits given a mask of known required bits.
    /// Non-zero means the reader must reject the file.
    pub const fn unknown_required(self, known_required: u64) -> u64 {
        unknown_required(self.0, known_required)
    }

    pub const fn has_bloom_filters(self) -> bool {
        self.0 & Self::BLOOM_FILTERS_BIT != 0
    }

    pub const fn with_bloom_filters(self) -> Self {
        Self(self.0 | Self::BLOOM_FILTERS_BIT)
    }

    pub const fn has_bloom_filters_external(self) -> bool {
        self.0 & Self::BLOOM_FILTERS_EXTERNAL_BIT != 0
    }

    pub const fn with_bloom_filters_external(self) -> Self {
        Self(self.0 | Self::BLOOM_FILTERS_EXTERNAL_BIT)
    }

    pub const fn has_sorting_is_dts_asc(self) -> bool {
        self.0 & Self::SORTING_IS_DTS_ASC_BIT != 0
    }

    pub const fn with_sorting_is_dts_asc(self) -> Self {
        Self(self.0 | Self::SORTING_IS_DTS_ASC_BIT)
    }

    pub const fn has_squash_tracker(self) -> bool {
        self.0 & Self::SQUASH_TRACKER_BIT != 0
    }

    pub const fn with_squash_tracker(self) -> Self {
        Self(self.0 | Self::SQUASH_TRACKER_BIT)
    }

    /// Validates bit dependencies: bit 1 (external) requires bit 0 (bloom filters).
    pub fn validate_bit_dependencies(self) -> ParquetMetaResult<()> {
        if self.has_bloom_filters_external() && !self.has_bloom_filters() {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
                "BLOOM_FILTERS_EXTERNAL (bit 1) requires BLOOM_FILTERS (bit 0)"
            ));
        }
        Ok(())
    }
}

// ── FooterFeatureFlags ───────────────────────────────────────────────

/// Feature flags stored in each footer.
///
/// Applies per footer: two footers in the MVCC chain reached via
/// `PREV_FOOTER_OFFSET` can carry different footer-flag sets and therefore
/// different footer feature sections. Use these for features that are
/// genuinely per-snapshot; file-wide capabilities belong in
/// [`HeaderFeatureFlags`].
///
/// Bits 0-31 are optional (reader ignores unknown bits).
/// Bits 32-63 are required (reader rejects the file if unknown bits are set).
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub struct FooterFeatureFlags(pub u64);

impl FooterFeatureFlags {
    pub const fn new() -> Self {
        Self(0)
    }

    pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
        Self(u64::from_le_bytes(bytes))
    }

    pub const fn to_le_bytes(self) -> [u8; 8] {
        self.0.to_le_bytes()
    }

    /// Returns the unknown required bits given a mask of known required bits.
    /// Non-zero means the reader must reject the footer.
    pub const fn unknown_required(self, known_required: u64) -> u64 {
        unknown_required(self.0, known_required)
    }
}

/// Size of the CRC32 checksum appended after the footer entries.
pub const FOOTER_CHECKSUM_SIZE: usize = 4;

/// Size of the footer length trailer appended after the CRC32.
/// Stores the total footer size (fixed + entries + CRC) as a little-endian u32,
/// enabling readers to locate the footer given only the file size.
pub const FOOTER_TRAILER_SIZE: usize = 4;

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
    pub fn from_byte_offset(offset: u64) -> ParquetMetaResult<Self> {
        if !offset.is_multiple_of(BLOCK_ALIGNMENT as u64) {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Alignment,
                "offset {} is not {}-byte aligned",
                offset,
                BLOCK_ALIGNMENT
            ));
        }
        let shifted = offset >> BLOCK_ALIGNMENT_SHIFT;
        if shifted > u32::MAX as u64 {
            return Err(parquet_meta_err!(
                ParquetMetaErrorKind::Alignment,
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
    type Error = crate::error::ParquetMetaError;

    fn try_from(value: u8) -> ParquetMetaResult<Self> {
        match value {
            0 => Ok(Codec::Uncompressed),
            1 => Ok(Codec::Snappy),
            2 => Ok(Codec::Gzip),
            3 => Ok(Codec::Lzo),
            4 => Ok(Codec::Brotli),
            5 => Ok(Codec::Lz4),
            6 => Ok(Codec::Zstd),
            7 => Ok(Codec::Lz4Raw),
            _ => Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
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

impl From<Codec> for parquet2::compression::Compression {
    fn from(c: Codec) -> Self {
        use parquet2::compression::Compression;
        match c {
            Codec::Uncompressed => Compression::Uncompressed,
            Codec::Snappy => Compression::Snappy,
            Codec::Gzip => Compression::Gzip,
            Codec::Lzo => Compression::Lzo,
            Codec::Brotli => Compression::Brotli,
            Codec::Lz4 => Compression::Lz4,
            Codec::Zstd => Compression::Zstd,
            Codec::Lz4Raw => Compression::Lz4Raw,
        }
    }
}

// ── EncodingMask ───────────────────────────────────────────────────────

/// Bitmask of parquet encodings used in a column chunk.
///
/// Each bit corresponds to an encoding type per the `_pm` spec:
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
                // not data encodings tracked in the pm mask.
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

impl From<FieldRepetition> for parquet2::schema::Repetition {
    fn from(r: FieldRepetition) -> Self {
        use parquet2::schema::Repetition;
        match r {
            FieldRepetition::Required => Repetition::Required,
            FieldRepetition::Optional => Repetition::Optional,
            FieldRepetition::Repeated => Repetition::Repeated,
        }
    }
}

impl TryFrom<u8> for FieldRepetition {
    type Error = crate::error::ParquetMetaError;

    fn try_from(value: u8) -> ParquetMetaResult<Self> {
        match value {
            0 => Ok(FieldRepetition::Required),
            1 => Ok(FieldRepetition::Optional),
            2 => Ok(FieldRepetition::Repeated),
            _ => Err(parquet_meta_err!(
                ParquetMetaErrorKind::InvalidValue,
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

    pub fn repetition(self) -> ParquetMetaResult<FieldRepetition> {
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

    // ── Codec → parquet2::Compression tests ─────────────────────────────

    #[test]
    fn codec_to_parquet2_compression() {
        use parquet2::compression::Compression;

        assert_eq!(
            Compression::from(Codec::Uncompressed),
            Compression::Uncompressed
        );
        assert_eq!(Compression::from(Codec::Snappy), Compression::Snappy);
        assert_eq!(Compression::from(Codec::Gzip), Compression::Gzip);
        assert_eq!(Compression::from(Codec::Lzo), Compression::Lzo);
        assert_eq!(Compression::from(Codec::Brotli), Compression::Brotli);
        assert_eq!(Compression::from(Codec::Lz4), Compression::Lz4);
        assert_eq!(Compression::from(Codec::Zstd), Compression::Zstd);
        assert_eq!(Compression::from(Codec::Lz4Raw), Compression::Lz4Raw);
    }

    #[test]
    fn codec_parquet2_round_trip() {
        use parquet2::compression::Compression;

        let all_codecs = [
            Codec::Uncompressed,
            Codec::Snappy,
            Codec::Gzip,
            Codec::Lzo,
            Codec::Brotli,
            Codec::Lz4,
            Codec::Zstd,
            Codec::Lz4Raw,
        ];
        for codec in all_codecs {
            let compression = Compression::from(codec);
            let back = Codec::from(compression);
            assert_eq!(back, codec, "round-trip failed for {:?}", codec);
        }
    }

    // ── FieldRepetition → parquet2::Repetition tests ────────────────────

    #[test]
    fn field_repetition_to_parquet2() {
        use parquet2::schema::Repetition;

        assert_eq!(
            Repetition::from(FieldRepetition::Required),
            Repetition::Required
        );
        assert_eq!(
            Repetition::from(FieldRepetition::Optional),
            Repetition::Optional
        );
        assert_eq!(
            Repetition::from(FieldRepetition::Repeated),
            Repetition::Repeated
        );
    }

    #[test]
    fn field_repetition_parquet2_round_trip() {
        use parquet2::schema::Repetition;

        let all_reps = [
            FieldRepetition::Required,
            FieldRepetition::Optional,
            FieldRepetition::Repeated,
        ];
        for rep in all_reps {
            let parquet_rep = Repetition::from(rep);
            let back = FieldRepetition::from(parquet_rep);
            assert_eq!(back, rep, "round-trip failed for {:?}", rep);
        }
    }

    // ── HeaderFeatureFlags bloom filter tests ─────────────────────────

    #[test]
    fn bloom_filter_flags_round_trip() {
        let f = HeaderFeatureFlags::new().with_bloom_filters();
        assert!(f.has_bloom_filters());
        assert!(!f.has_bloom_filters_external());

        let f = f.with_bloom_filters_external();
        assert!(f.has_bloom_filters());
        assert!(f.has_bloom_filters_external());
    }

    #[test]
    fn bloom_filter_external_requires_bloom_filters() {
        let f = HeaderFeatureFlags::new().with_bloom_filters_external();
        assert!(f.validate_bit_dependencies().is_err());
    }

    #[test]
    fn bloom_filter_both_bits_valid() {
        let f = HeaderFeatureFlags::new()
            .with_bloom_filters()
            .with_bloom_filters_external();
        assert!(f.validate_bit_dependencies().is_ok());
    }

    #[test]
    fn bloom_filter_bit0_alone_valid() {
        let f = HeaderFeatureFlags::new().with_bloom_filters();
        assert!(f.validate_bit_dependencies().is_ok());
    }

    #[test]
    fn bloom_filter_no_bits_valid() {
        let f = HeaderFeatureFlags::new();
        assert!(f.validate_bit_dependencies().is_ok());
    }
}
