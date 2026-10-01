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

//! 64-byte column chunk descriptor stored inside each row group block.

use crate::error::ParquetMetaResult;
use crate::types::{decode_stat_sizes, Codec, EncodingMask, StatFlags};

/// On-disk layout of a column chunk (64 bytes).
///
/// Stored inside each row group block, one per column.
/// Uses `#[repr(C)]` for zero-copy reads from mmapped data.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub struct ColumnChunkRaw {
    pub codec: u8,
    pub encodings: u8,
    pub stat_flags: u8,
    pub stat_sizes: u8,
    /// Reserved for layout preservation. Must be zero.
    pub _reserved: u32,
    pub num_values: u64,
    pub byte_range_start: u64,
    pub total_compressed: u64,
    pub null_count: u64,
    pub distinct_count: u64,
    pub min_stat: u64,
    pub max_stat: u64,
}

const _: () = assert!(size_of::<ColumnChunkRaw>() == 64);

impl ColumnChunkRaw {
    /// Returns a zeroed column chunk.
    pub const fn zeroed() -> Self {
        Self {
            codec: 0,
            encodings: 0,
            stat_flags: 0,
            stat_sizes: 0,
            _reserved: 0,
            num_values: 0,
            byte_range_start: 0,
            total_compressed: 0,
            null_count: 0,
            distinct_count: 0,
            min_stat: 0,
            max_stat: 0,
        }
    }

    /// Parses the codec byte.
    pub fn codec(&self) -> ParquetMetaResult<Codec> {
        Codec::try_from(self.codec)
    }

    /// Returns the encoding bitmask.
    pub const fn encodings(&self) -> EncodingMask {
        EncodingMask(self.encodings)
    }

    /// Returns the stat flags bitmask.
    pub const fn stat_flags(&self) -> StatFlags {
        StatFlags(self.stat_flags)
    }

    /// Returns `(min_stat_size, max_stat_size)` from the nibble-encoded field.
    /// These sizes apply to inline stats only.
    pub const fn stat_sizes(&self) -> (u8, u8) {
        decode_stat_sizes(self.stat_sizes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::encode_stat_sizes;

    #[test]
    fn size_is_64_bytes() {
        assert_eq!(size_of::<ColumnChunkRaw>(), 64);
    }

    #[test]
    fn zeroed_defaults() {
        let c = ColumnChunkRaw::zeroed();
        assert_eq!(c.codec().unwrap(), Codec::Uncompressed);
        assert_eq!(c.encodings(), EncodingMask::new());
        assert_eq!(c.stat_flags(), StatFlags::new());
        assert_eq!(c.stat_sizes(), (0, 0));
        assert_eq!(c._reserved, 0);
        assert_eq!(c.num_values, 0);
        assert_eq!(c.byte_range_start, 0);
        assert_eq!(c.total_compressed, 0);
    }

    #[test]
    fn codec_accessor() {
        let mut c = ColumnChunkRaw::zeroed();
        c.codec = Codec::Zstd as u8;
        assert_eq!(c.codec().unwrap(), Codec::Zstd);
    }

    #[test]
    fn stat_sizes_nibbles() {
        let mut c = ColumnChunkRaw::zeroed();
        c.stat_sizes = encode_stat_sizes(4, 8);
        assert_eq!(c.stat_sizes(), (4, 8));
    }

    #[test]
    fn inline_stats() {
        let mut c = ColumnChunkRaw::zeroed();
        c.stat_flags = StatFlags::new()
            .with_min(true, true)
            .with_max(true, true)
            .with_null_count()
            .0;
        c.stat_sizes = encode_stat_sizes(8, 8);
        c.min_stat = 42;
        c.max_stat = 100;
        c.null_count = 5;

        let flags = c.stat_flags();
        assert!(flags.has_min_stat());
        assert!(flags.is_min_inlined());
        assert!(flags.is_min_exact());
        assert!(flags.has_max_stat());
        assert!(flags.is_max_inlined());
        assert!(flags.has_null_count());
        assert_eq!(c.min_stat, 42);
        assert_eq!(c.max_stat, 100);
    }
}
