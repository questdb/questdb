use std::io;

use crate::parquet_write::util::{
    encode_all_ones_def_levels, encode_all_zeros_def_levels,
    encode_primitive_def_levels_from_bitmap,
};
use parquet2::write::Version;

#[derive(Clone, Copy, Debug, Default)]
pub struct DefLevelsMeta {
    pub definition_levels_byte_length: usize,
    pub null_count: usize,
}

#[derive(Debug, Default)]
pub struct FlatValidity {
    bits: Vec<u8>,
    write_idx: usize,
    num_rows: usize,
    null_count: usize,
}

impl FlatValidity {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self, num_rows: usize) {
        self.write_idx = 0;
        self.num_rows = num_rows;
        self.null_count = 0;

        let required_bytes = num_rows.saturating_add(7) / 8;
        if self.bits.len() < required_bytes {
            self.bits.resize(required_bytes, 0);
        }
    }

    #[inline]
    pub fn push_present(&mut self) {
        debug_assert!(self.write_idx < self.num_rows);
        let byte_idx = self.write_idx / 8;
        let bit_idx = self.write_idx % 8;
        self.bits[byte_idx] |= 1u8 << bit_idx;
        self.write_idx += 1;
    }

    #[inline]
    pub fn push_null(&mut self) {
        debug_assert!(self.write_idx < self.num_rows);
        let byte_idx = self.write_idx / 8;
        let bit_idx = self.write_idx % 8;
        self.bits[byte_idx] &= !(1u8 << bit_idx);
        self.write_idx += 1;
        self.null_count += 1;
    }

    pub fn encode_def_levels(
        &self,
        buffer: &mut Vec<u8>,
        version: Version,
    ) -> io::Result<DefLevelsMeta> {
        debug_assert_eq!(self.write_idx, self.num_rows);

        let start = buffer.len();
        if self.num_rows == 0 {
            return Ok(DefLevelsMeta {
                definition_levels_byte_length: 0,
                null_count: self.null_count,
            });
        }

        if self.null_count == 0 {
            encode_all_ones_def_levels(buffer, self.num_rows, version);
        } else if self.null_count == self.num_rows {
            encode_all_zeros_def_levels(buffer, self.num_rows, version);
        } else {
            let used_bytes = self.num_rows.saturating_add(7) / 8;
            encode_primitive_def_levels_from_bitmap(
                buffer,
                &self.bits[..used_bytes],
                self.num_rows,
                version,
            )?;
        }

        Ok(DefLevelsMeta {
            definition_levels_byte_length: buffer.len() - start,
            null_count: self.null_count,
        })
    }
}
