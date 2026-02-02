//! SIMD-optimized encoding for Parquet definition levels.
//!
//! This module provides fast encoding of definition levels (null bitmaps) using
//! portable SIMD operations. Definition levels indicate which values are present
//! vs null in nullable columns.

use std::io::Write;
use std::simd::cmp::{SimdOrd, SimdPartialEq, SimdPartialOrd};
use std::simd::num::{SimdFloat, SimdInt};
use std::simd::Simd;

use crate::parquet_write::util::MaxMin;

/// Result of SIMD definition level encoding, containing both the encoded
/// buffer and statistics computed during the encoding pass.
pub struct DefLevelResult<T> {
    pub null_count: usize,
    pub max: Option<T>,
    pub min: Option<T>,
}

/// Encodes definition levels for i64 slices using SIMD.
///
/// Returns the null count and optionally computes min/max statistics.
#[allow(unused_assignments)]
pub fn encode_i64_def_levels<W: Write>(
    writer: &mut W,
    slice: &[i64],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<i64>> {
    // Fast path: if no column_top and we can quickly verify no nulls, use RLE
    if column_top == 0 {
        if let Some(result) = try_encode_i64_all_present(writer, slice, compute_stats)? {
            return Ok(result);
        }
    }

    // Slow path: there are nulls or column_top, use bitpacked encoding
    encode_i64_def_levels_bitpacked(writer, slice, column_top, compute_stats)
}

/// Fast path: check if all i64 values are non-null and encode with RLE if so.
/// Returns None if any null (i64::MIN) is found.
fn try_encode_i64_all_present<W: Write>(
    writer: &mut W,
    slice: &[i64],
    compute_stats: bool,
) -> std::io::Result<Option<DefLevelResult<i64>>> {
    let null_val = Simd::<i64, 8>::splat(i64::MIN);

    // SIMD scan with deferred null check - no branching in hot loop
    let chunks = slice.chunks_exact(8);
    let remainder = chunks.remainder();

    // Accumulate validity without branching
    let mut all_valid_mask = Simd::<i64, 8>::splat(-1);
    let mut min_vec = Simd::<i64, 8>::splat(i64::MAX);
    let mut max_vec = Simd::<i64, 8>::splat(i64::MIN + 1);

    if compute_stats {
        for chunk in chunks {
            let values = Simd::<i64, 8>::from_slice(chunk);
            let is_not_null = values.simd_ne(null_val);
            all_valid_mask &= is_not_null.to_int();
            min_vec = min_vec.simd_min(values);
            max_vec = max_vec.simd_max(values);
        }
    } else {
        for chunk in chunks {
            let values = Simd::<i64, 8>::from_slice(chunk);
            let is_not_null = values.simd_ne(null_val);
            all_valid_mask &= is_not_null.to_int();
        }
    }

    // Check if any null was found
    if all_valid_mask.reduce_and() == 0 {
        return Ok(None);
    }

    // Check remainder
    for &val in remainder {
        if val == i64::MIN {
            return Ok(None);
        }
    }

    // All values are present! Use RLE encoding
    write_rle_all_ones(writer, slice.len())?;

    // Compute final statistics
    let (min_val, max_val) = if compute_stats {
        let mut min_i = i64::MAX;
        let mut max_i = i64::MIN + 1;

        for i in 0..8 {
            min_i = min_i.min(min_vec[i]);
            max_i = max_i.max(max_vec[i]);
        }

        for &val in remainder {
            min_i = min_i.min(val);
            max_i = max_i.max(val);
        }

        if slice.is_empty() {
            (None, None)
        } else {
            (Some(min_i), Some(max_i))
        }
    } else {
        (None, None)
    };

    Ok(Some(DefLevelResult {
        null_count: 0,
        max: max_val,
        min: min_val,
    }))
}

/// Slow path: bitpacked encoding for i64 slices with nulls or column_top
/// Note: Returns null_count for the data slice ONLY, not including column_top.
/// The caller should add column_top to get the total null count for statistics.
#[allow(unused_assignments)]
fn encode_i64_def_levels_bitpacked<W: Write>(
    writer: &mut W,
    slice: &[i64],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<i64>> {
    let null_val = Simd::<i64, 8>::splat(i64::MIN);
    let mut data_null_count = 0usize; // Only nulls in slice data, not column_top
    let mut stats = MaxMin::<i64>::new();

    let num_rows = column_top + slice.len();

    // Write header for hybrid RLE bitpacked encoding
    write_bitpacked_header(writer, num_rows)?;

    // Handle column_top prefix (all nulls) - write zero bytes
    let top_full_bytes = column_top / 8;
    let top_remaining_bits = column_top % 8;

    // Write full zero bytes for column_top (these are nulls, but not counted in data_null_count)
    for _ in 0..top_full_bytes {
        writer.write_all(&[0u8])?;
    }

    // Track bits accumulated in the current partial byte
    let mut partial_byte: u8 = 0;
    let partial_bits: usize = top_remaining_bits;

    // Process slice in chunks of 8 for SIMD
    let chunks = slice.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let values = Simd::<i64, 8>::from_slice(chunk);
        let not_null_mask = values.simd_ne(null_val);
        let byte = not_null_mask.to_bitmask() as u8;
        let chunk_nulls = 8 - byte.count_ones() as usize;
        data_null_count += chunk_nulls;

        // Update statistics for non-null values
        if compute_stats && chunk_nulls < 8 {
            for &val in chunk {
                if val != i64::MIN {
                    stats.update(val);
                }
            }
        }

        // Merge with partial byte if needed
        if partial_bits == 0 {
            writer.write_all(&[byte])?;
        } else {
            // Combine: partial_byte has `partial_bits` bits in low positions
            // byte has 8 bits to add
            let combined = partial_byte | (byte << partial_bits);
            writer.write_all(&[combined])?;
            partial_byte = byte >> (8 - partial_bits);
        }
    }

    // Handle remainder (< 8 values)
    if !remainder.is_empty() {
        let mut rem_byte: u8 = 0;
        for (i, &val) in remainder.iter().enumerate() {
            if val != i64::MIN {
                rem_byte |= 1 << i;
                if compute_stats {
                    stats.update(val);
                }
            } else {
                data_null_count += 1;
            }
        }

        if partial_bits == 0 {
            writer.write_all(&[rem_byte])?;
        } else {
            let combined = partial_byte | (rem_byte << partial_bits);
            writer.write_all(&[combined])?;
            let total_bits = partial_bits + remainder.len();
            if total_bits > 8 {
                // Flush the remaining bits
                let leftover = rem_byte >> (8 - partial_bits);
                writer.write_all(&[leftover])?;
            }
        }
    } else if partial_bits > 0 {
        // Flush remaining partial byte
        writer.write_all(&[partial_byte])?;
    }

    Ok(DefLevelResult {
        null_count: data_null_count,
        max: stats.max,
        min: stats.min,
    })
}

/// Encodes definition levels for i32 slices using SIMD.
/// Note: Returns null_count for the data slice ONLY, not including column_top.
/// The caller should add column_top to get the total null count for statistics.
#[allow(unused_assignments)]
pub fn encode_i32_def_levels<W: Write>(
    writer: &mut W,
    slice: &[i32],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<i32>> {
    // Fast path: if no column_top and we can quickly verify no nulls, use RLE
    if column_top == 0 {
        if let Some(result) = try_encode_i32_all_present(writer, slice, compute_stats)? {
            return Ok(result);
        }
    }

    // Slow path: there are nulls or column_top, use bitpacked encoding
    encode_i32_def_levels_bitpacked(writer, slice, column_top, compute_stats)
}

/// Fast path: check if all i32 values are non-null and encode with RLE if so.
/// Returns None if any null (i32::MIN) is found.
fn try_encode_i32_all_present<W: Write>(
    writer: &mut W,
    slice: &[i32],
    compute_stats: bool,
) -> std::io::Result<Option<DefLevelResult<i32>>> {
    let null_val = Simd::<i32, 16>::splat(i32::MIN);

    // SIMD scan with deferred null check - no branching in hot loop
    let chunks = slice.chunks_exact(16);
    let remainder = chunks.remainder();

    // Accumulate validity without branching
    let mut all_valid_mask = Simd::<i32, 16>::splat(-1);
    let mut min_vec = Simd::<i32, 16>::splat(i32::MAX);
    let mut max_vec = Simd::<i32, 16>::splat(i32::MIN + 1);

    if compute_stats {
        for chunk in chunks {
            let values = Simd::<i32, 16>::from_slice(chunk);
            let is_not_null = values.simd_ne(null_val);
            all_valid_mask &= is_not_null.to_int();
            min_vec = min_vec.simd_min(values);
            max_vec = max_vec.simd_max(values);
        }
    } else {
        for chunk in chunks {
            let values = Simd::<i32, 16>::from_slice(chunk);
            let is_not_null = values.simd_ne(null_val);
            all_valid_mask &= is_not_null.to_int();
        }
    }

    // Check if any null was found
    if all_valid_mask.reduce_and() == 0 {
        return Ok(None);
    }

    // Check remainder
    for &val in remainder {
        if val == i32::MIN {
            return Ok(None);
        }
    }

    // All values are present! Use RLE encoding
    write_rle_all_ones(writer, slice.len())?;

    // Compute final statistics
    let (min_val, max_val) = if compute_stats {
        let mut min_i = i32::MAX;
        let mut max_i = i32::MIN + 1;

        for i in 0..16 {
            min_i = min_i.min(min_vec[i]);
            max_i = max_i.max(max_vec[i]);
        }

        for &val in remainder {
            min_i = min_i.min(val);
            max_i = max_i.max(val);
        }

        if slice.is_empty() {
            (None, None)
        } else {
            (Some(min_i), Some(max_i))
        }
    } else {
        (None, None)
    };

    Ok(Some(DefLevelResult {
        null_count: 0,
        max: max_val,
        min: min_val,
    }))
}

/// Slow path: bitpacked encoding for i32 slices with nulls or column_top
/// Note: Returns null_count for the data slice ONLY, not including column_top.
/// The caller should add column_top to get the total null count for statistics.
#[allow(unused_assignments)]
fn encode_i32_def_levels_bitpacked<W: Write>(
    writer: &mut W,
    slice: &[i32],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<i32>> {
    let null_val = Simd::<i32, 16>::splat(i32::MIN);
    let mut data_null_count = 0usize; // Only nulls in slice data, not column_top
    let mut stats = MaxMin::<i32>::new();

    let num_rows = column_top + slice.len();

    write_bitpacked_header(writer, num_rows)?;

    // Handle column_top prefix (these are nulls but not counted in data_null_count)
    let top_full_bytes = column_top / 8;
    let top_remaining_bits = column_top % 8;

    for _ in 0..top_full_bytes {
        writer.write_all(&[0u8])?;
    }

    let mut partial_byte: u8 = 0;
    let mut partial_bits: usize = top_remaining_bits;

    // Process in chunks of 16 (produces 2 bytes)
    let chunks = slice.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let values = Simd::<i32, 16>::from_slice(chunk);
        let not_null_mask = values.simd_ne(null_val);
        let mask16 = not_null_mask.to_bitmask() as u16;
        let chunk_nulls = 16 - mask16.count_ones() as usize;
        data_null_count += chunk_nulls;

        if compute_stats && chunk_nulls < 16 {
            for &val in chunk {
                if val != i32::MIN {
                    stats.update(val);
                }
            }
        }

        let bytes = mask16.to_le_bytes();

        if partial_bits == 0 {
            writer.write_all(&bytes)?;
        } else {
            // Handle partial byte merging
            let combined0 = partial_byte | (bytes[0] << partial_bits);
            writer.write_all(&[combined0])?;
            let combined1 = (bytes[0] >> (8 - partial_bits)) | (bytes[1] << partial_bits);
            writer.write_all(&[combined1])?;
            partial_byte = bytes[1] >> (8 - partial_bits);
        }
    }

    // Handle remainder
    if !remainder.is_empty() {
        // Process 8 at a time if possible
        let rem_chunks = remainder.chunks(8);
        for rem_chunk in rem_chunks {
            let mut rem_byte: u8 = 0;
            for (i, &val) in rem_chunk.iter().enumerate() {
                if val != i32::MIN {
                    rem_byte |= 1 << i;
                    if compute_stats {
                        stats.update(val);
                    }
                } else {
                    data_null_count += 1;
                }
            }

            if partial_bits == 0 {
                if rem_chunk.len() == 8 {
                    writer.write_all(&[rem_byte])?;
                } else {
                    partial_byte = rem_byte;
                    partial_bits = rem_chunk.len();
                }
            } else {
                let combined = partial_byte | (rem_byte << partial_bits);
                let total_bits = partial_bits + rem_chunk.len();
                if total_bits >= 8 {
                    writer.write_all(&[combined])?;
                    partial_byte = rem_byte >> (8 - partial_bits);
                    partial_bits = total_bits - 8;
                } else {
                    partial_byte = combined;
                    partial_bits = total_bits;
                }
            }
        }
    }

    if partial_bits > 0 {
        writer.write_all(&[partial_byte])?;
    }

    Ok(DefLevelResult {
        null_count: data_null_count,
        max: stats.max,
        min: stats.min,
    })
}

/// Encodes definition levels for f64 slices using SIMD.
/// NaN values are considered null.
#[allow(unused_assignments)]
pub fn encode_f64_def_levels<W: Write>(
    writer: &mut W,
    slice: &[f64],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<f64>> {
    // Fast path: if no column_top and we can quickly verify no NaNs, use RLE
    if column_top == 0 {
        if let Some(result) = try_encode_f64_all_present(writer, slice, compute_stats)? {
            return Ok(result);
        }
    }

    // Slow path: there are nulls or column_top, use bitpacked encoding
    encode_f64_def_levels_bitpacked(writer, slice, column_top, compute_stats)
}

/// Fast path: check if all values are non-null and encode with RLE if so.
/// Returns None if any null (NaN) is found.
fn try_encode_f64_all_present<W: Write>(
    writer: &mut W,
    slice: &[f64],
    compute_stats: bool,
) -> std::io::Result<Option<DefLevelResult<f64>>> {
    // SIMD scan with deferred NaN check - no branching in hot loop
    let chunks = slice.chunks_exact(8);
    let remainder = chunks.remainder();

    // NaN check using integer bitwise operations (more explicit than val == val):
    // A f64 is NaN iff (bits & 0x7FFFFFFFFFFFFFFF) > 0x7FF0000000000000
    // We check NOT NaN: abs_bits <= INFINITY_BITS
    const SIGN_MASK: i64 = 0x7FFFFFFFFFFFFFFF_u64 as i64;
    const INFINITY_BITS: i64 = 0x7FF0000000000000_u64 as i64;
    let sign_mask_vec = Simd::<i64, 8>::splat(SIGN_MASK);
    let infinity_vec = Simd::<i64, 8>::splat(INFINITY_BITS);

    // Accumulate: all_valid stays true only if no NaN found
    let mut all_valid_mask = Simd::<i64, 8>::splat(-1); // All 1s = all valid
    let mut min_vec = Simd::<f64, 8>::splat(f64::INFINITY);
    let mut max_vec = Simd::<f64, 8>::splat(f64::NEG_INFINITY);

    if compute_stats {
        for chunk in chunks {
            let values = Simd::<f64, 8>::from_slice(chunk);
            // Reinterpret as i64 for bitwise NaN check (zero-cost transmute)
            let bits: Simd<i64, 8> = unsafe { std::mem::transmute(values) };
            let abs_bits = bits & sign_mask_vec;
            // is_not_nan: true if abs_bits <= infinity (i.e., not NaN)
            let is_not_nan = abs_bits.simd_le(infinity_vec);
            all_valid_mask &= is_not_nan.to_int();
            // Update stats unconditionally (branch-free)
            min_vec = min_vec.simd_min(values);
            max_vec = max_vec.simd_max(values);
        }
    } else {
        // No stats needed - just check for NaN
        for chunk in chunks {
            let values = Simd::<f64, 8>::from_slice(chunk);
            let bits: Simd<i64, 8> = unsafe { std::mem::transmute(values) };
            let abs_bits = bits & sign_mask_vec;
            let is_not_nan = abs_bits.simd_le(infinity_vec);
            all_valid_mask &= is_not_nan.to_int();
        }
    }

    // Check if any NaN was found (any lane in mask is 0)
    if all_valid_mask.reduce_and() == 0 {
        return Ok(None);
    }

    // Check remainder using bitwise NaN check
    for &val in remainder {
        let bits = val.to_bits() as i64;
        if (bits & SIGN_MASK) > INFINITY_BITS {
            return Ok(None);
        }
    }

    // All values are present! Use RLE encoding
    write_rle_all_ones(writer, slice.len())?;

    // Compute final statistics from SIMD vectors
    let (min_val, max_val) = if compute_stats {
        let mut min_f = f64::INFINITY;
        let mut max_f = f64::NEG_INFINITY;

        // Reduce SIMD vectors
        for i in 0..8 {
            min_f = min_f.min(min_vec[i]);
            max_f = max_f.max(max_vec[i]);
        }

        // Handle remainder
        for &val in remainder {
            min_f = min_f.min(val);
            max_f = max_f.max(val);
        }

        if min_f == f64::INFINITY {
            (None, None)
        } else {
            (Some(min_f), Some(max_f))
        }
    } else {
        (None, None)
    };

    Ok(Some(DefLevelResult {
        null_count: 0,
        max: max_val,
        min: min_val,
    }))
}

/// Slow path: bitpacked encoding for slices with nulls or column_top
/// Note: Returns null_count for the data slice ONLY, not including column_top.
/// The caller should add column_top to get the total null count for statistics.
#[allow(unused_assignments)]
fn encode_f64_def_levels_bitpacked<W: Write>(
    writer: &mut W,
    slice: &[f64],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<f64>> {
    let mut data_null_count = 0usize; // Only nulls in slice data, not column_top
    let mut max_val: Option<f64> = None;
    let mut min_val: Option<f64> = None;

    let num_rows = column_top + slice.len();

    write_bitpacked_header(writer, num_rows)?;

    let top_full_bytes = column_top / 8;
    let top_remaining_bits = column_top % 8;

    // Write column_top nulls but don't count them in data_null_count
    for _ in 0..top_full_bytes {
        writer.write_all(&[0u8])?;
    }

    let mut partial_byte: u8 = 0;
    let partial_bits: usize = top_remaining_bits;

    // NaN check using integer bitwise operations
    const SIGN_MASK: i64 = 0x7FFFFFFFFFFFFFFF_u64 as i64;
    const INFINITY_BITS: i64 = 0x7FF0000000000000_u64 as i64;
    let sign_mask_vec = Simd::<i64, 8>::splat(SIGN_MASK);
    let infinity_vec = Simd::<i64, 8>::splat(INFINITY_BITS);

    // Process in chunks of 8
    let chunks = slice.chunks_exact(8);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let values = Simd::<f64, 8>::from_slice(chunk);
        // Bitwise NaN check: NOT NaN if abs_bits <= infinity
        let bits: Simd<i64, 8> = unsafe { std::mem::transmute(values) };
        let abs_bits = bits & sign_mask_vec;
        let is_not_nan = abs_bits.simd_le(infinity_vec);
        let byte = is_not_nan.to_bitmask() as u8;
        let chunk_nulls = 8 - byte.count_ones() as usize;
        data_null_count += chunk_nulls;

        if compute_stats && chunk_nulls < 8 {
            for &val in chunk {
                if !val.is_nan() {
                    max_val = Some(max_val.map_or(val, |m| m.max(val)));
                    min_val = Some(min_val.map_or(val, |m| m.min(val)));
                }
            }
        }

        if partial_bits == 0 {
            writer.write_all(&[byte])?;
        } else {
            let combined = partial_byte | (byte << partial_bits);
            writer.write_all(&[combined])?;
            partial_byte = byte >> (8 - partial_bits);
        }
    }

    // Handle remainder
    if !remainder.is_empty() {
        let mut rem_byte: u8 = 0;
        for (i, &val) in remainder.iter().enumerate() {
            if !val.is_nan() {
                rem_byte |= 1 << i;
                if compute_stats {
                    max_val = Some(max_val.map_or(val, |m| m.max(val)));
                    min_val = Some(min_val.map_or(val, |m| m.min(val)));
                }
            } else {
                data_null_count += 1;
            }
        }

        if partial_bits == 0 {
            writer.write_all(&[rem_byte])?;
        } else {
            let combined = partial_byte | (rem_byte << partial_bits);
            writer.write_all(&[combined])?;
            let total_bits = partial_bits + remainder.len();
            if total_bits > 8 {
                let leftover = rem_byte >> (8 - partial_bits);
                writer.write_all(&[leftover])?;
            }
        }
    } else if partial_bits > 0 {
        writer.write_all(&[partial_byte])?;
    }

    Ok(DefLevelResult {
        null_count: data_null_count,
        max: max_val,
        min: min_val,
    })
}

/// Encodes definition levels for f32 slices using SIMD.
/// NaN values are considered null.
#[allow(unused_assignments)]
pub fn encode_f32_def_levels<W: Write>(
    writer: &mut W,
    slice: &[f32],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<f32>> {
    // Fast path: if no column_top and we can quickly verify no NaNs, use RLE
    if column_top == 0 {
        if let Some(result) = try_encode_f32_all_present(writer, slice, compute_stats)? {
            return Ok(result);
        }
    }

    // Slow path: there are nulls or column_top, use bitpacked encoding
    encode_f32_def_levels_bitpacked(writer, slice, column_top, compute_stats)
}

/// Fast path: check if all f32 values are non-null and encode with RLE if so.
/// Returns None if any null (NaN) is found.
fn try_encode_f32_all_present<W: Write>(
    writer: &mut W,
    slice: &[f32],
    compute_stats: bool,
) -> std::io::Result<Option<DefLevelResult<f32>>> {
    // SIMD scan with deferred NaN check - no branching in hot loop
    let chunks = slice.chunks_exact(16);
    let remainder = chunks.remainder();

    // NaN check using integer bitwise operations:
    // A f32 is NaN iff (bits & 0x7FFFFFFF) > 0x7F800000
    // We check NOT NaN: abs_bits <= INFINITY_BITS
    const SIGN_MASK: i32 = 0x7FFFFFFF_u32 as i32;
    const INFINITY_BITS: i32 = 0x7F800000_u32 as i32;
    let sign_mask_vec = Simd::<i32, 16>::splat(SIGN_MASK);
    let infinity_vec = Simd::<i32, 16>::splat(INFINITY_BITS);

    // Accumulate: all_valid stays true only if no NaN found
    let mut all_valid_mask = Simd::<i32, 16>::splat(-1); // All 1s = all valid
    let mut min_vec = Simd::<f32, 16>::splat(f32::INFINITY);
    let mut max_vec = Simd::<f32, 16>::splat(f32::NEG_INFINITY);

    if compute_stats {
        for chunk in chunks {
            let values = Simd::<f32, 16>::from_slice(chunk);
            // Reinterpret as i32 for bitwise NaN check (zero-cost transmute)
            let bits: Simd<i32, 16> = unsafe { std::mem::transmute(values) };
            let abs_bits = bits & sign_mask_vec;
            // is_not_nan: true if abs_bits <= infinity (i.e., not NaN)
            let is_not_nan = abs_bits.simd_le(infinity_vec);
            all_valid_mask &= is_not_nan.to_int();
            // Update stats unconditionally (branch-free)
            min_vec = min_vec.simd_min(values);
            max_vec = max_vec.simd_max(values);
        }
    } else {
        // No stats needed - just check for NaN
        for chunk in chunks {
            let values = Simd::<f32, 16>::from_slice(chunk);
            let bits: Simd<i32, 16> = unsafe { std::mem::transmute(values) };
            let abs_bits = bits & sign_mask_vec;
            let is_not_nan = abs_bits.simd_le(infinity_vec);
            all_valid_mask &= is_not_nan.to_int();
        }
    }

    // Check if any NaN was found (any lane in mask is 0)
    if all_valid_mask.reduce_and() == 0 {
        return Ok(None);
    }

    // Check remainder using bitwise NaN check
    for &val in remainder {
        let bits = val.to_bits() as i32;
        if (bits & SIGN_MASK) > INFINITY_BITS {
            return Ok(None);
        }
    }

    // All values are present! Use RLE encoding
    write_rle_all_ones(writer, slice.len())?;

    // Compute final statistics from SIMD vectors
    let (min_val, max_val) = if compute_stats {
        let mut min_f = f32::INFINITY;
        let mut max_f = f32::NEG_INFINITY;

        // Reduce SIMD vectors
        for i in 0..16 {
            min_f = min_f.min(min_vec[i]);
            max_f = max_f.max(max_vec[i]);
        }

        // Handle remainder
        for &val in remainder {
            min_f = min_f.min(val);
            max_f = max_f.max(val);
        }

        if min_f == f32::INFINITY {
            (None, None)
        } else {
            (Some(min_f), Some(max_f))
        }
    } else {
        (None, None)
    };

    Ok(Some(DefLevelResult {
        null_count: 0,
        max: max_val,
        min: min_val,
    }))
}

/// Slow path: bitpacked encoding for f32 slices with nulls or column_top
/// Note: Returns null_count for the data slice ONLY, not including column_top.
/// The caller should add column_top to get the total null count for statistics.
#[allow(unused_assignments)]
fn encode_f32_def_levels_bitpacked<W: Write>(
    writer: &mut W,
    slice: &[f32],
    column_top: usize,
    compute_stats: bool,
) -> std::io::Result<DefLevelResult<f32>> {
    let mut data_null_count = 0usize; // Only nulls in slice data, not column_top
    let mut max_val: Option<f32> = None;
    let mut min_val: Option<f32> = None;

    let num_rows = column_top + slice.len();

    write_bitpacked_header(writer, num_rows)?;

    let top_full_bytes = column_top / 8;
    let top_remaining_bits = column_top % 8;

    // Write column_top nulls but don't count them in data_null_count
    for _ in 0..top_full_bytes {
        writer.write_all(&[0u8])?;
    }

    let mut partial_byte: u8 = 0;
    let mut partial_bits: usize = top_remaining_bits;

    // NaN check using integer bitwise operations for f32
    const SIGN_MASK: i32 = 0x7FFFFFFF_u32 as i32;
    const INFINITY_BITS: i32 = 0x7F800000_u32 as i32;
    let sign_mask_vec = Simd::<i32, 16>::splat(SIGN_MASK);
    let infinity_vec = Simd::<i32, 16>::splat(INFINITY_BITS);

    // Process in chunks of 16
    let chunks = slice.chunks_exact(16);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let values = Simd::<f32, 16>::from_slice(chunk);
        // Bitwise NaN check: NOT NaN if abs_bits <= infinity
        let bits: Simd<i32, 16> = unsafe { std::mem::transmute(values) };
        let abs_bits = bits & sign_mask_vec;
        let is_not_nan = abs_bits.simd_le(infinity_vec);
        let mask16 = is_not_nan.to_bitmask() as u16;
        let chunk_nulls = 16 - mask16.count_ones() as usize;
        data_null_count += chunk_nulls;

        if compute_stats && chunk_nulls < 16 {
            for &val in chunk {
                if !val.is_nan() {
                    max_val = Some(max_val.map_or(val, |m| m.max(val)));
                    min_val = Some(min_val.map_or(val, |m| m.min(val)));
                }
            }
        }

        let bytes = mask16.to_le_bytes();

        if partial_bits == 0 {
            writer.write_all(&bytes)?;
        } else {
            let combined0 = partial_byte | (bytes[0] << partial_bits);
            writer.write_all(&[combined0])?;
            let combined1 = (bytes[0] >> (8 - partial_bits)) | (bytes[1] << partial_bits);
            writer.write_all(&[combined1])?;
            partial_byte = bytes[1] >> (8 - partial_bits);
        }
    }

    // Handle remainder
    if !remainder.is_empty() {
        let rem_chunks = remainder.chunks(8);
        for rem_chunk in rem_chunks {
            let mut rem_byte: u8 = 0;
            for (i, &val) in rem_chunk.iter().enumerate() {
                if !val.is_nan() {
                    rem_byte |= 1 << i;
                    if compute_stats {
                        max_val = Some(max_val.map_or(val, |m| m.max(val)));
                        min_val = Some(min_val.map_or(val, |m| m.min(val)));
                    }
                } else {
                    data_null_count += 1;
                }
            }

            if partial_bits == 0 {
                if rem_chunk.len() == 8 {
                    writer.write_all(&[rem_byte])?;
                } else {
                    partial_byte = rem_byte;
                    partial_bits = rem_chunk.len();
                }
            } else {
                let combined = partial_byte | (rem_byte << partial_bits);
                let total_bits = partial_bits + rem_chunk.len();
                if total_bits >= 8 {
                    writer.write_all(&[combined])?;
                    partial_byte = rem_byte >> (8 - partial_bits);
                    partial_bits = total_bits - 8;
                } else {
                    partial_byte = combined;
                    partial_bits = total_bits;
                }
            }
        }
    }

    if partial_bits > 0 {
        writer.write_all(&[partial_byte])?;
    }

    Ok(DefLevelResult {
        null_count: data_null_count,
        max: max_val,
        min: min_val,
    })
}

/// Writes the hybrid RLE bitpacked header.
/// Format: ULEB128 encoded value where (value >> 1) is the number of 8-value groups,
/// and (value & 1) == 1 indicates bitpacked encoding.
#[inline]
fn write_bitpacked_header<W: Write>(writer: &mut W, num_values: usize) -> std::io::Result<()> {
    // Number of groups of 8 values (rounded up)
    let num_groups = num_values.div_ceil(8);
    // Header: (num_groups << 1) | 1 (the 1 indicates bitpacked)
    let header = ((num_groups as u64) << 1) | 1;

    // ULEB128 encode
    let mut value = header;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }
    Ok(())
}

/// Writes RLE encoding for "all values present" (all definition levels = 1).
/// This is much more compact than bitpacked when there are no nulls.
/// Format: ULEB128(count << 1 | 0) followed by the value byte (0x01).
#[inline]
fn write_rle_all_ones<W: Write>(writer: &mut W, count: usize) -> std::io::Result<()> {
    // RLE header: (count << 1) | 0 (the 0 indicates RLE, not bitpacked)
    let header = (count as u64) << 1;

    // ULEB128 encode the header
    let mut value = header;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        writer.write_all(&[byte])?;
        if value == 0 {
            break;
        }
    }

    // Write the repeated value (1 = present)
    writer.write_all(&[0x01])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

    /// Helper to decode definition levels from the encoded buffer.
    /// Returns a Vec<bool> where true = present, false = null.
    fn decode_def_levels(buffer: &[u8], num_values: usize) -> Vec<bool> {
        let mut decoder = Decoder::new(buffer, 1);
        let mut result = Vec::with_capacity(num_values);

        while result.len() < num_values {
            match decoder.next() {
                Some(Ok(HybridEncoded::Bitpacked(packed))) => {
                    // Decode bitpacked values
                    for byte in packed {
                        for bit in 0..8 {
                            if result.len() < num_values {
                                result.push((byte >> bit) & 1 == 1);
                            }
                        }
                    }
                }
                Some(Ok(HybridEncoded::Rle(value_bytes, count))) => {
                    // For 1-bit values, the value is in the first byte
                    let val = !value_bytes.is_empty() && value_bytes[0] != 0;
                    for _ in 0..count.min(num_values - result.len()) {
                        result.push(val);
                    }
                }
                _ => break,
            }
        }
        result.truncate(num_values);
        result
    }

    // ==========================================================================
    // i64 tests
    // ==========================================================================

    #[test]
    fn test_i64_no_nulls_no_column_top() {
        let data: Vec<i64> = (0..100).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(99));

        // Verify decoding - RLE encoding for all ones
        let decoded = decode_def_levels(&buffer, 100);
        assert_eq!(decoded.len(), 100);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_i64_no_nulls_no_stats() {
        let data: Vec<i64> = (0..100).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, false).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_i64_all_nulls() {
        let data: Vec<i64> = vec![i64::MIN; 100];
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 100);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);

        let decoded = decode_def_levels(&buffer, 100);
        assert!(decoded.iter().all(|&x| !x), "All values should be null");
    }

    #[test]
    fn test_i64_with_nulls() {
        let mut data: Vec<i64> = (0..100).collect();
        data[10] = i64::MIN;
        data[50] = i64::MIN;
        data[99] = i64::MIN;

        let mut buffer = Vec::new();
        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 3);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(98));

        let decoded = decode_def_levels(&buffer, 100);
        assert!(!decoded[10], "Index 10 should be null");
        assert!(!decoded[50], "Index 50 should be null");
        assert!(!decoded[99], "Index 99 should be null");
        assert!(decoded[0], "Index 0 should be present");
        assert!(decoded[11], "Index 11 should be present");
    }

    #[test]
    fn test_i64_with_column_top_no_nulls() {
        let data: Vec<i64> = (0..50).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 10, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(49));

        let decoded = decode_def_levels(&buffer, 60);
        // First 10 should be null (column_top)
        for i in 0..10 {
            assert!(!decoded[i], "Column top index {} should be null", i);
        }
        // Rest should be present
        for i in 10..60 {
            assert!(decoded[i], "Data index {} should be present", i);
        }
    }

    #[test]
    fn test_i64_with_column_top_with_nulls() {
        let mut data: Vec<i64> = (0..50).collect();
        data[5] = i64::MIN;
        data[25] = i64::MIN;

        let mut buffer = Vec::new();
        let result = encode_i64_def_levels(&mut buffer, &data, 10, true).unwrap();

        assert_eq!(result.null_count, 2);

        let decoded = decode_def_levels(&buffer, 60);
        // First 10 are column_top nulls
        for i in 0..10 {
            assert!(!decoded[i], "Column top index {} should be null", i);
        }
        // Index 15 (10 + 5) and 35 (10 + 25) should be null in data
        assert!(!decoded[15], "Data null at index 15");
        assert!(!decoded[35], "Data null at index 35");
    }

    #[test]
    fn test_i64_all_column_top() {
        let data: Vec<i64> = vec![];
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 20, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);

        let decoded = decode_def_levels(&buffer, 20);
        assert!(decoded.iter().all(|&x| !x), "All column_top should be null");
    }

    #[test]
    fn test_i64_empty() {
        let data: Vec<i64> = vec![];
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_i64_single_element() {
        let data = vec![42i64];
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(42));
        assert_eq!(result.max, Some(42));

        let decoded = decode_def_levels(&buffer, 1);
        assert!(decoded[0]);
    }

    #[test]
    fn test_i64_single_null() {
        let data = vec![i64::MIN];
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_i64_exact_simd_chunk_size() {
        // Exact multiple of 8 (SIMD chunk size for i64)
        let data: Vec<i64> = (0..64).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(63));

        let decoded = decode_def_levels(&buffer, 64);
        assert!(decoded.iter().all(|&x| x));
    }

    #[test]
    fn test_i64_remainder_handling() {
        // 67 elements = 8 * 8 + 3 remainder
        let data: Vec<i64> = (0..67).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.max, Some(66));

        let decoded = decode_def_levels(&buffer, 67);
        assert_eq!(decoded.len(), 67);
        assert!(decoded.iter().all(|&x| x));
    }

    #[test]
    fn test_i64_null_in_remainder() {
        // 67 elements, null in remainder
        let mut data: Vec<i64> = (0..67).collect();
        data[65] = i64::MIN; // In the remainder portion

        let mut buffer = Vec::new();
        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);

        let decoded = decode_def_levels(&buffer, 67);
        assert!(!decoded[65]);
        assert!(decoded[64]);
        assert!(decoded[66]);
    }

    #[test]
    fn test_i64_column_top_partial_byte() {
        // column_top = 3 (not byte-aligned), data = 10 elements
        let data: Vec<i64> = (0..10).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 3, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 13);
        assert!(!decoded[0]);
        assert!(!decoded[1]);
        assert!(!decoded[2]);
        for i in 3..13 {
            assert!(decoded[i], "Index {} should be present", i);
        }
    }

    #[test]
    fn test_i64_large_data() {
        let data: Vec<i64> = (0..10000).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(9999));
    }

    #[test]
    fn test_i64_alternating_nulls() {
        let data: Vec<i64> = (0..100)
            .map(|i| if i % 2 == 0 { i } else { i64::MIN })
            .collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 50);

        let decoded = decode_def_levels(&buffer, 100);
        for (i, &present) in decoded.iter().enumerate() {
            assert_eq!(present, i % 2 == 0, "Index {} mismatch", i);
        }
    }

    // ==========================================================================
    // i32 tests
    // ==========================================================================

    #[test]
    fn test_i32_no_nulls_no_column_top() {
        let data: Vec<i32> = (0..100).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(99));

        let decoded = decode_def_levels(&buffer, 100);
        assert!(decoded.iter().all(|&x| x));
    }

    #[test]
    fn test_i32_all_nulls() {
        let data: Vec<i32> = vec![i32::MIN; 100];
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 100);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_i32_with_nulls() {
        let mut data: Vec<i32> = (0..100).map(|x| x as i32).collect();
        data[5] = i32::MIN;
        data[50] = i32::MIN;
        data[95] = i32::MIN;

        let mut buffer = Vec::new();
        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 3);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(99));

        let decoded = decode_def_levels(&buffer, 100);
        assert!(!decoded[5]);
        assert!(!decoded[50]);
        assert!(!decoded[95]);
    }

    #[test]
    fn test_i32_with_column_top() {
        let data: Vec<i32> = (0..50).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 15, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 65);
        for i in 0..15 {
            assert!(!decoded[i], "Column top index {} should be null", i);
        }
        for i in 15..65 {
            assert!(decoded[i], "Data index {} should be present", i);
        }
    }

    #[test]
    fn test_i32_all_column_top() {
        let data: Vec<i32> = vec![];
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 25, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_i32_exact_simd_chunk_size() {
        // Exact multiple of 16 (SIMD chunk size for i32)
        let data: Vec<i32> = (0..64).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(63));
    }

    #[test]
    fn test_i32_remainder_handling() {
        // 67 elements = 4 * 16 + 3 remainder
        let data: Vec<i32> = (0..67).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.max, Some(66));

        let decoded = decode_def_levels(&buffer, 67);
        assert_eq!(decoded.len(), 67);
    }

    #[test]
    fn test_i32_column_top_partial_byte() {
        let data: Vec<i32> = (0..10).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let _result = encode_i32_def_levels(&mut buffer, &data, 5, true).unwrap();

        let decoded = decode_def_levels(&buffer, 15);
        for i in 0..5 {
            assert!(!decoded[i]);
        }
        for i in 5..15 {
            assert!(decoded[i]);
        }
    }

    #[test]
    fn test_i32_no_stats() {
        let data: Vec<i32> = (0..100).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, false).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    // ==========================================================================
    // f64 tests
    // ==========================================================================

    #[test]
    fn test_f64_no_nulls_no_column_top() {
        let data: Vec<f64> = (0..100).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(99.0));

        let decoded = decode_def_levels(&buffer, 100);
        assert!(decoded.iter().all(|&x| x));
    }

    #[test]
    fn test_f64_all_nulls() {
        let data: Vec<f64> = vec![f64::NAN; 100];
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 100);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_f64_with_nans() {
        let mut data: Vec<f64> = (0..100).map(|x| x as f64).collect();
        data[10] = f64::NAN;
        data[50] = f64::NAN;
        data[99] = f64::NAN;

        let mut buffer = Vec::new();
        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 3);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(98.0));

        let decoded = decode_def_levels(&buffer, 100);
        assert!(!decoded[10]);
        assert!(!decoded[50]);
        assert!(!decoded[99]);
    }

    #[test]
    fn test_f64_with_column_top() {
        let data: Vec<f64> = (0..50).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 10, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(49.0));

        let decoded = decode_def_levels(&buffer, 60);
        for i in 0..10 {
            assert!(!decoded[i]);
        }
        for i in 10..60 {
            assert!(decoded[i]);
        }
    }

    #[test]
    fn test_f64_all_column_top() {
        let data: Vec<f64> = vec![];
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 20, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_f64_special_values() {
        let data = vec![f64::NEG_INFINITY, -1.0, 0.0, 1.0, f64::INFINITY, f64::NAN];
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);
        assert_eq!(result.min, Some(f64::NEG_INFINITY));
        assert_eq!(result.max, Some(f64::INFINITY));

        let decoded = decode_def_levels(&buffer, 6);
        assert!(decoded[0]); // NEG_INFINITY
        assert!(decoded[4]); // INFINITY
        assert!(!decoded[5]); // NAN
    }

    #[test]
    fn test_f64_negative_nan() {
        // Negative NaN has different bit pattern
        let neg_nan = f64::from_bits(0xFFF8000000000000);
        assert!(neg_nan.is_nan());

        let data = vec![1.0, neg_nan, 3.0];
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);

        let decoded = decode_def_levels(&buffer, 3);
        assert!(!decoded[1]);
    }

    #[test]
    fn test_f64_exact_simd_chunk_size() {
        let data: Vec<f64> = (0..64).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.max, Some(63.0));
    }

    #[test]
    fn test_f64_remainder_handling() {
        let data: Vec<f64> = (0..67).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 67);
        assert_eq!(decoded.len(), 67);
    }

    #[test]
    fn test_f64_no_stats() {
        let data: Vec<f64> = (0..100).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, false).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    // ==========================================================================
    // f32 tests
    // ==========================================================================

    #[test]
    fn test_f32_no_nulls_no_column_top() {
        let data: Vec<f32> = (0..100).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(99.0));
    }

    #[test]
    fn test_f32_all_nulls() {
        let data: Vec<f32> = vec![f32::NAN; 100];
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 100);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_f32_with_nans() {
        let mut data: Vec<f32> = (0..100).map(|x| x as f32).collect();
        data[10] = f32::NAN;
        data[50] = f32::NAN;
        data[95] = f32::NAN;

        let mut buffer = Vec::new();
        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 3);

        let decoded = decode_def_levels(&buffer, 100);
        assert!(!decoded[10]);
        assert!(!decoded[50]);
        assert!(!decoded[95]);
    }

    #[test]
    fn test_f32_with_column_top() {
        let data: Vec<f32> = (0..50).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 15, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 65);
        for i in 0..15 {
            assert!(!decoded[i]);
        }
        for i in 15..65 {
            assert!(decoded[i]);
        }
    }

    #[test]
    fn test_f32_all_column_top() {
        let data: Vec<f32> = vec![];
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 25, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    #[test]
    fn test_f32_special_values() {
        let data = vec![
            f32::NEG_INFINITY,
            -1.0f32,
            0.0f32,
            1.0f32,
            f32::INFINITY,
            f32::NAN,
        ];
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);
        assert_eq!(result.min, Some(f32::NEG_INFINITY));
        assert_eq!(result.max, Some(f32::INFINITY));
    }

    #[test]
    fn test_f32_exact_simd_chunk_size() {
        // Exact multiple of 16 (SIMD chunk size for f32)
        let data: Vec<f32> = (0..64).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.max, Some(63.0));
    }

    #[test]
    fn test_f32_remainder_handling() {
        let data: Vec<f32> = (0..67).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 67);
        assert_eq!(decoded.len(), 67);
    }

    #[test]
    fn test_f32_column_top_partial_byte() {
        let data: Vec<f32> = (0..10).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let _result = encode_f32_def_levels(&mut buffer, &data, 5, true).unwrap();

        let decoded = decode_def_levels(&buffer, 15);
        for i in 0..5 {
            assert!(!decoded[i]);
        }
        for i in 5..15 {
            assert!(decoded[i]);
        }
    }

    #[test]
    fn test_f32_no_stats() {
        let data: Vec<f32> = (0..100).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, false).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, None);
        assert_eq!(result.max, None);
    }

    // ==========================================================================
    // Header encoding tests
    // ==========================================================================

    #[test]
    fn test_bitpacked_header_small() {
        let mut buffer = Vec::new();
        write_bitpacked_header(&mut buffer, 8).unwrap();

        // 8 values = 1 group of 8
        // Header = (1 << 1) | 1 = 3
        assert_eq!(buffer, vec![3]);
    }

    #[test]
    fn test_bitpacked_header_medium() {
        let mut buffer = Vec::new();
        write_bitpacked_header(&mut buffer, 100).unwrap();

        // 100 values = ceil(100/8) = 13 groups
        // Header = (13 << 1) | 1 = 27
        assert_eq!(buffer, vec![27]);
    }

    #[test]
    fn test_bitpacked_header_large() {
        let mut buffer = Vec::new();
        write_bitpacked_header(&mut buffer, 1000).unwrap();

        // 1000 values = ceil(1000/8) = 125 groups
        // Header = (125 << 1) | 1 = 251
        assert_eq!(buffer, vec![251, 1]);
    }

    #[test]
    fn test_rle_all_ones_small() {
        let mut buffer = Vec::new();
        write_rle_all_ones(&mut buffer, 100).unwrap();

        // Header = 100 << 1 = 200, then value byte = 1
        assert_eq!(buffer, vec![200, 1, 1]);
    }

    #[test]
    fn test_rle_all_ones_large() {
        let mut buffer = Vec::new();
        write_rle_all_ones(&mut buffer, 10000).unwrap();

        // Header = 10000 << 1 = 20000 in ULEB128
        // 20000 = 0x4E20, ULEB128 = [0xA0, 0x9C, 0x01]
        // Then value byte = 1
        assert!(buffer.len() >= 3);
        assert_eq!(*buffer.last().unwrap(), 1);
    }

    // ==========================================================================
    // Edge case tests
    // ==========================================================================

    #[test]
    fn test_mixed_column_top_and_null_boundary() {
        // column_top = 7 (leaves 1 bit in partial byte)
        // data has null at position 0 (bit position 7 in the byte)
        let mut data: Vec<i64> = (0..10).collect();
        data[0] = i64::MIN;

        let mut buffer = Vec::new();
        let result = encode_i64_def_levels(&mut buffer, &data, 7, true).unwrap();

        assert_eq!(result.null_count, 1);

        let decoded = decode_def_levels(&buffer, 17);
        // First 7 are column_top (null)
        for i in 0..7 {
            assert!(!decoded[i], "Column top at {} should be null", i);
        }
        // Index 7 is data[0] which is null
        assert!(!decoded[7], "Data null at index 7");
        // Rest should be present
        for i in 8..17 {
            assert!(decoded[i], "Data at {} should be present", i);
        }
    }

    #[test]
    fn test_column_top_exactly_byte_aligned() {
        // column_top = 16 (exactly 2 bytes)
        let data: Vec<i64> = (0..10).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 16, true).unwrap();

        assert_eq!(result.null_count, 0);

        let decoded = decode_def_levels(&buffer, 26);
        for i in 0..16 {
            assert!(!decoded[i]);
        }
        for i in 16..26 {
            assert!(decoded[i]);
        }
    }

    #[test]
    fn test_all_first_chunk_null_rest_present() {
        // First 8 values null, rest present
        let mut data: Vec<i64> = (0..20).collect();
        for i in 0..8 {
            data[i] = i64::MIN;
        }

        let mut buffer = Vec::new();
        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 8);
        assert_eq!(result.min, Some(8));
        assert_eq!(result.max, Some(19));
    }

    #[test]
    fn test_stats_with_negative_values() {
        let data: Vec<i64> = (-50..50).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(-50));
        assert_eq!(result.max, Some(49));
    }

    #[test]
    fn test_f64_stats_with_negative_values() {
        let data: Vec<f64> = (-50..50).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(-50.0));
        assert_eq!(result.max, Some(49.0));
    }

    // ==========================================================================
    // Fast path (RLE) verification tests
    // ==========================================================================

    #[test]
    fn test_i32_rle_fast_path_used() {
        // No nulls, no column_top - should use compact RLE encoding
        let data: Vec<i32> = (0..1000).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(999));
        // RLE encoding for 1000 "all present" should be very compact
        // Header (ULEB128 of 2000) + value byte (0x01) = ~3 bytes
        assert!(
            buffer.len() < 10,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values - all should be present
        let decoded = decode_def_levels(&buffer, 1000);
        assert_eq!(decoded.len(), 1000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_i32_bitpacked_slow_path_used() {
        // Has nulls - should use bitpacked encoding
        let mut data: Vec<i32> = (0..1000).map(|x| x as i32).collect();
        data[500] = i32::MIN;
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(999));
        // Bitpacked encoding: header + ceil(1000/8) = header + 125 bytes
        assert!(
            buffer.len() > 100,
            "Bitpacked should be larger, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 1000);
        assert!(!decoded[500], "Index 500 should be null");
        assert!(decoded[499], "Index 499 should be present");
        assert!(decoded[501], "Index 501 should be present");
    }

    #[test]
    fn test_f32_rle_fast_path_used() {
        // No nulls, no column_top - should use compact RLE encoding
        let data: Vec<f32> = (0..1000).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(999.0));
        assert!(
            buffer.len() < 10,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 1000);
        assert_eq!(decoded.len(), 1000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_f32_bitpacked_slow_path_used() {
        // Has NaN - should use bitpacked encoding
        let mut data: Vec<f32> = (0..1000).map(|x| x as f32).collect();
        data[500] = f32::NAN;
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 1);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(999.0));
        assert!(
            buffer.len() > 100,
            "Bitpacked should be larger, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 1000);
        assert!(!decoded[500], "Index 500 should be null (NaN)");
        assert!(decoded[499], "Index 499 should be present");
        assert!(decoded[501], "Index 501 should be present");
    }

    #[test]
    fn test_i64_rle_fast_path_used() {
        let data: Vec<i64> = (0..1000).collect();
        let mut buffer = Vec::new();

        let result = encode_i64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(999));
        assert!(
            buffer.len() < 10,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 1000);
        assert_eq!(decoded.len(), 1000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_f64_rle_fast_path_used() {
        let data: Vec<f64> = (0..1000).map(|x| x as f64).collect();
        let mut buffer = Vec::new();

        let result = encode_f64_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(999.0));
        assert!(
            buffer.len() < 10,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 1000);
        assert_eq!(decoded.len(), 1000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_i32_stats_with_negative_values() {
        let data: Vec<i32> = (-50..50).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(-50));
        assert_eq!(result.max, Some(49));

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 100);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_f32_stats_with_negative_values() {
        let data: Vec<f32> = (-50..50).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(-50.0));
        assert_eq!(result.max, Some(49.0));

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 100);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_i32_fast_path_large_data() {
        let data: Vec<i32> = (0..100000).map(|x| x as i32).collect();
        let mut buffer = Vec::new();

        let result = encode_i32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0));
        assert_eq!(result.max, Some(99999));
        // RLE should still be compact even for large data
        assert!(
            buffer.len() < 20,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 100000);
        assert_eq!(decoded.len(), 100000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }

    #[test]
    fn test_f32_fast_path_large_data() {
        let data: Vec<f32> = (0..100000).map(|x| x as f32).collect();
        let mut buffer = Vec::new();

        let result = encode_f32_def_levels(&mut buffer, &data, 0, true).unwrap();

        assert_eq!(result.null_count, 0);
        assert_eq!(result.min, Some(0.0));
        assert_eq!(result.max, Some(99999.0));
        assert!(
            buffer.len() < 20,
            "RLE should be compact, got {} bytes",
            buffer.len()
        );

        // Verify decoded values
        let decoded = decode_def_levels(&buffer, 100000);
        assert_eq!(decoded.len(), 100000);
        assert!(decoded.iter().all(|&x| x), "All values should be present");
    }
}
