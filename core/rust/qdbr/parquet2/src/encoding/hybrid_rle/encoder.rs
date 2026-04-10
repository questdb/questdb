use crate::encoding::bitpacked;
use crate::encoding::{ceil8, uleb128};

use std::io::Write;

use super::bitpacked_encode;

/// Minimum run length before we emit an RLE block instead of bitpacking.
/// At num_bits >= 2, RLE costs ~2-3 bytes for any run length, while bitpacking
/// costs count * num_bits / 8 bytes. At 8 values and 2 bits, bitpacking costs
/// 2 bytes vs RLE's ~3 bytes, so 8 is roughly break-even. We use 8 as the
/// threshold since it matches the bitpacked group size and RLE wins clearly
/// for longer runs.
const MIN_RLE_RUN: usize = 8;

/// RLE-hybrid encoding of `u32`. Emits interleaved RLE and bitpacked blocks.
///
/// For `num_bits <= 1` or when data has no runs >= MIN_RLE_RUN, this falls back
/// to pure bitpacking. Otherwise, runs of identical values are RLE-encoded
/// and remaining values are bitpacked.
pub fn encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    iterator: I,
    length: usize,
    num_bits: u32,
) -> std::io::Result<()> {
    let num_bits = num_bits as u8;

    // Fast path: for 0 or 1 bit width, RLE doesn't help — use pure bitpacking.
    // Also use pure bitpacking for empty input.
    if num_bits <= 1 || length == 0 {
        encode_bitpacked_header(writer, length)?;
        bitpacked_encode_u32(writer, iterator, length, num_bits as usize)?;
        return Ok(());
    }

    let values: Vec<u32> = iterator.collect();
    debug_assert_eq!(values.len(), length);

    // Scan for runs and emit interleaved RLE/bitpacked blocks.
    //
    // Non-terminal bitpacked blocks must have lengths that are multiples of 8,
    // because the bitpacked header only encodes group count (groups of 8) and
    // the decoder reads exactly num_groups * 8 values. Padding zeros in a
    // non-terminal block would be read as real values, corrupting the stream.
    //
    // When pending values before a run aren't 8-aligned, we absorb values from
    // the run's start into the bitpacked block to reach a multiple of 8.
    let mut pending_start: usize = 0; // start of non-run values to be bitpacked
    let mut i: usize = 0;

    while i < length {
        let run_len = count_run(&values, i);
        if run_len >= MIN_RLE_RUN {
            let pending_count = i - pending_start;
            let remainder = pending_count % 8;

            if remainder == 0 {
                // Pending is already 8-aligned — flush and emit RLE.
                if pending_count > 0 {
                    emit_bitpacked_block(writer, &values[pending_start..i], num_bits)?;
                }
                emit_rle_block(writer, values[i], run_len, num_bits)?;
                i += run_len;
                pending_start = i;
            } else {
                // Absorb values from the run to pad bitpacked block to 8-aligned.
                let pad_needed = 8 - remainder;
                let rle_remaining = run_len - pad_needed;
                if rle_remaining >= MIN_RLE_RUN {
                    // After absorbing, still enough for a worthwhile RLE block.
                    let bp_end = i + pad_needed;
                    emit_bitpacked_block(writer, &values[pending_start..bp_end], num_bits)?;
                    emit_rle_block(writer, values[i], rle_remaining, num_bits)?;
                    i += run_len;
                    pending_start = i;
                } else {
                    // After absorbing, not enough left for RLE — skip this run.
                    i += 1;
                }
            }
        } else {
            i += 1;
        }
    }

    // Final flush — last block can have non-multiple-of-8 length since the
    // decoder uses the outer remaining counter to stop reading.
    if pending_start < length {
        emit_bitpacked_block(writer, &values[pending_start..length], num_bits)?;
    }

    Ok(())
}

/// Counts consecutive identical values starting at `start`.
#[inline]
fn count_run(values: &[u32], start: usize) -> usize {
    let val = values[start];
    let mut count = 1;
    while start + count < values.len() && values[start + count] == val {
        count += 1;
    }
    count
}

/// Emits an RLE block: ULEB128 indicator `(count << 1) | 0`, then the value
/// in `ceil8(num_bits)` little-endian bytes.
fn emit_rle_block<W: Write>(
    writer: &mut W,
    value: u32,
    count: usize,
    num_bits: u8,
) -> std::io::Result<()> {
    // Header: (count << 1) | 0  (low bit 0 = RLE)
    let indicator = (count as u64) << 1;
    let mut container = [0u8; 10];
    let used = uleb128::encode(indicator, &mut container);
    writer.write_all(&container[..used])?;

    // Value in ceil8(num_bits) little-endian bytes
    let value_bytes = ceil8(num_bits as usize).max(1);
    let le = value.to_le_bytes();
    writer.write_all(&le[..value_bytes])?;
    Ok(())
}

/// Emits a bitpacked block: ULEB128 indicator `(num_groups << 1) | 1`, then
/// the bitpacked data. Pads the last group to 8 values with zeros.
fn emit_bitpacked_block<W: Write>(
    writer: &mut W,
    values: &[u32],
    num_bits: u8,
) -> std::io::Result<()> {
    let length = values.len();
    if length == 0 {
        return Ok(());
    }
    let num_bits = num_bits as usize;
    let num_groups = ceil8(length);

    // Header: (num_groups << 1) | 1  (low bit 1 = bitpacked)
    let indicator = ((num_groups as u64) << 1) | 1;
    let mut container = [0u8; 10];
    let used = uleb128::encode(indicator, &mut container);
    writer.write_all(&container[..used])?;

    // Bitpack the values in chunks of U32_BLOCK_LEN
    let total_values_declared = num_groups * 8;
    let total_bytes_needed = ceil8(total_values_declared * num_bits);

    let chunks = length / U32_BLOCK_LEN;
    let remainder = length - chunks * U32_BLOCK_LEN;
    let compressed_chunk_size = ceil8(U32_BLOCK_LEN * num_bits);
    let mut buffer = [0u32; U32_BLOCK_LEN];

    for c in 0..chunks {
        buffer.copy_from_slice(&values[c * U32_BLOCK_LEN..(c + 1) * U32_BLOCK_LEN]);
        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack::<u32>(&buffer, num_bits, packed.as_mut());
        writer.write_all(&packed[..compressed_chunk_size])?;
    }

    if remainder != 0 {
        let start = chunks * U32_BLOCK_LEN;
        buffer[..remainder].copy_from_slice(&values[start..start + remainder]);
        buffer[remainder..].fill(0);
        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack(&buffer, num_bits, packed.as_mut());
        let remaining_bytes = total_bytes_needed - chunks * compressed_chunk_size;
        writer.write_all(&packed[..remaining_bytes])?;
    }
    Ok(())
}

fn encode_bitpacked_header<W: Write>(writer: &mut W, length: usize) -> std::io::Result<()> {
    // write the length + indicator
    let mut header = ceil8(length) as u64;
    header <<= 1;
    header |= 1; // it is bitpacked => first bit is set
    let mut container = [0; 10];
    let used = uleb128::encode(header, &mut container);
    writer.write_all(&container[..used])?;
    Ok(())
}

const U32_BLOCK_LEN: usize = 32;

fn bitpacked_encode_u32<W: Write, I: Iterator<Item = u32>>(
    writer: &mut W,
    mut iterator: I,
    length: usize,
    num_bits: usize,
) -> std::io::Result<()> {
    // The RLE header declares ceil8(length) groups of 8 values.
    // We must write exactly ceil8(ceil8(length) * 8 * num_bits) bytes
    // to match what the header declares, per Parquet spec.
    let num_groups = ceil8(length);
    let total_values_declared = num_groups * 8;
    let total_bytes_needed = ceil8(total_values_declared * num_bits);

    let chunks = length / U32_BLOCK_LEN;
    let remainder = length - chunks * U32_BLOCK_LEN;
    let mut buffer = [0u32; U32_BLOCK_LEN];
    let compressed_chunk_size = ceil8(U32_BLOCK_LEN * num_bits);

    for _ in 0..chunks {
        iterator
            .by_ref()
            .take(U32_BLOCK_LEN)
            .zip(buffer.iter_mut())
            .for_each(|(item, buf)| *buf = item);

        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack::<u32>(&buffer, num_bits, packed.as_mut());
        writer.write_all(&packed[..compressed_chunk_size])?;
    }

    if remainder != 0 {
        iterator
            .by_ref()
            .take(remainder)
            .zip(buffer.iter_mut())
            .for_each(|(item, buf)| *buf = item);

        buffer[remainder..].fill(0);
        let mut packed = [0u8; 4 * U32_BLOCK_LEN];
        bitpacked::encode_pack(&buffer, num_bits, packed.as_mut());
        let remaining_bytes = total_bytes_needed - chunks * compressed_chunk_size;
        writer.write_all(&packed[..remaining_bytes])?;
    }
    Ok(())
}

/// the bitpacked part of the encoder.
pub fn encode_bool<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    iterator: I,
    length: usize,
) -> std::io::Result<()> {
    encode_bitpacked_header(writer, length)?;
    // encode the iterator
    bitpacked_encode(writer, iterator, length)
}

#[cfg(test)]
mod tests {
    use super::super::bitmap::BitmapIter;
    use super::super::HybridRleDecoder;
    use super::*;

    /// Helper: encode then decode, assert roundtrip matches.
    fn roundtrip_u32(values: &[u32], num_bits: u32) {
        let mut buf = vec![];
        encode_u32(&mut buf, values.iter().cloned(), values.len(), num_bits).unwrap();
        let decoder = HybridRleDecoder::try_new(&buf, num_bits, values.len()).unwrap();
        let result: Vec<u32> = decoder.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(result, values);
    }

    #[test]
    fn bool_basics_1() -> std::io::Result<()> {
        let iter = BitmapIter::new(&[0b10011101u8, 0b10011101], 0, 14);

        let mut vec = vec![];
        let len = iter.size_hint().1.unwrap();

        encode_bool(&mut vec, iter, len)?;

        assert_eq!(vec, vec![(2 << 1 | 1), 0b10011101u8, 0b00011101]);

        Ok(())
    }

    #[test]
    fn bool_from_iter() -> std::io::Result<()> {
        let mut vec = vec![];

        let values = vec![true, true, true, true, true, true, true, true];
        let len = values.len();
        let iter = values.into_iter();
        encode_bool(&mut vec, iter, len)?;

        assert_eq!(vec, vec![(1 << 1 | 1), 0b11111111]);
        Ok(())
    }

    // Roundtrip tests for data without long runs (should produce identical output
    // to old encoder since no RLE blocks are emitted).
    #[test]
    fn test_encode_u32_roundtrip() {
        let values = vec![0, 1, 2, 1, 2, 1, 1, 0, 3];
        roundtrip_u32(&values, 2);
    }

    #[test]
    fn test_encode_u32_large_roundtrip() {
        let values: Vec<_> = (0..128).map(|x| x % 4).collect();
        roundtrip_u32(&values, 2);
    }

    #[test]
    fn test_u32_other_roundtrip() {
        let values = vec![3, 3, 0, 3, 2, 3, 3, 3, 3, 1, 3, 3, 3, 0, 3];
        roundtrip_u32(&values, 2);
    }

    // New tests for hybrid RLE/bitpacking behavior.

    #[test]
    fn test_all_same_values() {
        // 1000 identical values — should RLE-encode very compactly.
        let values = vec![5u32; 1000];
        let mut buf = vec![];
        encode_u32(&mut buf, values.iter().cloned(), values.len(), 8).unwrap();

        // Verify roundtrip.
        let decoder = HybridRleDecoder::try_new(&buf, 8, values.len()).unwrap();
        let result: Vec<u32> = decoder.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(result, values);

        // RLE should be much smaller than pure bitpacking (1000 bytes).
        // A single RLE block costs ~4 bytes.
        assert!(
            buf.len() < 10,
            "encoded size {} should be tiny for all-same data",
            buf.len()
        );
    }

    #[test]
    fn test_exactly_8_identical() {
        // Exactly MIN_RLE_RUN identical values — should trigger RLE.
        let values = vec![7u32; 8];
        let mut buf = vec![];
        encode_u32(&mut buf, values.iter().cloned(), values.len(), 4).unwrap();

        let decoder = HybridRleDecoder::try_new(&buf, 4, values.len()).unwrap();
        let result: Vec<u32> = decoder.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(result, values);
    }

    #[test]
    fn test_7_identical_no_rle() {
        // 7 identical values — below MIN_RLE_RUN, should bitpack.
        let values = vec![7u32; 7];
        roundtrip_u32(&values, 4);
    }

    #[test]
    fn test_run_at_start() {
        // Run at start, then random data.
        let mut values = vec![3u32; 20];
        values.extend_from_slice(&[0, 1, 2, 3, 0, 1, 2]);
        roundtrip_u32(&values, 2);
    }

    #[test]
    fn test_run_at_end() {
        // Random data, then run at end.
        let mut values = vec![0, 1, 2, 3, 0, 1, 2];
        values.extend(vec![3u32; 20]);
        roundtrip_u32(&values, 2);
    }

    #[test]
    fn test_mixed_runs_and_random() {
        // Interleaved runs and random data (all values fit in 2 bits: 0-3).
        let mut values = Vec::new();
        values.extend(vec![0u32; 50]); // run
        values.extend((0..10).map(|x| x % 4)); // random (0-3)
        values.extend(vec![3u32; 100]); // run
        values.extend((0..20).map(|x| x % 4)); // random
        values.extend(vec![1u32; 30]); // run
        roundtrip_u32(&values, 2);
    }

    #[test]
    fn test_single_value() {
        roundtrip_u32(&[42], 8);
    }

    #[test]
    fn test_empty() {
        roundtrip_u32(&[], 8);
    }

    #[test]
    fn test_1bit_fast_path() {
        // 1-bit should use the fast path (pure bitpacking).
        let values: Vec<u32> = vec![0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        roundtrip_u32(&values, 1);
    }

    #[test]
    fn test_0bit_fast_path() {
        // 0-bit: all values must be 0.
        let values = vec![0u32; 100];
        roundtrip_u32(&values, 0);
    }

    #[test]
    fn test_large_num_bits() {
        // Higher bit widths with runs.
        let mut values = vec![1023u32; 50];
        values.extend(0..50);
        values.extend(vec![500u32; 100]);
        roundtrip_u32(&values, 10);
    }

    #[test]
    fn test_size_savings_for_runs() {
        // Verify that encoding data with long runs produces smaller output
        // than the pure bitpacking size.
        let values = vec![42u32; 500];
        let mut buf = vec![];
        encode_u32(&mut buf, values.iter().cloned(), values.len(), 8).unwrap();

        // Pure bitpacking would need ~500 bytes + header. RLE should be << that.
        let bitpacked_size = values.len(); // 500 * 8 bits / 8 = 500 bytes
        assert!(
            buf.len() < bitpacked_size / 10,
            "encoded size {} should be much smaller than bitpacked size {}",
            buf.len(),
            bitpacked_size
        );
    }
}
