use crate::encoding::ceil8;

use super::super::bitpacked;
use super::super::uleb128;
use super::super::zigzag_leb128;

/// Encodes an iterator of `i64` according to parquet's `DELTA_BINARY_PACKED`,
/// computing deltas in 64-bit wrapping arithmetic.
///
/// # Implementation
/// * This function does not allocate on the heap.
/// * The number of mini-blocks is always 1. This may change in the future.
pub fn encode<I: Iterator<Item = i64>>(iterator: I, buffer: &mut Vec<u8>) {
    encode_impl::<_, false>(iterator, buffer)
}

/// Like [`encode`], but computes deltas in 32-bit wrapping arithmetic.
///
/// Use this when encoding values that originate from INT32 columns. Without
/// 32-bit wrapping, the delta range of i32 values cast to i64 can exceed 32
/// bits, producing bit widths that Parquet readers reject for INT32 pages.
pub fn encode_i32<I: Iterator<Item = i64>>(iterator: I, buffer: &mut Vec<u8>) {
    encode_impl::<_, true>(iterator, buffer)
}

fn encode_impl<I: Iterator<Item = i64>, const WRAP32: bool>(
    mut iterator: I,
    buffer: &mut Vec<u8>,
) {
    let block_size = 128;
    let mini_blocks = 1;

    let mut container = [0u8; 10];
    let encoded_len = uleb128::encode(block_size, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let encoded_len = uleb128::encode(mini_blocks, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let length = iterator.size_hint().1.unwrap();
    let encoded_len = uleb128::encode(length as u64, &mut container);
    buffer.extend_from_slice(&container[..encoded_len]);

    let mut values = [0i64; 128];
    let mut deltas = [0u64; 128];

    let first_value = iterator.next().unwrap_or_default();
    let (container, encoded_len) = zigzag_leb128::encode(first_value);
    buffer.extend_from_slice(&container[..encoded_len]);

    let mut prev = first_value;
    let mut length = iterator.size_hint().1.unwrap();
    while length != 0 {
        let mut min_delta = i64::MAX;
        let mut max_delta = i64::MIN;
        let mut num_bits = 0;
        for (i, integer) in (0..128).zip(&mut iterator) {
            let delta = if WRAP32 {
                ((integer as i32).wrapping_sub(prev as i32)) as i64
            } else {
                integer.wrapping_sub(prev)
            };
            min_delta = min_delta.min(delta);
            max_delta = max_delta.max(delta);

            num_bits = 64 - (max_delta.wrapping_sub(min_delta)).leading_zeros();
            values[i] = delta;
            prev = integer;
        }
        let consumed = std::cmp::min(length - iterator.size_hint().1.unwrap(), 128);
        length = iterator.size_hint().1.unwrap();
        let values = &values[..consumed];

        values.iter().zip(deltas.iter_mut()).for_each(|(v, delta)| {
            *delta = (v.wrapping_sub(min_delta)) as u64;
        });

        // <min delta> <list of bitwidths of miniblocks> <miniblocks>
        let (container, encoded_len) = zigzag_leb128::encode(min_delta);
        buffer.extend_from_slice(&container[..encoded_len]);

        // one miniblock => 1 byte
        buffer.push(num_bits as u8);
        write_miniblock(buffer, num_bits as usize, deltas);
    }
}

fn write_miniblock(buffer: &mut Vec<u8>, num_bits: usize, deltas: [u64; 128]) {
    if num_bits > 0 {
        let start = buffer.len();

        // bitpack encode all (deltas.len = 128 which is a multiple of 32)
        let bytes_needed = start + ceil8(deltas.len() * num_bits);
        buffer.resize(bytes_needed, 0);
        bitpacked::encode(deltas.as_ref(), num_bits, &mut buffer[start..]);

        let bytes_needed = start + ceil8(deltas.len() * num_bits);
        buffer.truncate(bytes_needed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_delta() {
        // header: [128, 1, 1, 5, 2]:
        //  block size: 128    <=u> 128, 1
        //  mini-blocks: 1     <=u> 1
        //  elements: 5        <=u> 5
        //  first_value: 2     <=z> 1
        // block1: [2, 0, 0, 0, 0]
        //  min_delta: 1        <=z> 2
        //  bitwidth: 0
        let data = 1..=5;
        let expected = vec![128u8, 1, 1, 5, 2, 2, 0];

        let mut buffer = vec![];
        encode(data, &mut buffer);
        assert_eq!(expected, buffer);
    }

    #[test]
    fn negative_min_delta() {
        // max - min = 1 - -4 = 5
        let data = vec![1, 2, 3, 4, 5, 1];
        // header: [128, 1, 4, 6, 2]
        //  block size: 128    <=u> 128, 1
        //  mini-blocks: 1     <=u> 1
        //  elements: 6        <=u> 5
        //  first_value: 2     <=z> 1
        // block1: [7, 3, 253, 255]
        //  min_delta: -4        <=z> 7
        //  bitwidth: 3
        //  values: [5, 5, 5, 5, 0] <=b> [
        //      0b01101101
        //      0b00001011
        // ]
        let mut expected = vec![128u8, 1, 1, 6, 2, 7, 3, 0b01101101, 0b00001011];
        expected.extend(std::iter::repeat(0).take(128 * 3 / 8 - 2)); // 128 values, 3 bits, 2 already used

        let mut buffer = vec![];
        encode(data.into_iter(), &mut buffer);
        assert_eq!(expected, buffer);
    }

    #[test]
    fn i32_extreme_deltas_wrap_to_32_bits() {
        // Consecutive i32::MIN → i32::MAX → i32::MIN cast to i64.
        // In i64 space, delta range would need 33 bits.
        // With encode_i32, deltas wrap at 32 bits and stay within 32-bit width.
        let data = vec![i32::MIN as i64, i32::MAX as i64, i32::MIN as i64];

        let mut buffer = vec![];
        encode_i32(data.into_iter(), &mut buffer);

        // Verify the bit width byte in the first block is <= 32.
        // Header: block_size(varint) + mini_blocks(varint) + count(varint) + first_value(zigzag)
        // For 3 values: block_size=128(2 bytes), mini_blocks=1(1), count=3(1),
        // first_value=i32::MIN as i64=-2147483648 (zigzag: 4294967295, varint: 5 bytes)
        // Then: min_delta(zigzag varint), then bit_width(1 byte)
        // We just need to verify it doesn't exceed 32.
        let header_end = 2 + 1 + 1 + 5; // block_size + mini_blocks + count + first_value
        // Skip min_delta (variable length zigzag), find the bit_width byte after it
        let mut offset = header_end;
        // Decode zigzag varint for min_delta to skip it
        let (_min_delta, delta_len) =
            zigzag_leb128::decode(&buffer[offset..]).expect("valid zigzag");
        offset += delta_len;
        let bit_width = buffer[offset];
        assert!(
            bit_width <= 32,
            "bit width {bit_width} exceeds 32 for i32 values"
        );
    }
}
