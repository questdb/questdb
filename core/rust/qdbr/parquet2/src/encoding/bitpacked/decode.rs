use crate::error::Error;

use super::{Packed, Unpackable, Unpacked};

/// An [`Iterator`] of [`Unpackable`] unpacked from a bitpacked slice of bytes.
/// # Implementation
/// This iterator unpacks bytes in chunks and does not allocate.
#[derive(Debug, Clone)]
pub struct Decoder<'a, T: Unpackable> {
    packed: std::slice::Chunks<'a, u8>,
    num_bits: usize,
    pub remaining: usize,          // in number of items
    pub current_pack_index: usize, // invariant: < T::PACK_LENGTH
    pub unpacked: T::Unpacked,     // has the current unpacked values.
}

#[inline]
fn decode_pack<T: Unpackable>(packed: &[u8], num_bits: usize, unpacked: &mut T::Unpacked) {
    if packed.len() < T::Unpacked::LENGTH * num_bits / 8 {
        let mut buf = T::Packed::zero();
        buf.as_mut()[..packed.len()].copy_from_slice(packed);
        T::unpack(buf.as_ref(), num_bits, unpacked)
    } else {
        T::unpack(packed, num_bits, unpacked)
    }
}

impl<'a, T: Unpackable> Decoder<'a, T> {
    /// Returns a [`Decoder`] with `T` encoded in `packed` with `num_bits`.
    pub fn try_new(packed: &'a [u8], num_bits: usize, mut length: usize) -> Result<Self, Error> {
        let block_size = std::mem::size_of::<T>() * num_bits;

        if num_bits == 0 {
            return Err(Error::oos("Bitpacking requires num_bits > 0"));
        }

        if packed.len() * 8 < length * num_bits {
            return Err(Error::oos(format!(
                "Unpacking {length} items with a number of bits {num_bits} requires at least {} bytes.",
                length * num_bits / 8
            )));
        }

        let mut packed = packed.chunks(block_size);
        let mut unpacked = T::Unpacked::zero();
        if let Some(chunk) = packed.next() {
            decode_pack::<T>(chunk, num_bits, &mut unpacked);
        } else {
            length = 0
        };

        Ok(Self {
            remaining: length,
            packed,
            num_bits,
            unpacked,
            current_pack_index: 0,
        })
    }
}

impl<'a, T: Unpackable> Decoder<'a, T> {
    /// Decode the next pack of values. Call this after consuming all values
    /// from the current pack (i.e., when `current_pack_index == T::Unpacked::LENGTH`).
    #[inline]
    pub fn decode_next_pack(&mut self) {
        if let Some(packed) = self.packed.next() {
            decode_pack::<T>(packed, self.num_bits, &mut self.unpacked);
            self.current_pack_index = 0;
        }
    }

    #[inline]
    pub fn advance(&mut self, n: usize) -> usize {
        let n = n.min(self.remaining);
        if n == 0 {
            return 0;
        }

        self.remaining -= n;

        let left_in_pack = T::Unpacked::LENGTH - self.current_pack_index;

        if n < left_in_pack {
            self.current_pack_index += n;
        } else {
            let mut to_skip = n - left_in_pack;

            let packs_to_skip = to_skip / T::Unpacked::LENGTH;
            if packs_to_skip > 0 {
                self.packed.nth(packs_to_skip - 1);
            }
            to_skip %= T::Unpacked::LENGTH;

            if let Some(packed) = self.packed.next() {
                decode_pack::<T>(packed, self.num_bits, &mut self.unpacked);
                self.current_pack_index = to_skip;
            } else {
                self.current_pack_index = 0;
            }
        }

        n
    }
}

impl<'a, T: Unpackable> Iterator for Decoder<'a, T> {
    type Item = T;

    #[inline] // -71% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let result = self.unpacked[self.current_pack_index];
        self.current_pack_index += 1;
        self.remaining -= 1;
        if self.current_pack_index == T::Unpacked::LENGTH {
            if let Some(packed) = self.packed.next() {
                decode_pack::<T>(packed, self.num_bits, &mut self.unpacked);
                self.current_pack_index = 0;
            }
        }
        Some(result)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::case1;
    use super::*;

    #[test]
    fn test_decode_rle() {
        // Test data: 0-7 with bit width 3
        // 0: 000
        // 1: 001
        // 2: 010
        // 3: 011
        // 4: 100
        // 5: 101
        // 6: 110
        // 7: 111
        let num_bits = 3;
        let length = 8;
        // encoded: 0b10001000u8, 0b11000110, 0b11111010
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let decoded = Decoder::<u32>::try_new(&data, num_bits, length)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![0, 1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn decode_large() {
        let (num_bits, expected, data) = case1();

        let decoded = Decoder::<u32>::try_new(&data, num_bits, expected.len())
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_decode_bool() {
        let num_bits = 1;
        let length = 8;
        let data = vec![0b10101010];

        let decoded = Decoder::<u32>::try_new(&data, num_bits, length)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn test_decode_u64() {
        let num_bits = 1;
        let length = 8;
        let data = vec![0b10101010];

        let decoded = Decoder::<u64>::try_new(&data, num_bits, length)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![0, 1, 0, 1, 0, 1, 0, 1]);
    }

    #[test]
    fn even_case() {
        // [0, 1, 2, 3, 4, 5, 6, 0]x99
        let data = &[0b10001000u8, 0b11000110, 0b00011010];
        let num_bits = 3;
        let copies = 99; // 8 * 99 % 32 != 0
        let expected = std::iter::repeat(&[0u32, 1, 2, 3, 4, 5, 6, 0])
            .take(copies)
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let data = std::iter::repeat(data)
            .take(copies)
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let length = expected.len();

        let decoded = Decoder::<u32>::try_new(&data, num_bits, length)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn odd_case() {
        // [0, 1, 2, 3, 4, 5, 6, 0]x4 + [2]
        let data = &[0b10001000u8, 0b11000110, 0b00011010];
        let num_bits = 3;
        let copies = 4;
        let expected = std::iter::repeat(&[0u32, 1, 2, 3, 4, 5, 6, 0])
            .take(copies)
            .flatten()
            .copied()
            .chain(std::iter::once(2))
            .collect::<Vec<_>>();
        let data = std::iter::repeat(data)
            .take(copies)
            .flatten()
            .copied()
            .chain(std::iter::once(0b00000010u8))
            .collect::<Vec<_>>();
        let length = expected.len();

        let decoded = Decoder::<u32>::try_new(&data, num_bits, length)
            .unwrap()
            .collect::<Vec<_>>();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_errors() {
        // zero length
        assert!(Decoder::<u64>::try_new(&[], 1, 0).is_ok());
        // no bytes
        assert!(Decoder::<u64>::try_new(&[], 1, 1).is_err());
        // too few bytes
        assert!(Decoder::<u64>::try_new(&[1], 1, 8).is_ok());
        assert!(Decoder::<u64>::try_new(&[1, 1], 2, 8).is_ok());
        assert!(Decoder::<u64>::try_new(&[1], 1, 9).is_err());
        // zero num_bits
        assert!(Decoder::<u64>::try_new(&[1], 0, 1).is_err());
    }

    #[test]
    fn test_advance_within_pack() {
        let num_bits = 3;
        let length = 8;
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        // Advance by 2, should skip 0 and 1
        let advanced = decoder.advance(2);
        assert_eq!(advanced, 2);
        assert_eq!(decoder.size_hint(), (6, Some(6)));

        // Next item should be 2
        assert_eq!(decoder.next(), Some(2));
        assert_eq!(decoder.next(), Some(3));
    }

    #[test]
    fn test_advance_cross_pack() {
        // For u32, pack size is 32. Create data with 64 items (2 packs).
        let num_bits = 3;
        let length = 64;
        // Each pack of 32 items at 3 bits = 96 bits = 12 bytes
        let data: Vec<u8> = (0..24).collect();

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        let advanced = decoder.advance(35);
        assert_eq!(advanced, 35);
        assert_eq!(decoder.size_hint(), (29, Some(29)));
        let remaining: Vec<_> = decoder.collect();
        assert_eq!(remaining.len(), 29);
    }

    #[test]
    fn test_advance_end_of_stream() {
        let num_bits = 3;
        let length = 8;
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();
        let advanced = decoder.advance(100);
        assert_eq!(advanced, 8);
        assert_eq!(decoder.size_hint(), (0, Some(0)));
        assert_eq!(decoder.next(), None);
    }

    #[test]
    fn test_advance_zero() {
        let num_bits = 3;
        let length = 8;
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        let advanced = decoder.advance(0);
        assert_eq!(advanced, 0);
        assert_eq!(decoder.size_hint(), (8, Some(8)));
        assert_eq!(decoder.next(), Some(0));
    }

    #[test]
    fn test_advance_multiple_packs() {
        let num_bits = 3;
        let length = 128;
        // 128 items at 3 bits = 384 bits = 48 bytes
        let data: Vec<u8> = (0..48).collect();

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        // Advance 100 items, skipping ~3 packs
        let advanced = decoder.advance(100);
        assert_eq!(advanced, 100);
        assert_eq!(decoder.size_hint(), (28, Some(28)));

        // Should be able to continue iterating
        let remaining: Vec<_> = decoder.collect();
        assert_eq!(remaining.len(), 28);
    }

    #[test]
    fn test_advance_exact_pack_boundary() {
        // For u32, pack size is 32
        let num_bits = 3;
        let length = 64;
        let data: Vec<u8> = (0..24).collect();

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        // Advance exactly 32 items (one full pack)
        let advanced = decoder.advance(32);
        assert_eq!(advanced, 32);
        assert_eq!(decoder.size_hint(), (32, Some(32)));

        // Should be at start of second pack, iteration should work
        let remaining: Vec<_> = decoder.collect();
        assert_eq!(remaining.len(), 32);
    }

    #[test]
    fn test_advance_chained() {
        let num_bits = 3;
        let length = 8;
        let data = vec![0b10001000u8, 0b11000110, 0b11111010];

        let mut decoder = Decoder::<u32>::try_new(&data, num_bits, length).unwrap();

        assert_eq!(decoder.advance(2), 2); // skip 0, 1
        assert_eq!(decoder.next(), Some(2));
        assert_eq!(decoder.advance(2), 2); // skip 3, 4
        assert_eq!(decoder.next(), Some(5));
        assert_eq!(decoder.advance(10), 2); // only 2 left (6, 7)
        assert_eq!(decoder.next(), None);
    }
}
