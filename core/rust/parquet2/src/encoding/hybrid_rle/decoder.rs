use crate::error::Error;

use super::super::uleb128;
use super::{super::ceil8, HybridEncoded};

/// An [`Iterator`] of [`HybridEncoded`].
#[derive(Debug, Clone)]
pub struct Decoder<'a> {
    values: &'a [u8],
    num_bits: usize,
}

impl<'a> Decoder<'a> {
    /// Returns a new [`Decoder`]
    pub fn new(values: &'a [u8], num_bits: usize) -> Self {
        Self { values, num_bits }
    }

    /// Returns the number of bits being used by this decoder.
    #[inline]
    pub fn num_bits(&self) -> usize {
        self.num_bits
    }
}

impl<'a> Iterator for Decoder<'a> {
    type Item = Result<HybridEncoded<'a>, Error>;

    #[inline] // -18% improvement in bench
    fn next(&mut self) -> Option<Self::Item> {
        if self.num_bits == 0 {
            return None;
        }

        if self.values.is_empty() {
            return None;
        }

        let (indicator, consumed) = match uleb128::decode(self.values) {
            Ok((indicator, consumed)) => (indicator, consumed),
            Err(e) => return Some(Err(e)),
        };
        self.values = &self.values[consumed..];
        if self.values.is_empty() {
            return None;
        };

        if indicator & 1 == 1 {
            // is bitpacking
            let bytes = (indicator as usize >> 1) * self.num_bits;
            let bytes = std::cmp::min(bytes, self.values.len());
            let (result, remaining) = self.values.split_at(bytes);
            self.values = remaining;
            Some(Ok(HybridEncoded::Bitpacked(result)))
        } else {
            // is rle
            let run_length = indicator as usize >> 1;
            // repeated-value := value that is repeated, using a fixed-width of round-up-to-next-byte(bit-width)
            let rle_bytes = ceil8(self.num_bits);
            // A foreign or corrupt page can declare an RLE run whose fixed-width
            // value spills past the remaining buffer. split_at would panic and
            // abort the JVM across the JNI boundary; surface a clean error
            // instead (the bitpacked branch above guards the same overflow with
            // a min()).
            if rle_bytes > self.values.len() {
                return Some(Err(Error::oos(
                    "RLE run value width exceeds the available buffer",
                )));
            }
            let (result, remaining) = self.values.split_at(rle_bytes);
            self.values = remaining;
            Some(Ok(HybridEncoded::Rle(result, run_length)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::super::bitpacked;

    #[test]
    fn basics_1() {
        let bit_width = 1usize;
        let length = 5;
        let values = vec![
            2, 0, 0, 0, // length
            0b00000011, 0b00001011, // data
        ];

        let mut decoder = Decoder::new(&values[4..6], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            assert_eq!(values, &[0b00001011]);
            let result = bitpacked::Decoder::<u32>::try_new(values, bit_width, length)
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, &[1, 1, 0, 1, 0]);
        } else {
            panic!()
        };
    }

    #[test]
    fn basics_2() {
        // This test was validated by the result of what pyarrow3 outputs when
        // the bitmap is used.
        let bit_width = 1;
        let values = vec![
            3, 0, 0, 0, // length
            0b00000101, 0b11101011, 0b00000010, // data
        ];
        let expected = &[1, 1, 0, 1, 0, 1, 1, 1, 0, 1];

        let mut decoder = Decoder::new(&values[4..4 + 3], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Bitpacked(values) = run.unwrap() {
            assert_eq!(values, &[0b11101011, 0b00000010]);
            let result = bitpacked::Decoder::<u32>::try_new(values, bit_width, 10)
                .unwrap()
                .collect::<Vec<_>>();
            assert_eq!(result, expected);
        } else {
            panic!()
        };
    }

    #[test]
    fn basics_3() {
        let bit_width = 1;
        let length = 8;
        let values = vec![
            2, 0, 0, 0,          // length
            0b00010000, // data
            0b00000001,
        ];

        let mut decoder = Decoder::new(&values[4..4 + 2], bit_width);

        let run = decoder.next().unwrap();

        if let HybridEncoded::Rle(values, items) = run.unwrap() {
            assert_eq!(values, &[0b00000001]);
            assert_eq!(items, length);
        } else {
            panic!()
        };
    }

    #[test]
    fn rle_run_value_exceeds_buffer_errors() {
        // A foreign/corrupt page declares an RLE run (indicator LSB 0) whose
        // fixed-width repeated value is ceil8(num_bits) bytes wide, but fewer
        // bytes follow. split_at would panic and abort the JVM across the JNI
        // boundary; the decoder must surface a clean error instead. Here
        // num_bits = 9 -> rle_bytes = 2, but only 1 value byte follows the
        // indicator (0x02 => run_length 1, LSB 0 => RLE).
        let bit_width = 9usize;
        let values = [0x02u8, 0x00];
        let mut decoder = Decoder::new(&values, bit_width);
        let run = decoder.next().expect("a run is produced");
        assert!(
            run.is_err(),
            "RLE run past the buffer must error, not panic"
        );
    }
}
