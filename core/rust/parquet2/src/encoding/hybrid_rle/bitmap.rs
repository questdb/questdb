use std::io::Write;

const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

/// Sets bit at position `i` in `byte`
#[inline]
pub fn set(byte: u8, i: usize) -> u8 {
    byte | BIT_MASK[i]
}

/// An [`Iterator`] of bool that decodes a bitmap.
/// This is a specialization of [`super::super::bitpacked::Decoder`] for `num_bits == 1`.
#[derive(Debug)]
pub struct BitmapIter<'a> {
    iter: std::slice::Iter<'a, u8>,
    current_byte: &'a u8,
    remaining: usize,
    mask: u8,
}

impl<'a> BitmapIter<'a> {
    /// Returns a new [`BitmapIter`].
    /// # Panics
    /// This function panics iff `offset / 8 > slice.len()`
    #[inline]
    pub fn new(slice: &'a [u8], offset: usize, len: usize) -> Self {
        let bytes = &slice[offset / 8..];

        let mut iter = bytes.iter();

        let current_byte = iter.next().unwrap_or(&0);

        Self {
            iter,
            mask: 1u8.rotate_left(offset as u32),
            remaining: len,
            current_byte,
        }
    }

    #[inline]
    pub fn advance(&mut self, count: usize) {
        if count == 0 || self.remaining == 0 {
            return;
        }

        let count = count.min(self.remaining);
        self.remaining -= count;

        // Fast path for small values
        macro_rules! advance_one {
            ($self:ident) => {
                $self.mask = $self.mask.rotate_left(1);
                if $self.mask == 1 {
                    $self.current_byte = $self.iter.next().unwrap_or(&0);
                }
            };
        }

        match count {
            1 => {
                advance_one!(self);
                return;
            }
            2 => {
                advance_one!(self);
                advance_one!(self);
                return;
            }
            3 => {
                advance_one!(self);
                advance_one!(self);
                advance_one!(self);
                return;
            }
            4 => {
                advance_one!(self);
                advance_one!(self);
                advance_one!(self);
                advance_one!(self);
                return;
            }
            _ => {}
        }

        let current_bit = self.mask.trailing_zeros() as usize;
        let bits_left_in_byte = 8 - current_bit;

        if count < bits_left_in_byte {
            self.mask = self.mask.rotate_left(count as u32);
        } else {
            let remaining_to_skip = count - bits_left_in_byte;
            let bytes_to_skip = remaining_to_skip / 8;
            let final_bits = remaining_to_skip % 8;

            let slice = self.iter.as_slice();
            let skip = bytes_to_skip.min(slice.len());
            self.iter = slice[skip..].iter();

            self.current_byte = self.iter.next().unwrap_or(&0);
            self.mask = 1u8.rotate_left(final_bits as u32);
        }
    }
}

impl<'a> Iterator for BitmapIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        // easily predictable in branching
        if self.remaining == 0 {
            return None;
        } else {
            self.remaining -= 1;
        }
        let value = self.current_byte & self.mask != 0;
        self.mask = self.mask.rotate_left(1);
        if self.mask == 1 {
            // reached a new byte => try to fetch it from the iterator
            if let Some(v) = self.iter.next() {
                self.current_byte = v
            }
        }
        Some(value)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Writes an iterator of bools into writer, with LSB first.
pub fn encode_bool<W: Write, I: Iterator<Item = bool>>(
    writer: &mut W,
    mut iterator: I,
    length: usize,
) -> std::io::Result<()> {
    let chunks = length / 8;
    let reminder = length % 8;

    (0..chunks).try_for_each(|_| {
        let mut byte = 0u8;
        (0..8).for_each(|i| {
            if iterator.next().unwrap() {
                byte = set(byte, i)
            }
        });
        writer.write_all(&[byte])
    })?;

    if reminder != 0 {
        let mut last = 0u8;
        iterator.enumerate().for_each(|(i, value)| {
            if value {
                last = set(last, i)
            }
        });
        writer.write_all(&[last])
    } else {
        Ok(())
    }
}
