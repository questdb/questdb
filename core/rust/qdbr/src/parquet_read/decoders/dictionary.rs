//! Dictionary page decoders used by dictionary-encoded value paths.
//!
//! The types in this module parse dictionary pages and provide typed dictionary
//! lookup traits consumed by `RleDictionaryDecoder`.

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::decoders::Converter;
use crate::parquet_read::page::DictPage;
use std::mem::size_of;

use std::ptr;

/// Dictionary lookup abstraction for variable-width values (string/binary).
pub trait VarDictDecoder {
    fn get_dict_value(&self, index: u32) -> &[u8];
    fn avg_key_len(&self) -> f32;
    fn len(&self) -> u32;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Variable-width dictionary represented as slices into the dictionary page.
pub struct BaseVarDictDecoder<'a> {
    pub dict_values: Vec<&'a [u8]>,
    pub avg_key_len: f32,
}

impl VarDictDecoder for BaseVarDictDecoder<'_> {
    #[inline]
    fn get_dict_value(&self, index: u32) -> &[u8] {
        self.dict_values[index as usize]
    }

    #[inline]
    fn avg_key_len(&self) -> f32 {
        self.avg_key_len
    }

    #[inline]
    fn len(&self) -> u32 {
        self.dict_values.len() as u32
    }
}

impl<'a> BaseVarDictDecoder<'a> {
    pub fn try_new(dict_page: &'a DictPage<'a>) -> ParquetResult<Self> {
        let dict_data = &dict_page.buffer;

        // num_values comes from the dictionary page's Thrift header and is
        // attacker-controlled: slice_reader validates the page *buffer* against
        // max_page_size and rejects a negative count, but never bounds
        // num_values against the buffer (so it can be up to i32::MAX). Each
        // value is a 4-byte length prefix plus its bytes, so a page holding N
        // values needs at least N * 4 bytes. Reject a header that claims more
        // values than the buffer can possibly hold, before
        // Vec::with_capacity(num_values) reserves tens of gigabytes (16 bytes
        // per &[u8]) and aborts the process on allocation failure.
        if dict_page.num_values > dict_data.len() / size_of::<u32>() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page is too short to hold {} values",
                dict_page.num_values
            ));
        }

        let mut dict_values: Vec<&[u8]> = Vec::with_capacity(dict_page.num_values);
        let mut offset = 0usize;

        let mut total_key_len = 0;
        for i in 0..dict_page.num_values {
            if offset + size_of::<u32>() > dict_data.len() {
                return Err(fmt_err!(
                    Layout,
                    "dictionary data page is too short to read value length {i}"
                ));
            }

            let str_len =
                unsafe { ptr::read_unaligned(dict_data.as_ptr().add(offset) as *const u32) }
                    as usize;
            offset += size_of::<u32>();

            if offset + str_len > dict_data.len() {
                return Err(fmt_err!(
                    Layout,
                    "dictionary data page is too short to read value {i}"
                ));
            }

            let str_slice = &dict_data[offset..offset + str_len];
            dict_values.push(str_slice);
            offset += str_len;
            total_key_len += str_len;
        }

        let avg_key_len = if dict_page.num_values == 0 {
            0.0
        } else {
            total_key_len as f32 / dict_page.num_values as f32
        };

        Ok(Self { dict_values, avg_key_len })
    }
}

/// Fixed-width dictionary view for values with constant byte size.
pub struct FixedDictDecoder<'a, const N: usize> {
    dict_page: &'a [u8],
}

impl<const N: usize> VarDictDecoder for FixedDictDecoder<'_, N> {
    #[inline]
    fn get_dict_value(&self, index: u32) -> &[u8] {
        self.dict_page[index as usize * N..(index as usize + 1) * N].as_ref()
    }

    #[inline]
    fn avg_key_len(&self) -> f32 {
        N as f32
    }

    #[inline]
    fn len(&self) -> u32 {
        (self.dict_page.len() / N) as u32
    }
}

impl<'a, const N: usize> FixedDictDecoder<'a, N> {
    pub fn try_new(dict_page: &'a DictPage<'a>) -> ParquetResult<Self> {
        if N * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not multiple of {N}"
            ));
        }

        Ok(Self { dict_page: dict_page.buffer })
    }
}

pub trait PrimitiveDictDecoder<T> {
    /// Decodes value at `index`. Caller guarantees index is in bounds.
    fn get_dict_value(&self, index: u32) -> T;

    /// Number of values in this dictionary.
    fn len(&self) -> u32;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A dictionary decoder for primitive types.
/// U is the physical type of the dictionary value.
/// T is the destination type of the dictionary value (e.g. short, etc.).
pub struct BasePrimitiveDictDecoder<'a, U, T> {
    dict_page: &'a [u8],
    _u: std::marker::PhantomData<U>,
    _t: std::marker::PhantomData<T>,
}

impl<U, T> PrimitiveDictDecoder<T> for BasePrimitiveDictDecoder<'_, U, T> {
    #[inline]
    #[cfg(target_endian = "little")]
    fn get_dict_value(&self, index: u32) -> T {
        const {
            assert!(
                size_of::<T>() <= size_of::<U>(),
                "destination type T must not be larger than physical type U"
            )
        };
        // SAFETY: Caller guarantees index is in bounds and dict_page is properly sized.
        // We also require little-endian platforms, so no byte swapping is necessary.
        unsafe {
            ptr::read_unaligned(
                self.dict_page.as_ptr().add(index as usize * size_of::<U>()) as *const T
            )
        }
    }

    #[cfg(not(target_endian = "little"))]
    fn get_dict_value(&self, index: u32) -> T {
        unimplemented!("Big-endian platforms are not supported");
    }

    #[inline]
    fn len(&self) -> u32 {
        (self.dict_page.len() / size_of::<U>()) as u32
    }
}

impl<'a, U, T> BasePrimitiveDictDecoder<'a, U, T> {
    pub fn try_new(dict_page: &'a DictPage<'a>) -> ParquetResult<Self> {
        assert!(size_of::<U>() >= size_of::<T>());

        if size_of::<U>() * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not a multiple of {}",
                size_of::<U>()
            ));
        }

        Ok(Self {
            dict_page: dict_page.buffer,
            _u: std::marker::PhantomData,
            _t: std::marker::PhantomData,
        })
    }
}

pub struct RleLocalIsGlobalSymbolDictDecoder {
    len: u32,
}

impl RleLocalIsGlobalSymbolDictDecoder {
    pub fn new(dict: &DictPage) -> Self {
        let len = dict.num_values as u32;
        Self { len }
    }
}

impl PrimitiveDictDecoder<i32> for RleLocalIsGlobalSymbolDictDecoder {
    #[inline]
    fn len(&self) -> u32 {
        self.len
    }

    #[inline]
    fn get_dict_value(&self, idx: u32) -> i32 {
        idx as i32
    }
}

/// Primitive dictionary with value conversion on every lookup.
pub struct ConvertablePrimitiveDictDecoder<'a, U, T, V> {
    dict_page: &'a [u8],
    converter: V,
    _phantom: std::marker::PhantomData<(U, T)>,
}

impl<U, T, V> PrimitiveDictDecoder<T> for ConvertablePrimitiveDictDecoder<'_, U, T, V>
where
    V: Converter<U, T>,
{
    #[inline]
    fn get_dict_value(&self, index: u32) -> T {
        let raw = unsafe {
            ptr::read_unaligned(
                self.dict_page.as_ptr().add(index as usize * size_of::<U>()) as *const U
            )
        };
        self.converter.convert(raw)
    }

    #[inline]
    fn len(&self) -> u32 {
        (self.dict_page.len() / size_of::<U>()) as u32
    }
}

impl<'a, U, T, V> ConvertablePrimitiveDictDecoder<'a, U, T, V> {
    pub fn try_new(dict_page: &'a DictPage<'a>, converter: V) -> ParquetResult<Self> {
        if size_of::<U>() * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not a multiple of {}",
                size_of::<U>()
            ));
        }

        Ok(Self {
            dict_page: dict_page.buffer,
            converter,
            _phantom: std::marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::BaseVarDictDecoder;
    use crate::parquet_read::page::DictPage;

    #[test]
    fn try_new_rejects_num_values_exceeding_buffer() {
        // A foreign dictionary page header can claim a huge num_values (up to
        // i32::MAX) while carrying a tiny buffer. Each value needs at least a
        // 4-byte length prefix, so try_new must reject the header up front with
        // its own "too short to hold" error -- distinct from the per-element
        // loop's "too short to read" -- before it reaches
        // Vec::with_capacity(num_values), whose tens-of-gigabytes reservation
        // aborts the process when the allocator refuses it. (with_capacity is
        // lazy, so this pins the guard's rejection and its distinct message, not
        // the abort itself, which is allocator/overcommit dependent.)
        let buffer = [0u8; 8]; // room for at most 2 zero-length values
        let dict_page = DictPage {
            buffer: &buffer,
            num_values: i32::MAX as usize,
            is_sorted: false,
        };
        let err = BaseVarDictDecoder::try_new(&dict_page)
            .err()
            .expect("oversized num_values must error, not abort");
        assert!(format!("{err}").contains("too short to hold"), "got: {err}");
    }

    #[test]
    fn try_new_accepts_values_filling_buffer() {
        // Boundary: num_values == buffer.len() / 4 (all zero-length values) is a
        // well-formed page and must pass the guard unchanged.
        let buffer = [0u8; 8]; // two consecutive zero-length prefixes
        let dict_page = DictPage { buffer: &buffer, num_values: 2, is_sorted: false };
        let dict = BaseVarDictDecoder::try_new(&dict_page).unwrap();
        assert_eq!(dict.dict_values.len(), 2);
        assert!(dict.dict_values.iter().all(|v| v.is_empty()));
    }

    #[test]
    fn try_new_rejects_num_values_one_over_buffer() {
        // Tight boundary: one more than the buffer can hold (len/4 + 1) must be
        // rejected. Sits right next to try_new_accepts_values_filling_buffer
        // (which accepts exactly len/4) to pin the comparison and catch a
        // `>`-to-`>=` or off-by-one regression in the bound.
        let buffer = [0u8; 8]; // holds at most 2 zero-length values (len/4 == 2)
        let dict_page = DictPage { buffer: &buffer, num_values: 3, is_sorted: false };
        let err = BaseVarDictDecoder::try_new(&dict_page)
            .err()
            .expect("num_values one over capacity must error");
        assert!(format!("{err}").contains("too short to hold"), "got: {err}");
    }
}
