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
    pub fn try_new(dict_page: &'a DictPage<'a>, is_utf8: bool) -> ParquetResult<Self> {
        let mut dict_values: Vec<&[u8]> = Vec::with_capacity(dict_page.num_values);
        let mut offset = 0usize;
        let dict_data = &dict_page.buffer;

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
            if is_utf8 && std::str::from_utf8(str_slice).is_err() {
                return Err(fmt_err!(
                    Layout,
                    "dictionary value {i} ({str_slice:?}) is not valid utf8"
                ));
            }

            dict_values.push(str_slice);
            offset += str_len;
            total_key_len += str_len;
        }

        Ok(Self {
            dict_values,
            avg_key_len: total_key_len as f32 / dict_page.num_values as f32,
        })
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

        Ok(Self { dict_page: dict_page.buffer.as_ref() })
    }
}

pub trait PrimitiveDictDecoder<T> {
    /// Decodes value at `index`. Caller guarantees index is in bounds.
    fn get_dict_value(&self, index: u32) -> T;

    /// Number of values in this dictionary.
    fn len(&self) -> u32;
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
    fn get_dict_value(&self, index: u32) -> T {
        unsafe {
            ptr::read_unaligned(
                self.dict_page.as_ptr().add(index as usize * size_of::<U>()) as *const T
            )
        }
    }

    #[inline]
    fn len(&self) -> u32 {
        (self.dict_page.len() / size_of::<U>()) as u32
    }
}

impl<'a, U, T> BasePrimitiveDictDecoder<'a, U, T> {
    pub fn try_new(dict_page: &'a DictPage<'a>) -> ParquetResult<Self> {
        if size_of::<U>() * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not a multiple of {}",
                size_of::<U>()
            ));
        }

        Ok(Self {
            dict_page: dict_page.buffer.as_ref(),
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
        self.len as u32
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
    _u: std::marker::PhantomData<U>,
    _t: std::marker::PhantomData<T>,
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
    pub fn new(dict_page: &'a DictPage<'a>, converter: V) -> Self {
        Self {
            dict_page: dict_page.buffer.as_ref(),
            converter,
            _u: std::marker::PhantomData,
            _t: std::marker::PhantomData,
        }
    }
}
