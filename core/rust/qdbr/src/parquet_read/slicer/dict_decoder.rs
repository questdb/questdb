use crate::parquet::error::{fmt_err, ParquetResult};
use parquet2::page::sliced::DictPageRef;
use std::mem::size_of;
use std::ptr;

pub trait DictDecoder {
    fn get_dict_value(&self, index: u32) -> &[u8];
    fn avg_key_len(&self) -> f32;
    fn len(&self) -> u32;
}

pub struct VarDictDecoder<'a> {
    pub dict_values: Vec<&'a [u8]>,
    pub avg_key_len: f32,
}

impl DictDecoder for VarDictDecoder<'_> {
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

impl<'a> VarDictDecoder<'a> {
    pub fn try_new(dict_page: &'a DictPageRef<'a>, is_utf8: bool) -> ParquetResult<Self> {
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

pub struct FixedDictDecoder<'a, const N: usize> {
    dict_page: &'a [u8],
}

impl<const N: usize> DictDecoder for FixedDictDecoder<'_, N> {
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
    pub fn try_new(dict_page: &'a DictPageRef<'a>) -> ParquetResult<Self> {
        if N * dict_page.num_values != dict_page.buffer.len() {
            return Err(fmt_err!(
                Layout,
                "dictionary data page size is not multiple of {N}"
            ));
        }

        Ok(Self { dict_page: dict_page.buffer.as_ref() })
    }
}
