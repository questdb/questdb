use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::page::DictPage;
use crate::parquet_read::slicer::rle::{RepeatN, RleIterator};
use crate::parquet_read::ColumnChunkBuffers;
use std::mem::size_of;

use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};
use std::ptr;

pub trait DictDecoder {
    fn get_dict_value(&self, index: u32) -> &[u8];
    fn avg_key_len(&self) -> f32;
    fn len(&self) -> u32;
}

pub trait PrimitiveDictDecoder<T> {
    // Decode a value from the dictionary at the given index. The caller must ensure that the index is within bounds.
    fn get_dict_value(&self, index: u32) -> T;

    // Get the number of values in the dictionary.
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

/// A dictionary decoder for primitive types.
/// U is the physical type of the dictionary value.
/// T is the destination type of the dictionary value (e.g. short, etc.).
pub struct PrimitiveFixedDictDecoder<'a, U, T> {
    dict_page: &'a [u8],
    _u: std::marker::PhantomData<U>,
    _t: std::marker::PhantomData<T>,
}

impl<U, T> PrimitiveDictDecoder<T> for PrimitiveFixedDictDecoder<'_, U, T> {
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

impl<'a, U, T> PrimitiveFixedDictDecoder<'a, U, T> {
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

struct FastSlicerInner<'a> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    error: Option<ParquetError>,
}

impl<'a> FastSlicerInner<'a> {
    fn new(decoder: Option<Decoder<'a>>, iter: RleIterator<'a>) -> Self {
        Self { decoder, data: iter, error: None }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        let iter_err = || Err(fmt_err!(Layout, "Unexpected end of rle iterator"));

        let Some(ref mut decoder) = self.decoder else {
            return iter_err();
        };

        let Some(run) = decoder.next() else {
            return iter_err();
        };

        let num_bits = decoder.num_bits();
        match run? {
            HybridEncoded::Bitpacked(values) => {
                if num_bits == 8 {
                    // Fast path: each byte IS a dict index, skip the unpack step.
                    self.data = RleIterator::ByteIndices { data: values, pos: 0 };
                } else {
                    let count = values.len() * 8 / num_bits;
                    let inner_decoder =
                        bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                    self.data = RleIterator::Bitpacked(inner_decoder)
                }
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                let iterator = RepeatN::new(value, repeat);
                self.data = RleIterator::Rle(iterator);
            }
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        if self.error.is_some() {
            return;
        }

        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                match self.decode() {
                    Ok(()) => {}
                    Err(err) => {
                        self.error = Some(err);
                        return;
                    }
                }
            }
        }
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

pub struct RleDictionaryDecoder<'a, U, T: PrimitiveDictDecoder<U>>
where
    U: Copy + 'static,
{
    dict: T,
    inner: FastSlicerInner<'a>,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut U,
    buffers_offset: usize,
    null_value: U,
    _phantom: std::marker::PhantomData<U>,
}

impl<U, T: PrimitiveDictDecoder<U>> Pushable for RleDictionaryDecoder<'_, U, T>
where
    U: Copy + 'static,
{
    fn push(&mut self) -> ParquetResult<()> {
        if self.inner.error.is_some() {
            return Ok(());
        }

        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                unsafe {
                    *self.buffers_ptr.add(self.buffers_offset) = self.dict.get_dict_value(idx);
                    self.buffers_offset += 1;
                }
                Ok(())
            } else {
                self.inner.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ));
                Ok(())
            }
        } else {
            // This recursive is safe, it cannot go deeper than 1 level down.
            // After a successful call to `self.inner.decode()` there will be a value in `self.inner.data.next()`
            match self.decode() {
                Ok(()) => self.push(),
                Err(err) => {
                    self.inner.error = Some(err);
                    return Ok(());
                }
            }
        }
    }

    fn push_null(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.null_value;
            self.buffers_offset += 1;
        }
        Ok(())
    }

    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        for i in 0..count {
            unsafe {
                *out.add(i) = self.null_value;
            }
        }
        self.buffers_offset += count;
        Ok(())
    }

    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let mut remaining = count;
        loop {
            if remaining == 0 || self.inner.error.is_some() {
                return Ok(());
            }

            let consumed = match &mut self.inner.data {
                RleIterator::Rle(repeat) => {
                    let n = remaining.min(repeat.remaining);
                    if n == 0 {
                        0
                    } else {
                        if repeat.value >= self.dict.len() {
                            self.inner.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                repeat.value,
                                self.dict.len()
                            ));
                            return Ok(());
                        }
                        let value = self.dict.get_dict_value(repeat.value);
                        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
                        for i in 0..n {
                            unsafe {
                                *out.add(i) = value;
                            }
                        }
                        self.buffers_offset += n;
                        repeat.remaining -= n;
                        n
                    }
                }
                RleIterator::Bitpacked(bp) => {
                    let dict_len = self.dict.len();
                    let out_base = self.buffers_ptr;
                    let mut offset = self.buffers_offset;
                    let mut consumed = 0usize;
                    let unpack_ptr = bp.unpacked.as_ref().as_ptr();

                    while consumed < remaining && bp.remaining > 0 {
                        let avail_in_pack = (32 - bp.current_pack_index).min(bp.remaining);
                        let n = avail_in_pack.min(remaining - consumed);
                        let start = bp.current_pack_index;

                        for i in 0..n {
                            let idx = unsafe { *unpack_ptr.add(start + i) };
                            if idx >= dict_len {
                                self.buffers_offset = offset;
                                self.inner.error = Some(fmt_err!(
                                    Layout,
                                    "index {} is out of dict bounds {}",
                                    idx,
                                    dict_len
                                ));
                                return Ok(());
                            }
                            unsafe {
                                *out_base.add(offset) = self.dict.get_dict_value(idx);
                            }
                            offset += 1;
                        }

                        bp.current_pack_index += n;
                        bp.remaining -= n;
                        consumed += n;

                        if bp.current_pack_index == 32 {
                            bp.decode_next_pack();
                        }
                    }

                    self.buffers_offset = offset;
                    consumed
                }
                RleIterator::ByteIndices { data, pos } => {
                    let dict_len = self.dict.len();
                    let out_base = self.buffers_ptr;
                    let mut offset = self.buffers_offset;
                    let avail = data.len() - *pos;
                    let n = remaining.min(avail);
                    let bytes = &data[*pos..];

                    for i in 0..n {
                        let idx = unsafe { *bytes.get_unchecked(i) } as u32;
                        if idx >= dict_len {
                            self.buffers_offset = offset;
                            self.inner.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                idx,
                                dict_len
                            ));
                            return Ok(());
                        }
                        unsafe {
                            *out_base.add(offset) = self.dict.get_dict_value(idx);
                        }
                        offset += 1;
                    }

                    *pos += n;
                    self.buffers_offset = offset;
                    n
                }
            };

            remaining -= consumed;
            if remaining > 0 {
                match self.decode() {
                    Ok(()) => {}
                    Err(err) => {
                        self.inner.error = Some(err);
                        return Ok(());
                    }
                }
            }
        }
    }

    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = (self.buffers_offset + count) * std::mem::size_of::<U>();
        if self.buffers.data_vec.len() < needed {
            let additional = needed - self.buffers.data_vec.len();
            self.buffers.data_vec.reserve(additional)?;
            unsafe {
                self.buffers.data_vec.set_len(needed);
            }
        }
        self.buffers_ptr = self.buffers.data_vec.as_mut_ptr().cast();
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        self.inner.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.inner.result()
    }
}

impl<'a, T: PrimitiveDictDecoder<U>, U> RleDictionaryDecoder<'a, U, T>
where
    U: Copy + 'static,
{
    pub fn try_new(
        mut buffer: &'a [u8],
        dict: T,
        row_count: usize,
        null_value: U,
        buffers: &'a mut ColumnChunkBuffers,
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                dict,
                inner: FastSlicerInner::new(Some(decoder), RleIterator::Rle(RepeatN::new(0, 0))),
                _phantom: std::marker::PhantomData,
                buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
                buffers,
                buffers_offset: 0,
                null_value,
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                dict,
                inner: FastSlicerInner::new(None, RleIterator::Rle(RepeatN::new(0, row_count))),
                _phantom: std::marker::PhantomData,
                buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
                buffers,
                buffers_offset: 0,
                null_value,
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        self.inner.decode()
    }
}
