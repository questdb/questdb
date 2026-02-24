//! Specialized decoder for RLE-dictionary-encoded VarcharSlice columns.
//!
//! Combines RLE index decoding with direct VarcharSlice aux entry writing,
//! eliminating the `RleDictionarySlicer` → `VarcharSliceColumnSink` indirection.
//! Dict values are pre-computed into 16-byte aux entries at construction time,
//! so each decoded index requires only a single 16-byte memcpy.

use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::dictionary::BaseVarDictDecoder;
use crate::parquet_read::decoders::{RepeatN, RleIterator};
use crate::parquet_read::page::DictPage;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::varchar::SLICE_NULL_HEADER;

use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

const AUX_ENTRY_SIZE: usize = 16;

pub struct RleDictVarcharSliceDecoder<'a> {
    buffers: &'a mut ColumnChunkBuffers,
    /// Pre-computed aux entries for each dict value: [len_and_flags, ptr].
    dict_aux: Vec<[u64; 2]>,
    null_entry: [u64; 2],
    dict_len: u32,
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    error: Option<ParquetError>,
}

impl<'a> RleDictVarcharSliceDecoder<'a> {
    pub fn try_new(
        mut buffer: &'a [u8],
        dict_page: &'a DictPage<'a>,
        buffers: &'a mut ColumnChunkBuffers,
        ascii: bool,
    ) -> ParquetResult<Self> {
        let dict_decoder = BaseVarDictDecoder::try_new(dict_page, true)?;

        let mut dict_aux = Vec::with_capacity(dict_decoder.dict_values.len());
        for &value in &dict_decoder.dict_values {
            let len = value.len() as u32;
            let header: u32 = (len << 4) | if ascii || len == 0 { 3 } else { 1 };
            let ptr = value.as_ptr() as u64;
            dict_aux.push([header as u64, ptr]);
        }

        let null_entry = [SLICE_NULL_HEADER as u64, 0u64];
        let dict_len = dict_decoder.dict_values.len() as u32;

        let num_bits = *buffer
            .first()
            .ok_or_else(|| fmt_err!(Layout, "empty RLE dictionary buffer"))?;
        if num_bits > 0 {
            buffer = &buffer[1..];
            let hybrid_decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                buffers,
                dict_aux,
                null_entry,
                dict_len,
                decoder: Some(hybrid_decoder),
                data: RleIterator::Rle(RepeatN::new(0, 0)),
                error: None,
            };
            res.decode_next_run()?;
            Ok(res)
        } else {
            // Zero bit width: single dict entry, all indices are 0.
            // We don't know row_count here, but we use usize::MAX as a
            // practically infinite repeat count — the caller controls how
            // many values are consumed via push/push_slice.
            Ok(Self {
                buffers,
                dict_aux,
                null_entry,
                dict_len,
                decoder: None,
                data: RleIterator::Rle(RepeatN::new(0, usize::MAX)),
                error: None,
            })
        }
    }

    fn decode_next_run(&mut self) -> ParquetResult<()> {
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
                    self.data = RleIterator::ByteIndices { data: values, pos: 0 };
                } else {
                    let count = values.len() * 8 / num_bits;
                    let inner_decoder =
                        bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                    self.data = RleIterator::Bitpacked(inner_decoder);
                }
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                self.data = RleIterator::Rle(RepeatN::new(value, repeat));
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn write_aux_entry(&self, aux_ptr: *mut u8, entry: &[u64; 2]) {
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, entry[0]);
            std::ptr::write_unaligned(addr.add(1), entry[1]);
        }
    }
}

impl Pushable for RleDictVarcharSliceDecoder<'_> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * AUX_ENTRY_SIZE)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        if self.error.is_some() {
            return Ok(());
        }

        if let Some(idx) = self.data.next() {
            if idx < self.dict_len {
                let entry = self.dict_aux[idx as usize];
                let aux_ptr = unsafe {
                    self.buffers
                        .aux_vec
                        .as_mut_ptr()
                        .add(self.buffers.aux_vec.len())
                };
                self.write_aux_entry(aux_ptr, &entry);
                unsafe {
                    self.buffers
                        .aux_vec
                        .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
                }
                Ok(())
            } else {
                self.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict_len
                ));
                Ok(())
            }
        } else {
            match self.decode_next_run() {
                Ok(()) => self.push(),
                Err(err) => {
                    self.error = Some(err);
                    Ok(())
                }
            }
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        self.write_aux_entry(aux_ptr, &self.null_entry);
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        let null = self.null_entry;
        for i in 0..count {
            unsafe {
                let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                std::ptr::write_unaligned(addr, null[0]);
                std::ptr::write_unaligned(addr.add(1), null[1]);
            }
        }
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + count * AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let mut remaining = count;
        loop {
            if remaining == 0 || self.error.is_some() {
                return Ok(());
            }

            let consumed = match &mut self.data {
                RleIterator::Rle(repeat) => {
                    let n = remaining.min(repeat.remaining);
                    if n == 0 {
                        0
                    } else {
                        if repeat.value >= self.dict_len {
                            self.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                repeat.value,
                                self.dict_len
                            ));
                            return Ok(());
                        }
                        let entry = self.dict_aux[repeat.value as usize];
                        let aux_ptr = unsafe {
                            self.buffers
                                .aux_vec
                                .as_mut_ptr()
                                .add(self.buffers.aux_vec.len())
                        };
                        for i in 0..n {
                            unsafe {
                                let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                                std::ptr::write_unaligned(addr, entry[0]);
                                std::ptr::write_unaligned(addr.add(1), entry[1]);
                            }
                        }
                        unsafe {
                            self.buffers
                                .aux_vec
                                .set_len(self.buffers.aux_vec.len() + n * AUX_ENTRY_SIZE);
                        }
                        repeat.remaining -= n;
                        n
                    }
                }
                RleIterator::Bitpacked(bp) => {
                    let dict_len = self.dict_len;
                    let dict_aux = &self.dict_aux;
                    let mut consumed = 0usize;
                    let unpack_ptr = bp.unpacked.as_ref().as_ptr();

                    while consumed < remaining && bp.remaining > 0 {
                        let avail_in_pack = (32 - bp.current_pack_index).min(bp.remaining);
                        let n = avail_in_pack.min(remaining - consumed);
                        let start = bp.current_pack_index;

                        let aux_ptr = unsafe {
                            self.buffers
                                .aux_vec
                                .as_mut_ptr()
                                .add(self.buffers.aux_vec.len())
                        };

                        for i in 0..n {
                            let idx = unsafe { *unpack_ptr.add(start + i) };
                            if idx >= dict_len {
                                unsafe {
                                    self.buffers.aux_vec.set_len(
                                        self.buffers.aux_vec.len() + consumed * AUX_ENTRY_SIZE,
                                    );
                                }
                                self.error = Some(fmt_err!(
                                    Layout,
                                    "index {} is out of dict bounds {}",
                                    idx,
                                    dict_len
                                ));
                                return Ok(());
                            }
                            let entry = dict_aux[idx as usize];
                            unsafe {
                                let addr =
                                    aux_ptr.add((consumed + i) * AUX_ENTRY_SIZE).cast::<u64>();
                                std::ptr::write_unaligned(addr, entry[0]);
                                std::ptr::write_unaligned(addr.add(1), entry[1]);
                            }
                        }

                        bp.current_pack_index += n;
                        bp.remaining -= n;
                        consumed += n;

                        if bp.current_pack_index == 32 {
                            bp.decode_next_pack();
                        }
                    }

                    unsafe {
                        self.buffers
                            .aux_vec
                            .set_len(self.buffers.aux_vec.len() + consumed * AUX_ENTRY_SIZE);
                    }
                    consumed
                }
                RleIterator::ByteIndices { data, pos } => {
                    let dict_len = self.dict_len;
                    let dict_aux = &self.dict_aux;
                    let avail = data.len() - *pos;
                    let n = remaining.min(avail);
                    let bytes = &data[*pos..];

                    let aux_ptr = unsafe {
                        self.buffers
                            .aux_vec
                            .as_mut_ptr()
                            .add(self.buffers.aux_vec.len())
                    };

                    for i in 0..n {
                        let idx = unsafe { *bytes.get_unchecked(i) } as u32;
                        if idx >= dict_len {
                            unsafe {
                                self.buffers
                                    .aux_vec
                                    .set_len(self.buffers.aux_vec.len() + i * AUX_ENTRY_SIZE);
                            }
                            *pos += i;
                            self.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                idx,
                                dict_len
                            ));
                            return Ok(());
                        }
                        let entry = dict_aux[idx as usize];
                        unsafe {
                            let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                            std::ptr::write_unaligned(addr, entry[0]);
                            std::ptr::write_unaligned(addr.add(1), entry[1]);
                        }
                    }

                    *pos += n;
                    unsafe {
                        self.buffers
                            .aux_vec
                            .set_len(self.buffers.aux_vec.len() + n * AUX_ENTRY_SIZE);
                    }
                    n
                }
            };

            remaining -= consumed;
            if remaining > 0 {
                match self.decode_next_run() {
                    Ok(()) => {}
                    Err(err) => {
                        self.error = Some(err);
                        return Ok(());
                    }
                }
            }
        }
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        if self.error.is_some() {
            return Ok(());
        }

        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                match self.decode_next_run() {
                    Ok(()) => {}
                    Err(err) => {
                        self.error = Some(err);
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}
