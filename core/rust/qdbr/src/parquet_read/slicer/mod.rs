pub mod dict_decoder;
pub mod dict_slicer;
pub mod rle;

use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use parquet2::encoding::delta_bitpacked;
use parquet2::encoding::hybrid_rle::BitmapIter;
use std::mem::size_of;
use std::ptr;

pub trait DataPageSlicer {
    fn next(&mut self) -> &[u8];
    fn next_slice(&mut self, count: usize) -> Option<&[u8]>;
    fn skip(&mut self, count: usize);
    fn count(&self) -> usize;
    fn data_size(&self) -> usize;
    fn result(&self) -> ParquetResult<()>;
}

pub struct DataPageFixedSlicer<'a, const N: usize> {
    data: &'a [u8],
    pos: usize,
    sliced_row_count: usize,
}

impl<const N: usize> DataPageSlicer for DataPageFixedSlicer<'_, N> {
    fn next(&mut self) -> &[u8] {
        let res = &self.data[self.pos..self.pos + N];
        self.pos += N;
        res
    }

    fn next_slice(&mut self, count: usize) -> Option<&[u8]> {
        let res = &self.data[self.pos..self.pos + N * count];
        self.pos += N * count;
        Some(res)
    }

    fn skip(&mut self, count: usize) {
        self.pos += N * count;
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a, const N: usize> DataPageFixedSlicer<'a, N> {
    pub fn new(data: &'a [u8], row_count: usize) -> Self {
        Self { data, pos: 0, sliced_row_count: row_count }
    }
}

pub struct DeltaBinaryPackedSlicer<'a, const N: usize> {
    decoder: delta_bitpacked::Decoder<'a>,
    sliced_row_count: usize,
    error: ParquetResult<()>,
    error_value: [u8; N],
    buffer: [u8; N],
}

impl<const N: usize> DataPageSlicer for DeltaBinaryPackedSlicer<'_, N> {
    fn next(&mut self) -> &[u8] {
        let res = self.decoder.next();
        match res {
            Some(val) => match val {
                Ok(val) => {
                    let bytes = val.to_le_bytes();
                    self.buffer[..N].copy_from_slice(&bytes[..N]);
                    &self.buffer
                }
                Err(_) => {
                    // TODO(amunra): Clean-up, this is _not_ a layout error!
                    self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                    &self.error_value
                }
            },
            None => {
                // TODO(amunra): Clean-up, this is _not_ a layout error!
                self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                &self.error_value
            }
        }
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.decoder.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count * N
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a, const N: usize> DeltaBinaryPackedSlicer<'a, N> {
    pub fn try_new(data: &'a [u8], row_count: usize) -> ParquetResult<Self> {
        let decoder = delta_bitpacked::Decoder::try_new(data)?;
        Ok(Self {
            decoder,
            sliced_row_count: row_count,
            error: Ok(()),
            error_value: [0; N],
            buffer: [0; N],
        })
    }
}

pub struct DeltaLengthArraySlicer<'a> {
    data: &'a [u8],
    sliced_row_count: usize,
    index: usize,
    lengths: Vec<i32>,
    pos: usize,
}

impl DataPageSlicer for DeltaLengthArraySlicer<'_> {
    fn next(&mut self) -> &[u8] {
        let len = self.lengths[self.index] as usize;
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        self.index += 1;
        res
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.pos += self.lengths[self.index] as usize;
            self.index += 1;
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> DeltaLengthArraySlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let mut decoder = delta_bitpacked::Decoder::try_new(data)?;
        let lengths: Vec<_> = decoder
            .by_ref()
            .take(row_count)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();

        let data_offset = decoder.consumed_bytes();
        Ok(Self {
            data: &data[data_offset..],
            sliced_row_count,
            index: 0,
            lengths,
            pos: 0,
        })
    }
}

pub struct DeltaBytesArraySlicer<'a> {
    prefix: std::vec::IntoIter<i32>,
    suffix: std::vec::IntoIter<i32>,
    data: &'a [u8],
    data_offset: usize,
    sliced_row_count: usize,
    last_value: Vec<u8>,
    error: ParquetResult<()>,
}

impl<'a> DataPageSlicer for DeltaBytesArraySlicer<'a> {
    fn next(&mut self) -> &[u8] {
        let prefix_len = self.prefix.next();
        match &self.error {
            Ok(_) => match prefix_len {
                Some(prefix_len) => {
                    let prefix_len = prefix_len as usize;
                    let suffix_len = self.suffix.next();
                    match suffix_len {
                        Some(suffix_len) => {
                            let suffix_len = suffix_len as usize;
                            self.last_value.truncate(prefix_len);

                            self.last_value.extend_from_slice(
                                &self.data[self.data_offset..self.data_offset + suffix_len],
                            );
                            self.data_offset += suffix_len;

                            let extend_lifetime = unsafe {
                                std::mem::transmute::<&[u8], &'a [u8]>(self.last_value.as_slice())
                            };
                            extend_lifetime
                        }
                        None => {
                            self.error =
                                Err(fmt_err!(Layout, "not enough suffix values to iterate"));
                            &[]
                        }
                    }
                }
                None => {
                    self.error = Err(fmt_err!(Layout, "not enough prefix values to iterate"));
                    &[]
                }
            },
            Err(_) => &[],
        }
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> DeltaBytesArraySlicer<'a> {
    pub fn try_new(
        data: &'a [u8],
        row_count: usize,
        sliced_row_count: usize,
    ) -> ParquetResult<Self> {
        let values = data;
        let mut decoder = delta_bitpacked::Decoder::try_new(values)?;
        let prefix = (&mut decoder)
            .take(row_count)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();

        let mut data_offset = decoder.consumed_bytes();
        let mut decoder = delta_bitpacked::Decoder::try_new(&values[decoder.consumed_bytes()..])?;
        let suffix = (&mut decoder)
            .map(|r| r.map(|v| v as i32).unwrap())
            .collect::<Vec<_>>();
        data_offset += decoder.consumed_bytes();

        Ok(Self {
            prefix: prefix.into_iter(),
            suffix: suffix.into_iter(),
            data: values,
            data_offset,
            sliced_row_count,
            last_value: vec![],
            error: Ok(()),
        })
    }
}

pub struct PlainVarSlicer<'a> {
    data: &'a [u8],
    pos: usize,
    sliced_row_count: usize,
}

impl DataPageSlicer for PlainVarSlicer<'_> {
    #[inline]
    fn next(&mut self) -> &[u8] {
        let len =
            unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) } as usize;
        self.pos += size_of::<u32>();
        let res = &self.data[self.pos..self.pos + len];
        self.pos += len;
        res
    }

    #[inline]
    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            let len =
                unsafe { ptr::read_unaligned(self.data.as_ptr().add(self.pos) as *const u32) };
            self.pos += len as usize + size_of::<u32>();
        }
    }

    #[inline]
    fn count(&self) -> usize {
        self.sliced_row_count
    }

    #[inline]
    fn data_size(&self) -> usize {
        self.data.len()
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

impl<'a> PlainVarSlicer<'a> {
    pub fn new(data: &'a [u8], sliced_row_count: usize) -> Self {
        Self { data, pos: 0, sliced_row_count }
    }
}

pub struct BooleanBitmapSlicer<'a> {
    bitmap_iter: BitmapIter<'a>,
    sliced_row_count: usize,
    error: ParquetResult<()>,
}

const BOOL_TRUE: [u8; 1] = [1];
const BOOL_FALSE: [u8; 1] = [0];

impl DataPageSlicer for BooleanBitmapSlicer<'_> {
    fn next(&mut self) -> &[u8] {
        if let Some(val) = self.bitmap_iter.next() {
            if val {
                return &BOOL_TRUE;
            }
            return &BOOL_FALSE;
        }
        self.error = Err(fmt_err!(Layout, "not enough bitmap values to iterate"));
        &BOOL_FALSE
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        for _ in 0..count {
            self.bitmap_iter.next();
        }
    }

    fn count(&self) -> usize {
        self.sliced_row_count
    }

    fn data_size(&self) -> usize {
        self.sliced_row_count
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}

impl<'a> BooleanBitmapSlicer<'a> {
    pub fn new(data: &'a [u8], row_count: usize, sliced_row_count: usize) -> Self {
        let bitmap_iter = BitmapIter::new(data, 0, row_count);
        Self { bitmap_iter, sliced_row_count, error: Ok(()) }
    }
}

pub struct ValueConvertSlicer<const N: usize, T: DataPageSlicer, F: Fn(&[u8], &mut [u8; N])> {
    inner_slicer: T,
    converter: F,
    error: ParquetResult<()>,
    buffer: [u8; N],
}

impl<const N: usize, T: DataPageSlicer, F: Fn(&[u8], &mut [u8; N])> DataPageSlicer
    for ValueConvertSlicer<N, T, F>
{
    fn next(&mut self) -> &[u8] {
        let slice = self.inner_slicer.next();
        (self.converter)(slice, &mut self.buffer);
        &self.buffer
    }

    fn next_slice(&mut self, _count: usize) -> Option<&[u8]> {
        None
    }

    fn skip(&mut self, count: usize) {
        self.inner_slicer.skip(count);
    }

    fn count(&self) -> usize {
        self.inner_slicer.count()
    }

    fn data_size(&self) -> usize {
        self.inner_slicer.count() * N
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone().or(self.inner_slicer.result())
    }
}

impl<const N: usize, T: DataPageSlicer, F: Fn(&[u8], &mut [u8; N])> ValueConvertSlicer<N, T, F> {
    pub fn new(inner_slicer: T, converter: F) -> Self {
        Self {
            inner_slicer,
            converter,
            error: Ok(()),
            buffer: [0; N],
        }
    }
}
