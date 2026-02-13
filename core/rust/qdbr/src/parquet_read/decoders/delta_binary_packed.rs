//! Decoder for Parquet `DELTA_BINARY_PACKED` encoded primitive data.
//!
//! This adapter wraps `parquet2`'s delta decoder and implements the `Pushable`
//! interface so row-group decode can stream values directly into
//! `ColumnChunkBuffers`.

use num_traits::AsPrimitive;
use parquet2::encoding::delta_bitpacked;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::ColumnChunkBuffers;

/// A decoder for primitive types with delta-binary packed encoding
/// T is the destination type
pub struct DeltaBinaryPackedPrimitiveDecoder<'a, T>
where
    T: Copy + 'static,
    i64: AsPrimitive<T>,
{
    decoder: delta_bitpacked::Decoder<'a>,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut T,
    buffers_offset: usize,
    null_value: T,
    error: ParquetResult<()>,
}

impl<'a, T> DeltaBinaryPackedPrimitiveDecoder<'a, T>
where
    T: Copy + 'static,
    i64: AsPrimitive<T>,
{
    pub fn try_new(
        data: &'a [u8],
        buffers: &'a mut ColumnChunkBuffers,
        null_value: T,
    ) -> ParquetResult<Self> {
        let decoder = delta_bitpacked::Decoder::try_new(data)?;
        Ok(Self {
            decoder,
            buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
            buffers,
            buffers_offset: 0,
            null_value,
            error: Ok(()),
        })
    }

    #[inline]
    fn next(&mut self) -> T {
        let res = self.decoder.next();
        match res {
            Some(val) => match val {
                Ok(val) => val.as_(),
                Err(_) => {
                    // TODO(amunra): Clean-up, this is _not_ a layout error!
                    self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                    0i64.as_()
                }
            },
            None => {
                // TODO(amunra): Clean-up, this is _not_ a layout error!
                self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                0i64.as_()
            }
        }
    }
}

impl<'a, T> Pushable for DeltaBinaryPackedPrimitiveDecoder<'a, T>
where
    T: Copy + 'static,
    i64: AsPrimitive<T>,
{
    fn push(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) = self.next();
            self.buffers_offset += 1;
        }
        Ok(())
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
        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
        let mut remaining = count;
        let mut offset = 0;

        // Batch decode: decode i64 values in chunks, then convert to T
        let mut i64_buf = [0i64; 128];
        while remaining > 0 {
            let batch = remaining.min(128);
            match self.decoder.decode_batch(&mut i64_buf[..batch]) {
                Ok(decoded) => {
                    if decoded == 0 {
                        self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                        break;
                    }
                    unsafe {
                        for i in 0..decoded {
                            *out.add(offset + i) = i64_buf[i].as_();
                        }
                    }
                    offset += decoded;
                    remaining -= decoded;
                }
                Err(_) => {
                    self.error = Err(fmt_err!(Layout, "not enough values to iterate"));
                    break;
                }
            }
        }
        self.buffers_offset += offset;
        Ok(())
    }

    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = (self.buffers_offset + count) * std::mem::size_of::<T>();
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
        for _ in 0..count {
            self.next();
        }
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone()
    }
}
