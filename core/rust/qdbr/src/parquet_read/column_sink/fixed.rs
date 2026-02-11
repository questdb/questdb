use crate::allocator::AcVec;
use crate::parquet::error::ParquetResult;
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use std::ptr;

/// A sink for fixed length columns
/// This is a sink that is used to push data into a column chunk buffer.
/// N is the length of the fixed length column.
/// R is the length of the data that is read from the parquet file.
pub struct FixedColumnSink<'a, const N: usize, const R: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: &'static [u8; N],
}

// pub type FixedShortColumnSink<'a, T> = FixedColumnSink<'a, 2, 2, T>;
pub type FixedIntColumnSink<'a, T> = FixedColumnSink<'a, 4, 4, T>;
pub type FixedLongColumnSink<'a, T> = FixedColumnSink<'a, 8, 8, T>;
pub type FixedDoubleColumnSink<'a, T> = FixedColumnSink<'a, 8, 8, T>;
pub type FixedFloatColumnSink<'a, T> = FixedColumnSink<'a, 4, 4, T>;
pub type FixedInt2ShortColumnSink<'a, T> = FixedColumnSink<'a, 2, 4, T>;
pub type FixedInt2ByteColumnSink<'a, T> = FixedColumnSink<'a, 1, 4, T>;
pub type FixedLong256ColumnSink<'a, T> = FixedColumnSink<'a, 32, 32, T>;
pub type FixedLong128ColumnSink<'a, T> = FixedColumnSink<'a, 16, 16, T>;
pub type FixedBooleanColumnSink<'a, T> = FixedColumnSink<'a, 1, 1, T>;

impl<const N: usize, const R: usize, T: DataPageSlicer> Pushable for FixedColumnSink<'_, N, R, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        if N == R {
            self.slicer.next_into(&mut self.buffers.data_vec)?;
        } else {
            self.buffers
                .data_vec
                .extend_from_slice(&self.slicer.next()[..N])?;
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        if N == R {
            self.slicer
                .next_slice_into(count, &mut self.buffers.data_vec)?;
        } else {
            for _ in 0..count {
                self.push()?;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, const N: usize, const R: usize, T: DataPageSlicer> FixedColumnSink<'a, N, R, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: &'static [u8; N],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }
}

pub struct ReverseFixedColumnSink<'a, const N: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
}

impl<const N: usize, T: DataPageSlicer> Pushable for ReverseFixedColumnSink<'_, N, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * N)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let slice = self.slicer.next();
        let base = self.buffers.data_vec.len();
        debug_assert!(base + N <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for i in 0..N {
                *ptr.add(i) = slice[N - i - 1];
            }
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push(),
            2 => {
                self.push()?;
                self.push()
            }
            3 => {
                self.push()?;
                self.push()?;
                self.push()
            }
            4 => {
                self.push()?;
                self.push()?;
                self.push()?;
                self.push()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for c in 0..count {
                        let slice = self.slicer.next();
                        let dest = ptr.add(c * N);
                        for i in 0..N {
                            *dest.add(i) = slice[N - i - 1];
                        }
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * N;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(self.null_value.as_ptr(), ptr.add(i * N), N);
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result().clone()
    }
}

impl<'a, const N: usize, T: DataPageSlicer> ReverseFixedColumnSink<'a, N, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }
}

pub struct NanoTimestampColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: &'static [u8],
}

impl<T: DataPageSlicer> Pushable for NanoTimestampColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * 8)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let x = self.slicer.next();
        Self::push_int96_as_epoch_nanos(&mut self.buffers.data_vec, x)?;
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.push()?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let null_size = self.null_value.len();
                let base = self.buffers.data_vec.len();
                let total_bytes = count * null_size;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(
                            self.null_value.as_ptr(),
                            ptr.add(i * null_size),
                            null_size,
                        );
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> NanoTimestampColumnSink<'a, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: &'static [u8],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }

    fn push_int96_as_epoch_nanos(data_vec: &mut AcVec<u8>, bytes: &[u8]) -> ParquetResult<()> {
        // INT96 layout:
        // - bytes[0..8]: nanoseconds within the day (8 bytes)
        // - bytes[8..12]: Julian date (4 bytes)
        const NANOS_PER_DAY: i64 = 86400 * 1_000_000_000;
        const JULIAN_UNIX_EPOCH_OFFSET: i64 = 2440588;

        // Extract nanoseconds within the day (little-endian)
        let nanos_bytes = &bytes[0..8];
        let nanos = u64::from_le_bytes(nanos_bytes.try_into().unwrap());

        // Extract Julian date (little-endian)
        let julian_date_bytes = &bytes[8..12];
        let julian_date = u32::from_le_bytes(julian_date_bytes.try_into().unwrap());

        // Convert Julian date to days since Unix epoch
        let days_since_epoch = julian_date as i64 - JULIAN_UNIX_EPOCH_OFFSET; // Julian date epoch to Unix epoch offset

        // Calculate total nanoseconds since Unix epoch
        let nanos_since_epoch = days_since_epoch * NANOS_PER_DAY + nanos as i64;

        data_vec.extend_from_slice(nanos_since_epoch.to_le_bytes().as_ref())?;
        Ok(())
    }
}

pub struct IntDecimalColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: &'static [u8],
    factor: f64,
}

impl<T: DataPageSlicer> Pushable for IntDecimalColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.data_vec.reserve(count * 8)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let x = self.slicer.next();
        let x = unsafe { ptr::read_unaligned(x.as_ptr() as *const i32) };
        let double = x as f64 / self.factor;
        self.buffers
            .data_vec
            .extend_from_slice(&double.to_le_bytes())?;
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push(),
            2 => {
                self.push()?;
                self.push()
            }
            3 => {
                self.push()?;
                self.push()?;
                self.push()
            }
            4 => {
                self.push()?;
                self.push()?;
                self.push()?;
                self.push()
            }
            _ => {
                let base = self.buffers.data_vec.len();
                let total_bytes = count * 8;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let out_ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        let x = self.slicer.next();
                        let x = ptr::read_unaligned(x.as_ptr() as *const i32);
                        let double = x as f64 / self.factor;
                        ptr::copy_nonoverlapping(
                            double.to_le_bytes().as_ptr(),
                            out_ptr.add(i * 8),
                            8,
                        );
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(),
            2 => {
                self.push_null()?;
                self.push_null()
            }
            3 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            4 => {
                self.push_null()?;
                self.push_null()?;
                self.push_null()?;
                self.push_null()
            }
            _ => {
                let null_size = self.null_value.len();
                let base = self.buffers.data_vec.len();
                let total_bytes = count * null_size;
                debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

                unsafe {
                    let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
                    for i in 0..count {
                        ptr::copy_nonoverlapping(
                            self.null_value.as_ptr(),
                            ptr.add(i * null_size),
                            null_size,
                        );
                    }
                    self.buffers.data_vec.set_len(base + total_bytes);
                }
                Ok(())
            }
        }
    }

    #[inline]
    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> IntDecimalColumnSink<'a, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: &'static [u8],
        scale: i32,
    ) -> Self {
        Self {
            slicer,
            buffers,
            null_value,
            factor: 10_f64.powi(scale),
        }
    }
}
