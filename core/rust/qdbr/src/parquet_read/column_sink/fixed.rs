use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
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

/// A sink for Decimal128/256 types that swaps bytes within each 8-byte word.
/// Parquet stores decimals as big-endian byte arrays, but QuestDB stores
/// Decimal128/256 as multiple i64 values in little-endian order.
/// N is the total size (16 for Decimal128, 32 for Decimal256).
/// WORDS is the number of 8-byte words (2 for Decimal128, 4 for Decimal256).
pub struct WordSwapDecimalColumnSink<'a, const N: usize, const WORDS: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
}

impl<const N: usize, const WORDS: usize, T: DataPageSlicer> Pushable
    for WordSwapDecimalColumnSink<'_, N, WORDS, T>
{
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
            // Swap bytes within each 8-byte word from big-endian to little-endian
            for w in 0..WORDS {
                let src_offset = w * 8;
                let dst_offset = w * 8;
                for i in 0..8 {
                    *ptr.add(dst_offset + i) = slice[src_offset + 7 - i];
                }
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
                        // Swap bytes within each 8-byte word
                        for w in 0..WORDS {
                            let src_offset = w * 8;
                            let dst_offset = w * 8;
                            for i in 0..8 {
                                *dest.add(dst_offset + i) = slice[src_offset + 7 - i];
                            }
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

impl<'a, const N: usize, const WORDS: usize, T: DataPageSlicer>
    WordSwapDecimalColumnSink<'a, N, WORDS, T>
{
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
    ) -> Self {
        Self { slicer, buffers, null_value }
    }
}

/// A sink for decimal types that sign-extends from a smaller source size to a larger target size.
/// Parquet stores decimals as big-endian byte arrays, and QuestDB stores them as little-endian.
///
/// N is the target size in bytes (1, 2, 4, 8, 16, or 32).
/// `src_len` is the source size in bytes from the Parquet file.
/// If `src_len` > N, the value must be sign-extended (only leading sign bytes may be truncated).
///
/// For simple decimals (N <= 8): sign-extend and reverse all bytes.
/// For multi-word decimals (N = 16 or 32): sign-extend and swap bytes within each 8-byte word.
pub struct SignExtendDecimalColumnSink<'a, const N: usize, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: [u8; N],
    src_len: usize,
}

impl<const N: usize, T: DataPageSlicer> Pushable for SignExtendDecimalColumnSink<'_, N, T> {
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
            Self::sign_extend_and_convert(slice, ptr, self.src_len)?;
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for c in 0..count {
                let slice = self.slicer.next();
                let dest = ptr.add(c * N);
                Self::sign_extend_and_convert(slice, dest, self.src_len)?;
            }
            self.buffers.data_vec.set_len(base + total_bytes);
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers.data_vec.extend_from_slice(&self.null_value)?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
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

    #[inline]
    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result().clone()
    }
}

impl<'a, const N: usize, T: DataPageSlicer> SignExtendDecimalColumnSink<'a, N, T> {
    pub fn new(
        slicer: &'a mut T,
        buffers: &'a mut ColumnChunkBuffers,
        null_value: [u8; N],
        src_len: usize,
    ) -> Self {
        Self { slicer, buffers, null_value, src_len }
    }

    /// Sign-extend the source bytes to target size and convert from BE to LE.
    ///
    /// The source is in big-endian (most significant byte first).
    /// For N <= 8: we simply reverse bytes while sign-extending.
    /// For N = 16 or 32 (multi-word): we swap bytes within each 8-byte word.
    #[inline]
    unsafe fn sign_extend_and_convert(
        src: &[u8],
        dest: *mut u8,
        src_len: usize,
    ) -> ParquetResult<()> {
        debug_assert!(src.len() == src_len);
        if src_len == 0 {
            return Err(fmt_err!(
                Unsupported,
                "invalid decimal source length 0 for target size {}",
                N
            ));
        }
        let mut src = src;
        let mut src_len = src_len;
        if src_len > N {
            let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };
            let trunc = src_len - N;
            if src[..trunc].iter().any(|b| *b != sign_byte) {
                return Err(fmt_err!(
                    Unsupported,
                    "FixedLenByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and not sign-extended",
                    src_len,
                    N
                ));
            }
            let msb = src[trunc];
            if (msb & 0x80) != (sign_byte & 0x80) {
                return Err(fmt_err!(
                    Unsupported,
                    "FixedLenByteArray({}) decimal cannot be decoded to target size {} bytes: \
                     source is larger than target and would truncate significant digits",
                    src_len,
                    N
                ));
            }
            src = &src[trunc..];
            src_len = N;
        }

        // Determine sign byte from the most significant byte of source
        let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };

        if N <= 8 {
            // Simple case: reverse all bytes while sign-extending
            // Output is little-endian, so we write from least significant to most significant
            // The source bytes (big-endian) are at indices 0..src_len where 0 is MSB
            // After reversal, source[src_len-1] becomes output[0], source[src_len-2] becomes output[1], etc.
            // Sign extension bytes fill the remaining positions (src_len..N)

            // Write source bytes in reverse order (LE output)
            for i in 0..src_len {
                *dest.add(i) = src[src_len - 1 - i];
            }
            // Fill remaining bytes with sign extension
            for i in src_len..N {
                *dest.add(i) = sign_byte;
            }
        } else {
            // Multi-word case (N = 16 or 32)
            // QuestDB stores Decimal128/256 as multiple i64 words in a specific layout.
            // Each 8-byte word is little-endian internally.
            //
            // For Parquet big-endian source of R bytes, we need to:
            // 1. Sign-extend to N bytes (prepend N-R sign bytes)
            // 2. Swap bytes within each 8-byte word
            //
            // The extended big-endian representation looks like:
            // [sign_byte × (N-src_len)] [src[0]] [src[1]] ... [src[src_len-1]]
            //
            // Then we swap bytes within each 8-byte word.

            let words = N / 8;
            let sign_prefix = N - src_len;

            for w in 0..words {
                let word_start_in_extended = w * 8;
                let word_dest = dest.add(w * 8);

                // For each byte position in the output word (0..8),
                // we need the byte at position (word_start_in_extended + 7 - i) in the extended BE array
                for i in 0..8 {
                    let extended_pos = word_start_in_extended + 7 - i;
                    let byte = if extended_pos < sign_prefix {
                        // This position is in the sign-extension prefix
                        sign_byte
                    } else {
                        // This position maps to source byte at (extended_pos - (N - R))
                        src[extended_pos - sign_prefix]
                    };
                    *word_dest.add(i) = byte;
                }
            }
        }
        Ok(())
    }
}
