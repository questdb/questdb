use num_traits::AsPrimitive;
use parquet2::encoding::delta_bitpacked;

use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use std::any::TypeId;
use std::mem::size_of;
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

/// A decoder for primitive types with plain encoding
/// T is the source type
/// U is the destination type
pub struct PlainPrimitiveDecoder<'a, T, U> {
    values: *const T,
    values_offset: usize,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut U,
    buffers_offset: usize,
    null_value: U,
}

impl<'a, T, U> PlainPrimitiveDecoder<'a, T, U> {
    pub fn new(values: &'a [u8], buffers: &'a mut ColumnChunkBuffers, null_value: U) -> Self {
        let existing = buffers.data_vec.len();
        debug_assert_eq!(
            existing % std::mem::size_of::<U>(),
            0,
            "data_vec length is not aligned to element size"
        );
        let buffers_offset = existing / std::mem::size_of::<U>();
        Self {
            values: values.as_ptr().cast(),
            values_offset: 0,
            buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
            buffers,
            buffers_offset,
            null_value,
        }
    }
}

impl<'a, T, U> Pushable for PlainPrimitiveDecoder<'a, T, U>
where
    U: Copy + 'static,
    T: AsPrimitive<U>,
{
    fn push(&mut self) -> ParquetResult<()> {
        unsafe {
            *self.buffers_ptr.add(self.buffers_offset) =
                self.values.add(self.values_offset).read_unaligned().as_();
            self.buffers_offset += 1;
            self.values_offset += 1;
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
        if size_of::<T>() == size_of::<U>() && TypeId::of::<T>() == TypeId::of::<U>() {
            // Same type, same layout: bulk copy (also handles unaligned source)
            unsafe {
                ptr::copy_nonoverlapping(
                    self.values.add(self.values_offset) as *const u8,
                    self.buffers_ptr.add(self.buffers_offset) as *mut u8,
                    count * size_of::<T>(),
                );
            }
        } else {
            let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
            for i in 0..count {
                unsafe {
                    *out.add(i) = self
                        .values
                        .add(self.values_offset + i)
                        .read_unaligned()
                        .as_();
                }
            }
        }
        self.buffers_offset += count;
        self.values_offset += count;
        Ok(())
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
        self.values_offset += count;
    }

    fn result(&self) -> ParquetResult<()> {
        Ok(())
    }
}

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

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Day(i32);

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Millis(i64);

impl AsPrimitive<Millis> for Day {
    fn as_(self) -> Millis {
        Millis(self.0 as i64 * 24 * 60 * 60 * 1000)
    }
}

impl Millis {
    pub const NULL_VALUE: Self = Self(i64::MIN);

    pub fn new(value: i64) -> Self {
        Self(value)
    }
}

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
            let dest = self.buffers.data_vec.as_mut_ptr().add(base);
            reverse_bytes::<N>(slice.as_ptr(), dest);
            self.buffers.data_vec.set_len(base + N);
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        if count == 0 {
            return Ok(());
        }
        let base = self.buffers.data_vec.len();
        let total_bytes = count * N;
        debug_assert!(base + total_bytes <= self.buffers.data_vec.capacity());

        // Bulk copy from slicer, then reverse each chunk in-place.
        self.slicer
            .next_slice_into(count, &mut self.buffers.data_vec)?;

        unsafe {
            let ptr = self.buffers.data_vec.as_mut_ptr().add(base);
            for c in 0..count {
                reverse_bytes_inplace::<N>(ptr.add(c * N));
            }
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
        if count == 0 {
            return Ok(());
        }
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

/// Reverse N bytes from src to dest (out-of-place).
#[inline(always)]
unsafe fn reverse_bytes<const N: usize>(src: *const u8, dest: *mut u8) {
    if N == 16 {
        // UUID fast path: use u64 swap_bytes
        let lo = (src as *const u64).read_unaligned();
        let hi = (src.add(8) as *const u64).read_unaligned();
        (dest as *mut u64).write_unaligned(hi.swap_bytes());
        (dest.add(8) as *mut u64).write_unaligned(lo.swap_bytes());
    } else {
        for i in 0..N {
            *dest.add(i) = *src.add(N - i - 1);
        }
    }
}

/// Reverse N bytes in-place.
#[inline(always)]
unsafe fn reverse_bytes_inplace<const N: usize>(ptr: *mut u8) {
    if N == 16 {
        // UUID fast path: read two u64s, swap and write in reverse order
        let lo = (ptr as *const u64).read_unaligned();
        let hi = (ptr.add(8) as *const u64).read_unaligned();
        (ptr as *mut u64).write_unaligned(hi.swap_bytes());
        (ptr.add(8) as *mut u64).write_unaligned(lo.swap_bytes());
    } else {
        for i in 0..N / 2 {
            let a = *ptr.add(i);
            let b = *ptr.add(N - 1 - i);
            *ptr.add(i) = b;
            *ptr.add(N - 1 - i) = a;
        }
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
