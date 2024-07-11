use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::ParquetResult;
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
pub type FixedBooleanColumnSink<'a, T> = FixedColumnSink<'a, 1, 1, T>;

impl<const N: usize, const R: usize, T: DataPageSlicer> Pushable for FixedColumnSink<'_, N, R, T> {
    fn reserve(&mut self) {
        self.buffers.data_vec.reserve(self.slicer.count() * N);
    }

    #[inline]
    fn push(&mut self) {
        if N == R {
            self.buffers.data_vec.extend_from_slice(self.slicer.next());
        } else {
            self.buffers
                .data_vec
                .extend_from_slice(&self.slicer.next()[..N]);
        }
    }

    #[inline]
    fn push_slice(&mut self, count: usize) {
        if N == R {
            if let Some(slice) = self.slicer.next_slice(count) {
                self.buffers.data_vec.extend_from_slice(slice);
                return;
            }
        }
        for _ in 0..count {
            self.push();
        }
    }

    #[inline]
    fn push_null(&mut self) {
        self.buffers.data_vec.extend_from_slice(self.null_value);
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) {
        for _ in 0..count {
            self.buffers.data_vec.extend_from_slice(self.null_value);
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
    fn reserve(&mut self) {
        self.buffers.data_vec.reserve(self.slicer.count() * N);
    }

    #[inline]
    fn push(&mut self) {
        let slice = self.slicer.next();
        for i in 0..N {
            self.buffers.data_vec.push(slice[N - i - 1]);
        }
    }

    #[inline]
    fn push_slice(&mut self, count: usize) {
        for _ in 0..count {
            self.push();
        }
    }

    #[inline]
    fn push_null(&mut self) {
        self.buffers.data_vec.extend_from_slice(&self.null_value);
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) {
        for _ in 0..count {
            self.buffers.data_vec.extend_from_slice(&self.null_value);
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
    fn reserve(&mut self) {
        self.buffers.data_vec.reserve(self.slicer.count() * 8);
    }

    #[inline]
    fn push(&mut self) {
        let x = self.slicer.next();
        Self::push_int96_as_epoch_microseconds(&mut self.buffers.data_vec, x);
    }

    #[inline]
    fn push_slice(&mut self, count: usize) {
        for _ in 0..count {
            self.push();
        }
    }

    #[inline]
    fn push_null(&mut self) {
        self.buffers.data_vec.extend_from_slice(self.null_value);
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) {
        for _ in 0..count {
            self.buffers.data_vec.extend_from_slice(self.null_value);
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

    fn push_int96_as_epoch_microseconds(data_vec: &mut Vec<u8>, bytes: &[u8]) {
        // INT96 layout:
        // - bytes[0..8]: nanoseconds within the day (8 bytes)
        // - bytes[8..12]: Julian date (4 bytes)

        // Extract nanoseconds within the day (little-endian)
        let nanoseconds_bytes = &bytes[0..8];
        let nanoseconds = u64::from_le_bytes(nanoseconds_bytes.try_into().unwrap());

        // Extract Julian date (little-endian)
        let julian_date_bytes = &bytes[8..12];
        let julian_date = u32::from_le_bytes(julian_date_bytes.try_into().unwrap());

        // Convert Julian date to days since Unix epoch
        let days_since_epoch = julian_date as i64 - 2440588; // Julian date epoch to Unix epoch offset

        // Calculate total microseconds since Unix epoch
        let microseconds_since_epoch =
            days_since_epoch * 86400i64 * 1_000_000i64 + nanoseconds as i64 / 1_000;

        data_vec.extend_from_slice(microseconds_since_epoch.to_le_bytes().as_ref());
    }
}

pub struct IntDecimalColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    null_value: &'static [u8],
    factor: f64,
}

impl<T: DataPageSlicer> Pushable for IntDecimalColumnSink<'_, T> {
    fn reserve(&mut self) {
        self.buffers.data_vec.reserve(self.slicer.count() * 4);
    }

    #[inline]
    fn push(&mut self) {
        let x = self.slicer.next();
        let x = unsafe { ptr::read_unaligned(x.as_ptr() as *const i32) };
        let double = x as f64 / self.factor;
        self.buffers
            .data_vec
            .extend_from_slice(double.to_le_bytes().as_ref());
    }

    #[inline]
    fn push_slice(&mut self, count: usize) {
        for _ in 0..count {
            self.push();
        }
    }

    #[inline]
    fn push_null(&mut self) {
        self.buffers.data_vec.extend_from_slice(self.null_value);
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) {
        for _ in 0..count {
            self.buffers.data_vec.extend_from_slice(self.null_value);
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
