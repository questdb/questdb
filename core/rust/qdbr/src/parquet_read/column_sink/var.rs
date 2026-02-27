use crate::allocator::AcVec;
use crate::parquet::error::{ParquetErrorReason, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::array::{append_array_null, append_array_nulls, append_raw_array};
use crate::parquet_write::varchar::{append_varchar, append_varchar_null, append_varchar_nulls};
use std::mem::size_of;
use std::ptr;

const VARCHAR_AUX_SIZE: usize = 2 * size_of::<u64>();
const STRING_AUX_SIZE: usize = size_of::<u64>();
pub const ARRAY_AUX_SIZE: usize = 2 * size_of::<u64>();

#[inline]
fn write_offset_sequence(
    aux_vec: &mut AcVec<u8>,
    start: usize,
    step: usize,
    count: usize,
) -> ParquetResult<()> {
    const BATCH: usize = 128;
    let mut buf = [0u64; BATCH];
    let mut offset = start;
    let mut remaining = count;

    while remaining > 0 {
        let n = remaining.min(BATCH);
        for slot in buf.iter_mut().take(n) {
            *slot = (offset as u64).to_le();
            offset += step;
        }
        aux_vec.extend_from_slice(unsafe {
            std::slice::from_raw_parts(buf.as_ptr().cast(), n * size_of::<u64>())
        })?;
        remaining -= n;
    }
    Ok(())
}

pub struct VarcharColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    pub buffers: &'a mut ColumnChunkBuffers,
}

impl<T: DataPageSlicer> Pushable for VarcharColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        self.buffers.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        append_varchar(
            &mut self.buffers.aux_vec,
            &mut self.buffers.data_vec,
            self.slicer.next(),
        )
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        // TODO: optimize
        for _ in 0..count {
            append_varchar(
                &mut self.buffers.aux_vec,
                &mut self.buffers.data_vec,
                self.slicer.next(),
            )?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        append_varchar_null(&mut self.buffers.aux_vec, &self.buffers.data_vec)
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        append_varchar_nulls(&mut self.buffers.aux_vec, &self.buffers.data_vec, count)
    }

    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> VarcharColumnSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers) -> Self {
        Self { slicer, buffers }
    }
}

pub struct StringColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
    error: ParquetResult<()>,
}

impl<T: DataPageSlicer> Pushable for StringColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        if count > 0 {
            self.buffers
                .aux_vec
                .reserve((count + 1) * STRING_AUX_SIZE)?;
            if self.buffers.aux_vec.is_empty() {
                self.buffers
                    .aux_vec
                    .extend_from_slice(0u64.to_le_bytes().as_ref())?;
            }
        }
        self.buffers
            .data_vec
            .reserve(2 * self.slicer.data_size() + size_of::<u32>() * count)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let utf8 = self.slicer.next();
        let utf8_str = std::str::from_utf8(utf8);

        match utf8_str {
            Ok(utf8_str) => {
                let pos = self.buffers.data_vec.len();
                self.buffers
                    .data_vec
                    .resize(self.buffers.data_vec.len() + 4, 0u8)?;
                for c in utf8_str.encode_utf16() {
                    self.buffers.data_vec.extend_from_slice(&c.to_le_bytes())?;
                }

                // Set length in utf16 characters
                let len = (self.buffers.data_vec.len() - pos - 4) as u32 / 2;
                self.buffers.data_vec[pos..pos + 4].copy_from_slice(len.to_le_bytes().as_ref());

                // set aux pointer
                self.buffers
                    .aux_vec
                    .extend_from_slice(self.buffers.data_vec.len().to_le_bytes().as_ref())?;
            }
            Err(utf8_str_err) => {
                self.error = Err(ParquetErrorReason::Utf8Decode(utf8_str_err).into_err());
                self.push_null()?;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        // TODO: optimize
        for _ in 0..count {
            self.push()?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        self.buffers
            .data_vec
            .extend_from_slice((-1i32).to_le_bytes().as_ref())?;
        // set aux pointer
        self.buffers
            .aux_vec
            .extend_from_slice(self.buffers.data_vec.len().to_le_bytes().as_ref())?;
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
                self.buffers.data_vec.reserve(count * size_of::<i32>())?;

                // Fill data_vec with 0xff bytes (-1i32 per null)
                unsafe {
                    ptr::write_bytes(
                        self.buffers.data_vec.as_mut_ptr().add(base),
                        0xff,
                        count * size_of::<i32>(),
                    );
                    self.buffers
                        .data_vec
                        .set_len(base + count * size_of::<i32>());
                }
                write_offset_sequence(
                    &mut self.buffers.aux_vec,
                    base + size_of::<i32>(),
                    size_of::<i32>(),
                    count,
                )
            }
        }
    }

    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.error.clone().or(self.slicer.result().clone())
    }
}

impl<'a, T: DataPageSlicer> StringColumnSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers) -> Self {
        Self { slicer, buffers, error: Ok(()) }
    }
}

pub struct BinaryColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    buffers: &'a mut ColumnChunkBuffers,
}

impl<T: DataPageSlicer> Pushable for BinaryColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        if count > 0 {
            self.buffers
                .aux_vec
                .reserve((count + 1) * STRING_AUX_SIZE)?;
            if self.buffers.aux_vec.is_empty() {
                self.buffers
                    .aux_vec
                    .extend_from_slice(0u64.to_le_bytes().as_ref())?;
            }
        }
        self.buffers
            .data_vec
            .reserve(self.slicer.data_size() + size_of::<u64>() * count)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let slice = self.slicer.next();
        self.buffers
            .data_vec
            .extend_from_slice(slice.len().to_le_bytes().as_ref())?;
        self.buffers.data_vec.extend_from_slice(slice)?;

        // set aux pointer
        self.buffers
            .aux_vec
            .extend_from_slice(self.buffers.data_vec.len().to_le_bytes().as_ref())?;
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
        self.buffers
            .data_vec
            .extend_from_slice((-1i64).to_le_bytes().as_ref())?;
        // set aux pointer
        self.buffers
            .aux_vec
            .extend_from_slice(self.buffers.data_vec.len().to_le_bytes().as_ref())?;
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
                self.buffers.data_vec.reserve(count * size_of::<i64>())?;

                // Fill data_vec with 0xff bytes (-1i64 per null)
                unsafe {
                    ptr::write_bytes(
                        self.buffers.data_vec.as_mut_ptr().add(base),
                        0xff,
                        count * size_of::<i64>(),
                    );
                    self.buffers
                        .data_vec
                        .set_len(base + count * size_of::<i64>());
                }
                write_offset_sequence(
                    &mut self.buffers.aux_vec,
                    base + size_of::<i64>(),
                    size_of::<i64>(),
                    count,
                )
            }
        }
    }

    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result().clone()
    }
}

impl<'a, T: DataPageSlicer> BinaryColumnSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers) -> Self {
        Self { slicer, buffers }
    }
}

pub struct RawArrayColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    pub buffers: &'a mut ColumnChunkBuffers,
}

impl<T: DataPageSlicer> Pushable for RawArrayColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * ARRAY_AUX_SIZE)?;
        self.buffers.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        append_raw_array(
            &mut self.buffers.aux_vec,
            &mut self.buffers.data_vec,
            self.slicer.next(),
        )
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            append_raw_array(
                &mut self.buffers.aux_vec,
                &mut self.buffers.data_vec,
                self.slicer.next(),
            )?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        append_array_null(&mut self.buffers.aux_vec, &self.buffers.data_vec)
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        append_array_nulls(&mut self.buffers.aux_vec, &self.buffers.data_vec, count)
    }

    fn skip(&mut self, count: usize) {
        self.slicer.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> RawArrayColumnSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers) -> Self {
        Self { slicer, buffers }
    }
}
