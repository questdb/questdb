use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetErrorReason, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::array::{append_array_null, append_array_nulls, append_raw_array};
use crate::parquet_write::varchar::{
    append_varchar, append_varchar_null, append_varchar_nulls, append_varchar_slice,
    append_varchar_slice_null, append_varchar_slice_nulls,
};
use std::mem::size_of;
use std::ptr;

const VARCHAR_AUX_SIZE: usize = 2 * size_of::<u64>();
const STRING_AUX_SIZE: usize = size_of::<u64>();
pub const ARRAY_AUX_SIZE: usize = 2 * size_of::<u64>();

/// Marker written to bytes 4-7 of VarcharSlice aux entries by VarcharSliceSpillSink.
/// Distinguishes spill entries (which store data_vec offsets needing fixup) from
/// non-spill entries (which store absolute pointers and must not be touched).
/// Uses bit 31 to avoid collision with ASCII_FLAG (bit 0), which is stamped
/// into the same field after fixup completes.
const SPILL_MARKER: u32 = 0x8000_0000;

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
            *slot = offset as u64;
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

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
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

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
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

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
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

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
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

pub struct VarcharSliceColumnSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    pub buffers: &'a mut ColumnChunkBuffers,
    ascii: bool,
}

impl<T: DataPageSlicer> Pushable for VarcharSliceColumnSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let value = self.slicer.next();
        append_varchar_slice(&mut self.buffers.aux_vec, value, self.ascii)
    }

    #[inline]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            let value = self.slicer.next();
            append_varchar_slice(&mut self.buffers.aux_vec, value, self.ascii)?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        append_varchar_slice_null(&mut self.buffers.aux_vec)
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        append_varchar_slice_nulls(&mut self.buffers.aux_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> VarcharSliceColumnSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers, ascii: bool) -> Self {
        Self { slicer, buffers, ascii }
    }
}

/// VarcharSlice sink for DeltaByteArray encoding.
/// Strings are reconstructed incrementally - each slicer.next() overwrites the previous value.
/// We copy each reconstructed string to data_vec, storing offsets temporarily in aux,
/// then do a fixup pass to convert offsets to pointers.
pub struct VarcharSliceSpillSink<'a, T: DataPageSlicer> {
    slicer: &'a mut T,
    pub buffers: &'a mut ColumnChunkBuffers,
    ascii: bool,
}

impl<T: DataPageSlicer> Pushable for VarcharSliceSpillSink<'_, T> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.buffers.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        self.buffers.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        let value = self.slicer.next();
        let offset = self.buffers.data_vec.len();
        self.buffers.data_vec.extend_from_slice(value)?;
        // Write (header as u32, SPILL_MARKER as u32, offset as u64).
        // The header uses the VARCHAR-compatible format: (len << 4) | flags.
        // The spill marker (bit 31 of bytes 4-7) distinguishes spill entries
        // (offsets needing fixup) from non-spill entries (absolute pointers).
        let len = value.len();
        if len >= (1 << 28) {
            return Err(fmt_err!(
                Layout,
                "varchar_slice spill value length {} exceeds 28-bit header capacity",
                len
            ));
        }
        let len = len as u32;
        let header: u32 = (len << 4) | if self.ascii || len == 0 { 3 } else { 1 };
        let combined = (SPILL_MARKER as u64) << 32 | (header as u64);
        self.buffers
            .aux_vec
            .extend_from_slice(&combined.to_le_bytes())?;
        self.buffers
            .aux_vec
            .extend_from_slice(&(offset as u64).to_le_bytes())?;
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
        append_varchar_slice_null(&mut self.buffers.aux_vec)
    }

    #[inline]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        append_varchar_slice_nulls(&mut self.buffers.aux_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count);
        Ok(())
    }

    fn result(&self) -> ParquetResult<()> {
        self.slicer.result()
    }
}

impl<'a, T: DataPageSlicer> VarcharSliceSpillSink<'a, T> {
    pub fn new(slicer: &'a mut T, buffers: &'a mut ColumnChunkBuffers, ascii: bool) -> Self {
        Self { slicer, buffers, ascii }
    }
}

/// Fixup pass: convert offsets stored in spill aux entries to absolute pointers.
///
/// Must be called exactly once after ALL pages in a column chunk are decoded,
/// not per-page. DeltaByteArray spill entries store offsets into data_vec;
/// data_vec may reallocate between pages, so converting offsets to pointers
/// before all pages are decoded would produce stale pointers.
///
/// Only entries with the spill marker (bit 31 of bytes 4-7) are touched.
/// Non-spill entries from other encodings already contain absolute pointers and
/// are left unchanged. This is safe even when a chunk mixes DeltaByteArray
/// pages with Plain/DeltaLength/RleDictionary pages.
pub fn fixup_varchar_slice_spill_pointers(bufs: &mut ColumnChunkBuffers) {
    let data_base = bufs.data_vec.as_ptr() as u64;
    let aux = &mut bufs.aux_vec;
    let entry_count = aux.len() / VARCHAR_AUX_SIZE;
    for i in 0..entry_count {
        let entry_offset = i * VARCHAR_AUX_SIZE;
        // Check spill marker in bytes 4-7
        let marker = u32::from_le_bytes([
            aux[entry_offset + 4],
            aux[entry_offset + 5],
            aux[entry_offset + 6],
            aux[entry_offset + 7],
        ]);
        if marker & SPILL_MARKER == 0 {
            // Not a spill entry: either NULL or a non-spill entry with an
            // absolute pointer. Skip.
            continue;
        }
        // Clear bytes 4-7 entirely. The ASCII flag is now in the header (bytes 0-3),
        // so we zero out the reserved field.
        aux[entry_offset + 4..entry_offset + 8].copy_from_slice(&0u32.to_le_bytes());
        // Read offset from bytes 8..16
        let offset = u64::from_le_bytes([
            aux[entry_offset + 8],
            aux[entry_offset + 9],
            aux[entry_offset + 10],
            aux[entry_offset + 11],
            aux[entry_offset + 12],
            aux[entry_offset + 13],
            aux[entry_offset + 14],
            aux[entry_offset + 15],
        ]);
        // Replace with pointer = data_base + offset
        let ptr = data_base + offset;
        aux[entry_offset + 8..entry_offset + 16].copy_from_slice(&ptr.to_le_bytes());
    }
}
