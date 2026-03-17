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

pub struct VarcharColumnSink<T: DataPageSlicer> {
    slicer: T,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for VarcharColumnSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        sink.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        sink.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_varchar(
            &mut sink.aux_vec,
            &mut sink.data_vec,
            self.slicer.next()?,
        )
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        // TODO: optimize
        for _ in 0..count {
            append_varchar(
                &mut sink.aux_vec,
                &mut sink.data_vec,
                self.slicer.next()?,
            )?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_varchar_null(&mut sink.aux_vec, &sink.data_vec)
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        append_varchar_nulls(&mut sink.aux_vec, &sink.data_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> VarcharColumnSink<T> {
    pub fn new(slicer: T) -> Self {
        Self { slicer }
    }
}

pub struct StringColumnSink<T: DataPageSlicer> {
    slicer: T,
    error: ParquetResult<()>,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for StringColumnSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        if count > 0 {
            sink.aux_vec.reserve((count + 1) * STRING_AUX_SIZE)?;
            if sink.aux_vec.is_empty() {
                sink.aux_vec
                    .extend_from_slice(0u64.to_le_bytes().as_ref())?;
            }
        }
        sink.data_vec
            .reserve(2 * self.slicer.data_size() + size_of::<u32>() * count)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let utf8 = self.slicer.next()?;
        let utf8_str = std::str::from_utf8(utf8);

        match utf8_str {
            Ok(utf8_str) => {
                let pos = sink.data_vec.len();
                sink.data_vec.resize(sink.data_vec.len() + 4, 0u8)?;
                for c in utf8_str.encode_utf16() {
                    sink.data_vec.extend_from_slice(&c.to_le_bytes())?;
                }

                // Set length in utf16 characters
                let len = (sink.data_vec.len() - pos - 4) as u32 / 2;
                sink.data_vec[pos..pos + 4].copy_from_slice(len.to_le_bytes().as_ref());

                // set aux pointer
                sink.aux_vec
                    .extend_from_slice(sink.data_vec.len().to_le_bytes().as_ref())?;
            }
            Err(utf8_str_err) => {
                self.error = Err(ParquetErrorReason::Utf8Decode(utf8_str_err).into_err());
                self.push_null(sink)?;
            }
        }
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        // TODO: optimize
        for _ in 0..count {
            self.push(sink)?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        sink.data_vec
            .extend_from_slice((-1i32).to_le_bytes().as_ref())?;
        // set aux pointer
        sink.aux_vec
            .extend_from_slice(sink.data_vec.len().to_le_bytes().as_ref())?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(sink),
            2 => {
                self.push_null(sink)?;
                self.push_null(sink)
            }
            3 => {
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)
            }
            4 => {
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)
            }
            _ => {
                let base = sink.data_vec.len();
                sink.data_vec.reserve(count * size_of::<i32>())?;

                // Fill data_vec with 0xff bytes (-1i32 per null)
                unsafe {
                    ptr::write_bytes(
                        sink.data_vec.as_mut_ptr().add(base),
                        0xff,
                        count * size_of::<i32>(),
                    );
                    sink.data_vec.set_len(base + count * size_of::<i32>());
                }
                write_offset_sequence(
                    &mut sink.aux_vec,
                    base + size_of::<i32>(),
                    size_of::<i32>(),
                    count,
                )
            }
        }
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> StringColumnSink<T> {
    pub fn new(slicer: T) -> Self {
        Self { slicer, error: Ok(()) }
    }
}

pub struct BinaryColumnSink<T: DataPageSlicer> {
    slicer: T,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for BinaryColumnSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        if count > 0 {
            sink.aux_vec.reserve((count + 1) * STRING_AUX_SIZE)?;
            if sink.aux_vec.is_empty() {
                sink.aux_vec
                    .extend_from_slice(0u64.to_le_bytes().as_ref())?;
            }
        }
        sink.data_vec
            .reserve(self.slicer.data_size() + size_of::<u64>() * count)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let slice = self.slicer.next()?;
        sink.data_vec
            .extend_from_slice(slice.len().to_le_bytes().as_ref())?;
        sink.data_vec.extend_from_slice(slice)?;

        // set aux pointer
        sink.aux_vec
            .extend_from_slice(sink.data_vec.len().to_le_bytes().as_ref())?;
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.push(sink)?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        sink.data_vec
            .extend_from_slice((-1i64).to_le_bytes().as_ref())?;
        // set aux pointer
        sink.aux_vec
            .extend_from_slice(sink.data_vec.len().to_le_bytes().as_ref())?;
        Ok(())
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        match count {
            0 => Ok(()),
            1 => self.push_null(sink),
            2 => {
                self.push_null(sink)?;
                self.push_null(sink)
            }
            3 => {
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)
            }
            4 => {
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)?;
                self.push_null(sink)
            }
            _ => {
                let base = sink.data_vec.len();
                sink.data_vec.reserve(count * size_of::<i64>())?;

                // Fill data_vec with 0xff bytes (-1i64 per null)
                unsafe {
                    ptr::write_bytes(
                        sink.data_vec.as_mut_ptr().add(base),
                        0xff,
                        count * size_of::<i64>(),
                    );
                    sink.data_vec.set_len(base + count * size_of::<i64>());
                }
                write_offset_sequence(
                    &mut sink.aux_vec,
                    base + size_of::<i64>(),
                    size_of::<i64>(),
                    count,
                )
            }
        }
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> BinaryColumnSink<T> {
    pub fn new(slicer: T) -> Self {
        Self { slicer }
    }
}

pub struct RawArrayColumnSink<T: DataPageSlicer> {
    slicer: T,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for RawArrayColumnSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        sink.aux_vec.reserve(count * ARRAY_AUX_SIZE)?;
        sink.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_raw_array(
            &mut sink.aux_vec,
            &mut sink.data_vec,
            self.slicer.next()?,
        )
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            append_raw_array(
                &mut sink.aux_vec,
                &mut sink.data_vec,
                self.slicer.next()?,
            )?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_array_null(&mut sink.aux_vec, &sink.data_vec)
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        append_array_nulls(&mut sink.aux_vec, &sink.data_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> RawArrayColumnSink<T> {
    pub fn new(slicer: T) -> Self {
        Self { slicer }
    }
}

pub struct VarcharSliceColumnSink<T: DataPageSlicer> {
    slicer: T,
    ascii: bool,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for VarcharSliceColumnSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        sink.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let value = self.slicer.next()?;
        append_varchar_slice(&mut sink.aux_vec, value, self.ascii)
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            let value = self.slicer.next()?;
            append_varchar_slice(&mut sink.aux_vec, value, self.ascii)?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_varchar_slice_null(&mut sink.aux_vec)
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        append_varchar_slice_nulls(&mut sink.aux_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> VarcharSliceColumnSink<T> {
    pub fn new(slicer: T, ascii: bool) -> Self {
        Self { slicer, ascii }
    }
}

/// VarcharSlice sink for DeltaByteArray encoding.
/// Strings are reconstructed incrementally - each slicer.next() overwrites the previous value.
/// We copy each reconstructed string to data_vec, storing offsets temporarily in aux,
/// then do a fixup pass to convert offsets to pointers.
pub struct VarcharSliceSpillSink<T: DataPageSlicer> {
    slicer: T,
    ascii: bool,
}

impl<T: DataPageSlicer> Pushable<ColumnChunkBuffers> for VarcharSliceSpillSink<T> {
    fn reserve(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        sink.aux_vec.reserve(count * VARCHAR_AUX_SIZE)?;
        sink.data_vec.reserve(self.slicer.data_size())?;
        Ok(())
    }

    #[inline]
    fn push(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        let value = self.slicer.next()?;
        let len = value.len();
        if len == 0 {
            // Empty string: no data to spill, no fixup needed.
            // Write clean aux entry without SPILL_MARKER; pointer is irrelevant
            // since Java never dereferences it when length is 0.
            let header: u64 = 3; // (0 << 4) | non-null | ascii
            sink.aux_vec.extend_from_slice(&header.to_le_bytes())?;
            sink.aux_vec.extend_from_slice(&0u64.to_le_bytes())?;
            return Ok(());
        }
        if len >= (1 << 28) {
            return Err(fmt_err!(
                Layout,
                "varchar_slice spill value length {} exceeds 28-bit header capacity",
                len
            ));
        }
        // Write (header as u32, SPILL_MARKER as u32, offset as u64).
        // The header uses the VARCHAR-compatible format: (len << 4) | flags.
        // The spill marker (bit 31 of bytes 4-7) distinguishes spill entries
        // (offsets needing fixup) from non-spill entries (absolute pointers).
        let offset = sink.data_vec.len();
        sink.data_vec.extend_from_slice(value)?;
        let len = len as u32;
        let header: u32 = (len << 4) | if self.ascii { 3 } else { 1 };
        let combined = (SPILL_MARKER as u64) << 32 | (header as u64);
        sink.aux_vec.extend_from_slice(&combined.to_le_bytes())?;
        sink.aux_vec
            .extend_from_slice(&(offset as u64).to_le_bytes())?;
        Ok(())
    }

    #[inline]
    fn push_slice(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        for _ in 0..count {
            self.push(sink)?;
        }
        Ok(())
    }

    #[inline]
    fn push_null(&mut self, sink: &mut ColumnChunkBuffers) -> ParquetResult<()> {
        append_varchar_slice_null(&mut sink.aux_vec)
    }

    #[inline]
    fn push_nulls(&mut self, sink: &mut ColumnChunkBuffers, count: usize) -> ParquetResult<()> {
        append_varchar_slice_nulls(&mut sink.aux_vec, count)
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        self.slicer.skip(count)
    }
}

impl<T: DataPageSlicer> VarcharSliceSpillSink<T> {
    pub fn new(slicer: T, ascii: bool) -> Self {
        Self { slicer, ascii }
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
