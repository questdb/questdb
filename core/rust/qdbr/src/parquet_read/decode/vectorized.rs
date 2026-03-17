//! Vectorized aggregation and filter pushdown using `PageDecoder<Sink>`.
//!
//! Uses `PrimitiveSink<U>` — a trait for sinks that accept typed primitive
//! values — so the same decoders that write into `ColumnChunkBuffers` can also
//! push directly into aggregation accumulators or filter evaluators.

use crate::parquet::error::ParquetResult;
use std::ptr;

// ---------------------------------------------------------------------------
// PrimitiveSink trait
// ---------------------------------------------------------------------------

/// Newtype wrapper to use a `PrimitiveSink` as a `Pushable` sink type.
/// Avoids coherence conflicts with `Pushable<ColumnChunkBuffers>` impls by
/// being a distinct type from `ColumnChunkBuffers`.
pub struct PrimitiveSinkWrapper<S>(pub S);

/// A sink that accepts typed primitive values during page decoding.
///
/// Implemented by `ColumnChunkWriteSink` for materialization and by
/// aggregation/filter sinks for pushdown. Decoders implement
/// `Pushable<S: PrimitiveSink<U>>` so the same decoding logic handles both
/// cases — monomorphization gives native performance for each.
pub trait PrimitiveSink<U: Copy> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()>;
    fn write_value(&mut self, value: U);
    fn write_values_repeat(&mut self, value: U, count: usize);
    fn write_null(&mut self);
    fn write_nulls(&mut self, count: usize);
    /// Bulk write from a raw pointer. Default impl calls `write_value` in a loop.
    /// `ColumnChunkWriteSink` overrides with `memcpy`.
    ///
    /// # Safety
    /// `src` must point to `count` readable elements of type `U`.
    unsafe fn write_values_raw(&mut self, src: *const U, count: usize) {
        for i in 0..count {
            self.write_value(src.add(i).read_unaligned());
        }
    }
}

// ---------------------------------------------------------------------------
// ColumnChunkWriteSink: materialization into ColumnChunkBuffers
// ---------------------------------------------------------------------------

use crate::parquet_read::ColumnChunkBuffers;

/// Adapter that makes `ColumnChunkBuffers` act as a `PrimitiveSink<U>`.
/// Owns write_offset + null_value so decoders don't need to.
pub struct ColumnChunkWriteSink<'a, U: Copy> {
    bufs: &'a mut ColumnChunkBuffers,
    write_offset: usize,
    null_value: U,
}

impl<'a, U: Copy> ColumnChunkWriteSink<'a, U> {
    pub fn new(bufs: &'a mut ColumnChunkBuffers, null_value: U) -> Self {
        let write_offset = bufs.data_vec.len() / std::mem::size_of::<U>();
        Self { bufs, write_offset, null_value }
    }
}

impl<U: Copy + 'static> PrimitiveSink<U> for ColumnChunkWriteSink<'_, U> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let needed = (self.write_offset + count) * std::mem::size_of::<U>();
        if self.bufs.data_vec.len() < needed {
            let additional = needed - self.bufs.data_vec.len();
            self.bufs.data_vec.reserve(additional)?;
            unsafe {
                self.bufs.data_vec.set_len(needed);
            }
        }
        Ok(())
    }

    #[inline]
    fn write_value(&mut self, value: U) {
        let out_ptr: *mut U = self.bufs.data_vec.as_mut_ptr().cast();
        unsafe {
            *out_ptr.add(self.write_offset) = value;
        }
        self.write_offset += 1;
    }

    #[inline]
    fn write_values_repeat(&mut self, value: U, count: usize) {
        let out_ptr: *mut U = self.bufs.data_vec.as_mut_ptr().cast();
        let out = unsafe { out_ptr.add(self.write_offset) };
        for i in 0..count {
            unsafe {
                *out.add(i) = value;
            }
        }
        self.write_offset += count;
    }

    #[inline]
    fn write_null(&mut self) {
        self.write_value(self.null_value);
    }

    #[inline]
    fn write_nulls(&mut self, count: usize) {
        self.write_values_repeat(self.null_value, count);
    }

    #[inline]
    unsafe fn write_values_raw(&mut self, src: *const U, count: usize) {
        let out_ptr: *mut U = self.bufs.data_vec.as_mut_ptr().cast();
        ptr::copy_nonoverlapping(
            src as *const u8,
            out_ptr.add(self.write_offset) as *mut u8,
            count * std::mem::size_of::<U>(),
        );
        self.write_offset += count;
    }
}

// ---------------------------------------------------------------------------
// Aggregation sinks
// ---------------------------------------------------------------------------

/// SUM(double) accumulator implementing `PrimitiveSink<f64>`.
pub struct SumDoubleSink {
    pub sum: f64,
    pub count: usize,
    pub null_count: usize,
}

impl SumDoubleSink {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0, null_count: 0 }
    }
}

impl PrimitiveSink<f64> for SumDoubleSink {
    fn reserve(&mut self, _count: usize) -> ParquetResult<()> {
        Ok(())
    }

    #[inline]
    fn write_value(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
    }

    #[inline]
    fn write_values_repeat(&mut self, value: f64, count: usize) {
        self.sum += value * count as f64;
        self.count += count;
    }

    #[inline]
    fn write_null(&mut self) {
        self.null_count += 1;
    }

    #[inline]
    fn write_nulls(&mut self, count: usize) {
        self.null_count += count;
    }
}

// ---------------------------------------------------------------------------
// Filter pushdown sinks
// ---------------------------------------------------------------------------

/// Filter sink that collects row indices where i64 values satisfy a predicate.
pub struct FilterSink {
    pub matching_rows: Vec<usize>,
    pub current_row: usize,
}

impl FilterSink {
    pub fn new() -> Self {
        Self { matching_rows: Vec::new(), current_row: 0 }
    }
}

/// Filter for i64 > threshold.
pub struct GtInt64FilterSink {
    pub inner: FilterSink,
    pub threshold: i64,
}

impl GtInt64FilterSink {
    pub fn new(threshold: i64) -> Self {
        Self { inner: FilterSink::new(), threshold }
    }
}

impl PrimitiveSink<i64> for GtInt64FilterSink {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        self.inner.matching_rows.reserve(count);
        Ok(())
    }

    #[inline]
    fn write_value(&mut self, value: i64) {
        if value > self.threshold {
            self.inner.matching_rows.push(self.inner.current_row);
        }
        self.inner.current_row += 1;
    }

    #[inline]
    fn write_values_repeat(&mut self, value: i64, count: usize) {
        if value > self.threshold {
            for i in 0..count {
                self.inner.matching_rows.push(self.inner.current_row + i);
            }
        }
        self.inner.current_row += count;
    }

    #[inline]
    fn write_null(&mut self) {
        self.inner.current_row += 1;
    }

    #[inline]
    fn write_nulls(&mut self, count: usize) {
        self.inner.current_row += count;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_read::decode::def_level_iter::DefLevelBatchIter;
    use crate::parquet_read::decode::{BoundPageDecoder, PageDecoder};
    use crate::parquet_read::decoders::PlainPrimitiveDecoder;
    use crate::parquet_read::page::DataPage;
    use parquet2::encoding::hybrid_rle::encode_u32;
    use parquet2::encoding::Encoding;
    use parquet2::metadata::Descriptor;
    use parquet2::page::{DataPageHeader, DataPageHeaderV1};
    use parquet2::read::levels::get_bit_width;
    use parquet2::schema::types::{FieldInfo, PhysicalType, PrimitiveType};
    use parquet2::schema::Repetition;

    fn make_descriptor(physical_type: PhysicalType) -> Descriptor {
        Descriptor {
            primitive_type: PrimitiveType {
                field_info: FieldInfo {
                    name: "test_col".to_string(),
                    repetition: Repetition::Optional,
                    id: None,
                },
                logical_type: None,
                converted_type: None,
                physical_type,
            },
            max_def_level: 1,
            max_rep_level: 0,
        }
    }

    fn encode_def_levels(bitmap: &[bool]) -> Vec<u8> {
        let def_levels: Vec<u32> = bitmap.iter().map(|&v| if v { 1 } else { 0 }).collect();
        let len = def_levels.len();
        let bit_width = get_bit_width(1);
        let mut encoded = Vec::new();
        encode_u32(&mut encoded, def_levels.into_iter(), len, bit_width).unwrap();
        let mut buf = Vec::new();
        buf.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        buf.extend_from_slice(&encoded);
        buf
    }

    fn build_page_buffer(values_bytes: &[u8], null_bitmap: &[bool]) -> Vec<u8> {
        let mut buf = encode_def_levels(null_bitmap);
        buf.extend_from_slice(values_bytes);
        buf
    }

    fn make_header(num_values: usize) -> DataPageHeader {
        DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_values as i32,
            encoding: Encoding::Plain.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics: None,
        })
    }

    // -----------------------------------------------------------------------
    // Same decoder, different sinks — the whole point
    // -----------------------------------------------------------------------

    #[test]
    fn test_plain_f64_decoder_sum_sink() {
        // 100 f64 values, no nulls. Use PlainPrimitiveDecoder with SumDoubleSink.
        let values: Vec<f64> = (0..100).map(|i| i as f64 * 1.5).collect();
        let values_bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let all_non_null = vec![true; 100];
        let page_buf = build_page_buffer(&values_bytes, &all_non_null);
        let header = make_header(100);
        let descriptor = make_descriptor(PhysicalType::Double);
        let page = DataPage {
            buffer: &page_buf,
            header: &header,
            descriptor: &descriptor,
        };

        // Create the SAME PlainPrimitiveDecoder that decode.rs uses for f64,
        // but push into SumDoubleSink instead of ColumnChunkBuffers.
        let mut sink = SumDoubleSink::new();
        let pushable =
            PlainPrimitiveDecoder::<f64, f64>::new_for_sink(&values_bytes, &mut sink, f64::NAN);
        let mut wrapper = PrimitiveSinkWrapper(sink);

        let iter = DefLevelBatchIter::new(&page, 0, 100).unwrap();
        let mut decoder = BoundPageDecoder::new(iter, pushable);
        decoder.decode_all(&mut wrapper).unwrap();

        let sink = &wrapper.0;
        let expected_sum: f64 = values.iter().sum();
        assert_eq!(sink.count, 100);
        assert_eq!(sink.null_count, 0);
        assert!((sink.sum - expected_sum).abs() < 1e-10);
    }

    #[test]
    fn test_plain_f64_decoder_sum_sink_with_nulls() {
        // Values: 1.0, NULL, 3.0, NULL, 5.0 (3 non-null out of 5)
        let non_null_values: Vec<f64> = vec![1.0, 3.0, 5.0];
        let null_bitmap = vec![true, false, true, false, true];
        let values_bytes: Vec<u8> = non_null_values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let page_buf = build_page_buffer(&values_bytes, &null_bitmap);
        let header = make_header(5);
        let descriptor = make_descriptor(PhysicalType::Double);
        let page = DataPage {
            buffer: &page_buf,
            header: &header,
            descriptor: &descriptor,
        };

        let mut sink = SumDoubleSink::new();
        let pushable =
            PlainPrimitiveDecoder::<f64, f64>::new_for_sink(&values_bytes, &mut sink, f64::NAN);
        let mut wrapper = PrimitiveSinkWrapper(sink);

        let iter = DefLevelBatchIter::new(&page, 0, 5).unwrap();
        let mut decoder = BoundPageDecoder::new(iter, pushable);
        decoder.decode_all(&mut wrapper).unwrap();

        let sink = &wrapper.0;
        assert_eq!(sink.count, 3);
        assert_eq!(sink.null_count, 2);
        assert!((sink.sum - 9.0).abs() < 1e-10);
    }

    #[test]
    fn test_plain_i64_decoder_filter_sink() {
        // Values 0..10, filter > 5
        let values: Vec<i64> = (0..10).collect();
        let values_bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let all_non_null = vec![true; 10];
        let page_buf = build_page_buffer(&values_bytes, &all_non_null);
        let header = make_header(10);
        let descriptor = make_descriptor(PhysicalType::Int64);
        let page = DataPage {
            buffer: &page_buf,
            header: &header,
            descriptor: &descriptor,
        };

        let mut sink = GtInt64FilterSink::new(5);
        let pushable =
            PlainPrimitiveDecoder::<i64, i64>::new_for_sink(&values_bytes, &mut sink, i64::MIN);
        let mut wrapper = PrimitiveSinkWrapper(sink);

        let iter = DefLevelBatchIter::new(&page, 0, 10).unwrap();
        let mut decoder = BoundPageDecoder::new(iter, pushable);
        decoder.decode_all(&mut wrapper).unwrap();

        // Values > 5: 6,7,8,9 -> rows 6,7,8,9
        assert_eq!(wrapper.0.inner.matching_rows, vec![6, 7, 8, 9]);
    }

    #[test]
    fn test_plain_i64_decoder_filter_with_nulls() {
        // Non-null values: 100, 20, 80. bitmap: [true, false, true, true, false]
        // Logical: 100, NULL, 20, 80, NULL.  Filter: > 50
        let non_null_values: Vec<i64> = vec![100, 20, 80];
        let null_bitmap = vec![true, false, true, true, false];
        let values_bytes: Vec<u8> = non_null_values
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let page_buf = build_page_buffer(&values_bytes, &null_bitmap);
        let header = make_header(5);
        let descriptor = make_descriptor(PhysicalType::Int64);
        let page = DataPage {
            buffer: &page_buf,
            header: &header,
            descriptor: &descriptor,
        };

        let mut sink = GtInt64FilterSink::new(50);
        let pushable =
            PlainPrimitiveDecoder::<i64, i64>::new_for_sink(&values_bytes, &mut sink, i64::MIN);
        let mut wrapper = PrimitiveSinkWrapper(sink);

        let iter = DefLevelBatchIter::new(&page, 0, 5).unwrap();
        let mut decoder = BoundPageDecoder::new(iter, pushable);
        decoder.decode_all(&mut wrapper).unwrap();

        // row 0: 100 > 50, row 3: 80 > 50
        assert_eq!(wrapper.0.inner.matching_rows, vec![0, 3]);
    }

    #[test]
    fn test_sum_batched() {
        let values: Vec<f64> = (0..1000).map(|i| i as f64).collect();
        let values_bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        let all_non_null = vec![true; 1000];
        let page_buf = build_page_buffer(&values_bytes, &all_non_null);
        let header = make_header(1000);
        let descriptor = make_descriptor(PhysicalType::Double);
        let page = DataPage {
            buffer: &page_buf,
            header: &header,
            descriptor: &descriptor,
        };

        let mut sink = SumDoubleSink::new();
        let pushable =
            PlainPrimitiveDecoder::<f64, f64>::new_for_sink(&values_bytes, &mut sink, f64::NAN);
        let mut wrapper = PrimitiveSinkWrapper(sink);

        let iter = DefLevelBatchIter::new(&page, 0, 1000).unwrap();
        let mut decoder = BoundPageDecoder::new(iter, pushable);

        while !decoder.is_exhausted() {
            decoder.decode_batch(&mut wrapper, 64).unwrap();
        }

        let sink = &wrapper.0;
        let expected_sum: f64 = values.iter().sum();
        assert_eq!(sink.count, 1000);
        assert!((sink.sum - expected_sum).abs() < 1e-10);
    }
}
