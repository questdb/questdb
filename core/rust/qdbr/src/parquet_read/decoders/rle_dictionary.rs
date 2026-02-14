//! Decoder for Parquet `RLE_DICTIONARY` encoded primitive values.
//!
//! The decoder consumes a hybrid-RLE index stream, resolves indices through a
//! dictionary reader, and writes the final values to `ColumnChunkBuffers`.

use crate::parquet::error::{fmt_err, ParquetError, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::dictionary::PrimitiveDictDecoder;
use crate::parquet_read::decoders::{RepeatN, RleIterator};
use crate::parquet_read::ColumnChunkBuffers;

use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

/// Maintains current decoded run and transitions to the next run on demand.
struct Slicer<'a> {
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
    error: Option<ParquetError>,
}

impl<'a> Slicer<'a> {
    fn new(decoder: Option<Decoder<'a>>, iter: RleIterator<'a>) -> Self {
        Self { decoder, data: iter, error: None }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        let iter_err = || Err(fmt_err!(Layout, "Unexpected end of rle iterator"));

        let Some(ref mut decoder) = self.decoder else {
            return iter_err();
        };

        let Some(run) = decoder.next() else {
            return iter_err();
        };

        let num_bits = decoder.num_bits();
        match run? {
            HybridEncoded::Bitpacked(values) => {
                if num_bits == 8 {
                    // Fast path: each byte IS a dict index, skip the unpack step.
                    self.data = RleIterator::ByteIndices { data: values, pos: 0 };
                } else {
                    let count = values.len() * 8 / num_bits;
                    let inner_decoder =
                        bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                    self.data = RleIterator::Bitpacked(inner_decoder)
                }
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                let iterator = RepeatN::new(value, repeat);
                self.data = RleIterator::Rle(iterator);
            }
        }
        Ok(())
    }

    fn skip(&mut self, count: usize) {
        if self.error.is_some() {
            return;
        }

        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                match self.decode() {
                    Ok(()) => {}
                    Err(err) => {
                        self.error = Some(err);
                        return;
                    }
                }
            }
        }
    }

    fn result(&self) -> ParquetResult<()> {
        match &self.error {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    }
}

pub struct RleDictionaryDecoder<'a, U, T: PrimitiveDictDecoder<U>>
where
    U: Copy + 'static,
{
    dict: T,
    inner: Slicer<'a>,
    buffers: &'a mut ColumnChunkBuffers,
    buffers_ptr: *mut U,
    buffers_offset: usize,
    null_value: U,
    _phantom: std::marker::PhantomData<U>,
}

impl<U, T: PrimitiveDictDecoder<U>> Pushable for RleDictionaryDecoder<'_, U, T>
where
    U: Copy + 'static,
{
    fn push(&mut self) -> ParquetResult<()> {
        if self.inner.error.is_some() {
            return Ok(());
        }

        if let Some(idx) = self.inner.data.next() {
            if idx < self.dict.len() {
                unsafe {
                    *self.buffers_ptr.add(self.buffers_offset) = self.dict.get_dict_value(idx);
                    self.buffers_offset += 1;
                }
                Ok(())
            } else {
                self.inner.error = Some(fmt_err!(
                    Layout,
                    "index {} is out of dict bounds {}",
                    idx,
                    self.dict.len()
                ));
                Ok(())
            }
        } else {
            // This recursive is safe, it cannot go deeper than 1 level down.
            // After a successful call to `self.inner.decode()` there will be a value in `self.inner.data.next()`
            match self.decode() {
                Ok(()) => self.push(),
                Err(err) => {
                    self.inner.error = Some(err);
                    return Ok(());
                }
            }
        }
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
        let mut remaining = count;
        loop {
            if remaining == 0 || self.inner.error.is_some() {
                return Ok(());
            }

            let consumed = match &mut self.inner.data {
                RleIterator::Rle(repeat) => {
                    let n = remaining.min(repeat.remaining);
                    if n == 0 {
                        0
                    } else {
                        if repeat.value >= self.dict.len() {
                            self.inner.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                repeat.value,
                                self.dict.len()
                            ));
                            return Ok(());
                        }
                        let value = self.dict.get_dict_value(repeat.value);
                        let out = unsafe { self.buffers_ptr.add(self.buffers_offset) };
                        for i in 0..n {
                            unsafe {
                                *out.add(i) = value;
                            }
                        }
                        self.buffers_offset += n;
                        repeat.remaining -= n;
                        n
                    }
                }
                RleIterator::Bitpacked(bp) => {
                    let dict_len = self.dict.len();
                    let out_base = self.buffers_ptr;
                    let mut offset = self.buffers_offset;
                    let mut consumed = 0usize;
                    let unpack_ptr = bp.unpacked.as_ref().as_ptr();

                    while consumed < remaining && bp.remaining > 0 {
                        let avail_in_pack = (32 - bp.current_pack_index).min(bp.remaining);
                        let n = avail_in_pack.min(remaining - consumed);
                        let start = bp.current_pack_index;

                        for i in 0..n {
                            let idx = unsafe { *unpack_ptr.add(start + i) };
                            if idx >= dict_len {
                                self.buffers_offset = offset;
                                self.inner.error = Some(fmt_err!(
                                    Layout,
                                    "index {} is out of dict bounds {}",
                                    idx,
                                    dict_len
                                ));
                                return Ok(());
                            }
                            unsafe {
                                *out_base.add(offset) = self.dict.get_dict_value(idx);
                            }
                            offset += 1;
                        }

                        bp.current_pack_index += n;
                        bp.remaining -= n;
                        consumed += n;

                        if bp.current_pack_index == 32 {
                            bp.decode_next_pack();
                        }
                    }

                    self.buffers_offset = offset;
                    consumed
                }
                RleIterator::ByteIndices { data, pos } => {
                    let dict_len = self.dict.len();
                    let out_base = self.buffers_ptr;
                    let mut offset = self.buffers_offset;
                    let avail = data.len() - *pos;
                    let n = remaining.min(avail);
                    let bytes = &data[*pos..];

                    for i in 0..n {
                        let idx = unsafe { *bytes.get_unchecked(i) } as u32;
                        if idx >= dict_len {
                            self.buffers_offset = offset;
                            self.inner.error = Some(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                idx,
                                dict_len
                            ));
                            return Ok(());
                        }
                        unsafe {
                            *out_base.add(offset) = self.dict.get_dict_value(idx);
                        }
                        offset += 1;
                    }

                    *pos += n;
                    self.buffers_offset = offset;
                    n
                }
            };

            remaining -= consumed;
            if remaining > 0 {
                match self.decode() {
                    Ok(()) => {}
                    Err(err) => {
                        self.inner.error = Some(err);
                        return Ok(());
                    }
                }
            }
        }
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
        self.inner.skip(count);
    }

    fn result(&self) -> ParquetResult<()> {
        self.inner.result()
    }
}

impl<'a, T: PrimitiveDictDecoder<U>, U> RleDictionaryDecoder<'a, U, T>
where
    U: Copy + 'static,
{
    pub fn try_new(
        mut buffer: &'a [u8],
        dict: T,
        row_count: usize,
        null_value: U,
        buffers: &'a mut ColumnChunkBuffers,
    ) -> ParquetResult<Self> {
        let num_bits = buffer[0];
        if num_bits > 0 {
            buffer = &buffer[1..];
            let decoder = Decoder::new(buffer, num_bits as usize);
            let mut res = Self {
                dict,
                inner: Slicer::new(Some(decoder), RleIterator::Rle(RepeatN::new(0, 0))),
                _phantom: std::marker::PhantomData,
                buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
                buffers,
                buffers_offset: 0,
                null_value,
            };
            res.decode()?;
            Ok(res)
        } else {
            Ok(Self {
                dict,
                inner: Slicer::new(None, RleIterator::Rle(RepeatN::new(0, row_count))),
                _phantom: std::marker::PhantomData,
                buffers_ptr: buffers.data_vec.as_mut_ptr().cast(),
                buffers,
                buffers_offset: 0,
                null_value,
            })
        }
    }

    fn decode(&mut self) -> ParquetResult<()> {
        self.inner.decode()
    }
}

#[cfg(test)]
mod tests {
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::decoders::{
        rle_dictionary::PrimitiveDictDecoder, RleDictionaryDecoder,
    };
    use crate::parquet_read::ColumnChunkBuffers;
    use std::ptr;

    fn encode_rle_data(values: &[u32], num_bits: u8) -> Vec<u8> {
        let mut buffer = vec![num_bits];
        parquet2::encoding::hybrid_rle::encode_u32(
            &mut buffer,
            values.iter().copied(),
            values.len(),
            num_bits as u32,
        )
        .unwrap();
        buffer
    }

    struct TestPrimitiveDictDecoder {
        values: Vec<i32>,
    }

    impl TestPrimitiveDictDecoder {
        fn new(values: Vec<i32>) -> Self {
            Self { values }
        }
    }

    impl PrimitiveDictDecoder<i32> for TestPrimitiveDictDecoder {
        #[inline]
        fn get_dict_value(&self, index: u32) -> i32 {
            self.values[index as usize]
        }

        #[inline]
        fn len(&self) -> u32 {
            self.values.len() as u32
        }
    }

    const I32_NULL: i32 = i32::MIN;

    fn create_test_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
        ColumnChunkBuffers {
            data_size: 0,
            data_ptr: ptr::null_mut(),
            data_vec: AcVec::new_in(allocator.clone()),
            aux_size: 0,
            aux_ptr: ptr::null_mut(),
            aux_vec: AcVec::new_in(allocator.clone()),
        }
    }

    fn read_i32_results(buffers: &ColumnChunkBuffers) -> Vec<i32> {
        buffers
            .data_vec
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    fn read_i32_results_n(buffers: &ColumnChunkBuffers, count: usize) -> Vec<i32> {
        buffers.data_vec[..count * 4]
            .chunks(4)
            .map(|c| i32::from_le_bytes(c.try_into().unwrap()))
            .collect()
    }

    // --- Basic push ---

    #[test]
    fn test_rle_dict_decoder_push_single_values() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100, 200, 300]);
        let encoded = encode_rle_data(&[0, 1, 2, 1, 0], 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(5).unwrap();

        for _ in 0..5 {
            decoder.push().unwrap();
        }
        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![100, 200, 300, 200, 100]);
    }

    #[test]
    fn test_rle_dict_decoder_push_from_single_entry_dict() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![42]);
        // zero bit width: all indices are 0
        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 10, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(10).unwrap();

        for _ in 0..10 {
            decoder.push().unwrap();
        }
        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![42; 10]);
    }

    // --- push_slice ---

    #[test]
    fn test_rle_dict_decoder_push_slice_basic() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30, 40, 50]);
        let values: Vec<u32> = vec![0, 1, 2, 3, 4, 4, 3, 2, 1, 0];
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 10, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(10).unwrap();
        decoder.push_slice(10).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(
            read_i32_results(&buffers),
            vec![10, 20, 30, 40, 50, 50, 40, 30, 20, 10]
        );
    }

    #[test]
    fn test_rle_dict_decoder_push_slice_repeated_values() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![111, 222]);
        // All same value → RLE encoding will likely use RLE runs
        let values: Vec<u32> = vec![0; 50];
        let encoded = encode_rle_data(&values, 1);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 50, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(50).unwrap();
        decoder.push_slice(50).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![111; 50]);
    }

    #[test]
    fn test_rle_dict_decoder_push_slice_with_8bit_indices() {
        // 8 bits per index triggers the ByteIndices fast path in FastSlicerInner::decode
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict_values: Vec<i32> = (0..200).collect();
        let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
        let indices: Vec<u32> = (0..200).map(|i| i as u32).collect();
        let encoded = encode_rle_data(&indices, 8);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 200, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(200).unwrap();
        decoder.push_slice(200).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), dict_values);
    }

    #[test]
    fn test_rle_dict_decoder_push_slice_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![1, 2, 3]);
        let encoded = encode_rle_data(&[0, 1, 2], 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 3, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(0).unwrap();
        decoder.push_slice(0).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), Vec::<i32>::new());
    }

    #[test]
    fn test_rle_dict_decoder_push_slice_multiple_calls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30, 40, 50]);
        let values: Vec<u32> = vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4];
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 10, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(10).unwrap();

        decoder.push_slice(3).unwrap();
        decoder.push_slice(4).unwrap();
        decoder.push_slice(3).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(
            read_i32_results(&buffers),
            vec![10, 20, 30, 40, 50, 10, 20, 30, 40, 50]
        );
    }

    // --- push_null / push_nulls ---

    #[test]
    fn test_rle_dict_decoder_push_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100]);
        let encoded: Vec<u8> = vec![0]; // zero bit width

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push_null().unwrap();
        decoder.push_null().unwrap();
        decoder.push_null().unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![I32_NULL; 3]);
    }

    #[test]
    fn test_rle_dict_decoder_push_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100]);
        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(7).unwrap();
        decoder.push_nulls(7).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![I32_NULL; 7]);
    }

    #[test]
    fn test_rle_dict_decoder_push_nulls_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100]);
        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(0).unwrap();
        decoder.push_nulls(0).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), Vec::<i32>::new());
    }

    // --- skip ---

    #[test]
    fn test_rle_dict_decoder_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30, 40, 50]);
        let values: Vec<u32> = vec![0, 1, 2, 3, 4];
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(3).unwrap();

        decoder.skip(2);
        decoder.push_slice(3).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![30, 40, 50]);
    }

    #[test]
    fn test_rle_dict_decoder_skip_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30]);
        let values: Vec<u32> = vec![0, 1, 2];
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 3, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(1).unwrap();
        decoder.skip(0);
        decoder.push().unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![10]);
    }

    #[test]
    fn test_rle_dict_decoder_skip_all() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30]);
        let values: Vec<u32> = vec![0, 1, 2, 0, 1];
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.skip(5);

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), Vec::<i32>::new());
    }

    #[test]
    fn test_rle_dict_decoder_skip_large_across_runs() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dict_values: Vec<i32> = (0..16).collect();
        // Generate 200 values, skip to various positions
        let values: Vec<u32> = (0..200).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&values, 4);

        for skip in [0, 1, 31, 32, 33, 63, 64, 65, 99, 100, 150, 199] {
            let mut buffers = create_test_buffers(&allocator);
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let remaining = 200 - skip;
            let to_read = remaining.min(5);

            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 200, I32_NULL, &mut buffers).unwrap();
            decoder.reserve(to_read).unwrap();
            decoder.skip(skip);
            decoder.push_slice(to_read).unwrap();

            assert!(decoder.result().is_ok(), "skip={skip}");
            let expected: Vec<i32> = (skip..skip + to_read).map(|i| (i % 16) as i32).collect();
            assert_eq!(read_i32_results(&buffers), expected, "skip={skip}");
        }
    }

    // --- Mixed operations ---

    #[test]
    fn test_rle_dict_decoder_mixed_push_null_skip() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30, 40, 50, 60, 70, 80]);
        // positions:                                 0   1   2   3   4   5   6   7   8   9  10  11
        let values: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7];
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 16, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(10).unwrap();

        // push_slice(2): positions 0,1 → dict[0]=10, dict[1]=20
        decoder.push_slice(2).unwrap();
        // push_nulls(2): writes [NULL, NULL]
        decoder.push_nulls(2).unwrap();
        // skip(3): positions 2,3,4 → skipped
        decoder.skip(3);
        // push(): position 5 → dict[5]=60
        decoder.push().unwrap();
        // push_null(): writes [NULL]
        decoder.push_null().unwrap();
        // push_slice(3): positions 6,7,8 → dict[6]=70, dict[7]=80, dict[0]=10
        decoder.push_slice(3).unwrap();
        // skip(2): positions 9,10 → skipped
        decoder.skip(2);
        // push(): position 11 → dict[3]=40
        decoder.push().unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(
            read_i32_results_n(&buffers, 10),
            vec![10, 20, I32_NULL, I32_NULL, 60, I32_NULL, 70, 80, 10, 40]
        );
    }

    #[test]
    fn test_rle_dict_decoder_interleaved_push_and_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100, 200, 300, 400]);
        let values: Vec<u32> = vec![0, 1, 2, 3, 0, 1, 2, 3];
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 8, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push().unwrap(); // 100
        decoder.push_slice(2).unwrap(); // 200, 300
        decoder.push().unwrap(); // 400
        decoder.push().unwrap(); // 100
        decoder.push_slice(3).unwrap(); // 200, 300, 400

        assert!(decoder.result().is_ok());
        assert_eq!(
            read_i32_results(&buffers),
            vec![100, 200, 300, 400, 100, 200, 300, 400]
        );
    }

    // --- Zero bit width ---

    #[test]
    fn test_rle_dict_decoder_zero_bit_width_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![999]);
        let encoded: Vec<u8> = vec![0]; // zero bit width

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(100).unwrap();
        decoder.push_slice(100).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![999; 100]);
    }

    #[test]
    fn test_rle_dict_decoder_zero_bit_width_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![42]);
        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 20, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(5).unwrap();
        decoder.skip(15);
        decoder.push_slice(5).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![42; 5]);
    }

    #[test]
    fn test_rle_dict_decoder_zero_bit_width_mixed() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![7]);
        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 20, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push().unwrap(); // 7
        decoder.skip(3);
        decoder.push_null().unwrap(); // NULL
        decoder.push_slice(3).unwrap(); // 7, 7, 7
        decoder.skip(5);
        decoder.push_nulls(2).unwrap(); // NULL, NULL
        decoder.push().unwrap(); // 7

        assert!(decoder.result().is_ok());
        assert_eq!(
            read_i32_results(&buffers),
            vec![7, I32_NULL, 7, 7, 7, I32_NULL, I32_NULL, 7]
        );
    }

    // --- Out-of-bounds dictionary index ---

    #[test]
    fn test_rle_dict_decoder_oob_index_in_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20]); // len=2
                                                                // Index 5 is out of bounds
        let values: Vec<u32> = vec![0, 1, 5];
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 3, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap(); // ok: 10
        decoder.push().unwrap(); // ok: 20
        decoder.push().unwrap(); // oob: sets error

        assert!(decoder.result().is_err());
    }

    #[test]
    fn test_rle_dict_decoder_oob_index_in_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20]); // len=2
        let values: Vec<u32> = vec![0, 1, 0, 3, 0]; // index 3 is oob
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 5, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(5).unwrap();
        decoder.push_slice(5).unwrap();

        assert!(decoder.result().is_err());
    }

    // --- Large data spanning multiple runs ---

    #[test]
    fn test_rle_dict_decoder_large_bitpacked() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // 16 dictionary values, 1000 entries
        let dict_values: Vec<i32> = (0..16).map(|i| i * 100).collect();
        let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
        let values: Vec<u32> = (0..1000).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&values, 4);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 1000, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(1000).unwrap();
        decoder.push_slice(1000).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..1000).map(|i| (i % 16) * 100).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    #[test]
    fn test_rle_dict_decoder_large_with_incremental_reads() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict_values: Vec<i32> = (0..8).map(|i| i * 10).collect();
        let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
        let values: Vec<u32> = (0..500).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 500, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(500).unwrap();

        // Read in small chunks to stress the cross-run boundary logic
        for _ in 0..50 {
            decoder.push_slice(10).unwrap();
        }

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..500).map(|i| (i % 8) * 10).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    #[test]
    fn test_rle_dict_decoder_large_push_one_by_one() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict_values: Vec<i32> = vec![1, 2, 3, 4];
        let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
        let values: Vec<u32> = (0..200).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 200, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(200).unwrap();

        for _ in 0..200 {
            decoder.push().unwrap();
        }

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..200).map(|i| (i % 4) as i32 + 1).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    // --- push_slice crossing RLE/bitpacked run boundaries ---

    #[test]
    fn test_rle_dict_decoder_push_slice_crossing_run_boundaries() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        // Bitpacked runs are typically groups of 8 values.
        // We create 80 values (10 groups of 8) and read across boundaries.
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30, 40]);
        let values: Vec<u32> = (0..80).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 80, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(80).unwrap();

        // Read in chunk sizes that don't align with bitpacked groups of 8
        decoder.push_slice(3).unwrap();
        decoder.push_slice(7).unwrap();
        decoder.push_slice(13).unwrap();
        decoder.push_slice(25).unwrap();
        decoder.push_slice(32).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..80).map(|i| [10, 20, 30, 40][i % 4]).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    // --- Different null values ---

    #[test]
    fn test_rle_dict_decoder_custom_null_value() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![1, 2]);
        let values: Vec<u32> = vec![0, 1, 0, 1];
        let encoded = encode_rle_data(&values, 1);
        let custom_null: i32 = -1;

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 4, custom_null, &mut buffers).unwrap();
        decoder.reserve(6).unwrap();

        decoder.push().unwrap(); // 1
        decoder.push_null().unwrap(); // -1
        decoder.push().unwrap(); // 2
        decoder.push_nulls(2).unwrap(); // -1, -1
        decoder.push().unwrap(); // 1

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![1, -1, 2, -1, -1, 1]);
    }

    // --- reserve growing the buffer ---

    #[test]
    fn test_rle_dict_decoder_reserve_grows_buffer() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10, 20, 30]);
        let values: Vec<u32> = (0..100).map(|i| (i % 3) as u32).collect();
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers).unwrap();

        // Reserve in stages to test buffer growth and pointer refresh
        decoder.reserve(10).unwrap();
        decoder.push_slice(10).unwrap();

        decoder.reserve(40).unwrap();
        decoder.push_slice(40).unwrap();

        decoder.reserve(50).unwrap();
        decoder.push_slice(50).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..100).map(|i| [10, 20, 30][i % 3]).collect();
        assert_eq!(read_i32_results_n(&buffers, 100), expected);
    }

    // --- Bit width variations ---

    #[test]
    fn test_rle_dict_decoder_1bit() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![0, 1]);
        let values: Vec<u32> = (0..64).map(|i| (i % 2) as u32).collect();
        let encoded = encode_rle_data(&values, 1);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 64, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(64).unwrap();
        decoder.push_slice(64).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..64).map(|i| (i % 2) as i32).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    #[test]
    fn test_rle_dict_decoder_2bit() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100, 200, 300, 400]);
        let values: Vec<u32> = (0..64).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 64, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(64).unwrap();
        decoder.push_slice(64).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..64).map(|i| [100, 200, 300, 400][i % 4]).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    #[test]
    fn test_rle_dict_decoder_4bit() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict_values: Vec<i32> = (0..16).collect();
        let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
        let values: Vec<u32> = (0..128).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&values, 4);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 128, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(128).unwrap();
        decoder.push_slice(128).unwrap();

        assert!(decoder.result().is_ok());
        let expected: Vec<i32> = (0..128).map(|i| (i % 16) as i32).collect();
        assert_eq!(read_i32_results(&buffers), expected);
    }

    // --- Consistency: push vs push_slice produce same results ---

    #[test]
    fn test_rle_dict_decoder_push_vs_push_slice_consistency() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dict_values: Vec<i32> = (0..8).map(|i| (i + 1) * 10).collect();
        let values: Vec<u32> = (0..100).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&values, 3);

        // Method 1: push one-by-one
        let mut buffers1 = create_test_buffers(&allocator);
        {
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers1)
                    .unwrap();
            decoder.reserve(100).unwrap();
            for _ in 0..100 {
                decoder.push().unwrap();
            }
            assert!(decoder.result().is_ok());
        }

        // Method 2: push_slice all at once
        let mut buffers2 = create_test_buffers(&allocator);
        {
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers2)
                    .unwrap();
            decoder.reserve(100).unwrap();
            decoder.push_slice(100).unwrap();
            assert!(decoder.result().is_ok());
        }

        // Method 3: push_slice in small chunks
        let mut buffers3 = create_test_buffers(&allocator);
        {
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers3)
                    .unwrap();
            decoder.reserve(100).unwrap();
            for _ in 0..20 {
                decoder.push_slice(5).unwrap();
            }
            assert!(decoder.result().is_ok());
        }

        let r1 = read_i32_results(&buffers1);
        let r2 = read_i32_results(&buffers2);
        let r3 = read_i32_results(&buffers3);
        assert_eq!(r1, r2, "push vs push_slice(all)");
        assert_eq!(r1, r3, "push vs push_slice(chunks)");
    }

    // --- Skip + push_slice consistency ---

    #[test]
    fn test_rle_dict_decoder_skip_push_consistency() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let dict_values: Vec<i32> = (0..8).map(|i| (i + 1) * 10).collect();
        let values: Vec<u32> = (0..100).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&values, 3);

        // Reference: push all, then compare slices
        let mut ref_buffers = create_test_buffers(&allocator);
        {
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut ref_buffers)
                    .unwrap();
            decoder.reserve(100).unwrap();
            decoder.push_slice(100).unwrap();
            assert!(decoder.result().is_ok());
        }
        let reference = read_i32_results(&ref_buffers);

        // Test: skip N, then push remaining
        for skip in [0, 1, 7, 8, 9, 31, 32, 33, 50, 63, 64, 65, 99] {
            let mut buffers = create_test_buffers(&allocator);
            let remaining = 100 - skip;
            let dict = TestPrimitiveDictDecoder::new(dict_values.clone());
            let mut decoder =
                RleDictionaryDecoder::try_new(&encoded, dict, 100, I32_NULL, &mut buffers).unwrap();
            decoder.reserve(remaining).unwrap();
            decoder.skip(skip);
            decoder.push_slice(remaining).unwrap();
            assert!(decoder.result().is_ok(), "skip={skip}");
            assert_eq!(
                read_i32_results(&buffers),
                reference[skip..].to_vec(),
                "skip={skip}"
            );
        }
    }

    // --- i64 type test (to verify generic type parameter works) ---

    struct TestI64DictDecoder {
        values: Vec<i64>,
    }

    impl PrimitiveDictDecoder<i64> for TestI64DictDecoder {
        fn get_dict_value(&self, index: u32) -> i64 {
            self.values[index as usize]
        }
        fn len(&self) -> u32 {
            self.values.len() as u32
        }
    }

    #[test]
    fn test_rle_dict_decoder_i64_type() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestI64DictDecoder {
            values: vec![1_000_000_000_000, 2_000_000_000_000, 3_000_000_000_000],
        };
        let values: Vec<u32> = vec![0, 1, 2, 0, 1, 2];
        let encoded = encode_rle_data(&values, 2);
        let null_val: i64 = i64::MIN;

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 6, null_val, &mut buffers).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push_slice(2).unwrap();
        decoder.push_null().unwrap();
        decoder.push_slice(2).unwrap();
        decoder.push_nulls(2).unwrap();
        decoder.push().unwrap();

        assert!(decoder.result().is_ok());
        let results: Vec<i64> = buffers
            .data_vec
            .chunks(8)
            .map(|c| i64::from_le_bytes(c.try_into().unwrap()))
            .collect();
        // push_slice(2) → indices [0,1] → [1T, 2T]
        // push_null() → [MIN]
        // push_slice(2) → indices [2,0] → [3T, 1T]
        // push_nulls(2) → [MIN, MIN]
        // push() → index [1] → [2T]
        assert_eq!(
            results,
            vec![
                1_000_000_000_000,
                2_000_000_000_000,
                i64::MIN,
                3_000_000_000_000,
                1_000_000_000_000,
                i64::MIN,
                i64::MIN,
                2_000_000_000_000,
            ]
        );
    }

    // --- Error propagation: after error, push still returns Ok but result() fails ---

    #[test]
    fn test_rle_dict_decoder_error_propagation() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![10]); // len=1
        let values: Vec<u32> = vec![0, 5, 0]; // index 5 is oob
        let encoded = encode_rle_data(&values, 3);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 3, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap(); // ok
        decoder.push().unwrap(); // sets error internally
        assert!(decoder.result().is_err());

        // After error, push still returns Ok (error is sticky)
        decoder.push().unwrap();
        assert!(decoder.result().is_err());
    }

    // --- Alternating RLE runs: values that repeat to trigger RLE encoding ---

    #[test]
    fn test_rle_dict_decoder_alternating_rle_runs() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![100, 200, 300]);
        // Create data with long runs of the same value (will be RLE encoded)
        let mut values: Vec<u32> = Vec::new();
        for _ in 0..30 {
            values.push(0);
        }
        for _ in 0..30 {
            values.push(1);
        }
        for _ in 0..30 {
            values.push(2);
        }
        let encoded = encode_rle_data(&values, 2);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 90, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(90).unwrap();

        // Read across RLE run boundaries
        decoder.push_slice(25).unwrap(); // 25 × 100
        decoder.push_slice(20).unwrap(); // 5 × 100, 15 × 200
        decoder.push_slice(20).unwrap(); // 15 × 200, 5 × 300
        decoder.push_slice(25).unwrap(); // 25 × 300

        assert!(decoder.result().is_ok());
        let mut expected = Vec::new();
        expected.extend(std::iter::repeat_n(100, 30));
        expected.extend(std::iter::repeat_n(200, 30));
        expected.extend(std::iter::repeat_n(300, 30));
        assert_eq!(read_i32_results(&buffers), expected);
    }

    // --- Edge case: exactly one value ---

    #[test]
    fn test_rle_dict_decoder_single_value() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![42, 99]);
        let values: Vec<u32> = vec![1];
        let encoded = encode_rle_data(&values, 1);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 1, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push().unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![99]);
    }

    #[test]
    fn test_rle_dict_decoder_single_value_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);
        let dict = TestPrimitiveDictDecoder::new(vec![42, 99]);
        let values: Vec<u32> = vec![1];
        let encoded = encode_rle_data(&values, 1);

        let mut decoder =
            RleDictionaryDecoder::try_new(&encoded, dict, 1, I32_NULL, &mut buffers).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push_slice(1).unwrap();

        assert!(decoder.result().is_ok());
        assert_eq!(read_i32_results(&buffers), vec![99]);
    }
}
