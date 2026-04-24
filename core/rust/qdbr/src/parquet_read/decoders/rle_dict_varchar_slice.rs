//! Specialized decoder for RLE-dictionary-encoded VarcharSlice columns.
//!
//! Combines RLE index decoding with direct VarcharSlice aux entry writing,
//! eliminating the `RleDictionarySlicer` → `VarcharSliceColumnSink` indirection.
//! Dict values are pre-computed into 16-byte aux entries at construction time,
//! so each decoded index requires only a single 16-byte memcpy.

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::decoders::dictionary::BaseVarDictDecoder;
use crate::parquet_read::decoders::{RepeatN, RleIterator};
use crate::parquet_read::page::DictPage;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::varchar::SLICE_NULL_HEADER;

use parquet2::encoding::bitpacked;
use parquet2::encoding::hybrid_rle::{Decoder, HybridEncoded};

const AUX_ENTRY_SIZE: usize = 16;

pub struct RleDictVarcharSliceDecoder<'a> {
    buffers: &'a mut ColumnChunkBuffers,
    /// Pre-computed aux entries for each dict value: [len_and_flags, ptr].
    dict_aux: Vec<[u64; 2]>,
    null_entry: [u64; 2],
    dict_len: u32,
    decoder: Option<Decoder<'a>>,
    data: RleIterator<'a>,
}

impl<'a> RleDictVarcharSliceDecoder<'a> {
    pub fn try_new(
        mut buffer: &'a [u8],
        dict_page: &'a DictPage<'a>,
        buffers: &'a mut ColumnChunkBuffers,
        ascii: bool,
    ) -> ParquetResult<Self> {
        let dict_decoder = BaseVarDictDecoder::try_new(dict_page)?;

        let mut dict_aux = Vec::with_capacity(dict_decoder.dict_values.len());
        for &value in &dict_decoder.dict_values {
            if value.len() >= (1usize << 28) {
                return Err(fmt_err!(
                    Layout,
                    "dictionary value length {} exceeds 28-bit header capacity",
                    value.len()
                ));
            }
            let len = value.len() as u32;
            let header: u32 = (len << 4) | if ascii || len == 0 { 3 } else { 1 };
            let ptr = value.as_ptr() as u64;
            dict_aux.push([header as u64, ptr]);
        }

        let null_entry = [SLICE_NULL_HEADER as u64, 0u64];
        let dict_len = dict_decoder.dict_values.len() as u32;

        let num_bits = *buffer
            .first()
            .ok_or_else(|| fmt_err!(Layout, "empty RLE dictionary buffer"))?;
        if num_bits > 0 {
            buffer = &buffer[1..];
            let hybrid_decoder = Decoder::new(buffer, num_bits as usize);
            // We mustn't eagerly decode here, a page may have zero non-null values.
            Ok(Self {
                buffers,
                dict_aux,
                null_entry,
                dict_len,
                decoder: Some(hybrid_decoder),
                data: RleIterator::Rle(RepeatN::new(0, 0)),
            })
        } else {
            // Zero bit width: single dict entry, all indices are 0.
            // We don't know row_count here, but we use usize::MAX as a
            // practically infinite repeat count — the caller controls how
            // many values are consumed via push/push_slice.
            Ok(Self {
                buffers,
                dict_aux,
                null_entry,
                dict_len,
                decoder: None,
                data: RleIterator::Rle(RepeatN::new(0, usize::MAX)),
            })
        }
    }

    fn decode_next_run(&mut self) -> ParquetResult<()> {
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
                    self.data = RleIterator::ByteIndices { data: values, pos: 0 };
                } else {
                    let count = values.len() * 8 / num_bits;
                    let inner_decoder =
                        bitpacked::Decoder::<u32>::try_new(values, num_bits, count)?;
                    self.data = RleIterator::Bitpacked(inner_decoder);
                }
            }
            HybridEncoded::Rle(pack, repeat) => {
                let mut bytes = [0u8; std::mem::size_of::<u32>()];
                pack.iter().zip(bytes.iter_mut()).for_each(|(src, dst)| {
                    *dst = *src;
                });
                let value = u32::from_le_bytes(bytes);
                self.data = RleIterator::Rle(RepeatN::new(value, repeat));
            }
        }
        Ok(())
    }

    #[inline(always)]
    fn write_aux_entry(&self, aux_ptr: *mut u8, entry: &[u64; 2]) {
        unsafe {
            let addr = aux_ptr.cast::<u64>();
            std::ptr::write_unaligned(addr, entry[0]);
            std::ptr::write_unaligned(addr.add(1), entry[1]);
        }
    }
}

impl Pushable for RleDictVarcharSliceDecoder<'_> {
    fn reserve(&mut self, count: usize) -> ParquetResult<()> {
        let reserve_bytes = count
            .checked_mul(AUX_ENTRY_SIZE)
            .ok_or_else(|| fmt_err!(Layout, "requested reserve size exceeds usize limits"))?;
        self.buffers.aux_vec.reserve(reserve_bytes)?;
        Ok(())
    }

    #[inline]
    fn push(&mut self) -> ParquetResult<()> {
        loop {
            if let Some(idx) = self.data.next() {
                return if idx < self.dict_len {
                    let entry = self.dict_aux[idx as usize];
                    let aux_ptr = unsafe {
                        self.buffers
                            .aux_vec
                            .as_mut_ptr()
                            .add(self.buffers.aux_vec.len())
                    };
                    self.write_aux_entry(aux_ptr, &entry);
                    unsafe {
                        self.buffers
                            .aux_vec
                            .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
                    }
                    Ok(())
                } else {
                    Err(fmt_err!(
                        Layout,
                        "index {} is out of dict bounds {}",
                        idx,
                        self.dict_len
                    ))
                };
            }
            self.decode_next_run()?;
        }
    }

    #[inline]
    fn push_null(&mut self) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        self.write_aux_entry(aux_ptr, &self.null_entry);
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()> {
        let aux_ptr = unsafe {
            self.buffers
                .aux_vec
                .as_mut_ptr()
                .add(self.buffers.aux_vec.len())
        };
        let null = self.null_entry;
        for i in 0..count {
            unsafe {
                let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                std::ptr::write_unaligned(addr, null[0]);
                std::ptr::write_unaligned(addr.add(1), null[1]);
            }
        }
        unsafe {
            self.buffers
                .aux_vec
                .set_len(self.buffers.aux_vec.len() + count * AUX_ENTRY_SIZE);
        }
        Ok(())
    }

    #[inline(always)]
    fn push_slice(&mut self, count: usize) -> ParquetResult<()> {
        let mut remaining = count;
        loop {
            if remaining == 0 {
                return Ok(());
            }

            let consumed = match &mut self.data {
                RleIterator::Rle(repeat) => {
                    let n = remaining.min(repeat.remaining);
                    if n == 0 {
                        0
                    } else {
                        if repeat.value >= self.dict_len {
                            return Err(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                repeat.value,
                                self.dict_len
                            ));
                        }
                        let entry = self.dict_aux[repeat.value as usize];
                        let aux_ptr = unsafe {
                            self.buffers
                                .aux_vec
                                .as_mut_ptr()
                                .add(self.buffers.aux_vec.len())
                        };
                        for i in 0..n {
                            unsafe {
                                let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                                std::ptr::write_unaligned(addr, entry[0]);
                                std::ptr::write_unaligned(addr.add(1), entry[1]);
                            }
                        }
                        unsafe {
                            self.buffers
                                .aux_vec
                                .set_len(self.buffers.aux_vec.len() + n * AUX_ENTRY_SIZE);
                        }
                        repeat.remaining -= n;
                        n
                    }
                }
                RleIterator::Bitpacked(bp) => {
                    let dict_len = self.dict_len;
                    let dict_aux = &self.dict_aux;
                    let mut consumed = 0usize;
                    let unpack_ptr = bp.unpacked.as_ref().as_ptr();

                    while consumed < remaining && bp.remaining > 0 {
                        let avail_in_pack = (32 - bp.current_pack_index).min(bp.remaining);
                        let n = avail_in_pack.min(remaining - consumed);
                        let start = bp.current_pack_index;

                        let aux_ptr = unsafe {
                            self.buffers
                                .aux_vec
                                .as_mut_ptr()
                                .add(self.buffers.aux_vec.len())
                        };

                        for i in 0..n {
                            let idx = unsafe { *unpack_ptr.add(start + i) };
                            if idx >= dict_len {
                                unsafe {
                                    self.buffers.aux_vec.set_len(
                                        self.buffers.aux_vec.len() + consumed * AUX_ENTRY_SIZE,
                                    );
                                }
                                return Err(fmt_err!(
                                    Layout,
                                    "index {} is out of dict bounds {}",
                                    idx,
                                    dict_len
                                ));
                            }
                            let entry = dict_aux[idx as usize];
                            unsafe {
                                let addr =
                                    aux_ptr.add((consumed + i) * AUX_ENTRY_SIZE).cast::<u64>();
                                std::ptr::write_unaligned(addr, entry[0]);
                                std::ptr::write_unaligned(addr.add(1), entry[1]);
                            }
                        }

                        bp.current_pack_index += n;
                        bp.remaining -= n;
                        consumed += n;

                        if bp.current_pack_index == 32 {
                            bp.decode_next_pack();
                        }
                    }

                    unsafe {
                        self.buffers
                            .aux_vec
                            .set_len(self.buffers.aux_vec.len() + consumed * AUX_ENTRY_SIZE);
                    }
                    consumed
                }
                RleIterator::ByteIndices { data, pos } => {
                    let dict_len = self.dict_len;
                    let dict_aux = &self.dict_aux;
                    let avail = data.len() - *pos;
                    let n = remaining.min(avail);
                    let bytes = &data[*pos..];

                    let aux_ptr = unsafe {
                        self.buffers
                            .aux_vec
                            .as_mut_ptr()
                            .add(self.buffers.aux_vec.len())
                    };

                    for i in 0..n {
                        let idx = unsafe { *bytes.get_unchecked(i) } as u32;
                        if idx >= dict_len {
                            unsafe {
                                self.buffers
                                    .aux_vec
                                    .set_len(self.buffers.aux_vec.len() + i * AUX_ENTRY_SIZE);
                            }
                            *pos += i;
                            return Err(fmt_err!(
                                Layout,
                                "index {} is out of dict bounds {}",
                                idx,
                                dict_len
                            ));
                        }
                        let entry = dict_aux[idx as usize];
                        unsafe {
                            let addr = aux_ptr.add(i * AUX_ENTRY_SIZE).cast::<u64>();
                            std::ptr::write_unaligned(addr, entry[0]);
                            std::ptr::write_unaligned(addr.add(1), entry[1]);
                        }
                    }

                    *pos += n;
                    unsafe {
                        self.buffers
                            .aux_vec
                            .set_len(self.buffers.aux_vec.len() + n * AUX_ENTRY_SIZE);
                    }
                    n
                }
            };

            remaining -= consumed;
            if remaining > 0 {
                self.decode_next_run()?;
            }
        }
    }

    fn skip(&mut self, count: usize) -> ParquetResult<()> {
        let mut remaining = count;
        while remaining > 0 {
            let skipped = self.data.skip(remaining);
            remaining -= skipped;

            if remaining > 0 {
                self.decode_next_run()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::allocator::{AcVec, TestAllocatorState};
    use crate::parquet_read::column_sink::Pushable;
    use crate::parquet_read::decoders::rle_dict_varchar_slice::RleDictVarcharSliceDecoder;
    use crate::parquet_read::page::DictPage;
    use crate::parquet_read::ColumnChunkBuffers;
    use std::ptr;

    fn build_dict_page_buffer(values: &[&[u8]]) -> Vec<u8> {
        let mut buf = Vec::new();
        for v in values {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        buf
    }

    fn make_dict_page(buffer: &[u8], num_values: usize) -> DictPage<'_> {
        DictPage { buffer, num_values, is_sorted: false }
    }

    fn create_test_buffers(allocator: &crate::allocator::QdbAllocator) -> ColumnChunkBuffers {
        ColumnChunkBuffers {
            data_size: 0,
            data_ptr: ptr::null_mut(),
            data_vec: AcVec::new_in(allocator.clone()),
            aux_size: 0,
            aux_ptr: ptr::null_mut(),
            aux_vec: AcVec::new_in(allocator.clone()),
            page_buffers: Vec::new(),
        }
    }

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

    /// Manually craft an RLE-encoded buffer containing a single RLE run.
    /// The parquet2 encoder only produces bitpacked runs, so this is the
    /// only way to exercise the `HybridEncoded::Rle` decode path.
    ///
    /// Wire format: [num_bits, indicator_uleb128, value_bytes...]
    /// where indicator = run_length << 1 (even = RLE), and value is
    /// ceil8(num_bits) bytes in little-endian.
    fn encode_rle_run(value: u32, run_length: usize, num_bits: u8) -> Vec<u8> {
        let mut buf = vec![num_bits];
        // Indicator = run_length << 1 (bit 0 = 0 means RLE), single-byte ULEB128
        let indicator = (run_length << 1) as u8;
        buf.push(indicator);
        // Write the value in ceil8(num_bits) bytes, little-endian
        let rle_bytes = (num_bits as usize).div_ceil(8);
        buf.extend_from_slice(&value.to_le_bytes()[..rle_bytes]);
        buf
    }

    /// Craft an RLE buffer with two consecutive RLE runs.
    fn encode_two_rle_runs(
        value1: u32,
        run_length1: usize,
        value2: u32,
        run_length2: usize,
        num_bits: u8,
    ) -> Vec<u8> {
        let mut buf = vec![num_bits];
        let rle_bytes = (num_bits as usize).div_ceil(8);

        for &(value, run_length) in &[(value1, run_length1), (value2, run_length2)] {
            let indicator = (run_length << 1) as u8;
            buf.push(indicator);
            buf.extend_from_slice(&value.to_le_bytes()[..rle_bytes]);
        }
        buf
    }

    fn read_aux_entries(buffers: &ColumnChunkBuffers) -> Vec<[u64; 2]> {
        buffers
            .aux_vec
            .chunks(16)
            .map(|c| {
                let lo = u64::from_le_bytes(c[0..8].try_into().unwrap());
                let hi = u64::from_le_bytes(c[8..16].try_into().unwrap());
                [lo, hi]
            })
            .collect()
    }

    fn expected_header(value: &[u8], ascii: bool) -> u64 {
        let len = value.len() as u32;
        let flags: u32 = if ascii || len == 0 { 3 } else { 1 };
        ((len << 4) | flags) as u64
    }

    fn extract_headers(entries: &[[u64; 2]]) -> Vec<u64> {
        entries.iter().map(|e| e[0]).collect()
    }

    const NULL_HEADER: u64 = 4;

    // --- Basic push ---

    #[test]
    fn test_zero_values_positive_bit_width() {
        // Regression: the writer can emit `[bits_per_key, 0x01]` for a
        // data page with zero non-null values but a non-empty global
        // dictionary (bits_per_key > 0). `try_new` must not surface
        // an "Unexpected end of rle iterator" error for this valid
        // payload -- eager decoding would do so because the hybrid-RLE
        // stream returns None after the bitpacked-zero-groups header.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aaa", b"bbb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[], 6);
        assert_eq!(encoded, vec![6, 0x01]);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true)
                .expect("try_new must not fail on a valid zero-values stream");

        // Caller only feeds nulls for this page; push_nulls must not touch
        // the RLE stream.
        decoder.reserve(3).unwrap();
        decoder.push_nulls(3).unwrap();
        assert_eq!(
            extract_headers(&read_aux_entries(&buffers)),
            vec![NULL_HEADER, NULL_HEADER, NULL_HEADER]
        );
    }

    #[test]
    fn test_truncated_payload_errors_on_first_consume() {
        // The lazy-decode change shifts the error for a truncated payload
        // from `try_new` to the first consumer call. This test pins that
        // contract: a positive `bits_per_key` with no hybrid-RLE body
        // constructs successfully but errors the moment the caller tries
        // to read a value.
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_strings: &[&[u8]] = &[b"aaa", b"bbb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // push(): single-value consumer path.
        {
            let mut buffers = create_test_buffers(&allocator);
            let buffer: Vec<u8> = vec![6];
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&buffer, &dict_page, &mut buffers, true)
                    .unwrap();
            decoder.reserve(1).unwrap();
            let err = decoder.push().unwrap_err();
            assert!(
                err.to_string().contains("Unexpected end of rle iterator"),
                "unexpected error: {err}"
            );
        }

        // push_slice(): bulk consumer path.
        {
            let mut buffers = create_test_buffers(&allocator);
            let buffer: Vec<u8> = vec![6];
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&buffer, &dict_page, &mut buffers, true)
                    .unwrap();
            decoder.reserve(1).unwrap();
            let err = decoder.push_slice(1).unwrap_err();
            assert!(
                err.to_string().contains("Unexpected end of rle iterator"),
                "unexpected error: {err}"
            );
        }

        // skip(): skip consumer path.
        {
            let mut buffers = create_test_buffers(&allocator);
            let buffer: Vec<u8> = vec![6];
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&buffer, &dict_page, &mut buffers, true)
                    .unwrap();
            let err = decoder.skip(1).unwrap_err();
            assert!(
                err.to_string().contains("Unexpected end of rle iterator"),
                "unexpected error: {err}"
            );
        }
    }

    #[test]
    fn test_push_single_values() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aaa", b"bbb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 1, 2, 1, 0], 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        for _ in 0..5 {
            decoder.push().unwrap();
        }

        let entries = read_aux_entries(&buffers);
        let expected = vec![
            expected_header(b"aaa", true),
            expected_header(b"bbb", true),
            expected_header(b"ccc", true),
            expected_header(b"bbb", true),
            expected_header(b"aaa", true),
        ];
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_push_single_entry_dict() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"only"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0]; // zero bit width

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();

        for _ in 0..10 {
            decoder.push().unwrap();
        }

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 10);
        let exp = expected_header(b"only", true);
        for entry in &entries {
            assert_eq!(entry[0], exp);
        }
    }

    // --- push_slice ---

    #[test]
    fn test_push_slice_basic() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc", b"dddd", b"eeeee"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 4, 3, 2, 1, 0];
        let encoded = encode_rle_data(&indices, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();
        decoder.push_slice(10).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = [
            b"a" as &[u8],
            b"bb",
            b"ccc",
            b"dddd",
            b"eeeee",
            b"eeeee",
            b"dddd",
            b"ccc",
            b"bb",
            b"a",
        ]
        .iter()
        .map(|v| expected_header(v, true))
        .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_push_slice_repeated() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"val1", b"val2"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0; 50];
        let encoded = encode_rle_data(&indices, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(50).unwrap();
        decoder.push_slice(50).unwrap();

        let entries = read_aux_entries(&buffers);
        let exp = expected_header(b"val1", true);
        assert_eq!(extract_headers(&entries), vec![exp; 50]);
    }

    #[test]
    fn test_push_slice_8bit_indices() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: Vec<Vec<u8>> =
            (0..200).map(|i| format!("v_{i:03}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);
        let dict_page = make_dict_page(&dict_buf, dict_refs.len());

        let indices: Vec<u32> = (0..200).map(|i| i as u32).collect();
        let encoded = encode_rle_data(&indices, 8);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(200).unwrap();
        decoder.push_slice(200).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = dict_refs.iter().map(|v| expected_header(v, true)).collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_push_slice_zero_count() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 1, 2], 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(0).unwrap();
        decoder.push_slice(0).unwrap();
        assert_eq!(read_aux_entries(&buffers).len(), 0);
    }

    #[test]
    fn test_push_slice_multiple_calls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc", b"dddd", b"eeeee"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 0, 1, 2, 3, 4];
        let encoded = encode_rle_data(&indices, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();
        decoder.push_slice(3).unwrap();
        decoder.push_slice(4).unwrap();
        decoder.push_slice(3).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = [
            b"a" as &[u8],
            b"bb",
            b"ccc",
            b"dddd",
            b"eeeee",
            b"a",
            b"bb",
            b"ccc",
            b"dddd",
            b"eeeee",
        ]
        .iter()
        .map(|v| expected_header(v, true))
        .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- push_null / push_nulls ---

    #[test]
    fn test_push_null() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"x"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();
        decoder.push_null().unwrap();
        decoder.push_null().unwrap();
        decoder.push_null().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 3);
        for entry in &entries {
            assert_eq!(entry[0], NULL_HEADER);
            assert_eq!(entry[1], 0);
        }
    }

    #[test]
    fn test_push_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"x"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(7).unwrap();
        decoder.push_nulls(7).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 7);
        for entry in &entries {
            assert_eq!(entry[0], NULL_HEADER);
            assert_eq!(entry[1], 0);
        }
    }

    #[test]
    fn test_push_nulls_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"x"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(0).unwrap();
        decoder.push_nulls(0).unwrap();
        assert_eq!(read_aux_entries(&buffers).len(), 0);
    }

    // --- skip ---

    #[test]
    fn test_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc", b"dddd", b"eeeee"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0, 1, 2, 3, 4];
        let encoded = encode_rle_data(&indices, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();
        decoder.skip(2).unwrap();
        decoder.push_slice(3).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = [b"ccc" as &[u8], b"dddd", b"eeeee"]
            .iter()
            .map(|v| expected_header(v, true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_skip_zero() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 1, 2], 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.skip(0).unwrap();
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries[0][0], expected_header(b"a", true));
    }

    #[test]
    fn test_skip_all() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 1, 2, 0, 1], 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.skip(5).unwrap();
        assert_eq!(read_aux_entries(&buffers).len(), 0);
    }

    #[test]
    fn test_skip_large_across_runs() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_strings: Vec<Vec<u8>> = (0..16).map(|i| format!("v{i:02}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);

        let values: Vec<u32> = (0..200).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&values, 4);

        for skip in [0, 1, 31, 32, 33, 63, 64, 65, 99, 100, 150, 199] {
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut buffers = create_test_buffers(&allocator);
            let remaining = 200 - skip;
            let to_read = remaining.min(5);

            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true)
                    .unwrap();
            decoder.reserve(to_read).unwrap();
            decoder.skip(skip).unwrap();
            decoder.push_slice(to_read).unwrap();

            let entries = read_aux_entries(&buffers);
            let expected: Vec<u64> = (skip..skip + to_read)
                .map(|i| expected_header(dict_refs[i % 16], true))
                .collect();
            assert_eq!(extract_headers(&entries), expected, "skip={skip}");
        }
    }

    // --- Mixed operations ---

    #[test]
    fn test_mixed_push_null_skip() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc", b"dddd", b"eeeee", b"f", b"gg", b"hhh"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7];
        let encoded = encode_rle_data(&indices, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();

        // push_slice(2): indices 0,1 -> "a", "bb"
        decoder.push_slice(2).unwrap();
        // push_nulls(2): [NULL, NULL]
        decoder.push_nulls(2).unwrap();
        // skip(3): indices 2,3,4 -> skipped
        decoder.skip(3).unwrap();
        // push(): index 5 -> "f"
        decoder.push().unwrap();
        // push_null(): [NULL]
        decoder.push_null().unwrap();
        // push_slice(3): indices 6,7,8(=0) -> "gg", "hhh", "a"
        decoder.push_slice(3).unwrap();
        // skip(2): indices 9,10(=1,2) -> skipped
        decoder.skip(2).unwrap();
        // push(): index 11(=3) -> "dddd"
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        let expected_headers = vec![
            expected_header(b"a", true),
            expected_header(b"bb", true),
            NULL_HEADER,
            NULL_HEADER,
            expected_header(b"f", true),
            NULL_HEADER,
            expected_header(b"gg", true),
            expected_header(b"hhh", true),
            expected_header(b"a", true),
            expected_header(b"dddd", true),
        ];
        assert_eq!(extract_headers(&entries), expected_headers);

        // Verify null pointers are zero
        assert_eq!(entries[2][1], 0);
        assert_eq!(entries[3][1], 0);
        assert_eq!(entries[5][1], 0);
    }

    #[test]
    fn test_interleaved_push_and_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb", b"cc", b"dd"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = vec![0, 1, 2, 3, 0, 1, 2, 3];
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push().unwrap(); // aa
        decoder.push_slice(2).unwrap(); // bb, cc
        decoder.push().unwrap(); // dd
        decoder.push().unwrap(); // aa
        decoder.push_slice(3).unwrap(); // bb, cc, dd

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = [
            b"aa" as &[u8],
            b"bb",
            b"cc",
            b"dd",
            b"aa",
            b"bb",
            b"cc",
            b"dd",
        ]
        .iter()
        .map(|v| expected_header(v, true))
        .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Zero bit width ---

    #[test]
    fn test_zero_bit_width_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"constant"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(100).unwrap();
        decoder.push_slice(100).unwrap();

        let entries = read_aux_entries(&buffers);
        let exp = expected_header(b"constant", true);
        assert_eq!(extract_headers(&entries), vec![exp; 100]);
    }

    #[test]
    fn test_zero_bit_width_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"val"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();
        decoder.skip(15).unwrap();
        decoder.push_slice(5).unwrap();

        let entries = read_aux_entries(&buffers);
        let exp = expected_header(b"val", true);
        assert_eq!(extract_headers(&entries), vec![exp; 5]);
    }

    #[test]
    fn test_zero_bit_width_mixed() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"v"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded: Vec<u8> = vec![0];

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push().unwrap(); // v
        decoder.skip(3).unwrap();
        decoder.push_null().unwrap(); // NULL
        decoder.push_slice(3).unwrap(); // v, v, v
        decoder.skip(5).unwrap();
        decoder.push_nulls(2).unwrap(); // NULL, NULL
        decoder.push().unwrap(); // v

        let entries = read_aux_entries(&buffers);
        let v = expected_header(b"v", true);
        let expected = vec![v, NULL_HEADER, v, v, v, NULL_HEADER, NULL_HEADER, v];
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Large data ---

    #[test]
    fn test_large_bitpacked() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: Vec<Vec<u8>> = (0..16)
            .map(|i| format!("val_{i:02}").into_bytes())
            .collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);
        let dict_page = make_dict_page(&dict_buf, dict_refs.len());

        let indices: Vec<u32> = (0..1000).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&indices, 4);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(1000).unwrap();
        decoder.push_slice(1000).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..1000)
            .map(|i| expected_header(dict_refs[i % 16], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_large_incremental_reads() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: Vec<Vec<u8>> = (0..8).map(|i| format!("s{i}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);
        let dict_page = make_dict_page(&dict_buf, dict_refs.len());

        let indices: Vec<u32> = (0..500).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&indices, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(500).unwrap();

        for _ in 0..50 {
            decoder.push_slice(10).unwrap();
        }

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..500)
            .map(|i| expected_header(dict_refs[i % 8], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_large_push_one_by_one() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"w", b"x", b"y", b"z"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = (0..200).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(200).unwrap();

        for _ in 0..200 {
            decoder.push().unwrap();
        }

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..200)
            .map(|i| expected_header(dict_strings[i % 4], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Run boundaries ---

    #[test]
    fn test_push_slice_crossing_run_boundaries() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb", b"cc", b"dd"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = (0..80).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(80).unwrap();

        decoder.push_slice(3).unwrap();
        decoder.push_slice(7).unwrap();
        decoder.push_slice(13).unwrap();
        decoder.push_slice(25).unwrap();
        decoder.push_slice(32).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..80)
            .map(|i| expected_header(dict_strings[i % 4], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_alternating_rle_runs() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"alpha", b"beta", b"gamma"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let mut indices: Vec<u32> = Vec::new();
        indices.extend(std::iter::repeat_n(0u32, 30));
        indices.extend(std::iter::repeat_n(1u32, 30));
        indices.extend(std::iter::repeat_n(2u32, 30));
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(90).unwrap();

        // Read across RLE run boundaries
        decoder.push_slice(25).unwrap();
        decoder.push_slice(20).unwrap();
        decoder.push_slice(20).unwrap();
        decoder.push_slice(25).unwrap();

        let entries = read_aux_entries(&buffers);
        let mut expected = Vec::new();
        expected.extend(std::iter::repeat_n(expected_header(b"alpha", true), 30));
        expected.extend(std::iter::repeat_n(expected_header(b"beta", true), 30));
        expected.extend(std::iter::repeat_n(expected_header(b"gamma", true), 30));
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Edge cases ---

    #[test]
    fn test_single_value() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"xx", b"yy"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[1], 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push().unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries[0][0], expected_header(b"yy", true));
    }

    #[test]
    fn test_single_value_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"xx", b"yy"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[1], 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(1).unwrap();
        decoder.push_slice(1).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries[0][0], expected_header(b"yy", true));
    }

    #[test]
    fn test_ascii_vs_non_ascii_flags() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_strings: &[&[u8]] = &[b"hello"];
        let dict_buf = build_dict_page_buffer(dict_strings);

        // ASCII mode
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());
        let mut buffers_ascii = create_test_buffers(&allocator);
        let encoded: Vec<u8> = vec![0];
        let mut dec_ascii =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers_ascii, true)
                .unwrap();
        dec_ascii.reserve(1).unwrap();
        dec_ascii.push().unwrap();

        // Non-ASCII mode
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());
        let mut buffers_non_ascii = create_test_buffers(&allocator);
        let encoded: Vec<u8> = vec![0];
        let mut dec_non_ascii = RleDictVarcharSliceDecoder::try_new(
            &encoded,
            &dict_page,
            &mut buffers_non_ascii,
            false,
        )
        .unwrap();
        dec_non_ascii.reserve(1).unwrap();
        dec_non_ascii.push().unwrap();

        let ascii_entries = read_aux_entries(&buffers_ascii);
        let non_ascii_entries = read_aux_entries(&buffers_non_ascii);

        // ASCII: flags = 3 (bits 0,1), non-ASCII: flags = 1 (bit 0 only)
        assert_eq!(ascii_entries[0][0], expected_header(b"hello", true));
        assert_eq!(non_ascii_entries[0][0], expected_header(b"hello", false));
        assert_ne!(ascii_entries[0][0], non_ascii_entries[0][0]);

        // len=5, ascii header = (5<<4)|3 = 83, non-ascii header = (5<<4)|1 = 81
        assert_eq!(ascii_entries[0][0], 83);
        assert_eq!(non_ascii_entries[0][0], 81);
    }

    #[test]
    fn test_empty_string_in_dict() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"", b"abc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 1], 1);

        // ascii=false, but empty string should still get flags=3
        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, false).unwrap();
        decoder.reserve(2).unwrap();
        decoder.push_slice(2).unwrap();

        let entries = read_aux_entries(&buffers);
        // Empty string: (0 << 4) | 3 = 3
        assert_eq!(entries[0][0], 3);
        // Non-empty, non-ascii: (3 << 4) | 1 = 49
        assert_eq!(entries[1][0], expected_header(b"abc", false));
    }

    #[test]
    fn test_reserve_grows_buffer() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"bb", b"ccc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = (0..100).map(|i| (i % 3) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();

        decoder.reserve(10).unwrap();
        decoder.push_slice(10).unwrap();

        decoder.reserve(40).unwrap();
        decoder.push_slice(40).unwrap();

        decoder.reserve(50).unwrap();
        decoder.push_slice(50).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..100)
            .map(|i| expected_header(dict_strings[i % 3], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Consistency ---

    #[test]
    fn test_push_vs_push_slice_consistency() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_strings: Vec<Vec<u8>> = (0..8).map(|i| format!("v{i}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);

        let indices: Vec<u32> = (0..100).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&indices, 3);

        // Method 1: push one-by-one
        let mut buffers1 = create_test_buffers(&allocator);
        {
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers1, true)
                    .unwrap();
            decoder.reserve(100).unwrap();
            for _ in 0..100 {
                decoder.push().unwrap();
            }
        }

        // Method 2: push_slice all at once
        let mut buffers2 = create_test_buffers(&allocator);
        {
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers2, true)
                    .unwrap();
            decoder.reserve(100).unwrap();
            decoder.push_slice(100).unwrap();
        }

        // Method 3: push_slice in small chunks
        let mut buffers3 = create_test_buffers(&allocator);
        {
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers3, true)
                    .unwrap();
            decoder.reserve(100).unwrap();
            for _ in 0..20 {
                decoder.push_slice(5).unwrap();
            }
        }

        let h1 = extract_headers(&read_aux_entries(&buffers1));
        let h2 = extract_headers(&read_aux_entries(&buffers2));
        let h3 = extract_headers(&read_aux_entries(&buffers3));
        assert_eq!(h1, h2, "push vs push_slice(all)");
        assert_eq!(h1, h3, "push vs push_slice(chunks)");
    }

    #[test]
    fn test_skip_push_consistency() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();

        let dict_strings: Vec<Vec<u8>> = (0..8).map(|i| format!("v{i}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);

        let indices: Vec<u32> = (0..100).map(|i| (i % 8) as u32).collect();
        let encoded = encode_rle_data(&indices, 3);

        // Reference: push all
        let mut ref_buffers = create_test_buffers(&allocator);
        {
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut ref_buffers, true)
                    .unwrap();
            decoder.reserve(100).unwrap();
            decoder.push_slice(100).unwrap();
        }
        let reference = extract_headers(&read_aux_entries(&ref_buffers));

        for skip in [0, 1, 7, 8, 9, 31, 32, 33, 50, 63, 64, 65, 99] {
            let mut buffers = create_test_buffers(&allocator);
            let remaining = 100 - skip;
            let dict_page = make_dict_page(&dict_buf, dict_refs.len());
            let mut decoder =
                RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true)
                    .unwrap();
            decoder.reserve(remaining).unwrap();
            decoder.skip(skip).unwrap();
            decoder.push_slice(remaining).unwrap();
            assert_eq!(
                extract_headers(&read_aux_entries(&buffers)),
                reference[skip..].to_vec()
            );
        }
    }

    // --- Bit width variations ---

    #[test]
    fn test_1bit_indices() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"zero", b"one"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = (0..64).map(|i| (i % 2) as u32).collect();
        let encoded = encode_rle_data(&indices, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(64).unwrap();
        decoder.push_slice(64).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..64)
            .map(|i| expected_header(dict_strings[i % 2], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_2bit_indices() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb", b"cc", b"dd"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let indices: Vec<u32> = (0..64).map(|i| (i % 4) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(64).unwrap();
        decoder.push_slice(64).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..64)
            .map(|i| expected_header(dict_strings[i % 4], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    #[test]
    fn test_4bit_indices() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: Vec<Vec<u8>> = (0..16).map(|i| format!("v{i:02}").into_bytes()).collect();
        let dict_refs: Vec<&[u8]> = dict_strings.iter().map(|v| v.as_slice()).collect();
        let dict_buf = build_dict_page_buffer(&dict_refs);
        let dict_page = make_dict_page(&dict_buf, dict_refs.len());

        let indices: Vec<u32> = (0..128).map(|i| (i % 16) as u32).collect();
        let encoded = encode_rle_data(&indices, 4);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(128).unwrap();
        decoder.push_slice(128).unwrap();

        let entries = read_aux_entries(&buffers);
        let expected: Vec<u64> = (0..128)
            .map(|i| expected_header(dict_refs[i % 16], true))
            .collect();
        assert_eq!(extract_headers(&entries), expected);
    }

    // --- Unhappy paths ---

    #[test]
    fn test_empty_rle_buffer() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"x"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let result = RleDictVarcharSliceDecoder::try_new(&[], &dict_page, &mut buffers, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_oob_index_in_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b"]; // len=2
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Index 5 is out of bounds
        let encoded = encode_rle_data(&[0, 1, 5], 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap(); // ok: a
        decoder.push().unwrap(); // ok: b
        assert!(decoder.push().is_err()); // oob: immediate error
    }

    #[test]
    fn test_oob_index_in_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b"]; // len=2
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Index 3 is out of bounds
        let encoded = encode_rle_data(&[0, 1, 0, 3, 0], 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();
        assert!(decoder.push_slice(5).is_err());
    }

    #[test]
    fn test_error_propagation() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a"]; // len=1
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Index 5 is oob
        let encoded = encode_rle_data(&[0, 5, 0], 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap(); // ok
        assert!(decoder.push().is_err()); // immediate error on oob
    }

    #[test]
    fn test_error_sticky_after_skip() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a"]; // len=1
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 5, 0], 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(3).unwrap();

        decoder.push().unwrap();
        assert!(decoder.push().is_err()); // immediate error on oob
    }

    #[test]
    fn test_error_sticky_after_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a"]; // len=1
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        let encoded = encode_rle_data(&[0, 5, 0], 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        decoder.push().unwrap();
        assert!(decoder.push().is_err()); // immediate error on oob
    }

    // Dict value length >= 2^28 exceeds header capacity
    #[test]
    fn test_dict_value_exceeds_28bit_header() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        // Craft a dict buffer with a length field of 1 << 28.
        let mut dict_buf = Vec::new();
        const HUGE_LEN: u32 = 1u32 << 28;
        dict_buf.extend_from_slice(&HUGE_LEN.to_le_bytes());
        // Fill the rest of the buffer to match the declared length
        dict_buf.extend_from_slice(&vec![b'x'; HUGE_LEN as usize]); // actual content doesn't matter

        let dict_page = make_dict_page(&dict_buf, 1);
        let encoded: Vec<u8> = vec![0];

        let result = RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true);
        // BaseVarDictDecoder catches the short buffer before we reach the 28-bit check
        assert!(result.is_err());
    }

    // push_slice detects OOB value via the RleIterator::Rle path.
    // encode_rle_data only produces bitpacked runs, so we use encode_rle_run
    // to craft a genuine RLE run.
    #[test]
    fn test_oob_index_in_push_slice_rle_run() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b"]; // dict_len = 2
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Craft an RLE run of 10 repeats of OOB index 5, using 3 bits
        let encoded = encode_rle_run(5, 10, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        // push_slice hits the OOB repeated value
        assert!(decoder.push_slice(5).is_err());
    }

    // push_slice OOB in ByteIndices (8-bit) path
    #[test]
    fn test_oob_index_in_push_slice_8bit() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"]; // dict_len = 3
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // 8-bit encoding with an OOB index (200 >= dict_len 3)
        let indices: Vec<u32> = vec![0, 1, 2, 0, 200];
        let encoded = encode_rle_data(&indices, 8);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();
        assert!(decoder.push_slice(5).is_err());
    }

    // push() exhausts encoded data, decode_next_run fails
    //
    // Bitpacked encoding pads to groups of 8, so we need enough values to
    // fill complete groups. With 64 values at 2-bit encoding, we get exactly
    // 2 bitpacked groups of 32. Reading 65 should exhaust the decoder.
    #[test]
    fn test_exhaustion_in_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Encode 64 values (fills bitpacked groups exactly)
        let indices: Vec<u32> = (0..64).map(|i| (i % 3) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(100).unwrap();

        // Push all 64 valid values
        for _ in 0..64 {
            decoder.push().unwrap();
        }

        // 65th push exhausts the decoder
        assert!(decoder.push().is_err());
    }

    // push_slice() exhausts encoded data
    #[test]
    fn test_exhaustion_in_push_slice() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Encode 64 values
        let indices: Vec<u32> = (0..64).map(|i| (i % 3) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(100).unwrap();

        // Push all 64, then try to push more
        decoder.push_slice(64).unwrap();
        assert!(decoder.push_slice(1).is_err());
    }

    // skip() exhausts encoded data
    #[test]
    fn test_exhaustion_in_skip() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Encode 64 values
        let indices: Vec<u32> = (0..64).map(|i| (i % 3) as u32).collect();
        let encoded = encode_rle_data(&indices, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();

        // Skip all 64, then try to skip more
        decoder.skip(64).unwrap();
        assert!(decoder.skip(1).is_err());
    }

    // First RLE run exhausted (remaining=0), triggers decode_next_run
    // which decodes a second RLE run.
    // Uses encode_two_rle_runs to craft genuine HybridEncoded::Rle runs.
    #[test]
    fn test_push_slice_rle_run_exhausted_then_next_run() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b", b"c"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Two consecutive RLE runs: 20 repeats of index 0, then 10 repeats of index 2
        let encoded = encode_two_rle_runs(0, 20, 2, 10, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(30).unwrap();

        // Push exactly the first RLE run to exhaust it
        decoder.push_slice(20).unwrap();

        // Push more — forces decode_next_run, loads second RLE run
        decoder.push_slice(10).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 30);

        // First 20 should be "a"
        let a_header = expected_header(b"a", true);
        for entry in &entries[..20] {
            assert_eq!(entry[0], a_header);
        }

        // Last 10 should be "c"
        let c_header = expected_header(b"c", true);
        for entry in &entries[20..] {
            assert_eq!(entry[0], c_header);
        }
    }

    // RLE run decoded with OOB value, detected by push().
    // Uses encode_two_rle_runs: first run is valid, second has OOB value.
    #[test]
    fn test_oob_rle_run_via_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a", b"b"]; // dict_len = 2
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // First RLE run: 5 repeats of valid index 0
        // Second RLE run: 5 repeats of OOB index 5
        let encoded = encode_two_rle_runs(0, 5, 5, 5, 3);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();

        // Push 5 valid values from first RLE run
        for _ in 0..5 {
            decoder.push().unwrap();
        }

        // Next push loads second RLE run, then hits OOB
        assert!(decoder.push().is_err());
    }

    // Happy path — decode a genuine RLE run with valid values.
    #[test]
    fn test_rle_run_happy_path() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb", b"cc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Single RLE run: 20 repeats of index 1 ("bb")
        let encoded = encode_rle_run(1, 20, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(20).unwrap();
        decoder.push_slice(20).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 20);
        let bb_header = expected_header(b"bb", true);
        for entry in &entries {
            assert_eq!(entry[0], bb_header);
        }
    }

    // Consume an RLE run one value at a time via push()
    #[test]
    fn test_rle_run_push_one_by_one() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"x", b"y"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // RLE run: 10 repeats of index 0
        let encoded = encode_rle_run(0, 10, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();

        for _ in 0..10 {
            decoder.push().unwrap();
        }

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 10);
        let x_header = expected_header(b"x", true);
        for entry in &entries {
            assert_eq!(entry[0], x_header);
        }
    }

    // RLE run exhausted, then decoder has no more runs.
    #[test]
    fn test_rle_run_exhaustion() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Single RLE run: 5 repeats of index 0
        let encoded = encode_rle_run(0, 5, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(10).unwrap();

        // Consume all 5
        decoder.push_slice(5).unwrap();

        // 6th value: RLE run exhausted, decode_next_run finds no more runs
        assert!(decoder.push().is_err());
    }

    // push_slice crossing from one RLE run to another
    #[test]
    fn test_rle_run_push_slice_across_boundary() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb", b"cc"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Two RLE runs: 10 repeats of index 0, then 15 repeats of index 2
        let encoded = encode_two_rle_runs(0, 10, 2, 15, 2);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(25).unwrap();

        // push_slice that crosses the boundary between the two runs
        decoder.push_slice(25).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 25);

        let aa_header = expected_header(b"aa", true);
        let cc_header = expected_header(b"cc", true);
        for entry in &entries[..10] {
            assert_eq!(entry[0], aa_header);
        }
        for entry in &entries[10..] {
            assert_eq!(entry[0], cc_header);
        }
    }

    // skip across RLE runs
    #[test]
    fn test_rle_run_skip_then_push() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"aa", b"bb"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Two RLE runs: 10 repeats of index 0, then 10 repeats of index 1
        let encoded = encode_two_rle_runs(0, 10, 1, 10, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(5).unwrap();

        // Skip past the first run into the second
        decoder.skip(12).unwrap();

        // Push remaining from second run
        decoder.push_slice(5).unwrap();

        let entries = read_aux_entries(&buffers);
        assert_eq!(entries.len(), 5);
        let bb_header = expected_header(b"bb", true);
        for entry in &entries {
            assert_eq!(entry[0], bb_header);
        }
    }

    // skip exhausts all RLE runs
    #[test]
    fn test_rle_run_skip_exhaustion() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"a"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // Single RLE run: 5 repeats of index 0
        let encoded = encode_rle_run(0, 5, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();

        // Skip more than the run contains
        assert!(decoder.skip(10).is_err());
    }

    // RLE run with nulls interleaved
    #[test]
    fn test_rle_run_with_nulls() {
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let mut buffers = create_test_buffers(&allocator);

        let dict_strings: &[&[u8]] = &[b"val"];
        let dict_buf = build_dict_page_buffer(dict_strings);
        let dict_page = make_dict_page(&dict_buf, dict_strings.len());

        // RLE run: 10 repeats of index 0
        let encoded = encode_rle_run(0, 10, 1);

        let mut decoder =
            RleDictVarcharSliceDecoder::try_new(&encoded, &dict_page, &mut buffers, true).unwrap();
        decoder.reserve(8).unwrap();

        decoder.push_slice(3).unwrap(); // val, val, val
        decoder.push_null().unwrap(); // NULL
        decoder.push_slice(2).unwrap(); // val, val
        decoder.push_nulls(2).unwrap(); // NULL, NULL

        let entries = read_aux_entries(&buffers);
        let val_header = expected_header(b"val", true);
        let expected = vec![
            val_header,
            val_header,
            val_header,
            NULL_HEADER,
            val_header,
            val_header,
            NULL_HEADER,
            NULL_HEADER,
        ];
        assert_eq!(extract_headers(&entries), expected);
        // Verify null pointers
        assert_eq!(entries[3][1], 0);
        assert_eq!(entries[6][1], 0);
        assert_eq!(entries[7][1], 0);
    }
}
