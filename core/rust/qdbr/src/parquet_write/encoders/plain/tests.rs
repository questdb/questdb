use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use super::*;
use crate::parquet::error::ParquetErrorReason;
use crate::parquet::tests::ColumnTypeTagExt;
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::column_type_to_parquet_type;
use crate::parquet_write::tests::make_column_with_top;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPageHeader, Page};
use parquet2::schema::types::{ParquetType, PrimitiveType};
use parquet2::statistics::PrimitiveStatistics;
use parquet2::write::Version;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

fn write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: Version::V2,
        compression: CompressionOptions::Uncompressed,
        row_group_size: None,
        data_page_size: None,
        raw_array_encoding: false,
        bloom_filter_fpp: 0.01,
        min_compression_ratio: 0.0,
    }
}

fn primitive_type_for(tag: ColumnTypeTag) -> PrimitiveType {
    let column_type = ColumnType::new(tag, 0);
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
    match parquet_type {
        ParquetType::PrimitiveType(pt) => pt,
        _ => panic!("expected primitive type for {:?}", tag),
    }
}

fn data_page_header(page: &Page) -> &DataPageHeader {
    match page {
        Page::Data(d) => &d.header,
        _ => panic!("expected data page"),
    }
}

fn v2_header(page: &Page) -> (i32, i32, i32) {
    match data_page_header(page) {
        DataPageHeader::V2(h) => (h.num_values, h.num_nulls, h.encoding.0),
        DataPageHeader::V1(_) => panic!("expected V2 header"),
    }
}

fn v2_header_with_def_levels(page: &Page) -> (i32, i32, i32, i32) {
    match data_page_header(page) {
        DataPageHeader::V2(h) => (
            h.num_values,
            h.num_nulls,
            h.definition_levels_byte_length,
            h.encoding.0,
        ),
        DataPageHeader::V1(_) => panic!("expected V2 header"),
    }
}

fn page_i32_min_max(page: &Page) -> (i32, i32) {
    match page {
        Page::Data(d) => {
            let arc = d.statistics().expect("statistics present").expect("ok");
            let stats = arc
                .as_any()
                .downcast_ref::<PrimitiveStatistics<i32>>()
                .expect("PrimitiveStatistics<i32>");
            (stats.min_value.unwrap(), stats.max_value.unwrap())
        }
        _ => panic!("expected data page"),
    }
}

fn page_f32_min_max(page: &Page) -> (f32, f32) {
    match page {
        Page::Data(d) => {
            let arc = d.statistics().expect("statistics present").expect("ok");
            let stats = arc
                .as_any()
                .downcast_ref::<PrimitiveStatistics<f32>>()
                .expect("PrimitiveStatistics<f32>");
            (stats.min_value.unwrap(), stats.max_value.unwrap())
        }
        _ => panic!("expected data page"),
    }
}

#[test]
fn encode_simd_int_single_partition_round_trip() {
    let data: Vec<i32> = (0..100i32)
        .map(|i| if i % 5 == 0 { i32::MIN } else { i })
        .collect();
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages =
        encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, enc) = v2_header(&pages[0]);
    assert_eq!(num_values, 100);
    assert_eq!(num_nulls, 20);
    assert_eq!(enc, 0);
}

#[test]
fn encode_simd_int_multi_partition_single_page() {
    let parts: Vec<Vec<i32>> = (0..4)
        .map(|p| ((p * 25)..(p * 25 + 25)).collect())
        .collect();
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 25, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, def_levels_len, enc) = v2_header_with_def_levels(&pages[0]);
    assert_eq!(num_values, 100);
    assert_eq!(num_nulls, 0);
    assert_eq!(
        def_levels_len, 3,
        "all-present chunks should use a single RLE run"
    );
    assert_eq!(enc, 0);
    let (min, max) = page_i32_min_max(&pages[0]);
    assert_eq!((min, max), (0, 99));
}

#[test]
fn encode_simd_long_with_column_top() {
    let data: Vec<i64> = (0..100).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 50, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let pages = encode_simd::<i64>(&[col], 0, 150, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _enc) = v2_header(&pages[0]);
    assert_eq!(num_values, 150);
    assert_eq!(num_nulls, 50, "column top rows should appear as nulls");
}

#[test]
fn encode_simd_honors_data_page_size() {
    let data: Vec<i64> = (0..1000).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let opts = WriteOptions { data_page_size: Some(256), ..write_options() };
    let pages = encode_simd::<i64>(&[col], 0, data.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 32);
    for page in &pages[..pages.len() - 1] {
        let (num_values, _, _) = v2_header(page);
        assert_eq!(num_values, 32);
    }
    let (last_num_values, _, _) = v2_header(pages.last().expect("last page"));
    assert_eq!(last_num_values, 8);
}

#[test]
fn encode_simd_empty_partition_yields_zero_pages() {
    let data: Vec<i32> = vec![];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&[col], 0, 0, &pt, write_options(), None).expect("encode");
    assert!(pages.is_empty());
}

#[test]
fn encode_simd_all_nulls_partition() {
    let data: Vec<i32> = vec![i32::MIN; 50];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&[col], 0, 50, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, def_levels_len, _) = v2_header_with_def_levels(&pages[0]);
    assert_eq!(num_values, 50);
    assert_eq!(num_nulls, 50);
    assert_eq!(
        def_levels_len, 2,
        "all-null chunks should use a single RLE run"
    );
}

#[test]
fn encode_int_notnull_byte_widening() {
    let data: Vec<i8> = vec![1, 2, 3, 4, 5, -1, -2, 127, -128];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let pages = encode_int_notnull::<i8, i32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, enc) = v2_header(&pages[0]);
    assert_eq!(num_values, 9);
    assert_eq!(num_nulls, 0);
    assert_eq!(enc, 0);
}

#[test]
fn encode_int_notnull_short_widening() {
    let data: Vec<i16> = (0..100i16).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Short, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Short);
    let pages = encode_int_notnull::<i16, i32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 100);
    assert_eq!(num_nulls, 0);
}

#[test]
fn encode_int_notnull_char_widening() {
    let data: Vec<u16> = (0..100u16).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Char, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Char);
    let pages = encode_int_notnull::<u16, i32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, _, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 100);
}

#[test]
fn encode_int_nullable_ipv4_unsigned_stats() {
    let data_raw: Vec<i32> = vec![1, 0x7FFFFFFF, -1i32, 0, 0x10];
    let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data_raw, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::IPv4);
    let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
        &[col],
        0,
        data_raw.len(),
        &pt,
        write_options(),
        None,
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 1);
}

#[test]
fn encode_int_nullable_geobyte() {
    let data_raw: Vec<i8> = vec![0, 1, 2, -1, 3, -1, 4];
    let col = make_column_with_top("col", ColumnTypeTag::GeoByte, &data_raw, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::GeoByte);
    let pages = encode_int_nullable::<crate::parquet_write::GeoByte, i32, false>(
        &[col],
        0,
        data_raw.len(),
        &pt,
        write_options(),
        None,
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 7);
    assert_eq!(num_nulls, 2);
}

#[test]
fn encode_decimal_decimal64_round_trip() {
    use crate::parquet_write::decimal::Decimal64;
    let data: Vec<Decimal64> = (0..10)
        .map(|i| Decimal64(i as i64 * 100))
        .chain(std::iter::once(Decimal64(i64::MIN)))
        .collect();
    let column_type = ColumnType::new_decimal(18, 3).expect("valid decimal");
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
    let pt = match parquet_type {
        ParquetType::PrimitiveType(p) => p,
        _ => panic!("expected primitive"),
    };
    let col = Column::from_raw_data(
        0,
        "col",
        column_type.code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        data.len() * std::mem::size_of::<Decimal64>(),
        std::ptr::null(),
        0,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pages = encode_decimal::<Decimal64>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 11);
    assert_eq!(num_nulls, 1);
}

#[test]
fn encode_boolean_basic_round_trip() {
    let data: Vec<u8> = (0..100u8).map(|i| i % 2).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Boolean, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let pages = encode_boolean(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, _, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 100);
}

#[test]
fn encode_string_utf16_round_trip() {
    let strings = ["hello", "world", "rust"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        0,
        offsets.len(),
        data_buf.as_ptr(),
        data_buf.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::String);
    let pages =
        encode_string(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, def_levels_len, _) = v2_header_with_def_levels(&pages[0]);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
    assert_eq!(def_levels_len, 2);
}

#[test]
fn encode_binary_basic_round_trip() {
    let bytes = [b"abc".as_ref(), b"defgh".as_ref(), b"ij".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        0,
        offsets.len(),
        data_buf.as_ptr(),
        data_buf.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let pages =
        encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
}

#[test]
fn encode_varchar_inlined_and_split() {
    let aux = [
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_inlined(b"abcdefghi"),
        make_varchar_aux_split(b"hello world!!", 0),
        make_varchar_aux_null(),
    ];
    let mut data = Vec::new();
    data.extend_from_slice(b"hello world!!");
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Varchar.into_type().code(),
        0,
        aux.len(),
        data.as_ptr(),
        data.len(),
        aux.as_ptr() as *const u8,
        aux.len() * 16,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let pages = encode_varchar(&[col], 0, aux.len(), &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 4);
    assert_eq!(num_nulls, 1);
}

#[test]
fn encode_fixed_len_bytes_uuid_no_reverse() {
    let data: Vec<[u8; 16]> = (1u128..=5u128).map(|v| v.to_le_bytes()).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long128, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let pages =
        encode_fixed_len_bytes::<16>(&[col], 0, data.len(), &pt, write_options(), false, None)
            .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, def_levels_len, _) = v2_header_with_def_levels(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 0);
    assert_eq!(def_levels_len, 2);
}

#[test]
fn encode_fixed_len_bytes_uuid_with_reverse() {
    let data: Vec<[u8; 16]> = (1u128..=5u128).map(|v| v.to_le_bytes()).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Uuid, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Uuid);
    let pages =
        encode_fixed_len_bytes::<16>(&[col], 0, data.len(), &pt, write_options(), true, None)
            .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 0);
}

fn make_string_aux(strings: &[&str]) -> (Vec<u8>, Vec<i64>) {
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    for s in strings {
        let utf16: Vec<u16> = s.encode_utf16().collect();
        offsets.push(data.len() as i64);
        data.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts(
                utf16.as_ptr() as *const u8,
                utf16.len() * std::mem::size_of::<u16>(),
            )
        };
        data.extend_from_slice(bytes);
    }
    (data, offsets)
}

fn make_binary_aux(slices: &[&[u8]]) -> (Vec<u8>, Vec<i64>) {
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    for s in slices {
        offsets.push(data.len() as i64);
        data.extend_from_slice(&(s.len() as i64).to_le_bytes());
        data.extend_from_slice(s);
    }
    (data, offsets)
}

fn make_varchar_aux_inlined(value: &[u8]) -> [u8; 16] {
    assert!(value.len() <= 9);
    let mut entry = [0u8; 16];
    entry[0] = ((value.len() as u8) << 4) | 0b11;
    entry[1..1 + value.len()].copy_from_slice(value);
    entry
}

fn make_varchar_aux_split(value: &[u8], offset: usize) -> [u8; 16] {
    assert!(value.len() > 9);
    let mut entry = [0u8; 16];
    let header: u32 = ((value.len() as u32) << 4) | 0b10;
    entry[0..4].copy_from_slice(&header.to_le_bytes());
    entry[4..10].copy_from_slice(&value[..6]);
    entry[10..12].copy_from_slice(&(offset as u16).to_le_bytes());
    entry[12..16].copy_from_slice(&((offset >> 16) as u32).to_le_bytes());
    entry
}

fn make_varchar_aux_null() -> [u8; 16] {
    let mut entry = [0u8; 16];
    entry[0] = 0b100;
    entry
}

// Parquet thrift Encoding enum bytes that the writer emits.
const ENC_PLAIN: i32 = 0;
const ENC_DELTA_LENGTH_BYTE_ARRAY: i32 = 6;

#[test]
fn binary_to_page_delta_round_trip() {
    let bytes = [b"abc".as_ref(), b"defgh".as_ref(), b"ij".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let page = binary_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        None,
    )
    .expect("encode");
    let (num_values, num_nulls, enc) = v2_header(&page);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
    assert_eq!(enc, ENC_DELTA_LENGTH_BYTE_ARRAY);
}

#[test]
fn string_to_page_delta_round_trip() {
    let strings = ["hello", "world", "rust"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let page = string_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        None,
    )
    .expect("encode");
    let (num_values, num_nulls, enc) = v2_header(&page);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
    assert_eq!(enc, ENC_DELTA_LENGTH_BYTE_ARRAY);
}

#[test]
fn varchar_to_page_delta_round_trip() {
    let aux = vec![
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_inlined(b"abcdefghi"),
        make_varchar_aux_split(b"hello world!!", 0),
        make_varchar_aux_null(),
    ];
    let mut data = Vec::new();
    data.extend_from_slice(b"hello world!!");
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let page = varchar_to_page(
        &aux,
        &data,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        None,
    )
    .expect("encode");
    let (num_values, num_nulls, enc) = v2_header(&page);
    assert_eq!(num_values, 4);
    assert_eq!(num_nulls, 1);
    assert_eq!(enc, ENC_DELTA_LENGTH_BYTE_ARRAY);
}

#[test]
fn binary_to_page_with_null_entries() {
    // A negative i64 length header marks the entry as null.
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    // Row 0: real value "abc"
    offsets.push(data.len() as i64);
    data.extend_from_slice(&3i64.to_le_bytes());
    data.extend_from_slice(b"abc");
    // Row 1: null
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i64).to_le_bytes());
    // Row 2: real value "z"
    offsets.push(data.len() as i64);
    data.extend_from_slice(&1i64.to_le_bytes());
    data.extend_from_slice(b"z");

    let pt = primitive_type_for(ColumnTypeTag::Binary);
    for encoding in [Encoding::Plain, Encoding::DeltaLengthByteArray] {
        let page = binary_to_page(
            &offsets,
            &data,
            0,
            write_options(),
            pt.clone(),
            encoding,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 1, "encoding {encoding:?}");
    }
}

#[test]
fn binary_to_page_negative_offset_errors() {
    // A negative offset can never be a valid index into the data buffer.
    let data = vec![0u8; 16];
    let offsets: Vec<i64> = vec![-1];
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let result = binary_to_page(
        &offsets,
        &data,
        0,
        write_options(),
        pt,
        Encoding::Plain,
        None,
    );
    let err = result.expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Layout),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string().contains("invalid offset"),
        "error should mention 'invalid offset', got: {err}"
    );
}

#[test]
fn string_to_page_truncated_header_errors() {
    // The header for a String entry is `i32` (4 bytes). If the offset
    // points within 3 bytes of the buffer end the header is truncated.
    let data = vec![0u8; 2];
    let offsets: Vec<i64> = vec![0];
    let pt = primitive_type_for(ColumnTypeTag::String);
    let err = string_to_page(
        &offsets,
        &data,
        0,
        write_options(),
        pt,
        Encoding::Plain,
        None,
    )
    .expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Layout),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string()
            .contains("not enough bytes for string header"),
        "got: {err}"
    );
}

#[test]
fn varchar_to_page_split_out_of_bounds_errors() {
    // A split varchar entry whose `offset + size` exceeds `data.len()`
    // must be rejected as a corrupt aux entry.
    let aux = vec![make_varchar_aux_split(b"hello world!!", 1_000_000)];
    let data: Vec<u8> = vec![];
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let err = varchar_to_page(&aux, &data, 0, write_options(), pt, Encoding::Plain, None)
        .expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Layout),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string().contains("data corruption in VARCHAR"),
        "got: {err}"
    );
}

#[test]
fn binary_to_page_unsupported_encoding_errors() {
    // The Plain page builder rejects encodings other than Plain and
    // DeltaLengthByteArray (the dict path lives elsewhere).
    let bytes = [b"abc".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let err = binary_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::RleDictionary,
        None,
    )
    .expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Unsupported),
        "expected Unsupported, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string().contains("unsupported encoding"),
        "got: {err}"
    );
}

#[test]
fn string_to_page_unsupported_encoding_errors() {
    let strings = ["abc"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let err = string_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::RleDictionary,
        None,
    )
    .expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Unsupported),
        "expected Unsupported, got {:?}",
        err.reason()
    );
}

#[test]
fn varchar_to_page_unsupported_encoding_errors() {
    let aux = vec![make_varchar_aux_inlined(b"abc")];
    let data: Vec<u8> = vec![];
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let err = varchar_to_page(
        &aux,
        &data,
        0,
        write_options(),
        pt,
        Encoding::RleDictionary,
        None,
    )
    .expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Unsupported),
        "expected Unsupported, got {:?}",
        err.reason()
    );
}

// Suppress dead-code warning on ENC_PLAIN; it's a documentary constant kept
// alongside ENC_DELTA_LENGTH_BYTE_ARRAY for parity.
const _: i32 = ENC_PLAIN;

#[test]
fn bytes_to_page_no_reverse_round_trip() {
    let data: Vec<[u8; 16]> = (1u128..=5u128).map(|v| v.to_le_bytes()).collect();
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let page = bytes_to_page::<16>(&data, false, 0, write_options(), pt, None).expect("encode");
    let (num_values, num_nulls, _) = v2_header(&page);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 0);
}

#[test]
fn bytes_to_page_with_reverse() {
    let data: Vec<[u8; 16]> = (1u128..=3u128).map(|v| v.to_le_bytes()).collect();
    let pt = primitive_type_for(ColumnTypeTag::Uuid);
    let page = bytes_to_page::<16>(&data, true, 0, write_options(), pt, None).expect("encode");
    let (num_values, num_nulls, _) = v2_header(&page);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
}

#[test]
fn bytes_to_page_with_column_top_and_nulls() {
    let null_value = {
        let mut v = [0u8; 16];
        let long_bytes = i64::MIN.to_le_bytes();
        for i in 0..16 {
            v[i] = long_bytes[i % long_bytes.len()];
        }
        v
    };
    let data: Vec<[u8; 16]> = vec![1u128.to_le_bytes(), null_value, 3u128.to_le_bytes()];
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let page = bytes_to_page::<16>(&data, false, 2, write_options(), pt, None).expect("encode");
    let (num_values, num_nulls, _) = v2_header(&page);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 3); // 2 column-top + 1 null sentinel
}

#[test]
fn bytes_to_page_with_bloom() {
    let data: Vec<[u8; 16]> = (1u128..=3u128).map(|v| v.to_le_bytes()).collect();
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let mut bloom = HashSet::new();
    let page =
        bytes_to_page::<16>(&data, false, 0, write_options(), pt, Some(&mut bloom)).expect("ok");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 3);
    assert_eq!(bloom.len(), 3);
}

#[test]
fn bytes_to_page_reverse_with_bloom() {
    let data: Vec<[u8; 16]> = (1u128..=2u128).map(|v| v.to_le_bytes()).collect();
    let pt = primitive_type_for(ColumnTypeTag::Uuid);
    let mut bloom = HashSet::new();
    let page =
        bytes_to_page::<16>(&data, true, 0, write_options(), pt, Some(&mut bloom)).expect("ok");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn boolean_to_page_basic_round_trip() {
    let data: Vec<u8> = vec![1, 0, 1, 1, 0];
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let page = boolean_to_page(&data, 0, write_options(), pt).expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 5);
}

#[test]
fn boolean_to_page_with_column_top() {
    let data: Vec<u8> = vec![1, 0, 1];
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let page = boolean_to_page(&data, 3, write_options(), pt).expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 6);
}

#[test]
fn boolean_to_page_no_stats() {
    let data: Vec<u8> = vec![1, 0];
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let page = boolean_to_page(&data, 0, opts, pt).expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
}

#[test]
fn encode_simd_with_bloom_filter() {
    let data: Vec<i64> = vec![1, 2, 3, i64::MIN, 5];
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_simd::<i64>(
        &[col],
        0,
        data.len(),
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 4); // 4 non-null values
}

#[test]
fn encode_simd_multi_partition_with_column_top_and_nulls() {
    // Single partition with column_top=3 and 2 data rows (one null).
    // row_count includes column_top here — the column has column_top=3 and
    // data.len()=2, but the partition has 5 logical rows.
    let data: Vec<i32> = vec![42, i32::MIN];
    // Use make_column_with_top: row_count=2, column_top=3.
    // Call with first_partition_start=0, last_partition_end=2. chunk_length=2.
    // compute_chunk_slice(3,0,2) => adjusted_top=2, lb=0, ub=0 => 2 null rows only.
    // That's the API contract: row_count is the physical data count. OK, let's
    // just exercise the column-top path in a simpler way.
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 3, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    // For a single partition, chunk_length = last_partition_end - first_partition_start.
    // We need chunk_length = column_top + data.len() = 5.
    let pages = encode_simd::<i32>(
        &[col],
        0,
        5, // last_partition_end must cover top + data
        &pt,
        write_options(),
        None,
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5); // 3 top + 2 data
    assert_eq!(num_nulls, 4); // 3 column-top + 1 null sentinel
    let (min, max) = page_i32_min_max(&pages[0]);
    assert_eq!((min, max), (42, 42)); // the sole non-null value
}

#[test]
fn encode_simd_multi_partition_with_bloom() {
    let data0: Vec<i32> = vec![1, 2, 3];
    let data1: Vec<i32> = vec![4, 5];
    let col0 = make_column_with_top("col", ColumnTypeTag::Int, &data0, 0, 0);
    let col1 = make_column_with_top("col", ColumnTypeTag::Int, &data1, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_simd::<i32>(
        &[col0, col1],
        0,
        data1.len(),
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (min, max) = page_i32_min_max(&pages[0]);
    assert_eq!((min, max), (1, 5));
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 5);
}

#[test]
fn encode_simd_float_multi_partition() {
    // Exercises the f32 SimdEncodable impl through the multi-view path
    // (which uses SimdMaxMin::new() → f32::min()/max()).
    let data0: Vec<f32> = vec![1.0, f32::NAN, 3.0];
    let data1: Vec<f32> = vec![4.0, 5.0];
    let col0 = make_column_with_top("col", ColumnTypeTag::Float, &data0, 0, 0);
    let col1 = make_column_with_top("col", ColumnTypeTag::Float, &data1, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Float);
    let pages = encode_simd::<f32>(&[col0, col1], 0, data1.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 1);
    // NaN is treated as null; stats are over the 4 non-null values.
    let (min, max) = page_f32_min_max(&pages[0]);
    assert_eq!((min, max), (1.0, 5.0));
}

/// Regression test for the `SimdMaxMin::update` bug: on strictly ascending
/// input through the multi-partition Plain path (`simd_multi_view_page`) every
/// value took the `max` branch, skipping the `else if` that updated `min`.
/// The emitted page statistics had `min = i32::MAX` and `max = 99`.
#[test]
fn encode_simd_int_multi_partition_ascending_stats() {
    let parts: Vec<Vec<i32>> = (0..4)
        .map(|p| ((p * 25)..(p * 25 + 25)).collect())
        .collect();
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 25, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (min, max) = page_i32_min_max(&pages[0]);
    assert_eq!((min, max), (0, 99));
}

/// Same regression test for the f32 branch of the multi-partition Plain path.
#[test]
fn encode_simd_float_multi_partition_ascending_stats() {
    let data0: Vec<f32> = vec![1.0, 2.0, 3.0];
    let data1: Vec<f32> = vec![4.0, 5.0];
    let col0 = make_column_with_top("col", ColumnTypeTag::Float, &data0, 0, 0);
    let col1 = make_column_with_top("col", ColumnTypeTag::Float, &data1, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Float);
    let pages = encode_simd::<f32>(&[col0, col1], 0, data1.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(pages.len(), 1);
    let (min, max) = page_f32_min_max(&pages[0]);
    assert_eq!((min, max), (1.0, 5.0));
}

#[test]
fn encode_simd_no_stats() {
    let data: Vec<i64> = vec![1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_simd::<i64>(&[col], 0, data.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_int_notnull_with_column_top() {
    let data: Vec<i8> = vec![1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 2, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    // last_partition_end = column_top + data.len() = 5
    let pages =
        encode_int_notnull::<i8, i32>(&[col], 0, 5, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, _, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5); // 2 column-top defaults + 3 data
}

#[test]
fn encode_int_notnull_with_bloom() {
    let data: Vec<i8> = vec![1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_int_notnull::<i8, i32>(
        &[col],
        0,
        data.len(),
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 3);
}

#[test]
fn encode_int_notnull_no_stats() {
    let data: Vec<i8> = vec![1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages =
        encode_int_notnull::<i8, i32>(&[col], 0, data.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_int_nullable_with_column_top_and_bloom() {
    // IPv4 null sentinel is 0, not i32::MIN
    let data: Vec<i32> = vec![1, 0, 3];
    let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data, 2, 0);
    let pt = primitive_type_for(ColumnTypeTag::IPv4);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
        &[col],
        0,
        5,
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 3); // 2 column-top + 1 null sentinel
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2); // 2 non-null values
}

#[test]
fn encode_int_nullable_no_stats() {
    let data: Vec<i32> = vec![1, i32::MIN, 3];
    let col = make_column_with_top("col", ColumnTypeTag::IPv4, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::IPv4);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
        &[col],
        0,
        data.len(),
        &pt,
        opts,
        None,
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_decimal_with_column_top_and_bloom() {
    use crate::parquet_write::decimal::Decimal64;
    let data: Vec<Decimal64> = vec![Decimal64(100), Decimal64(i64::MIN), Decimal64(300)];
    let column_type = ColumnType::new_decimal(18, 3).expect("valid decimal");
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
    let pt = match parquet_type {
        ParquetType::PrimitiveType(p) => p,
        _ => panic!("expected primitive"),
    };
    let col = Column::from_raw_data(
        0,
        "col",
        column_type.code(),
        2,
        data.len(),
        data.as_ptr() as *const u8,
        data.len() * std::mem::size_of::<Decimal64>(),
        std::ptr::null(),
        0,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages =
        encode_decimal::<Decimal64>(&[col], 0, 5, &pt, write_options(), Some(bloom.clone()))
            .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 3); // 2 column-top + 1 null
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_decimal_no_stats() {
    use crate::parquet_write::decimal::Decimal64;
    let data: Vec<Decimal64> = vec![Decimal64(100)];
    let column_type = ColumnType::new_decimal(18, 3).expect("valid decimal");
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
    let pt = match parquet_type {
        ParquetType::PrimitiveType(p) => p,
        _ => panic!("expected primitive"),
    };
    let col = Column::from_raw_data(
        0,
        "col",
        column_type.code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        data.len() * std::mem::size_of::<Decimal64>(),
        std::ptr::null(),
        0,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_decimal::<Decimal64>(&[col], 0, data.len(), &pt, opts, None).expect("ok");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_string_with_column_top_and_bloom() {
    // Single partition with column_top=2 and 2 string rows.
    let strings = ["hello", "world"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        2,
        offsets.len(),
        data_buf.as_ptr(),
        data_buf.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::String);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_string(
        &[col],
        0,
        4, // column_top(2) + data(2)
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 4); // 2 top + 2 data
    assert_eq!(num_nulls, 2);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_binary_with_bloom_and_column_top() {
    let bytes0 = [b"abc".as_ref(), b"def".as_ref()];
    let (data0, offsets0) = make_binary_aux(&bytes0);
    let col0 = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        1,
        offsets0.len(),
        data0.as_ptr(),
        data0.len(),
        offsets0.as_ptr() as *const u8,
        offsets0.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_binary(
        &[col0],
        0,
        offsets0.len() + 1,
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 3); // 1 top + 2 data
    assert_eq!(num_nulls, 1);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_varchar_with_bloom_and_column_top() {
    let aux = [
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_inlined(b"bye"),
    ];
    let data: Vec<u8> = Vec::new();
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Varchar.into_type().code(),
        1,
        aux.len(),
        data.as_ptr(),
        data.len(),
        aux.as_ptr() as *const u8,
        aux.len() * 16,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_varchar(
        &[col],
        0,
        aux.len() + 1,
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 1);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_binary_with_nulls_and_column_top() {
    // Binary column with a mix of null and non-null entries + column top.
    // This covers the null visitor branch in binary_segments_to_page.
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    // Row 0: real value "abc"
    offsets.push(data.len() as i64);
    data.extend_from_slice(&3i64.to_le_bytes());
    data.extend_from_slice(b"abc");
    // Row 1: null
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i64).to_le_bytes());
    // Row 2: real value "z"
    offsets.push(data.len() as i64);
    data.extend_from_slice(&1i64.to_le_bytes());
    data.extend_from_slice(b"z");
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        1, // column_top
        offsets.len(),
        data.as_ptr(),
        data.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_binary(
        &[col],
        0,
        4, // 1 top + 3 data
        &pt,
        write_options(),
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 4);
    assert_eq!(num_nulls, 2); // 1 column-top + 1 null entry
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_string_with_nulls_and_column_top() {
    // String column with null entries + column top.
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    // Row 0: "hi"
    offsets.push(data.len() as i64);
    let utf16: Vec<u16> = "hi".encode_utf16().collect();
    data.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            utf16.as_ptr() as *const u8,
            utf16.len() * std::mem::size_of::<u16>(),
        )
    };
    data.extend_from_slice(bytes);
    // Row 1: null (negative length)
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i32).to_le_bytes());
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        1, // column_top
        offsets.len(),
        data.as_ptr(),
        data.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::String);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages =
        encode_string(&[col], 0, 3, &pt, write_options(), Some(bloom.clone())).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 2); // 1 column-top + 1 null entry
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 1);
}

#[test]
fn encode_varchar_with_nulls_and_column_top() {
    let aux = [
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_null(),
        make_varchar_aux_inlined(b"bye"),
    ];
    let data: Vec<u8> = Vec::new();
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Varchar.into_type().code(),
        1, // column_top
        aux.len(),
        data.as_ptr(),
        data.len(),
        aux.as_ptr() as *const u8,
        aux.len() * 16,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages =
        encode_varchar(&[col], 0, 4, &pt, write_options(), Some(bloom.clone())).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 4);
    assert_eq!(num_nulls, 2); // 1 column-top + 1 null
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_string_no_stats() {
    let strings = ["hello"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        0,
        offsets.len(),
        data_buf.as_ptr(),
        data_buf.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::String);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_string(&[col], 0, offsets.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_binary_no_stats() {
    let bytes = [b"abc".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        0,
        offsets.len(),
        data_buf.as_ptr(),
        data_buf.len(),
        offsets.as_ptr() as *const u8,
        offsets.len() * std::mem::size_of::<i64>(),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_binary(&[col], 0, offsets.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_varchar_no_stats() {
    let aux = [make_varchar_aux_inlined(b"hi")];
    let data: Vec<u8> = Vec::new();
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Varchar.into_type().code(),
        0,
        aux.len(),
        data.as_ptr(),
        data.len(),
        aux.as_ptr() as *const u8,
        aux.len() * 16,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_varchar(&[col], 0, aux.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}

#[test]
fn string_to_page_plain_round_trip() {
    let strings = ["hello", "world"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let mut bloom = HashSet::new();
    let page = string_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::Plain,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, num_nulls, enc) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(num_nulls, 0);
    assert_eq!(enc, ENC_PLAIN);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn varchar_to_page_plain_round_trip() {
    let aux = vec![
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_inlined(b"bye"),
    ];
    let data: Vec<u8> = Vec::new();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let mut bloom = HashSet::new();
    let page = varchar_to_page(
        &aux,
        &data,
        0,
        write_options(),
        pt,
        Encoding::Plain,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, num_nulls, enc) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(num_nulls, 0);
    assert_eq!(enc, ENC_PLAIN);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn binary_to_page_plain_with_bloom() {
    let bytes = [b"abc".as_ref(), b"def".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let mut bloom = HashSet::new();
    let page = binary_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::Plain,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn binary_to_page_delta_all_nulls() {
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    // Two null entries
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i64).to_le_bytes());
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i64).to_le_bytes());
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let page = binary_to_page(
        &offsets,
        &data,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        None,
    )
    .expect("encode");
    let (num_values, num_nulls, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(num_nulls, 2);
}

#[test]
fn binary_to_page_delta_with_bloom() {
    let bytes = [b"abc".as_ref(), b"def".as_ref()];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let mut bloom = HashSet::new();
    let page = binary_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn string_to_page_delta_with_bloom() {
    let strings = ["hello", "world"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let mut bloom = HashSet::new();
    let page = string_to_page(
        &offsets,
        &data_buf,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn varchar_to_page_delta_with_bloom() {
    let aux = vec![
        make_varchar_aux_inlined(b"hi"),
        make_varchar_aux_inlined(b"bye"),
    ];
    let data: Vec<u8> = Vec::new();
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let mut bloom = HashSet::new();
    let page = varchar_to_page(
        &aux,
        &data,
        0,
        write_options(),
        pt,
        Encoding::DeltaLengthByteArray,
        Some(&mut bloom),
    )
    .expect("encode");
    let (num_values, _, _) = v2_header(&page);
    assert_eq!(num_values, 2);
    assert_eq!(bloom.len(), 2);
}

#[test]
fn encode_fixed_len_bytes_with_column_top_and_bloom() {
    let data: Vec<[u8; 16]> = (1u128..=3u128).map(|v| v.to_le_bytes()).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long128, &data, 2, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_fixed_len_bytes::<16>(
        &[col],
        0,
        5,
        &pt,
        write_options(),
        false,
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 2);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 3);
}

#[test]
fn encode_fixed_len_bytes_reverse_with_bloom() {
    let data: Vec<[u8; 16]> = (1u128..=2u128).map(|v| v.to_le_bytes()).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Uuid, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Uuid);
    let bloom = Arc::new(Mutex::new(HashSet::<u64>::new()));
    let pages = encode_fixed_len_bytes::<16>(
        &[col],
        0,
        data.len(),
        &pt,
        write_options(),
        true,
        Some(bloom.clone()),
    )
    .expect("encode");
    assert_eq!(pages.len(), 1);
    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 2);
}

#[test]
fn encode_fixed_len_bytes_no_stats() {
    let data: Vec<[u8; 16]> = vec![1u128.to_le_bytes()];
    let col = make_column_with_top("col", ColumnTypeTag::Long128, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages =
        encode_fixed_len_bytes::<16>(&[col], 0, data.len(), &pt, opts, false, None).expect("ok");
    assert_eq!(pages.len(), 1);
}

#[test]
fn encode_boolean_with_column_top() {
    let data: Vec<u8> = vec![1, 0, 1];
    let col = make_column_with_top("col", ColumnTypeTag::Boolean, &data, 3, 0);
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let pages = encode_boolean(&[col], 0, 6, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 1);
    let (num_values, _, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 6);
}

#[test]
fn encode_boolean_no_stats() {
    let data: Vec<u8> = vec![1, 0];
    let col = make_column_with_top("col", ColumnTypeTag::Boolean, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Boolean);
    let opts = WriteOptions { write_statistics: false, ..write_options() };
    let pages = encode_boolean(&[col], 0, data.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 1);
}
