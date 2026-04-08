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
fn encode_simd_int_multi_partition_independent_pages() {
    let parts: Vec<Vec<i32>> = (0..4)
        .map(|p| ((p * 25)..(p * 25 + 25)).collect())
        .collect();
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 25, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 4);
    for page in &pages {
        let (num_values, num_nulls, enc) = v2_header(page);
        assert_eq!(num_values, 25);
        assert_eq!(num_nulls, 0);
        assert_eq!(enc, 0);
    }
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
fn encode_simd_page_splitting_honors_data_page_size() {
    let data: Vec<i64> = (0..1000).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let opts = WriteOptions { data_page_size: Some(256), ..write_options() };
    let pages = encode_simd::<i64>(&[col], 0, data.len(), &pt, opts, None).expect("encode");
    assert_eq!(pages.len(), 32);
    let (last_num_values, _, _) = v2_header(pages.last().unwrap());
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
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 50);
    assert_eq!(num_nulls, 50);
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
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 3);
    assert_eq!(num_nulls, 0);
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
    let (num_values, num_nulls, _) = v2_header(&pages[0]);
    assert_eq!(num_values, 5);
    assert_eq!(num_nulls, 0);
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
fn binary_to_dict_pages_round_trip() {
    // ["a", "b", "a", "c", "b"] -> 3 dict entries, 5 keys.
    let bytes = [
        b"a".as_ref(),
        b"b".as_ref(),
        b"a".as_ref(),
        b"c".as_ref(),
        b"b".as_ref(),
    ];
    let (data_buf, offsets) = make_binary_aux(&bytes);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let pages: Vec<Page> = binary_to_dict_pages(&offsets, &data_buf, 0, write_options(), pt, None)
        .expect("encode")
        .collect::<crate::parquet::error::ParquetResult<Vec<_>>>()
        .expect("collect pages");
    let dict_pages: Vec<_> = pages
        .iter()
        .filter_map(|p| if let Page::Dict(d) = p { Some(d) } else { None })
        .collect();
    let data_pages: Vec<_> = pages
        .iter()
        .filter_map(|p| if let Page::Data(d) = p { Some(d) } else { None })
        .collect();
    assert_eq!(dict_pages.len(), 1);
    assert_eq!(dict_pages[0].num_values, 3, "3 unique values in the dict");
    assert_eq!(data_pages.len(), 1);
    let h = match &data_pages[0].header {
        DataPageHeader::V2(h) => h,
        _ => panic!("expected V2 header"),
    };
    assert_eq!(h.num_values, 5);
    assert_eq!(h.num_nulls, 0);
}

#[test]
fn string_to_dict_pages_round_trip() {
    let strings = ["alpha", "beta", "alpha", "gamma", "beta"];
    let (data_buf, offsets) = make_string_aux(&strings);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let pages: Vec<Page> = string_to_dict_pages(&offsets, &data_buf, 0, write_options(), pt, None)
        .expect("encode")
        .collect::<crate::parquet::error::ParquetResult<Vec<_>>>()
        .expect("collect pages");
    let dict_count = pages.iter().filter(|p| matches!(p, Page::Dict(_))).count();
    let data_count = pages.iter().filter(|p| matches!(p, Page::Data(_))).count();
    assert_eq!(dict_count, 1);
    assert_eq!(data_count, 1);
    if let Page::Dict(d) = &pages[0] {
        assert_eq!(d.num_values, 3, "3 unique strings");
    } else {
        panic!("expected dict page first");
    }
}

#[test]
fn varchar_to_dict_pages_round_trip() {
    // Two duplicates, one inlined-null, one split, one inlined.
    let aux = vec![
        make_varchar_aux_inlined(b"x"),
        make_varchar_aux_inlined(b"x"),
        make_varchar_aux_null(),
        make_varchar_aux_split(b"hello world!!", 0),
        make_varchar_aux_inlined(b"yz"),
    ];
    let mut data = Vec::new();
    data.extend_from_slice(b"hello world!!");
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let pages: Vec<Page> = varchar_to_dict_pages(&aux, &data, 0, write_options(), pt, None)
        .expect("encode")
        .collect::<crate::parquet::error::ParquetResult<Vec<_>>>()
        .expect("collect pages");
    let dict_pages: Vec<_> = pages
        .iter()
        .filter_map(|p| if let Page::Dict(d) = p { Some(d) } else { None })
        .collect();
    let data_pages: Vec<_> = pages
        .iter()
        .filter_map(|p| if let Page::Data(d) = p { Some(d) } else { None })
        .collect();
    assert_eq!(dict_pages.len(), 1);
    // 3 unique non-null values: "x", "hello world!!", "yz"
    assert_eq!(dict_pages[0].num_values, 3);
    assert_eq!(data_pages.len(), 1);
    let h = match &data_pages[0].header {
        DataPageHeader::V2(h) => h,
        _ => panic!("expected V2 header"),
    };
    assert_eq!(h.num_values, 5);
    assert_eq!(h.num_nulls, 1);
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
