use super::*;
use crate::parquet::error::{ParquetErrorReason, ParquetResult};
use crate::parquet::tests::ColumnTypeTagExt;
use crate::parquet_write::decimal::{
    Decimal128, Decimal16, Decimal256, Decimal32, Decimal64, Decimal8,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::{column_type_to_parquet_type, Column};
use crate::parquet_write::tests::make_column_with_top;
use crate::parquet_write::util::transmute_slice;
use parquet2::compression::CompressionOptions;
use parquet2::page::{DataPageHeader, Page};
use parquet2::schema::types::{ParquetType, PrimitiveType};
use parquet2::statistics::{BinaryStatistics, FixedLenStatistics, PrimitiveStatistics};
use parquet2::write::Version;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

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
    match column_type_to_parquet_type(0, "col", column_type, false, false).expect("type") {
        ParquetType::PrimitiveType(pt) => pt,
        _ => panic!("expected primitive type for {:?}", tag),
    }
}

fn decimal_primitive_type(precision: u8, scale: u8) -> (PrimitiveType, ColumnType) {
    let column_type = ColumnType::new_decimal(precision, scale).expect("decimal type");
    let pt = match column_type_to_parquet_type(0, "col", column_type, false, false).expect("type") {
        ParquetType::PrimitiveType(p) => p,
        _ => panic!("expected primitive"),
    };
    (pt, column_type)
}

fn data_pages(pages: &[Page]) -> Vec<&parquet2::page::DataPage> {
    pages
        .iter()
        .filter_map(|p| match p {
            Page::Data(d) => Some(d),
            _ => None,
        })
        .collect()
}

fn dict_pages(pages: &[Page]) -> Vec<&parquet2::page::DictPage> {
    pages
        .iter()
        .filter_map(|p| match p {
            Page::Dict(d) => Some(d),
            _ => None,
        })
        .collect()
}

fn page_i32_min_max(page: &parquet2::page::DataPage) -> (i32, i32) {
    let arc = page
        .statistics()
        .expect("statistics present")
        .expect("deserialized");
    let stats = arc
        .as_any()
        .downcast_ref::<PrimitiveStatistics<i32>>()
        .expect("PrimitiveStatistics<i32>");
    (stats.min_value.unwrap(), stats.max_value.unwrap())
}

fn page_v2_header(page: &parquet2::page::DataPage) -> &parquet_format_safe::DataPageHeaderV2 {
    match &page.header {
        DataPageHeader::V2(h) => h,
        DataPageHeader::V1(_) => panic!("expected V2 header"),
    }
}

#[test]
fn dict_global_uniqueness_int() {
    let parts: Vec<Vec<i32>> = vec![vec![1, 2], vec![2, 3], vec![3, 4], vec![4, 5]];
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 2, &pt, write_options(), None).expect("encode");

    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 5);
    assert_eq!(dicts[0].buffer.len(), 5 * 4);

    let bytes = &dicts[0].buffer;
    let read_i32 = |off: usize| -> i32 {
        i32::from_le_bytes([bytes[off], bytes[off + 1], bytes[off + 2], bytes[off + 3]])
    };
    assert_eq!(read_i32(0), 1);
    assert_eq!(read_i32(4), 2);
    assert_eq!(read_i32(8), 3);
    assert_eq!(read_i32(12), 4);
    assert_eq!(read_i32(16), 5);
}

#[test]
fn dict_chunk_stats_int() {
    let parts: Vec<Vec<i32>> = vec![
        (1..=10).collect(),
        (100..=110).collect(),
        (1000..=1010).collect(),
        (10_000..=10_010).collect(),
    ];
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 11, &pt, write_options(), None).expect("encode");

    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    let (min, max) = page_i32_min_max(datas[0]);
    assert_eq!((min, max), (1, 10_010));
}

#[test]
fn dict_chunk_stats_decimal64() {
    let parts: Vec<Vec<Decimal64>> = vec![
        (1..=10).map(|i| Decimal64(i * 100)).collect(),
        (100..=110).map(|i| Decimal64(i * 100)).collect(),
    ];
    let (pt, ct) = decimal_primitive_type(18, 3);
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| {
            Column::from_raw_data(
                0,
                "col",
                ct.code(),
                0,
                d.len(),
                d.as_ptr() as *const u8,
                d.len() * std::mem::size_of::<Decimal64>(),
                std::ptr::null(),
                0,
                std::ptr::null(),
                0,
                false,
                false,
                0,
            )
            .unwrap()
        })
        .collect();
    let pages =
        encode_decimal::<Decimal64>(&columns, 0, 11, &pt, write_options(), None).expect("encode");

    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    let read_stats = |page: &parquet2::page::DataPage| -> (Vec<u8>, Vec<u8>) {
        let arc = page.statistics().expect("present").expect("ok");
        let stats = arc
            .as_any()
            .downcast_ref::<FixedLenStatistics>()
            .expect("FixedLenStatistics");
        (
            stats.min_value.clone().expect("min"),
            stats.max_value.clone().expect("max"),
        )
    };
    let (min, max) = read_stats(datas[0]);
    assert_eq!(min, 100i64.to_be_bytes());
    assert_eq!(max, 11_000i64.to_be_bytes());
}

#[test]
fn dict_chunk_stats_varchar() {
    let strings1: Vec<&[u8]> = vec![b"alpha", b"beta", b"gamma"];
    let strings2: Vec<&[u8]> = vec![b"xenon", b"yankee", b"zulu"];

    let aux1: Vec<[u8; 16]> = strings1
        .iter()
        .map(|s| make_varchar_aux_inlined_test(s))
        .collect();
    let aux2: Vec<[u8; 16]> = strings2
        .iter()
        .map(|s| make_varchar_aux_inlined_test(s))
        .collect();
    let data: Vec<u8> = vec![];
    let col1 = build_varchar_column(&aux1, &data);
    let col2 = build_varchar_column(&aux2, &data);

    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let pages = encode_varchar(&[col1, col2], 0, 3, &pt, write_options(), None).expect("encode");
    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);

    let read_stats = |page: &parquet2::page::DataPage| -> (Vec<u8>, Vec<u8>) {
        let arc = page.statistics().expect("present").expect("ok");
        let stats = arc
            .as_any()
            .downcast_ref::<BinaryStatistics>()
            .expect("BinaryStatistics");
        (
            stats.min_value.clone().expect("min"),
            stats.max_value.clone().expect("max"),
        )
    };
    let (min, max) = read_stats(datas[0]);
    assert_eq!(&min, b"alpha");
    assert_eq!(&max, b"zulu");
}

#[test]
fn dict_chunk_null_counts() {
    let parts: Vec<Vec<i32>> = vec![
        vec![1, 2, 3, 4, 5],
        vec![1, i32::MIN, 2, i32::MIN, 3],
        vec![i32::MIN, i32::MIN, i32::MIN, 1, 2],
        vec![1, 2, 3, 4, i32::MIN],
    ];
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 5, &pt, write_options(), None).expect("encode");

    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_nulls, 6);
}

#[test]
fn dict_exactly_one_dict_page() {
    let parts: Vec<Vec<i32>> = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&columns, 0, 2, &pt, write_options(), None).expect("encode");
    assert_eq!(pages.len(), 2);
    assert!(matches!(pages[0], Page::Dict(_)));
    assert!(matches!(pages[1], Page::Data(_)));
}

#[test]
fn dict_bloom_hashes_from_all_partitions() {
    let parts: Vec<Vec<i32>> = vec![vec![10, 20], vec![30, 40], vec![50, 60]];
    let columns: Vec<Column> = parts
        .iter()
        .map(|d| make_column_with_top("col", ColumnTypeTag::Int, d, 0, 0))
        .collect();
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let bloom = Arc::new(Mutex::new(HashSet::new()));
    let _ = encode_simd::<i32>(&columns, 0, 2, &pt, write_options(), Some(bloom.clone()))
        .expect("encode");

    let set = bloom.lock().expect("lock");
    assert_eq!(set.len(), 6);
}

#[test]
fn dict_int_round_trip() {
    let data: Vec<i32> = vec![1, 2, 3, 1, 2, 3, i32::MIN, 1];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages =
        encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    assert!(matches!(pages[0], Page::Dict(_)));
    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_values, 8);
    assert_eq!(h.num_nulls, 1);
}

#[test]
fn dict_long_round_trip() {
    let data: Vec<i64> = vec![100, 200, 100, 300, 200];
    let col = make_column_with_top("col", ColumnTypeTag::Long, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long);
    let pages =
        encode_simd::<i64>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 3);
    assert_eq!(dicts[0].buffer.len(), 24);
}

#[test]
fn dict_double_round_trip() {
    let data: Vec<f64> = vec![1.5, 2.5, 1.5, 3.5, 2.5];
    let col = make_column_with_top("col", ColumnTypeTag::Double, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Double);
    let pages =
        encode_simd::<f64>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 3);
}

#[test]
fn dict_byte_notnull_round_trip() {
    let data: Vec<i8> = vec![1, 2, 3, 1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let pages = encode_int_notnull::<i8, i32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 3);
    let datas = data_pages(&pages);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_values, 6);
    assert_eq!(h.num_nulls, 0);
}

#[test]
fn dict_byte_notnull_with_column_top() {
    let data: Vec<i8> = vec![5, 6, 7];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 4, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let pages =
        encode_int_notnull::<i8, i32>(&[col], 0, 7, &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 4);
    let datas = data_pages(&pages);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_values, 7);
    assert_eq!(h.num_nulls, 0);
}

#[test]
fn dict_byte_notnull_null_in_slice_errors() {
    let data: Vec<i8> = vec![i8::MIN, i8::MAX, 0];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);
    let pages = encode_int_notnull::<i8, i32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 3);
}

#[test]
fn dict_decimal8_round_trip() {
    let data: Vec<Decimal8> = vec![Decimal8(1), Decimal8(2), Decimal8(1), Decimal8(3)];
    let (pt, ct) = decimal_primitive_type(2, 0);
    let col = build_decimal_column::<Decimal8>(&data, ct);
    let pages = encode_decimal::<Decimal8>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 3);
}

#[test]
fn dict_decimal16_round_trip() {
    let data: Vec<Decimal16> = vec![Decimal16(10), Decimal16(20), Decimal16(10)];
    let (pt, ct) = decimal_primitive_type(4, 1);
    let col = build_decimal_column::<Decimal16>(&data, ct);
    let pages = encode_decimal::<Decimal16>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(dict_pages(&pages)[0].num_values, 2);
}

#[test]
fn dict_decimal32_round_trip() {
    let data: Vec<Decimal32> = vec![Decimal32(100), Decimal32(200), Decimal32(100)];
    let (pt, ct) = decimal_primitive_type(9, 2);
    let col = build_decimal_column::<Decimal32>(&data, ct);
    let pages = encode_decimal::<Decimal32>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(dict_pages(&pages)[0].num_values, 2);
}

#[test]
fn dict_decimal128_round_trip() {
    let data: Vec<Decimal128> = vec![Decimal128(1, 0), Decimal128(2, 0), Decimal128(1, 0)];
    let (pt, ct) = decimal_primitive_type(30, 4);
    let col = build_decimal_column::<Decimal128>(&data, ct);
    let pages = encode_decimal::<Decimal128>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(dict_pages(&pages)[0].num_values, 2);
}

#[test]
fn dict_decimal256_round_trip() {
    let data: Vec<Decimal256> = vec![
        Decimal256(1, 0, 0, 0),
        Decimal256(2, 0, 0, 0),
        Decimal256(1, 0, 0, 0),
    ];
    let (pt, ct) = decimal_primitive_type(60, 6);
    let col = build_decimal_column::<Decimal256>(&data, ct);
    let pages = encode_decimal::<Decimal256>(&[col], 0, data.len(), &pt, write_options(), None)
        .expect("encode");
    assert_eq!(dict_pages(&pages)[0].num_values, 2);
}

#[test]
fn dict_long128_round_trip() {
    let data: Vec<[u8; 16]> = (1u128..=4u128).map(|v| v.to_le_bytes()).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Long128, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Long128);
    let pages =
        encode_fixed_len_bytes::<16>(&[col], 0, data.len(), &pt, write_options(), false, None)
            .expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 4);
    assert_eq!(dicts[0].buffer.len(), 4 * 16);
}

#[test]
fn dict_uuid_reverse_byte_order() {
    let data: Vec<[u8; 16]> = vec![1u128.to_le_bytes(), 2u128.to_le_bytes()];
    let col = make_column_with_top("col", ColumnTypeTag::Uuid, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Uuid);
    let pages =
        encode_fixed_len_bytes::<16>(&[col], 0, data.len(), &pt, write_options(), true, None)
            .expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 2);
    let mut expected_first = 1u128.to_le_bytes();
    expected_first.reverse();
    assert_eq!(&dicts[0].buffer[..16], &expected_first[..]);
}

#[test]
fn dict_empty_input_yields_empty_dict() {
    let data: Vec<i32> = vec![];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages = encode_simd::<i32>(&[col], 0, 0, &pt, write_options(), None).expect("encode");
    assert!(pages.is_empty());
}

#[test]
fn dict_single_value_partition() {
    let data: Vec<i32> = vec![42, 42, 42, 42];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages =
        encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 1);
    let (min, max) = page_i32_min_max(data_pages(&pages)[0]);
    assert_eq!((min, max), (42, 42));
}

#[test]
fn dict_keeps_single_data_page_per_chunk() {
    let data: Vec<i32> = (0..20).collect();
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let opts = WriteOptions { data_page_size: Some(64), ..write_options() };
    let pages = encode_simd::<i32>(&[col], 0, data.len(), &pt, opts, None).expect("encode");

    assert_eq!(dict_pages(&pages).len(), 1);
    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    assert_eq!(page_v2_header(datas[0]).num_values, 20);
}

#[test]
fn dict_all_nulls_partition() {
    let data: Vec<i32> = vec![i32::MIN; 10];
    let col = make_column_with_top("col", ColumnTypeTag::Int, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Int);
    let pages =
        encode_simd::<i32>(&[col], 0, data.len(), &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 0);
    let datas = data_pages(&pages);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_nulls, 10);
}

#[test]
fn dict_string_invalid_utf16_errors() {
    let mut data = Vec::new();
    data.extend_from_slice(&1i32.to_le_bytes());
    data.extend_from_slice(&0xD800u16.to_le_bytes());
    let offsets: Vec<i64> = vec![0];
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        0,
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
    let result = encode_string(&[col], 0, offsets.len(), &pt, write_options(), None);
    assert!(result.is_err());
}

#[test]
fn dict_varchar_corrupt_offset_errors() {
    let aux = vec![make_varchar_aux_split_test(b"hello world!!", 1_000_000)];
    let data: Vec<u8> = vec![];
    let col = build_varchar_column(&aux, &data);
    let pt = primitive_type_for(ColumnTypeTag::Varchar);
    let result = encode_varchar(&[col], 0, aux.len(), &pt, write_options(), None);
    assert!(result.is_err());
}

#[test]
fn dict_byte_slice_dict_buffer_layout() {
    let mut data = Vec::new();
    let mut offsets = Vec::new();
    for s in [b"abc".as_ref(), b"defgh".as_ref(), b"ij".as_ref()] {
        offsets.push(data.len() as i64);
        data.extend_from_slice(&(s.len() as i64).to_le_bytes());
        data.extend_from_slice(s);
    }
    let col = Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        0,
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
    let pages =
        encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");
    let dicts = dict_pages(&pages);
    assert_eq!(dicts[0].num_values, 3);
    let buf = &dicts[0].buffer;
    assert_eq!(&buf[0..4], &3u32.to_le_bytes());
    assert_eq!(&buf[4..7], b"abc");
    assert_eq!(&buf[7..11], &5u32.to_le_bytes());
    assert_eq!(&buf[11..16], b"defgh");
    assert_eq!(&buf[16..20], &2u32.to_le_bytes());
    assert_eq!(&buf[20..22], b"ij");
}

fn make_varchar_aux_inlined_test(value: &[u8]) -> [u8; 16] {
    assert!(value.len() <= 9);
    let mut entry = [0u8; 16];
    entry[0] = ((value.len() as u8) << 4) | 0b11;
    entry[1..1 + value.len()].copy_from_slice(value);
    entry
}

fn make_varchar_aux_split_test(value: &[u8], offset: usize) -> [u8; 16] {
    assert!(value.len() > 9);
    let mut entry = [0u8; 16];
    let header: u32 = ((value.len() as u32) << 4) | 0b10;
    entry[0..4].copy_from_slice(&header.to_le_bytes());
    entry[4..10].copy_from_slice(&value[..6]);
    entry[10..12].copy_from_slice(&(offset as u16).to_le_bytes());
    entry[12..16].copy_from_slice(&((offset >> 16) as u32).to_le_bytes());
    entry
}

fn build_varchar_column(aux: &[[u8; 16]], data: &[u8]) -> Column {
    Column::from_raw_data(
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
    .unwrap()
}

fn build_decimal_column<T>(data: &[T], ct: ColumnType) -> Column {
    Column::from_raw_data(
        0,
        "col",
        ct.code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        std::ptr::null(),
        0,
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap()
}

fn build_string_column_raw(data: &[u8], offsets: &[i64]) -> Column {
    Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::String.into_type().code(),
        0,
        offsets.len(),
        data.as_ptr(),
        data.len(),
        offsets.as_ptr() as *const u8,
        std::mem::size_of_val(offsets),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap()
}

fn build_binary_column_raw(data: &[u8], offsets: &[i64]) -> Column {
    Column::from_raw_data(
        0,
        "col",
        ColumnTypeTag::Binary.into_type().code(),
        0,
        offsets.len(),
        data.as_ptr(),
        data.len(),
        offsets.as_ptr() as *const u8,
        std::mem::size_of_val(offsets),
        std::ptr::null(),
        0,
        false,
        false,
        0,
    )
    .unwrap()
}

#[test]
fn dict_string_with_null_value() {
    // Layout: one valid UTF-16 string ("hi") followed by an entry whose
    // i32 length header is -1 (the QuestDB null sentinel).
    let mut data: Vec<u8> = Vec::new();
    let mut offsets: Vec<i64> = Vec::new();

    // Row 0: real string "hi"
    offsets.push(data.len() as i64);
    let hi_utf16: Vec<u16> = "hi".encode_utf16().collect();
    data.extend_from_slice(&(hi_utf16.len() as i32).to_le_bytes());
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            hi_utf16.as_ptr() as *const u8,
            hi_utf16.len() * std::mem::size_of::<u16>(),
        )
    };
    data.extend_from_slice(bytes);

    // Row 1: null
    offsets.push(data.len() as i64);
    data.extend_from_slice(&(-1i32).to_le_bytes());

    // Row 2: real string "yo"
    offsets.push(data.len() as i64);
    let yo_utf16: Vec<u16> = "yo".encode_utf16().collect();
    data.extend_from_slice(&(yo_utf16.len() as i32).to_le_bytes());
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(
            yo_utf16.as_ptr() as *const u8,
            yo_utf16.len() * std::mem::size_of::<u16>(),
        )
    };
    data.extend_from_slice(bytes);

    let col = build_string_column_raw(&data, &offsets);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let pages =
        encode_string(&[col], 0, offsets.len(), &pt, write_options(), None).expect("encode");

    // 1 dict page (with 2 unique non-null entries) + 1 data page (with 1 null).
    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 2);
    let datas = data_pages(&pages);
    assert_eq!(datas.len(), 1);
    let h = page_v2_header(datas[0]);
    assert_eq!(h.num_values, 3);
    assert_eq!(h.num_nulls, 1);
}

#[test]
fn dict_binary_corrupt_offset_errors() {
    // Build a Binary entry whose i64 length header decodes to a value
    // larger than the remaining data, exercising the value bounds check.
    let mut data: Vec<u8> = Vec::new();
    data.extend_from_slice(&100i64.to_le_bytes());
    data.extend_from_slice(b"abc"); // only 3 bytes after the header
    let offsets: Vec<i64> = vec![0];

    let col = build_binary_column_raw(&data, &offsets);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let err = encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None)
        .expect_err("expected error");
    assert!(
        matches!(
            err.reason(),
            crate::parquet::error::ParquetErrorReason::Layout
        ),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string().contains("invalid offset and length"),
        "got: {err}"
    );
}

#[test]
fn dict_binary_offset_header_out_of_bounds_errors() {
    // The offset points past the buffer; the header read must fail.
    let data: Vec<u8> = vec![0u8; 4];
    let offsets: Vec<i64> = vec![100]; // way out of range

    let col = build_binary_column_raw(&data, &offsets);
    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let err = encode_binary(&[col], 0, offsets.len(), &pt, write_options(), None)
        .expect_err("expected error");
    assert!(
        matches!(
            err.reason(),
            crate::parquet::error::ParquetErrorReason::Layout
        ),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(err.to_string().contains("invalid offset"), "got: {err}");
}

#[test]
fn dict_string_offset_out_of_bounds_errors() {
    // Offset past the buffer; the data slice lookup must fail.
    let data: Vec<u8> = vec![0u8; 4];
    let offsets: Vec<i64> = vec![100];

    let col = build_string_column_raw(&data, &offsets);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let err = encode_string(&[col], 0, offsets.len(), &pt, write_options(), None)
        .expect_err("expected error");
    assert!(
        matches!(
            err.reason(),
            crate::parquet::error::ParquetErrorReason::Layout
        ),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(err.to_string().contains("out of bounds"), "got: {err}");
}

#[test]
fn dict_string_negative_offset_errors() {
    // A negative i64 offset cannot be a valid index.
    let data: Vec<u8> = vec![0u8; 16];
    let offsets: Vec<i64> = vec![-1];
    let col = build_string_column_raw(&data, &offsets);
    let pt = primitive_type_for(ColumnTypeTag::String);
    let err = encode_string(&[col], 0, offsets.len(), &pt, write_options(), None)
        .expect_err("expected error");
    assert!(
        matches!(
            err.reason(),
            crate::parquet::error::ParquetErrorReason::Layout
        ),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(err.to_string().contains("invalid offset"), "got: {err}");
}

// The two tests below call the private `encode_primitive` helper directly so
// the defensive Required+null and Required+missing-default arms can be
// exercised — the public callers (encode_int_notnull) hardcode is_null=false
// and column_top_default=Some(...), so neither arm can be reached from them.

#[test]
fn encode_primitive_required_with_null_in_slice_errors() {
    let data: Vec<i8> = vec![1, 2, i8::MIN, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 0, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);

    let result = encode_primitive::<i8, i32, _, _>(
        &[col],
        0,
        data.len(),
        &pt,
        write_options(),
        None,
        Repetition::Required,
        Some(0i32),
        |column| -> ParquetResult<&[i8]> { Ok(unsafe { transmute_slice(column.primary_data) }) },
        |v: i8| v == i8::MIN,
        |v: i8| v as i32,
    );

    let err = result.expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Layout),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string().contains("null value in Required column"),
        "got: {err}"
    );
}

#[test]
fn encode_primitive_required_with_column_top_no_default_errors() {
    // Required + non-zero column_top + None default must error rather than
    // silently emit unknown bytes for the column-top rows.
    let data: Vec<i8> = vec![1, 2, 3];
    let col = make_column_with_top("col", ColumnTypeTag::Byte, &data, 4, 0);
    let pt = primitive_type_for(ColumnTypeTag::Byte);

    let result = encode_primitive::<i8, i32, _, _>(
        &[col],
        0,
        7, // 4 column-top rows + 3 data rows
        &pt,
        write_options(),
        None,
        Repetition::Required,
        None, // intentionally missing default
        |column| -> ParquetResult<&[i8]> { Ok(unsafe { transmute_slice(column.primary_data) }) },
        |_v: i8| false,
        |v: i8| v as i32,
    );

    let err = result.expect_err("expected error");
    assert!(
        matches!(err.reason(), ParquetErrorReason::Layout),
        "expected Layout, got {:?}",
        err.reason()
    );
    assert!(
        err.to_string()
            .contains("Required column has column top but no default"),
        "got: {err}"
    );
}

#[test]
fn dict_global_uniqueness_string() {
    // Two partitions with overlapping UTF-16 string values.
    // Partition 0: ["aa", "bb", "cc"]
    // Partition 1: ["bb", "cc", "dd"]
    // Global dictionary should contain 4 unique entries: aa, bb, cc, dd.
    fn build_string_data(values: &[&str]) -> (Vec<u8>, Vec<i64>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for s in values {
            offsets.push(data.len() as i64);
            let utf16: Vec<u16> = s.encode_utf16().collect();
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

    let (data0, offsets0) = build_string_data(&["aa", "bb", "cc"]);
    let (data1, offsets1) = build_string_data(&["bb", "cc", "dd"]);

    let col0 = build_string_column_raw(&data0, &offsets0);
    let col1 = build_string_column_raw(&data1, &offsets1);

    let pt = primitive_type_for(ColumnTypeTag::String);
    let pages = encode_string(&[col0, col1], 0, 3, &pt, write_options(), None).expect("encode");

    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 4);
}

#[test]
fn dict_global_uniqueness_binary() {
    // Two partitions with overlapping binary values.
    // Partition 0: [b"abc", b"def"]
    // Partition 1: [b"def", b"ghi"]
    // Global dictionary should contain 3 unique entries.
    fn build_binary_data(values: &[&[u8]]) -> (Vec<u8>, Vec<i64>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for s in values {
            offsets.push(data.len() as i64);
            data.extend_from_slice(&(s.len() as i64).to_le_bytes());
            data.extend_from_slice(s);
        }
        (data, offsets)
    }

    let (data0, offsets0) = build_binary_data(&[b"abc", b"def"]);
    let (data1, offsets1) = build_binary_data(&[b"def", b"ghi"]);

    let col0 = build_binary_column_raw(&data0, &offsets0);
    let col1 = build_binary_column_raw(&data1, &offsets1);

    let pt = primitive_type_for(ColumnTypeTag::Binary);
    let pages = encode_binary(&[col0, col1], 0, 2, &pt, write_options(), None).expect("encode");

    let dicts = dict_pages(&pages);
    assert_eq!(dicts.len(), 1);
    assert_eq!(dicts[0].num_values, 3);
}
