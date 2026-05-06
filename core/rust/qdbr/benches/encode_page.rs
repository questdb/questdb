use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use num_traits::AsPrimitive;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::schema::types::{
    FieldInfo, ParquetType, PhysicalType, PrimitiveLogicalType, PrimitiveType,
};
use parquet2::schema::Repetition;
use parquet2::write::Version;
use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
use questdbr::parquet_write::bench::{
    array_to_raw_page, binary_to_page, boolean_to_page, bytes_to_page, encode_column_chunk,
    int_slice_to_page_notnull, int_slice_to_page_nullable, slice_to_page_simd, string_to_page,
    symbol_to_pages, varchar_to_page, Column, WriteOptions,
};
use questdbr::parquet_write::schema::column_type_to_parquet_type;
use questdbr::parquet_write::Nullable;
use std::hint::black_box;
use std::ptr::null;

const ROW_COUNT: usize = 100_000;
const NULL_PCTS: [u8; 2] = [0, 20];

const DICT_CARDINALITIES: [usize; 4] = [10, 100, 256, 1000];
const DEFAULT_CARDINALITY: usize = 8;

const INT_ENCODINGS: [Encoding; 2] = [Encoding::Plain, Encoding::DeltaBinaryPacked];
const LEN_ENCODINGS: [Encoding; 2] = [Encoding::Plain, Encoding::DeltaLengthByteArray];

fn is_null_at(i: usize, null_pct: u8) -> bool {
    null_pct > 0 && (i % 100) < null_pct as usize
}

fn null_pcts(nullable: bool) -> &'static [u8] {
    if nullable {
        &NULL_PCTS
    } else {
        &NULL_PCTS[..1]
    }
}

#[derive(Debug, Copy, Clone)]
struct BenchGeoByte(i8);

impl Nullable for BenchGeoByte {
    fn is_null(&self) -> bool {
        self.0 == -1
    }
}

impl AsPrimitive<i32> for BenchGeoByte {
    fn as_(self) -> i32 {
        self.0 as i32
    }
}

#[derive(Debug, Copy, Clone)]
struct BenchGeoShort(i16);

impl Nullable for BenchGeoShort {
    fn is_null(&self) -> bool {
        self.0 == -1
    }
}

impl AsPrimitive<i32> for BenchGeoShort {
    fn as_(self) -> i32 {
        self.0 as i32
    }
}

#[derive(Debug, Copy, Clone)]
struct BenchGeoInt(i32);

impl Nullable for BenchGeoInt {
    fn is_null(&self) -> bool {
        self.0 == -1
    }
}

impl AsPrimitive<i32> for BenchGeoInt {
    fn as_(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Copy, Clone)]
struct BenchGeoLong(i64);

impl Nullable for BenchGeoLong {
    fn is_null(&self) -> bool {
        self.0 == -1
    }
}

impl AsPrimitive<i64> for BenchGeoLong {
    fn as_(self) -> i64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone)]
struct BenchIPv4(i32);

impl Nullable for BenchIPv4 {
    fn is_null(&self) -> bool {
        self.0 == 0
    }
}

impl AsPrimitive<i32> for BenchIPv4 {
    fn as_(self) -> i32 {
        self.0
    }
}

fn write_options() -> WriteOptions {
    WriteOptions {
        write_statistics: true,
        version: Version::V1,
        compression: CompressionOptions::Uncompressed,
        row_group_size: None,
        data_page_size: None,
        raw_array_encoding: true,
        min_compression_ratio: 0.0,
        bloom_filter_fpp: 0.01,
    }
}

fn primitive_type_for(column_type: ColumnType) -> PrimitiveType {
    let raw_array_encoding = column_type.tag() == ColumnTypeTag::Array;
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, raw_array_encoding)
            .expect("parquet type");

    match parquet_type {
        ParquetType::PrimitiveType(prim) => prim,
        ParquetType::GroupType { .. } => panic!("expected primitive type"),
    }
}

fn enc_label(encoding: Encoding) -> &'static str {
    match encoding {
        Encoding::Plain => "plain",
        Encoding::DeltaBinaryPacked => "delta_bp",
        Encoding::DeltaLengthByteArray => "delta_len",
        Encoding::RleDictionary => "dict",
        Encoding::PlainDictionary => "dict_plain",
        _ => "enc",
    }
}

fn decimal_primitive_type(
    physical_type: PhysicalType,
    precision: usize,
    scale: usize,
) -> PrimitiveType {
    PrimitiveType {
        field_info: FieldInfo {
            name: "dec_col".to_string(),
            repetition: Repetition::Optional,
            id: None,
        },
        logical_type: Some(PrimitiveLogicalType::Decimal(precision, scale)),
        converted_type: None,
        physical_type,
    }
}

fn decimal_col_type(precision: u8, scale: u8) -> ColumnType {
    ColumnType::new_decimal(precision, scale).expect("valid decimal column type")
}

// --- Data generators ---

fn make_i8_data(row_count: usize, null_pct: u8, null_value: i8) -> Vec<i8> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let base = if null_pct > 0 {
            (i % 120) as i8
        } else {
            (i as i8).wrapping_add(1)
        };
        let v = if is_null_at(i, null_pct) {
            null_value
        } else if base == null_value {
            base.wrapping_add(1)
        } else {
            base
        };
        data.push(v);
    }
    data
}

fn make_i16_data(row_count: usize, null_pct: u8, null_value: i16) -> Vec<i16> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let base = if null_pct > 0 {
            (i % 32_000) as i16
        } else {
            (i as i16).wrapping_add(1)
        };
        let v = if is_null_at(i, null_pct) {
            null_value
        } else if base == null_value {
            base.wrapping_add(1)
        } else {
            base
        };
        data.push(v);
    }
    data
}

fn make_char_data(row_count: usize) -> Vec<u16> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        data.push((i as u16).wrapping_add(1));
    }
    data
}

fn make_i32_data(row_count: usize, null_pct: u8, null_value: i32) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            i as i32
        };
        data.push(v);
    }
    data
}

fn make_i64_data(row_count: usize, null_pct: u8, null_value: i64) -> Vec<i64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            i as i64
        };
        data.push(v);
    }
    data
}

fn make_decimal_i32_data(row_count: usize, null_pct: u8) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            i32::MIN
        } else {
            ((i as i32 % 60_001) - 30_000).wrapping_mul(17)
        };
        data.push(v);
    }
    data
}

fn make_decimal_i64_data(row_count: usize, null_pct: u8) -> Vec<i64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            i64::MIN
        } else {
            ((i as i64 % 2_000_001) - 1_000_000).wrapping_mul(1_003)
        };
        data.push(v);
    }
    data
}

fn be_from_i64(value: i64, len: usize) -> Vec<u8> {
    let sign = if value < 0 { 0xFF } else { 0x00 };
    let mut out = vec![sign; len];
    let be = value.to_be_bytes();
    if len >= be.len() {
        out[len - be.len()..].copy_from_slice(&be);
    } else {
        out.copy_from_slice(&be[be.len() - len..]);
    }
    out
}

fn decimal_flba_null_value<const N: usize>() -> [u8; N] {
    let mut null_value = [0u8; N];
    let null_bytes = i64::MIN.to_le_bytes();
    for i in 0..N {
        null_value[i] = null_bytes[i % null_bytes.len()];
    }
    null_value
}

fn make_decimal_flba_data<const N: usize>(row_count: usize, null_pct: u8) -> Vec<[u8; N]> {
    let null_value = decimal_flba_null_value::<N>();
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            data.push(null_value);
        } else {
            let value = ((i as i64 % 20_001) - 10_000).wrapping_mul(7);
            let mut be: [u8; N] = be_from_i64(value, N).try_into().expect("fixed length");
            if be == null_value {
                be[0] ^= 1;
            }
            data.push(be);
        }
    }
    data
}

fn make_f32_data(row_count: usize, null_pct: u8) -> Vec<f32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            f32::NAN
        } else {
            i as f32
        };
        data.push(v);
    }
    data
}

fn make_f64_data(row_count: usize, null_pct: u8) -> Vec<f64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            f64::NAN
        } else {
            i as f64
        };
        data.push(v);
    }
    data
}

fn make_boolean_data(row_count: usize) -> Vec<u8> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        data.push((i % 2) as u8);
    }
    data
}

struct BinaryData {
    data: Vec<u8>,
    offsets: Vec<i64>,
}

fn make_binary_data(row_count: usize, null_pct: u8) -> BinaryData {
    let values: Vec<Vec<u8>> = (0..8)
        .map(|i| vec![i as u8; 8 + (i as usize % 5)])
        .collect();

    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(row_count);

    for i in 0..row_count {
        offsets.push(data.len() as i64);
        if is_null_at(i, null_pct) {
            data.extend_from_slice(&(-1i64).to_le_bytes());
        } else {
            let value = &values[i % values.len()];
            data.extend_from_slice(&(value.len() as i64).to_le_bytes());
            data.extend_from_slice(value);
        }
    }

    BinaryData { data, offsets }
}

fn make_decimal_var_data(row_count: usize, null_pct: u8, max_len: usize) -> BinaryData {
    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(row_count);
    for i in 0..row_count {
        offsets.push(data.len() as i64);
        if is_null_at(i, null_pct) {
            data.extend_from_slice(&(-1i64).to_le_bytes());
            continue;
        }
        let len = 1 + (i % max_len);
        let value = ((i as i64 % 20_001) - 10_000).wrapping_mul(13);
        let be = be_from_i64(value, len);
        data.extend_from_slice(&(be.len() as i64).to_le_bytes());
        data.extend_from_slice(&be);
    }
    BinaryData { data, offsets }
}

struct StringData {
    data: Vec<u8>,
    offsets: Vec<i64>,
}

fn make_string_data(row_count: usize, null_pct: u8) -> StringData {
    let values: Vec<String> = (0..8).map(|i| format!("str{i}")).collect();

    let mut data = Vec::new();
    let mut offsets = Vec::with_capacity(row_count);

    for i in 0..row_count {
        offsets.push(data.len() as i64);
        if is_null_at(i, null_pct) {
            data.extend_from_slice(&(-1i32).to_le_bytes());
        } else {
            let value = &values[i % values.len()];
            let utf16: Vec<u16> = value.encode_utf16().collect();
            data.extend_from_slice(&(utf16.len() as i32).to_le_bytes());
            for u in utf16 {
                data.extend_from_slice(&u.to_le_bytes());
            }
        }
    }

    StringData { data, offsets }
}

fn append_offset(aux: &mut Vec<u8>, offset: usize) {
    aux.extend_from_slice(&(offset as u16).to_le_bytes());
    aux.extend_from_slice(&((offset >> 16) as u32).to_le_bytes());
}

struct VarcharData {
    data: Vec<u8>,
    aux: Vec<[u8; 16]>,
}

fn make_varchar_data_sized(
    row_count: usize,
    null_pct: u8,
    str_len: usize,
    cardinality: usize,
) -> VarcharData {
    const HEADER_FLAG_INLINED: u8 = 1 << 0;
    const HEADER_FLAG_ASCII: u8 = 1 << 1;
    const HEADER_FLAG_ASCII_32: u32 = 1 << 1;
    const HEADER_FLAG_NULL: u8 = 1 << 2;
    const HEADER_FLAGS_WIDTH: u32 = 4;
    const VARCHAR_MAX_BYTES_FULLY_INLINED: usize = 9;
    const VARCHAR_INLINED_PREFIX_BYTES: usize = 6;

    let values: Vec<Vec<u8>> = (0..cardinality)
        .map(|i| {
            let base = format!("v{i}");
            let mut v = base.into_bytes();
            while v.len() < str_len {
                v.push(b'a' + (v.len() % 26) as u8);
            }
            v.truncate(str_len);
            v
        })
        .collect();

    let mut data = Vec::new();
    let mut aux_bytes = Vec::with_capacity(row_count * 16);

    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            aux_bytes.push(HEADER_FLAG_NULL);
            aux_bytes.extend_from_slice(&[0u8; 9]);
            append_offset(&mut aux_bytes, data.len());
            continue;
        }

        let value = &values[i % values.len()];
        let size = value.len();
        if size <= VARCHAR_MAX_BYTES_FULLY_INLINED {
            let header = ((size as u8) << HEADER_FLAGS_WIDTH as u8)
                | HEADER_FLAG_INLINED
                | HEADER_FLAG_ASCII;
            aux_bytes.push(header);
            aux_bytes.extend_from_slice(value);
            if size < VARCHAR_MAX_BYTES_FULLY_INLINED {
                let pad = VARCHAR_MAX_BYTES_FULLY_INLINED - size;
                aux_bytes.resize(aux_bytes.len() + pad, 0u8);
            }
            append_offset(&mut aux_bytes, data.len());
        } else {
            let header = ((size as u32) << HEADER_FLAGS_WIDTH) | HEADER_FLAG_ASCII_32;
            aux_bytes.extend_from_slice(&header.to_le_bytes());
            aux_bytes.extend_from_slice(&value[..VARCHAR_INLINED_PREFIX_BYTES]);
            let offset = data.len();
            data.extend_from_slice(value);
            append_offset(&mut aux_bytes, offset);
        }
    }

    let aux: Vec<[u8; 16]> = aux_bytes
        .chunks_exact(16)
        .map(|chunk| <[u8; 16]>::try_from(chunk).expect("chunk"))
        .collect();

    VarcharData { data, aux }
}

struct ArrayData {
    data: Vec<u8>,
    aux: Vec<[u8; 16]>,
}

fn make_array_data(row_count: usize, null_pct: u8) -> ArrayData {
    let values: Vec<Vec<u8>> = (0..8)
        .map(|i| vec![i as u8; 16 + (i as usize % 4) * 8])
        .collect();

    let mut data = Vec::new();
    let mut aux = Vec::with_capacity(row_count);

    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            let mut entry = [0u8; 16];
            let offset = data.len() as u64;
            let size = 0u64;
            entry[..8].copy_from_slice(&offset.to_le_bytes());
            entry[8..].copy_from_slice(&size.to_le_bytes());
            aux.push(entry);
            continue;
        }

        let value = &values[i % values.len()];
        let offset = data.len() as u64;
        let size = value.len() as u64;
        let mut entry = [0u8; 16];
        entry[..8].copy_from_slice(&offset.to_le_bytes());
        entry[8..].copy_from_slice(&size.to_le_bytes());
        aux.push(entry);
        data.extend_from_slice(value);
    }

    ArrayData { data, aux }
}

struct SymbolData {
    keys: Vec<i32>,
    offsets: Vec<u64>,
    chars: Vec<u8>,
}

fn make_symbol_data(row_count: usize, null_pct: u8) -> SymbolData {
    let values: Vec<String> = (0..16).map(|i| format!("sym{i}")).collect();

    let mut symbol_chars = Vec::new();
    let mut symbol_offsets = Vec::new();
    for value in &values {
        symbol_offsets.push(symbol_chars.len() as u64);
        let utf16: Vec<u16> = value.encode_utf16().collect();
        symbol_chars.extend_from_slice(&(utf16.len() as u32).to_le_bytes());
        for u in utf16 {
            symbol_chars.extend_from_slice(&u.to_le_bytes());
        }
    }

    let mut keys = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            keys.push(-1i32);
        } else {
            keys.push((i % values.len()) as i32);
        }
    }

    SymbolData {
        keys,
        offsets: symbol_offsets,
        chars: symbol_chars,
    }
}

fn make_long128_data(row_count: usize, null_pct: u8) -> Vec<[u8; 16]> {
    let mut null_value = [0u8; 16];
    let null_bytes = i64::MIN.to_le_bytes();
    for i in 0..16 {
        null_value[i] = null_bytes[i % null_bytes.len()];
    }

    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            data.push(null_value);
        } else {
            data.push((i as u128).to_le_bytes());
        }
    }
    data
}

fn make_long256_data(row_count: usize, null_pct: u8) -> Vec<[u8; 32]> {
    let mut null_value = [0u8; 32];
    let null_bytes = i64::MIN.to_le_bytes();
    for i in 0..32 {
        null_value[i] = null_bytes[i % null_bytes.len()];
    }

    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            data.push(null_value);
        } else {
            let mut value = [0u8; 32];
            for (j, b) in value.iter_mut().enumerate() {
                *b = (i as u8).wrapping_add(j as u8);
            }
            data.push(value);
        }
    }
    data
}

fn int96_primitive_type() -> PrimitiveType {
    PrimitiveType::from_physical("col".to_string(), PhysicalType::Int96)
}

fn make_int96_data(row_count: usize, null_pct: u8) -> Vec<[u8; 12]> {
    const JULIAN_UNIX_EPOCH: u32 = 2_440_588;

    let null_value = {
        let mut v = [0u8; 12];
        let long_bytes = i64::MIN.to_le_bytes();
        for i in 0..12 {
            v[i] = long_bytes[i % long_bytes.len()];
        }
        v
    };

    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            data.push(null_value);
        } else {
            let julian_date = JULIAN_UNIX_EPOCH + 18_000 + (i / 1000) as u32;
            let nanos_in_day = (i % 1000) as u64 * 1_000_000_000;
            let mut bytes = [0u8; 12];
            bytes[0..8].copy_from_slice(&nanos_in_day.to_le_bytes());
            bytes[8..12].copy_from_slice(&julian_date.to_le_bytes());
            if bytes == null_value {
                bytes[0] = 1;
            }
            data.push(bytes);
        }
    }
    data
}

fn make_ipv4_data(row_count: usize, null_pct: u8) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            0
        } else {
            (0x0A00_0000u32 + (i as u32 % 255) + 1) as i32
        };
        data.push(v);
    }
    data
}

// --- Dict data generators (low-cardinality data for dictionary encoding benchmarks) ---

fn make_dict_i32_data(
    row_count: usize,
    null_pct: u8,
    null_value: i32,
    cardinality: usize,
) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            (i % cardinality) as i32
        };
        data.push(v);
    }
    data
}

fn make_dict_i64_data(
    row_count: usize,
    null_pct: u8,
    null_value: i64,
    cardinality: usize,
) -> Vec<i64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            (i % cardinality) as i64
        };
        data.push(v);
    }
    data
}

fn dict_partition_shift(cardinality: usize) -> usize {
    (cardinality / 4).max(1)
}

fn make_partitioned_dict_i32_data(
    row_count: usize,
    null_pct: u8,
    null_value: i32,
    cardinality: usize,
    partition_idx: usize,
) -> Vec<i32> {
    let shift = dict_partition_shift(cardinality);
    let offset = partition_idx * shift;
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            ((i % cardinality) + offset) as i32
        };
        data.push(v);
    }
    data
}

fn make_partitioned_dict_i64_data(
    row_count: usize,
    null_pct: u8,
    null_value: i64,
    cardinality: usize,
    partition_idx: usize,
) -> Vec<i64> {
    let shift = dict_partition_shift(cardinality);
    let offset = partition_idx * shift;
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            ((i % cardinality) + offset) as i64
        };
        data.push(v);
    }
    data
}

fn make_dict_f32_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<f32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            f32::NAN
        } else {
            (i % cardinality) as f32
        };
        data.push(v);
    }
    data
}

fn make_dict_f64_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<f64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            f64::NAN
        } else {
            (i % cardinality) as f64
        };
        data.push(v);
    }
    data
}

fn make_dict_i8_data(row_count: usize, cardinality: usize) -> Vec<i8> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        data.push(((i % cardinality) as i8).wrapping_add(1));
    }
    data
}

fn make_dict_i16_data(row_count: usize, cardinality: usize) -> Vec<i16> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        data.push(((i % cardinality) as i16).wrapping_add(1));
    }
    data
}

fn make_dict_char_data(row_count: usize, cardinality: usize) -> Vec<u16> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        data.push(((i % cardinality) as u16).wrapping_add(1));
    }
    data
}

fn make_partitioned_dict_long128_data(
    row_count: usize,
    null_pct: u8,
    cardinality: usize,
    partition_idx: usize,
) -> Vec<[u8; 16]> {
    let mut null_value = [0u8; 16];
    let null_bytes = i64::MIN.to_le_bytes();
    for i in 0..16 {
        null_value[i] = null_bytes[i % null_bytes.len()];
    }
    let shift = dict_partition_shift(cardinality);
    let offset = partition_idx * shift;
    let unique_values: Vec<[u8; 16]> = (0..cardinality)
        .map(|i| ((i + offset) as u128).to_le_bytes())
        .collect();
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            data.push(null_value);
        } else {
            data.push(unique_values[i % cardinality]);
        }
    }
    data
}

fn make_dict_ipv4_data_raw(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            0
        } else {
            (0x0A00_0000u32 + (i as u32 % cardinality as u32) + 1) as i32
        };
        data.push(v);
    }
    data
}

fn make_dict_geobyte_data_raw(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i8> {
    let effective_card = cardinality.min(127);
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            -1
        } else {
            (i % effective_card) as i8
        };
        data.push(v);
    }
    data
}

fn make_dict_decimal_i32_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i32> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            i32::MIN
        } else {
            ((i % cardinality) as i32).wrapping_mul(17)
        };
        data.push(v);
    }
    data
}

fn make_dict_decimal_i64_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i64> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let v = if is_null_at(i, null_pct) {
            i64::MIN
        } else {
            ((i % cardinality) as i64).wrapping_mul(1_003)
        };
        data.push(v);
    }
    data
}

// --- Encode benchmark case ---

struct EncodeBenchCase {
    name: String,
    row_count: usize,
    encode_fn: Box<dyn Fn()>,
}

// --- Encode-specific macros ---

macro_rules! encode_simd_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $encodings:expr, |$np:ident| $data:expr) => {
        for &encoding in $encodings {
            let enc = enc_label(encoding);
            for &$np in null_pcts(true) {
                let data = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let opts = $opts;
                $cases.push(EncodeBenchCase {
                    name: format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        black_box(
                            slice_to_page_simd(&data, 0, opts, pt.clone(), encoding, None)
                                .expect("encode"),
                        );
                    }),
                });
            }
        }
    };
}

macro_rules! encode_int_notnull_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $T:ty, $U:ty, |$np:ident| $data:expr) => {
        for &encoding in &INT_ENCODINGS {
            let enc = enc_label(encoding);
            for &$np in null_pcts(false) {
                let data: Vec<$T> = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let opts = $opts;
                $cases.push(EncodeBenchCase {
                    name: format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        black_box(
                            int_slice_to_page_notnull::<$T, $U>(
                                &data,
                                0,
                                opts,
                                pt.clone(),
                                encoding,
                                None,
                            )
                            .expect("encode"),
                        );
                    }),
                });
            }
        }
    };
}

macro_rules! encode_int_nullable_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $T:ty, $U:ty, |$np:ident| $data:expr) => {
        for &encoding in &INT_ENCODINGS {
            let enc = enc_label(encoding);
            for &$np in null_pcts(true) {
                let data: Vec<$T> = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let opts = $opts;
                $cases.push(EncodeBenchCase {
                    name: format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        black_box(
                            int_slice_to_page_nullable::<$T, $U, false>(
                                &data,
                                0,
                                opts,
                                pt.clone(),
                                encoding,
                                None,
                            )
                            .expect("encode"),
                        );
                    }),
                });
            }
        }
    };
}

macro_rules! encode_bytes_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $swap:expr, |$np:ident| $data:expr) => {
        for &$np in null_pcts(true) {
            let data = $data;
            let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
            let pt = primitive_type_for(ct);
            let opts = $opts;
            $cases.push(EncodeBenchCase {
                name: format!(concat!($name, "_plain_n{null_pct}"), null_pct = $np),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        bytes_to_page(&data, $swap, 0, opts, pt.clone(), None).expect("encode"),
                    );
                }),
            });
        }
    };
}

macro_rules! encode_decimal_flba_cases {
    ($cases:ident, $opts:ident, $src_len:expr, $precision:expr, $scale:expr, $label:literal) => {{
        let primitive_type = decimal_primitive_type(
            PhysicalType::FixedLenByteArray($src_len),
            $precision,
            $scale,
        );

        for &null_pct in null_pcts(true) {
            let data = make_decimal_flba_data::<$src_len>(ROW_COUNT, null_pct);
            let pt = primitive_type.clone();
            let opts = $opts;
            $cases.push(EncodeBenchCase {
                name: format!(concat!($label, "_plain_n{null_pct}"), null_pct = null_pct),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        bytes_to_page(&data, false, 0, opts, pt.clone(), None).expect("encode"),
                    );
                }),
            });
        }
    }};
}

macro_rules! encode_simd_dict_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     |$np:ident, $card:ident| $data:expr) => {
        for &$card in &DICT_CARDINALITIES {
            for &$np in null_pcts(true) {
                let data = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let parquet_type =
                    column_type_to_parquet_type(0, "col", ct, false, false).expect("type");
                let opts = $opts;
                let row_count = data.len();
                $cases.push(EncodeBenchCase {
                    name: format!(
                        concat!($name, "_dict_c{card}_n{np}"),
                        card = $card,
                        np = $np
                    ),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        let col = make_test_column("col", ColumnTypeTag::$tag, &data);
                        let pages = encode_column_chunk(
                            Encoding::RleDictionary,
                            &parquet_type,
                            &[col],
                            0,
                            row_count,
                            opts,
                            None,
                        )
                        .expect("encode");
                        for page in pages {
                            black_box(page);
                        }
                    }),
                });
            }
        }
    };
}

macro_rules! encode_dict_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $nullable:expr, |$np:ident, $card:ident| $data:expr) => {
        for &$card in &DICT_CARDINALITIES {
            for &$np in null_pcts($nullable) {
                let data = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let parquet_type =
                    column_type_to_parquet_type(0, "col", ct, false, false).expect("type");
                let opts = $opts;
                let row_count = data.len();
                $cases.push(EncodeBenchCase {
                    name: format!(
                        concat!($name, "_dict_c{card}_n{np}"),
                        card = $card,
                        np = $np
                    ),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        let col = make_test_column("col", ColumnTypeTag::$tag, &data);
                        let pages = encode_column_chunk(
                            Encoding::RleDictionary,
                            &parquet_type,
                            &[col],
                            0,
                            row_count,
                            opts,
                            None,
                        )
                        .expect("encode");
                        for page in pages {
                            black_box(page);
                        }
                    }),
                });
            }
        }
    };
}

fn build_cases() -> Vec<EncodeBenchCase> {
    let mut cases = Vec::new();
    let options = write_options();

    // Boolean — plain encoding only
    {
        let data = make_boolean_data(ROW_COUNT);
        let column_type = ColumnType::new(ColumnTypeTag::Boolean, 0);
        let primitive_type = primitive_type_for(column_type);
        let opts = options;
        cases.push(EncodeBenchCase {
            name: "boolean_plain_n0".to_string(),
            row_count: ROW_COUNT,
            encode_fn: Box::new(move || {
                black_box(boolean_to_page(&data, 0, opts, primitive_type.clone()).expect("encode"));
            }),
        });
    }

    // Non-nullable integer types (widened via int_slice_to_page_notnull)
    encode_int_notnull_cases!(cases, options, "byte", Byte, i8, i32, |np| make_i8_data(
        ROW_COUNT, np, 0
    ));
    encode_int_notnull_cases!(cases, options, "short", Short, i16, i32, |np| {
        make_i16_data(ROW_COUNT, np, 0)
    });
    encode_int_notnull_cases!(
        cases,
        options,
        "char",
        Char,
        u16,
        i32,
        |_np| make_char_data(ROW_COUNT)
    );

    // SIMD-path types (slice_to_page_simd)
    encode_simd_cases!(cases, options, "int", Int, &INT_ENCODINGS, |np| {
        make_i32_data(ROW_COUNT, np, i32::MIN)
    });
    encode_simd_cases!(cases, options, "long", Long, &INT_ENCODINGS, |np| {
        make_i64_data(ROW_COUNT, np, i64::MIN)
    });
    encode_simd_cases!(cases, options, "date", Date, &INT_ENCODINGS, |np| {
        make_i64_data(ROW_COUNT, np, i64::MIN)
    });
    encode_simd_cases!(
        cases,
        options,
        "timestamp",
        Timestamp,
        &INT_ENCODINGS,
        |np| make_i64_data(ROW_COUNT, np, i64::MIN)
    );
    encode_simd_cases!(cases, options, "float", Float, &[Encoding::Plain], |np| {
        make_f32_data(ROW_COUNT, np)
    });
    encode_simd_cases!(cases, options, "double", Double, &[Encoding::Plain], |np| {
        make_f64_data(ROW_COUNT, np)
    });

    // Int96 timestamp (plain) — bytes_to_page
    {
        let int96_pt = int96_primitive_type();
        for &null_pct in null_pcts(true) {
            let data = make_int96_data(ROW_COUNT, null_pct);
            let pt = int96_pt.clone();
            let opts = options;
            cases.push(EncodeBenchCase {
                name: format!("timestamp_int96_plain_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        bytes_to_page(&data, false, 0, opts, pt.clone(), None).expect("encode"),
                    );
                }),
            });
        }
    }

    // Fixed-size byte array types
    encode_bytes_cases!(cases, options, "long128", Long128, false, |np| {
        make_long128_data(ROW_COUNT, np)
    });
    encode_bytes_cases!(cases, options, "uuid", Uuid, true, |np| make_long128_data(
        ROW_COUNT, np
    ));
    encode_bytes_cases!(cases, options, "long256", Long256, false, |np| {
        make_long256_data(ROW_COUNT, np)
    });

    // Nullable integer types (int_slice_to_page_nullable)
    encode_int_nullable_cases!(cases, options, "ipv4", IPv4, BenchIPv4, i32, |np| {
        make_ipv4_data(ROW_COUNT, np)
            .into_iter()
            .map(BenchIPv4)
            .collect()
    });
    encode_int_nullable_cases!(
        cases,
        options,
        "geobyte",
        GeoByte,
        BenchGeoByte,
        i32,
        |np| make_i8_data(ROW_COUNT, np, -1)
            .into_iter()
            .map(BenchGeoByte)
            .collect()
    );
    encode_int_nullable_cases!(
        cases,
        options,
        "geoshort",
        GeoShort,
        BenchGeoShort,
        i32,
        |np| make_i16_data(ROW_COUNT, np, -1)
            .into_iter()
            .map(BenchGeoShort)
            .collect()
    );
    encode_int_nullable_cases!(cases, options, "geoint", GeoInt, BenchGeoInt, i32, |np| {
        make_i32_data(ROW_COUNT, np, -1)
            .into_iter()
            .map(BenchGeoInt)
            .collect()
    });
    encode_int_nullable_cases!(
        cases,
        options,
        "geolong",
        GeoLong,
        BenchGeoLong,
        i64,
        |np| make_i64_data(ROW_COUNT, np, -1)
            .into_iter()
            .map(BenchGeoLong)
            .collect()
    );

    // String
    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_string_data(ROW_COUNT, null_pct);
            let column_type = ColumnType::new(ColumnTypeTag::String, 0);
            let primitive_type = primitive_type_for(column_type);
            cases.push(EncodeBenchCase {
                name: format!("string_{enc}_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        string_to_page(
                            &data.offsets,
                            &data.data,
                            0,
                            options,
                            primitive_type.clone(),
                            encoding,
                            None,
                        )
                        .expect("encode"),
                    );
                }),
            });
        }
    }

    // Varchar — plain and delta_len
    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &str_len in &[2usize, 200] {
            for &null_pct in null_pcts(true) {
                let data =
                    make_varchar_data_sized(ROW_COUNT, null_pct, str_len, DEFAULT_CARDINALITY);
                let column_type = ColumnType::new(ColumnTypeTag::Varchar, 0);
                let primitive_type = primitive_type_for(column_type);
                cases.push(EncodeBenchCase {
                    name: format!("varchar_{enc}_s{str_len}_n{null_pct}"),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        black_box(
                            varchar_to_page(
                                &data.aux,
                                &data.data,
                                0,
                                options,
                                primitive_type.clone(),
                                encoding,
                                None,
                            )
                            .expect("encode"),
                        );
                    }),
                });
            }
        }
    }

    // Binary
    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_binary_data(ROW_COUNT, null_pct);
            let column_type = ColumnType::new(ColumnTypeTag::Binary, 0);
            let primitive_type = primitive_type_for(column_type);
            cases.push(EncodeBenchCase {
                name: format!("binary_{enc}_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        binary_to_page(
                            &data.offsets,
                            &data.data,
                            0,
                            options,
                            primitive_type.clone(),
                            encoding,
                            None,
                        )
                        .expect("encode"),
                    );
                }),
            });
        }
    }

    // Array (raw encoding)
    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_array_data(ROW_COUNT, null_pct);
            let column_type = encode_array_type(ColumnTypeTag::Double, 1).expect("array type");
            let primitive_type = primitive_type_for(column_type);
            cases.push(EncodeBenchCase {
                name: format!("array_{enc}_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        array_to_raw_page(
                            &data.aux,
                            &data.data,
                            0,
                            options,
                            primitive_type.clone(),
                            encoding,
                        )
                        .expect("encode"),
                    );
                }),
            });
        }
    }

    // Symbol — dict page extraction
    for &null_pct in null_pcts(true) {
        let data = make_symbol_data(ROW_COUNT, null_pct);
        let column_type = ColumnType::new(ColumnTypeTag::Symbol, 0);
        let primitive_type = primitive_type_for(column_type);
        cases.push(EncodeBenchCase {
            name: format!("symbol_dict_n{null_pct}"),
            row_count: ROW_COUNT,
            encode_fn: Box::new(move || {
                let iter = symbol_to_pages(
                    &data.keys,
                    &data.offsets,
                    &data.chars,
                    0,
                    options,
                    primitive_type.clone(),
                    false,
                    None,
                )
                .expect("encode");
                for page in iter {
                    black_box(page.expect("page"));
                }
            }),
        });
    }

    // Decimal (physical Int32)
    {
        let precision = 9usize;
        let scale = 2usize;
        let primitive_type = decimal_primitive_type(PhysicalType::Int32, precision, scale);
        let column_type = decimal_col_type(precision as u8, scale as u8);
        let _ = column_type; // only needed for decode

        for &null_pct in null_pcts(true) {
            let data = make_decimal_i32_data(ROW_COUNT, null_pct);
            let pt = primitive_type.clone();
            cases.push(EncodeBenchCase {
                name: format!("decimal_int32_plain_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        int_slice_to_page_nullable::<i32, i32, false>(
                            &data,
                            0,
                            options,
                            pt.clone(),
                            Encoding::Plain,
                            None,
                        )
                        .expect("encode"),
                    );
                }),
            });
        }
    }

    // Decimal (physical Int64)
    {
        let precision = 18usize;
        let scale = 3usize;
        let primitive_type = decimal_primitive_type(PhysicalType::Int64, precision, scale);
        let column_type = decimal_col_type(precision as u8, scale as u8);
        let _ = column_type;

        for &encoding in &INT_ENCODINGS {
            let enc = enc_label(encoding);
            for &null_pct in null_pcts(true) {
                let data = make_decimal_i64_data(ROW_COUNT, null_pct);
                let pt = primitive_type.clone();
                cases.push(EncodeBenchCase {
                    name: format!("decimal_int64_{enc}_n{null_pct}"),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        black_box(
                            int_slice_to_page_nullable::<i64, i64, false>(
                                &data,
                                0,
                                options,
                                pt.clone(),
                                encoding,
                                None,
                            )
                            .expect("encode"),
                        );
                    }),
                });
            }
        }
    }

    // Decimal (physical ByteArray) -> Decimal64 / Decimal128
    for (precision, scale, label) in [(18usize, 3usize, "dec64"), (30usize, 4usize, "dec128")] {
        let primitive_type = decimal_primitive_type(PhysicalType::ByteArray, precision, scale);
        let column_type = decimal_col_type(precision as u8, scale as u8);
        let _ = column_type;

        for &null_pct in null_pcts(true) {
            let data = make_decimal_var_data(ROW_COUNT, null_pct, 12);
            let pt = primitive_type.clone();
            cases.push(EncodeBenchCase {
                name: format!("decimal_byte_array_{label}_plain_n{null_pct}"),
                row_count: ROW_COUNT,
                encode_fn: Box::new(move || {
                    black_box(
                        binary_to_page(
                            &data.offsets,
                            &data.data,
                            0,
                            options,
                            pt.clone(),
                            Encoding::Plain,
                            None,
                        )
                        .expect("encode"),
                    );
                }),
            });
        }
    }

    // Decimal (physical FixedLenByteArray) — plain only
    encode_decimal_flba_cases!(cases, options, 7, 18usize, 3usize, "decimal_flba7_dec64");
    encode_decimal_flba_cases!(cases, options, 8, 18usize, 3usize, "decimal_flba8_dec64");
    encode_decimal_flba_cases!(cases, options, 9, 18usize, 3usize, "decimal_flba9_dec64");
    encode_decimal_flba_cases!(cases, options, 15, 18usize, 3usize, "decimal_flba15_dec64");
    encode_decimal_flba_cases!(cases, options, 16, 18usize, 3usize, "decimal_flba16_dec64");

    encode_decimal_flba_cases!(cases, options, 15, 30usize, 4usize, "decimal_flba15_dec128");
    encode_decimal_flba_cases!(cases, options, 16, 30usize, 4usize, "decimal_flba16_dec128");
    encode_decimal_flba_cases!(cases, options, 17, 30usize, 4usize, "decimal_flba17_dec128");

    encode_decimal_flba_cases!(cases, options, 31, 60usize, 6usize, "decimal_flba31_dec256");
    encode_decimal_flba_cases!(cases, options, 32, 60usize, 6usize, "decimal_flba32_dec256");

    // === RLE Dictionary encoding benchmarks ===

    // SIMD types — dict
    encode_simd_dict_cases!(cases, options, "int", Int, |np, card| {
        make_dict_i32_data(ROW_COUNT, np, i32::MIN, card)
    });
    encode_simd_dict_cases!(cases, options, "long", Long, |np, card| {
        make_dict_i64_data(ROW_COUNT, np, i64::MIN, card)
    });
    encode_simd_dict_cases!(cases, options, "float", Float, |np, card| {
        make_dict_f32_data(ROW_COUNT, np, card)
    });
    encode_simd_dict_cases!(cases, options, "double", Double, |np, card| {
        make_dict_f64_data(ROW_COUNT, np, card)
    });

    // Non-nullable int types — dict
    encode_dict_cases!(cases, options, "byte", Byte, false, |_np, card| {
        make_dict_i8_data(ROW_COUNT, card)
    });
    encode_dict_cases!(cases, options, "short", Short, false, |_np, card| {
        make_dict_i16_data(ROW_COUNT, card)
    });
    encode_dict_cases!(cases, options, "char", Char, false, |_np, card| {
        make_dict_char_data(ROW_COUNT, card)
    });

    // Nullable int types — dict
    encode_dict_cases!(cases, options, "ipv4", IPv4, true, |np, card| {
        make_dict_ipv4_data_raw(ROW_COUNT, np, card)
    });
    encode_dict_cases!(cases, options, "geobyte", GeoByte, true, |np, card| {
        make_dict_geobyte_data_raw(ROW_COUNT, np, card)
    });

    // Decimal Int32 — dict
    {
        let precision = 9u8;
        let scale = 2u8;
        let column_type = decimal_col_type(precision, scale);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        for &card in &DICT_CARDINALITIES {
            for &null_pct in null_pcts(true) {
                let data = make_dict_decimal_i32_data(ROW_COUNT, null_pct, card);
                let pt = parquet_type.clone();
                let row_count = data.len();
                cases.push(EncodeBenchCase {
                    name: format!("decimal_int32_dict_c{card}_n{null_pct}"),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        let col = make_test_column_ct("col", column_type, &data);
                        let pages = encode_column_chunk(
                            Encoding::RleDictionary,
                            &pt,
                            &[col],
                            0,
                            row_count,
                            options,
                            None,
                        )
                        .expect("encode");
                        for page in pages {
                            black_box(page);
                        }
                    }),
                });
            }
        }
    }

    // Decimal Int64 — dict
    {
        let precision = 18u8;
        let scale = 3u8;
        let column_type = decimal_col_type(precision, scale);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        for &card in &DICT_CARDINALITIES {
            for &null_pct in null_pcts(true) {
                let data = make_dict_decimal_i64_data(ROW_COUNT, null_pct, card);
                let pt = parquet_type.clone();
                let row_count = data.len();
                cases.push(EncodeBenchCase {
                    name: format!("decimal_int64_dict_c{card}_n{null_pct}"),
                    row_count: ROW_COUNT,
                    encode_fn: Box::new(move || {
                        let col = make_test_column_ct("col", column_type, &data);
                        let pages = encode_column_chunk(
                            Encoding::RleDictionary,
                            &pt,
                            &[col],
                            0,
                            row_count,
                            options,
                            None,
                        )
                        .expect("encode");
                        for page in pages {
                            black_box(page);
                        }
                    }),
                });
            }
        }
    }

    // === Multi-partition RLE-dictionary cases ===
    //
    // 4 partitions × 25K rows = 100K rows total. Each partition has its own
    // overlapping 256-value window, shifted by 64 values, so the global dict
    // merge path must union partially-overlapping per-partition dictionaries.
    // The bench measures end-to-end column-chunk encoding via
    // `encode_column_chunk`.
    add_multi_partition_dict_cases(&mut cases, options);

    cases
}

fn make_test_column<T>(name: &'static str, tag: ColumnTypeTag, data: &[T]) -> Column {
    make_test_column_ct(name, ColumnType::new(tag, 0), data)
}

fn make_test_column_ct<T>(name: &'static str, column_type: ColumnType, data: &[T]) -> Column {
    Column::from_raw_data(
        0,
        name,
        column_type.code(),
        0,
        data.len(),
        data.as_ptr() as *const u8,
        std::mem::size_of_val(data),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("Column::from_raw_data")
}

fn add_multi_partition_dict_cases(cases: &mut Vec<EncodeBenchCase>, options: WriteOptions) {
    const PARTITION_COUNT: usize = 4;
    const PART_ROWS: usize = 25_000;
    const CARDINALITY: usize = 256;

    // Int dict — 4 partitions of 25K rows each, 256 unique values, 0% nulls.
    {
        let parts: Vec<Vec<i32>> = (0..PARTITION_COUNT)
            .map(|part_idx| {
                make_partitioned_dict_i32_data(PART_ROWS, 0, i32::MIN, CARDINALITY, part_idx)
            })
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::Int, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        cases.push(EncodeBenchCase {
            name: "int_dict_multi_partition_p4_n0".to_string(),
            row_count: PART_ROWS * PARTITION_COUNT,
            encode_fn: Box::new(move || {
                let columns: Vec<Column> = parts
                    .iter()
                    .map(|d| make_test_column("col", ColumnTypeTag::Int, d))
                    .collect();
                let pages = encode_column_chunk(
                    Encoding::RleDictionary,
                    &parquet_type,
                    &columns,
                    0,
                    PART_ROWS,
                    options,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // Long dict
    {
        let parts: Vec<Vec<i64>> = (0..PARTITION_COUNT)
            .map(|part_idx| {
                make_partitioned_dict_i64_data(PART_ROWS, 0, i64::MIN, CARDINALITY, part_idx)
            })
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::Long, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        cases.push(EncodeBenchCase {
            name: "long_dict_multi_partition_p4_n0".to_string(),
            row_count: PART_ROWS * PARTITION_COUNT,
            encode_fn: Box::new(move || {
                let columns: Vec<Column> = parts
                    .iter()
                    .map(|d| make_test_column("col", ColumnTypeTag::Long, d))
                    .collect();
                let pages = encode_column_chunk(
                    Encoding::RleDictionary,
                    &parquet_type,
                    &columns,
                    0,
                    PART_ROWS,
                    options,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // Long128 dict (FixedLenByteArray)
    {
        let parts: Vec<Vec<[u8; 16]>> = (0..PARTITION_COUNT)
            .map(|part_idx| make_partitioned_dict_long128_data(PART_ROWS, 0, CARDINALITY, part_idx))
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::Long128, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        cases.push(EncodeBenchCase {
            name: "long128_dict_multi_partition_p4_n0".to_string(),
            row_count: PART_ROWS * PARTITION_COUNT,
            encode_fn: Box::new(move || {
                let columns: Vec<Column> = parts
                    .iter()
                    .map(|d| make_test_column("col", ColumnTypeTag::Long128, d))
                    .collect();
                let pages = encode_column_chunk(
                    Encoding::RleDictionary,
                    &parquet_type,
                    &columns,
                    0,
                    PART_ROWS,
                    options,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // UUID dict (FixedLenByteArray with reverse=true)
    {
        let parts: Vec<Vec<[u8; 16]>> = (0..PARTITION_COUNT)
            .map(|part_idx| make_partitioned_dict_long128_data(PART_ROWS, 0, CARDINALITY, part_idx))
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::Uuid, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        cases.push(EncodeBenchCase {
            name: "uuid_dict_multi_partition_p4_n0".to_string(),
            row_count: PART_ROWS * PARTITION_COUNT,
            encode_fn: Box::new(move || {
                let columns: Vec<Column> = parts
                    .iter()
                    .map(|d| make_test_column("col", ColumnTypeTag::Uuid, d))
                    .collect();
                let pages = encode_column_chunk(
                    Encoding::RleDictionary,
                    &parquet_type,
                    &columns,
                    0,
                    PART_ROWS,
                    options,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }
}

fn add_splice_probe_cases(cases: &mut Vec<EncodeBenchCase>, options: WriteOptions) {
    const BIG_ROWS: usize = 2_000_000;

    // Nullable int Plain path — int_nullable_segments_to_page.
    {
        let data: Vec<BenchIPv4> = make_ipv4_data(BIG_ROWS, 5)
            .into_iter()
            .map(BenchIPv4)
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::IPv4, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        let opts = options;
        cases.push(EncodeBenchCase {
            name: "splice_probe_ipv4_plain_2M".to_string(),
            row_count: BIG_ROWS,
            encode_fn: Box::new(move || {
                let column = make_test_column("col", ColumnTypeTag::IPv4, &data);
                let columns = vec![column];
                let pages = encode_column_chunk(
                    Encoding::Plain,
                    &parquet_type,
                    &columns,
                    0,
                    BIG_ROWS,
                    opts,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // SIMD multi-view Plain path — simd_multi_view_page.
    // Four partitions of 500k i64 each forces the multi-view branch.
    {
        const PARTS: usize = 4;
        const PART_ROWS: usize = BIG_ROWS / PARTS;
        let parts: Vec<Vec<i64>> = (0..PARTS)
            .map(|_| make_i64_data(PART_ROWS, 5, i64::MIN))
            .collect();
        let column_type = ColumnType::new(ColumnTypeTag::Long, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        let opts = options;
        cases.push(EncodeBenchCase {
            name: "splice_probe_long_plain_2M_p4".to_string(),
            row_count: BIG_ROWS,
            encode_fn: Box::new(move || {
                let columns: Vec<Column> = parts
                    .iter()
                    .map(|d| make_test_column("col", ColumnTypeTag::Long, d))
                    .collect();
                let pages = encode_column_chunk(
                    Encoding::Plain,
                    &parquet_type,
                    &columns,
                    0,
                    PART_ROWS,
                    opts,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // Decimal Plain path — decimal_segments_to_page.
    {
        let data: Vec<i32> = make_decimal_i32_data(BIG_ROWS, 5);
        let column_type = decimal_col_type(9, 2);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        let opts = options;
        cases.push(EncodeBenchCase {
            name: "splice_probe_decimal_i32_plain_2M".to_string(),
            row_count: BIG_ROWS,
            encode_fn: Box::new(move || {
                let column = make_test_column_ct("col", column_type, &data);
                let columns = vec![column];
                let pages = encode_column_chunk(
                    Encoding::Plain,
                    &parquet_type,
                    &columns,
                    0,
                    BIG_ROWS,
                    opts,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }

    // FixedLenByteArray Plain path — fixed.rs bytes_to_page_segments.
    {
        let data: Vec<[u8; 16]> = make_long128_data(BIG_ROWS, 5);
        let column_type = ColumnType::new(ColumnTypeTag::Long128, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        let opts = options;
        cases.push(EncodeBenchCase {
            name: "splice_probe_long128_plain_2M".to_string(),
            row_count: BIG_ROWS,
            encode_fn: Box::new(move || {
                let column = make_test_column("col", ColumnTypeTag::Long128, &data);
                let columns = vec![column];
                let pages = encode_column_chunk(
                    Encoding::Plain,
                    &parquet_type,
                    &columns,
                    0,
                    BIG_ROWS,
                    opts,
                    None,
                )
                .expect("encode");
                for page in pages {
                    black_box(page);
                }
            }),
        });
    }
}

fn bench_encode_page(c: &mut Criterion) {
    let mut cases = build_cases();
    add_splice_probe_cases(&mut cases, write_options());
    let mut group = c.benchmark_group("encode_page");

    for case in cases {
        group.throughput(Throughput::Elements(case.row_count as u64));
        group.bench_function(case.name, move |b| {
            b.iter(|| (case.encode_fn)());
        });
    }

    group.finish();
}

criterion_group!(benches, bench_encode_page);
criterion_main!(benches);
