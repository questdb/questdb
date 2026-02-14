use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use num_traits::AsPrimitive;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::hybrid_rle::{encode_bool, encode_u32};
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DictPage, Page};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::statistics::{serialize_statistics, PrimitiveStatistics};
use parquet2::types::NativeType;
use parquet2::write::Version;
use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
use questdbr::allocator::{MemTracking, QdbAllocator};
use questdbr::parquet::{QdbMetaCol, QdbMetaColFormat};
use questdbr::parquet_read::decode::decode_page;
use questdbr::parquet_read::page;
use questdbr::parquet_read::ColumnChunkBuffers;
use questdbr::parquet_write::bench::{
    array_to_raw_page, binary_to_page, boolean_to_page, bytes_to_page, int_slice_to_page_notnull,
    int_slice_to_page_nullable, slice_to_page_simd, string_to_page, symbol_to_pages,
    varchar_to_page, WriteOptions,
};
use questdbr::parquet_write::schema::column_type_to_parquet_type;
use questdbr::parquet_write::Nullable;
use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;

const ROW_COUNT: usize = 100_000;
const NULL_PCTS: [u8; 2] = [0, 20];
const DICT_CARDINALITIES: [usize; 4] = [10, 100, 256, 1000];

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

struct BenchAllocator {
    _mem_tracking: Box<MemTracking>,
    _tagged_used: Box<AtomicUsize>,
    allocator: QdbAllocator,
}

impl BenchAllocator {
    fn new() -> Self {
        let mem_tracking = Box::new(MemTracking::new());
        let tagged_used = Box::new(AtomicUsize::new(0));
        let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);
        Self {
            _mem_tracking: mem_tracking,
            _tagged_used: tagged_used,
            allocator,
        }
    }

    fn allocator(&self) -> QdbAllocator {
        self.allocator.clone()
    }
}

struct BenchCase {
    name: String,
    page: DataPage,
    dict: Option<DictPage>,
    col_info: QdbMetaCol,
    row_count: usize,
}

struct DecodeBenchState {
    _alloc: BenchAllocator,
    bufs: ColumnChunkBuffers,
}

impl DecodeBenchState {
    fn new() -> Self {
        let alloc = BenchAllocator::new();
        let bufs = ColumnChunkBuffers::new(alloc.allocator());
        Self {
            _alloc: alloc,
            bufs,
        }
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
    }
}

fn primitive_type_for(column_type: ColumnType) -> PrimitiveType {
    let raw_array_encoding = column_type.tag() == ColumnTypeTag::Array;
    let parquet_type =
        column_type_to_parquet_type(0, "col", column_type, false, false, raw_array_encoding)
            .expect("parquet type");

    match parquet_type {
        ParquetType::PrimitiveType(prim) => prim,
        ParquetType::GroupType { .. } => panic!("expected primitive type"),
    }
}

fn data_page_from(page: Page) -> DataPage {
    match page {
        Page::Data(page) => page,
        Page::Dict(_) => panic!("unexpected dict page"),
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

fn build_case(
    name: String,
    page: DataPage,
    dict: Option<DictPage>,
    column_type: ColumnType,
    format: Option<QdbMetaColFormat>,
    row_count: usize,
) -> BenchCase {
    BenchCase {
        name,
        page,
        dict,
        col_info: QdbMetaCol {
            column_type,
            column_top: 0,
            format,
        },
        row_count,
    }
}

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
        } else {
            if base == null_value {
                base.wrapping_add(1)
            } else {
                base
            }
        };
        data.push(v);
    }
    data
}

fn make_i16_data(row_count: usize, null_pct: u8, null_value: i16) -> Vec<i16> {
    let mut data = Vec::with_capacity(row_count);
    for i in 0..row_count {
        let base = if null_pct > 0 {
            (i % 32000) as i16
        } else {
            (i as i16).wrapping_add(1)
        };
        let v = if is_null_at(i, null_pct) {
            null_value
        } else {
            if base == null_value {
                base.wrapping_add(1)
            } else {
                base
            }
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

fn boolean_to_rle_data_page(
    slice: &[u8],
    column_top: usize,
    primitive_type: PrimitiveType,
) -> DataPage {
    let num_rows = column_top + slice.len();
    let mut data_buffer = vec![0u8; 4];
    let payload_start = data_buffer.len();

    let iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            slice[i - column_top] != 0
        }
    });
    encode_bool(&mut data_buffer, iter, num_rows).expect("encode boolean rle");

    let payload_len = (data_buffer.len() - payload_start) as i32;
    data_buffer[..4].copy_from_slice(&payload_len.to_le_bytes());

    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: num_rows as i32,
        encoding: Encoding::Rle.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    });

    DataPage::new(
        header,
        data_buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    )
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

fn make_varchar_data(row_count: usize, null_pct: u8) -> VarcharData {
    const HEADER_FLAG_INLINED: u8 = 1 << 0;
    const HEADER_FLAG_ASCII: u8 = 1 << 1;
    const HEADER_FLAG_NULL: u8 = 1 << 2;
    const HEADER_FLAGS_WIDTH: u8 = 4;
    const VARCHAR_MAX_BYTES_FULLY_INLINED: usize = 9;

    let values: Vec<Vec<u8>> = (0..8).map(|i| format!("v{i}").into_bytes()).collect();

    let data = Vec::new();
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
        debug_assert!(size <= VARCHAR_MAX_BYTES_FULLY_INLINED);
        let header = ((size as u8) << HEADER_FLAGS_WIDTH) | HEADER_FLAG_INLINED | HEADER_FLAG_ASCII;
        aux_bytes.push(header);
        aux_bytes.extend_from_slice(value);
        if size < VARCHAR_MAX_BYTES_FULLY_INLINED {
            let pad = VARCHAR_MAX_BYTES_FULLY_INLINED - size;
            aux_bytes.resize(aux_bytes.len() + pad, 0u8);
        }
        append_offset(&mut aux_bytes, data.len());
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
            let julian_date = JULIAN_UNIX_EPOCH + 18000 + (i / 1000) as u32;
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

fn make_int96_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<[u32; 3]> {
    const JULIAN_UNIX_EPOCH: u32 = 2_440_588;

    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                [0u32; 3]
            } else {
                let j = i % cardinality;
                let julian_date = JULIAN_UNIX_EPOCH + 18000 + (j / 1000) as u32;
                let nanos_in_day = (j % 1000) as u64 * 1_000_000_000;
                [
                    nanos_in_day as u32,
                    (nanos_in_day >> 32) as u32,
                    julian_date,
                ]
            }
        })
        .collect()
}

fn make_byte_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i32> {
    let card = cardinality.min(120);
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                0i32
            } else {
                ((i % card) + 1) as i32
            }
        })
        .collect()
}

fn make_short_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i32> {
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                0i32
            } else {
                ((i % cardinality) + 1) as i32
            }
        })
        .collect()
}

fn make_i32_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i32> {
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                i32::MIN
            } else {
                (i % cardinality) as i32
            }
        })
        .collect()
}

fn make_i64_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<i64> {
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                i64::MIN
            } else {
                (i % cardinality) as i64
            }
        })
        .collect()
}

fn make_f32_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<f32> {
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                f32::NAN
            } else {
                (i % cardinality) as f32
            }
        })
        .collect()
}

fn make_f64_dict_data(row_count: usize, null_pct: u8, cardinality: usize) -> Vec<f64> {
    (0..row_count)
        .map(|i| {
            if is_null_at(i, null_pct) {
                f64::NAN
            } else {
                (i % cardinality) as f64
            }
        })
        .collect()
}

fn dict_bit_width(max: u64) -> u8 {
    (64 - max.leading_zeros()) as u8
}

/// Build RLE dictionary-encoded DataPage + DictPage for fixed-size values.
///
/// Null rows (determined by `null_pct` and `is_null_at`) are excluded from the
/// dictionary and encoded via definition levels. Statistics (min/max/null_count)
/// are computed from the typed values.
fn build_fixed_rle_dict_pages<T: NativeType>(
    data: &[T],
    null_pct: u8,
    row_count: usize,
    primitive_type: PrimitiveType,
) -> (DataPage, DictPage) {
    let elem_size = std::mem::size_of::<T>();
    let raw_data: &[u8] =
        unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * elem_size) };

    let mut dict_map: HashMap<&[u8], u32> = HashMap::new();
    let mut dict_entries: Vec<&[u8]> = Vec::new();
    let mut indices: Vec<u32> = Vec::new();
    let mut null_count = 0usize;
    let mut min_value: Option<T> = None;
    let mut max_value: Option<T> = None;

    for i in 0..row_count {
        if is_null_at(i, null_pct) {
            null_count += 1;
            continue;
        }

        let val = data[i];
        min_value = Some(match min_value {
            Some(m) => {
                if val.ord(&m) == std::cmp::Ordering::Less {
                    val
                } else {
                    m
                }
            }
            None => val,
        });
        max_value = Some(match max_value {
            Some(m) => {
                if val.ord(&m) == std::cmp::Ordering::Greater {
                    val
                } else {
                    m
                }
            }
            None => val,
        });

        let start = i * elem_size;
        let raw_val = &raw_data[start..start + elem_size];
        let idx = match dict_map.get(raw_val) {
            Some(&idx) => idx,
            None => {
                let idx = dict_entries.len() as u32;
                dict_map.insert(raw_val, idx);
                dict_entries.push(raw_val);
                idx
            }
        };
        indices.push(idx);
    }

    let mut dict_buffer = Vec::with_capacity(dict_entries.len() * elem_size);
    for entry in &dict_entries {
        dict_buffer.extend_from_slice(entry);
    }

    let max_key = if dict_entries.is_empty() {
        0u32
    } else {
        dict_entries.len() as u32 - 1
    };
    let bits_per_key = dict_bit_width(max_key as u64);

    // Build data page: definition levels (V1) + RLE-encoded indices
    let mut data_buffer = Vec::new();

    // Definition levels with V1 length prefix
    data_buffer.extend_from_slice(&[0u8; 4]);
    let dl_start = data_buffer.len();
    let def_levels = (0..row_count).map(|i| !is_null_at(i, null_pct));
    encode_bool(&mut data_buffer, def_levels, row_count).unwrap();
    let dl_len = (data_buffer.len() - dl_start) as i32;
    data_buffer[dl_start - 4..dl_start].copy_from_slice(&dl_len.to_le_bytes());

    // RLE-encoded dictionary indices
    let non_null_len = indices.len();
    data_buffer.push(bits_per_key);
    encode_u32(
        &mut data_buffer,
        indices.into_iter(),
        non_null_len,
        bits_per_key as u32,
    )
    .unwrap();

    let statistics = serialize_statistics(
        &(PrimitiveStatistics::<T> {
            primitive_type: primitive_type.clone(),
            null_count: Some(null_count as i64),
            distinct_count: None,
            min_value,
            max_value,
        }),
    );

    let header = DataPageHeader::V1(DataPageHeaderV1 {
        num_values: row_count as i32,
        encoding: Encoding::RleDictionary.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: Some(statistics),
    });
    let data_page = DataPage::new(
        header,
        data_buffer,
        Descriptor {
            primitive_type,
            max_def_level: 1,
            max_rep_level: 0,
        },
        Some(row_count),
    );

    let dict_page = DictPage::new(dict_buffer, dict_entries.len(), false);

    (data_page, dict_page)
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

macro_rules! simd_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $encodings:expr, |$np:ident| $data:expr) => {
        for &encoding in $encodings {
            let enc = enc_label(encoding);
            for &$np in null_pcts(true) {
                let data = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let page = data_page_from(
                    slice_to_page_simd(&data, 0, $opts, pt, encoding).expect("page"),
                );
                $cases.push(build_case(
                    format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    page,
                    None,
                    ct,
                    None,
                    ROW_COUNT,
                ));
            }
        }
    };
}

macro_rules! int_notnull_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $T:ty, $U:ty, |$np:ident| $data:expr) => {
        for &encoding in &INT_ENCODINGS {
            let enc = enc_label(encoding);
            for &$np in null_pcts(false) {
                let data: Vec<$T> = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let page = data_page_from(
                    int_slice_to_page_notnull::<$T, $U>(&data, 0, $opts, pt, encoding)
                        .expect("page"),
                );
                $cases.push(build_case(
                    format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    page,
                    None,
                    ct,
                    None,
                    ROW_COUNT,
                ));
            }
        }
    };
}

macro_rules! int_nullable_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $T:ty, $U:ty, |$np:ident| $data:expr) => {
        for &encoding in &INT_ENCODINGS {
            let enc = enc_label(encoding);
            for &$np in null_pcts(true) {
                let data: Vec<$T> = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let page = data_page_from(
                    int_slice_to_page_nullable::<$T, $U>(&data, 0, $opts, pt, encoding)
                        .expect("page"),
                );
                $cases.push(build_case(
                    format!(
                        concat!($name, "_{enc}_n{null_pct}"),
                        enc = enc,
                        null_pct = $np
                    ),
                    page,
                    None,
                    ct,
                    None,
                    ROW_COUNT,
                ));
            }
        }
    };
}

macro_rules! bytes_cases {
    ($cases:ident, $opts:ident, $name:literal, $tag:ident,
     $swap:expr, |$np:ident| $data:expr) => {
        for &$np in null_pcts(true) {
            let data = $data;
            let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
            let pt = primitive_type_for(ct);
            let page = data_page_from(bytes_to_page(&data, $swap, 0, $opts, pt).expect("page"));
            $cases.push(build_case(
                format!(concat!($name, "_plain_n{null_pct}"), null_pct = $np),
                page,
                None,
                ct,
                None,
                ROW_COUNT,
            ));
        }
    };
}

macro_rules! fixed_dict_cases {
    ($cases:ident, $name:literal, $tag:ident, $T:ty,
     |$np:ident, $card:ident| $data:expr) => {
        for &$card in &DICT_CARDINALITIES {
            for &$np in null_pcts(true) {
                let data: Vec<$T> = $data;
                let ct = ColumnType::new(ColumnTypeTag::$tag, 0);
                let pt = primitive_type_for(ct);
                let (page, dict) = build_fixed_rle_dict_pages(&data, $np, ROW_COUNT, pt);
                $cases.push(build_case(
                    format!(
                        concat!($name, "_dict_c{card}_n{null_pct}"),
                        card = $card,
                        null_pct = $np,
                    ),
                    page,
                    Some(dict),
                    ct,
                    None,
                    ROW_COUNT,
                ));
            }
        }
    };
}

fn build_cases() -> Vec<BenchCase> {
    let mut cases = Vec::new();
    let options = write_options();

    // Boolean — unique page function, not worth a macro
    for &null_pct in null_pcts(false) {
        let data = make_boolean_data(ROW_COUNT);
        let column_type = ColumnType::new(ColumnTypeTag::Boolean, 0);
        let primitive_type = primitive_type_for(column_type);
        let page = data_page_from(
            boolean_to_page(&data, 0, options, primitive_type.clone()).expect("page"),
        );
        cases.push(build_case(
            format!("boolean_plain_n{null_pct}"),
            page,
            None,
            column_type,
            None,
            ROW_COUNT,
        ));

        let rle_page = boolean_to_rle_data_page(&data, 0, primitive_type);
        cases.push(build_case(
            format!("boolean_rle_n{null_pct}"),
            rle_page,
            None,
            column_type,
            None,
            ROW_COUNT,
        ));
    }

    // Non-nullable integer types (widened via int_slice_to_page_notnull)
    int_notnull_cases!(cases, options, "byte", Byte, i8, i32, |np| make_i8_data(
        ROW_COUNT, np, 0
    ));
    int_notnull_cases!(
        cases,
        options,
        "short",
        Short,
        i16,
        i32,
        |np| make_i16_data(ROW_COUNT, np, 0)
    );
    int_notnull_cases!(
        cases,
        options,
        "char",
        Char,
        u16,
        i32,
        |_np| make_char_data(ROW_COUNT)
    );

    // SIMD-path types (slice_to_page_simd)
    simd_cases!(cases, options, "int", Int, &INT_ENCODINGS, |np| {
        make_i32_data(ROW_COUNT, np, i32::MIN)
    });
    simd_cases!(cases, options, "long", Long, &INT_ENCODINGS, |np| {
        make_i64_data(ROW_COUNT, np, i64::MIN)
    });
    simd_cases!(cases, options, "date", Date, &INT_ENCODINGS, |np| {
        make_i64_data(ROW_COUNT, np, i64::MIN)
    });
    simd_cases!(
        cases,
        options,
        "timestamp",
        Timestamp,
        &INT_ENCODINGS,
        |np| make_i64_data(ROW_COUNT, np, i64::MIN)
    );
    simd_cases!(cases, options, "float", Float, &[Encoding::Plain], |np| {
        make_f32_data(ROW_COUNT, np)
    });
    simd_cases!(cases, options, "double", Double, &[Encoding::Plain], |np| {
        make_f64_data(ROW_COUNT, np)
    });

    // Int96 timestamp (plain) — external Parquet files may use Int96 physical type
    {
        let int96_pt = int96_primitive_type();
        for &null_pct in null_pcts(true) {
            let data = make_int96_data(ROW_COUNT, null_pct);
            let page = data_page_from(
                bytes_to_page(&data, false, 0, options, int96_pt.clone()).expect("page"),
            );
            cases.push(build_case(
                format!("timestamp_int96_plain_n{null_pct}"),
                page,
                None,
                ColumnType::new(ColumnTypeTag::Timestamp, 0),
                None,
                ROW_COUNT,
            ));
        }
    }

    // RLE dictionary-encoded fixed-size types
    fixed_dict_cases!(cases, "byte", Byte, i32, |np, card| make_byte_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "short", Short, i32, |np, card| make_short_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "int", Int, i32, |np, card| make_i32_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "long", Long, i64, |np, card| make_i64_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "date", Date, i64, |np, card| make_i64_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "timestamp", Timestamp, i64, |np, card| {
        make_i64_dict_data(ROW_COUNT, np, card)
    });

    // Int96 timestamp (RLE dictionary)
    {
        let int96_pt = int96_primitive_type();
        for &card in &DICT_CARDINALITIES {
            for &null_pct in null_pcts(true) {
                let data = make_int96_dict_data(ROW_COUNT, null_pct, card);
                let ct = ColumnType::new(ColumnTypeTag::Timestamp, 0);
                let (page, dict) =
                    build_fixed_rle_dict_pages(&data, null_pct, ROW_COUNT, int96_pt.clone());
                cases.push(build_case(
                    format!(
                        "timestamp_int96_dict_c{card}_n{null_pct}",
                        card = card,
                        null_pct = null_pct,
                    ),
                    page,
                    Some(dict),
                    ct,
                    None,
                    ROW_COUNT,
                ));
            }
        }
    }

    fixed_dict_cases!(cases, "float", Float, f32, |np, card| make_f32_dict_data(
        ROW_COUNT, np, card
    ));
    fixed_dict_cases!(cases, "double", Double, f64, |np, card| make_f64_dict_data(
        ROW_COUNT, np, card
    ));

    // Variable-length types — each uses a different page function with different args
    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_string_data(ROW_COUNT, null_pct);
            let column_type = ColumnType::new(ColumnTypeTag::String, 0);
            let primitive_type = primitive_type_for(column_type);
            let page = data_page_from(
                string_to_page(
                    &data.offsets,
                    &data.data,
                    0,
                    options,
                    primitive_type,
                    encoding,
                )
                .expect("page"),
            );
            cases.push(build_case(
                format!("string_{enc}_n{null_pct}"),
                page,
                None,
                column_type,
                None,
                ROW_COUNT,
            ));
        }
    }

    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_varchar_data(ROW_COUNT, null_pct);
            let column_type = ColumnType::new(ColumnTypeTag::Varchar, 0);
            let primitive_type = primitive_type_for(column_type);
            let page = data_page_from(
                varchar_to_page(&data.aux, &data.data, 0, options, primitive_type, encoding)
                    .expect("page"),
            );
            cases.push(build_case(
                format!("varchar_{enc}_n{null_pct}"),
                page,
                None,
                column_type,
                None,
                ROW_COUNT,
            ));
        }
    }

    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_binary_data(ROW_COUNT, null_pct);
            let column_type = ColumnType::new(ColumnTypeTag::Binary, 0);
            let primitive_type = primitive_type_for(column_type);
            let page = data_page_from(
                binary_to_page(
                    &data.offsets,
                    &data.data,
                    0,
                    options,
                    primitive_type,
                    encoding,
                )
                .expect("page"),
            );
            cases.push(build_case(
                format!("binary_{enc}_n{null_pct}"),
                page,
                None,
                column_type,
                None,
                ROW_COUNT,
            ));
        }
    }

    for &encoding in &LEN_ENCODINGS {
        let enc = enc_label(encoding);
        for &null_pct in null_pcts(true) {
            let data = make_array_data(ROW_COUNT, null_pct);
            let column_type = encode_array_type(ColumnTypeTag::Double, 1).expect("array type");
            let primitive_type = primitive_type_for(column_type);
            let page = data_page_from(
                array_to_raw_page(&data.aux, &data.data, 0, options, primitive_type, encoding)
                    .expect("page"),
            );
            cases.push(build_case(
                format!("array_{enc}_n{null_pct}"),
                page,
                None,
                column_type,
                None,
                ROW_COUNT,
            ));
        }
    }

    // Symbol — unique pattern with dict page extraction
    for &null_pct in null_pcts(true) {
        let data = make_symbol_data(ROW_COUNT, null_pct);
        let column_type = ColumnType::new(ColumnTypeTag::Symbol, 0);
        let primitive_type = primitive_type_for(column_type);
        let mut dict = None;
        let mut data_page = None;
        let iter = symbol_to_pages(
            &data.keys,
            &data.offsets,
            &data.chars,
            0,
            options,
            primitive_type,
            false,
        )
        .expect("symbol pages");
        for page in iter {
            let page = page.expect("page");
            match page {
                Page::Dict(p) => dict = Some(p),
                Page::Data(p) => data_page = Some(p),
            }
        }
        let page = data_page.expect("data page");
        cases.push(build_case(
            format!("symbol_dict_n{null_pct}"),
            page,
            dict,
            column_type,
            Some(QdbMetaColFormat::LocalKeyIsGlobal),
            ROW_COUNT,
        ));
    }

    // Fixed-size byte array types
    bytes_cases!(cases, options, "long128", Long128, false, |np| {
        make_long128_data(ROW_COUNT, np)
    });
    bytes_cases!(cases, options, "uuid", Uuid, true, |np| make_long128_data(
        ROW_COUNT, np
    ));
    bytes_cases!(cases, options, "long256", Long256, false, |np| {
        make_long256_data(ROW_COUNT, np)
    });

    // Nullable integer types (int_slice_to_page_nullable)
    int_nullable_cases!(cases, options, "ipv4", IPv4, BenchIPv4, i32, |np| {
        make_ipv4_data(ROW_COUNT, np)
            .into_iter()
            .map(BenchIPv4)
            .collect()
    });
    int_nullable_cases!(
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
    int_nullable_cases!(
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
    int_nullable_cases!(cases, options, "geoint", GeoInt, BenchGeoInt, i32, |np| {
        make_i32_data(ROW_COUNT, np, -1)
            .into_iter()
            .map(BenchGeoInt)
            .collect()
    });
    int_nullable_cases!(
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

    cases
}

fn bench_decode_page(c: &mut Criterion) {
    let cases = build_cases();
    let mut group = c.benchmark_group("decode_page");

    for case in cases {
        let BenchCase {
            name,
            page,
            dict,
            col_info,
            row_count,
        } = case;
        group.throughput(Throughput::Elements(row_count as u64));
        let state: &'static mut DecodeBenchState = Box::leak(Box::new(DecodeBenchState::new()));
        group.bench_function(name, move |b| {
            b.iter(|| {
                state.bufs.data_vec.clear();
                state.bufs.aux_vec.clear();
                decode_page_ref(
                    black_box(&page),
                    dict.as_ref(),
                    &mut state.bufs,
                    col_info,
                    0,
                    row_count,
                );
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_decode_page);
criterion_main!(benches);

pub fn decode_page_ref(
    page: &DataPage,
    dict: Option<&DictPage>,
    bufs: &mut ColumnChunkBuffers,
    col_info: QdbMetaCol,
    row_lo: usize,
    row_hi: usize,
) {
    let page = page::DataPage {
        header: &page.header,
        buffer: &page.buffer,
        descriptor: &page.descriptor,
    };

    let dict = dict.map(|d| page::DictPage {
        buffer: &d.buffer,
        num_values: d.num_values,
        is_sorted: d.is_sorted,
    });
    decode_page(
        black_box(&page),
        dict.as_ref(),
        bufs,
        col_info,
        row_lo,
        row_hi,
    )
    .expect("decode_page");
}
