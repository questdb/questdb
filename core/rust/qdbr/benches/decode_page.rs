use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use num_traits::AsPrimitive;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, DictPage, Page};
use parquet2::schema::types::{ParquetType, PrimitiveType};
use parquet2::write::Version;
use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};
use questdbr::allocator::{MemTracking, QdbAllocator};
use questdbr::parquet::{QdbMetaCol, QdbMetaColFormat};
use questdbr::parquet_read::decode::decode_page;
use questdbr::parquet_read::ColumnChunkBuffers;
use questdbr::parquet_write::bench::{
    array_to_raw_page, binary_to_page, boolean_to_page, bytes_to_page, int_slice_to_page_notnull,
    int_slice_to_page_nullable, slice_to_page_simd, string_to_page, symbol_to_pages,
    varchar_to_page, WriteOptions,
};
use questdbr::parquet_write::schema::column_type_to_parquet_type;
use questdbr::parquet_write::Nullable;
use std::hint::black_box;
use std::sync::atomic::AtomicUsize;

const ROW_COUNT: usize = 100_000;
const NULL_PCTS: [u8; 2] = [0, 20];

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
        write_statistics: false,
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
            (i % 32000) as i16
        } else {
            (i as i16).wrapping_add(1)
        };
        let v = if is_null_at(i, null_pct) {
            null_value
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

fn build_cases() -> Vec<BenchCase> {
    let mut cases = Vec::new();
    let options = write_options();

    // Boolean — unique page function, not worth a macro
    for &null_pct in null_pcts(false) {
        let data = make_boolean_data(ROW_COUNT);
        let column_type = ColumnType::new(ColumnTypeTag::Boolean, 0);
        let primitive_type = primitive_type_for(column_type);
        let page =
            data_page_from(boolean_to_page(&data, 0, options, primitive_type).expect("page"));
        cases.push(build_case(
            format!("boolean_plain_n{null_pct}"),
            page,
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
                decode_page(
                    black_box(&page),
                    dict.as_ref(),
                    &mut state.bufs,
                    col_info,
                    0,
                    row_count,
                )
                .expect("decode_page");
            })
        });
    }

    group.finish();
}

criterion_group!(benches, bench_decode_page);
criterion_main!(benches);
