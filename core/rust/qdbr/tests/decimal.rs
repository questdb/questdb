mod common;

use std::sync::Arc;

use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    data_type::{ByteArray, FixedLenByteArray, FixedLenByteArrayType, Int32Type, Int64Type},
    schema::types::Type,
};
use qdb_core::col_type::{nulls, ColumnTypeTag};

use common::{
    decode_file, decode_file_filtered, def_levels_from_nulls, encode_decode_byte_array,
    encode_decode_byte_array_filtered, every_other_row_filter, generate_nulls, non_null_only,
    optional_byte_array_schema, qdb_props, required_byte_array_schema, write_parquet_column,
    Encoding, Null, ALL_NULLS, VERSIONS,
};

const ROW_COUNT: usize = 4096;
const INT32_DECIMAL_TARGETS: [(ColumnTypeTag, usize); 3] = [
    (ColumnTypeTag::Decimal8, 1),
    (ColumnTypeTag::Decimal16, 2),
    (ColumnTypeTag::Decimal32, 4),
];
const ALL_DECIMAL_TARGETS: [(ColumnTypeTag, usize); 6] = [
    (ColumnTypeTag::Decimal8, 1),
    (ColumnTypeTag::Decimal16, 2),
    (ColumnTypeTag::Decimal32, 4),
    (ColumnTypeTag::Decimal64, 8),
    (ColumnTypeTag::Decimal128, 16),
    (ColumnTypeTag::Decimal256, 32),
];
const DECIMAL8_NULL_BYTES: [u8; 1] = nulls::DECIMAL8.to_le_bytes();
const DECIMAL16_NULL_BYTES: [u8; 2] = nulls::DECIMAL16.to_le_bytes();
const DECIMAL32_NULL_BYTES: [u8; 4] = nulls::DECIMAL32.to_le_bytes();
const DECIMAL64_NULL_BYTES: [u8; 8] = nulls::DECIMAL64.to_le_bytes();
const DECIMAL128_NULL_BYTES: [u8; 16] = {
    let hi = i64::MIN.to_le_bytes();
    let lo = 0i64.to_le_bytes();
    [
        hi[0], hi[1], hi[2], hi[3], hi[4], hi[5], hi[6], hi[7], lo[0], lo[1], lo[2], lo[3], lo[4],
        lo[5], lo[6], lo[7],
    ]
};
const DECIMAL256_NULL_BYTES: [u8; 32] = {
    let hh = i64::MIN.to_le_bytes();
    let hi = 0i64.to_le_bytes();
    let lo = 0i64.to_le_bytes();
    let ll = 0i64.to_le_bytes();
    [
        hh[0], hh[1], hh[2], hh[3], hh[4], hh[5], hh[6], hh[7], hi[0], hi[1], hi[2], hi[3], hi[4],
        hi[5], hi[6], hi[7], lo[0], lo[1], lo[2], lo[3], lo[4], lo[5], lo[6], lo[7], ll[0], ll[1],
        ll[2], ll[3], ll[4], ll[5], ll[6], ll[7],
    ]
};

fn repetition_for_null(null: Null) -> Repetition {
    if matches!(null, Null::None) {
        Repetition::REQUIRED
    } else {
        Repetition::OPTIONAL
    }
}

fn decimal_null_bytes(target_size: usize) -> &'static [u8] {
    match target_size {
        1 => &DECIMAL8_NULL_BYTES,
        2 => &DECIMAL16_NULL_BYTES,
        4 => &DECIMAL32_NULL_BYTES,
        8 => &DECIMAL64_NULL_BYTES,
        16 => &DECIMAL128_NULL_BYTES,
        32 => &DECIMAL256_NULL_BYTES,
        _ => panic!("unsupported decimal target size {target_size}"),
    }
}

fn decimal_primitive_schema(
    physical: PhysicalType,
    repetition: Repetition,
    precision: i32,
    scale: i32,
) -> Type {
    let col = Type::primitive_type_builder("col", physical)
        .with_repetition(repetition)
        .with_precision(precision)
        .with_scale(scale)
        .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
        .build()
        .unwrap();

    Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col)])
        .build()
        .unwrap()
}

fn decimal_fixed_schema(len: usize, repetition: Repetition, precision: i32, scale: i32) -> Type {
    let col = Type::primitive_type_builder("col", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(repetition)
        .with_length(len as i32)
        .with_precision(precision)
        .with_scale(scale)
        .with_logical_type(Some(LogicalType::Decimal { scale, precision }))
        .build()
        .unwrap();

    Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col)])
        .build()
        .unwrap()
}

fn int32_to_target_bytes(v: i32, target_size: usize) -> Vec<u8> {
    match target_size {
        1 => (v as i8).to_le_bytes().to_vec(),
        2 => (v as i16).to_le_bytes().to_vec(),
        4 => v.to_le_bytes().to_vec(),
        _ => panic!("unsupported int32 decimal target size {target_size}"),
    }
}

fn generate_int32_values(count: usize, target_size: usize, encoding: Encoding) -> Vec<i32> {
    if matches!(encoding, Encoding::RleDictionary) {
        let base: &[i32] = match target_size {
            1 => &[-100, -1, 0, 1, 100],
            2 => &[-30000, -1, 0, 1, 30000],
            4 => &[-1_000_000, -1, 0, 1, 1_000_000],
            _ => panic!("unsupported int32 decimal target size {target_size}"),
        };
        (0..count).map(|i| base[i % base.len()]).collect()
    } else {
        (0..count)
            .map(|i| match target_size {
                1 => (i as i32 % 201) - 100,
                2 => (i as i32 % 60001) - 30000,
                4 => (i as i32).wrapping_mul(17).wrapping_sub(500_000),
                _ => panic!("unsupported int32 decimal target size {target_size}"),
            })
            .collect()
    }
}

fn generate_int64_values(count: usize, encoding: Encoding) -> Vec<i64> {
    if matches!(encoding, Encoding::RleDictionary) {
        let base = [-5_000_000i64, -1, 0, 1, 5_000_000];
        (0..count).map(|i| base[i % base.len()]).collect()
    } else {
        (0..count)
            .map(|i| (i as i64).wrapping_mul(1_000_003).wrapping_sub(100_000_000))
            .collect()
    }
}

fn be_from_i64(value: i64, len: usize) -> Vec<u8> {
    assert!(len > 0);
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

fn generate_flba_values(count: usize, src_len: usize, encoding: Encoding) -> Vec<Vec<u8>> {
    if matches!(encoding, Encoding::RleDictionary) {
        let base = [-128i64, -2, -1, 0, 1, 2, 123, 127];
        (0..count)
            .map(|i| be_from_i64(base[i % base.len()], src_len))
            .collect()
    } else {
        (0..count)
            .map(|i| be_from_i64((i as i64 % 256) - 128, src_len))
            .collect()
    }
}

fn byte_array_templates() -> Vec<Vec<u8>> {
    vec![
        vec![0x7B], // 123
        vec![0xFF], // -1
        vec![0x80], // -128
        vec![0x00], // 0
        vec![0x00, 0x7B],
        vec![0xFF, 0xFF],
        vec![0xFF, 0x80],
        vec![0x00, 0x00, 0x7B],
        vec![0xFF, 0xFF, 0xFF],
        vec![0xFF, 0xFF, 0x80],
        vec![0x00, 0x00, 0x00, 0x7B],
        vec![0xFF; 9],
        vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B],
        vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x80],
    ]
}

fn generate_byte_array_values(count: usize, _encoding: Encoding) -> Vec<Vec<u8>> {
    let templates = byte_array_templates();
    (0..count)
        .map(|i| templates[i % templates.len()].clone())
        .collect()
}

fn be_to_qdb_decimal(src: &[u8], target_size: usize) -> Vec<u8> {
    assert!(!src.is_empty());
    let mut src = src;
    let sign_byte = if src[0] & 0x80 != 0 { 0xFF } else { 0x00 };

    if src.len() > target_size {
        let trunc = src.len() - target_size;
        assert!(
            src[..trunc].iter().all(|b| *b == sign_byte),
            "source is not sign-extended: src={src:?}, target_size={target_size}"
        );
        let msb = src[trunc];
        assert_eq!(
            msb & 0x80,
            sign_byte & 0x80,
            "source truncates significant bits: src={src:?}, target_size={target_size}"
        );
        src = &src[trunc..];
    }

    let mut out = vec![0u8; target_size];
    if target_size <= 8 {
        for i in 0..src.len() {
            out[i] = src[src.len() - 1 - i];
        }
        for b in out.iter_mut().take(target_size).skip(src.len()) {
            *b = sign_byte;
        }
    } else {
        let words = target_size / 8;
        let sign_prefix = target_size - src.len();

        for w in 0..words {
            for i in 0..8 {
                let ext_pos = w * 8 + 7 - i;
                let byte = if ext_pos < sign_prefix {
                    sign_byte
                } else {
                    src[ext_pos - sign_prefix]
                };
                out[w * 8 + i] = byte;
            }
        }
    }
    out
}

#[test]
fn test_decimal_int32_targets() {
    let encodings = [Encoding::Plain, Encoding::RleDictionary];

    for (tag, target_size) in INT32_DECIMAL_TARGETS {
        for version in VERSIONS {
            for encoding in encodings {
                for null in ALL_NULLS {
                    eprintln!(
                        "Testing Int32->{tag:?} version={version:?}, encoding={encoding:?}, null={null:?}"
                    );

                    let nulls = generate_nulls(ROW_COUNT, null);
                    let values = generate_int32_values(ROW_COUNT, target_size, encoding);
                    let non_null_values = non_null_only(&values, &nulls);
                    let def_levels = def_levels_from_nulls(&nulls);
                    let schema = decimal_primitive_schema(
                        PhysicalType::INT32,
                        repetition_for_null(null),
                        9,
                        2,
                    );
                    let props = qdb_props(tag, version, encoding);

                    let parquet_buf = write_parquet_column::<Int32Type>(
                        "col",
                        schema,
                        &non_null_values,
                        Some(&def_levels),
                        Arc::new(props),
                    );

                    let (data, aux) = decode_file(&parquet_buf);
                    assert!(
                        aux.is_empty(),
                        "decimal primitive should not produce aux bytes"
                    );

                    let mut expected = Vec::with_capacity(ROW_COUNT * target_size);
                    for i in 0..ROW_COUNT {
                        if nulls[i] {
                            expected.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected
                                .extend_from_slice(&int32_to_target_bytes(values[i], target_size));
                        }
                    }
                    assert_eq!(
                        data, expected,
                        "mismatch for {tag:?} / {encoding:?} / {null:?}"
                    );

                    // Filtered decode test
                    let rows_filter = every_other_row_filter(ROW_COUNT);
                    let (data_f, aux_f) = decode_file_filtered(&parquet_buf, &rows_filter);
                    assert!(aux_f.is_empty(), "filtered decimal primitive should not produce aux bytes");
                    let mut expected_f = Vec::with_capacity(rows_filter.len() * target_size);
                    for &r in &rows_filter {
                        let i = r as usize;
                        if nulls[i] {
                            expected_f.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected_f.extend_from_slice(&int32_to_target_bytes(values[i], target_size));
                        }
                    }
                    assert_eq!(data_f, expected_f, "filtered mismatch for {tag:?} / {encoding:?} / {null:?}");
                }
            }
        }
    }
}

#[test]
fn test_decimal_int64_target() {
    let encodings = [
        Encoding::Plain,
        Encoding::DeltaBinaryPacked,
        Encoding::RleDictionary,
    ];
    let target_size = 8usize;

    for version in VERSIONS {
        for encoding in encodings {
            for null in ALL_NULLS {
                eprintln!(
                    "Testing Int64->Decimal64 version={version:?}, encoding={encoding:?}, null={null:?}"
                );

                let nulls = generate_nulls(ROW_COUNT, null);
                let values = generate_int64_values(ROW_COUNT, encoding);
                let non_null_values = non_null_only(&values, &nulls);
                let def_levels = def_levels_from_nulls(&nulls);
                let schema =
                    decimal_primitive_schema(PhysicalType::INT64, repetition_for_null(null), 18, 3);
                let props = qdb_props(ColumnTypeTag::Decimal64, version, encoding);

                let parquet_buf = write_parquet_column::<Int64Type>(
                    "col",
                    schema,
                    &non_null_values,
                    Some(&def_levels),
                    Arc::new(props),
                );

                let (data, aux) = decode_file(&parquet_buf);
                assert!(
                    aux.is_empty(),
                    "decimal primitive should not produce aux bytes"
                );

                let mut expected = Vec::with_capacity(ROW_COUNT * target_size);
                for i in 0..ROW_COUNT {
                    if nulls[i] {
                        expected.extend_from_slice(decimal_null_bytes(target_size));
                    } else {
                        expected.extend_from_slice(&values[i].to_le_bytes());
                    }
                }
                assert_eq!(
                    data, expected,
                    "mismatch for Decimal64 / {encoding:?} / {null:?}"
                );

                // Filtered decode test
                let rows_filter = every_other_row_filter(ROW_COUNT);
                let (data_f, aux_f) = decode_file_filtered(&parquet_buf, &rows_filter);
                assert!(aux_f.is_empty(), "filtered decimal primitive should not produce aux bytes");
                let mut expected_f = Vec::with_capacity(rows_filter.len() * target_size);
                for &r in &rows_filter {
                    let i = r as usize;
                    if nulls[i] {
                        expected_f.extend_from_slice(decimal_null_bytes(target_size));
                    } else {
                        expected_f.extend_from_slice(&values[i].to_le_bytes());
                    }
                }
                assert_eq!(data_f, expected_f, "filtered mismatch for Decimal64 / {encoding:?} / {null:?}");
            }
        }
    }
}

#[test]
fn test_decimal_flba_all_target_sizes() {
    let encodings = [Encoding::Plain, Encoding::RleDictionary];

    for (tag, target_size) in ALL_DECIMAL_TARGETS {
        for version in VERSIONS {
            for encoding in encodings {
                for null in ALL_NULLS {
                    eprintln!(
                        "Testing FLBA(src={target_size})->{tag:?} version={version:?}, encoding={encoding:?}, null={null:?}"
                    );

                    let src_len = target_size;
                    let nulls = generate_nulls(ROW_COUNT, null);
                    let src_values = generate_flba_values(ROW_COUNT, src_len, encoding);
                    let non_null_values = non_null_only(&src_values, &nulls);
                    let non_null_values: Vec<FixedLenByteArray> = non_null_values
                        .into_iter()
                        .map(FixedLenByteArray::from)
                        .collect();
                    let def_levels = def_levels_from_nulls(&nulls);
                    let schema = decimal_fixed_schema(src_len, repetition_for_null(null), 2, 0);
                    let props = qdb_props(tag, version, encoding);

                    let parquet_buf = write_parquet_column::<FixedLenByteArrayType>(
                        "col",
                        schema,
                        &non_null_values,
                        Some(&def_levels),
                        Arc::new(props),
                    );

                    let (data, aux) = decode_file(&parquet_buf);
                    assert!(aux.is_empty(), "decimal FLBA should not produce aux bytes");

                    let mut expected = Vec::with_capacity(ROW_COUNT * target_size);
                    for i in 0..ROW_COUNT {
                        if nulls[i] {
                            expected.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected
                                .extend_from_slice(&be_to_qdb_decimal(&src_values[i], target_size));
                        }
                    }
                    assert_eq!(
                        data, expected,
                        "mismatch for {tag:?} / {encoding:?} / {null:?}"
                    );

                    // Filtered decode test
                    let rows_filter = every_other_row_filter(ROW_COUNT);
                    let (data_f, aux_f) = decode_file_filtered(&parquet_buf, &rows_filter);
                    assert!(aux_f.is_empty(), "filtered decimal FLBA should not produce aux bytes");
                    let mut expected_f = Vec::with_capacity(rows_filter.len() * target_size);
                    for &r in &rows_filter {
                        let i = r as usize;
                        if nulls[i] {
                            expected_f.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected_f.extend_from_slice(&be_to_qdb_decimal(&src_values[i], target_size));
                        }
                    }
                    assert_eq!(data_f, expected_f, "filtered mismatch for {tag:?} / {encoding:?} / {null:?}");
                }
            }
        }
    }
}

#[test]
fn test_decimal_flba_sign_extend_from_larger_source() {
    let cases = [
        (ColumnTypeTag::Decimal64, 8usize, 16usize),
        (ColumnTypeTag::Decimal128, 16usize, 32usize),
    ];
    let encodings = [Encoding::Plain, Encoding::RleDictionary];

    for (tag, target_size, src_len) in cases {
        for version in VERSIONS {
            for encoding in encodings {
                for null in ALL_NULLS {
                    eprintln!(
                        "Testing FLBA(src={src_len})->{tag:?} version={version:?}, encoding={encoding:?}, null={null:?}"
                    );

                    let nulls = generate_nulls(ROW_COUNT, null);
                    let src_values = generate_flba_values(ROW_COUNT, src_len, encoding);
                    let non_null_values = non_null_only(&src_values, &nulls);
                    let non_null_values: Vec<FixedLenByteArray> = non_null_values
                        .into_iter()
                        .map(FixedLenByteArray::from)
                        .collect();
                    let def_levels = def_levels_from_nulls(&nulls);
                    let schema = decimal_fixed_schema(src_len, repetition_for_null(null), 2, 0);
                    let props = qdb_props(tag, version, encoding);

                    let parquet_buf = write_parquet_column::<FixedLenByteArrayType>(
                        "col",
                        schema,
                        &non_null_values,
                        Some(&def_levels),
                        Arc::new(props),
                    );

                    let (data, aux) = decode_file(&parquet_buf);
                    assert!(aux.is_empty(), "decimal FLBA should not produce aux bytes");

                    let mut expected = Vec::with_capacity(ROW_COUNT * target_size);
                    for i in 0..ROW_COUNT {
                        if nulls[i] {
                            expected.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected
                                .extend_from_slice(&be_to_qdb_decimal(&src_values[i], target_size));
                        }
                    }
                    assert_eq!(
                        data, expected,
                        "mismatch for FLBA sign-extension {tag:?} / {encoding:?} / {null:?}"
                    );

                    // Filtered decode test
                    let rows_filter = every_other_row_filter(ROW_COUNT);
                    let (data_f, aux_f) = decode_file_filtered(&parquet_buf, &rows_filter);
                    assert!(aux_f.is_empty(), "filtered decimal FLBA should not produce aux bytes");
                    let mut expected_f = Vec::with_capacity(rows_filter.len() * target_size);
                    for &r in &rows_filter {
                        let i = r as usize;
                        if nulls[i] {
                            expected_f.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected_f.extend_from_slice(&be_to_qdb_decimal(&src_values[i], target_size));
                        }
                    }
                    assert_eq!(data_f, expected_f, "filtered mismatch for FLBA sign-extension {tag:?} / {encoding:?} / {null:?}");
                }
            }
        }
    }
}

#[test]
fn test_decimal_byte_array_all_target_sizes() {
    let encodings = [Encoding::Plain, Encoding::RleDictionary];

    for (tag, target_size) in ALL_DECIMAL_TARGETS {
        for version in VERSIONS {
            for encoding in encodings {
                for null in ALL_NULLS {
                    eprintln!(
                        "Testing ByteArray->{tag:?} version={version:?}, encoding={encoding:?}, null={null:?}"
                    );

                    let nulls = generate_nulls(ROW_COUNT, null);
                    let raw_values = generate_byte_array_values(ROW_COUNT, encoding);
                    let values: Vec<ByteArray> =
                        raw_values.iter().cloned().map(ByteArray::from).collect();

                    let schema = if matches!(null, Null::None) {
                        required_byte_array_schema(
                            "col",
                            Some(LogicalType::Decimal {
                                scale: 2,
                                precision: 20,
                            }),
                        )
                    } else {
                        optional_byte_array_schema(
                            "col",
                            Some(LogicalType::Decimal {
                                scale: 2,
                                precision: 20,
                            }),
                        )
                    };
                    let props = qdb_props(tag, version, encoding);

                    let (data, aux) = encode_decode_byte_array(&values, &nulls, schema, props);
                    assert!(
                        aux.is_empty(),
                        "decimal ByteArray should not produce aux bytes"
                    );

                    let mut expected = Vec::with_capacity(ROW_COUNT * target_size);
                    for i in 0..ROW_COUNT {
                        if nulls[i] {
                            expected.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected
                                .extend_from_slice(&be_to_qdb_decimal(&raw_values[i], target_size));
                        }
                    }
                    assert_eq!(
                        data, expected,
                        "mismatch for {tag:?} / {encoding:?} / {null:?}"
                    );

                    // Filtered decode test
                    let rows_filter = every_other_row_filter(ROW_COUNT);
                    let schema_f = if matches!(null, Null::None) {
                        required_byte_array_schema(
                            "col",
                            Some(LogicalType::Decimal {
                                scale: 2,
                                precision: 20,
                            }),
                        )
                    } else {
                        optional_byte_array_schema(
                            "col",
                            Some(LogicalType::Decimal {
                                scale: 2,
                                precision: 20,
                            }),
                        )
                    };
                    let props_f = qdb_props(tag, version, encoding);
                    let (data_f, aux_f) = encode_decode_byte_array_filtered(&values, &nulls, schema_f, props_f, &rows_filter);
                    assert!(aux_f.is_empty(), "filtered decimal ByteArray should not produce aux bytes");
                    let mut expected_f = Vec::with_capacity(rows_filter.len() * target_size);
                    for &r in &rows_filter {
                        let i = r as usize;
                        if nulls[i] {
                            expected_f.extend_from_slice(decimal_null_bytes(target_size));
                        } else {
                            expected_f.extend_from_slice(&be_to_qdb_decimal(&raw_values[i], target_size));
                        }
                    }
                    assert_eq!(data_f, expected_f, "filtered mismatch for {tag:?} / {encoding:?} / {null:?}");
                }
            }
        }
    }
}
