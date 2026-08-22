mod common;

use std::sync::Arc;

use parquet::{
    basic::{LogicalType, Repetition},
    data_type::DataType,
    file::properties::WriterVersion,
    schema::types::Type,
};

use common::{
    decode_file, decode_file_filtered, every_other_row_filter, generate_nulls, qdb_props,
    types::primitives::{
        generate_data, Boolean, Byte, Char, Date, DateInt32, Double, Float, GeoByte, GeoInt,
        GeoLong, GeoShort, IPv4, Int, Long, Long128, Long256, PrimitiveType, Short, Timestamp,
        TimestampInt96, Uuid,
    },
    write_parquet_column, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};

fn encode_data<T: PrimitiveType>(
    values: &[<T::U as DataType>::T],
    nulls: &[bool],
    version: WriterVersion,
    encoding: Encoding,
    repetition: Repetition,
) -> Vec<u8>
where
    T::T: Clone,
{
    let mut col_builder =
        Type::primitive_type_builder("col", <T::U as DataType>::get_physical_type())
            .with_repetition(repetition);
    if let Some(len) = T::FIXED_LEN {
        col_builder = col_builder.with_length(len);
    }
    if let Some(lt) = T::LOGICAL_TYPE {
        if let LogicalType::Decimal { scale, precision } = lt {
            col_builder = col_builder.with_precision(precision).with_scale(scale);
        }
        col_builder = col_builder.with_logical_type(Some(lt));
    }
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();
    let def_levels = common::def_levels_from_nulls(nulls);
    let props = qdb_props(T::TAG, version, encoding);
    write_parquet_column::<T::U>("col", schema, values, Some(&def_levels), Arc::new(props))
}

fn assert_decoding<T: PrimitiveType>(expected: Vec<T::T>, nulls: Vec<bool>, actual: Vec<u8>) {
    let null_count = nulls.iter().filter(|x| **x).count();
    assert_eq!(
        actual.len(),
        (expected.len() + null_count) * size_of::<T::T>()
    );
    assert_eq!(actual.len(), nulls.len() * size_of::<T::T>());

    let actual = actual.as_ptr().cast::<T::T>();
    let mut expected_offset = 0;
    for (i, null) in nulls.iter().enumerate() {
        let current = unsafe { std::ptr::read_unaligned(actual.add(i)) };
        let expected = if *null {
            T::NULL
        } else {
            let exp = expected[expected_offset];
            expected_offset += 1;
            exp
        };
        assert!(
            T::eq(current, expected),
            "mismatch at index {i}: {current:?} != {expected:?}"
        );
    }
}

fn run_primitive_test<T: PrimitiveType>(
    version: WriterVersion,
    encoding: Encoding,
    null: Null,
    repetition: Repetition,
) {
    let nulls = generate_nulls(COUNT, null);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet, native) = generate_data::<T>(COUNT - null_count);

    let parquet_file = encode_data::<T>(&parquet, &nulls, version, encoding, repetition);
    let (data, aux) = decode_file(&parquet_file);
    assert_decoding::<T>(native, nulls, data);
    assert!(aux.is_empty());
}

fn run_primitive_test_filtered<T: PrimitiveType>(
    version: WriterVersion,
    encoding: Encoding,
    null: Null,
    repetition: Repetition,
) {
    let nulls = generate_nulls(COUNT, null);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet, native) = generate_data::<T>(COUNT - null_count);

    let parquet_file = encode_data::<T>(&parquet, &nulls, version, encoding, repetition);
    let rows_filter = every_other_row_filter(COUNT);

    let mut full_expected: Vec<T::T> = Vec::with_capacity(COUNT);
    let mut val_idx = 0;
    for null in nulls.iter().take(COUNT) {
        if *null {
            full_expected.push(T::NULL);
        } else {
            full_expected.push(native[val_idx]);
            val_idx += 1;
        }
    }

    let filtered_expected: Vec<T::T> = rows_filter
        .iter()
        .map(|&r| full_expected[r as usize])
        .collect();
    let (data, aux) = decode_file_filtered(&parquet_file, &rows_filter);

    assert_eq!(
        data.len(),
        filtered_expected.len() * size_of::<T::T>(),
        "filtered data size mismatch"
    );

    let actual = data.as_ptr().cast::<T::T>();
    for (idx, &expected) in filtered_expected.iter().enumerate() {
        let current = unsafe { std::ptr::read_unaligned(actual.add(idx)) };
        assert!(
            T::eq(current, expected),
            "filtered mismatch at index {idx}: {current:?} != {expected:?}"
        );
    }
    assert!(aux.is_empty());
}

fn run_all_combos<T: PrimitiveType>(name: &str) {
    for version in &VERSIONS {
        for encoding in T::ENCODINGS {
            for null in &ALL_NULLS {
                let repetition = if matches!(null, Null::None) {
                    Repetition::REQUIRED
                } else {
                    Repetition::OPTIONAL
                };
                eprintln!(
                    "Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}"
                );
                run_primitive_test::<T>(*version, *encoding, *null, repetition);
                run_primitive_test_filtered::<T>(*version, *encoding, *null, repetition);
            }
        }
    }
}

// --- Tests ---

#[test]
fn test_boolean() {
    run_all_combos::<Boolean>("Boolean");
}

#[test]
fn test_geobyte() {
    run_all_combos::<GeoByte>("GeoByte");
}

#[test]
fn test_geoshort() {
    run_all_combos::<GeoShort>("GeoShort");
}

#[test]
fn test_geoint() {
    run_all_combos::<GeoInt>("GeoInt");
}

#[test]
fn test_geolong() {
    run_all_combos::<GeoLong>("GeoLong");
}

#[test]
fn test_byte() {
    run_all_combos::<Byte>("Byte");
}

#[test]
fn test_short() {
    run_all_combos::<Short>("Short");
}

#[test]
fn test_int() {
    run_all_combos::<Int>("Int");
}

#[test]
fn test_ipv4() {
    run_all_combos::<IPv4>("IPv4");
}

#[test]
fn test_long() {
    run_all_combos::<Long>("Long");
}

#[test]
fn test_timestamp() {
    run_all_combos::<Timestamp>("Timestamp");
}

#[test]
fn test_date() {
    run_all_combos::<Date>("Date");
}

#[test]
fn test_date_int32() {
    run_all_combos::<DateInt32>("DateInt32");
}

#[test]
fn test_float() {
    run_all_combos::<Float>("Float");
}

#[test]
fn test_double() {
    run_all_combos::<Double>("Double");
}

#[test]
fn test_long128() {
    run_all_combos::<Long128>("Long128");
}

#[test]
fn test_long256() {
    run_all_combos::<Long256>("Long256");
}

#[test]
fn test_char() {
    run_all_combos::<Char>("Char");
}

#[test]
fn test_uuid() {
    run_all_combos::<Uuid>("Uuid");
}

#[test]
fn test_timestamp_int96() {
    run_all_combos::<TimestampInt96>("TimestampInt96");
}
