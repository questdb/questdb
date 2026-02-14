mod common;

use parquet::basic::LogicalType;
use parquet::data_type::ByteArray;

use common::{
    encode_decode_byte_array, generate_nulls, optional_byte_array_schema, qdb_props,
    required_byte_array_schema, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};
use qdb_core::col_type::ColumnTypeTag;

const HEADER_FLAG_INLINED: u8 = 1;
const HEADER_FLAG_NULL: u8 = 4;
const HEADER_FLAGS_WIDTH: u8 = 4;

fn read_offset(aux: &[u8], base: usize) -> usize {
    let lo = u16::from_le_bytes([aux[base], aux[base + 1]]) as usize;
    let hi =
        u32::from_le_bytes([aux[base + 2], aux[base + 3], aux[base + 4], aux[base + 5]]) as usize;
    lo | (hi << 16)
}

fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| ByteArray::from(format!("val_{i:04}").as_str()))
        .collect()
}

fn assert_varchar(nulls: &[bool], data: &[u8], aux: &[u8]) {
    let row_count = nulls.len();
    assert_eq!(aux.len(), row_count * 16, "varchar aux size mismatch");

    for i in 0..row_count {
        let aux_base = i * 16;
        let header_byte = aux[aux_base];

        if nulls[i] {
            assert_eq!(
                header_byte & HEADER_FLAG_NULL,
                HEADER_FLAG_NULL,
                "row {i}: null varchar should have NULL flag set, got header {header_byte:#04x}"
            );
        } else {
            let expected_str = format!("val_{i:04}");
            let expected_bytes = expected_str.as_bytes();
            let str_len = expected_bytes.len();

            if str_len <= 9 {
                assert_ne!(
                    header_byte & HEADER_FLAG_INLINED,
                    0,
                    "row {i}: short varchar should have INLINED flag"
                );
                let inline_len = (header_byte >> HEADER_FLAGS_WIDTH) as usize;
                assert_eq!(inline_len, str_len, "row {i}: inline length mismatch");
                let inline_data = &aux[aux_base + 1..aux_base + 1 + str_len];
                assert_eq!(inline_data, expected_bytes, "row {i}: inline data mismatch");
            } else {
                let header_u32 = u32::from_le_bytes([
                    aux[aux_base],
                    aux[aux_base + 1],
                    aux[aux_base + 2],
                    aux[aux_base + 3],
                ]);
                let stored_len = (header_u32 >> HEADER_FLAGS_WIDTH as u32) as usize;
                assert_eq!(stored_len, str_len, "row {i}: overflow length mismatch");

                let prefix = &aux[aux_base + 4..aux_base + 10];
                assert_eq!(
                    prefix,
                    &expected_bytes[..6],
                    "row {i}: overflow prefix mismatch"
                );

                let offset = read_offset(aux, aux_base + 10);
                let actual_data = &data[offset..offset + str_len];
                assert_eq!(
                    actual_data, expected_bytes,
                    "row {i}: overflow data mismatch"
                );
            }
        }
    }
}

fn run_varchar_test(name: &str, encoding: Encoding) {
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!(
                "Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}"
            );

            let nulls = generate_nulls(COUNT, *null);
            let values = generate_values(COUNT);

            let schema = if matches!(null, Null::None) {
                required_byte_array_schema("col", Some(LogicalType::String))
            } else {
                optional_byte_array_schema("col", Some(LogicalType::String))
            };

            let props = qdb_props(ColumnTypeTag::Varchar, *version, encoding);
            let (data, aux) = encode_decode_byte_array(&values, &nulls, schema, props);
            assert_varchar(&nulls, &data, &aux);
        }
    }
}

#[test]
fn test_varchar_plain() {
    run_varchar_test("Varchar", Encoding::Plain);
}

#[test]
fn test_varchar_delta_length_byte_array() {
    run_varchar_test("Varchar", Encoding::DeltaLengthByteArray);
}

#[test]
fn test_varchar_delta_byte_array() {
    run_varchar_test("Varchar", Encoding::DeltaByteArray);
}

#[test]
fn test_varchar_rle_dictionary() {
    run_varchar_test("Varchar", Encoding::RleDictionary);
}
