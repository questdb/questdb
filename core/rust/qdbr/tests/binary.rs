mod common;

use parquet::data_type::ByteArray;

use common::{
    encode_decode_byte_array, generate_nulls, optional_byte_array_schema, qdb_props,
    required_byte_array_schema, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};
use qdb_core::col_type::ColumnTypeTag;

fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            let bytes: Vec<u8> = (0..10).map(|j| ((i * 7 + j) % 256) as u8).collect();
            ByteArray::from(bytes)
        })
        .collect()
}

fn assert_binary(nulls: &[bool], data: &[u8]) {
    let row_count = nulls.len();

    // Binary format in QuestDB: each value is i64 length + raw bytes; null = i64(-1)
    let mut offset = 0;
    for i in 0..row_count {
        if nulls[i] {
            let len = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            assert_eq!(len, -1, "row {i}: null binary should have length -1");
            offset += 8;
        } else {
            let expected_bytes: Vec<u8> = (0..10).map(|j| ((i * 7 + j) % 256) as u8).collect();
            let len = i64::from_le_bytes(data[offset..offset + 8].try_into().unwrap());
            assert_eq!(
                len as usize,
                expected_bytes.len(),
                "row {i}: binary length mismatch"
            );
            offset += 8;
            let actual = &data[offset..offset + expected_bytes.len()];
            assert_eq!(
                actual,
                expected_bytes.as_slice(),
                "row {i}: binary data mismatch"
            );
            offset += expected_bytes.len();
        }
    }
    assert_eq!(offset, data.len(), "binary data length mismatch");
}

fn run_binary_test(name: &str, encoding: Encoding) {
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!("Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}");

            let nulls = generate_nulls(COUNT, *null);
            let values = generate_values(COUNT);

            let schema = if matches!(null, Null::None) {
                required_byte_array_schema("col", None)
            } else {
                optional_byte_array_schema("col", None)
            };

            let props = qdb_props(ColumnTypeTag::Binary, *version, encoding);
            let (data, _aux) = encode_decode_byte_array(&values, &nulls, schema, props);
            assert_binary(&nulls, &data);
        }
    }
}

#[test]
fn test_binary_plain() {
    run_binary_test("Binary", Encoding::Plain);
}

#[test]
fn test_binary_delta_length_byte_array() {
    run_binary_test("Binary", Encoding::DeltaLengthByteArray);
}

#[test]
fn test_binary_rle_dictionary() {
    run_binary_test("Binary", Encoding::RleDictionary);
}
