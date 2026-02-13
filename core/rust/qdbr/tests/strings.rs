mod common;

use parquet::basic::LogicalType;
use parquet::data_type::ByteArray;

use common::{
    encode_decode_byte_array, generate_nulls, optional_byte_array_schema, qdb_props,
    required_byte_array_schema, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};
use qdb_core::col_type::ColumnTypeTag;

fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| ByteArray::from(format!("str_{i:04}").as_str()))
        .collect()
}

fn assert_string(nulls: &[bool], data: &[u8], aux: &[u8]) {
    let row_count = nulls.len();

    // aux_vec: initial 0u64 offset, then one u64 offset per row
    assert_eq!(aux.len(), (row_count + 1) * 8, "string aux size mismatch");

    let initial_offset = u64::from_le_bytes(aux[0..8].try_into().unwrap());
    assert_eq!(initial_offset, 0, "string initial aux offset should be 0");

    let mut data_offset = 0usize;
    for i in 0..row_count {
        if nulls[i] {
            let len = i32::from_le_bytes(data[data_offset..data_offset + 4].try_into().unwrap());
            assert_eq!(len, -1, "row {i}: null string should have length -1");
            data_offset += 4;
        } else {
            let expected_str = format!("str_{i:04}");
            let utf16_chars: Vec<u16> = expected_str.encode_utf16().collect();
            let utf16_char_count = utf16_chars.len();

            let stored_len =
                i32::from_le_bytes(data[data_offset..data_offset + 4].try_into().unwrap());
            assert_eq!(
                stored_len as usize, utf16_char_count,
                "row {i}: string utf16 char count mismatch"
            );
            data_offset += 4;

            for (j, &expected_char) in utf16_chars.iter().enumerate() {
                let actual_char =
                    u16::from_le_bytes(data[data_offset..data_offset + 2].try_into().unwrap());
                assert_eq!(
                    actual_char, expected_char,
                    "row {i}: string utf16 char {j} mismatch"
                );
                data_offset += 2;
            }
        }

        let aux_offset = u64::from_le_bytes(aux[(i + 1) * 8..(i + 2) * 8].try_into().unwrap());
        assert_eq!(
            aux_offset as usize, data_offset,
            "row {i}: string aux offset mismatch"
        );
    }
    assert_eq!(data_offset, data.len(), "string data length mismatch");
}

fn run_string_test(name: &str, encoding: Encoding) {
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!("Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}");

            let nulls = generate_nulls(COUNT, *null);
            let values = generate_values(COUNT);

            let schema = if matches!(null, Null::None) {
                required_byte_array_schema("col", Some(LogicalType::String))
            } else {
                optional_byte_array_schema("col", Some(LogicalType::String))
            };

            let props = qdb_props(ColumnTypeTag::String, *version, encoding);
            let (data, aux) = encode_decode_byte_array(&values, &nulls, schema, props);
            assert_string(&nulls, &data, &aux);
        }
    }
}

#[test]
fn test_string_plain() {
    run_string_test("String", Encoding::Plain);
}

#[test]
fn test_string_delta_length_byte_array() {
    run_string_test("String", Encoding::DeltaLengthByteArray);
}
