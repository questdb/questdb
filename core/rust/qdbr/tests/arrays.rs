mod common;

use std::io::Cursor;
use std::sync::Arc;

use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use parquet::data_type::{ByteArray, DoubleType};
use parquet::file::properties::WriterVersion;
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::types::Type;

use common::{
    decode_file, encode_decode_byte_array, generate_nulls, optional_byte_array_schema,
    qdb_props_col_type, required_byte_array_schema, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};
use qdb_core::col_type::{encode_array_type, ColumnType, ColumnTypeTag};

// ---- Raw ByteArray arrays ----

fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| {
            // Generate 1D double arrays of varying lengths (1-5 elements)
            let len = (i % 5) + 1;
            let mut bytes = Vec::with_capacity(len * 8);
            for j in 0..len {
                let val = (i * 10 + j) as f64;
                bytes.extend_from_slice(&val.to_le_bytes());
            }
            ByteArray::from(bytes)
        })
        .collect()
}

fn assert_raw_array(nulls: &[bool], data: &[u8], aux: &[u8]) {
    let row_count = nulls.len();
    assert_eq!(aux.len(), row_count * 16, "array aux size mismatch");

    let non_null_indices: Vec<usize> = (0..row_count).filter(|i| !nulls[*i]).collect();
    let mut nn_idx = 0;

    for i in 0..row_count {
        let aux_base = i * 16;
        let offset = u64::from_le_bytes(aux[aux_base..aux_base + 8].try_into().unwrap()) as usize;
        let size = u64::from_le_bytes(aux[aux_base + 8..aux_base + 16].try_into().unwrap()) as usize;

        if nulls[i] {
            assert_eq!(size, 0, "row {i}: null array should have size 0");
        } else {
            let orig = non_null_indices[nn_idx];
            let len = (orig % 5) + 1;
            let expected_size = len * 8;
            assert_eq!(size, expected_size, "row {i}: array size mismatch");

            let actual = &data[offset..offset + size];
            for j in 0..len {
                let expected_val = (orig * 10 + j) as f64;
                let actual_val =
                    f64::from_le_bytes(actual[j * 8..(j + 1) * 8].try_into().unwrap());
                assert_eq!(
                    actual_val.to_bits(),
                    expected_val.to_bits(),
                    "row {i}: array element {j} mismatch"
                );
            }
            nn_idx += 1;
        }
    }
}

fn run_raw_array_test(name: &str, encoding: Encoding) {
    let col_type = encode_array_type(ColumnTypeTag::Double, 1).unwrap();
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

            let props = qdb_props_col_type(col_type, *version, encoding);
            let (data, aux) = encode_decode_byte_array(&values, &nulls, schema, props);
            assert_raw_array(&nulls, &data, &aux);
        }
    }
}

#[test]
fn test_array_raw_plain() {
    run_raw_array_test("RawArray", Encoding::Plain);
}

#[test]
fn test_array_raw_delta_length_byte_array() {
    run_raw_array_test("RawArray", Encoding::DeltaLengthByteArray);
}

// ---- Double-typed (LIST) arrays ----

/// Build a 1D LIST schema: optional/required group col (LIST) { repeated group list { optional double element; } }
fn list_double_schema(optional: bool) -> Type {
    let element = Arc::new(
        Type::primitive_type_builder("element", PhysicalType::DOUBLE)
            .build()
            .unwrap(),
    );
    let list = Arc::new(
        Type::group_type_builder("list")
            .with_repetition(Repetition::REPEATED)
            .with_fields(vec![element])
            .build()
            .unwrap(),
    );
    let col_rep = if optional {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };
    let col = Arc::new(
        Type::group_type_builder("col")
            .with_repetition(col_rep)
            .with_logical_type(Some(LogicalType::List))
            .with_fields(vec![list])
            .build()
            .unwrap(),
    );
    Type::group_type_builder("schema")
        .with_fields(vec![col])
        .build()
        .unwrap()
}

/// Generate array contents for row `i`: (i % 5) + 1 doubles.
fn array_element_count(i: usize) -> usize {
    (i % 5) + 1
}

fn array_element_value(i: usize, j: usize) -> f64 {
    (i * 10 + j) as f64
}

/// Flatten arrays into (values, def_levels, rep_levels) for a 1D optional LIST.
/// Optional schema: max_def=3, max_rep=1.
///   null array   → (rep=0, def=0, no value)
///   first elem   → (rep=0, def=3, value)
///   next elems   → (rep=1, def=3, value)
fn flatten_arrays_optional(
    count: usize,
    nulls: &[bool],
) -> (Vec<f64>, Vec<i16>, Vec<i16>) {
    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();
    for i in 0..count {
        if nulls[i] {
            def_levels.push(0);
            rep_levels.push(0);
        } else {
            let len = array_element_count(i);
            for j in 0..len {
                values.push(array_element_value(i, j));
                def_levels.push(3);
                rep_levels.push(if j == 0 { 0 } else { 1 });
            }
        }
    }
    (values, def_levels, rep_levels)
}

/// Flatten arrays for a 1D required LIST.
/// Required schema: max_def=2, max_rep=1.
///   first elem → (rep=0, def=2, value)
///   next elems → (rep=1, def=2, value)
fn flatten_arrays_required(count: usize) -> (Vec<f64>, Vec<i16>, Vec<i16>) {
    let mut values = Vec::new();
    let mut def_levels = Vec::new();
    let mut rep_levels = Vec::new();
    for i in 0..count {
        let len = array_element_count(i);
        for j in 0..len {
            values.push(array_element_value(i, j));
            def_levels.push(2);
            rep_levels.push(if j == 0 { 0 } else { 1 });
        }
    }
    (values, def_levels, rep_levels)
}

fn write_double_array_parquet(
    schema: Type,
    values: &[f64],
    def_levels: &[i16],
    rep_levels: &[i16],
    col_type: ColumnType,
    version: WriterVersion,
    encoding: Encoding,
) -> Vec<u8> {
    let props = qdb_props_col_type(col_type, version, encoding);
    let mut cursor = Cursor::new(Vec::new());
    let mut file_writer =
        SerializedFileWriter::new(&mut cursor, Arc::new(schema), Arc::new(props))
            .expect("create file writer");

    let mut row_group_writer = file_writer.next_row_group().expect("next row group");
    if let Some(mut col_writer) = row_group_writer.next_column().expect("next column") {
        let typed = col_writer.typed::<DoubleType>();
        typed
            .write_batch(values, Some(def_levels), Some(rep_levels))
            .expect("write_batch");
        col_writer.close().expect("close column writer");
    }
    row_group_writer.close().expect("close row group writer");
    file_writer.close().expect("close file writer");
    cursor.into_inner()
}

/// Assert decoded output from the Double-typed LIST path.
/// Decoded format per non-null array: [u32: element_count][u32: pad=0][f64 × element_count]
fn assert_double_array(nulls: &[bool], data: &[u8], aux: &[u8]) {
    let row_count = nulls.len();
    assert_eq!(aux.len(), row_count * 16, "array aux size mismatch");

    for i in 0..row_count {
        let aux_base = i * 16;
        let offset =
            u64::from_le_bytes(aux[aux_base..aux_base + 8].try_into().unwrap()) as usize;
        let size =
            u64::from_le_bytes(aux[aux_base + 8..aux_base + 16].try_into().unwrap()) as usize;

        if nulls[i] {
            assert_eq!(size, 0, "row {i}: null array should have size 0");
        } else {
            let len = array_element_count(i);
            let expected_size = 8 + 8 * len; // shape header + elements
            assert_eq!(size, expected_size, "row {i}: array size mismatch");

            let arr = &data[offset..offset + size];

            // Shape header: [element_count: u32, pad: u32]
            let elem_count =
                u32::from_le_bytes(arr[0..4].try_into().unwrap()) as usize;
            let pad = u32::from_le_bytes(arr[4..8].try_into().unwrap());
            assert_eq!(elem_count, len, "row {i}: element count mismatch");
            assert_eq!(pad, 0, "row {i}: padding should be 0");

            // Elements
            for j in 0..len {
                let expected = array_element_value(i, j);
                let actual = f64::from_le_bytes(
                    arr[8 + j * 8..8 + (j + 1) * 8].try_into().unwrap(),
                );
                assert_eq!(
                    actual.to_bits(),
                    expected.to_bits(),
                    "row {i}: element {j} mismatch"
                );
            }
        }
    }
}

fn run_double_array_test(name: &str, encoding: Encoding) {
    let col_type = encode_array_type(ColumnTypeTag::Double, 1).unwrap();
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!("Testing {name} with version={version:?}, encoding={encoding:?}, null={null:?}");

            let nulls = generate_nulls(COUNT, *null);
            let (values, def_levels, rep_levels) = if matches!(null, Null::None) {
                flatten_arrays_required(COUNT)
            } else {
                flatten_arrays_optional(COUNT, &nulls)
            };
            let schema = list_double_schema(!matches!(null, Null::None));

            let buf = write_double_array_parquet(
                schema, &values, &def_levels, &rep_levels, col_type, *version, encoding,
            );
            let (data, aux) = decode_file(&buf);
            assert_double_array(&nulls, &data, &aux);
        }
    }
}

#[test]
fn test_array_double_plain() {
    run_double_array_test("DoubleArray", Encoding::Plain);
}

#[test]
fn test_array_double_rle_dictionary() {
    run_double_array_test("DoubleArray", Encoding::RleDictionary);
}
