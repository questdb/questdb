mod common;

use arrow::array::{Array, Float64Array, ListArray};
use common::encode::{
    generate_nulls, read_parquet_batches, write_parquet, EncodeEncoding, ALL_NULL_PATTERNS,
};
use common::types::primitives::rnd;
use qdb_core::col_type::{encode_array_type, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

const COUNT: usize = 200;

fn align8(n: usize) -> usize {
    (n + 7) & !7
}

/// Build QDB-format 1D Double array data.
///
/// Returns (primary_data, aux_data) where:
/// - primary_data: concatenated array payloads [u32 elem_count, u32 pad, f64...]
/// - aux_data: [u128; COUNT] packed aux entries (offset in low 48 bits, size in bits 64-95)
fn build_1d_double_arrays(nulls: &[bool]) -> (Vec<u8>, Vec<u8>, Vec<Option<Vec<f64>>>) {
    let count = nulls.len();
    let mut primary = Vec::new();
    let mut aux = Vec::new();
    let mut expected: Vec<Option<Vec<f64>>> = Vec::with_capacity(count);

    for (i, null) in nulls.iter().enumerate().take(COUNT) {
        if *null {
            // Null array: offset = primary.len(), size = 0
            let offset = primary.len() as u64;
            let size = 0u32;
            let packed: u128 = (offset as u128) | ((size as u128) << 64);
            aux.extend_from_slice(&packed.to_le_bytes());
            expected.push(None);
        } else {
            // Generate array of 1-5 elements
            let elem_count = (rnd(i) % 5) + 1;
            let offset = primary.len() as u64;

            // Shape header: [u32 elem_count, u32 pad=0]
            let shape_size = 4; // 1 dimension * 4 bytes
            let data_offset = align8(shape_size);
            let total_size = data_offset + elem_count * 8; // f64 = 8 bytes

            primary.extend_from_slice(&(elem_count as u32).to_le_bytes());
            // Pad to 8-byte alignment
            let padding = data_offset - shape_size;
            primary.extend(std::iter::repeat_n(0, padding));

            let mut values = Vec::with_capacity(elem_count);
            for j in 0..elem_count {
                let v = rnd(i * 100 + j) as f64 * 0.1;
                primary.extend_from_slice(&v.to_le_bytes());
                values.push(v);
            }

            let size = total_size as u32;
            let packed: u128 = (offset as u128) | ((size as u128) << 64);
            aux.extend_from_slice(&packed.to_le_bytes());
            expected.push(Some(values));
        }
    }

    (primary, aux, expected)
}

/// Build a Column for a 1D Double[] array and write it.
fn encode_and_read_array(
    primary: &[u8],
    aux: &[u8],
    row_count: usize,
    encoding: EncodeEncoding,
) -> Vec<arrow::record_batch::RecordBatch> {
    let col_type = encode_array_type(ColumnTypeTag::Double, 1).expect("array type");
    let column = questdbr::parquet_write::schema::Column::from_raw_data(
        0,
        "col",
        col_type.code(),
        0,
        row_count,
        primary.as_ptr(),
        primary.len(),
        aux.as_ptr(),
        aux.len(),
        std::ptr::null(),
        0,
        false,
        false,
        encoding.config(),
    )
    .expect("Column::from_raw_data");

    let partition = Partition {
        table: "test_table".to_string(),
        columns: vec![column],
    };
    let bytes = write_parquet(partition);
    read_parquet_batches(&bytes)
}

#[test]
fn test_encode_1d_double_array() {
    // Arrays only support Plain encoding (RleDictionary falls back to Plain for Array)
    let encoding = EncodeEncoding::Plain;
    for null_pattern in &ALL_NULL_PATTERNS {
        let nulls = generate_nulls(COUNT, *null_pattern);
        let (primary, aux, expected) = build_1d_double_arrays(&nulls);
        let batches = encode_and_read_array(&primary, &aux, COUNT, encoding);

        let arr = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("ListArray");
        assert_eq!(arr.len(), COUNT);

        for (i, exp) in expected.iter().enumerate() {
            match exp {
                None => {
                    assert!(
                        arr.is_null(i),
                        "Array {null_pattern:?}: expected null at {i}"
                    );
                }
                Some(values) => {
                    assert!(
                        !arr.is_null(i),
                        "Array {null_pattern:?}: expected non-null at {i}"
                    );
                    let inner = arr.value(i);
                    let f64_arr = inner
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("Float64Array");
                    assert_eq!(
                        f64_arr.len(),
                        values.len(),
                        "Array {null_pattern:?}: length mismatch at {i}"
                    );
                    for (j, &expected_val) in values.iter().enumerate() {
                        assert!(
                            !f64_arr.is_null(j),
                            "Array {null_pattern:?}: unexpected null element at [{i}][{j}]"
                        );
                        assert_eq!(
                            f64_arr.value(j).to_bits(),
                            expected_val.to_bits(),
                            "Array {null_pattern:?}: element mismatch at [{i}][{j}]"
                        );
                    }
                }
            }
        }
    }
}
