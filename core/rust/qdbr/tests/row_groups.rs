mod common;

use std::io::Cursor;
use std::sync::Arc;

use parquet::{
    basic::Repetition,
    data_type::{DataType, Int64Type},
    file::properties::WriterVersion,
    schema::types::Type,
};
use qdb_core::col_type::{nulls, ColumnType, ColumnTypeTag};
use questdbr::{
    allocator::{MemTracking, QdbAllocator},
    parquet_read::{DecodeContext, ParquetDecoder, RowGroupBuffers},
};

use common::{
    decode_file, decode_file_filtered, decode_file_filtered_fill_nulls, every_other_row_filter,
    qdb_props, qdb_props_with_column_top, write_parquet_column, Encoding,
};

use std::sync::atomic::AtomicUsize;

fn make_allocator() -> (Box<MemTracking>, Box<AtomicUsize>, QdbAllocator) {
    let mem_tracking = Box::new(MemTracking::new());
    let tagged_used = Box::new(AtomicUsize::new(0));
    let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);
    (mem_tracking, tagged_used, allocator)
}

fn write_i64_parquet(count: usize, version: WriterVersion, encoding: Encoding) -> Vec<u8> {
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(
            Type::primitive_type_builder("col", <Int64Type as DataType>::get_physical_type())
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
        )])
        .build()
        .unwrap();

    let values: Vec<i64> = (0..count).map(|i| i as i64 * 100).collect();
    let def_levels: Vec<i16> = vec![1; count];
    let props = qdb_props(ColumnTypeTag::Long, version, encoding);
    write_parquet_column::<Int64Type>("col", schema, &values, Some(&def_levels), Arc::new(props))
}

// --- Test 1: FILL_NULLS=true filtered path ---

#[test]
fn test_filtered_fill_nulls() {
    let count = 100;
    let buf = write_i64_parquet(count, WriterVersion::PARQUET_2_0, Encoding::Plain);
    let filter = every_other_row_filter(count); // [0, 2, 4, ...]

    let (data, _aux) = decode_file_filtered_fill_nulls(&buf, &filter);

    // With FILL_NULLS=true, output has the full row count
    assert_eq!(
        data.len(),
        count * size_of::<i64>(),
        "output should contain all rows"
    );

    let actual = data.as_ptr().cast::<i64>();
    for i in 0..count {
        let val = unsafe { std::ptr::read_unaligned(actual.add(i)) };
        if i % 2 == 0 {
            // Selected row: should have the original value
            assert_eq!(val, i as i64 * 100, "selected row {i} mismatch");
        } else {
            // Non-selected row: should be null sentinel
            assert_eq!(
                val,
                nulls::LONG,
                "non-selected row {i} should be null sentinel"
            );
        }
    }
}

// --- Test 2: Empty filter early-return ---

#[test]
fn test_empty_filter_returns_zero() {
    let buf = write_i64_parquet(50, WriterVersion::PARQUET_2_0, Encoding::Plain);
    let empty_filter: &[i64] = &[];

    let (data, aux) = decode_file_filtered(&buf, empty_filter);

    assert!(data.is_empty(), "data should be empty for empty filter");
    assert!(aux.is_empty(), "aux should be empty for empty filter");
}

// --- Test 3: Row group index out-of-range error ---

#[test]
fn test_row_group_index_out_of_range() {
    let buf = write_i64_parquet(10, WriterVersion::PARQUET_2_0, Encoding::Plain);

    let (_mt, _tu, allocator) = make_allocator();
    let buf_len = buf.len() as u64;
    let mut reader = Cursor::new(&buf);
    let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)
        .expect("ParquetDecoder::read");

    let mut rgb = RowGroupBuffers::new(allocator.clone());
    let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);
    let col_type = decoder.columns[0].column_type.unwrap();
    let columns = vec![(0i32, col_type)];

    // Use row_group_count (1 past the end) for decode_row_group
    let err = decoder
        .decode_row_group(&mut ctx, &mut rgb, &columns, decoder.row_group_count, 0, 10)
        .expect_err("should fail with out of range");
    assert!(
        format!("{err}").contains("out of range"),
        "error should mention 'out of range', got: {err}"
    );

    // Same test for decode_row_group_filtered
    let err = decoder
        .decode_row_group_filtered::<false>(
            &mut ctx,
            &mut rgb,
            0,
            &columns,
            decoder.row_group_count,
            0,
            10,
            &[0, 1],
        )
        .expect_err("should fail with out of range");
    assert!(
        format!("{err}").contains("out of range"),
        "filtered error should mention 'out of range', got: {err}"
    );
}

// --- Test 4: Column type mismatch error ---

#[test]
fn test_column_type_mismatch() {
    // Write a Long (Int64) column
    let buf = write_i64_parquet(10, WriterVersion::PARQUET_2_0, Encoding::Plain);

    let (_mt, _tu, allocator) = make_allocator();
    let buf_len = buf.len() as u64;
    let mut reader = Cursor::new(&buf);
    let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)
        .expect("ParquetDecoder::read");

    let mut rgb = RowGroupBuffers::new(allocator.clone());
    let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);

    // Request it as Boolean (type mismatch: file has Long, we request Boolean)
    let wrong_type = ColumnType::new(ColumnTypeTag::Boolean, 0);
    let columns = vec![(0i32, wrong_type)];

    let err = decoder
        .decode_row_group(&mut ctx, &mut rgb, &columns, 0, 0, 10)
        .expect_err("should fail with type mismatch");
    assert!(
        format!("{err}").contains("does not match"),
        "error should mention 'does not match', got: {err}"
    );
}

// --- Test 5: column_top >= row_group_hi branch ---

#[test]
fn test_column_top_exceeds_row_group() {
    let count = 50;
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(
            Type::primitive_type_builder("col", <Int64Type as DataType>::get_physical_type())
                .with_repetition(Repetition::OPTIONAL)
                .build()
                .unwrap(),
        )])
        .build()
        .unwrap();

    let values: Vec<i64> = (0..count).map(|i| i as i64).collect();
    let def_levels: Vec<i16> = vec![1; count];
    // Set column_top = 1000, which is >= row_group_hi (50)
    let props = qdb_props_with_column_top(
        ColumnTypeTag::Long,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        1000,
    );
    let buf = write_parquet_column::<Int64Type>(
        "col",
        schema,
        &values,
        Some(&def_levels),
        Arc::new(props),
    );

    let (data, aux) = decode_file(&buf);

    // When column_top >= row_group_hi, the column buffer is reset (empty)
    assert!(
        data.is_empty(),
        "data should be empty when column_top >= row_group_hi"
    );
    assert!(
        aux.is_empty(),
        "aux should be empty when column_top >= row_group_hi"
    );
}
