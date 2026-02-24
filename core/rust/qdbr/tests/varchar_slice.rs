mod common;

use std::io::Cursor;
use std::sync::Arc;

use parquet::basic::{Compression, LogicalType};
use parquet::data_type::{ByteArray, ByteArrayType};

use common::{
    def_levels_from_nulls, generate_nulls, non_null_only, optional_byte_array_schema,
    qdb_props_ascii, qdb_props_compressed_ascii, required_byte_array_schema,
    write_parquet_column, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
};
use qdb_core::col_type::ColumnTypeTag;
use questdbr::{
    allocator::{MemTracking, QdbAllocator},
    parquet_read::{DecodeContext, ParquetDecoder, RowGroupBuffers},
};
use std::sync::atomic::AtomicUsize;

const VARCHAR_SLICE_AUX_SIZE: usize = 16;

fn generate_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| ByteArray::from(format!("val_{i:04}").as_str()))
        .collect()
}

/// Read VarcharSlice aux entry fields at the given row index.
/// Returns (length: i32, reserved: u32, pointer: u64).
fn read_aux_entry(aux: &[u8], row: usize) -> (i32, u32, u64) {
    let base = row * VARCHAR_SLICE_AUX_SIZE;
    let length = i32::from_le_bytes([aux[base], aux[base + 1], aux[base + 2], aux[base + 3]]);
    let reserved =
        u32::from_le_bytes([aux[base + 4], aux[base + 5], aux[base + 6], aux[base + 7]]);
    let ptr = u64::from_le_bytes([
        aux[base + 8],
        aux[base + 9],
        aux[base + 10],
        aux[base + 11],
        aux[base + 12],
        aux[base + 13],
        aux[base + 14],
        aux[base + 15],
    ]);
    (length, reserved, ptr)
}

/// Represents a known valid memory range for pointer verification.
struct MemRange {
    start: u64,
    end: u64,
}

impl MemRange {
    fn new(ptr: *const u8, len: usize) -> Self {
        let start = ptr as u64;
        Self {
            start,
            end: start + len as u64,
        }
    }

    fn contains(&self, ptr: u64, len: usize) -> bool {
        ptr >= self.start && ptr + len as u64 <= self.end
    }
}

const ASCII_FLAG: u32 = 1;

/// Verify VarcharSlice aux buffer in-place while the underlying buffers are still alive.
///
/// For each row, verifies:
/// - NULL rows: length == -1, flags == 0, pointer == 0
/// - Non-NULL rows: length matches expected, pointer valid, and column-level ASCII flag correct
///
/// The `ascii` parameter is the column-level ASCII flag from metadata. All non-null entries
/// should have flags matching this column-level flag (not per-value).
///
/// `valid_ranges` contains the memory ranges where pointers may legitimately point
/// (file buffer, data_vec, page_buffers). Every non-null, non-empty pointer must fall
/// within a known valid range; an out-of-range pointer fails the test immediately
/// to catch stale/dangling pointer bugs.
fn assert_varchar_slice_aux(
    nulls: &[bool],
    aux: &[u8],
    expected_values: &[String],
    valid_ranges: &[MemRange],
    ascii: bool,
) {
    let row_count = nulls.len();
    assert_eq!(
        aux.len(),
        row_count * VARCHAR_SLICE_AUX_SIZE,
        "varchar_slice aux size mismatch: expected {} bytes for {row_count} rows, got {}",
        row_count * VARCHAR_SLICE_AUX_SIZE,
        aux.len()
    );

    let expected_flags = if ascii { ASCII_FLAG } else { 0 };

    for i in 0..row_count {
        let (length, reserved, ptr) = read_aux_entry(aux, i);

        if nulls[i] {
            assert_eq!(
                length, -1,
                "row {i}: null varchar_slice should have length == -1, got {length}"
            );
            assert_eq!(
                reserved, 0,
                "row {i}: null varchar_slice flags should be 0, got {reserved}"
            );
            assert_eq!(
                ptr, 0,
                "row {i}: null varchar_slice pointer should be 0, got {ptr:#x}"
            );
        } else {
            let expected_str = &expected_values[i];
            let expected_bytes = expected_str.as_bytes();
            let str_len = expected_bytes.len();

            assert_eq!(
                length as usize, str_len,
                "row {i}: varchar_slice length mismatch, expected {str_len}, got {length}"
            );
            // Column-level ASCII flag: all non-null entries should have the same flag
            assert_eq!(
                reserved, expected_flags,
                "row {i}: varchar_slice flags mismatch, expected {expected_flags}, got {reserved} \
                 (ascii={ascii}, value={expected_str:?})"
            );

            if str_len > 0 {
                assert_ne!(
                    ptr, 0,
                    "row {i}: non-null non-empty varchar_slice should have non-zero pointer"
                );

                let in_valid_range = valid_ranges.iter().any(|r| r.contains(ptr, str_len));
                assert!(
                    in_valid_range,
                    "row {i}: varchar_slice pointer {ptr:#x} (len={str_len}) is outside all \
                     known valid memory ranges — likely a stale/dangling pointer"
                );

                // SAFETY: We verified the pointer is within a live buffer
                // (file buffer, data_vec, or page_buffers).
                let actual_bytes =
                    unsafe { std::slice::from_raw_parts(ptr as *const u8, str_len) };
                assert_eq!(
                    actual_bytes, expected_bytes,
                    "row {i}: varchar_slice data mismatch, expected {:?}",
                    expected_str
                );
            }
        }
    }
}

/// Encode values into a Parquet file, decode as VarcharSlice, and verify the aux format.
///
/// This function keeps the RowGroupBuffers alive during verification so that
/// the pointers stored in aux entries remain valid and can be safely dereferenced.
/// The file buffer (`buf`) is also kept alive for the same reason.
fn encode_decode_and_verify_varchar_slice(
    values: &[ByteArray],
    nulls: &[bool],
    schema: parquet::schema::types::Type,
    props: parquet::file::properties::WriterProperties,
    expected_values: &[String],
    ascii: bool,
) {
    let non_null_values = non_null_only(values, nulls);
    let def_levels = def_levels_from_nulls(nulls);
    let buf = write_parquet_column::<ByteArrayType>(
        "col",
        schema,
        &non_null_values,
        Some(&def_levels),
        Arc::new(props),
    );

    // Set up allocator.
    let mem_tracking = Box::new(MemTracking::new());
    let tagged_used = Box::new(AtomicUsize::new(0));
    let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);

    let buf_len = buf.len() as u64;
    let mut reader = Cursor::new(&buf);
    let decoder = ParquetDecoder::read(allocator.clone(), &mut reader, buf_len)
        .expect("ParquetDecoder::read");

    assert_eq!(decoder.col_count, 1, "expected single column");
    let col_type = decoder.columns[0]
        .column_type
        .expect("column type should be recognized");

    let row_group_count = decoder.row_group_count;
    let mut rgb = RowGroupBuffers::new(allocator);
    let mut ctx = DecodeContext::new(buf.as_ptr(), buf_len);
    let columns = vec![(0i32, col_type)];

    // The file buffer is a known valid memory range for pointer verification.
    let file_range = MemRange::new(buf.as_ptr(), buf.len());

    // Track the cumulative row offset across row groups.
    let mut row_offset = 0usize;

    for rg_idx in 0..row_group_count {
        let rg_size = decoder.row_group_sizes[rg_idx as usize] as u32;
        decoder
            .decode_row_group(&mut ctx, &mut rgb, &columns, rg_idx, 0, rg_size)
            .unwrap_or_else(|e| panic!("decode row group {rg_idx}: {e}"));

        let bufs = &rgb.column_buffers()[0];
        let rg_row_count = bufs.aux_vec.len() / VARCHAR_SLICE_AUX_SIZE;

        // Build the list of valid memory ranges for pointer verification.
        // Pointers may point into:
        // - the original file buffer (for non-spill encodings with uncompressed data)
        // - data_vec (for DeltaByteArray spill path after fixup_pointers)
        // - page_buffers (for non-spill encodings with decompressed page data)
        let data_range = if !bufs.data_vec.is_empty() {
            Some(MemRange::new(bufs.data_vec.as_ptr(), bufs.data_vec.len()))
        } else {
            None
        };
        let page_ranges: Vec<MemRange> = bufs
            .page_buffers
            .iter()
            .filter(|pb| !pb.is_empty())
            .map(|pb| MemRange::new(pb.as_ptr(), pb.len()))
            .collect();

        let mut all_ranges = vec![MemRange {
            start: file_range.start,
            end: file_range.end,
        }];
        if let Some(ref dr) = data_range {
            all_ranges.push(MemRange {
                start: dr.start,
                end: dr.end,
            });
        }
        for pr in &page_ranges {
            all_ranges.push(MemRange {
                start: pr.start,
                end: pr.end,
            });
        }

        // Verify this row group's aux data while buffers are alive.
        let rg_nulls = &nulls[row_offset..row_offset + rg_row_count];
        assert_varchar_slice_aux(
            rg_nulls,
            &bufs.aux_vec,
            &expected_values[row_offset..],
            &all_ranges,
            ascii,
        );

        row_offset += rg_row_count;
    }

    assert_eq!(
        row_offset,
        nulls.len(),
        "total decoded rows should match input"
    );
}

fn run_varchar_slice_test(name: &str, encoding: Encoding) {
    run_varchar_slice_test_with_compression(name, encoding, Compression::UNCOMPRESSED);
}

fn run_varchar_slice_test_compressed(name: &str, encoding: Encoding) {
    run_varchar_slice_test_with_compression(name, encoding, Compression::SNAPPY);
}

fn run_varchar_slice_test_with_compression(
    name: &str,
    encoding: Encoding,
    compression: Compression,
) {
    for version in &VERSIONS {
        for null in &ALL_NULLS {
            eprintln!(
                "Testing {name} with version={version:?}, encoding={encoding:?}, \
                 compression={compression:?}, null={null:?}"
            );

            let nulls = generate_nulls(COUNT, *null);
            let values = generate_values(COUNT);
            let expected_values: Vec<String> =
                (0..COUNT).map(|i| format!("val_{i:04}")).collect();

            let schema = if matches!(null, Null::None) {
                required_byte_array_schema("col", Some(LogicalType::String))
            } else {
                optional_byte_array_schema("col", Some(LogicalType::String))
            };

            let props = qdb_props_compressed_ascii(
                ColumnTypeTag::VarcharSlice,
                *version,
                encoding,
                compression,
                true,
            );
            encode_decode_and_verify_varchar_slice(
                &values,
                &nulls,
                schema,
                props,
                &expected_values,
                true,
            );
        }
    }
}

#[test]
fn test_varchar_slice_plain() {
    run_varchar_slice_test("VarcharSlice", Encoding::Plain);
}

#[test]
fn test_varchar_slice_delta_length_byte_array() {
    run_varchar_slice_test("VarcharSlice", Encoding::DeltaLengthByteArray);
}

#[test]
fn test_varchar_slice_delta_byte_array() {
    run_varchar_slice_test("VarcharSlice", Encoding::DeltaByteArray);
}

#[test]
fn test_varchar_slice_rle_dictionary() {
    run_varchar_slice_test("VarcharSlice", Encoding::RleDictionary);
}

#[test]
fn test_varchar_slice_empty_strings() {
    let count = 1000;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| {
            if i % 3 == 0 {
                ByteArray::from("")
            } else {
                ByteArray::from(format!("val_{i:04}").as_str())
            }
        })
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| {
            if i % 3 == 0 {
                String::new()
            } else {
                format!("val_{i:04}")
            }
        })
        .collect();
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing empty strings with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, Encoding::Plain, true);
        encode_decode_and_verify_varchar_slice(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            true,
        );
    }
}

#[test]
fn test_varchar_slice_long_strings() {
    let count = 1000;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| {
            ByteArray::from(format!("this_is_a_long_string_value_for_row_{i:06}").as_str())
        })
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| format!("this_is_a_long_string_value_for_row_{i:06}"))
        .collect();
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing long strings with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, Encoding::Plain, true);
        encode_decode_and_verify_varchar_slice(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            true,
        );
    }
}

#[test]
fn test_varchar_slice_all_nulls() {
    let count = 500;
    let values: Vec<ByteArray> = Vec::new();
    let expected_values: Vec<String> = (0..count).map(|_| String::new()).collect();
    let nulls = vec![true; count];

    for version in &VERSIONS {
        eprintln!("Testing all nulls with version={version:?}");
        let schema = optional_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, Encoding::Plain, true);
        encode_decode_and_verify_varchar_slice(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            true,
        );
    }
}

#[test]
fn test_varchar_slice_utf8() {
    let count = 100;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| match i % 4 {
            0 => ByteArray::from("hello"),
            1 => ByteArray::from("h\u{00e9}llo w\u{00f6}rld"),
            2 => ByteArray::from("\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}"),
            _ => ByteArray::from("\u{1f389}\u{1f38a}\u{1f388}"),
        })
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| match i % 4 {
            0 => "hello".to_string(),
            1 => "h\u{00e9}llo w\u{00f6}rld".to_string(),
            2 => "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}".to_string(),
            _ => "\u{1f389}\u{1f38a}\u{1f388}".to_string(),
        })
        .collect();
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing UTF-8 strings with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, Encoding::Plain, false);
        encode_decode_and_verify_varchar_slice(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            false,
        );
    }
}

// Compressed (Snappy) variants — exercise the page_buffers memory-lifetime path.
// With compression, decompressed page data is stored in per-page buffers
// (page_buffers) that must remain alive while VarcharSlice pointers reference them.

#[test]
fn test_varchar_slice_plain_compressed() {
    run_varchar_slice_test_compressed("VarcharSlice compressed", Encoding::Plain);
}

#[test]
fn test_varchar_slice_delta_length_byte_array_compressed() {
    run_varchar_slice_test_compressed("VarcharSlice compressed", Encoding::DeltaLengthByteArray);
}

#[test]
fn test_varchar_slice_delta_byte_array_compressed() {
    run_varchar_slice_test_compressed("VarcharSlice compressed", Encoding::DeltaByteArray);
}

#[test]
fn test_varchar_slice_rle_dictionary_compressed() {
    run_varchar_slice_test_compressed("VarcharSlice compressed", Encoding::RleDictionary);
}
