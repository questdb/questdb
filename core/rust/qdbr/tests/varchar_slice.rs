mod common;

use std::io::Cursor;
use std::sync::Arc;

use parquet::basic::{Compression, LogicalType};
use parquet::data_type::{ByteArray, ByteArrayType};

use common::{
    def_levels_from_nulls, every_other_row_filter, generate_nulls, non_null_only,
    optional_byte_array_schema, qdb_props_ascii, qdb_props_compressed_ascii,
    required_byte_array_schema, write_parquet_column, Encoding, Null, ALL_NULLS, COUNT, VERSIONS,
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
/// Returns (header: u32, reserved: u32, pointer: u64).
///
/// Header format (VARCHAR-compatible):
///   NULL:     0x00000004 (bit 2 set)
///   Non-null: (length << 4) | flags
///     bit 0 = 1 (always set for non-null)
///     bit 1 = ASCII flag
///     bit 2 = 0 (null flag, never set for non-null)
fn read_aux_entry(aux: &[u8], row: usize) -> (u32, u32, u64) {
    let base = row * VARCHAR_SLICE_AUX_SIZE;
    let header = u32::from_le_bytes([aux[base], aux[base + 1], aux[base + 2], aux[base + 3]]);
    let reserved = u32::from_le_bytes([aux[base + 4], aux[base + 5], aux[base + 6], aux[base + 7]]);
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
    (header, reserved, ptr)
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

/// NULL header sentinel: bit 2 set (= VARCHAR_HEADER_FLAG_NULL = 4).
const SLICE_NULL_HEADER: u32 = 4;

/// Verify VarcharSlice aux buffer in-place while the underlying buffers are still alive.
///
/// For each row, verifies:
/// - NULL rows: header == 4 (SLICE_NULL_HEADER), reserved == 0, pointer == 0
/// - Non-NULL rows: header encodes correct length and ascii flag, pointer valid
///
/// The `ascii` parameter is the column-level ASCII flag from metadata.
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

    for i in 0..row_count {
        let (header, reserved, ptr) = read_aux_entry(aux, i);

        if nulls[i] {
            assert_eq!(
                header, SLICE_NULL_HEADER,
                "row {i}: null varchar_slice should have header == {SLICE_NULL_HEADER}, got {header}"
            );
            assert_eq!(
                reserved, 0,
                "row {i}: null varchar_slice reserved should be 0, got {reserved}"
            );
            assert_eq!(
                ptr, 0,
                "row {i}: null varchar_slice pointer should be 0, got {ptr:#x}"
            );
        } else {
            let expected_str = &expected_values[i];
            let expected_bytes = expected_str.as_bytes();
            let str_len = expected_bytes.len();

            // Verify header: (len << 4) | flags
            let decoded_len = (header >> 4) as usize;
            assert_eq!(
                decoded_len, str_len,
                "row {i}: varchar_slice length mismatch, expected {str_len}, got {decoded_len}"
            );
            // bit 0 must always be set for non-null
            assert_ne!(
                header & 1,
                0,
                "row {i}: non-null varchar_slice header bit 0 must be set, got header={header:#x}"
            );
            // bit 2 (null flag) must be clear for non-null
            assert_eq!(
                header & 4, 0,
                "row {i}: non-null varchar_slice header bit 2 (null) must be clear, got header={header:#x}"
            );
            // Check ASCII flag (bit 1)
            let expected_ascii = ascii || str_len == 0;
            let actual_ascii = (header & 2) != 0;
            assert_eq!(
                actual_ascii, expected_ascii,
                "row {i}: varchar_slice ASCII flag mismatch, expected {expected_ascii}, got {actual_ascii} \
                 (header={header:#x}, value={expected_str:?})"
            );
            // Reserved field must be zero
            assert_eq!(
                reserved, 0,
                "row {i}: varchar_slice reserved should be 0, got {reserved}"
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
                let actual_bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, str_len) };
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
            let expected_values: Vec<String> = (0..COUNT).map(|i| format!("val_{i:04}")).collect();

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
        .map(|i| ByteArray::from(format!("this_is_a_long_string_value_for_row_{i:06}").as_str()))
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
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            false,
        );
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

/// Helper that runs a test across all 4 encodings x compressed/uncompressed x parquet versions.
fn run_varchar_slice_test_encoding_matrix(
    name: &str,
    values: &[ByteArray],
    nulls: &[bool],
    expected_values: &[String],
    ascii: bool,
) {
    let encodings = [
        Encoding::Plain,
        Encoding::DeltaLengthByteArray,
        Encoding::DeltaByteArray,
        Encoding::RleDictionary,
    ];
    let compressions = [Compression::UNCOMPRESSED, Compression::SNAPPY];

    let has_nulls = nulls.iter().any(|&n| n);
    for version in &VERSIONS {
        for encoding in &encodings {
            for compression in &compressions {
                eprintln!(
                    "Testing {name} with version={version:?}, encoding={encoding:?}, \
                     compression={compression:?}"
                );

                let schema = if has_nulls {
                    optional_byte_array_schema("col", Some(LogicalType::String))
                } else {
                    required_byte_array_schema("col", Some(LogicalType::String))
                };

                let props = qdb_props_compressed_ascii(
                    ColumnTypeTag::VarcharSlice,
                    *version,
                    *encoding,
                    *compression,
                    ascii,
                );
                encode_decode_and_verify_varchar_slice(
                    values,
                    nulls,
                    schema,
                    props,
                    expected_values,
                    ascii,
                );
            }
        }
    }
}

// --- Non-ASCII with specific encodings ---

fn utf8_values(count: usize) -> Vec<ByteArray> {
    (0..count)
        .map(|i| match i % 4 {
            0 => ByteArray::from("hello"),
            1 => ByteArray::from("h\u{00e9}llo w\u{00f6}rld"),
            2 => ByteArray::from("\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}"),
            _ => ByteArray::from("\u{1f389}\u{1f38a}\u{1f388}"),
        })
        .collect()
}

fn utf8_expected(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| match i % 4 {
            0 => "hello".to_string(),
            1 => "h\u{00e9}llo w\u{00f6}rld".to_string(),
            2 => "\u{3053}\u{3093}\u{306b}\u{3061}\u{306f}".to_string(),
            _ => "\u{1f389}\u{1f38a}\u{1f388}".to_string(),
        })
        .collect()
}

#[test]
fn test_varchar_slice_utf8_delta_length() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing UTF-8 DeltaLengthByteArray with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::DeltaLengthByteArray,
            false,
        );
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

#[test]
fn test_varchar_slice_utf8_delta_byte_array() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing UTF-8 DeltaByteArray with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::DeltaByteArray,
            false,
        );
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

#[test]
fn test_varchar_slice_utf8_rle_dictionary() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing UTF-8 RleDictionary with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::RleDictionary,
            false,
        );
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

// --- Edge-case value patterns with all 4 encodings ---

#[test]
fn test_varchar_slice_empty_strings_all_encodings() {
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

    run_varchar_slice_test_encoding_matrix(
        "empty strings all encodings",
        &values,
        &nulls,
        &expected_values,
        true,
    );
}

#[test]
fn test_varchar_slice_long_strings_all_encodings() {
    let count = 1000;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| ByteArray::from(format!("this_is_a_long_string_value_for_row_{i:06}").as_str()))
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| format!("this_is_a_long_string_value_for_row_{i:06}"))
        .collect();
    let nulls = vec![false; count];

    run_varchar_slice_test_encoding_matrix(
        "long strings all encodings",
        &values,
        &nulls,
        &expected_values,
        true,
    );
}

#[test]
fn test_varchar_slice_single_value() {
    let values = vec![ByteArray::from("single")];
    let expected_values = vec!["single".to_string()];
    let nulls = vec![false];

    run_varchar_slice_test_encoding_matrix("single value", &values, &nulls, &expected_values, true);
}

#[test]
fn test_varchar_slice_mixed_nulls_and_empty() {
    let count = 100;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| match i % 5 {
            0 => ByteArray::from(""),      // empty string (non-null)
            1 => ByteArray::from("hello"), // normal value
            2 => ByteArray::from(""),      // empty string (non-null)
            _ => ByteArray::from(format!("v_{i:03}").as_str()),
        })
        .collect();
    // Nulls at every 7th row
    let nulls: Vec<bool> = (0..count).map(|i| i % 7 == 0).collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| {
            if nulls[i] {
                String::new() // placeholder for null rows
            } else {
                match i % 5 {
                    0 => String::new(),
                    1 => "hello".to_string(),
                    2 => String::new(),
                    _ => format!("v_{i:03}"),
                }
            }
        })
        .collect();

    run_varchar_slice_test_encoding_matrix(
        "mixed nulls and empty",
        &values,
        &nulls,
        &expected_values,
        true,
    );
}

// --- Compressed variants of edge cases ---

#[test]
fn test_varchar_slice_empty_strings_compressed() {
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
        eprintln!("Testing empty strings compressed with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_compressed_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            Compression::SNAPPY,
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

#[test]
fn test_varchar_slice_long_strings_compressed() {
    let count = 1000;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| ByteArray::from(format!("this_is_a_long_string_value_for_row_{i:06}").as_str()))
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| format!("this_is_a_long_string_value_for_row_{i:06}"))
        .collect();
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing long strings compressed with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_compressed_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            Compression::SNAPPY,
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

#[test]
fn test_varchar_slice_utf8_compressed() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing UTF-8 compressed with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_compressed_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            Compression::SNAPPY,
            false,
        );
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

#[test]
fn test_varchar_slice_all_nulls_compressed() {
    let count = 500;
    let values: Vec<ByteArray> = Vec::new();
    let expected_values: Vec<String> = (0..count).map(|_| String::new()).collect();
    let nulls = vec![true; count];

    for version in &VERSIONS {
        eprintln!("Testing all nulls compressed with version={version:?}");
        let schema = optional_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_compressed_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            Compression::SNAPPY,
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

// --- High volume ---

#[test]
fn test_varchar_slice_large_count() {
    let count = 10_000;
    let values: Vec<ByteArray> = (0..count)
        .map(|i| ByteArray::from(format!("val_{i:06}").as_str()))
        .collect();
    let expected_values: Vec<String> = (0..count).map(|i| format!("val_{i:06}")).collect();
    let nulls = vec![false; count];

    for version in &VERSIONS {
        for encoding in &[Encoding::DeltaLengthByteArray, Encoding::DeltaByteArray] {
            eprintln!("Testing large count with version={version:?}, encoding={encoding:?}");
            let schema = required_byte_array_schema("col", Some(LogicalType::String));
            let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, *encoding, true);
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

// --- Filtered decode (skip) path ---

/// Encode values into a Parquet file, decode with a row filter, and verify the aux format.
///
/// Similar to `encode_decode_and_verify_varchar_slice` but uses `decode_row_group_filtered`
/// and builds expected results by picking only the rows specified in `rows_filter`.
/// Keeps RowGroupBuffers alive during verification so pointers remain valid.
fn encode_decode_and_verify_varchar_slice_filtered(
    values: &[ByteArray],
    nulls: &[bool],
    schema: parquet::schema::types::Type,
    props: parquet::file::properties::WriterProperties,
    expected_values: &[String],
    ascii: bool,
    rows_filter: &[i64],
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

    let file_range = MemRange::new(buf.as_ptr(), buf.len());

    let mut row_offset = 0usize;

    for rg_idx in 0..row_group_count {
        let rg_size = decoder.row_group_sizes[rg_idx as usize] as u32;

        // Build filter indices relative to this row group.
        let rg_filter: Vec<i64> = rows_filter
            .iter()
            .filter(|&&idx| {
                let i = idx as usize;
                i >= row_offset && i < row_offset + rg_size as usize
            })
            .map(|&idx| idx - row_offset as i64)
            .collect();

        decoder
            .decode_row_group_filtered::<false>(
                &mut ctx, &mut rgb, 0, &columns, rg_idx, 0, rg_size, &rg_filter,
            )
            .unwrap_or_else(|e| panic!("decode row group filtered {rg_idx}: {e}"));

        let bufs = &rgb.column_buffers()[0];
        let rg_row_count = bufs.aux_vec.len() / VARCHAR_SLICE_AUX_SIZE;

        // Build expected filtered nulls and values.
        let filtered_nulls: Vec<bool> = rg_filter
            .iter()
            .map(|&idx| nulls[row_offset + idx as usize])
            .collect();
        let filtered_expected: Vec<String> = rg_filter
            .iter()
            .map(|&idx| expected_values[row_offset + idx as usize].clone())
            .collect();

        assert_eq!(
            rg_row_count,
            filtered_nulls.len(),
            "filtered row count mismatch in row group {rg_idx}"
        );

        // Build valid memory ranges.
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

        assert_varchar_slice_aux(
            &filtered_nulls,
            &bufs.aux_vec,
            &filtered_expected,
            &all_ranges,
            ascii,
        );

        row_offset += rg_size as usize;
    }
}

fn run_varchar_slice_filtered_test(name: &str, encoding: Encoding) {
    run_varchar_slice_filtered_test_with_compression(name, encoding, Compression::UNCOMPRESSED);
}

fn run_varchar_slice_filtered_test_with_compression(
    name: &str,
    encoding: Encoding,
    compression: Compression,
) {
    for version in &VERSIONS {
        for null in &[Null::None, Null::Sparse] {
            let count = COUNT;
            let nulls = generate_nulls(count, *null);
            let values = generate_values(count);
            let expected_values: Vec<String> = (0..count).map(|i| format!("val_{i:04}")).collect();
            let rows_filter = every_other_row_filter(count);

            eprintln!(
                "Testing filtered {name} with version={version:?}, encoding={encoding:?}, \
                 compression={compression:?}, null={null:?}"
            );

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
            encode_decode_and_verify_varchar_slice_filtered(
                &values,
                &nulls,
                schema,
                props,
                &expected_values,
                true,
                &rows_filter,
            );
        }
    }
}

#[test]
fn test_varchar_slice_filtered_plain() {
    run_varchar_slice_filtered_test("VarcharSlice filtered", Encoding::Plain);
}

#[test]
fn test_varchar_slice_filtered_delta_length() {
    run_varchar_slice_filtered_test("VarcharSlice filtered", Encoding::DeltaLengthByteArray);
}

#[test]
fn test_varchar_slice_filtered_delta_byte_array() {
    run_varchar_slice_filtered_test("VarcharSlice filtered", Encoding::DeltaByteArray);
}

#[test]
fn test_varchar_slice_filtered_rle_dictionary() {
    run_varchar_slice_filtered_test("VarcharSlice filtered", Encoding::RleDictionary);
}

#[test]
fn test_varchar_slice_filtered_compressed() {
    run_varchar_slice_filtered_test_with_compression(
        "VarcharSlice filtered compressed",
        Encoding::Plain,
        Compression::SNAPPY,
    );
}

#[test]
fn test_varchar_slice_filtered_with_nulls() {
    for version in &VERSIONS {
        let count = COUNT;
        let nulls = generate_nulls(count, Null::Dense);
        let values = generate_values(count);
        let expected_values: Vec<String> = (0..count).map(|i| format!("val_{i:04}")).collect();
        let rows_filter = every_other_row_filter(count);

        eprintln!("Testing filtered with dense nulls, version={version:?}");

        let schema = optional_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(ColumnTypeTag::VarcharSlice, *version, Encoding::Plain, true);
        encode_decode_and_verify_varchar_slice_filtered(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            true,
            &rows_filter,
        );
    }
}

// --- RLE dict single distinct value (num_bits==0) ---

#[test]
fn test_varchar_slice_rle_dict_single_distinct_value() {
    let count = 1000;
    let values: Vec<ByteArray> = vec![ByteArray::from("same_val"); count];
    let expected_values: Vec<String> = vec!["same_val".to_string(); count];
    let nulls = vec![false; count];

    for version in &VERSIONS {
        eprintln!("Testing RLE dict single distinct value with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::RleDictionary,
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

// --- Consecutive null runs (push_nulls bulk path) ---

#[test]
fn test_varchar_slice_consecutive_nulls() {
    let count = 500;
    let nulls: Vec<bool> = (0..count).map(|i| i < 200).collect();
    // Non-null values for rows 200..499
    let values: Vec<ByteArray> = (0..count)
        .map(|i| ByteArray::from(format!("val_{i:04}").as_str()))
        .collect();
    let expected_values: Vec<String> = (0..count)
        .map(|i| {
            if nulls[i] {
                String::new()
            } else {
                format!("val_{i:04}")
            }
        })
        .collect();

    for version in &VERSIONS {
        eprintln!("Testing consecutive nulls with version={version:?}");
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

// --- Filtered non-ASCII and encoding matrix ---

#[test]
fn test_varchar_slice_filtered_utf8() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];
    let rows_filter = every_other_row_filter(count);

    for version in &VERSIONS {
        eprintln!("Testing filtered UTF-8 with version={version:?}");
        let schema = required_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::Plain,
            false,
        );
        encode_decode_and_verify_varchar_slice_filtered(
            &values,
            &nulls,
            schema,
            props,
            &expected_values,
            false,
            &rows_filter,
        );
    }
}

/// Helper that runs a filtered test across all 4 encodings x compressed/uncompressed x parquet versions.
fn run_varchar_slice_filtered_test_encoding_matrix(
    name: &str,
    values: &[ByteArray],
    nulls: &[bool],
    expected_values: &[String],
    ascii: bool,
) {
    let encodings = [
        Encoding::Plain,
        Encoding::DeltaLengthByteArray,
        Encoding::DeltaByteArray,
        Encoding::RleDictionary,
    ];
    let compressions = [Compression::UNCOMPRESSED, Compression::SNAPPY];
    let rows_filter = every_other_row_filter(nulls.len());

    let has_nulls = nulls.iter().any(|&n| n);
    for version in &VERSIONS {
        for encoding in &encodings {
            for compression in &compressions {
                eprintln!(
                    "Testing filtered {name} with version={version:?}, encoding={encoding:?}, \
                     compression={compression:?}"
                );

                let schema = if has_nulls {
                    optional_byte_array_schema("col", Some(LogicalType::String))
                } else {
                    required_byte_array_schema("col", Some(LogicalType::String))
                };

                let props = qdb_props_compressed_ascii(
                    ColumnTypeTag::VarcharSlice,
                    *version,
                    *encoding,
                    *compression,
                    ascii,
                );
                encode_decode_and_verify_varchar_slice_filtered(
                    values,
                    nulls,
                    schema,
                    props,
                    expected_values,
                    ascii,
                    &rows_filter,
                );
            }
        }
    }
}

#[test]
fn test_varchar_slice_filtered_encoding_matrix() {
    let count = COUNT;
    let values = generate_values(count);
    let expected_values: Vec<String> = (0..count).map(|i| format!("val_{i:04}")).collect();
    let nulls = generate_nulls(count, Null::Sparse);

    run_varchar_slice_filtered_test_encoding_matrix(
        "encoding matrix",
        &values,
        &nulls,
        &expected_values,
        true,
    );
}

#[test]
fn test_varchar_slice_filtered_utf8_encoding_matrix() {
    let count = 100;
    let values = utf8_values(count);
    let expected_values = utf8_expected(count);
    let nulls = vec![false; count];

    run_varchar_slice_filtered_test_encoding_matrix(
        "UTF-8 encoding matrix",
        &values,
        &nulls,
        &expected_values,
        false,
    );
}

// --- All-null with RLE dictionary encoding ---

#[test]
fn test_varchar_slice_all_nulls_rle_dictionary() {
    let count = 500;
    let values: Vec<ByteArray> = Vec::new();
    let expected_values: Vec<String> = (0..count).map(|_| String::new()).collect();
    let nulls = vec![true; count];

    for version in &VERSIONS {
        eprintln!("Testing all nulls RLE dictionary with version={version:?}");
        let schema = optional_byte_array_schema("col", Some(LogicalType::String));
        let props = qdb_props_ascii(
            ColumnTypeTag::VarcharSlice,
            *version,
            Encoding::RleDictionary,
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
