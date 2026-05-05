//! Integration tests for the `pm_inspect` CLI binary.
//!
//! Each test creates parquet + `_pm` files in a temp directory, invokes the
//! binary via `std::process::Command`, and asserts on stdout/stderr/exit code.

mod common;

use std::io::Cursor;
use std::path::PathBuf;
use std::process::{Command, Output};
use std::ptr::null;
use std::sync::Arc;

use parquet::basic::{Compression as ParquetCompression, Repetition};
use parquet::data_type::DataType;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::Type;

use common::{
    def_levels_from_nulls, generate_nulls, qdb_props, qdb_props_compressed,
    types::primitives::{generate_data, Int, PrimitiveType, Timestamp},
    write_parquet_column, Encoding, Null,
};

use parquet2::read::read_metadata_with_size;
use questdbr::parquet::qdb_metadata::QdbMeta;
use questdbr::parquet_metadata::convert::convert_from_parquet;
use questdbr::parquet_metadata::row_group::RowGroupBlockBuilder;
use questdbr::parquet_metadata::types::ColumnFlags;
use questdbr::parquet_metadata::writer::ParquetMetaWriter;

use tempfile::TempDir;

// ── Helpers ──────────────────────────────────────────────────────────

fn pm_inspect_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_pm_inspect"))
}

fn run_inspect(args: &[&str]) -> Output {
    Command::new(pm_inspect_bin())
        .args(args)
        .output()
        .expect("failed to execute pm_inspect")
}

fn read_parquet2_metadata(parquet_bytes: &[u8]) -> (parquet2::metadata::FileMetaData, u64, u32) {
    let buf_len = parquet_bytes.len() as u64;
    let mut cursor = Cursor::new(parquet_bytes);
    let metadata = read_metadata_with_size(&mut cursor, buf_len).expect("read parquet metadata");

    let footer_len_offset = parquet_bytes.len() - 8;
    let thrift_footer_len = u32::from_le_bytes(
        parquet_bytes[footer_len_offset..footer_len_offset + 4]
            .try_into()
            .unwrap(),
    );
    let parquet_footer_offset = buf_len - 8 - thrift_footer_len as u64;
    // Use raw thrift footer length (not +8) to match pm_generate convention.
    let parquet_footer_length = thrift_footer_len;

    (metadata, parquet_footer_offset, parquet_footer_length)
}

fn extract_qdb_meta(metadata: &parquet2::metadata::FileMetaData) -> Option<QdbMeta> {
    metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize QdbMeta"))
}

fn make_pm_bytes(parquet_bytes: &[u8]) -> Vec<u8> {
    let (metadata, footer_offset, footer_length) = read_parquet2_metadata(parquet_bytes);
    let qdb_meta = extract_qdb_meta(&metadata);
    let (pm_bytes, _) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        footer_offset,
        footer_length,
        None,
    )
    .expect("convert_from_parquet");
    pm_bytes
}

/// Write `data.parquet` and `_pm` as siblings in a temp directory.
/// Returns (TempDir, pm_path, parquet_path).
fn write_fixtures(parquet_bytes: &[u8]) -> (TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("create temp dir");
    let parquet_path = dir.path().join("data.parquet");
    let pm_path = dir.path().join("_pm");

    std::fs::write(&parquet_path, parquet_bytes).expect("write parquet");
    let pm_bytes = make_pm_bytes(parquet_bytes);
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");

    (dir, pm_path, parquet_path)
}

fn stdout_str(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr_str(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

fn make_single_column_parquet<T: PrimitiveType>(col_name: &str, count: usize, null: Null) -> Vec<u8>
where
    T::T: Clone,
{
    let nulls = generate_nulls(count, null);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet_vals, _) = generate_data::<T>(count - null_count);

    let repetition = if matches!(null, Null::None) {
        Repetition::REQUIRED
    } else {
        Repetition::OPTIONAL
    };

    let mut col_builder =
        Type::primitive_type_builder(col_name, <T::U as DataType>::get_physical_type())
            .with_repetition(repetition);
    if let Some(len) = T::FIXED_LEN {
        col_builder = col_builder.with_length(len);
    }
    if let Some(lt) = T::LOGICAL_TYPE {
        if let parquet::basic::LogicalType::Decimal { scale, precision } = lt {
            col_builder = col_builder.with_precision(precision).with_scale(scale);
        }
        col_builder = col_builder.with_logical_type(Some(lt));
    }
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();

    let def_levels = def_levels_from_nulls(&nulls);
    let props = qdb_props(T::TAG, WriterVersion::PARQUET_2_0, Encoding::Plain);
    write_parquet_column::<T::U>(
        col_name,
        schema,
        &parquet_vals,
        Some(&def_levels),
        Arc::new(props),
    )
}

fn make_single_column_parquet_compressed<T: PrimitiveType>(
    col_name: &str,
    count: usize,
    compression: ParquetCompression,
) -> Vec<u8>
where
    T::T: Clone,
{
    let nulls = generate_nulls(count, Null::None);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet_vals, _) = generate_data::<T>(count - null_count);

    let col_builder =
        Type::primitive_type_builder(col_name, <T::U as DataType>::get_physical_type())
            .with_repetition(Repetition::REQUIRED);
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();

    let def_levels = def_levels_from_nulls(&nulls);
    let props = qdb_props_compressed(
        T::TAG,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        compression,
    );
    write_parquet_column::<T::U>(
        col_name,
        schema,
        &parquet_vals,
        Some(&def_levels),
        Arc::new(props),
    )
}

fn make_multi_row_group_parquet(row_count: usize, row_group_size: usize) -> Vec<u8> {
    use questdbr::parquet_write::schema::{Column, Partition};
    use questdbr::parquet_write::ParquetWriter;

    let ts_data: Vec<i64> = (0..row_count).map(|i| (i as i64) * 1_000_000).collect();

    let ts_col = Column::from_raw_data(
        0,
        "ts",
        8, // Timestamp
        0,
        row_count,
        ts_data.as_ptr() as *const u8,
        ts_data.len() * std::mem::size_of::<i64>(),
        null(),
        0,
        null(),
        0,
        true,
        false,
        0,
    )
    .expect("ts column");

    let partition = Partition {
        table: "test".to_string(),
        columns: vec![ts_col],
    };

    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_row_group_size(Some(row_group_size))
        .finish(partition)
        .expect("ParquetWriter::finish");
    buf.into_inner()
}

// ── CLI error handling ───────────────────────────────────────────────

#[test]
fn no_args_prints_usage() {
    let output = Command::new(pm_inspect_bin())
        .output()
        .expect("execute pm_inspect");
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_str(&output).contains("Usage:"));
}

#[test]
fn file_not_found() {
    let output = run_inspect(&["/nonexistent/path/_pm"]);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_str(&output).contains("File not found"));
}

#[test]
fn invalid_pm_file() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let bad_file = dir.path().join("_pm");
    std::fs::write(&bad_file, b"not a valid _pm file at all").expect("write");

    let output = run_inspect(&[bad_file.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    assert!(stderr_str(&output).contains("Failed to parse"));
}

#[test]
fn directory_arg_resolves_pm() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let (dir, _pm_path, _parquet_path) = write_fixtures(&parquet_bytes);

    // Pass the directory, not the _pm file directly.
    let output = run_inspect(&[dir.path().to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));
    let out = stdout_str(&output);
    assert!(out.contains("Checksum: OK"));
}

// ── Dump mode output ─────────────────────────────────────────────────

#[test]
fn dump_single_column() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let (dir, pm_path, _) = write_fixtures(&parquet_bytes);
    let _ = dir; // keep alive

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(out.contains("Checksum: OK"), "missing checksum");
    assert!(out.contains("Columns: 1"), "missing column count");
    assert!(
        out.contains("Designated timestamp:"),
        "missing designated timestamp"
    );
    assert!(
        out.contains("=== Column Descriptors ==="),
        "missing column descriptors section"
    );
    assert!(out.contains("\"col\""), "missing column name");
    assert!(out.contains("=== Footer ==="), "missing footer section");
    assert!(out.contains("Row groups: 1"), "missing row group count");
    assert!(
        out.contains("=== Row Group 0 ==="),
        "missing row group header"
    );
    assert!(out.contains("Rows: 100"), "missing row count");
    assert!(out.contains("Codec:"), "missing codec");
    assert!(out.contains("Encodings:"), "missing encodings");
    assert!(out.contains("Byte range:"), "missing byte range");
    assert!(out.contains("Values: 100"), "missing values");
    assert!(out.contains("required"), "expected 'required' repetition");
}

#[test]
fn dump_nullable_column() {
    let parquet_bytes = make_single_column_parquet::<Int>("col", 100, Null::Sparse);
    let (dir, pm_path, _) = write_fixtures(&parquet_bytes);
    let _ = dir;

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(out.contains("optional"), "expected 'optional' repetition");
    assert!(out.contains("Null count:"), "missing null count stat");
}

#[test]
fn dump_compressed_codec() {
    let parquet_bytes =
        make_single_column_parquet_compressed::<Timestamp>("col", 100, ParquetCompression::SNAPPY);
    let (dir, pm_path, _) = write_fixtures(&parquet_bytes);
    let _ = dir;

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(out.contains("Codec: Snappy"), "expected Snappy codec");
}

#[test]
fn dump_multiple_row_groups() {
    let parquet_bytes = make_multi_row_group_parquet(300, 100);
    let (dir, pm_path, _) = write_fixtures(&parquet_bytes);
    let _ = dir;

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(out.contains("Row groups: 3"), "expected 3 row groups");
    assert!(out.contains("=== Row Group 0 ==="), "missing rg 0");
    assert!(out.contains("=== Row Group 1 ==="), "missing rg 1");
    assert!(out.contains("=== Row Group 2 ==="), "missing rg 2");
}

// ── Dump mode — bloom filters ────────────────────────────────────────

/// Build a _pm file directly via ParquetMetaWriter with inlined bloom filters.
fn make_pm_with_inlined_bloom_filters() -> Vec<u8> {
    let mut w = ParquetMetaWriter::new();
    w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
    w.add_column("b", 1, 6, ColumnFlags::new(), 0, 0, 0, 0);

    let bitset_a = vec![0xAA_u8; 64];
    let bitset_b = vec![0xBB_u8; 64];

    let mut rg = RowGroupBlockBuilder::new(2);
    rg.set_num_rows(100);
    rg.add_bloom_filter(0, &bitset_a).unwrap();
    rg.add_bloom_filter(1, &bitset_b).unwrap();
    w.add_row_group(rg);

    let (bytes, _) = w.finish().unwrap();
    bytes
}

/// Build a _pm file directly via ParquetMetaWriter with external bloom filters.
fn make_pm_with_external_bloom_filters() -> Vec<u8> {
    let mut w = ParquetMetaWriter::new();
    w.add_column("a", 0, 5, ColumnFlags::new(), 0, 0, 0, 0);
    w.set_bloom_filters_external(true);

    let mut rg = RowGroupBlockBuilder::new(1);
    rg.set_num_rows(100);
    rg.add_external_bloom_filter(0, 4096, 512).unwrap();
    w.add_row_group(rg);

    let (bytes, _) = w.finish().unwrap();
    bytes
}

#[test]
fn dump_bloom_filters_inlined() {
    let pm_bytes = make_pm_with_inlined_bloom_filters();
    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(
        out.contains("[BLOOM_FILTERS]"),
        "expected BLOOM_FILTERS feature flag, got:\n{}",
        out
    );
    assert!(
        !out.contains("[BLOOM_FILTERS_EXTERNAL]"),
        "should not have BLOOM_FILTERS_EXTERNAL for inlined"
    );
}

#[test]
fn dump_bloom_filters_external() {
    let pm_bytes = make_pm_with_external_bloom_filters();
    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");

    let output = run_inspect(&[pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(
        out.contains("[BLOOM_FILTERS]"),
        "expected BLOOM_FILTERS feature flag"
    );
    assert!(
        out.contains("[BLOOM_FILTERS_EXTERNAL]"),
        "expected BLOOM_FILTERS_EXTERNAL feature flag, got:\n{}",
        out
    );
}

// ── Check mode — passing ─────────────────────────────────────────────

#[test]
fn check_all_pass() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let (dir, pm_path, parquet_path) = write_fixtures(&parquet_bytes);
    let _ = dir;

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        parquet_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));

    let out = stdout_str(&output);
    assert!(
        out.contains("[OK] _pm checksum valid"),
        "missing checksum OK"
    );
    assert!(out.contains("[OK] parquet footer:"), "missing footer OK");
    assert!(out.contains("[OK] column count:"), "missing col count OK");
    assert!(out.contains("[OK] row group count:"), "missing rg count OK");
    assert!(
        out.contains("[OK] column names match"),
        "missing col names OK"
    );
    assert!(out.contains("[OK] rg 0:"), "missing rg 0 OK");
    assert!(out.contains("All checks passed."), "missing summary");
    assert!(!out.contains("[FAIL]"), "unexpected failure");
}

#[test]
fn check_sibling_discovery() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let (dir, pm_path, _) = write_fixtures(&parquet_bytes);
    let _ = dir;

    // Only pass _pm path — should find sibling data.parquet automatically.
    let output = run_inspect(&["--check", pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(0));
    assert!(stdout_str(&output).contains("All checks passed."));
}

#[test]
fn check_explicit_parquet_path() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_bytes);

    // Write parquet to one temp dir, _pm to another — no sibling relationship.
    let dir_pm = tempfile::tempdir().expect("create pm dir");
    let dir_pq = tempfile::tempdir().expect("create pq dir");
    let pm_path = dir_pm.path().join("_pm");
    let pq_path = dir_pq.path().join("some_other_name.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_bytes).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(0));
    assert!(stdout_str(&output).contains("All checks passed."));
}

// ── Check mode — failures ────────────────────────────────────────────

#[test]
fn check_corrupted_checksum() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let mut pm_bytes = make_pm_bytes(&parquet_bytes);

    // Flip a byte in the footer to invalidate the CRC.
    // The header at bytes [0..8] stores the total file size; the trailer (last 4 bytes)
    // stores the footer length. Footer offset = file_size - 4 - footer_length.
    let file_size = u64::from_le_bytes(pm_bytes[0..8].try_into().unwrap()) as usize;
    let trailer_start = file_size - 4;
    let footer_length =
        u32::from_le_bytes(pm_bytes[trailer_start..file_size].try_into().unwrap()) as usize;
    let footer_offset = file_size - 4 - footer_length;
    // Corrupt the first byte of the footer.
    pm_bytes[footer_offset] ^= 0xFF;

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_bytes).expect("write parquet");

    let output = run_inspect(&["--check", pm_path.to_str().unwrap()]);
    let out = stdout_str(&output);
    assert!(
        out.contains("[FAIL] _pm checksum"),
        "expected checksum failure, got: {}",
        out
    );
}

#[test]
fn check_wrong_file_size() {
    // Generate _pm from a 50-row parquet, then check against a 200-row parquet.
    // Both are valid parquet files with the same schema, but different sizes,
    // so the footer offset stored in _pm won't match the larger file.
    let parquet_small = make_single_column_parquet::<Timestamp>("col", 50, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_small);

    let parquet_large = make_single_column_parquet::<Timestamp>("col", 200, Null::None);

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_large).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stdout_str(&output).contains("[FAIL] parquet footer:"),
        "expected footer mismatch"
    );
}

#[test]
fn check_column_count_mismatch() {
    // Generate _pm from a 1-column parquet.
    let parquet_1col = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_1col);

    // Generate a 2-column parquet.
    let parquet_2col = make_two_column_parquet(100);

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_2col).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stdout_str(&output).contains("[FAIL] column count: _pm=1, parquet=2"),
        "expected column count mismatch"
    );
}

fn make_two_column_parquet(count: usize) -> Vec<u8> {
    use questdbr::parquet_write::schema::{Column, Partition};
    use questdbr::parquet_write::ParquetWriter;

    let ts_data: Vec<i64> = (0..count).map(|i| (i as i64) * 1_000_000).collect();
    let int_data: Vec<i32> = (0..count).map(|i| i as i32).collect();

    let ts_col = Column::from_raw_data(
        0,
        "ts",
        8, // Timestamp
        0,
        count,
        ts_data.as_ptr() as *const u8,
        ts_data.len() * std::mem::size_of::<i64>(),
        null(),
        0,
        null(),
        0,
        true,
        false,
        0,
    )
    .expect("ts column");

    let int_col = Column::from_raw_data(
        1,
        "val",
        5, // Int
        0,
        count,
        int_data.as_ptr() as *const u8,
        int_data.len() * std::mem::size_of::<i32>(),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("int column");

    let partition = Partition {
        table: "test".to_string(),
        columns: vec![ts_col, int_col],
    };

    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .finish(partition)
        .expect("ParquetWriter::finish");
    buf.into_inner()
}

#[test]
fn check_column_name_mismatch() {
    let parquet_a = make_single_column_parquet::<Timestamp>("col_a", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_a);

    let parquet_b = make_single_column_parquet::<Timestamp>("col_b", 100, Null::None);

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_b).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1));

    let out = stdout_str(&output);
    assert!(
        out.contains("[FAIL] column 0 name: _pm=\"col_a\", parquet=\"col_b\""),
        "expected column name mismatch, got: {}",
        out
    );
}

#[test]
fn check_row_count_mismatch() {
    let parquet_100 = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_100);

    let parquet_200 = make_single_column_parquet::<Timestamp>("col", 200, Null::None);

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_200).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1));

    let out = stdout_str(&output);
    assert!(
        out.contains("[FAIL] rg 0 num_rows:"),
        "expected row count mismatch, got: {}",
        out
    );
}

#[test]
fn check_missing_sibling_parquet() {
    let parquet_bytes = make_single_column_parquet::<Timestamp>("col", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_bytes);

    // Write only the _pm file — no data.parquet alongside it.
    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");

    let output = run_inspect(&["--check", pm_path.to_str().unwrap()]);
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr_str(&output).contains("Parquet file not found"),
        "expected 'Parquet file not found' error"
    );
}

#[test]
fn check_error_count_reported() {
    // Use mismatched column name + row count to trigger multiple failures.
    let parquet_a = make_single_column_parquet::<Timestamp>("col_a", 100, Null::None);
    let pm_bytes = make_pm_bytes(&parquet_a);

    let parquet_b = make_single_column_parquet::<Timestamp>("col_b", 200, Null::None);

    let dir = tempfile::tempdir().expect("create temp dir");
    let pm_path = dir.path().join("_pm");
    let pq_path = dir.path().join("data.parquet");
    std::fs::write(&pm_path, &pm_bytes).expect("write _pm");
    std::fs::write(&pq_path, &parquet_b).expect("write parquet");

    let output = run_inspect(&[
        "--check",
        pm_path.to_str().unwrap(),
        pq_path.to_str().unwrap(),
    ]);
    assert_eq!(output.status.code(), Some(1));

    let out = stdout_str(&output);
    assert!(
        out.contains("discrepancy(ies) found."),
        "expected error count summary, got: {}",
        out
    );
}
