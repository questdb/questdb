//! External-reader compatibility for column statistics.
//!
//! QuestDB computes and writes per-row-group `min_value`/`max_value` and
//! `null_count`, but the Parquet spec leaves `min_value`/`max_value` *undefined*
//! unless the file footer also declares `column_orders`. Spec-conformant readers
//! (pyarrow, PyIceberg, Spark, DuckDB, Trino) then ignore the bounds and lose
//! row-group skipping / predicate pushdown, while `null_count` (order independent)
//! keeps working -- the observed `has_min_max=False` / `has_null_count=True`.
//!
//! These tests drive QuestDB's `ParquetWriter` and read the result back with the
//! independent Arrow `parquet` reader.

mod common;

use std::io::Cursor;
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::basic::{ColumnOrder, Encoding as ParquetEncoding, PageType as ParquetPageType};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use parquet::format::BoundaryOrder;
use parquet2::metadata::SortingColumn;
use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::FileMetaData as ThriftFileMetaData;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;
use questdbr::parquet_write::ParquetWriter;

use arrow::array::UInt32Array;

use crate::common::encode::{
    build_qdb_binary_data, build_qdb_varchar_data, make_primitive_column, make_string_column,
    make_symbol_column, make_varchar_column, serialize_as_symbols, write_parquet,
};
use crate::common::Encoding;

/// Parse the footer of QuestDB-written Parquet bytes with the independent Arrow reader.
fn external_metadata(data: &[u8]) -> Arc<ParquetMetaData> {
    let bytes: Bytes = data.to_vec().into();
    ParquetRecordBatchReaderBuilder::try_new(bytes)
        .expect("open parquet with arrow reader")
        .metadata()
        .clone()
}

/// Parse the raw Thrift footer of QuestDB-written Parquet bytes.
fn thrift_metadata(data: &[u8]) -> ThriftFileMetaData {
    let footer_len_offset = data.len() - 8;
    let footer_len = u32::from_le_bytes(
        data[footer_len_offset..footer_len_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let footer_offset = footer_len_offset - footer_len;
    let mut protocol =
        TCompactInputProtocol::new(&data[footer_offset..footer_len_offset], usize::MAX);
    ThriftFileMetaData::read_from_in_protocol(&mut protocol).expect("read thrift metadata")
}

/// Reinterpret a contiguous slice as raw bytes for `Column::from_raw_data`.
fn as_bytes<T>(data: &[T]) -> &[u8] {
    // SAFETY: slices are contiguous and `u8` has no alignment requirement.
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, std::mem::size_of_val(data)) }
}

/// Regression guard: the footer must declare `column_orders`, one `TypeDefinedOrder`
/// per leaf column. Without it, every external reader treats min/max as undefined.
#[test]
fn questdb_parquet_declares_column_orders_for_external_readers() {
    let longs: Vec<i64> = vec![100, -50, 7, 9_999, 42];
    let doubles: Vec<f64> = vec![3.5, -1.0, 100.25, 0.0, 7.5];
    let timestamps: Vec<i64> = vec![1, 2, 3, 4, 5];
    // A byte-array leaf with no natural signed order must still get an entry.
    let (vc_data, vc_aux) =
        build_qdb_varchar_data(&["alpha", "bravo", "charlie", "delta", "echo"], &[false; 5]);
    let row_count = longs.len();

    let long_bytes = as_bytes(&longs);
    let double_bytes = as_bytes(&doubles);
    let ts_bytes = as_bytes(&timestamps);

    let columns = vec![
        make_primitive_column(
            "l",
            ColumnType::new(ColumnTypeTag::Long, 0).code(),
            long_bytes.as_ptr(),
            long_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "d",
            ColumnType::new(ColumnTypeTag::Double, 0).code(),
            double_bytes.as_ptr(),
            double_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "t",
            ColumnType::new(ColumnTypeTag::Timestamp, 0).code(),
            ts_bytes.as_ptr(),
            ts_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_varchar_column(
            "v",
            ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
            vc_data.as_ptr(),
            vc_data.len(),
            vc_aux.as_ptr(),
            vc_aux.len(),
            row_count,
            Encoding::Plain.config(),
        ),
    ];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    let file_meta = metadata.file_metadata();
    let leaf_count = file_meta.schema_descr().num_columns();
    assert_eq!(leaf_count, 4, "expected 4 leaf columns in schema");

    let orders = file_meta.column_orders().unwrap_or_else(|| {
        panic!(
            "footer is missing column_orders; without it min_value/max_value are spec-undefined \
             and external readers ignore them (has_min_max=False)"
        )
    });
    assert_eq!(
        orders.len(),
        leaf_count,
        "column_orders must have one entry per leaf column"
    );
    for (i, order) in orders.iter().enumerate() {
        assert!(
            matches!(order, ColumnOrder::TYPE_DEFINED_ORDER(_)),
            "leaf column {i} must declare TypeDefinedOrder, got {order:?}"
        );
    }
}

/// Regression guard: QuestDB-written column chunks must expose Parquet page
/// encoding statistics so external readers can inspect page-body encodings
/// without scanning page headers.
#[test]
fn questdb_parquet_populates_encoding_stats_for_external_readers() {
    let longs: Vec<i64> = vec![10, 20, 30, 40, 50];
    let doubles: Vec<f64> = vec![1.5, 2.5, 3.5, 4.5, 5.5];
    let row_count = longs.len();
    let long_bytes = as_bytes(&longs);
    let double_bytes = as_bytes(&doubles);

    let columns = vec![
        make_primitive_column(
            "l",
            ColumnType::new(ColumnTypeTag::Long, 0).code(),
            long_bytes.as_ptr(),
            long_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "d",
            ColumnType::new(ColumnTypeTag::Double, 0).code(),
            double_bytes.as_ptr(),
            double_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
    ];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    assert_eq!(metadata.num_row_groups(), 1, "expected a single row group");
    let row_group = metadata.row_group(0);
    for column_index in 0..row_group.num_columns() {
        let stats = row_group
            .column(column_index)
            .page_encoding_stats()
            .unwrap_or_else(|| panic!("column {column_index} is missing page encoding statistics"));
        assert!(!stats.is_empty(), "column {column_index} has empty stats");
        assert!(
            stats.iter().any(|stat| {
                stat.page_type == ParquetPageType::DATA_PAGE
                    && stat.encoding == ParquetEncoding::PLAIN
                    && stat.count >= 1
            }),
            "column {column_index} must report at least one PLAIN data page, got {stats:?}"
        );
    }
}

/// Regression guard: QuestDB physically orders rows by the designated timestamp
/// and declares it as a `SortingColumn`. Its page-level `ColumnIndex` must then
/// advertise an `ASCENDING` `boundary_order` so external readers can binary-search
/// the timestamp's page bounds; a non-sorted column must stay `UNORDERED`.
#[test]
fn questdb_parquet_marks_sorted_timestamp_with_ascending_boundary_order() {
    let row_count = 8usize;
    // An unsorted value column (leaf 0) alongside the ascending designated
    // timestamp (leaf 1).
    let longs: Vec<i64> = vec![5, 1, 9, 3, 7, 2, 8, 4];
    let timestamps: Vec<i64> = (1..=row_count as i64).collect();
    let long_bytes = as_bytes(&longs);
    let ts_bytes = as_bytes(&timestamps);

    let columns = vec![
        make_primitive_column(
            "l",
            ColumnType::new(ColumnTypeTag::Long, 0).code(),
            long_bytes.as_ptr(),
            long_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "t",
            ColumnType::new(ColumnTypeTag::Timestamp, 0).code(),
            ts_bytes.as_ptr(),
            ts_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
    ];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };

    // Drive the real writer with the timestamp (leaf 1) declared as the
    // ascending sorting column. A small page size splits the timestamp across
    // several data pages, so the boundary order spans more than one page.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(16))
        .with_sorting_columns(Some(vec![SortingColumn::new(1, false, false)]))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    // Read the page index back with the independent Arrow reader.
    let options = ArrowReaderOptions::new().with_page_index(true);
    let bytes: Bytes = data.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
        .expect("open parquet with arrow reader");
    let column_index = builder
        .metadata()
        .column_index()
        .expect("page index must be present when statistics are written");

    assert_eq!(column_index.len(), 1, "expected a single row group");
    let row_group = &column_index[0];
    assert_eq!(row_group.len(), 2, "expected two leaf columns");

    assert_eq!(
        row_group[1].get_boundary_order(),
        Some(BoundaryOrder::ASCENDING),
        "the sorted designated timestamp must declare ASCENDING boundary order"
    );
    assert_eq!(
        row_group[0].get_boundary_order(),
        Some(BoundaryOrder::UNORDERED),
        "an unsorted column must keep UNORDERED boundary order"
    );
}

/// The DESCENDING counterpart of the test above. A descending designated
/// timestamp declares a `descending` `SortingColumn`, so its ColumnIndex must
/// advertise `DESCENDING` boundary order end-to-end through `ParquetWriter`.
#[test]
fn questdb_parquet_marks_sorted_timestamp_with_descending_boundary_order() {
    let row_count = 8usize;
    // An unsorted value column (leaf 0) alongside the descending designated
    // timestamp (leaf 1).
    let longs: Vec<i64> = vec![5, 1, 9, 3, 7, 2, 8, 4];
    let timestamps: Vec<i64> = (1..=row_count as i64).rev().collect();
    let long_bytes = as_bytes(&longs);
    let ts_bytes = as_bytes(&timestamps);

    let columns = vec![
        make_primitive_column(
            "l",
            ColumnType::new(ColumnTypeTag::Long, 0).code(),
            long_bytes.as_ptr(),
            long_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "t",
            ColumnType::new(ColumnTypeTag::Timestamp, 0).code(),
            ts_bytes.as_ptr(),
            ts_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
    ];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };

    // The timestamp (leaf 1) is declared as the descending sorting column; a
    // small page size spreads the boundary order across several data pages.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(16))
        .with_sorting_columns(Some(vec![SortingColumn::new(1, true, false)]))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    let options = ArrowReaderOptions::new().with_page_index(true);
    let bytes: Bytes = data.into();
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(bytes, options)
        .expect("open parquet with arrow reader");
    let column_index = builder
        .metadata()
        .column_index()
        .expect("page index must be present when statistics are written");

    assert_eq!(column_index.len(), 1, "expected a single row group");
    let row_group = &column_index[0];
    assert_eq!(row_group.len(), 2, "expected two leaf columns");

    assert_eq!(
        row_group[1].get_boundary_order(),
        Some(BoundaryOrder::DESCENDING),
        "the descending designated timestamp must declare DESCENDING boundary order"
    );
    assert_eq!(
        row_group[0].get_boundary_order(),
        Some(BoundaryOrder::UNORDERED),
        "an unsorted column must keep UNORDERED boundary order"
    );
}

/// Companion characterization: QuestDB does write per-row-group min/max and
/// null_count, and an independent reader decodes them correctly. This is what
/// makes the missing `column_orders` the only thing between QuestDB output and
/// working predicate pushdown.
#[test]
fn questdb_parquet_exposes_min_max_and_null_count_to_external_readers() {
    // The long column carries nulls (i64::MIN sentinel); the double column has none.
    let longs: Vec<i64> = vec![10, i64::MIN, 20, i64::MIN, 5];
    let doubles: Vec<f64> = vec![1.5, 2.5, 0.5, 100.0, -3.0];
    let row_count = longs.len();

    let long_bytes = as_bytes(&longs);
    let double_bytes = as_bytes(&doubles);

    let columns = vec![
        make_primitive_column(
            "l",
            ColumnType::new(ColumnTypeTag::Long, 0).code(),
            long_bytes.as_ptr(),
            long_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
        make_primitive_column(
            "d",
            ColumnType::new(ColumnTypeTag::Double, 0).code(),
            double_bytes.as_ptr(),
            double_bytes.len(),
            row_count,
            Encoding::Plain.config(),
        ),
    ];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    assert_eq!(metadata.num_row_groups(), 1, "expected a single row group");
    let row_group = metadata.row_group(0);

    match row_group.column(0).statistics().expect("long statistics") {
        Statistics::Int64(stats) => {
            assert_eq!(
                stats.min_opt(),
                Some(&5_i64),
                "long min must exclude the null sentinel"
            );
            assert_eq!(
                stats.max_opt(),
                Some(&20_i64),
                "long max must exclude the null sentinel"
            );
            assert_eq!(stats.null_count_opt(), Some(2), "two null longs");
        }
        other => panic!("expected Int64 statistics for long column, got {other:?}"),
    }

    match row_group.column(1).statistics().expect("double statistics") {
        Statistics::Double(stats) => {
            assert_eq!(stats.min_opt(), Some(&-3.0_f64));
            assert_eq!(stats.max_opt(), Some(&100.0_f64));
            assert_eq!(stats.null_count_opt(), Some(0), "no null doubles");
        }
        other => panic!("expected Double statistics for double column, got {other:?}"),
    }
}

/// The UTF-8 truncation bound the writer applies to String/Symbol/Varchar min/max
/// (mirrors `UTF8_STATS_TRUNCATE_LEN` in `parquet_write::util`). Advancing the last
/// codepoint of the max can grow its encoding by at most 1 byte.
const TEXT_STATS_BOUND: usize = 64;

/// A single multi-megabyte (here multi-kilobyte) text value must NOT bloat the
/// footer with an equally large min/max. The writer truncates the bounds to
/// `TEXT_STATS_BOUND` bytes while keeping them a valid byte-wise floor/ceiling, so an
/// external reader still gets correct (if loose) bounds at a fixed small size.
#[test]
fn questdb_parquet_truncates_long_string_statistics() {
    let value = "x".repeat(5_000);
    let value_bytes = value.as_bytes();
    let (vc_data, vc_aux) = build_qdb_varchar_data(&[value.as_str()], &[false]);

    let columns = vec![make_varchar_column(
        "v",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        vc_data.as_ptr(),
        vc_data.len(),
        vc_aux.as_ptr(),
        vc_aux.len(),
        1,
        Encoding::Plain.config(),
    )];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("varchar statistics");
    let min = stats.min_bytes_opt().expect("min_value present");
    let max = stats.max_bytes_opt().expect("max_value present");

    assert!(
        min.len() <= TEXT_STATS_BOUND,
        "min truncated to the bound, got {} bytes",
        min.len()
    );
    assert!(
        max.len() <= TEXT_STATS_BOUND + 1,
        "max stays bounded, got {} bytes",
        max.len()
    );
    assert!(min <= value_bytes, "min must be a byte-wise floor");
    assert!(max >= value_bytes, "max must be a byte-wise ceiling");

    let thrift_metadata = thrift_metadata(&data);
    let thrift_stats = thrift_metadata.row_groups[0].columns[0]
        .meta_data
        .as_ref()
        .and_then(|meta| meta.statistics.as_ref())
        .expect("thrift varchar statistics");
    assert_eq!(thrift_stats.is_min_value_exact, Some(false));
    assert_eq!(thrift_stats.is_max_value_exact, Some(false));
}

/// When the truncation length falls inside a multi-byte codepoint, the writer must
/// back off to a codepoint boundary so the stored min/max stay valid UTF-8 (readers
/// that decode the bounds as strings would otherwise choke), while remaining a valid
/// byte-wise floor/ceiling.
#[test]
fn questdb_parquet_truncates_multibyte_string_on_codepoint_boundary() {
    // 3-byte euro sign; 30 of them is 90 bytes, so the 64-byte cut lands mid-codepoint.
    let value = "\u{20ac}".repeat(30);
    let value_bytes = value.as_bytes();
    let (vc_data, vc_aux) = build_qdb_varchar_data(&[value.as_str()], &[false]);

    let columns = vec![make_varchar_column(
        "v",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        vc_data.as_ptr(),
        vc_data.len(),
        vc_aux.as_ptr(),
        vc_aux.len(),
        1,
        Encoding::Plain.config(),
    )];
    let partition = Partition {
        table: "compat".to_string(),
        columns,
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("varchar statistics");
    let min = stats.min_bytes_opt().expect("min_value present");
    let max = stats.max_bytes_opt().expect("max_value present");

    assert!(
        std::str::from_utf8(min).is_ok(),
        "min must not split a codepoint"
    );
    assert!(
        std::str::from_utf8(max).is_ok(),
        "max must not split a codepoint"
    );
    assert!(min.len() <= TEXT_STATS_BOUND, "min truncated to the bound");
    assert!(min <= value_bytes, "min must be a byte-wise floor");
    assert!(max >= value_bytes, "max must be a byte-wise ceiling");
}

/// A multi-page string column chunk exercises the row-group-level statistics
/// reduction: the writer emits one `BinaryStatistics` per data page, then
/// `reduce_binary` folds them into the chunk stats. Here one page holds a short
/// value (exact bounds) and another a long value (truncated, inexact). The
/// reduced chunk min/max must bracket both pages byte-wise, and a single
/// truncated page must poison both exact flags to `Some(false)`.
#[test]
fn questdb_parquet_reduces_multipage_string_exact_flags() {
    // A tiny page size (ByteArray counts as 8 bytes/row, so 8 -> 1 row/page)
    // puts each value on its own page, forcing reduce_binary to fold >1 page.
    let short = "short";
    let long = "L".repeat(200);
    let values = [short, long.as_str()];
    let (vc_data, vc_aux) = build_qdb_varchar_data(&values, &[false, false]);

    let column = make_varchar_column(
        "v",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        vc_data.as_ptr(),
        vc_data.len(),
        vc_aux.as_ptr(),
        vc_aux.len(),
        values.len(),
        Encoding::Plain.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };

    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(8))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    // The chunk must really span more than one data page, or reduce_binary
    // would never fold across pages.
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(data.clone()), options)
            .expect("open parquet with arrow reader");
    let offset_index = builder
        .metadata()
        .offset_index()
        .expect("offset index present when statistics are written");
    assert!(
        offset_index[0][0].page_locations().len() > 1,
        "the string chunk must span multiple data pages"
    );

    // The reduced chunk bounds must bracket both pages' values byte-wise.
    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("varchar statistics");
    let min = stats.min_bytes_opt().expect("min_value present");
    let max = stats.max_bytes_opt().expect("max_value present");
    assert!(min <= short.as_bytes(), "min is a floor across pages");
    assert!(min <= long.as_bytes(), "min is a floor across pages");
    assert!(max >= short.as_bytes(), "max is a ceiling across pages");
    assert!(max >= long.as_bytes(), "max is a ceiling across pages");

    // The truncated long page poisons both reduced exact flags to Some(false).
    let thrift_metadata = thrift_metadata(&data);
    let thrift_stats = thrift_metadata.row_groups[0].columns[0]
        .meta_data
        .as_ref()
        .and_then(|meta| meta.statistics.as_ref())
        .expect("thrift varchar statistics");
    assert_eq!(thrift_stats.is_min_value_exact, Some(false));
    assert_eq!(thrift_stats.is_max_value_exact, Some(false));
}

/// A multi-page string chunk whose page bounds are in a prefix relationship:
/// "ETH" is a proper prefix of "ETHW". Parquet orders BYTE_ARRAY unsigned, so
/// "ETH" < "ETHW", and the row-group reduce must apply that length tiebreaker.
/// If it collapses the chunk max to the shorter "ETH" -- byte-wise below a real
/// value -- a reader pruning on `sym = 'ETHW'` skips the row group and drops the
/// matching rows. The sibling test above uses values that differ at byte 0, so
/// only this one exercises the prefix path.
#[test]
fn questdb_parquet_reduces_multipage_string_prefix_bounds() {
    let low = "ETH";
    let high = "ETHW";
    let values = [low, high];
    let (vc_data, vc_aux) = build_qdb_varchar_data(&values, &[false, false]);

    let column = make_varchar_column(
        "v",
        ColumnType::new(ColumnTypeTag::Varchar, 0).code(),
        vc_data.as_ptr(),
        vc_data.len(),
        vc_aux.as_ptr(),
        vc_aux.len(),
        values.len(),
        Encoding::Plain.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };

    // A tiny page size puts each value on its own page, forcing reduce_binary to
    // fold more than one page.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(8))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    // The chunk must really span more than one data page, or reduce_binary would
    // never fold across pages.
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(data.clone()), options)
            .expect("open parquet with arrow reader");
    let offset_index = builder
        .metadata()
        .offset_index()
        .expect("offset index present when statistics are written");
    assert!(
        offset_index[0][0].page_locations().len() > 1,
        "the string chunk must span multiple data pages"
    );

    // The reduced max must keep the longer "ETHW", not the prefix "ETH": a bound
    // of "ETH" is byte-wise below the real "ETHW" and would prune it away.
    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("varchar statistics");
    let min = stats.min_bytes_opt().expect("min_value present");
    let max = stats.max_bytes_opt().expect("max_value present");
    assert_eq!(min, low.as_bytes(), "chunk min keeps the prefix");
    assert_eq!(max, high.as_bytes(), "chunk max keeps the extended value");
}

/// Symbol columns encode through the dictionary path, but their min/max
/// statistics must still take the String (UTF-8) truncation branch: a symbol
/// value longer than the 64-byte bound is truncated and flagged inexact, just
/// like String/Varchar. This drives the real Symbol encoder (whose production
/// `PrimitiveType` carries the String logical type), not the opaque-binary
/// 9-byte clamp the unit tests' hand-built type falls into.
#[test]
fn questdb_parquet_truncates_long_symbol_statistics() {
    // Two distinct symbols, both longer than the 64-byte truncation bound.
    let low = "a".repeat(100);
    let high = "z".repeat(100);
    let (chars_data, offsets) = serialize_as_symbols(&[low.as_str(), high.as_str()]);

    // Row keys reference both dictionary entries.
    let keys: Vec<i32> = vec![0, 1, 0, 1];
    let keys_bytes = as_bytes(&keys);

    let column = make_symbol_column(
        "s",
        ColumnType::new(ColumnTypeTag::Symbol, 0).code(),
        keys_bytes.as_ptr(),
        keys_bytes.len(),
        chars_data.as_ptr(),
        chars_data.len(),
        offsets.as_ptr(),
        offsets.len(),
        keys.len(),
        Encoding::RleDictionary.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };
    let data = write_parquet(partition);

    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("symbol statistics");
    let min = stats.min_bytes_opt().expect("min_value present");
    let max = stats.max_bytes_opt().expect("max_value present");

    // The all-ASCII low value truncates to exactly the 64-byte text bound. The
    // opaque-binary branch would instead clamp to 9 bytes, so this length pins
    // the Symbol encoder to the String (UTF-8) branch.
    assert_eq!(
        min.len(),
        TEXT_STATS_BOUND,
        "symbol min must truncate to the UTF-8 bound, not the 9-byte binary clamp"
    );
    assert!(
        max.len() > 9 && max.len() <= TEXT_STATS_BOUND + 1,
        "symbol max stays within the UTF-8 bound, got {} bytes",
        max.len()
    );
    assert!(min <= low.as_bytes(), "min must be a byte-wise floor");
    assert!(max >= high.as_bytes(), "max must be a byte-wise ceiling");

    let thrift_metadata = thrift_metadata(&data);
    let thrift_stats = thrift_metadata.row_groups[0].columns[0]
        .meta_data
        .as_ref()
        .and_then(|meta| meta.statistics.as_ref())
        .expect("thrift symbol statistics");
    assert_eq!(thrift_stats.is_min_value_exact, Some(false));
    assert_eq!(thrift_stats.is_max_value_exact, Some(false));
}

/// IPv4 addresses are stored as INT32 but logically UINT_32, so external readers
/// compare their min/max statistics as unsigned. The `RLE_DICTIONARY` encoder must
/// therefore compute those bounds with unsigned ordering, like the Plain/Delta
/// paths. If it used signed `i32` ordering, an address with the high bit set (any
/// `>= 128.0.0.0`, stored as a negative `i32`) would land on the wrong side of the
/// bound, and a conformant reader pruning `WHERE ip > '199.0.0.0'` would skip the
/// row group that actually holds the match -- silently dropping rows.
///
/// This drives the real `ParquetWriter` with a dict-encoded IPv4 column split into
/// two row groups (low addresses, then a sign-straddling high one), reads the
/// per-row-group bounds back with the independent Arrow reader, and replays the
/// unsigned row-group pruning an external engine would do.
#[test]
fn questdb_parquet_ipv4_dict_unsigned_bounds_survive_external_pruning() {
    // Row group 0: all below 128.0.0.0 (positive i32). Row group 1: a
    // sign-straddling high address that is a negative i32.
    const RG0_MIN: u32 = 0x0100_0000; // 1.0.0.0
    const RG0_MAX: u32 = 0x6400_0000; // 100.0.0.0
    const RG1_MIN: u32 = 0x0A00_0000; // 10.0.0.0
    const RG1_MAX: u32 = 0xC800_0000; // 200.0.0.0 (negative as i32)
    const PROBE: u32 = 0xC700_0000; // 199.0.0.0; only row group 1 has an address above it

    let ips: Vec<i32> = vec![
        RG0_MIN as i32,
        0x4000_0000, // 64.0.0.0
        RG0_MAX as i32,
        RG1_MIN as i32,
        RG1_MAX as i32,
    ];
    let ip_bytes = as_bytes(&ips);

    let column = make_primitive_column(
        "ip",
        ColumnType::new(ColumnTypeTag::IPv4, 0).code(),
        ip_bytes.as_ptr(),
        ip_bytes.len(),
        ips.len(),
        Encoding::RleDictionary.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };

    // Split into two row groups (3 + 2 rows) so unsigned pruning can pick one.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_row_group_size(Some(3))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    let metadata = external_metadata(&data);
    assert_eq!(metadata.num_row_groups(), 2, "expected two row groups");

    // Decode each row group's min/max from the footer as unsigned u32 -- the order
    // a conformant reader uses for a UINT_32 column with TypeDefinedOrder.
    let rg_bounds = |i: usize| -> (u32, u32) {
        let stats = metadata
            .row_group(i)
            .column(0)
            .statistics()
            .expect("ipv4 statistics");
        let min = u32::from_le_bytes(
            stats
                .min_bytes_opt()
                .expect("min_value present")
                .try_into()
                .expect("4-byte ipv4 min"),
        );
        let max = u32::from_le_bytes(
            stats
                .max_bytes_opt()
                .expect("max_value present")
                .try_into()
                .expect("4-byte ipv4 max"),
        );
        (min, max)
    };

    assert_eq!(
        rg_bounds(0),
        (RG0_MIN, RG0_MAX),
        "row group 0 unsigned bounds"
    );
    assert_eq!(
        rg_bounds(1),
        (RG1_MIN, RG1_MAX),
        "row group 1 unsigned bounds; a signed encoder would report max=10.0.0.0 here"
    );

    // Replay the row-group skipping an external reader does for `ip > PROBE`,
    // comparing unsigned: a row group is kept iff its unsigned max is above PROBE.
    let kept: Vec<usize> = (0..metadata.num_row_groups())
        .filter(|&i| rg_bounds(i).1 > PROBE)
        .collect();
    assert_eq!(
        kept,
        vec![1],
        "unsigned pruning must keep exactly row group 1 (holds 200.0.0.0); the signed \
         bug would prune it and drop the match"
    );

    // The kept row group really does contain a match for the probe, so pruning it
    // would have lost data.
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data))
        .expect("arrow reader")
        .with_row_groups(vec![1])
        .build()
        .expect("build reader over row group 1");
    let mut matched = false;
    for batch in reader.flatten() {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array");
        matched |= arr.iter().flatten().any(|v| v > PROBE);
    }
    assert!(
        matched,
        "row group 1 must contain an address > 199.0.0.0, so pruning it drops rows"
    );
}

/// Companion to the dict test above, but for the multi-page chunk reduce. IPv4
/// per-page bounds are unsigned; when one column chunk spans several data pages,
/// the footer's chunk-level `min_value`/`max_value` come from reducing those
/// per-page bounds, and that reduce must compare unsigned too. A signed reduce
/// understates the chunk max for any address >= 128.0.0.0 (a negative i32), so a
/// conformant reader pruning `WHERE ip > '199.0.0.0'` would skip the chunk that
/// holds the match -- silently dropping rows.
///
/// This forces a single row group split into several data pages with a tiny
/// `data_page_size`, places a sign-straddling high address on a later page than
/// the low one, then reads the chunk bounds back with the independent Arrow
/// reader and replays the unsigned pruning.
#[test]
fn questdb_parquet_reduces_multipage_ipv4_unsigned_bounds() {
    const LOW: u32 = 0x0100_0000; // 1.0.0.0
    const HIGH: u32 = 0xC800_0000; // 200.0.0.0 (negative as i32)
    const PROBE: u32 = 0xC700_0000; // 199.0.0.0; only HIGH is above it

    // LOW first, HIGH last; a 4-byte/row Int32 with an 8-byte page budget yields
    // 2 rows/page, so LOW and HIGH land on different pages and the chunk reduce
    // must fold the sign-straddling max across pages.
    let ips: Vec<i32> = vec![
        LOW as i32,
        0x2000_0000, // 32.0.0.0
        0x4000_0000, // 64.0.0.0
        0x6400_0000, // 100.0.0.0
        0x7F00_0000, // 127.0.0.0 (largest positive i32 here)
        HIGH as i32,
    ];
    let ip_bytes = as_bytes(&ips);

    let column = make_primitive_column(
        "ip",
        ColumnType::new(ColumnTypeTag::IPv4, 0).code(),
        ip_bytes.as_ptr(),
        ip_bytes.len(),
        ips.len(),
        Encoding::Plain.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };

    // One row group (default size) but a tiny page budget forces several data
    // pages within that single chunk.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(8))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    // The chunk must really span >1 data page, else the reduce never folds.
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(data.clone()), options)
            .expect("open parquet with arrow reader");
    assert_eq!(
        builder.metadata().num_row_groups(),
        1,
        "expected a single row group so the chunk reduce folds across pages"
    );
    let offset_index = builder
        .metadata()
        .offset_index()
        .expect("offset index present when statistics are written");
    assert!(
        offset_index[0][0].page_locations().len() > 1,
        "the ipv4 chunk must span multiple data pages"
    );

    // Decode the chunk bounds as unsigned u32 -- the order a conformant reader
    // uses for a UINT_32 column with TypeDefinedOrder.
    let metadata = external_metadata(&data);
    let stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("ipv4 statistics");
    let min = u32::from_le_bytes(
        stats
            .min_bytes_opt()
            .expect("min_value present")
            .try_into()
            .expect("4-byte ipv4 min"),
    );
    let max = u32::from_le_bytes(
        stats
            .max_bytes_opt()
            .expect("max_value present")
            .try_into()
            .expect("4-byte ipv4 max"),
    );
    assert_eq!(
        (min, max),
        (LOW, HIGH),
        "unsigned chunk bounds; a signed reduce understates the chunk max (the page holding \
         200.0.0.0 reduces to a smaller positive address)"
    );

    // Replay `ip > PROBE`: the chunk's unsigned max is above PROBE, so an external
    // reader keeps it. The signed bug understates the max and would prune it.
    assert!(
        max > PROBE,
        "unsigned chunk max must exceed the probe so the chunk is not pruned"
    );

    // The chunk really holds a match, so pruning it would lose rows.
    let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data))
        .expect("arrow reader")
        .build()
        .expect("build reader");
    let mut matched = false;
    for batch in reader.flatten() {
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("UInt32Array");
        matched |= arr.iter().flatten().any(|v| v > PROBE);
    }
    assert!(
        matched,
        "the chunk must contain an address > 199.0.0.0, so pruning it drops rows"
    );
}

/// Companion to the IPv4 multi-page reduce test, for the opaque-Binary unbounded-max
/// sentinel. A page whose `max_value` prefix is all `0xFF` has no short upper bound,
/// so the writer emits no `max_value` (min present, max absent) to mean "unbounded".
/// When the chunk spans several data pages, the footer's chunk-level max comes from
/// reducing the per-page bounds; that reduce must keep the chunk max absent too, or a
/// sibling page's smaller bounded max would understate it and a conformant reader
/// pruning `WHERE blob > ...` would silently drop the all-`0xFF` row.
#[test]
fn questdb_parquet_reduces_multipage_binary_unbounded_max() {
    // A value whose clamped 9-byte prefix is all 0xFF (no short upper bound exists, so
    // its page emits no max), placed among smaller values whose pages carry an
    // ordinary bounded max.
    let unbounded: Vec<u8> = vec![0xFF; 12];
    let values: Vec<&[u8]> = vec![&[0x00], &[0x01], &unbounded, &[0x02]];
    let nulls = vec![false; values.len()];
    let (primary, offsets) = build_qdb_binary_data(&values, &nulls);
    let offsets_bytes = as_bytes(&offsets);

    let column = make_string_column(
        "b",
        ColumnType::new(ColumnTypeTag::Binary, 0).code(),
        primary.as_ptr(),
        primary.len(),
        offsets_bytes.as_ptr(),
        offsets_bytes.len(),
        values.len(),
        Encoding::Plain.config(),
    );
    let partition = Partition {
        table: "compat".to_string(),
        columns: vec![column],
    };

    // One row group (default size) but a tiny page budget forces several data pages
    // within the single chunk, so the chunk reduce folds the unbounded page with the
    // bounded ones.
    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_data_page_size(Some(8))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let data = buf.into_inner();

    // The chunk must really span >1 data page, else the reduce never folds.
    let options = ArrowReaderOptions::new().with_page_index(true);
    let builder =
        ParquetRecordBatchReaderBuilder::try_new_with_options(Bytes::from(data.clone()), options)
            .expect("open parquet with arrow reader");
    assert_eq!(
        builder.metadata().num_row_groups(),
        1,
        "expected a single row group so the chunk reduce folds across pages"
    );
    let offset_index = builder
        .metadata()
        .offset_index()
        .expect("offset index present when statistics are written");
    assert!(
        offset_index[0][0].page_locations().len() > 1,
        "the binary chunk must span multiple data pages"
    );

    // The footer chunk max must be absent (unbounded). A None-as-empty reduce would
    // instead emit one of the small values (e.g. [0x02]) -- byte-wise less than the
    // all-0xFF value, an invalid upper bound. The min stays present.
    let thrift = thrift_metadata(&data);
    let column_chunk = &thrift.row_groups[0].columns[0];
    let stats = column_chunk
        .meta_data
        .as_ref()
        .and_then(|meta| meta.statistics.as_ref())
        .expect("thrift binary statistics");
    assert_eq!(stats.max_value, None, "unbounded chunk max stays absent");
    assert!(stats.min_value.is_some(), "chunk min is still present");

    // The page index degrades to OffsetIndex-only: an unbounded-max page cannot go in
    // a ColumnIndex, so the writer drops the ColumnIndex for the file while keeping
    // the OffsetIndex (the supported all-or-nothing degradation).
    assert!(
        column_chunk.column_index_offset.is_none(),
        "the ColumnIndex is omitted for a chunk with an unbounded-max page"
    );
    assert!(
        column_chunk.offset_index_offset.is_some(),
        "the OffsetIndex is still present"
    );

    // The independent Arrow reader agrees: no max bound, so `WHERE blob > ...` cannot
    // prune the chunk that holds the all-0xFF row.
    let metadata = external_metadata(&data);
    let arrow_stats = metadata
        .row_group(0)
        .column(0)
        .statistics()
        .expect("binary statistics");
    assert!(
        arrow_stats.max_bytes_opt().is_none(),
        "external reader must see no max bound for the unbounded chunk"
    );
}
