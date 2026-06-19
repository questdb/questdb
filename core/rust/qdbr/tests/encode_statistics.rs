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
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;
use questdbr::parquet_write::ParquetWriter;

use crate::common::encode::{
    build_qdb_varchar_data, make_primitive_column, make_varchar_column, write_parquet,
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
/// codepoint of the max can grow its encoding by up to 3 bytes.
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
        max.len() <= TEXT_STATS_BOUND + 3,
        "max stays bounded, got {} bytes",
        max.len()
    );
    assert!(min <= value_bytes, "min must be a byte-wise floor");
    assert!(max >= value_bytes, "max must be a byte-wise ceiling");
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
