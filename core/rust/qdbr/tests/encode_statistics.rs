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

use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::ColumnOrder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;
use qdb_core::col_type::{ColumnType, ColumnTypeTag};
use questdbr::parquet_write::schema::Partition;

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
