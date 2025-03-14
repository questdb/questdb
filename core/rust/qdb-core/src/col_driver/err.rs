use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::error::{CoreError, fmt_err};

pub(super) fn missing_aux(driver: &impl ColumnDriver, col: &MappedColumn) -> CoreError {
    fmt_err!(
        InvalidColumnData,
        "{} driver expects aux mapping, but missing for {} column {} in {}",
        driver.tag().name(),
        col.col_type,
        col.col_name,
        col.parent_path.display()
    )
}

pub(super) fn bad_aux_layout(driver: &impl ColumnDriver, col: &MappedColumn) -> String {
    format!(
        "bad layout of {} aux column {} in {}",
        driver.tag().name(),
        col.col_name,
        col.parent_path.display()
    )
}

pub(super) fn not_found(driver: &impl ColumnDriver, col: &MappedColumn, index: u64) -> CoreError {
    fmt_err!(
        InvalidColumnData,
        "{} entry index {} not found in aux for column {} in {}",
        driver.tag().name(),
        index,
        col.col_name,
        col.parent_path.display()
    )
}

pub(super) fn bad_data_size(
    driver: &impl ColumnDriver,
    col: &MappedColumn,
    data_size: u64,
) -> CoreError {
    fmt_err!(
        InvalidColumnData,
        "{} required data size {} exceeds data mmap len {} for column {} in {}",
        driver.tag().name(),
        data_size,
        col.data.len(),
        col.col_name,
        col.parent_path.display()
    )
}
