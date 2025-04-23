use crate::col_driver::{ColumnDriver, MappedColumn};
use crate::error::{CoreResult, fmt_err};

/// The designated timestamp carries 16-byte entries per row.
/// * The first 8 bytes are the timestamp in micros since epoch (as usual).
/// * The next 8 bytes are the insert index used for O3 merge logic.
pub struct DesignatedTimestampDriver;

impl ColumnDriver for DesignatedTimestampDriver {
    fn col_sizes_for_row_count(
        &self,
        col: &MappedColumn,
        row_count: u64,
    ) -> CoreResult<(u64, Option<u64>)> {
        let row_size = 16u64;
        let data_size = row_size * row_count;
        if data_size > col.data.len() as u64 {
            return Err(fmt_err!(
                InvalidLayout,
                "data file for {} column {} shorter than {} rows, expected at least {} bytes but is {} at {}",
                self.descr(),
                col.col_name,
                row_count,
                data_size,
                col.data.len(),
                col.parent_path.display()
            ));
        }
        Ok((data_size, None))
    }

    fn descr(&self) -> &'static str {
        "designated-timestamp"
    }
}
