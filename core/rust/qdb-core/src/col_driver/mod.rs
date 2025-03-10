
pub trait ColumnDriver {
    fn col_sizes_at_row(&self, txn: usize, ) -> Result<Vec<(usize, Option<usizex64>)>, Error>;
}