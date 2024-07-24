pub mod fixed;
pub mod var;

use crate::parquet_write::ParquetResult;

pub trait Pushable {
    fn reserve(&mut self);
    fn push(&mut self);
    fn push_slice(&mut self, count: usize);
    fn push_null(&mut self);
    fn push_nulls(&mut self, count: usize);
    fn skip(&mut self, count: usize);
    fn result(&self) -> ParquetResult<()>;
}
