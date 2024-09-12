use crate::parquet_read::error::ParquetReadResult;

pub mod fixed;
pub mod var;

pub trait Pushable {
    fn reserve(&mut self);
    fn push(&mut self);
    fn push_slice(&mut self, count: usize);
    fn push_null(&mut self);
    fn push_nulls(&mut self, count: usize);
    fn skip(&mut self, count: usize);
    fn result(&self) -> ParquetReadResult<()>;
}
