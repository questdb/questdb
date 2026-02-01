use crate::parquet::error::ParquetResult;

pub mod fixed;
pub mod var;

#[cfg(test)]
mod tests;

pub trait Pushable {
    fn reserve(&mut self) -> ParquetResult<()>;
    fn push(&mut self) -> ParquetResult<()>;
    fn push_slice(&mut self, count: usize) -> ParquetResult<()>;
    fn push_null(&mut self) -> ParquetResult<()>;
    fn push_nulls(&mut self, count: usize) -> ParquetResult<()>;
    fn skip(&mut self, count: usize);
    fn result(&self) -> ParquetResult<()>;
}
