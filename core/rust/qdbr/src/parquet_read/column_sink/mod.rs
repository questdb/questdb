use crate::parquet::error::ParquetResult;

pub mod var;

pub trait Pushable<Sink> {
    fn reserve(&mut self, sink: &mut Sink, count: usize) -> ParquetResult<()>;
    fn push(&mut self, sink: &mut Sink) -> ParquetResult<()>;
    fn push_slice(&mut self, sink: &mut Sink, count: usize) -> ParquetResult<()>;
    fn push_null(&mut self, sink: &mut Sink) -> ParquetResult<()>;
    fn push_nulls(&mut self, sink: &mut Sink, count: usize) -> ParquetResult<()>;
    fn skip(&mut self, count: usize) -> ParquetResult<()>;
}
