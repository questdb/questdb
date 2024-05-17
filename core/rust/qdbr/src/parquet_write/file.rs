use crate::parquet_write::schema::{
    to_encodings, to_parquet_schema, Column, ColumnType, Partition,
};
use crate::parquet_write::{binary, boolean, fixed_len_bytes, primitive, string, symbol};
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::SchemaDescriptor;
use parquet2::page::{CompressedPage, Page};
use parquet2::schema::types::{ParquetType, PrimitiveType};
use parquet2::write::{
    Compressor, DynIter, DynStreamingIterator, FileWriter, RowGroupIter, Version,
    WriteOptions as FileWriteOptions,
};
use std::io::Write;
use std::mem;
use arrow::compute::concat;
use crate::parquet_write::symbol::symbol_to_pages;

const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
const DEFAULT_ROW_GROUP_SIZE: usize = 512 * 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    /// Whether to write statistics
    pub write_statistics: bool,
    /// The page and file version to use
    pub version: Version,
    /// The compression to apply to every page
    pub compression: CompressionOptions,
    /// if `None` will be DEFAULT_ROW_GROUP_SIZE bytes
    pub row_group_size: Option<usize>,
    /// if `None` will be DEFAULT_PAGE_SIZE bytes
    pub data_page_size: Option<usize>,
}

pub struct ParquetWriter<W: Write> {
    writer: W,
    /// Data page compression
    compression: CompressionOptions,
    /// Compute and write column statistics.
    statistics: bool,
    /// If `None` will be all written to a single row group.
    row_group_size: Option<usize>,
    /// if `None` will be DEFAULT_PAGE_SIZE bytes
    data_page_size: Option<usize>,
}

impl<W: Write> ParquetWriter<W> {
    /// Create a new writer
    pub fn new(writer: W) -> Self
    where
        W: Write,
    {
        ParquetWriter {
            writer,
            compression: CompressionOptions::Uncompressed,
            statistics: true,
            row_group_size: None,
            data_page_size: None,
        }
    }

    /// Set the compression used. Defaults to `Uncompressed`.
    pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
        self.compression = compression;
        self
    }

    /// Compute and write statistic
    pub fn with_statistics(mut self, statistics: bool) -> Self {
        self.statistics = statistics;
        self
    }

    /// Set the row group size (in number of rows) during writing. This can reduce memory pressure and improve
    /// writing performance.
    pub fn with_row_group_size(mut self, size: Option<usize>) -> Self {
        self.row_group_size = size;
        self
    }

    /// Sets the maximum bytes size of a data page. If `None` will be `DEFAULT_PAGE_SIZE` bytes.
    pub fn with_data_page_size(mut self, limit: Option<usize>) -> Self {
        self.data_page_size = limit;
        self
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.statistics,
            compression: self.compression,
            version: Version::V2,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
        }
    }

    pub fn chunked(self, partition: &Partition) -> parquet2::error::Result<ChunkedWriter<W>> {
        let parquet_schema = to_parquet_schema(&partition)?;
        let encodings = to_encodings(&partition);
        let options = self.write_options();
        let file_write_options = FileWriteOptions {
            write_statistics: options.write_statistics,
            version: options.version,
        };

        let created_by = Some("QuestDB".to_string());
        let writer = FileWriter::new(
            self.writer,
            parquet_schema.clone(),
            file_write_options,
            created_by,
        );
        Ok(ChunkedWriter {
            writer,
            parquet_schema,
            encodings,
            options,
        })
    }

    /// Write the given `Partition` in the writer `W`. Returns the total size of the file.
    pub fn finish(self, partition: Partition) -> parquet2::error::Result<u64> {
        let mut chunked = self.chunked(&partition)?;
        chunked.write_chunk(partition)?;
        chunked.finish()
    }
}

pub struct ChunkedWriter<W: Write> {
    writer: FileWriter<W>,
    parquet_schema: SchemaDescriptor,
    encodings: Vec<Encoding>,
    options: WriteOptions,
}

impl<W: Write> ChunkedWriter<W> {
    /// Write a chunk to the parquet writer.
    pub fn write_chunk(&mut self, partition: Partition) -> parquet2::error::Result<()> {
        let schema = to_parquet_schema(&partition)?;
        let encodings = to_encodings(&partition);
        let row_group_size = self
            .options
            .row_group_size
            .unwrap_or(DEFAULT_ROW_GROUP_SIZE);
        let partition_length = partition.columns[0].row_count;
        let row_group_range = (0..partition_length)
            .step_by(row_group_size)
            .map(move |offset| {
                let length = if offset + row_group_size > partition_length {
                    partition_length - offset
                } else {
                    row_group_size
                };
                (offset, length)
            });
        for (offset, length) in row_group_range {
            let row_group = create_row_group(
                &partition,
                offset,
                length,
                schema.fields(),
                &encodings,
                self.options,
            );
            self.writer.write(row_group?)?;
        }
        Ok(())
    }

    pub fn get_writer(&self) -> &FileWriter<W> {
        &self.writer
    }

    /// Writes the footer of the parquet file. Returns the total size of the file.
    pub fn finish(&mut self) -> parquet2::error::Result<u64> {
        let size = self.writer.end(None)?;
        Ok(size)
    }
}

fn create_row_group(
    partition: &Partition,
    offset: usize,
    length: usize,
    column_types: &[ParquetType],
    encoding: &[Encoding],
    options: WriteOptions,
) -> parquet2::error::Result<RowGroupIter<'static, parquet2::error::Error>> {
    let func = move |((column, column_type), encoding): ((&Column, &ParquetType), &Encoding)| -> parquet2::error::Result<
        DynStreamingIterator<CompressedPage, parquet2::error::Error>,
    > {
        let encoded_column = column_chunk_to_pages(
            column.clone(),
            column_type.clone(),
            offset,
            length,
            options,
            *encoding,
        )
        .expect("encoded_column");

        Ok(DynStreamingIterator::new(Compressor::new(
            encoded_column,
            options.compression,
            vec![],
        )))
    };

    let columns = partition
        .columns
        .iter()
        .zip(column_types)
        .zip(encoding)
        .flat_map(func)
        .collect::<Vec<_>>();

    Ok(DynIter::new(columns.into_iter().map(|x| Ok(x))))
}

fn column_chunk_to_pages(
    column: Column,
    type_: ParquetType,
    chunk_offset: usize,
    chunk_length: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> parquet2::error::Result<DynIter<'static, parquet2::error::Result<Page>>> {

    let primitive_type = match type_ {
        ParquetType::PrimitiveType(primitive) => primitive,
        _ => unreachable!("GroupType is not supported"),
    };

    if matches!(column.data_type, ColumnType::Symbol) {
        let keys: &[i32] =
            unsafe { mem::transmute(&column.fixed_len_data[chunk_offset..chunk_offset + chunk_length]) };
        let offsets = column.symbol_offsets.expect("symbol offsets");
        let data = column.variable_len_data.expect("symbol data");
        return symbol::symbol_to_pages(keys, offsets, data, options, primitive_type);
    }

    let number_of_rows = chunk_length;
    let max_page_size = options.data_page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let max_page_size = max_page_size.min(2usize.pow(31) - 2usize.pow(25));
    // let rows_per_page = (max_page_size / (std::mem::size_of::<T>() + 1)).max(1);
    let rows_per_page = 10; //TODO: estimate chunk byte size

    let rows = (0..number_of_rows)
        .step_by(rows_per_page)
        .map(move |offset| {
            let length = if offset + rows_per_page > number_of_rows {
                number_of_rows - offset
            } else {
                rows_per_page
            };
            (chunk_offset + offset, length)
        });


    let pages = rows.map(move |(offset, length)| {
        chunk_to_page(
            column.clone(),
            offset,
            length,
            primitive_type.clone(),
            options,
            encoding,
        )
    });

    Ok(DynIter::new(pages))
}

fn chunk_to_page(
    column: Column,
    offset: usize,
    length: usize,
    type_: PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
) -> parquet2::error::Result<Page> {
    match column.data_type {
        ColumnType::Boolean => {
            let chunk: &[u8] = unsafe { mem::transmute(column.fixed_len_data) };
            boolean::slice_to_page(&chunk[offset..offset + length], options, type_)
        }
        ColumnType::Byte => {
            let chunk: &[i8] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::int_slice_to_page::<i8, i32>(&chunk[offset..offset + length], options, type_, encoding)
        }
        ColumnType::Short => {
            let chunk: &[i16] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::int_slice_to_page::<i16, i32>(&chunk[offset..offset + length], options, type_, encoding)
        }
        ColumnType::Int => {
            let chunk: &[i32] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::int_slice_to_page::<i32, i32>(&chunk[offset..offset + length], options, type_, encoding)
        }
        ColumnType::Long => {
            let chunk: &[i64] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::int_slice_to_page::<i64, i64>(&chunk[offset..offset + length], options, type_, encoding)
        }
        ColumnType::Float => {
            let chunk: &[f32] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::float_slice_to_page_plain::<f32, f32>(&chunk[offset..offset + length], options, type_)
        }
        ColumnType::Double => {
            let chunk: &[f64] = unsafe { mem::transmute(column.fixed_len_data) };
            primitive::float_slice_to_page_plain::<f64, f64>(&chunk[offset..offset + length], options, type_)
        }
        ColumnType::Binary => {
            let offsets: &[i64] = unsafe { mem::transmute(column.fixed_len_data) };
            let data = column.variable_len_data.expect("data");
            binary::binary_to_page(&offsets[offset..offset + length], data, options, type_, encoding)
        }
        ColumnType::String => {
            let offsets: &[i64] = unsafe { mem::transmute(column.fixed_len_data) };
            let data = column.variable_len_data.expect("data");
            string::string_to_page(&offsets[offset..offset + length], data, options, type_, encoding)
        }
        ColumnType::Long128 => {
            //TODO: fix slicing
            let chunk= &column.fixed_len_data[offset..offset + length];
            fixed_len_bytes::bytes_to_page(chunk, 16, options, type_)
        }
        ColumnType::Long256 => {
            //TODO: fix slicing
            let chunk= &column.fixed_len_data[offset..offset + length];
            fixed_len_bytes::bytes_to_page(chunk, 32, options, type_)
        }
        other => Err(parquet2::error::Error::FeatureNotSupported(format!(
            "Writing parquet pages for data type {:?}",
            other
        ))),
    }
}
