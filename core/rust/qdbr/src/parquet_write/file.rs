use std::io::Write;

use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::{SchemaDescriptor, SortingColumn};
use parquet2::page::{CompressedPage, Page};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::write::{
    Compressor, DynIter, DynStreamingIterator, FileWriter, RowGroupIter, Version,
    WriteOptions as FileWriteOptions,
};

use crate::parquet_write::schema::{
    to_encodings, to_parquet_schema, Column, ColumnType, Partition,
};
use crate::parquet_write::{
    binary, boolean, fixed_len_bytes, primitive, string, symbol, varchar, ParquetError,
    ParquetResult,
};

use super::{util, GeoByte, GeoInt, GeoLong, GeoShort, IPv4};

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
    version: Version,
    /// Sets sorting order of rows in the row group if any
    sorting_columns: Option<Vec<SortingColumn>>,
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
            sorting_columns: None,
            version: Version::V1,
        }
    }

    /// Set the compression used. Defaults to `Uncompressed`.
    #[allow(dead_code)]
    pub fn with_compression(mut self, compression: CompressionOptions) -> Self {
        self.compression = compression;
        self
    }

    /// Compute and write statistic
    #[allow(dead_code)]
    pub fn with_statistics(mut self, statistics: bool) -> Self {
        self.statistics = statistics;
        self
    }

    /// Set the row group size (in number of rows) during writing. This can reduce memory pressure and improve
    /// writing performance.
    #[allow(dead_code)]
    pub fn with_row_group_size(mut self, size: Option<usize>) -> Self {
        self.row_group_size = size;
        self
    }

    /// Sets the maximum bytes size of a data page. If `None` will be `DEFAULT_PAGE_SIZE` bytes.
    #[allow(dead_code)]
    pub fn with_data_page_size(mut self, limit: Option<usize>) -> Self {
        self.data_page_size = limit;
        self
    }

    /// Sets the maximum bytes size of a data page. If `None` will be `DEFAULT_PAGE_SIZE` bytes.
    #[allow(dead_code)]
    pub fn with_version(mut self, version: Version) -> Self {
        self.version = version;
        self
    }

    /// Sets sorting order of rows in the row group if any
    pub fn with_sorting_columns(mut self, sorting_columns: Option<Vec<SortingColumn>>) -> Self {
        self.sorting_columns = sorting_columns;
        self
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.statistics,
            compression: self.compression,
            version: self.version,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
        }
    }

    pub fn chunked(self, partition: &Partition) -> ParquetResult<ChunkedWriter<W>> {
        let parquet_schema = to_parquet_schema(partition)?;
        let encodings = to_encodings(partition);
        let options = self.write_options();
        let file_write_options = FileWriteOptions {
            write_statistics: options.write_statistics,
            version: options.version,
        };

        let created_by = Some("QuestDB version 8.0".to_string());
        let writer = FileWriter::with_sorting_columns(
            self.writer,
            parquet_schema.clone(),
            file_write_options,
            created_by,
            self.sorting_columns,
        );
        Ok(ChunkedWriter { writer, parquet_schema, encodings, options })
    }

    /// Write the given `Partition` with the writer `W`. Returns the total size of the file.
    pub fn finish(self, partition: Partition) -> ParquetResult<u64> {
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
    pub fn write_chunk(&mut self, partition: Partition) -> ParquetResult<()> {
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
        let schema = &self.parquet_schema;
        for (offset, length) in row_group_range {
            let row_group = create_row_group(
                &partition,
                offset,
                length,
                schema.fields(),
                &self.encodings,
                self.options,
            );
            self.writer.write(row_group?)?;
        }
        Ok(())
    }

    /// Write the footer of the parquet file. Returns the total size of the file.
    pub fn finish(&mut self) -> ParquetResult<u64> {
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
) -> ParquetResult<RowGroupIter<'static, ParquetError>> {
    let col_to_iter = move |((column, column_type), encoding): (
        (&Column, &ParquetType),
        &Encoding,
    )|
          -> ParquetResult<
        DynStreamingIterator<CompressedPage, ParquetError>,
    > {
        let encoded_column = column_chunk_to_pages(
            *column,
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
        .flat_map(col_to_iter)
        .collect::<Vec<_>>();

    Ok(DynIter::new(columns.into_iter().map(Ok)))
}

fn column_chunk_to_pages(
    column: Column,
    parquet_type: ParquetType,
    chunk_offset: usize,
    chunk_length: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let primitive_type = match parquet_type {
        ParquetType::PrimitiveType(primitive) => primitive,
        _ => unreachable!("GroupType is not supported"),
    };

    if matches!(column.data_type, ColumnType::Symbol) {
        let keys: &[i32] = unsafe { util::transmute_slice(column.primary_data) };

        let offsets = column.symbol_offsets;
        let data = column.secondary_data;
        let orig_column_top = column.column_top;

        let mut adjusted_column_top = 0;
        let lower_bound = if chunk_offset < orig_column_top {
            adjusted_column_top = orig_column_top - chunk_offset;
            0
        } else {
            chunk_offset - orig_column_top
        };
        let upper_bound = if chunk_offset + chunk_length < orig_column_top {
            adjusted_column_top = chunk_length;
            0
        } else {
            chunk_offset + chunk_length - orig_column_top
        };
        return symbol::symbol_to_pages(
            &keys[lower_bound..upper_bound],
            offsets,
            data,
            adjusted_column_top,
            options,
            primitive_type,
        );
    }

    let number_of_rows = chunk_length;
    let max_page_size = options.data_page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let rows_per_page = max_page_size / bytes_per_type(primitive_type.physical_type);

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
            column,
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
    primitive_type: PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<Page> {
    let orig_column_top = column.column_top;

    let mut adjusted_column_top = 0;
    let lower_bound = if offset < orig_column_top {
        adjusted_column_top = orig_column_top - offset;
        0
    } else {
        offset - orig_column_top
    };
    let upper_bound = if offset + length < orig_column_top {
        adjusted_column_top = length;
        0
    } else {
        offset + length - orig_column_top
    };

    match column.data_type {
        ColumnType::Boolean => {
            let column = column.primary_data;
            boolean::slice_to_page(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnType::Byte => {
            let column: &[i8] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<i8, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Char => {
            let column: &[u16] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<u16, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Short => {
            let column: &[i16] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<i16, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Int => {
            let column: &[i32] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<i32, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::IPv4 => {
            let column: &[IPv4] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<IPv4, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Long | ColumnType::Date | ColumnType::Timestamp => {
            let column: &[i64] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<i64, i64>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::GeoByte => {
            let column: &[GeoByte] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoByte, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::GeoShort => {
            let column: &[GeoShort] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoShort, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::GeoInt => {
            let column: &[GeoInt] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoInt, i32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::GeoLong => {
            let column: &[GeoLong] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoLong, i64>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Float => {
            let column: &[f32] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::float_slice_to_page_plain::<f32, f32>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnType::Double => {
            let column: &[f64] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::float_slice_to_page_plain::<f64, f64>(
                &column[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnType::Binary => {
            let data = column.primary_data;
            let offsets: &[i64] = unsafe { util::transmute_slice(column.secondary_data) };
            binary::binary_to_page(
                &offsets[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::String => {
            let data = column.primary_data;
            let offsets: &[i64] = unsafe { util::transmute_slice(column.secondary_data) };
            string::string_to_page(
                &offsets[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Varchar => {
            let data = column.primary_data;
            let aux: &[[u8; 16]] = unsafe { util::transmute_slice(column.secondary_data) };
            varchar::varchar_to_page(
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnType::Long128 | ColumnType::Uuid => {
            let reversed = column.data_type == ColumnType::Uuid;
            let column: &[[u8; 16]] = unsafe { util::transmute_slice(column.primary_data) };
            fixed_len_bytes::bytes_to_page(
                &column[lower_bound..upper_bound],
                reversed,
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnType::Long256 => {
            let column: &[[u8; 32]] = unsafe { util::transmute_slice(column.primary_data) };
            fixed_len_bytes::bytes_to_page(
                &column[lower_bound..upper_bound],
                false,
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnType::Symbol => {
            panic!("Symbol type is encoded in column_chunk_to_pages()")
        }
    }
}

fn bytes_per_type(primitive_type: PhysicalType) -> usize {
    match primitive_type {
        PhysicalType::Boolean => 1,
        PhysicalType::Int32 => 4,
        PhysicalType::Int96 => 12,
        PhysicalType::Float => 4,
        _ => 8,
    }
}
