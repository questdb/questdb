use std::cmp;
use std::collections::VecDeque;
use std::io::Write;

use crate::parquet::error::fmt_err;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::{KeyValue, SchemaDescriptor, SortingColumn};
use parquet2::page::{CompressedPage, Page};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::write::{
    compress, Compressor, DynIter, DynStreamingIterator, FileWriter, RowGroupIter, Version,
    WriteOptions as FileWriteOptions,
};
use parquet2::FallibleStreamingIterator;
use qdb_core::error::CoreResult;

use crate::parquet_write::schema::{to_encodings, to_parquet_schema, Column, Partition};
use crate::parquet_write::{
    array, binary, boolean, fixed_len_bytes, primitive, string, symbol, varchar,
};
use qdb_core::col_type::{ColumnType, ColumnTypeTag};

use super::{util, GeoByte, GeoInt, GeoLong, GeoShort, IPv4};
use crate::parquet::error::{ParquetError, ParquetResult};
use crate::POOL;
use rayon::prelude::*;

const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;
const DEFAULT_ROW_GROUP_SIZE: usize = 100_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    /// Whether to write statistics
    pub write_statistics: bool,
    /// The page and file version to use
    pub version: Version,
    /// The compression to apply to every page
    pub compression: CompressionOptions,
    /// If `None` will be DEFAULT_ROW_GROUP_SIZE bytes
    pub row_group_size: Option<usize>,
    /// If `None` will be DEFAULT_PAGE_SIZE bytes
    pub data_page_size: Option<usize>,
    /// If true array columns will be encoded in native QDB format instead of nested lists
    pub raw_array_encoding: bool,
}

pub struct ParquetWriter<W: Write> {
    writer: W,
    /// Data page compression
    compression: CompressionOptions,
    /// Compute and write column statistics.
    statistics: bool,
    /// Encode arrays in native QDB format instead of nested lists.
    raw_array_encoding: bool,
    /// If `None` will be all written to a single row group.
    row_group_size: Option<usize>,
    /// if `None` will be DEFAULT_PAGE_SIZE bytes
    data_page_size: Option<usize>,
    version: Version,
    /// Sets sorting order of rows in the row group if any
    sorting_columns: Option<Vec<SortingColumn>>,
    /// Encode columns in parallel
    parallel: bool,
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
            raw_array_encoding: false,
            row_group_size: None,
            data_page_size: None,
            sorting_columns: None,
            version: Version::V1,
            parallel: false,
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

    /// Encode arrays in native QDB format instead of nested lists.
    #[allow(dead_code)]
    pub fn with_raw_array_encoding(mut self, raw_array_encoding: bool) -> Self {
        self.raw_array_encoding = raw_array_encoding;
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

    /// Serialize columns in parallel
    #[allow(dead_code)]
    pub fn set_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    fn write_options(&self) -> WriteOptions {
        WriteOptions {
            write_statistics: self.statistics,
            compression: self.compression,
            version: self.version,
            row_group_size: self.row_group_size,
            data_page_size: self.data_page_size,
            raw_array_encoding: self.raw_array_encoding,
        }
    }

    pub fn chunked(
        self,
        parquet_schema: SchemaDescriptor,
        encodings: Vec<Encoding>,
    ) -> ParquetResult<ChunkedWriter<W>> {
        let options = self.write_options();
        let parallel = self.parallel;
        let file_write_options = FileWriteOptions {
            write_statistics: options.write_statistics,
            version: options.version,
        };

        let created_by = Some("QuestDB version 9.0".to_string());
        let writer = FileWriter::with_sorting_columns(
            self.writer,
            parquet_schema.clone(),
            file_write_options,
            created_by,
            self.sorting_columns,
        );
        Ok(ChunkedWriter {
            writer,
            parquet_schema,
            encodings,
            options,
            parallel,
        })
    }

    /// Write the given `Partition` with the writer `W`. Returns the total size of the file.
    pub fn finish(self, partition: Partition) -> ParquetResult<u64> {
        let (schema, additional_meta) = to_parquet_schema(&partition, self.raw_array_encoding)?;
        let encodings = to_encodings(&partition);
        let mut chunked = self.chunked(schema, encodings)?;
        chunked.write_chunk(partition)?;
        chunked.finish(additional_meta)
    }
}

pub struct ChunkedWriter<W: Write> {
    writer: FileWriter<W>,
    parquet_schema: SchemaDescriptor,
    encodings: Vec<Encoding>,
    options: WriteOptions,
    parallel: bool,
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
                self.parallel,
            );
            self.writer.write(row_group?)?;
        }
        Ok(())
    }

    /// Write the footer of the parquet file. Returns the total size of the file.
    pub fn finish(&mut self, additional_meta: Vec<KeyValue>) -> ParquetResult<u64> {
        let size = self.writer.end(Some(additional_meta))?;
        Ok(size)
    }
}

struct CompressedPages {
    pages: VecDeque<ParquetResult<CompressedPage>>,
    current: Option<CompressedPage>,
}

impl CompressedPages {
    fn new(pages: VecDeque<ParquetResult<CompressedPage>>) -> Self {
        Self { pages, current: None }
    }
}

impl FallibleStreamingIterator for CompressedPages {
    type Item = CompressedPage;
    type Error = ParquetError;

    fn advance(&mut self) -> Result<(), Self::Error> {
        self.current = self.pages.pop_front().transpose()?;
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}

pub fn create_row_group(
    partition: &Partition,
    offset: usize,
    length: usize,
    column_types: &[ParquetType],
    encoding: &[Encoding],
    options: WriteOptions,
    parallel: bool,
) -> ParquetResult<RowGroupIter<'static, ParquetError>> {
    let columns = if parallel {
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
            let compressed_pages = encoded_column
                .into_iter()
                .map(|page| {
                    let page = page?;
                    let page = compress(page, vec![], options.compression)?;
                    Ok(Ok(page))
                })
                .collect::<ParquetResult<VecDeque<_>>>()?;

            Ok(DynStreamingIterator::new(CompressedPages::new(
                compressed_pages,
            )))
        };

        POOL.install(|| {
            partition
                .columns
                .par_iter()
                .zip(column_types)
                .zip(encoding)
                .flat_map(col_to_iter)
                .collect::<Vec<_>>()
        })
    } else {
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
            let compression_iter = Compressor::new(encoded_column, options.compression, vec![]);
            Ok(DynStreamingIterator::new(compression_iter))
        };

        partition
            .columns
            .iter()
            .zip(column_types)
            .zip(encoding)
            .flat_map(col_to_iter)
            .collect::<Vec<_>>()
    };

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
    match parquet_type {
        ParquetType::PrimitiveType(primitive_type) => column_chunk_to_primitive_pages(
            column,
            primitive_type,
            chunk_offset,
            chunk_length,
            options,
            encoding,
        ),
        ParquetType::GroupType { .. } => column_chunk_to_group_pages(
            column,
            parquet_type,
            chunk_offset,
            chunk_length,
            options,
            encoding,
        ),
    }
}

fn column_chunk_to_group_pages(
    column: Column,
    parquet_type: ParquetType,
    chunk_offset: usize,
    chunk_length: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    let number_of_rows = chunk_length;
    let max_page_size = options.data_page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    let bytes_per_row = bytes_per_group_type(column.data_type)?;
    let rows_per_page = cmp::max(max_page_size / bytes_per_row, 1);

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
        chunk_to_group_page(
            column,
            parquet_type.clone(),
            offset,
            length,
            options,
            encoding,
        )
    });

    Ok(DynIter::new(pages))
}

fn chunk_to_group_page(
    column: Column,
    parquet_type: ParquetType,
    offset: usize,
    length: usize,
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

    match column.data_type.tag() {
        ColumnTypeTag::Array => {
            let primitive_type = match array_primitive_type(parquet_type) {
                None => Err(fmt_err!(
                    InvalidType,
                    "failed to find inner-most type for array column {}",
                    column.name
                )),
                Some(t) => Ok(t),
            }?;
            let dim = column.data_type.array_dimensionality()? as usize;
            let aux: &[[u8; 16]] = unsafe { util::transmute_slice(column.secondary_data) };
            let data = column.primary_data;
            array::array_to_page(
                primitive_type,
                dim,
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                encoding,
            )
        }
        _ => Err(fmt_err!(
            InvalidType,
            "unsupported group type for column {}",
            column.name
        )),
    }
}

fn array_primitive_type(parquet_type: ParquetType) -> Option<PrimitiveType> {
    let mut primitive_type = None;
    let mut cur_type = &parquet_type;
    loop {
        match cur_type {
            ParquetType::PrimitiveType(t) => {
                primitive_type = Some(t);
                break;
            }
            ParquetType::GroupType {
                field_info: _,
                logical_type: _,
                converted_type: _,
                fields,
            } => {
                if fields.len() == 1 {
                    cur_type = &fields[0];
                } else {
                    break;
                }
            }
        }
    }

    primitive_type.cloned()
}

fn column_chunk_to_primitive_pages(
    column: Column,
    primitive_type: PrimitiveType,
    chunk_offset: usize,
    chunk_length: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<DynIter<'static, ParquetResult<Page>>> {
    if column.data_type.tag() == ColumnTypeTag::Symbol {
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
    let rows_per_page = cmp::max(
        max_page_size / bytes_per_primitive_type(primitive_type.physical_type),
        1,
    );

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
        chunk_to_primitive_page(
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

fn chunk_to_primitive_page(
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

    match column.data_type.tag() {
        ColumnTypeTag::Boolean => {
            let data = column.primary_data;
            boolean::slice_to_page(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnTypeTag::Byte => {
            let data: &[i8] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<i8, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Char => {
            let data: &[u16] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<u16, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Short => {
            let data: &[i16] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_notnull::<i16, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Int => {
            let data: &[i32] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<i32, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::IPv4 => {
            let data: &[IPv4] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<IPv4, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Long | ColumnTypeTag::Date => {
            let data: &[i64] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<i64, i64>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Timestamp => {
            let data: &[i64] = unsafe { util::transmute_slice(column.primary_data) };
            if column.designated_timestamp {
                primitive::int_slice_to_page_notnull::<i64, i64>(
                    &data[lower_bound..upper_bound],
                    adjusted_column_top,
                    options,
                    primitive_type,
                    encoding,
                )
            } else {
                primitive::int_slice_to_page_nullable::<i64, i64>(
                    &data[lower_bound..upper_bound],
                    adjusted_column_top,
                    options,
                    primitive_type,
                    encoding,
                )
            }
        }
        ColumnTypeTag::GeoByte => {
            let data: &[GeoByte] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoByte, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::GeoShort => {
            let data: &[GeoShort] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoShort, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::GeoInt => {
            let data: &[GeoInt] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoInt, i32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::GeoLong => {
            let data: &[GeoLong] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::int_slice_to_page_nullable::<GeoLong, i64>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Float => {
            let data: &[f32] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::float_slice_to_page_plain::<f32, f32>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnTypeTag::Double => {
            let data: &[f64] = unsafe { util::transmute_slice(column.primary_data) };
            primitive::float_slice_to_page_plain::<f64, f64>(
                &data[lower_bound..upper_bound],
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnTypeTag::Binary => {
            let aux: &[i64] = unsafe { util::transmute_slice(column.secondary_data) };
            let data = column.primary_data;
            binary::binary_to_page(
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::String => {
            let aux: &[i64] = unsafe { util::transmute_slice(column.secondary_data) };
            let data = column.primary_data;
            string::string_to_page(
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Varchar => {
            let aux: &[[u8; 16]] = unsafe { util::transmute_slice(column.secondary_data) };
            let data = column.primary_data;
            varchar::varchar_to_page(
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Array => {
            let aux: &[[u8; 16]] = unsafe { util::transmute_slice(column.secondary_data) };
            let data = column.primary_data;
            array::array_to_raw_page(
                &aux[lower_bound..upper_bound],
                data,
                adjusted_column_top,
                options,
                primitive_type,
                encoding,
            )
        }
        ColumnTypeTag::Long128 | ColumnTypeTag::Uuid => {
            let reversed = column.data_type.tag() == ColumnTypeTag::Uuid;
            let data: &[[u8; 16]] = unsafe { util::transmute_slice(column.primary_data) };
            fixed_len_bytes::bytes_to_page(
                &data[lower_bound..upper_bound],
                reversed,
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnTypeTag::Long256 => {
            let data: &[[u8; 32]] = unsafe { util::transmute_slice(column.primary_data) };
            fixed_len_bytes::bytes_to_page(
                &data[lower_bound..upper_bound],
                false,
                adjusted_column_top,
                options,
                primitive_type,
            )
        }
        ColumnTypeTag::Symbol => Err(fmt_err!(
            InvalidType,
            "unexpected symbol type in primitive encoder for column {} (should be handled earlier)",
            column.name,
        )),
        _ => todo!()
    }
}

fn bytes_per_primitive_type(primitive_type: PhysicalType) -> usize {
    match primitive_type {
        PhysicalType::Boolean => 1,
        PhysicalType::Int32 => 4,
        PhysicalType::Int96 => 12,
        PhysicalType::Float => 4,
        _ => 8,
    }
}

// gives a rough estimate of a row size in bytes
fn bytes_per_group_type(column_type: ColumnType) -> CoreResult<usize> {
    match column_type.tag() {
        ColumnTypeTag::Array => {
            let dim = column_type.array_dimensionality()?;
            Ok((dim * 8) as usize)
        }
        _ => Ok(8),
    }
}
