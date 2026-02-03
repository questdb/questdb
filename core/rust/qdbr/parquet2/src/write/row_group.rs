use std::collections::HashSet;
use std::io::Write;
use std::sync::{Arc, Mutex};

#[cfg(feature = "async")]
use futures::AsyncWrite;

use parquet_format_safe::{ColumnChunk, RowGroup, SortingColumn};

use crate::{
    error::{Error, Result},
    metadata::{ColumnChunkMetaData, ColumnDescriptor},
    page::CompressedPage,
};

#[cfg(feature = "async")]
use super::column_chunk::write_column_chunk_async;

use super::{
    column_chunk::write_column_chunk,
    page::{is_data_page, PageWriteSpec},
    DynIter, DynStreamingIterator,
};

pub struct ColumnOffsetsMetadata {
    pub dictionary_page_offset: Option<i64>,
    pub data_page_offset: Option<i64>,
}

impl ColumnOffsetsMetadata {
    pub fn from_column_chunk(column_chunk: &ColumnChunk) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.dictionary_page_offset)
                .unwrap_or(None),
            data_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.data_page_offset),
        }
    }

    pub fn from_column_chunk_metadata(
        column_chunk_metadata: &ColumnChunkMetaData,
    ) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk_metadata.dictionary_page_offset(),
            data_page_offset: Some(column_chunk_metadata.data_page_offset()),
        }
    }

    pub fn calc_row_group_file_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
            .filter(|x| *x > 0_i64)
            .or(self.data_page_offset)
    }
}

fn compute_num_rows(columns: &[(ColumnChunk, Vec<PageWriteSpec>)]) -> Result<i64> {
    columns
        .first()
        .map(|(_, specs)| {
            let mut num_rows = 0;
            specs
                .iter()
                .filter(|x| is_data_page(x))
                .try_for_each(|spec| {
                    num_rows += spec.num_rows.ok_or_else(|| {
                        Error::oos("All data pages must declare the number of rows on it")
                    })? as i64;
                    Result::Ok(())
                })?;
            Result::Ok(num_rows)
        })
        .unwrap_or(Ok(0))
}

#[allow(clippy::too_many_arguments)]
pub fn write_row_group<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
    sorting_columns: &Option<Vec<SortingColumn>>,
    ordinal: usize,
    bloom_filter_fpp: f64,
    bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
) -> std::result::Result<(RowGroup, Vec<Vec<PageWriteSpec>>, u64), E>
where
    W: Write,
    E: std::error::Error + From<Error>,
{
    let column_iter = descriptors.iter().zip(columns).zip(bloom_hashes.iter());

    let initial = offset;
    let columns = column_iter
        .map(|((descriptor, page_iter), bloom)| {
            let bloom_ref = bloom.as_ref().map(|arc| arc.as_ref());
            let (column, page_specs, size) = write_column_chunk(
                writer,
                offset,
                descriptor,
                page_iter?,
                bloom_filter_fpp,
                bloom_ref,
            )?;
            offset += size;
            Ok((column, page_specs))
        })
        .collect::<std::result::Result<Vec<_>, E>>()?;
    let bytes_written = offset - initial;

    let num_rows = compute_num_rows(&columns)?;

    // compute row group stats
    let file_offset = columns
        .first()
        .map(|(column_chunk, _)| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_uncompressed_size)
        .sum();
    let total_compressed_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    let (columns, specs) = columns.into_iter().unzip();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows,
            sorting_columns: sorting_columns.clone(),
            file_offset,
            total_compressed_size: Some(total_compressed_size),
            ordinal: ordinal.try_into().ok(),
        },
        specs,
        bytes_written,
    ))
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub async fn write_row_group_async<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
    sorting_columns: &Option<Vec<SortingColumn>>,
    ordinal: usize,
    bloom_filter_fpp: f64,
    bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
) -> Result<(RowGroup, Vec<Vec<PageWriteSpec>>, u64)>
where
    W: AsyncWrite + Unpin + Send,
    Error: From<E>,
    E: std::error::Error,
{
    let column_iter = descriptors.iter().zip(columns).zip(bloom_hashes.iter());

    let initial = offset;
    let mut columns = vec![];
    for ((descriptor, page_iter), bloom) in column_iter {
        let bloom_ref = bloom.as_ref().map(|arc| arc.as_ref());
        let (column, page_specs, size) = write_column_chunk_async(
            writer,
            offset,
            descriptor,
            page_iter?,
            bloom_filter_fpp,
            bloom_ref,
        )
        .await?;
        offset += size;
        columns.push((column, page_specs));
    }
    let bytes_written = offset - initial;

    let num_rows = compute_num_rows(&columns)?;

    // compute row group stats
    let file_offset = columns
        .get(0)
        .map(|(column_chunk, _)| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_uncompressed_size)
        .sum();
    let total_compressed_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    let (columns, specs) = columns.into_iter().unzip();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows: num_rows as i64,
            sorting_columns: sorting_columns.clone(),
            file_offset,
            total_compressed_size: Some(total_compressed_size),
            ordinal: ordinal.try_into().ok(),
        },
        specs,
        bytes_written,
    ))
}
