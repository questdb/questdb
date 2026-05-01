use std::collections::HashSet;
use std::io::Write;
use std::sync::{Arc, Mutex};

use crate::parquet::error::fmt_err;
use parquet2::compression::CompressionOptions;
use parquet2::encoding::Encoding;
use parquet2::metadata::{KeyValue, SchemaDescriptor, SortingColumn};
use parquet2::schema::types::ParquetType;
use parquet2::write::{
    Compressor, DynIter, DynStreamingIterator, FileWriter, RowGroupIter, Version,
    WriteOptions as FileWriteOptions,
};

use crate::parquet_write::encode::encode_column_chunk;
use crate::parquet_write::schema::{
    to_compressions, to_encodings, to_parquet_schema, Column, Partition,
};

use crate::parquet::error::{ParquetError, ParquetResult};
use crate::POOL;
use rayon::prelude::*;

pub const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;

pub const DEFAULT_ROW_GROUP_SIZE: usize = 100_000;

/// Returns the compression for a given column, using per-column override if available.
fn column_compression(
    per_column_compressions: &[Option<CompressionOptions>],
    global_compression: CompressionOptions,
    col_idx: usize,
) -> CompressionOptions {
    per_column_compressions
        .get(col_idx)
        .and_then(|opt| *opt)
        .unwrap_or(global_compression)
}

#[derive(Debug, Copy, Clone, PartialEq)]
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
    /// False positive probability for bloom filters
    pub bloom_filter_fpp: f64,
    /// Minimum compression ratio (uncompressed/compressed) to keep compressed output.
    /// A value of 0.0 (or <= 1.0) means always keep compressed output.
    pub min_compression_ratio: f64,
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
    /// Column indices that should have bloom filters
    bloom_filter_columns: HashSet<usize>,
    /// False positive probability for bloom filters
    bloom_filter_fpp: f64,
    /// Minimum compression ratio to keep compressed output
    min_compression_ratio: f64,
    /// Partition squash tracker value to embed in QdbMeta footer (-1 = not set)
    squash_tracker: i64,
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
            bloom_filter_columns: HashSet::new(),
            bloom_filter_fpp: DEFAULT_BLOOM_FILTER_FPP,
            min_compression_ratio: 0.0,
            squash_tracker: -1,
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

    /// Encode arrays in native QDB format instead of nested lists.
    pub fn with_raw_array_encoding(mut self, raw_array_encoding: bool) -> Self {
        self.raw_array_encoding = raw_array_encoding;
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

    /// Sets the maximum bytes size of a data page. If `None` will be `DEFAULT_PAGE_SIZE` bytes.
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
    pub fn with_parallel(mut self, parallel: bool) -> Self {
        self.parallel = parallel;
        self
    }

    /// Set which columns should have bloom filters written
    pub fn with_bloom_filter_columns(mut self, columns: HashSet<usize>) -> Self {
        self.bloom_filter_columns = columns;
        self
    }

    /// Set false positive probability for bloom filters.
    pub fn with_bloom_filter_fpp(mut self, fpp: f64) -> Self {
        debug_assert!(
            fpp > 0.0 && fpp < 1.0,
            "bloom filter fpp must be in (0.0, 1.0)"
        );
        self.bloom_filter_fpp = fpp;
        self
    }

    /// Set the minimum compression ratio to keep compressed output.
    /// A value of 0.0 means always keep compressed output.
    pub fn with_min_compression_ratio(mut self, min_compression_ratio: f64) -> Self {
        self.min_compression_ratio = min_compression_ratio;
        self
    }

    pub fn with_squash_tracker(mut self, squash_tracker: i64) -> Self {
        self.squash_tracker = squash_tracker;
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
            bloom_filter_fpp: self.bloom_filter_fpp,
            min_compression_ratio: self.min_compression_ratio,
        }
    }

    pub fn chunked(
        self,
        parquet_schema: SchemaDescriptor,
        encodings: Vec<Encoding>,
    ) -> ParquetResult<ChunkedWriter<W>> {
        let compressions = encodings.iter().map(|_| None).collect();
        self.chunked_with_compressions(parquet_schema, encodings, compressions)
    }

    pub fn chunked_with_compressions(
        self,
        parquet_schema: SchemaDescriptor,
        encodings: Vec<Encoding>,
        per_column_compressions: Vec<Option<CompressionOptions>>,
    ) -> ParquetResult<ChunkedWriter<W>> {
        let options = self.write_options();
        let parallel = self.parallel;
        let file_write_options = FileWriteOptions {
            write_statistics: options.write_statistics,
            version: options.version,
            bloom_filter_fpp: options.bloom_filter_fpp,
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
            per_column_compressions,
            bloom_filter_columns: self.bloom_filter_columns,
            parallel,
        })
    }

    /// Write the given `Partition` with the writer `W`. Returns the total size of the file.
    pub fn finish(self, partition: Partition) -> ParquetResult<u64> {
        let (schema, additional_meta) =
            to_parquet_schema(&partition, self.raw_array_encoding, self.squash_tracker)?;
        let encodings = to_encodings(&partition);
        let compressions = to_compressions(&partition);
        let mut chunked = self.chunked_with_compressions(schema, encodings, compressions)?;
        chunked.write_chunk(&partition)?;
        chunked.finish(additional_meta)
    }
}

pub struct ChunkedWriter<W: Write> {
    writer: FileWriter<W>,
    parquet_schema: SchemaDescriptor,
    encodings: Vec<Encoding>,
    options: WriteOptions,
    per_column_compressions: Vec<Option<CompressionOptions>>,
    bloom_filter_columns: HashSet<usize>,
    parallel: bool,
}

impl<W: Write> ChunkedWriter<W> {
    /// Write a chunk to the parquet writer.
    pub fn write_chunk(&mut self, partition: &Partition) -> ParquetResult<()> {
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
            let (row_group, bloom_hashes) = create_row_group(
                partition,
                offset,
                length,
                schema.fields(),
                &self.encodings,
                self.options,
                &self.per_column_compressions,
                &self.bloom_filter_columns,
                self.parallel,
            )?;
            self.writer.write(row_group, &bloom_hashes)?;
        }
        Ok(())
    }

    pub fn write_row_group_from_partitions(
        &mut self,
        partitions: &[&Partition],
        first_partition_start: usize,
        last_partition_end: usize,
    ) -> ParquetResult<()> {
        let schema = &self.parquet_schema;
        let (row_group, bloom_hashes) = create_row_group_from_partitions(
            partitions,
            first_partition_start,
            last_partition_end,
            schema.fields(),
            &self.encodings,
            self.options,
            &self.per_column_compressions,
            &self.bloom_filter_columns,
            self.parallel,
        )?;
        self.writer.write(row_group, &bloom_hashes)?;
        Ok(())
    }

    /// Write the footer of the parquet file. Returns the total size of the file.
    pub fn finish(&mut self, additional_meta: Vec<KeyValue>) -> ParquetResult<u64> {
        let size = self.writer.end(Some(additional_meta))?;
        Ok(size)
    }
}

pub type BloomHashes = Vec<Option<Arc<Mutex<HashSet<u64>>>>>;

#[allow(clippy::too_many_arguments)]
pub fn create_row_group(
    partition: &Partition,
    offset: usize,
    length: usize,
    column_types: &[ParquetType],
    encoding: &[Encoding],
    options: WriteOptions,
    per_column_compressions: &[Option<CompressionOptions>],
    bloom_filter_columns: &HashSet<usize>,
    parallel: bool,
) -> ParquetResult<(RowGroupIter<'static, ParquetError>, BloomHashes)> {
    // A single-partition row group is a degenerate multi-partition write with
    // one partition; the bounds collapse to (offset, offset + length).
    let partitions = [partition];
    create_row_group_from_partitions(
        &partitions,
        offset,
        offset + length,
        column_types,
        encoding,
        options,
        per_column_compressions,
        bloom_filter_columns,
        parallel,
    )
}

/// Creates a single RowGroup from multiple partitions.
///
/// This function merges data from multiple partitions into one RowGroup.
/// The first partition starts at `first_partition_start`, middle partitions
/// use all their data, and the last partition ends at `last_partition_end`.
///
/// # Arguments
/// * `partitions` - Slice of partition references to merge
/// * `first_partition_start` - Start offset in the first partition
/// * `last_partition_end` - End position (exclusive) in the last partition
/// * `column_types` - Parquet types for each column
/// * `encoding` - Encoding for each column
/// * `options` - Write options
/// * `parallel` - Whether to process columns in parallel
#[allow(clippy::too_many_arguments)]
pub fn create_row_group_from_partitions(
    partitions: &[&Partition],
    first_partition_start: usize,
    last_partition_end: usize,
    column_types: &[ParquetType],
    encoding: &[Encoding],
    options: WriteOptions,
    per_column_compressions: &[Option<CompressionOptions>],
    bloom_filter_columns: &HashSet<usize>,
    parallel: bool,
) -> ParquetResult<(RowGroupIter<'static, ParquetError>, BloomHashes)> {
    if partitions.is_empty() {
        return Err(fmt_err!(
            InvalidLayout,
            "create_row_group_from_partitions: partitions cannot be empty"
        ));
    }
    let num_columns = partitions[0].columns.len();

    // Collect unique hash values for bloom filter construction.
    // See comment in create_row_group() for rationale on using HashSet vs direct bloom
    // filter writing. Memory: ~20 bytes per distinct value, ~20MB for 1M unique values.
    let bloom_hashes: BloomHashes = (0..num_columns)
        .map(|col_idx| {
            if bloom_filter_columns.contains(&col_idx) {
                Some(Arc::new(Mutex::new(HashSet::new())))
            } else {
                None
            }
        })
        .collect();

    // Per-column encoder closure. Calls into the new top-level dispatch in
    // `parquet_write::encode::encode_column_chunk`, which mirrors the decoder
    // side and handles single- and multi-partition writes uniformly.
    let col_to_iter = |col_idx: usize,
                       options: WriteOptions,
                       bloom_set: Option<Arc<Mutex<HashSet<u64>>>>|
     -> ParquetResult<
        DynStreamingIterator<'static, parquet2::page::CompressedPage, ParquetError>,
    > {
        let column_type = &column_types[col_idx];
        let col_encoding = encoding[col_idx];
        let first_partition_column = partitions[0].columns[col_idx];
        let col_compression =
            column_compression(per_column_compressions, options.compression, col_idx);

        // Designated timestamp columns force statistics on regardless of the
        // global write_statistics flag.
        let col_options = if first_partition_column.designated_timestamp {
            WriteOptions { write_statistics: true, ..options }
        } else {
            options
        };

        // Build the per-partition Column slice for this column index.
        let columns: Vec<Column> = partitions
            .iter()
            .map(|partition| partition.columns[col_idx])
            .collect();

        let pages = encode_column_chunk(
            col_encoding,
            column_type,
            &columns,
            first_partition_start,
            last_partition_end,
            col_options,
            bloom_set,
        )?;

        let compressor = Compressor::new(
            pages.into_iter().map(Ok),
            col_compression,
            vec![],
            col_options.min_compression_ratio,
        );
        Ok(DynStreamingIterator::new(compressor))
    };

    let columns: Vec<_> = if parallel {
        POOL.install(|| {
            (0..num_columns)
                .into_par_iter()
                .zip(&bloom_hashes)
                .map(|(col_idx, bloom)| col_to_iter(col_idx, options, bloom.clone()))
                .collect::<ParquetResult<Vec<_>>>()
        })?
    } else {
        (0..num_columns)
            .zip(&bloom_hashes)
            .map(|(col_idx, bloom)| col_to_iter(col_idx, options, bloom.clone()))
            .collect::<ParquetResult<Vec<_>>>()?
    };

    Ok((DynIter::new(columns.into_iter().map(Ok)), bloom_hashes))
}
