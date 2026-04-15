use std::cmp;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::transmute_slice;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRowWindow {
    pub row_offset: usize,
    pub row_count: usize,
}

pub fn page_row_windows(
    num_rows: usize,
    rows_per_page: usize,
) -> impl Iterator<Item = PageRowWindow> {
    (0..num_rows)
        .step_by(rows_per_page)
        .map(move |row_offset| PageRowWindow {
            row_offset,
            row_count: cmp::min(rows_per_page, num_rows - row_offset),
        })
}

/// Per-partition slice into a column chunk: byte range plus the number of
/// "column top" rows that fall inside the chunk (rows that have no backing
/// storage and read as null).
#[derive(Clone, Copy, Debug)]
pub struct ChunkSlice {
    pub lower_bound: usize,
    pub upper_bound: usize,
    pub adjusted_column_top: usize,
}

#[cfg(test)]
impl ChunkSlice {
    #[inline]
    pub fn len(&self) -> usize {
        self.upper_bound - self.lower_bound
    }

    /// Total number of rows this slice represents (data rows + adjusted top).
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.len()
    }
}

/// Compute the byte range and adjusted column top for a chunk that begins at
/// `chunk_offset` (in column rows) and is `chunk_length` rows long, given the
/// column's original column top.
///
/// `column_top` is the number of leading rows that have no backing data.
/// The returned `lower_bound`/`upper_bound` are indices into the data buffer
/// (after subtracting the column top).
#[inline]
pub fn compute_chunk_slice(
    column_top: usize,
    chunk_offset: usize,
    chunk_length: usize,
) -> ChunkSlice {
    let mut adjusted_column_top = 0;
    let lower_bound = if chunk_offset < column_top {
        adjusted_column_top = column_top - chunk_offset;
        0
    } else {
        chunk_offset - column_top
    };
    let upper_bound = if chunk_offset + chunk_length < column_top {
        adjusted_column_top = chunk_length;
        0
    } else {
        chunk_offset + chunk_length - column_top
    };
    ChunkSlice { lower_bound, upper_bound, adjusted_column_top }
}

/// Compute `(chunk_offset, chunk_length)` for the i-th partition in a
/// multi-partition row group write.
///
/// Single-partition (`num_partitions == 1`) uses the absolute bounds.
/// First partition starts at `first_partition_start` and runs to its end.
/// Last partition starts at 0 and ends at `last_partition_end`.
/// Middle partitions are taken in full.
#[inline]
pub fn partition_slice_range(
    part_idx: usize,
    num_partitions: usize,
    row_count: usize,
    first_partition_start: usize,
    last_partition_end: usize,
) -> (usize, usize) {
    if num_partitions == 1 {
        (
            first_partition_start,
            last_partition_end.saturating_sub(first_partition_start),
        )
    } else if part_idx == 0 {
        (
            first_partition_start,
            row_count.saturating_sub(first_partition_start),
        )
    } else if part_idx == num_partitions - 1 {
        (0, last_partition_end)
    } else {
        (0, row_count)
    }
}

/// Compute the per-partition `ChunkSlice` for the i-th column in a row group.
#[inline]
pub fn partition_chunk_slice(
    part_idx: usize,
    num_partitions: usize,
    column: &Column,
    first_partition_start: usize,
    last_partition_end: usize,
) -> ChunkSlice {
    let (chunk_offset, chunk_length) = partition_slice_range(
        part_idx,
        num_partitions,
        column.row_count,
        first_partition_start,
        last_partition_end,
    );
    compute_chunk_slice(column.column_top, chunk_offset, chunk_length)
}

/// Total number of logical rows covered by the selected column chunk across
/// all input partitions.
#[inline]
pub fn column_chunk_row_count(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> usize {
    let num_partitions = columns.len();
    columns
        .iter()
        .enumerate()
        .map(|(part_idx, column)| {
            partition_slice_range(
                part_idx,
                num_partitions,
                column.row_count,
                first_partition_start,
                last_partition_end,
            )
            .1
        })
        .sum()
}

/// Compute the logical chunk slice for each input partition.
pub fn column_chunk_slices<'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
) -> impl Iterator<Item = ChunkSlice> + 'a {
    let num_partitions = columns.len();
    columns.iter().enumerate().map(move |(part_idx, column)| {
        partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        )
    })
}

/// Borrowed typed view of one partition's contribution to a logical chunk.
#[derive(Clone, Copy, Debug)]
pub struct PartitionChunkView<'a, T> {
    pub adjusted_column_top: usize,
    pub slice: &'a [T],
}

impl<T> PartitionChunkView<'_, T> {
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.slice.len()
    }
}

/// Lazily yield typed partition chunk views for a single page window. Combines
/// chunk-slice computation, byte transmutation, and page-window slicing into one
/// iterator — no intermediate `Vec` needed.
///
/// SAFETY: Each column's `primary_data` must contain valid `T` values with correct
/// alignment (guaranteed for JNI/Java memory-mapped column data which is page-aligned).
pub unsafe fn page_chunk_views<'a, T: 'a>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
    window: PageRowWindow,
) -> impl Iterator<Item = PartitionChunkView<'a, T>> {
    let mut remaining_offset = window.row_offset;
    let mut remaining_rows = window.row_count;

    columns
        .iter()
        .zip(column_chunk_slices(
            columns,
            first_partition_start,
            last_partition_end,
        ))
        .filter_map(move |(column, chunk)| {
            if remaining_rows == 0 {
                return None;
            }

            let typed: &[T] = transmute_slice(column.primary_data);
            let view = PartitionChunkView {
                adjusted_column_top: chunk.adjusted_column_top,
                slice: &typed[chunk.lower_bound..chunk.upper_bound],
            };
            let view_rows = view.num_rows();

            if remaining_offset >= view_rows {
                remaining_offset -= view_rows;
                return None;
            }

            let rows_in_view = cmp::min(view_rows - remaining_offset, remaining_rows);
            let skip_data_rows = remaining_offset.saturating_sub(view.adjusted_column_top);
            let available_top_rows = view.adjusted_column_top.saturating_sub(remaining_offset);
            let top_rows = cmp::min(available_top_rows, rows_in_view);
            let data_rows = rows_in_view - top_rows;

            remaining_rows -= rows_in_view;
            remaining_offset = 0;

            Some(PartitionChunkView {
                adjusted_column_top: top_rows,
                slice: &view.slice[skip_data_rows..skip_data_rows + data_rows],
            })
        })
}

/// Borrowed view of one partition's contribution to a variable-length column
/// chunk. `I` is the per-row index type: `i64` for binary/string offsets,
/// `[u8; 16]` for varchar aux entries.
#[derive(Clone, Copy, Debug)]
pub struct VarlenChunkSegment<'a, I> {
    pub adjusted_column_top: usize,
    pub index: &'a [I],
    pub data: &'a [u8],
}

impl<I> VarlenChunkSegment<'_, I> {
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.adjusted_column_top + self.index.len()
    }
}

/// Collect per-partition varlen segment views for the selected column chunk.
pub fn collect_varlen_segments<'a, I>(
    columns: &'a [Column],
    first_partition_start: usize,
    last_partition_end: usize,
    transmuter: impl Fn(&'a Column) -> ParquetResult<&'a [I]>,
    data_source: impl Fn(&'a Column) -> &'a [u8],
) -> ParquetResult<Vec<VarlenChunkSegment<'a, I>>> {
    let mut segments = Vec::with_capacity(columns.len());

    for (column, chunk) in columns.iter().zip(column_chunk_slices(
        columns,
        first_partition_start,
        last_partition_end,
    )) {
        let index_data = transmuter(column)?;
        segments.push(VarlenChunkSegment {
            adjusted_column_top: chunk.adjusted_column_top,
            index: &index_data[chunk.lower_bound..chunk.upper_bound],
            data: data_source(column),
        });
    }

    Ok(segments)
}

/// Slice varlen segments to a page window, analogous to
/// `slice_partition_chunk_views` but for `VarlenChunkSegment`.
pub fn slice_varlen_segments<'a, I>(
    segments: &[VarlenChunkSegment<'a, I>],
    window: PageRowWindow,
) -> Vec<VarlenChunkSegment<'a, I>> {
    let mut remaining_offset = window.row_offset;
    let mut remaining_rows = window.row_count;
    let mut page_segments = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_rows = segment.num_rows();
        if remaining_offset >= segment_rows {
            remaining_offset -= segment_rows;
            continue;
        }
        if remaining_rows == 0 {
            break;
        }

        let rows_in_segment = cmp::min(segment_rows - remaining_offset, remaining_rows);
        let skip_data_rows = remaining_offset.saturating_sub(segment.adjusted_column_top);
        let available_top_rows = segment.adjusted_column_top.saturating_sub(remaining_offset);
        let top_rows = cmp::min(available_top_rows, rows_in_segment);
        let data_rows = rows_in_segment - top_rows;

        page_segments.push(VarlenChunkSegment {
            adjusted_column_top: top_rows,
            index: &segment.index[skip_data_rows..skip_data_rows + data_rows],
            data: segment.data,
        });

        remaining_rows -= rows_in_segment;
        remaining_offset = 0;
    }

    page_segments
}

/// Iterator over sub-chunks of a partition that respects `rows_per_page`.
/// Each yielded `ChunkSlice` covers at most `rows_per_page` rows.
#[derive(Clone)]
pub struct PartitionPageSlices {
    column_top: usize,
    chunk_offset: usize,
    chunk_length: usize,
    rows_per_page: usize,
    sub_offset: usize,
}

impl PartitionPageSlices {
    pub fn new(
        column: &Column,
        chunk_offset: usize,
        chunk_length: usize,
        rows_per_page: usize,
    ) -> Self {
        Self::from_parts(column.column_top, chunk_offset, chunk_length, rows_per_page)
    }

    /// Construct directly from a `column_top` value, without holding a `Column`.
    /// Mostly used by tests; production callers prefer `new(column, ...)`.
    pub fn from_parts(
        column_top: usize,
        chunk_offset: usize,
        chunk_length: usize,
        rows_per_page: usize,
    ) -> Self {
        Self {
            column_top,
            chunk_offset,
            chunk_length,
            rows_per_page,
            sub_offset: 0,
        }
    }
}

impl Iterator for PartitionPageSlices {
    type Item = ChunkSlice;

    fn next(&mut self) -> Option<Self::Item> {
        if self.sub_offset >= self.chunk_length {
            return None;
        }
        let sub_length = cmp::min(self.rows_per_page, self.chunk_length - self.sub_offset);
        let slice = compute_chunk_slice(
            self.column_top,
            self.chunk_offset + self.sub_offset,
            sub_length,
        );
        self.sub_offset += sub_length;
        Some(slice)
    }
}
