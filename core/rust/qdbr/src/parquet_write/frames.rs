/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

//! Frame-at-a-time parquet writing: [`FrameAccumulator`] cuts row groups of exactly
//! `row_group_size` rows across frames, each a run of rows in contiguous column buffers.

use std::fs::File;
use std::io::Write;

use parquet2::metadata::{KeyValue, SortingColumn};

use crate::parquet::error::{fmt_err, ParquetErrorExt, ParquetResult};
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet_metadata::types::SeqTxn;
use crate::parquet_write::file::ChunkedWriter;
use crate::parquet_write::schema::Partition;

/// Frames not yet fully written, each with a payload that must live as long as the frame.
pub(crate) struct FrameAccumulator<P> {
    row_group_size: usize,
    frames: Vec<(Partition, P)>,
    // Rows of the first frame already written to an earlier row group.
    first_frame_start: usize,
    accumulated_rows: usize,
    rows_written_to_row_groups: usize,
}

impl<P> FrameAccumulator<P> {
    pub(crate) fn new(row_group_size: usize) -> Self {
        Self {
            row_group_size: row_group_size.max(1),
            frames: Vec::new(),
            first_frame_start: 0,
            accumulated_rows: 0,
            rows_written_to_row_groups: 0,
        }
    }

    pub(crate) fn has_full_row_group(&self) -> bool {
        self.accumulated_rows >= self.row_group_size
    }

    pub(crate) fn push(&mut self, frame: Partition, payload: P) {
        let row_count = frame.columns.first().map_or(0, |c| c.row_count);
        if row_count > 0 {
            self.frames.push((frame, payload));
            self.accumulated_rows += row_count;
        }
    }

    /// Rows written to row groups so far; frames ending at or below it can be released.
    pub(crate) fn rows_written_to_row_groups(&self) -> usize {
        self.rows_written_to_row_groups
    }

    pub(crate) fn write_row_group<W: Write>(
        &mut self,
        writer: &mut ChunkedWriter<W>,
    ) -> ParquetResult<()> {
        debug_assert!(self.has_full_row_group());
        let mut rows_needed = self.row_group_size;
        let mut last_frame_idx = 0;
        let mut last_frame_end = 0;
        for (idx, (frame, _)) in self.frames.iter().enumerate() {
            let frame_rows = frame.columns[0].row_count;
            let start = if idx == 0 { self.first_frame_start } else { 0 };
            let available = frame_rows - start;
            last_frame_idx = idx;
            if available >= rows_needed {
                last_frame_end = start + rows_needed;
                break;
            }
            rows_needed -= available;
            last_frame_end = frame_rows;
        }
        self.write_frames(writer, last_frame_idx, last_frame_end)?;
        self.rows_written_to_row_groups += self.row_group_size;
        self.accumulated_rows -= self.row_group_size;

        let last_frame_rows = self.frames[last_frame_idx].0.columns[0].row_count;
        if last_frame_end >= last_frame_rows {
            self.frames.drain(..=last_frame_idx);
            self.first_frame_start = 0;
        } else {
            self.frames.drain(..last_frame_idx);
            self.first_frame_start = last_frame_end;
        }
        Ok(())
    }

    pub(crate) fn write_remaining<W: Write>(
        &mut self,
        writer: &mut ChunkedWriter<W>,
    ) -> ParquetResult<()> {
        if self.frames.is_empty() || self.accumulated_rows == 0 {
            return Ok(());
        }
        let last_frame_idx = self.frames.len() - 1;
        let last_frame_end = self.frames[last_frame_idx].0.columns[0].row_count;
        self.write_frames(writer, last_frame_idx, last_frame_end)?;
        self.rows_written_to_row_groups += self.accumulated_rows;
        self.accumulated_rows = 0;
        self.frames.clear();
        self.first_frame_start = 0;
        Ok(())
    }

    fn write_frames<W: Write>(
        &self,
        writer: &mut ChunkedWriter<W>,
        last_frame_idx: usize,
        last_frame_end: usize,
    ) -> ParquetResult<()> {
        let frames: Vec<&Partition> = self.frames[..=last_frame_idx]
            .iter()
            .map(|(frame, _)| frame)
            .collect();
        writer.write_row_group_from_partitions(&frames, self.first_frame_start, last_frame_end)
    }
}

/// Writes a parquet file and its `_pm` sidecar a frame at a time. A frame's column top
/// rows are written as nulls. The QuestDB metadata records as each column's top the rows
/// from the first that hold no value, the column top a native encode of the partition
/// records: the footer decoder skips the row groups inside it, and CONVERT TO NATIVE of a
/// column converted to a type without a NULL sentinel keeps it as the native column top.
pub(crate) struct PartitionFrameWriter {
    template: Partition,
    chunked_writer: ChunkedWriter<File>,
    file: File,
    additional_meta: Vec<KeyValue>,
    // Per template column: the rows from the first that hold no value so far, and whether
    // a frame held a value.
    column_tops: Vec<(usize, bool)>,
    frames: FrameAccumulator<()>,
    sorting_columns: Option<Vec<SortingColumn>>,
    // Owned by Java, which keeps it open across the writer's lifetime.
    parquet_meta_fd: i32,
    squash_tracker: i64,
    seq_txn: SeqTxn,
    is_finished: bool,
}

impl PartitionFrameWriter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        template: Partition,
        file: File,
        chunked_writer: ChunkedWriter<File>,
        additional_meta: Vec<KeyValue>,
        row_group_size: usize,
        sorting_columns: Option<Vec<SortingColumn>>,
        parquet_meta_fd: i32,
        squash_tracker: i64,
        seq_txn: SeqTxn,
    ) -> Self {
        let column_tops = vec![(0, false); template.columns.len()];
        Self {
            template,
            chunked_writer,
            file,
            additional_meta,
            column_tops,
            frames: FrameAccumulator::new(row_group_size),
            sorting_columns,
            parquet_meta_fd,
            squash_tracker,
            seq_txn,
            is_finished: false,
        }
    }

    pub(crate) fn rows_written_to_row_groups(&self) -> usize {
        self.frames.rows_written_to_row_groups()
    }

    pub(crate) fn template(&self) -> &Partition {
        &self.template
    }

    pub(crate) fn write_frame(&mut self, frame: Partition) -> ParquetResult<usize> {
        if self.is_finished {
            return Err(fmt_err!(InvalidLayout, "parquet frame writer is finished"));
        }
        for ((top, has_value), column) in self.column_tops.iter_mut().zip(&frame.columns) {
            if !*has_value {
                *has_value = column.column_top < column.row_count;
                *top += column.column_top.min(column.row_count);
            }
        }
        self.frames.push(frame, ());
        while self.frames.has_full_row_group() {
            self.frames.write_row_group(&mut self.chunked_writer)?;
        }
        Ok(self.frames.rows_written_to_row_groups())
    }

    pub(crate) fn finish(&mut self, sync: bool) -> ParquetResult<u64> {
        if self.is_finished {
            return Err(fmt_err!(InvalidLayout, "parquet frame writer is finished"));
        }
        self.is_finished = true;
        self.frames.write_remaining(&mut self.chunked_writer)?;
        let mut additional_meta = std::mem::take(&mut self.additional_meta);
        record_column_tops(&mut additional_meta, &self.column_tops)?;
        let parquet_file_size = self
            .chunked_writer
            .finish(additional_meta)
            .context("ParquetWriter::finish failed")?;
        if sync {
            self.file
                .sync_data()
                .map_err(crate::parquet::error::ParquetError::from)
                .context("could not sync parquet file")?;
        }

        let footer_offset = self.chunked_writer.parquet_footer_offset();
        let footer_length = parquet_file_size
            .checked_sub(footer_offset)
            .and_then(|v| v.checked_sub(8))
            .ok_or_else(|| {
                fmt_err!(
                    InvalidLayout,
                    "parquet footer offset {} exceeds file size {}",
                    footer_offset,
                    parquet_file_size
                )
            })? as u32;
        let col_infos = crate::parquet_write::jni::build_column_infos_from_partition(
            &self.template,
            self.chunked_writer.schema().columns(),
            self.sorting_columns.as_deref(),
        );
        let sorting_col_indices: Vec<u32> = self
            .sorting_columns
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|sc| sc.column_idx as u32)
            .collect();
        let designated_ts = self
            .template
            .columns
            .iter()
            .position(|c| c.designated_timestamp)
            .map(|i| i as i32)
            .unwrap_or(-1);
        let (parquet_meta_bytes, _) = crate::parquet_metadata::generate_parquet_metadata(
            &col_infos,
            self.chunked_writer.row_groups(),
            designated_ts,
            &sorting_col_indices,
            footer_offset,
            footer_length,
            self.chunked_writer.bloom_bitsets(),
            0, // unused_bytes: new file, no dead space
            self.squash_tracker,
            self.seq_txn,
        )
        .context("generate_parquet_metadata failed")?;

        if self.parquet_meta_fd >= 0 {
            // ManuallyDrop: Java owns the fd and closes it.
            let mut parquet_meta_file: std::mem::ManuallyDrop<File> =
                std::mem::ManuallyDrop::new(unsafe {
                    crate::parquet::io::FromRawFdI32Ext::from_raw_fd_i32(self.parquet_meta_fd)
                });
            parquet_meta_file
                .write_all(&parquet_meta_bytes)
                .map_err(crate::parquet::error::ParquetError::from)
                .context("could not write _pm file")?;
            if sync {
                parquet_meta_file
                    .sync_data()
                    .map_err(crate::parquet::error::ParquetError::from)
                    .context("could not sync _pm file")?;
            }
        }
        Ok(parquet_file_size)
    }
}

/// Sets the column top of each column in the QuestDB metadata of `additional_meta` to the rows
/// `column_tops` counts.
fn record_column_tops(
    additional_meta: &mut [KeyValue],
    column_tops: &[(usize, bool)],
) -> ParquetResult<()> {
    if column_tops.iter().all(|&(top, _)| top == 0) {
        return Ok(());
    }
    let Some(value) = additional_meta
        .iter_mut()
        .find(|kv| kv.key == QDB_META_KEY)
        .and_then(|kv| kv.value.as_mut())
    else {
        return Err(fmt_err!(
            InvalidLayout,
            "parquet frame writer has no QuestDB metadata"
        ));
    };
    let mut qdb_meta = QdbMeta::deserialize(value)?;
    if qdb_meta.schema.len() != column_tops.len() {
        return Err(fmt_err!(
            InvalidLayout,
            "QuestDB metadata has {} columns, the frames {}",
            qdb_meta.schema.len(),
            column_tops.len()
        ));
    }
    for (column, &(top, _)) in qdb_meta.schema.iter_mut().zip(column_tops) {
        column.column_top = top;
    }
    *value = qdb_meta.serialize()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_write::file::ParquetWriter;
    use crate::parquet_write::schema::{to_compressions, to_encodings, to_parquet_schema, Column};
    use parquet2::read::read_metadata;
    use std::io::Cursor;

    const LONG_TYPE: i32 = 6;

    fn long_column(name: &'static str, values: &'static [i64], column_top: usize) -> Column {
        let bytes: &'static [u8] = unsafe {
            std::slice::from_raw_parts(values.as_ptr() as *const u8, std::mem::size_of_val(values))
        };
        Column::from_raw_data(
            0,
            name,
            LONG_TYPE,
            column_top as i64,
            values.len() + column_top,
            bytes.as_ptr(),
            bytes.len(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            false,
            true,
            0,
        )
        .unwrap()
    }

    fn frame(values: &'static [i64], column_top: usize) -> Partition {
        Partition {
            table: "t".to_string(),
            columns: vec![long_column("x", values, column_top)],
        }
    }

    fn write_frames(row_group_size: usize, frames: Vec<Partition>) -> (Vec<u8>, Vec<usize>) {
        let template = frame(&[], 0);
        let (schema, meta) = to_parquet_schema(&template, false, -1, SeqTxn::UNSET).unwrap();
        let encodings = to_encodings(&template);
        let compressions = to_compressions(&template);
        let mut buf = Vec::new();
        let mut written = Vec::new();
        {
            let mut chunked = ParquetWriter::new(&mut buf)
                .with_row_group_size(Some(row_group_size))
                .chunked_with_compressions(schema, encodings, compressions)
                .unwrap();
            let mut acc = FrameAccumulator::new(row_group_size);
            for f in frames {
                acc.push(f, ());
                while acc.has_full_row_group() {
                    acc.write_row_group(&mut chunked).unwrap();
                }
                written.push(acc.rows_written_to_row_groups());
            }
            acc.write_remaining(&mut chunked).unwrap();
            chunked.finish(meta).unwrap();
        }
        (buf, written)
    }

    fn long_column_with_id(
        id: i32,
        name: &'static str,
        values: &'static [i64],
        column_top: usize,
    ) -> Column {
        let mut column = long_column(name, values, column_top);
        column.id = id;
        column
    }

    /// Writes `frames` of the columns `a`, `b`, `c` and `d` through a [`PartitionFrameWriter`]
    /// and returns the column tops its QuestDB metadata records.
    fn frame_writer_column_tops(frames: Vec<Partition>) -> Vec<usize> {
        let template = Partition {
            table: "t".to_string(),
            columns: frames[0].columns.clone(),
        };
        let (schema, meta) = to_parquet_schema(&template, false, -1, SeqTxn::UNSET).unwrap();
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let chunked = ParquetWriter::new(tmp.reopen().unwrap())
            .with_row_group_size(Some(4))
            .chunked_with_compressions(schema, to_encodings(&template), to_compressions(&template))
            .unwrap();
        let mut writer = PartitionFrameWriter::new(
            template,
            tmp.reopen().unwrap(),
            chunked,
            meta,
            4,
            None,
            -1,
            -1,
            SeqTxn::UNSET,
        );
        for frame in frames {
            writer.write_frame(frame).unwrap();
        }
        writer.finish(false).unwrap();
        let metadata = read_metadata(&mut tmp.reopen().unwrap()).unwrap();
        let qdb_meta = metadata
            .key_value_metadata
            .as_ref()
            .and_then(|kvs| kvs.iter().find(|kv| kv.key == QDB_META_KEY))
            .and_then(|kv| kv.value.as_deref())
            .map(|json| QdbMeta::deserialize(json).unwrap())
            .unwrap();
        qdb_meta.schema.iter().map(|c| c.column_top).collect()
    }

    #[test]
    fn frame_writer_records_leading_column_tops() {
        // a: no top; b: a top inside the first frame; c: absent from the first frame, a top
        // inside the second; d: absent from every frame. A top after the first value - b in the
        // second frame - is no leading top.
        static A1: [i64; 3] = [1, 2, 3];
        static A2: [i64; 4] = [4, 5, 6, 7];
        static B1: [i64; 1] = [8];
        static B2: [i64; 2] = [9, 10];
        static C2: [i64; 3] = [11, 12, 13];
        let frames = vec![
            Partition {
                table: "t".to_string(),
                columns: vec![
                    long_column_with_id(0, "a", &A1, 0),
                    long_column_with_id(1, "b", &B1, 2),
                    long_column_with_id(2, "c", &[], 3),
                    long_column_with_id(3, "d", &[], 3),
                ],
            },
            Partition {
                table: "t".to_string(),
                columns: vec![
                    long_column_with_id(0, "a", &A2, 0),
                    long_column_with_id(1, "b", &B2, 2),
                    long_column_with_id(2, "c", &C2, 1),
                    long_column_with_id(3, "d", &[], 4),
                ],
            },
        ];
        assert_eq!(frame_writer_column_tops(frames), vec![0, 2, 4, 7]);
    }

    #[test]
    fn frame_writer_without_column_tops_records_none() {
        static A: [i64; 2] = [1, 2];
        let frames = vec![Partition {
            table: "t".to_string(),
            columns: vec![
                long_column_with_id(0, "a", &A, 0),
                long_column_with_id(1, "b", &A, 0),
            ],
        }];
        assert_eq!(frame_writer_column_tops(frames), vec![0, 0]);
    }

    #[test]
    fn record_column_tops_rejects_metadata_it_did_not_write() {
        let template = Partition {
            table: "t".to_string(),
            columns: vec![long_column_with_id(0, "a", &[], 0)],
        };
        let (_, mut meta) = to_parquet_schema(&template, false, -1, SeqTxn::UNSET).unwrap();
        let err = record_column_tops(&mut meta, &[(1, true), (2, true)]).unwrap_err();
        assert!(
            err.to_string()
                .contains("QuestDB metadata has 1 columns, the frames 2"),
            "{err}"
        );
        let err = record_column_tops(&mut [], &[(1, true)]).unwrap_err();
        assert!(
            err.to_string()
                .contains("parquet frame writer has no QuestDB metadata"),
            "{err}"
        );
    }

    #[test]
    fn row_groups_span_frames_and_frames_span_row_groups() {
        static A: [i64; 3] = [1, 2, 3];
        static B: [i64; 1] = [4];
        static C: [i64; 6] = [5, 6, 7, 8, 9, 10];
        let (buf, written) = write_frames(4, vec![frame(&A, 0), frame(&B, 0), frame(&C, 0)]);
        assert_eq!(written, vec![0, 4, 8]);
        let metadata = read_metadata(&mut Cursor::new(buf)).unwrap();
        let sizes: Vec<usize> = metadata.row_groups.iter().map(|rg| rg.num_rows()).collect();
        assert_eq!(sizes, vec![4, 4, 2]);
    }

    #[test]
    fn frame_column_tops_count_as_rows() {
        static A: [i64; 2] = [1, 2];
        static B: [i64; 1] = [3];
        // The second frame's column top contributes two null rows.
        let (buf, _) = write_frames(3, vec![frame(&A, 0), frame(&B, 2)]);
        let metadata = read_metadata(&mut Cursor::new(buf)).unwrap();
        let sizes: Vec<usize> = metadata.row_groups.iter().map(|rg| rg.num_rows()).collect();
        assert_eq!(sizes, vec![3, 2]);
    }
}
