/*+*****************************************************************************
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

//! The column-top rows of a column of a type without a NULL sentinel (BOOLEAN, BYTE, SHORT,
//! CHAR). CONVERT TO PARQUET of a split or composite partition writes a row group from the
//! folders, or pieces, of the partition one after another, each starting with its own column
//! top, so the definition-level-0 rows of a chunk need not lead it. The files here are written
//! that way: a frame per folder, through the encoder the conversion uses. The `_pm` decode, which
//! the readers of a table use, must report those rows exactly, and each conversion to a type with
//! a NULL must read NULL there and the stored value, 0 included, everywhere else. The footer
//! decoder, which serves CONVERT TO NATIVE, takes the leading column top of the file metadata.

use crate::allocator::TestAllocatorState;
use crate::parquet::error::ParquetResult;
use crate::parquet::qdb_metadata::{QdbMeta, QDB_META_KEY};
use crate::parquet::tests::ColumnTypeTagExt;
use crate::parquet_metadata::convert::{convert_from_parquet, NoBloomFilterSource};
use crate::parquet_metadata::types::SeqTxn;
use crate::parquet_metadata::ParquetMetaReader;
use crate::parquet_read::parquet_meta_decode::{
    decode_row_group, decode_row_group_filtered, decode_row_group_range, ColumnChunkSource,
};
use crate::parquet_read::row_groups::post_convert;
use crate::parquet_read::{ColumnChunkBuffers, DecodeContext, ParquetDecoder, RowGroupBuffers};
use crate::parquet_write::file::ParquetWriter;
use crate::parquet_write::frames::FrameAccumulator;
use crate::parquet_write::schema::{
    to_compressions, to_encodings, to_parquet_schema, Column, ParquetEncodingConfig, Partition,
};
use parquet2::compression::CompressionOptions;
use parquet2::read::read_metadata_with_size;
use parquet2::schema::types::ParquetType;
use parquet2::schema::Repetition;
use parquet2::write::Version;
use qdb_core::col_type::{nulls, ColumnType, ColumnTypeTag};
use std::io::Cursor;

const ENCODING_PLAIN: i32 = 1;
const ENCODING_RLE_DICTIONARY: i32 = 2;
const ENCODING_DELTA_BINARY_PACKED: i32 = 4;

/// The rows of one folder: `top` column-top rows, then `values`.
#[derive(Clone, Copy)]
struct Folder {
    top: usize,
    values: &'static [i64],
}

const fn folder(top: usize, values: &'static [i64]) -> Folder {
    Folder { top, values }
}

/// The 9 rows `5 0 7 | T T 9 | T 0 1` of three folders, in one row group of 9 rows: stored
/// values, a stored 0 among them, ahead of the top rows of the later folders.
const FOLDERS: [Folder; 3] = [folder(0, &[5, 0, 7]), folder(2, &[9]), folder(1, &[0, 1])];
const FOLDERS_TOP_ROWS: [[u64; 2]; 2] = [[3, 5], [6, 7]];

/// A parquet file of a designated timestamp `ts` (column 0) and a column `v` (column 1) of type
/// `tag`, with its `_pm`.
struct TestFile {
    parquet: Vec<u8>,
    parquet_meta: Vec<u8>,
    parquet_meta_size: u64,
}

impl TestFile {
    fn parquet_meta_reader(&self) -> ParquetMetaReader<'_> {
        ParquetMetaReader::from_file_size(&self.parquet_meta, self.parquet_meta_size).unwrap()
    }

    fn footer_decoder(&self) -> ParquetDecoder {
        let tas = TestAllocatorState::new();
        ParquetDecoder::read(
            tas.allocator(),
            &mut Cursor::new(&self.parquet),
            self.parquet.len() as u64,
        )
        .unwrap()
    }

    fn decode_context(&self) -> DecodeContext {
        DecodeContext::new(self.parquet.as_ptr(), self.parquet.len() as u64)
    }
}

fn leak<T: Copy + 'static>(values: Vec<T>) -> &'static [u8] {
    let values: &'static [T] = Box::leak(values.into_boxed_slice());
    // SAFETY: reinterprets the leaked values as their bytes, which live for the program.
    unsafe {
        std::slice::from_raw_parts(values.as_ptr() as *const u8, std::mem::size_of_val(values))
    }
}

fn native_bytes(tag: ColumnTypeTag, values: &[i64]) -> &'static [u8] {
    match tag {
        ColumnTypeTag::Boolean => leak(values.iter().map(|&v| (v != 0) as u8).collect()),
        ColumnTypeTag::Byte => leak(values.iter().map(|&v| v as i8).collect()),
        ColumnTypeTag::Short => leak(values.iter().map(|&v| v as i16).collect()),
        ColumnTypeTag::Char => leak(values.iter().map(|&v| v as u16).collect()),
        ColumnTypeTag::Int => leak(values.iter().map(|&v| v as i32).collect()),
        _ => panic!("unsupported test column type {tag:?}"),
    }
}

#[allow(clippy::too_many_arguments)]
fn column(
    id: i32,
    name: &'static str,
    column_type: ColumnType,
    column_top: usize,
    row_count: usize,
    data: &'static [u8],
    designated_timestamp: bool,
    encoding: i32,
) -> Column {
    let encoding_config = if encoding == 0 {
        0
    } else {
        ParquetEncodingConfig::new(encoding, 0, -1).raw()
    };
    Column::from_raw_data(
        id,
        name,
        column_type.code(),
        column_top as i64,
        row_count,
        data.as_ptr(),
        data.len(),
        std::ptr::null(),
        0,
        std::ptr::null(),
        0,
        designated_timestamp,
        true,
        encoding_config,
    )
    .unwrap()
}

/// Writes `folders` a frame each, as CONVERT TO PARQUET of a split partition does, in row groups
/// of `row_group_size` rows and pages of `data_page_size` bytes. `v` of type `tag` takes encoding
/// `encoding`, or the default when 0.
fn write_folders(
    tag: ColumnTypeTag,
    encoding: i32,
    folders: &[Folder],
    row_group_size: usize,
    data_page_size: Option<usize>,
) -> TestFile {
    write_folders_with_version(
        tag,
        encoding,
        folders,
        row_group_size,
        data_page_size,
        Version::V2,
    )
}

/// [`write_folders`] with data pages of `version`: a V1 page holds its definition levels in
/// its values buffer and counts no rows in its header.
fn write_folders_with_version(
    tag: ColumnTypeTag,
    encoding: i32,
    folders: &[Folder],
    row_group_size: usize,
    data_page_size: Option<usize>,
    version: Version,
) -> TestFile {
    let mut first_ts = 0i64;
    let frames: Vec<Partition> = folders
        .iter()
        .map(|f| {
            let rows = f.top + f.values.len();
            let ts: Vec<i64> = (first_ts..first_ts + rows as i64).collect();
            first_ts += rows as i64;
            let ts_col = column(
                0,
                "ts",
                ColumnTypeTag::Timestamp.into_type(),
                0,
                rows,
                leak(ts),
                true,
                0,
            );
            let v_col = column(
                1,
                "v",
                tag.into_type(),
                f.top,
                rows,
                native_bytes(tag, f.values),
                false,
                encoding,
            );
            Partition {
                table: "t".to_string(),
                columns: vec![ts_col, v_col],
            }
        })
        .collect();
    // As the conversion before it recorded the top of the file: 0 for every column.
    let mut template = Partition {
        table: "t".to_string(),
        columns: frames[0].columns.clone(),
    };
    template.columns[1].column_top = 0;
    let (schema, meta) = to_parquet_schema(&template, false, -1, SeqTxn::UNSET).unwrap();
    let mut parquet = Vec::new();
    {
        let mut writer = ParquetWriter::new(&mut parquet)
            .with_version(version)
            .with_statistics(true)
            .with_compression(CompressionOptions::Uncompressed)
            .with_row_group_size(Some(row_group_size))
            .with_data_page_size(data_page_size)
            .chunked_with_compressions(schema, to_encodings(&template), to_compressions(&template))
            .unwrap();
        let mut frame_accumulator = FrameAccumulator::new(row_group_size);
        for frame in frames {
            frame_accumulator.push(frame, ());
            while frame_accumulator.has_full_row_group() {
                frame_accumulator.write_row_group(&mut writer).unwrap();
            }
        }
        frame_accumulator.write_remaining(&mut writer).unwrap();
        writer.finish(meta).unwrap();
    }
    with_parquet_meta(parquet)
}

/// Writes one folder of `top` column-top rows, then `values`, with `v` Required, as QuestDB
/// 9.4.x and older wrote a column of a type without a NULL sentinel: no definition levels, the
/// top rows stored as 0, and the top in the QuestDB metadata and the chunk null counts.
fn write_legacy_required(
    tag: ColumnTypeTag,
    top: usize,
    values: &'static [i64],
    row_group_size: usize,
) -> TestFile {
    let rows = top + values.len();
    let ts_col = column(
        0,
        "ts",
        ColumnTypeTag::Timestamp.into_type(),
        0,
        rows,
        leak((0..rows as i64).collect()),
        true,
        0,
    );
    let v_col = column(
        1,
        "v",
        tag.into_type(),
        top,
        rows,
        native_bytes(tag, values),
        false,
        0,
    );
    let partition = Partition {
        table: "t".to_string(),
        columns: vec![ts_col, v_col],
    };
    let (schema, meta) = to_parquet_schema(&partition, false, -1, SeqTxn::UNSET).unwrap();
    let mut fields = schema.fields().to_vec();
    if let ParquetType::PrimitiveType(ref mut pt) = fields[1] {
        pt.field_info.repetition = Repetition::Required;
    }
    let schema = parquet2::metadata::SchemaDescriptor::new(schema.name().to_string(), fields);
    let mut parquet = Vec::new();
    {
        let mut writer = ParquetWriter::new(&mut parquet)
            .with_statistics(true)
            .with_row_group_size(Some(row_group_size))
            .chunked(schema, to_encodings(&partition))
            .unwrap();
        writer.write_chunk(&partition).unwrap();
        writer.finish(meta).unwrap();
    }
    let file = with_parquet_meta(parquet);
    let decoder = file.footer_decoder();
    assert_eq!(
        decoder.metadata.schema_descr.columns()[1]
            .descriptor
            .max_def_level,
        0,
        "the legacy file must store v without definition levels"
    );
    file
}

fn with_parquet_meta(parquet: Vec<u8>) -> TestFile {
    let metadata =
        read_metadata_with_size(&mut Cursor::new(&parquet), parquet.len() as u64).unwrap();
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| kvs.iter().find(|kv| kv.key == QDB_META_KEY))
        .and_then(|kv| kv.value.as_deref())
        .map(|json| QdbMeta::deserialize(json).unwrap());
    let (parquet_meta, parquet_meta_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        0,
        0,
        &NoBloomFilterSource,
        None,
    )
    .unwrap();
    TestFile { parquet, parquet_meta, parquet_meta_size }
}

/// The rows of `folders`: `None` for a column-top row.
fn model(folders: &[Folder]) -> Vec<Option<i64>> {
    folders
        .iter()
        .flat_map(|f| std::iter::repeat_n(None, f.top).chain(f.values.iter().map(|&v| Some(v))))
        .collect()
}

/// The `[lo, hi)` runs of the `None` rows of `rows`.
fn top_rows_of(rows: &[Option<i64>]) -> Vec<[u64; 2]> {
    let mut runs: Vec<[u64; 2]> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        if row.is_none() {
            match runs.last_mut() {
                Some(run) if run[1] == i as u64 => run[1] += 1,
                _ => runs.push([i as u64, i as u64 + 1]),
            }
        }
    }
    runs
}

/// The value of row `i` of a decoded buffer of type `tag`, as i64 (a float as its integral value),
/// or `None` for the NULL of the type. A type without a NULL sentinel never reads `None`.
fn read_row(bufs: &ColumnChunkBuffers, column_type: ColumnType, i: usize) -> Option<i64> {
    let data = bufs.data_vec.as_slice();
    let at = |size: usize| &data[i * size..(i + 1) * size];
    match column_type.tag() {
        ColumnTypeTag::Boolean | ColumnTypeTag::Byte => Some(at(1)[0] as i8 as i64),
        ColumnTypeTag::Short => Some(i16::from_le_bytes(at(2).try_into().unwrap()) as i64),
        ColumnTypeTag::Char => Some(u16::from_le_bytes(at(2).try_into().unwrap()) as i64),
        ColumnTypeTag::Int => {
            let v = i32::from_le_bytes(at(4).try_into().unwrap());
            (v != nulls::INT).then_some(v as i64)
        }
        ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp => {
            let v = i64::from_le_bytes(at(8).try_into().unwrap());
            (v != nulls::LONG).then_some(v)
        }
        ColumnTypeTag::Float => {
            let v = f32::from_le_bytes(at(4).try_into().unwrap());
            (!v.is_nan()).then_some(v as i64)
        }
        ColumnTypeTag::Double => {
            let v = f64::from_le_bytes(at(8).try_into().unwrap());
            (!v.is_nan()).then_some(v as i64)
        }
        ColumnTypeTag::Decimal8 => {
            let v = at(1)[0] as i8;
            (v != nulls::DECIMAL8).then_some(v as i64)
        }
        ColumnTypeTag::Decimal16 => {
            let v = i16::from_le_bytes(at(2).try_into().unwrap());
            (v != nulls::DECIMAL16).then_some(v as i64)
        }
        ColumnTypeTag::Decimal32 => {
            let v = i32::from_le_bytes(at(4).try_into().unwrap());
            (v != nulls::DECIMAL32).then_some(v as i64)
        }
        ColumnTypeTag::Decimal64 => {
            let v = i64::from_le_bytes(at(8).try_into().unwrap());
            (v != nulls::DECIMAL64).then_some(v)
        }
        ColumnTypeTag::Decimal128 => {
            let hi = i64::from_le_bytes(at(16)[..8].try_into().unwrap());
            let lo = u64::from_le_bytes(at(16)[8..].try_into().unwrap());
            if hi == i64::MIN && lo == 0 {
                None
            } else {
                Some((((hi as i128) << 64) | lo as i128) as i64)
            }
        }
        ColumnTypeTag::Decimal256 => {
            let words: Vec<u64> = at(32)
                .chunks_exact(8)
                .map(|w| u64::from_le_bytes(w.try_into().unwrap()))
                .collect();
            if words == [i64::MIN as u64, 0, 0, 0] {
                None
            } else {
                // the test values fit the lowest word
                Some(words[3] as i64)
            }
        }
        tag => panic!("unsupported decoded type {tag:?}"),
    }
}

/// The value a stored value `v` of `source` reads as in `target`, as `read_row` reports it:
/// a decimal scaled by its scale, a boolean source as 0 or 1, a boolean target as 0 or 1.
fn converted(source: ColumnTypeTag, target: ColumnType, v: i64) -> i64 {
    let v = if source == ColumnTypeTag::Boolean {
        (v != 0) as i64
    } else {
        v
    };
    match target.tag() {
        ColumnTypeTag::Boolean => (v != 0) as i64,
        ColumnTypeTag::Decimal8
        | ColumnTypeTag::Decimal16
        | ColumnTypeTag::Decimal32
        | ColumnTypeTag::Decimal64
        | ColumnTypeTag::Decimal128
        | ColumnTypeTag::Decimal256 => v * 10i64.pow(target.decimal_scale() as u32),
        _ => v,
    }
}

/// Whether `target` reads a column-top row as its NULL rather than as 0.
fn has_null(target: ColumnType) -> bool {
    !matches!(
        target.tag(),
        ColumnTypeTag::Boolean | ColumnTypeTag::Byte | ColumnTypeTag::Short | ColumnTypeTag::Char
    )
}

/// Asserts that `bufs` holds `rows` converted from `source` to `target`: NULL in the top rows of a
/// target with a NULL, 0 in them for one without, and the stored values elsewhere.
fn assert_rows(
    context: &str,
    bufs: &ColumnChunkBuffers,
    source: ColumnTypeTag,
    target: ColumnType,
    rows: &[Option<i64>],
) {
    let width = bufs.data_vec.len() / rows.len().max(1);
    assert_eq!(
        width * rows.len(),
        bufs.data_vec.len(),
        "{context}: buffer size"
    );
    for (i, row) in rows.iter().enumerate() {
        let expected = match row {
            None if has_null(target) => None,
            None => Some(0),
            Some(v) => Some(converted(source, target, *v)),
        };
        assert_eq!(
            read_row(bufs, target, i),
            expected,
            "{context}: row {i} of {rows:?}"
        );
    }
}

fn no_sentinel_sources() -> [(ColumnTypeTag, &'static [i32]); 4] {
    [
        (
            ColumnTypeTag::Byte,
            &[
                0,
                ENCODING_PLAIN,
                ENCODING_RLE_DICTIONARY,
                ENCODING_DELTA_BINARY_PACKED,
            ],
        ),
        (
            ColumnTypeTag::Short,
            &[
                0,
                ENCODING_PLAIN,
                ENCODING_RLE_DICTIONARY,
                ENCODING_DELTA_BINARY_PACKED,
            ],
        ),
        (
            ColumnTypeTag::Char,
            &[
                0,
                ENCODING_PLAIN,
                ENCODING_RLE_DICTIONARY,
                ENCODING_DELTA_BINARY_PACKED,
            ],
        ),
        (ColumnTypeTag::Boolean, &[0]),
    ]
}

fn decimal(precision: u8, scale: u8) -> ColumnType {
    ColumnType::new_decimal(precision, scale).unwrap()
}

/// The conversions of `source` that decode at its own width, so that only the column-top rows
/// tell its top from a stored 0: the type itself, and the targets post_convert converts to.
fn targets_of(source: ColumnTypeTag) -> Vec<ColumnType> {
    match source {
        ColumnTypeTag::Boolean => vec![
            ColumnTypeTag::Boolean.into_type(),
            ColumnTypeTag::Byte.into_type(),
            ColumnTypeTag::Short.into_type(),
            ColumnTypeTag::Int.into_type(),
            ColumnTypeTag::Long.into_type(),
            ColumnTypeTag::Date.into_type(),
            ColumnTypeTag::Timestamp.into_type(),
            ColumnTypeTag::Float.into_type(),
            ColumnTypeTag::Double.into_type(),
        ],
        ColumnTypeTag::Byte | ColumnTypeTag::Short => vec![
            source.into_type(),
            ColumnTypeTag::Boolean.into_type(),
            if source == ColumnTypeTag::Byte {
                ColumnTypeTag::Short.into_type()
            } else {
                ColumnTypeTag::Byte.into_type()
            },
            ColumnTypeTag::Date.into_type(),
            ColumnTypeTag::Float.into_type(),
            ColumnTypeTag::Double.into_type(),
            decimal(2, 0),
            decimal(4, 1),
            decimal(9, 2),
            decimal(18, 3),
            decimal(38, 4),
            decimal(76, 5),
        ],
        ColumnTypeTag::Char => vec![ColumnTypeTag::Char.into_type()],
        tag => panic!("unsupported source {tag:?}"),
    }
}

#[test]
fn pm_decode_reads_column_tops_of_later_folders() -> ParquetResult<()> {
    let rows = model(&FOLDERS);
    for (source, encodings) in no_sentinel_sources() {
        for (&encoding, version) in encodings
            .iter()
            .flat_map(|e| [(e, Version::V1), (e, Version::V2)])
        {
            let file = write_folders_with_version(source, encoding, &FOLDERS, 9, None, version);
            let reader = file.parquet_meta_reader();
            assert_eq!(reader.row_group_count(), 1);
            for target in targets_of(source) {
                let context = format!("{source:?} -> {target:?}, encoding {encoding}, {version:?}");
                let tas = TestAllocatorState::new();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                let mut ctx = file.decode_context();
                let decoded = decode_row_group(
                    &mut ctx,
                    &mut bufs,
                    ColumnChunkSource::File(&file.parquet),
                    &reader,
                    &[(1, target)],
                    0,
                    0,
                    rows.len(),
                )?;
                assert_eq!(decoded, rows.len(), "{context}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &FOLDERS_TOP_ROWS, "{context}");
                assert_rows(&context, chunk, source, target, &rows);
            }
        }
    }
    Ok(())
}

/// The rows the footer decoder reads for `rows` of `source` decoded as `target`, where the file
/// metadata records no column top: the decoder takes no column-top rows from the definition levels
/// (see `ParquetDecoder::decode_row_group`), so a definition-level-0 row reads as the page decode
/// writes it - the NULL of a type a BYTE or SHORT decodes straight to, see
/// decode_to_a_type_with_a_null_reads_definition_levels_itself(), and 0 or false otherwise.
fn footer_rows(
    source: ColumnTypeTag,
    target: ColumnType,
    rows: &[Option<i64>],
) -> Vec<Option<i64>> {
    let is_decoded_as_target = matches!(source, ColumnTypeTag::Byte | ColumnTypeTag::Short)
        && matches!(
            target.tag(),
            ColumnTypeTag::Int | ColumnTypeTag::Long | ColumnTypeTag::Timestamp
        );
    rows.iter()
        .map(|&row| row.or(if is_decoded_as_target { None } else { Some(0) }))
        .collect()
}

#[test]
fn footer_decode_takes_leading_column_top_of_the_file_metadata_only() -> ParquetResult<()> {
    // The footer decoder serves CONVERT TO NATIVE, which keeps only the leading column top: a
    // native column holds no later one. It takes that top from the QuestDB metadata, where the
    // PartitionUpdater ends a top as a native O3 merge does, over rows whose NULLs it can leave in
    // the row groups it keeps. FOLDERS records no top: the later tops read as stored rows. With a
    // leading top of 2 recorded, as the frame writer records it, the rows of that top read NULL.
    let rows = model(&FOLDERS);
    const LEADING: [Folder; 2] = [folder(2, &[5, 0]), folder(1, &[7])];
    let leading_rows = model(&LEADING);
    for (source, encodings) in no_sentinel_sources() {
        for (&encoding, version) in encodings
            .iter()
            .flat_map(|e| [(e, Version::V1), (e, Version::V2)])
        {
            let file = write_folders_with_version(source, encoding, &FOLDERS, 9, None, version);
            let decoder = file.footer_decoder();
            let leading_file =
                write_folders_with_version(source, encoding, &LEADING, 6, None, version);
            let mut leading_decoder = leading_file.footer_decoder();
            leading_decoder.qdb_meta.as_mut().unwrap().schema[1].column_top = 2;
            for target in targets_of(source) {
                let context = format!("{source:?} -> {target:?}, encoding {encoding}, {version:?}");
                let tas = TestAllocatorState::new();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                let mut ctx = file.decode_context();
                let decoded = decoder.decode_row_group(
                    &mut ctx,
                    &mut bufs,
                    &[(1, target)],
                    0,
                    0,
                    rows.len() as u32,
                )?;
                assert_eq!(decoded, rows.len(), "{context}");
                let chunk = &bufs.column_bufs[0];
                assert!(chunk.top_rows().is_empty(), "{context}");
                assert_rows(
                    &context,
                    chunk,
                    source,
                    target,
                    &footer_rows(source, target, &rows),
                );

                let mut ctx = leading_file.decode_context();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                leading_decoder.decode_row_group(
                    &mut ctx,
                    &mut bufs,
                    &[(1, target)],
                    0,
                    0,
                    leading_rows.len() as u32,
                )?;
                let chunk = &bufs.column_bufs[0];
                let context = format!("{context}, leading top");
                assert_eq!(chunk.top_rows(), &[[0, 2]], "{context}");
                let mut expected = footer_rows(source, target, &leading_rows);
                expected[0] = None;
                expected[1] = None;
                assert_rows(&context, chunk, source, target, &expected);
            }
        }
    }
    Ok(())
}

#[test]
fn decode_to_a_type_with_a_null_reads_definition_levels_itself() -> ParquetResult<()> {
    // Byte/Short -> Int/Long/Timestamp decode straight to the target, which writes its NULL for
    // each definition-level-0 row: the buffer needs no column-top rows.
    let rows = model(&FOLDERS);
    for source in [ColumnTypeTag::Byte, ColumnTypeTag::Short] {
        let file = write_folders(source, 0, &FOLDERS, 9, None);
        let reader = file.parquet_meta_reader();
        let decoder = file.footer_decoder();
        for target in [
            ColumnTypeTag::Int.into_type(),
            ColumnTypeTag::Long.into_type(),
            ColumnTypeTag::Timestamp.into_type(),
        ] {
            let tas = TestAllocatorState::new();
            let mut pm_bufs = RowGroupBuffers::new(tas.allocator());
            let mut ctx = file.decode_context();
            decode_row_group(
                &mut ctx,
                &mut pm_bufs,
                ColumnChunkSource::File(&file.parquet),
                &reader,
                &[(1, target)],
                0,
                0,
                rows.len(),
            )?;
            let mut footer_bufs = RowGroupBuffers::new(tas.allocator());
            decoder.decode_row_group(
                &mut ctx,
                &mut footer_bufs,
                &[(1, target)],
                0,
                0,
                rows.len() as u32,
            )?;
            for (name, bufs) in [("_pm", &pm_bufs), ("footer", &footer_bufs)] {
                let context = format!("{name} {source:?} -> {target:?}");
                let chunk = &bufs.column_bufs[0];
                assert!(chunk.top_rows().is_empty(), "{context}");
                assert_rows(&context, chunk, source, target, &rows);
            }
        }
    }
    Ok(())
}

#[test]
fn decode_window_shifts_column_top_rows() -> ParquetResult<()> {
    // rows [2, 8) of the row group: 7 | T T 9 | T 0
    let rows = model(&FOLDERS);
    for (source, _) in no_sentinel_sources() {
        let file = write_folders(source, 0, &FOLDERS, 9, None);
        let reader = file.parquet_meta_reader();
        let tas = TestAllocatorState::new();
        let to_type = source.into_type();
        let mut bufs = RowGroupBuffers::new(tas.allocator());
        let mut ctx = file.decode_context();
        decode_row_group(
            &mut ctx,
            &mut bufs,
            ColumnChunkSource::File(&file.parquet),
            &reader,
            &[(1, to_type)],
            0,
            2,
            8,
        )?;
        let context = format!("{source:?} -> {to_type:?}");
        let chunk = &bufs.column_bufs[0];
        assert_eq!(chunk.top_rows(), &[[1, 3], [4, 5]], "{context}");
        assert_rows(&context, chunk, source, to_type, &rows[2..8]);
    }
    Ok(())
}

#[test]
fn decode_across_pages_appends_column_top_rows() -> ParquetResult<()> {
    // Pages of a few rows each, so the column top of a folder spans pages and a page starts
    // inside a top: the rows of each page land after those of the pages before it.
    const PAGED: [Folder; 4] = [
        folder(0, &[1, 0, 3, 4, 5, 0, 7]),
        folder(9, &[8, 0]),
        folder(4, &[2]),
        folder(0, &[6, 6, 0, 6]),
    ];
    let rows = model(&PAGED);
    for (source, encodings) in no_sentinel_sources() {
        for (&encoding, version) in encodings
            .iter()
            .flat_map(|e| [(e, Version::V1), (e, Version::V2)])
        {
            let file =
                write_folders_with_version(source, encoding, &PAGED, rows.len(), Some(8), version);
            let reader = file.parquet_meta_reader();
            let target = ColumnTypeTag::Double.into_type();
            let to_type = if source == ColumnTypeTag::Char {
                source.into_type()
            } else {
                target
            };
            let tas = TestAllocatorState::new();
            let mut bufs = RowGroupBuffers::new(tas.allocator());
            let mut ctx = file.decode_context();
            decode_row_group(
                &mut ctx,
                &mut bufs,
                ColumnChunkSource::File(&file.parquet),
                &reader,
                &[(1, to_type)],
                0,
                0,
                rows.len(),
            )?;
            let context = format!("{source:?} -> {to_type:?}, encoding {encoding}, {version:?}");
            let chunk = &bufs.column_bufs[0];
            assert_eq!(chunk.top_rows(), top_rows_of(&rows).as_slice(), "{context}");
            assert_rows(&context, chunk, source, to_type, &rows);
        }
    }
    Ok(())
}

#[test]
fn decode_filtered_compacts_column_top_rows() -> ParquetResult<()> {
    // Matching rows 1, 3, 4, 6, 8: 0 | T T | T 1 -> compacted rows 1..4 are column-top rows.
    let filter: [i64; 5] = [1, 3, 4, 6, 8];
    let rows = model(&FOLDERS);
    let compacted: Vec<Option<i64>> = filter.iter().map(|&r| rows[r as usize]).collect();
    for (source, encodings) in no_sentinel_sources() {
        for (&encoding, version) in encodings
            .iter()
            .flat_map(|e| [(e, Version::V1), (e, Version::V2)])
        {
            let file = write_folders_with_version(source, encoding, &FOLDERS, 9, None, version);
            let reader = file.parquet_meta_reader();
            for target in targets_of(source) {
                let tas = TestAllocatorState::new();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                let mut ctx = file.decode_context();
                let decoded = decode_row_group_filtered::<false>(
                    &mut ctx,
                    &mut bufs,
                    ColumnChunkSource::File(&file.parquet),
                    &reader,
                    0,
                    &[(1, target)],
                    0,
                    0,
                    rows.len(),
                    &filter,
                )?;
                assert_eq!(decoded, filter.len());
                let context = format!("{source:?} -> {target:?}, encoding {encoding}, {version:?}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &[[1, 4]], "{context}");
                assert_rows(&context, chunk, source, target, &compacted);
            }
        }
    }
    Ok(())
}

#[test]
fn decode_filling_filter_keeps_column_top_runs_whole() -> ParquetResult<()> {
    // A filling filter decodes every row of the window, NULL for the rows outside it. Of the
    // column tops at rows 3, 4 and 6 the filter matches 4 and 6. The rows outside the filter
    // after a matched top row join its run, so the top reads as one run: [4, 9) - the stored
    // values at rows 5, 7 and 8 are outside the filter and no reader reads them. Rows outside
    // the filter between stored values, 1 and 3, start no run.
    let filter: [i64; 4] = [0, 2, 4, 6];
    let rows = model(&FOLDERS);
    for (source, encodings) in no_sentinel_sources() {
        for (&encoding, version) in encodings
            .iter()
            .flat_map(|e| [(e, Version::V1), (e, Version::V2)])
        {
            let file = write_folders_with_version(source, encoding, &FOLDERS, 9, None, version);
            let reader = file.parquet_meta_reader();
            for target in targets_of(source) {
                let tas = TestAllocatorState::new();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                let mut ctx = file.decode_context();
                let decoded = decode_row_group_filtered::<true>(
                    &mut ctx,
                    &mut bufs,
                    ColumnChunkSource::File(&file.parquet),
                    &reader,
                    0,
                    &[(1, target)],
                    0,
                    0,
                    rows.len(),
                    &filter,
                )?;
                assert_eq!(decoded, rows.len());
                let context = format!("{source:?} -> {target:?}, encoding {encoding}, {version:?}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &[[4, 9]], "{context}");
                // the rows of the filter read as stored
                for &r in &filter {
                    let r = r as usize;
                    let expected = match rows[r] {
                        None if has_null(target) => None,
                        None => Some(0),
                        Some(v) => Some(converted(source, target, v)),
                    };
                    assert_eq!(read_row(chunk, target, r), expected, "{context}: row {r}");
                }
            }
        }
    }
    Ok(())
}

#[test]
fn decode_row_group_range_places_column_tops_of_each_group() -> ParquetResult<()> {
    // Row groups of 4 rows: [1 2 3 4] [5 T 6 7]. The top sits in the second group only, at row 5
    // of the concatenation; counting it as a leading run would NULL the stored 1 instead.
    const SHIFTED: [Folder; 2] = [folder(0, &[1, 2, 3, 4, 5]), folder(1, &[6, 7])];
    let rows = model(&SHIFTED);
    for (source, encodings) in no_sentinel_sources() {
        for &encoding in encodings {
            let file = write_folders(source, encoding, &SHIFTED, 4, None);
            let reader = file.parquet_meta_reader();
            assert_eq!(reader.row_group_count(), 2);
            for target in targets_of(source) {
                let context = format!("{source:?} -> {target:?}, encoding {encoding}");
                let tas = TestAllocatorState::new();
                let mut bufs = RowGroupBuffers::new(tas.allocator());
                let mut ctx = file.decode_context();
                let decoded = decode_row_group_range(
                    &mut ctx,
                    &mut bufs,
                    ColumnChunkSource::File(&file.parquet),
                    &reader,
                    &[(1, target)],
                    0,
                    1,
                )?;
                assert_eq!(decoded, rows.len(), "{context}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &[[5, 6]], "{context}");
                assert_rows(&context, chunk, source, target, &rows);
            }
        }
    }
    Ok(())
}

#[test]
fn legacy_required_chunk_takes_leading_column_top() -> ParquetResult<()> {
    // A 9.4.x Required chunk stores its 3 top rows as 0 and has no definition levels: the
    // column top of the file metadata (QuestDB metadata for the footer decoder, the chunk null
    // count for the _pm decoder) is all that tells them from stored values. The Required
    // BOOLEAN encoder counts no NULL, so the _pm decoder has no top of a BOOLEAN to read.
    const VALUES: &[i64] = &[0, 4, 0, 6];
    let rows: Vec<Option<i64>> = [None, None, None]
        .into_iter()
        .chain(VALUES.iter().map(|&v| Some(v)))
        .collect();
    for (source, _) in no_sentinel_sources() {
        let file = write_legacy_required(source, 3, VALUES, rows.len());
        let reader = file.parquet_meta_reader();
        let decoder = file.footer_decoder();
        for target in targets_of(source) {
            let tas = TestAllocatorState::new();
            let mut pm_bufs = RowGroupBuffers::new(tas.allocator());
            let mut ctx = file.decode_context();
            decode_row_group(
                &mut ctx,
                &mut pm_bufs,
                ColumnChunkSource::File(&file.parquet),
                &reader,
                &[(1, target)],
                0,
                0,
                rows.len(),
            )?;
            let mut footer_bufs = RowGroupBuffers::new(tas.allocator());
            decoder.decode_row_group(
                &mut ctx,
                &mut footer_bufs,
                &[(1, target)],
                0,
                0,
                rows.len() as u32,
            )?;
            for (name, bufs) in [("_pm", &pm_bufs), ("footer", &footer_bufs)] {
                if name == "_pm" && source == ColumnTypeTag::Boolean {
                    continue;
                }
                let context = format!("{name} {source:?} -> {target:?}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &[[0, 3]], "{context}");
                assert_rows(&context, chunk, source, target, &rows);
            }
            // a window starting inside the top keeps the rest of it
            let mut pm_window = RowGroupBuffers::new(tas.allocator());
            decode_row_group(
                &mut ctx,
                &mut pm_window,
                ColumnChunkSource::File(&file.parquet),
                &reader,
                &[(1, target)],
                0,
                2,
                rows.len(),
            )?;
            let mut footer_window = RowGroupBuffers::new(tas.allocator());
            decoder.decode_row_group(
                &mut ctx,
                &mut footer_window,
                &[(1, target)],
                0,
                2,
                rows.len() as u32,
            )?;
            for (name, bufs) in [("_pm", &pm_window), ("footer", &footer_window)] {
                if name == "_pm" && source == ColumnTypeTag::Boolean {
                    continue;
                }
                let context = format!("{name} window {source:?} -> {target:?}");
                let chunk = &bufs.column_bufs[0];
                assert_eq!(chunk.top_rows(), &[[0, 1]], "{context}");
                assert_rows(&context, chunk, source, target, &rows[2..]);
            }
        }
    }
    Ok(())
}

#[test]
fn footer_decode_of_a_file_without_questdb_metadata_records_no_top_rows() -> ParquetResult<()> {
    // A file another writer made (read_parquet()) has no column tops: its definition-level-0
    // rows of a type without a NULL sentinel read 0, and no reader converts its columns.
    let file = write_folders(ColumnTypeTag::Short, 0, &FOLDERS, 9, None);
    let mut metadata =
        read_metadata_with_size(&mut Cursor::new(&file.parquet), file.parquet.len() as u64)?;
    metadata.key_value_metadata = None;
    let tas = TestAllocatorState::new();
    let mut decoder = file.footer_decoder();
    decoder.qdb_meta = None;
    decoder.metadata = metadata;
    let mut bufs = RowGroupBuffers::new(tas.allocator());
    let mut ctx = file.decode_context();
    let to_type = ColumnTypeTag::Short.into_type();
    decoder.decode_row_group(&mut ctx, &mut bufs, &[(1, to_type)], 0, 0, 9)?;
    let chunk = &bufs.column_bufs[0];
    assert!(chunk.top_rows().is_empty());
    assert_rows(
        "foreign file",
        chunk,
        ColumnTypeTag::Short,
        to_type,
        &model(&FOLDERS),
    );
    Ok(())
}

#[test]
fn footer_decode_takes_column_top_of_a_source_with_a_null() -> ParquetResult<()> {
    // INT -> SHORT: the decode reads the NULL of INT as 0, a value of SHORT, and only the
    // column top of the QuestDB metadata tells the top apart, which CONVERT TO NATIVE keeps as
    // the native column top of the SHORT column. The footer decode takes the top for every target
    // of a source with a NULL, INT itself and LONG included: Java may convert the decoded values
    // to a type without a NULL, as it does a VARCHAR to BYTE. The _pm null counts count every NULL
    // of INT, not a column top, so its decode reports none.
    const INT_FOLDERS: [Folder; 2] = [folder(3, &[nulls::INT as i64, 2]), folder(0, &[3, 4])];
    let file = write_folders(ColumnTypeTag::Int, 0, &INT_FOLDERS, 7, None);
    let mut metadata =
        read_metadata_with_size(&mut Cursor::new(&file.parquet), file.parquet.len() as u64)?;
    // record the top of 3 rows, as the frame writer does
    let kv = metadata
        .key_value_metadata
        .as_mut()
        .unwrap()
        .iter_mut()
        .find(|kv| kv.key == QDB_META_KEY)
        .unwrap();
    let mut qdb_meta = QdbMeta::deserialize(kv.value.as_deref().unwrap())?;
    qdb_meta.schema[1].column_top = 3;
    kv.value = Some(qdb_meta.serialize()?);
    let mut decoder = file.footer_decoder();
    decoder.qdb_meta = Some(qdb_meta);
    decoder.metadata = metadata;
    let tas = TestAllocatorState::new();
    let mut ctx = file.decode_context();
    for target in [
        ColumnTypeTag::Short.into_type(),
        ColumnTypeTag::Int.into_type(),
        ColumnTypeTag::Long.into_type(),
    ] {
        let mut footer_bufs = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group(&mut ctx, &mut footer_bufs, &[(1, target)], 0, 0, 7)?;
        assert_eq!(
            footer_bufs.column_bufs[0].top_rows(),
            &[[0, 3]],
            "{target:?}"
        );
    }
    let target = ColumnTypeTag::Short.into_type();

    let reader = file.parquet_meta_reader();
    let mut pm_bufs = RowGroupBuffers::new(tas.allocator());
    decode_row_group(
        &mut ctx,
        &mut pm_bufs,
        ColumnChunkSource::File(&file.parquet),
        &reader,
        &[(1, target)],
        0,
        0,
        7,
    )?;
    assert!(pm_bufs.column_bufs[0].top_rows().is_empty());
    Ok(())
}

#[test]
fn post_convert_stamps_null_over_every_top_run() {
    // A run past the end of the buffer is clamped.
    let top_rows: [[u64; 2]; 3] = [[1, 2], [3, 5], [6, 99]];
    let rows = [Some(7), None, Some(0), None, None, Some(1), None, None];
    let tas = TestAllocatorState::new();
    for (source, _) in no_sentinel_sources() {
        // Byte <-> Short decode straight to the target; post_convert converts the rest.
        for target in targets_of(source).into_iter().filter(|t| {
            !matches!(
                (source, t.tag()),
                (ColumnTypeTag::Byte, ColumnTypeTag::Short)
                    | (ColumnTypeTag::Short, ColumnTypeTag::Byte)
            )
        }) {
            let mut bufs = ColumnChunkBuffers::new(tas.allocator());
            let values: Vec<i64> = rows.iter().map(|r| r.unwrap_or(0)).collect();
            bufs.data_vec
                .extend_from_slice(native_bytes(source, &values))
                .unwrap();
            for &[lo, hi] in &top_rows {
                bufs.add_top_rows(lo as usize, hi as usize).unwrap();
            }
            post_convert(source.into_type(), target, &mut bufs).unwrap();
            assert_rows(
                &format!("{source:?} -> {target:?}"),
                &bufs,
                source,
                target,
                &rows,
            );
        }
    }
}

#[test]
fn add_top_rows_joins_adjacent_runs() {
    let tas = TestAllocatorState::new();
    let mut bufs = ColumnChunkBuffers::new(tas.allocator());
    bufs.add_top_rows(0, 0).unwrap();
    assert!(bufs.top_rows().is_empty());
    bufs.add_top_rows(2, 4).unwrap();
    bufs.add_top_rows(4, 5).unwrap();
    bufs.add_top_rows(6, 7).unwrap();
    assert_eq!(bufs.top_rows(), &[[2, 5], [6, 7]]);
    assert_eq!(bufs.top_rows_count, 2);
    assert_eq!(bufs.top_rows_ptr, bufs.top_rows().as_ptr());
    bufs.reset();
    assert!(bufs.top_rows().is_empty());
    assert_eq!(bufs.top_rows_count, 0);
}

#[test]
fn legacy_required_chunk_takes_leading_column_top_of_each_decode() -> ParquetResult<()> {
    // The 9.4.x file of legacy_required_chunk_takes_leading_column_top() in row groups of 2
    // rows: [T T] [T 0] [4 0] [6]. A run of row groups decodes its top at the rows of each group,
    // and a filter decodes the matching rows of it.
    const VALUES: &[i64] = &[0, 4, 0, 6];
    let rows: Vec<Option<i64>> = [None, None, None]
        .into_iter()
        .chain(VALUES.iter().map(|&v| Some(v)))
        .collect();
    for source in [
        ColumnTypeTag::Byte,
        ColumnTypeTag::Short,
        ColumnTypeTag::Char,
    ] {
        let file = write_legacy_required(source, 3, VALUES, 2);
        let reader = file.parquet_meta_reader();
        assert_eq!(reader.row_group_count(), 4);
        let target = if source == ColumnTypeTag::Char {
            source.into_type()
        } else {
            ColumnTypeTag::Double.into_type()
        };
        let tas = TestAllocatorState::new();
        let mut ctx = file.decode_context();
        let mut range_bufs = RowGroupBuffers::new(tas.allocator());
        decode_row_group_range(
            &mut ctx,
            &mut range_bufs,
            ColumnChunkSource::File(&file.parquet),
            &reader,
            &[(1, target)],
            0,
            3,
        )?;
        let context = format!("range {source:?} -> {target:?}");
        assert_eq!(range_bufs.column_bufs[0].top_rows(), &[[0, 3]], "{context}");
        assert_rows(&context, &range_bufs.column_bufs[0], source, target, &rows);

        // row group 1 is [T 0]: the compacting filter matches both rows, the filling one row 1
        let decoder = file.footer_decoder();
        let filter: [i64; 2] = [0, 1];
        let mut pm_compacted = RowGroupBuffers::new(tas.allocator());
        decode_row_group_filtered::<false>(
            &mut ctx,
            &mut pm_compacted,
            ColumnChunkSource::File(&file.parquet),
            &reader,
            0,
            &[(1, target)],
            1,
            0,
            2,
            &filter,
        )?;
        let mut footer_compacted = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group_filtered::<false>(
            &mut ctx,
            &mut footer_compacted,
            0,
            &[(1, target)],
            1,
            0,
            2,
            &filter,
        )?;
        let fill_filter: [i64; 1] = [1];
        let mut pm_filled = RowGroupBuffers::new(tas.allocator());
        decode_row_group_filtered::<true>(
            &mut ctx,
            &mut pm_filled,
            ColumnChunkSource::File(&file.parquet),
            &reader,
            0,
            &[(1, target)],
            1,
            0,
            2,
            &fill_filter,
        )?;
        let mut footer_filled = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group_filtered::<true>(
            &mut ctx,
            &mut footer_filled,
            0,
            &[(1, target)],
            1,
            0,
            2,
            &fill_filter,
        )?;
        for (name, bufs) in [
            ("_pm compacted", &pm_compacted),
            ("footer compacted", &footer_compacted),
            ("_pm filled", &pm_filled),
            ("footer filled", &footer_filled),
        ] {
            let context = format!("{name} {source:?} -> {target:?}");
            let chunk = &bufs.column_bufs[0];
            assert_eq!(chunk.top_rows(), &[[0, 1]], "{context}");
            assert_rows(&context, chunk, source, target, &rows[2..4]);
        }
    }
    Ok(())
}

#[test]
fn footer_filtered_decode_takes_column_top_of_a_source_with_a_null() -> ParquetResult<()> {
    // The filtered decodes of footer_decode_takes_column_top_of_a_source_with_a_null(): INT -> SHORT
    // and INT -> LONG take the top of the QuestDB metadata at the rows the filter keeps.
    const INT_FOLDERS: [Folder; 2] = [folder(3, &[nulls::INT as i64, 2]), folder(0, &[3, 4])];
    let file = write_folders(ColumnTypeTag::Int, 0, &INT_FOLDERS, 7, None);
    let mut decoder = file.footer_decoder();
    let qdb_meta = decoder.qdb_meta.as_mut().unwrap();
    qdb_meta.schema[1].column_top = 3;
    let tas = TestAllocatorState::new();
    let mut ctx = file.decode_context();
    let filter: [i64; 3] = [1, 2, 5];
    for (target, compacted_top_rows, filled_top_rows) in [
        (
            ColumnTypeTag::Short.into_type(),
            vec![[0u64, 2]],
            vec![[0u64, 3]],
        ),
        (
            ColumnTypeTag::Long.into_type(),
            vec![[0u64, 2]],
            vec![[0u64, 3]],
        ),
    ] {
        let mut compacted = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group_filtered::<false>(
            &mut ctx,
            &mut compacted,
            0,
            &[(1, target)],
            0,
            0,
            7,
            &filter,
        )?;
        assert_eq!(
            compacted.column_bufs[0].top_rows(),
            compacted_top_rows.as_slice(),
            "{target:?}"
        );
        let mut filled = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group_filtered::<true>(
            &mut ctx,
            &mut filled,
            0,
            &[(1, target)],
            0,
            0,
            7,
            &filter,
        )?;
        assert_eq!(
            filled.column_bufs[0].top_rows(),
            filled_top_rows.as_slice(),
            "{target:?}"
        );
    }
    Ok(())
}

#[test]
fn decode_records_top_rows_of_rle_booleans() -> ParquetResult<()> {
    // QuestDB writes BOOLEAN pages Plain; another writer may write them RLE. Rows T T 1 0 T 1. The
    // _pm decode records the column tops from the definition levels; the footer decoder takes the
    // column top of the QuestDB metadata, 0 rows, so its rows of a top read false.
    use parquet::basic::{Encoding, Repetition as P1Repetition, Type as P1Type};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use std::sync::Arc;

    let column = Arc::new(
        Type::primitive_type_builder("v", P1Type::BOOLEAN)
            .with_id(Some(0))
            .with_repetition(P1Repetition::OPTIONAL)
            .build()
            .unwrap(),
    );
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![column])
            .build()
            .unwrap(),
    );
    let mut qdb_meta = QdbMeta::new(1);
    qdb_meta
        .schema
        .push(crate::parquet::qdb_metadata::QdbMetaCol {
            column_type: ColumnTypeTag::Boolean.into_type(),
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        });
    let props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::RLE)
            .set_key_value_metadata(Some(vec![KeyValue::new(
                QDB_META_KEY.to_string(),
                qdb_meta.serialize()?,
            )]))
            .build(),
    );
    let mut parquet = Vec::new();
    {
        let mut writer = SerializedFileWriter::new(&mut parquet, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut column_writer = row_group.next_column().unwrap().unwrap();
        column_writer
            .typed::<parquet::data_type::BoolType>()
            .write_batch(&[true, false, true], Some(&[0, 0, 1, 1, 0, 1]), None)
            .unwrap();
        column_writer.close().unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }
    let tas = TestAllocatorState::new();
    let decoder = ParquetDecoder::read(
        tas.allocator(),
        &mut Cursor::new(&parquet),
        parquet.len() as u64,
    )?;
    assert_eq!(
        decoder.metadata.row_groups[0].columns()[0].column_encoding(),
        &[parquet2::thrift_format::Encoding::RLE],
        "the values and levels must be RLE"
    );
    let metadata = read_metadata_with_size(&mut Cursor::new(&parquet), parquet.len() as u64)?;
    let (parquet_meta, parquet_meta_size) =
        convert_from_parquet(&metadata, Some(&qdb_meta), 0, 0, &NoBloomFilterSource, None)?;
    let reader = ParquetMetaReader::from_file_size(&parquet_meta, parquet_meta_size)?;
    let rows = [None, None, Some(1), Some(0), None, Some(1)];
    for target in [
        ColumnTypeTag::Boolean.into_type(),
        ColumnTypeTag::Int.into_type(),
    ] {
        let mut ctx = DecodeContext::new(parquet.as_ptr(), parquet.len() as u64);
        let mut bufs = RowGroupBuffers::new(tas.allocator());
        decode_row_group(
            &mut ctx,
            &mut bufs,
            ColumnChunkSource::File(&parquet),
            &reader,
            &[(0, target)],
            0,
            0,
            6,
        )?;
        let chunk = &bufs.column_bufs[0];
        assert_eq!(chunk.top_rows(), &[[0, 2], [4, 5]], "_pm {target:?}");
        assert_rows(
            &format!("_pm {target:?}"),
            chunk,
            ColumnTypeTag::Boolean,
            target,
            &rows,
        );

        let mut bufs = RowGroupBuffers::new(tas.allocator());
        decoder.decode_row_group(&mut ctx, &mut bufs, &[(0, target)], 0, 0, 6)?;
        let chunk = &bufs.column_bufs[0];
        assert!(chunk.top_rows().is_empty(), "footer {target:?}");
        assert_rows(
            &format!("footer {target:?}"),
            chunk,
            ColumnTypeTag::Boolean,
            target,
            &footer_rows(ColumnTypeTag::Boolean, target, &rows),
        );
    }
    Ok(())
}

#[test]
fn post_convert_reads_int_and_long_null_as_decimal_null() {
    // The NULL of INT and LONG is in band: it converts to the decimal NULL with no column-top rows,
    // over the narrowing (forward) and the widening (backward) loops of every decimal width.
    let tas = TestAllocatorState::new();
    for (source, values) in [
        (
            ColumnTypeTag::Int,
            vec![nulls::INT as i64, 3, nulls::INT as i64],
        ),
        (ColumnTypeTag::Long, vec![nulls::LONG, 3, nulls::LONG]),
    ] {
        for target in [
            decimal(2, 0),
            decimal(4, 1),
            decimal(9, 2),
            decimal(18, 3),
            decimal(38, 4),
            decimal(76, 5),
        ] {
            let mut bufs = ColumnChunkBuffers::new(tas.allocator());
            let bytes: &[u8] = if source == ColumnTypeTag::Int {
                leak(values.iter().map(|&v| v as i32).collect())
            } else {
                leak(values.clone())
            };
            bufs.data_vec.extend_from_slice(bytes).unwrap();
            post_convert(source.into_type(), target, &mut bufs).unwrap();
            let context = format!("{source:?} -> {target:?}");
            assert_eq!(read_row(&bufs, target, 0), None, "{context}");
            assert_eq!(
                read_row(&bufs, target, 1),
                Some(3 * 10i64.pow(target.decimal_scale() as u32)),
                "{context}"
            );
            assert_eq!(read_row(&bufs, target, 2), None, "{context}");
        }
    }
}

#[test]
fn decode_v1_nested_array_records_no_top_rows() -> ParquetResult<()> {
    // The header of a V1 page of a nested-list array counts its values, not its rows, so the
    // decodes count the rows of the page as they decode it. Rows [1.0, 2.0], NULL, [3.0].
    use parquet::basic::{ConvertedType, LogicalType, Repetition as P1Repetition, Type as P1Type};
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use parquet::file::writer::SerializedFileWriter;
    use parquet::format::KeyValue;
    use parquet::schema::types::Type;
    use std::sync::Arc;

    let element = Arc::new(
        Type::primitive_type_builder("element", P1Type::DOUBLE)
            .with_repetition(P1Repetition::OPTIONAL)
            .build()
            .unwrap(),
    );
    let list = Arc::new(
        Type::group_type_builder("list")
            .with_repetition(P1Repetition::REPEATED)
            .with_fields(vec![element])
            .build()
            .unwrap(),
    );
    let array = Arc::new(
        Type::group_type_builder("arr")
            .with_repetition(P1Repetition::OPTIONAL)
            .with_converted_type(ConvertedType::LIST)
            .with_logical_type(Some(LogicalType::List))
            .with_id(Some(0))
            .with_fields(vec![list])
            .build()
            .unwrap(),
    );
    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![array])
            .build()
            .unwrap(),
    );
    let array_type = qdb_core::col_type::encode_array_type(ColumnTypeTag::Double, 1).unwrap();
    let mut qdb_meta = QdbMeta::new(1);
    qdb_meta
        .schema
        .push(crate::parquet::qdb_metadata::QdbMetaCol {
            column_type: array_type,
            column_top: 0,
            format: None,
            ascii: None,
            id: None,
        });
    let props = Arc::new(
        WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_dictionary_enabled(false)
            .set_key_value_metadata(Some(vec![KeyValue::new(
                QDB_META_KEY.to_string(),
                qdb_meta.serialize()?,
            )]))
            .build(),
    );
    let mut parquet = Vec::new();
    {
        let mut writer = SerializedFileWriter::new(&mut parquet, schema, props).unwrap();
        let mut row_group = writer.next_row_group().unwrap();
        let mut column_writer = row_group.next_column().unwrap().unwrap();
        column_writer
            .typed::<parquet::data_type::DoubleType>()
            .write_batch(&[1.0, 2.0, 3.0], Some(&[3, 3, 0, 3]), Some(&[0, 1, 0, 0]))
            .unwrap();
        column_writer.close().unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }
    let file = with_parquet_meta(parquet);
    let decoder = file.footer_decoder();
    let reader = file.parquet_meta_reader();
    let tas = TestAllocatorState::new();
    let mut ctx = file.decode_context();
    let filter: [i64; 2] = [0, 2];
    let columns = [(0, array_type)];

    let mut footer = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decoder.decode_row_group(&mut ctx, &mut footer, &columns, 0, 0, 3)?,
        3
    );
    let mut footer_compacted = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decoder.decode_row_group_filtered::<false>(
            &mut ctx,
            &mut footer_compacted,
            0,
            &columns,
            0,
            0,
            3,
            &filter
        )?,
        2
    );
    let mut footer_filled = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decoder.decode_row_group_filtered::<true>(
            &mut ctx,
            &mut footer_filled,
            0,
            &columns,
            0,
            0,
            3,
            &filter
        )?,
        3
    );
    let source = || ColumnChunkSource::File(&file.parquet);
    let mut pm = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decode_row_group(&mut ctx, &mut pm, source(), &reader, &columns, 0, 0, 3)?,
        3
    );
    let mut pm_compacted = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decode_row_group_filtered::<false>(
            &mut ctx,
            &mut pm_compacted,
            source(),
            &reader,
            0,
            &columns,
            0,
            0,
            3,
            &filter
        )?,
        2
    );
    let mut pm_filled = RowGroupBuffers::new(tas.allocator());
    assert_eq!(
        decode_row_group_filtered::<true>(
            &mut ctx,
            &mut pm_filled,
            source(),
            &reader,
            0,
            &columns,
            0,
            0,
            3,
            &filter
        )?,
        3
    );
    for (name, bufs, rows) in [
        ("footer", &footer, 3),
        ("footer compacted", &footer_compacted, 2),
        ("footer filled", &footer_filled, 3),
        ("_pm", &pm, 3),
        ("_pm compacted", &pm_compacted, 2),
        ("_pm filled", &pm_filled, 3),
    ] {
        let chunk = &bufs.column_bufs[0];
        assert!(chunk.top_rows().is_empty(), "{name}");
        // an aux entry of 16 bytes per row
        assert_eq!(chunk.aux_vec.len(), rows * 16, "{name}");
    }
    Ok(())
}
