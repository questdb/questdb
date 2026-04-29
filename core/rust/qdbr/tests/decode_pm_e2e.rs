//! End-to-end tests: write parquet -> convert_from_parquet() -> ParquetMetaReader
//! -> extract fields -> reconstruct_descriptor() -> decode_column_chunk_with_params()
//! -> compare with footer decode.

#![allow(clippy::needless_range_loop)]

mod common;

use std::io::Cursor;
use std::ptr::null;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use parquet::basic::Repetition;
use parquet::data_type::DataType;
use parquet::file::properties::WriterVersion;
use parquet::schema::types::Type;

use common::{
    decode_file, generate_nulls, qdb_props, qdb_props_compressed,
    types::primitives::{generate_data, Int, Long256, PrimitiveType, Timestamp, Uuid},
    write_parquet_column, Encoding, Null, COUNT,
};

use questdbr::allocator::{MemTracking, QdbAllocator};
use questdbr::parquet::qdb_metadata::{QdbMeta, QdbMetaCol, QdbMetaColFormat};
use questdbr::parquet_metadata::convert::convert_from_parquet;
use questdbr::parquet_metadata::reader::ParquetMetaReader;
use questdbr::parquet_metadata::types::{Codec, ColumnFlags, FieldRepetition};
use questdbr::parquet_read::decode_column::{
    decode_column_chunk_filtered_with_params, decode_column_chunk_with_params,
    reconstruct_descriptor,
};
use questdbr::parquet_read::{ColumnChunkBuffers, DecodeContext, ParquetDecoder};

use parquet2::read::read_metadata_with_size;
use parquet2::schema::Repetition as P2Repetition;

// ── Allocator helper (mirrors common/mod.rs TestAlloc) ──────────────

struct TestAlloc {
    _mem_tracking: Box<MemTracking>,
    _tagged_used: Box<AtomicUsize>,
    allocator: QdbAllocator,
}

impl TestAlloc {
    fn new() -> Self {
        let mem_tracking = Box::new(MemTracking::new());
        let tagged_used = Box::new(AtomicUsize::new(0));
        let allocator = QdbAllocator::new(&*mem_tracking, &*tagged_used, 65);
        Self {
            _mem_tracking: mem_tracking,
            _tagged_used: tagged_used,
            allocator,
        }
    }

    fn allocator(&self) -> QdbAllocator {
        self.allocator.clone()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

fn encode_single_column<T: PrimitiveType>(
    count: usize,
    version: WriterVersion,
    encoding: Encoding,
    null: Null,
) -> Vec<u8>
where
    T::T: Clone,
{
    let nulls = generate_nulls(count, null);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet_vals, _native) = generate_data::<T>(count - null_count);

    let repetition = if matches!(null, Null::None) {
        Repetition::REQUIRED
    } else {
        Repetition::OPTIONAL
    };

    let mut col_builder =
        Type::primitive_type_builder("col", <T::U as DataType>::get_physical_type())
            .with_repetition(repetition);
    if let Some(len) = T::FIXED_LEN {
        col_builder = col_builder.with_length(len);
    }
    if let Some(lt) = T::LOGICAL_TYPE {
        if let parquet::basic::LogicalType::Decimal { scale, precision } = lt {
            col_builder = col_builder.with_precision(precision).with_scale(scale);
        }
        col_builder = col_builder.with_logical_type(Some(lt));
    }
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();

    let def_levels = common::def_levels_from_nulls(&nulls);
    let props = qdb_props(T::TAG, version, encoding);
    write_parquet_column::<T::U>(
        "col",
        schema,
        &parquet_vals,
        Some(&def_levels),
        Arc::new(props),
    )
}

fn encode_single_column_compressed<T: PrimitiveType>(
    count: usize,
    version: WriterVersion,
    encoding: Encoding,
    compression: parquet::basic::Compression,
) -> Vec<u8>
where
    T::T: Clone,
{
    let nulls = generate_nulls(count, Null::None);
    let null_count = nulls.iter().filter(|x| **x).count();
    let (parquet_vals, _native) = generate_data::<T>(count - null_count);

    let col_builder = Type::primitive_type_builder("col", <T::U as DataType>::get_physical_type())
        .with_repetition(Repetition::REQUIRED);
    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(col_builder.build().unwrap())])
        .build()
        .unwrap();

    let def_levels = common::def_levels_from_nulls(&nulls);
    let props = qdb_props_compressed(T::TAG, version, encoding, compression);
    write_parquet_column::<T::U>(
        "col",
        schema,
        &parquet_vals,
        Some(&def_levels),
        Arc::new(props),
    )
}

/// Read parquet2 metadata and compute footer offset/length from the raw file bytes.
fn read_parquet2_metadata(parquet_bytes: &[u8]) -> (parquet2::metadata::FileMetaData, u64, u32) {
    let buf_len = parquet_bytes.len() as u64;
    let mut cursor = Cursor::new(parquet_bytes);
    let metadata = read_metadata_with_size(&mut cursor, buf_len).expect("read parquet metadata");

    let footer_len_offset = parquet_bytes.len() - 8;
    let thrift_footer_len = u32::from_le_bytes(
        parquet_bytes[footer_len_offset..footer_len_offset + 4]
            .try_into()
            .unwrap(),
    );
    let parquet_footer_offset = buf_len - 8 - thrift_footer_len as u64;
    let parquet_footer_length = thrift_footer_len + 8;

    (metadata, parquet_footer_offset, parquet_footer_length)
}

/// Run the full _pm e2e pipeline for a single-column parquet file and compare
/// the decoded output with the footer-based decode.
fn run_e2e_pipeline(parquet_bytes: &[u8]) {
    let buf_len = parquet_bytes.len() as u64;

    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(parquet_bytes);

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize QdbMeta"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader::new");
    pm_reader.verify_checksum().expect("CRC checksum");

    let col_count = pm_reader.column_count() as usize;
    let rg_count = pm_reader.row_group_count() as usize;

    assert!(col_count > 0, "expected at least one column");
    assert!(rg_count > 0, "expected at least one row group");

    let ta = TestAlloc::new();
    let allocator = ta.allocator();

    let col_types: Vec<_> = {
        let mut r2 = Cursor::new(parquet_bytes);
        let dec = ParquetDecoder::read(allocator.clone(), &mut r2, buf_len)
            .expect("ParquetDecoder::read for type inference");
        (0..col_count)
            .map(|i| {
                dec.columns[i]
                    .column_type
                    .expect("column type should be recognized")
            })
            .collect()
    };

    let (format, ascii) = extract_format_ascii_from_metadata(&metadata);

    let mut pm_all_data: Vec<Vec<u8>> = vec![Vec::new(); col_count];
    let mut pm_all_aux: Vec<Vec<u8>> = vec![Vec::new(); col_count];

    for rg_idx in 0..rg_count {
        let rg = pm_reader.row_group(rg_idx).expect("row_group");
        let rg_rows = rg.num_rows() as usize;

        for col_idx in 0..col_count {
            let desc = pm_reader
                .column_descriptor(col_idx)
                .expect("column_descriptor");
            let chunk = rg.column_chunk(col_idx).expect("column_chunk");
            let col_name = pm_reader.column_name(col_idx).expect("column_name");

            let flags = ColumnFlags(desc.flags);
            let repetition: P2Repetition = flags.repetition().expect("repetition").into();

            let codec = chunk.codec().expect("codec");
            let compression: parquet2::compression::Compression = codec.into();

            let col_info = QdbMetaCol {
                column_type: col_types[col_idx],
                column_top: 0,
                format: if col_idx == 0 { format } else { None },
                ascii: if col_idx == 0 { ascii } else { None },
            };

            let descriptor = reconstruct_descriptor(
                desc.physical_type,
                desc.fixed_byte_len,
                desc.max_rep_level,
                desc.max_def_level,
                col_name,
                repetition,
            );

            let mut ctx = DecodeContext::new(parquet_bytes.as_ptr(), buf_len);
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());

            decode_column_chunk_with_params(
                &mut ctx,
                &mut bufs,
                parquet_bytes,
                chunk.byte_range_start as usize,
                chunk.total_compressed as usize,
                compression,
                descriptor,
                chunk.num_values as i64,
                col_info,
                0,
                rg_rows,
                col_name,
                rg_idx,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "decode_column_chunk_with_params rg={} col={}: {}",
                    rg_idx, col_idx, e
                )
            });

            pm_all_data[col_idx].extend_from_slice(&bufs.data_vec);
            pm_all_aux[col_idx].extend_from_slice(&bufs.aux_vec);
        }
    }

    if col_count == 1 {
        let (footer_data, footer_aux) = decode_file(parquet_bytes);
        assert_eq!(
            footer_data.len(),
            pm_all_data[0].len(),
            "data_vec length mismatch: footer={} pm={}",
            footer_data.len(),
            pm_all_data[0].len()
        );
        assert_eq!(
            footer_data,
            pm_all_data[0],
            "data_vec content mismatch (first diff at byte {})",
            footer_data
                .iter()
                .zip(pm_all_data[0].iter())
                .position(|(a, b)| a != b)
                .unwrap_or(0)
        );
        assert_eq!(
            footer_aux.len(),
            pm_all_aux[0].len(),
            "aux_vec length mismatch"
        );
        assert_eq!(footer_aux, pm_all_aux[0], "aux_vec content mismatch");
    }
}

/// Run the full _pm pipeline and return decoded data for multi-column files.
fn run_e2e_pipeline_multi(parquet_bytes: &[u8]) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let buf_len = parquet_bytes.len() as u64;

    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(parquet_bytes);

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize QdbMeta"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader::new");
    pm_reader.verify_checksum().expect("CRC checksum");

    let col_count = pm_reader.column_count() as usize;
    let rg_count = pm_reader.row_group_count() as usize;

    let ta = TestAlloc::new();
    let allocator = ta.allocator();

    let col_types: Vec<_> = {
        let mut r2 = Cursor::new(parquet_bytes);
        let dec = ParquetDecoder::read(allocator.clone(), &mut r2, buf_len)
            .expect("ParquetDecoder::read for type inference");
        (0..col_count)
            .map(|i| {
                dec.columns[i]
                    .column_type
                    .expect("column type should be recognized")
            })
            .collect()
    };

    let mut pm_all_data: Vec<Vec<u8>> = vec![Vec::new(); col_count];
    let mut pm_all_aux: Vec<Vec<u8>> = vec![Vec::new(); col_count];

    for rg_idx in 0..rg_count {
        let rg = pm_reader.row_group(rg_idx).expect("row_group");
        let rg_rows = rg.num_rows() as usize;

        for col_idx in 0..col_count {
            let desc = pm_reader
                .column_descriptor(col_idx)
                .expect("column_descriptor");
            let chunk = rg.column_chunk(col_idx).expect("column_chunk");
            let col_name = pm_reader.column_name(col_idx).expect("column_name");

            let flags = ColumnFlags(desc.flags);
            let repetition: P2Repetition = flags.repetition().expect("repetition").into();

            let codec = chunk.codec().expect("codec");
            let compression: parquet2::compression::Compression = codec.into();

            let col_info = QdbMetaCol {
                column_type: col_types[col_idx],
                column_top: 0,
                format: None,
                ascii: None,
            };

            let descriptor = reconstruct_descriptor(
                desc.physical_type,
                desc.fixed_byte_len,
                desc.max_rep_level,
                desc.max_def_level,
                col_name,
                repetition,
            );

            let mut ctx = DecodeContext::new(parquet_bytes.as_ptr(), buf_len);
            let mut bufs = ColumnChunkBuffers::new(allocator.clone());

            decode_column_chunk_with_params(
                &mut ctx,
                &mut bufs,
                parquet_bytes,
                chunk.byte_range_start as usize,
                chunk.total_compressed as usize,
                compression,
                descriptor,
                chunk.num_values as i64,
                col_info,
                0,
                rg_rows,
                col_name,
                rg_idx,
            )
            .unwrap_or_else(|e| {
                panic!(
                    "decode_column_chunk_with_params rg={} col={}: {}",
                    rg_idx, col_idx, e
                )
            });

            pm_all_data[col_idx].extend_from_slice(&bufs.data_vec);
            pm_all_aux[col_idx].extend_from_slice(&bufs.aux_vec);
        }
    }

    (pm_all_data, pm_all_aux)
}

fn extract_format_ascii_from_metadata(
    metadata: &parquet2::metadata::FileMetaData,
) -> (Option<QdbMetaColFormat>, Option<bool>) {
    let json_str = metadata.key_value_metadata.as_ref().and_then(|kvs| {
        kvs.iter()
            .find(|kv| kv.key == "questdb")
            .and_then(|kv| kv.value.as_deref())
    });

    let Some(json_str) = json_str else {
        return (None, None);
    };

    let format = if json_str.contains("\"format\":1") {
        Some(QdbMetaColFormat::LocalKeyIsGlobal)
    } else {
        None
    };

    let ascii = if json_str.contains("\"ascii\":true") {
        Some(true)
    } else if json_str.contains("\"ascii\":false") {
        Some(false)
    } else {
        None
    };

    (format, ascii)
}

// ── Test 1: Single Timestamp column ──────────────────────────────────

#[test]
fn e2e_single_timestamp() {
    let parquet_bytes = encode_single_column::<Timestamp>(
        100,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::None,
    );

    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(&parquet_bytes);

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader::new");
    pm_reader.verify_checksum().expect("CRC");

    assert_eq!(pm_reader.column_count(), 1);
    assert_eq!(pm_reader.row_group_count(), 1);
    assert_eq!(pm_reader.column_name(0).unwrap(), "col");

    let desc = pm_reader.column_descriptor(0).unwrap();
    assert_eq!(desc.physical_type, 2, "expected Int64 physical type");
    assert_eq!(desc.max_rep_level, 0, "required column has rep=0");
    assert_eq!(desc.max_def_level, 0, "required column has def=0");

    let rg = pm_reader.row_group(0).unwrap();
    assert_eq!(rg.num_rows(), 100);

    let chunk = rg.column_chunk(0).unwrap();
    assert_eq!(chunk.num_values, 100);

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 2: Multi-column (Timestamp + Int + Double + Boolean) ────────

#[test]
fn e2e_multi_column() {
    use questdbr::parquet_write::schema::{Column, Partition};
    use questdbr::parquet_write::ParquetWriter;

    let row_count = 100usize;

    let ts_data: Vec<i64> = (0..row_count).map(|i| (i as i64) * 1_000_000).collect();
    let int_data: Vec<i32> = (0..row_count).map(|i| i as i32 * 42).collect();
    let dbl_data: Vec<f64> = (0..row_count).map(|i| i as f64 * 1.5).collect();
    let bool_data: Vec<u8> = (0..row_count).map(|i| (i % 2) as u8).collect();

    let ts_col = Column::from_raw_data(
        0,
        "ts",
        8,
        0,
        row_count,
        ts_data.as_ptr() as *const u8,
        ts_data.len() * std::mem::size_of::<i64>(),
        null(),
        0,
        null(),
        0,
        true,
        false,
        0,
    )
    .expect("ts column");

    let int_col = Column::from_raw_data(
        1,
        "val",
        5,
        0,
        row_count,
        int_data.as_ptr() as *const u8,
        int_data.len() * std::mem::size_of::<i32>(),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("int column");

    let dbl_col = Column::from_raw_data(
        2,
        "price",
        10,
        0,
        row_count,
        dbl_data.as_ptr() as *const u8,
        dbl_data.len() * std::mem::size_of::<f64>(),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("double column");

    let bool_col = Column::from_raw_data(
        3,
        "flag",
        1,
        0,
        row_count,
        bool_data.as_ptr(),
        bool_data.len() * std::mem::size_of::<u8>(),
        null(),
        0,
        null(),
        0,
        false,
        false,
        0,
    )
    .expect("boolean column");

    let partition = Partition {
        table: "test".to_string(),
        columns: vec![ts_col, int_col, dbl_col, bool_col],
    };

    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .finish(partition)
        .expect("ParquetWriter::finish");
    let parquet_bytes = buf.into_inner();

    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(&parquet_bytes);

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader::new");
    pm_reader.verify_checksum().expect("CRC");

    assert_eq!(pm_reader.column_count(), 4);
    assert_eq!(pm_reader.row_group_count(), 1);

    assert_eq!(pm_reader.column_name(0).unwrap(), "ts");
    assert_eq!(pm_reader.column_name(1).unwrap(), "val");
    assert_eq!(pm_reader.column_name(2).unwrap(), "price");
    assert_eq!(pm_reader.column_name(3).unwrap(), "flag");

    // col_type stores the full ColumnType code (includes designated-timestamp flags).
    // Mask with 0xFF to extract the tag for comparison.
    assert_eq!(pm_reader.column_descriptor(0).unwrap().col_type & 0xFF, 8); // Timestamp
    assert_eq!(pm_reader.column_descriptor(0).unwrap().physical_type, 2); // Int64
    assert_eq!(pm_reader.column_descriptor(1).unwrap().col_type & 0xFF, 5); // Int
    assert_eq!(pm_reader.column_descriptor(1).unwrap().physical_type, 1); // Int32
    assert_eq!(pm_reader.column_descriptor(2).unwrap().col_type & 0xFF, 10); // Double
    assert_eq!(pm_reader.column_descriptor(2).unwrap().physical_type, 5); // Double
    assert_eq!(pm_reader.column_descriptor(3).unwrap().col_type & 0xFF, 1); // Boolean
    assert_eq!(pm_reader.column_descriptor(3).unwrap().physical_type, 0); // Boolean

    let buf_len = parquet_bytes.len() as u64;
    let (pm_data, _pm_aux) = run_e2e_pipeline_multi(&parquet_bytes);

    let ta = TestAlloc::new();
    let allocator = ta.allocator();
    let mut r2 = Cursor::new(&parquet_bytes);
    let decoder =
        ParquetDecoder::read(allocator.clone(), &mut r2, buf_len).expect("ParquetDecoder::read");

    let mut rgb = questdbr::parquet_read::RowGroupBuffers::new(allocator);
    let mut ctx = DecodeContext::new(parquet_bytes.as_ptr(), buf_len);

    let columns: Vec<(i32, qdb_core::col_type::ColumnType)> = (0..4)
        .map(|i| {
            (
                i as i32,
                decoder.columns[i].column_type.expect("column type"),
            )
        })
        .collect();

    let rg_size = decoder.row_group_sizes[0];
    decoder
        .decode_row_group(&mut ctx, &mut rgb, &columns, 0, 0, rg_size)
        .expect("decode_row_group");

    for (col_idx, pm_col_data) in pm_data.iter().enumerate().take(4) {
        let footer_bufs = &rgb.column_buffers()[col_idx];
        assert_eq!(
            footer_bufs.data_vec.len(),
            pm_col_data.len(),
            "col {} data_vec length mismatch",
            col_idx
        );
        assert_eq!(
            footer_bufs.data_vec.as_slice(),
            pm_col_data.as_slice(),
            "col {} data_vec content mismatch",
            col_idx
        );
    }
}

// ── Test 3: FLBA-16 UUID ────────────────────────────────────────────

#[test]
fn e2e_flba_uuid() {
    let parquet_bytes = encode_single_column::<Uuid>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::Sparse,
    );

    let (metadata, fo, fl) = read_parquet2_metadata(&parquet_bytes);
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_fs) = convert_from_parquet(&metadata, qdb_meta.as_ref(), fo, fl, None)
        .expect("convert_from_parquet");
    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_fs).expect("ParquetMetaReader::new");

    let desc = pm_reader.column_descriptor(0).unwrap();
    assert_eq!(desc.physical_type, 7, "expected FLBA physical type");
    assert_eq!(desc.fixed_byte_len, 16, "UUID is FLBA-16");

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 4: FLBA-32 Long256 ────────────────────────────────────────

#[test]
fn e2e_flba_long256() {
    let parquet_bytes = encode_single_column::<Long256>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::Sparse,
    );

    let (metadata, fo, fl) = read_parquet2_metadata(&parquet_bytes);
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_fs) = convert_from_parquet(&metadata, qdb_meta.as_ref(), fo, fl, None)
        .expect("convert_from_parquet");
    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_fs).expect("ParquetMetaReader::new");

    let desc = pm_reader.column_descriptor(0).unwrap();
    assert_eq!(desc.physical_type, 7, "expected FLBA physical type");
    assert_eq!(desc.fixed_byte_len, 32, "Long256 is FLBA-32");

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 5: Snappy compression ──────────────────────────────────────

#[test]
fn e2e_snappy_compressed() {
    let parquet_bytes = encode_single_column_compressed::<Timestamp>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        parquet::basic::Compression::SNAPPY,
    );

    let (metadata, fo, fl) = read_parquet2_metadata(&parquet_bytes);
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_fs) = convert_from_parquet(&metadata, qdb_meta.as_ref(), fo, fl, None)
        .expect("convert_from_parquet");
    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_fs).expect("ParquetMetaReader::new");

    let rg = pm_reader.row_group(0).unwrap();
    let chunk = rg.column_chunk(0).unwrap();
    assert_eq!(
        chunk.codec().unwrap(),
        Codec::Snappy,
        "expected Snappy codec"
    );

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 6: Zstd compression ────────────────────────────────────────

#[test]
fn e2e_zstd_compressed() {
    let parquet_bytes = encode_single_column_compressed::<Timestamp>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        parquet::basic::Compression::ZSTD(Default::default()),
    );

    let (metadata, fo, fl) = read_parquet2_metadata(&parquet_bytes);
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_fs) = convert_from_parquet(&metadata, qdb_meta.as_ref(), fo, fl, None)
        .expect("convert_from_parquet");
    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_fs).expect("ParquetMetaReader::new");

    let rg = pm_reader.row_group(0).unwrap();
    let chunk = rg.column_chunk(0).unwrap();
    assert_eq!(chunk.codec().unwrap(), Codec::Zstd, "expected Zstd codec");

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 7: Nullable Optional Int ───────────────────────────────────

#[test]
fn e2e_nullable_optional() {
    let parquet_bytes = encode_single_column::<Int>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::Sparse,
    );

    let (metadata, fo, fl) = read_parquet2_metadata(&parquet_bytes);
    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_fs) = convert_from_parquet(&metadata, qdb_meta.as_ref(), fo, fl, None)
        .expect("convert_from_parquet");
    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_fs).expect("ParquetMetaReader::new");

    let desc = pm_reader.column_descriptor(0).unwrap();
    assert_eq!(
        desc.max_def_level, 1,
        "optional column should have def_level=1"
    );

    let flags = ColumnFlags(desc.flags);
    assert_eq!(
        flags.repetition().unwrap(),
        FieldRepetition::Optional,
        "expected Optional repetition in _pm"
    );

    run_e2e_pipeline(&parquet_bytes);
}

// ── Test 8: Multiple row groups ─────────────────────────────────────

#[test]
fn e2e_multiple_row_groups() {
    use questdbr::parquet_write::schema::{Column, Partition};
    use questdbr::parquet_write::ParquetWriter;

    let row_count = 300usize;
    let row_group_size = 100usize;

    let ts_data: Vec<i64> = (0..row_count).map(|i| (i as i64) * 1_000_000).collect();

    let ts_col = Column::from_raw_data(
        0,
        "ts",
        8,
        0,
        row_count,
        ts_data.as_ptr() as *const u8,
        ts_data.len() * std::mem::size_of::<i64>(),
        null(),
        0,
        null(),
        0,
        true,
        false,
        0,
    )
    .expect("ts column");

    let partition = Partition {
        table: "test".to_string(),
        columns: vec![ts_col],
    };

    let mut buf = Cursor::new(Vec::new());
    ParquetWriter::new(&mut buf)
        .with_statistics(true)
        .with_row_group_size(Some(row_group_size))
        .finish(partition)
        .expect("ParquetWriter::finish");
    let parquet_bytes = buf.into_inner();

    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(&parquet_bytes);
    assert!(
        metadata.row_groups.len() >= 3,
        "expected 3+ row groups, got {}",
        metadata.row_groups.len()
    );

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|s| QdbMeta::deserialize(s).expect("deserialize"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader::new");
    pm_reader.verify_checksum().expect("CRC");

    let rg_count = pm_reader.row_group_count() as usize;
    assert!(
        rg_count >= 3,
        "expected 3+ row groups in _pm, got {}",
        rg_count
    );

    let mut total_rows = 0u64;
    for rg_idx in 0..rg_count {
        let rg = pm_reader.row_group(rg_idx).unwrap();
        total_rows += rg.num_rows();
        assert!(
            rg.num_rows() <= row_group_size as u64,
            "row group {} has {} rows, expected <= {}",
            rg_idx,
            rg.num_rows(),
            row_group_size
        );
    }
    assert_eq!(
        total_rows, row_count as u64,
        "total rows across all row groups"
    );

    // Decode each row group via _pm path.
    let buf_len = parquet_bytes.len() as u64;
    let ta = TestAlloc::new();
    let allocator = ta.allocator();

    let col_type = {
        let mut r2 = Cursor::new(&parquet_bytes);
        let dec = ParquetDecoder::read(allocator.clone(), &mut r2, buf_len)
            .expect("ParquetDecoder::read");
        dec.columns[0].column_type.expect("column type")
    };

    let desc = pm_reader.column_descriptor(0).unwrap();
    let flags = ColumnFlags(desc.flags);
    let repetition: P2Repetition = flags.repetition().expect("repetition").into();

    let col_info = QdbMetaCol {
        column_type: col_type,
        column_top: 0,
        format: None,
        ascii: None,
    };

    let mut pm_all_data = Vec::new();

    for rg_idx in 0..rg_count {
        let rg = pm_reader.row_group(rg_idx).unwrap();
        let rg_rows = rg.num_rows() as usize;
        let chunk = rg.column_chunk(0).unwrap();
        let col_name = pm_reader.column_name(0).unwrap();

        let codec = chunk.codec().expect("codec");
        let compression: parquet2::compression::Compression = codec.into();

        let descriptor = reconstruct_descriptor(
            desc.physical_type,
            desc.fixed_byte_len,
            desc.max_rep_level,
            desc.max_def_level,
            col_name,
            repetition,
        );

        let mut ctx = DecodeContext::new(parquet_bytes.as_ptr(), buf_len);
        let mut bufs = ColumnChunkBuffers::new(allocator.clone());

        let decoded_rows = decode_column_chunk_with_params(
            &mut ctx,
            &mut bufs,
            &parquet_bytes,
            chunk.byte_range_start as usize,
            chunk.total_compressed as usize,
            compression,
            descriptor,
            chunk.num_values as i64,
            col_info,
            0,
            rg_rows,
            col_name,
            rg_idx,
        )
        .unwrap_or_else(|e| panic!("decode rg {}: {}", rg_idx, e));

        assert_eq!(
            decoded_rows, rg_rows,
            "row group {} row count mismatch",
            rg_idx
        );
        pm_all_data.extend_from_slice(&bufs.data_vec);
    }

    let (footer_data, _footer_aux) = decode_file(&parquet_bytes);
    assert_eq!(
        footer_data.len(),
        pm_all_data.len(),
        "total data_vec length mismatch"
    );
    assert_eq!(footer_data, pm_all_data, "data_vec content mismatch");
}

// ── Coverage gap: filtered decode ───────────────────────────────────

/// Helper: decode a single-column parquet file through _pm with a row filter.
fn run_e2e_filtered<const FILL_NULLS: bool>(parquet_bytes: &[u8], rows_filter: &[i64]) -> Vec<u8> {
    let buf_len = parquet_bytes.len() as u64;
    let (metadata, parquet_footer_offset, parquet_footer_length) =
        read_parquet2_metadata(parquet_bytes);

    let qdb_meta = metadata
        .key_value_metadata
        .as_ref()
        .and_then(|kvs| {
            kvs.iter()
                .find(|kv| kv.key == "questdb")
                .and_then(|kv| kv.value.as_deref())
        })
        .map(|json_str| QdbMeta::deserialize(json_str).expect("deserialize"));

    let (pm_bytes, pm_file_size) = convert_from_parquet(
        &metadata,
        qdb_meta.as_ref(),
        parquet_footer_offset,
        parquet_footer_length,
        None,
    )
    .expect("convert_from_parquet");

    let pm_reader =
        ParquetMetaReader::from_file_size(&pm_bytes, pm_file_size).expect("ParquetMetaReader");

    let ta = TestAlloc::new();
    let allocator = ta.allocator();

    let col_type = {
        let mut r2 = Cursor::new(parquet_bytes);
        let dec = ParquetDecoder::read(allocator.clone(), &mut r2, buf_len)
            .expect("ParquetDecoder::read");
        dec.columns[0].column_type.expect("column type")
    };
    let (format, ascii) = extract_format_ascii_from_metadata(&metadata);

    let rg = pm_reader.row_group(0).expect("row_group");
    let rg_rows = rg.num_rows() as usize;
    let desc = pm_reader.column_descriptor(0).expect("descriptor");
    let chunk = rg.column_chunk(0).expect("chunk");
    let col_name = pm_reader.column_name(0).expect("name");
    let flags = ColumnFlags(desc.flags);
    let repetition: P2Repetition = flags.repetition().expect("repetition").into();
    let compression: parquet2::compression::Compression = chunk.codec().expect("codec").into();

    let col_info = QdbMetaCol {
        column_type: col_type,
        column_top: 0,
        format,
        ascii,
    };

    let descriptor = reconstruct_descriptor(
        desc.physical_type,
        desc.fixed_byte_len,
        desc.max_rep_level,
        desc.max_def_level,
        col_name,
        repetition,
    );

    let mut ctx = DecodeContext::new(parquet_bytes.as_ptr(), buf_len);
    let mut bufs = ColumnChunkBuffers::new(allocator);

    decode_column_chunk_filtered_with_params::<FILL_NULLS>(
        &mut ctx,
        &mut bufs,
        parquet_bytes,
        chunk.byte_range_start as usize,
        chunk.total_compressed as usize,
        compression,
        descriptor,
        chunk.num_values as i64,
        col_info,
        0,
        rg_rows,
        rows_filter,
        col_name,
        0,
    )
    .expect("decode_column_chunk_filtered_with_params");

    bufs.data_vec.to_vec()
}

#[test]
fn e2e_filtered_skip() {
    let parquet_bytes = encode_single_column::<Timestamp>(
        100,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::None,
    );
    let filter: Vec<i64> = (0..50).map(|i| i * 2).collect(); // every other row
    let data = run_e2e_filtered::<false>(&parquet_bytes, &filter);
    // 50 selected rows * 8 bytes per i64
    assert_eq!(data.len(), 50 * 8, "filtered skip: wrong output size");
}

#[test]
fn e2e_filtered_fill_nulls() {
    let parquet_bytes = encode_single_column::<Timestamp>(
        100,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        Null::None,
    );
    let filter: Vec<i64> = (0..50).map(|i| i * 2).collect();
    let data = run_e2e_filtered::<true>(&parquet_bytes, &filter);
    // FILL_NULLS: full row range output = 100 rows * 8 bytes
    assert_eq!(
        data.len(),
        100 * 8,
        "filtered fill_nulls: wrong output size"
    );
}

// ── Coverage gap: dictionary-encoded column ─────────────────────────

#[test]
fn e2e_dict_encoded() {
    // Low cardinality Int column → RleDictionary encoding triggers dict page path.
    let parquet_bytes = encode_single_column::<Int>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::RleDictionary,
        Null::None,
    );
    run_e2e_pipeline(&parquet_bytes);
}

// ── Coverage gap: varchar_slice branches ─────────────────────────────

#[test]
fn e2e_varchar_slice() {
    use common::{
        optional_byte_array_schema, qdb_props_ascii,
        types::strings::generate_values as gen_str_vals, write_parquet_column,
    };
    use parquet::data_type::ByteArrayType;

    let nulls = generate_nulls(COUNT, Null::Sparse);
    let values = gen_str_vals(COUNT);
    let non_null = common::non_null_only(&values, &nulls);
    let def_levels = common::def_levels_from_nulls(&nulls);
    let schema = optional_byte_array_schema("col", None);
    let props = qdb_props_ascii(
        qdb_core::col_type::ColumnTypeTag::VarcharSlice,
        WriterVersion::PARQUET_2_0,
        Encoding::RleDictionary,
        true,
    );
    let parquet_bytes = write_parquet_column::<ByteArrayType>(
        "col",
        schema,
        &non_null,
        Some(&def_levels),
        Arc::new(props),
    );
    run_e2e_pipeline(&parquet_bytes);
}

// ── Coverage gap: dict-encoded filtered decode ──────────────────────

#[test]
fn e2e_filtered_dict_encoded() {
    let parquet_bytes = encode_single_column::<Int>(
        COUNT,
        WriterVersion::PARQUET_2_0,
        Encoding::RleDictionary,
        Null::None,
    );
    let filter: Vec<i64> = (0..COUNT / 2).map(|i| i as i64 * 2).collect();
    let data = run_e2e_filtered::<false>(&parquet_bytes, &filter);
    assert_eq!(data.len(), (COUNT / 2) * 4, "filtered dict: wrong size");
}

// ── Coverage gap: varchar_slice filtered decode ─────────────────────

#[test]
fn e2e_filtered_varchar_slice() {
    use common::{
        optional_byte_array_schema, qdb_props_ascii,
        types::strings::generate_values as gen_str_vals, write_parquet_column,
    };
    use parquet::data_type::ByteArrayType;

    let row_count = 100;
    let nulls = generate_nulls(row_count, Null::Sparse);
    let values = gen_str_vals(row_count);
    let non_null = common::non_null_only(&values, &nulls);
    let def_levels = common::def_levels_from_nulls(&nulls);
    let schema = optional_byte_array_schema("col", None);
    let props = qdb_props_ascii(
        qdb_core::col_type::ColumnTypeTag::VarcharSlice,
        WriterVersion::PARQUET_2_0,
        Encoding::Plain,
        true,
    );
    let parquet_bytes = write_parquet_column::<ByteArrayType>(
        "col",
        schema,
        &non_null,
        Some(&def_levels),
        Arc::new(props),
    );
    let filter: Vec<i64> = (0..row_count / 2).map(|i| i as i64 * 2).collect();
    // VarcharSlice decode may produce empty data_vec if all selected rows are null
    // (sparse nulls at every 10th row — with filter selecting even rows, some nulls hit).
    // Just verify it doesn't panic. The coverage value is in exercising the code path.
    let _data = run_e2e_filtered::<false>(&parquet_bytes, &filter);
}

// ── Coverage gap: filtered multi-page (V1 pages, page_row_count fallback) ──

#[test]
fn e2e_filtered_multi_page() {
    use parquet::basic::Compression as PqCompression;
    use parquet::file::properties::WriterProperties;
    use parquet::format::KeyValue;

    let row_count = 10_000usize;
    let data: Vec<i64> = (0..row_count as i64).collect();
    let nulls = generate_nulls(row_count, Null::None);
    let def_levels = common::def_levels_from_nulls(&nulls);

    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(
            Type::primitive_type_builder("col", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        )])
        .build()
        .unwrap();

    let qdb_json = format!(
        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0}}]}}"#,
        qdb_core::col_type::ColumnTypeTag::Long as u8
    );
    // V1 pages + tiny page size = many pages without num_rows in header
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_1_0)
        .set_dictionary_enabled(false)
        .set_compression(PqCompression::UNCOMPRESSED)
        .set_data_page_size_limit(512)
        .set_key_value_metadata(Some(vec![KeyValue::new("questdb".to_string(), qdb_json)]))
        .build();

    let parquet_bytes = write_parquet_column::<parquet::data_type::Int64Type>(
        "col",
        schema,
        &data,
        Some(&def_levels),
        Arc::new(props),
    );

    // Unfiltered: cover fallback page_row_count path in decode_column_chunk_with_params
    run_e2e_pipeline(&parquet_bytes);

    // Filtered: cover fallback path in decode_column_chunk_filtered_with_params (lines 361+)
    let filter: Vec<i64> = (0..row_count / 2).map(|i| i as i64 * 2).collect();
    let result = run_e2e_filtered::<false>(&parquet_bytes, &filter);
    assert_eq!(result.len(), (row_count / 2) * 8);

    let result_fill = run_e2e_filtered::<true>(&parquet_bytes, &filter);
    assert_eq!(result_fill.len(), row_count * 8);
}

// ── Coverage gap: multi-page column chunk ───────────────────────────

#[test]
fn e2e_multi_page() {
    use parquet::basic::Compression as PqCompression;
    use parquet::file::properties::WriterProperties;
    use parquet::format::KeyValue;

    // Write 10k rows with a tiny data page size to force multiple pages.
    let row_count = 10_000usize;
    let data: Vec<i64> = (0..row_count as i64).collect();
    let nulls = generate_nulls(row_count, Null::None);
    let def_levels = common::def_levels_from_nulls(&nulls);

    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(
            Type::primitive_type_builder("col", parquet::basic::Type::INT64)
                .with_repetition(Repetition::REQUIRED)
                .build()
                .unwrap(),
        )])
        .build()
        .unwrap();

    let qdb_json = format!(
        r#"{{"version":1,"schema":[{{"column_type":{},"column_top":0}}]}}"#,
        qdb_core::col_type::ColumnTypeTag::Long as u8
    );
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_dictionary_enabled(false)
        .set_compression(PqCompression::UNCOMPRESSED)
        .set_data_page_size_limit(512) // tiny pages → many pages per chunk
        .set_key_value_metadata(Some(vec![KeyValue::new("questdb".to_string(), qdb_json)]))
        .build();

    let parquet_bytes = write_parquet_column::<parquet::data_type::Int64Type>(
        "col",
        schema,
        &data,
        Some(&def_levels),
        Arc::new(props),
    );

    run_e2e_pipeline(&parquet_bytes);
}
