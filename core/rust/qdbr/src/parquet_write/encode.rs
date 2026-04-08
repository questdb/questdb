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
 ******************************************************************************/

//! Top-level dispatch for the writer-side encoders.
//!
//! Mirrors `parquet_read/decode.rs` by splitting per physical type into
//! focused dispatch functions. Each function matches `(encoding, column_tag)`
//! and constructs the right concrete encoder from `encoders::*`.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use parquet2::encoding::Encoding;
use parquet2::page::Page;
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::array;
use crate::parquet_write::decimal::{
    Decimal128, Decimal16, Decimal256, Decimal32, Decimal64, Decimal8,
};
use crate::parquet_write::encoders::{
    delta_binary_packed, delta_length_array, plain, rle_dictionary, symbol,
};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::transmute_slice;
use crate::parquet_write::{GeoByte, GeoInt, GeoLong, GeoShort, IPv4};
use qdb_core::col_type::ColumnTypeTag;

/// Encode a column chunk into Parquet pages, dispatching by physical type.
///
/// `columns` carries one Column per partition (length 1 for single-partition
/// row groups, N for multi-partition). Dict-encoded paths build a single
/// global dictionary across the input partitions; plain/delta paths emit one
/// page per partition independently.
#[allow(clippy::too_many_arguments)]
pub fn encode_column_chunk(
    encoding: Encoding,
    parquet_type: &ParquetType,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    if columns.is_empty() {
        return Err(fmt_err!(
            InvalidLayout,
            "encode_column_chunk: columns slice cannot be empty"
        ));
    }

    let column_tag = columns[0].data_type.tag();

    match parquet_type {
        ParquetType::PrimitiveType(pt) => match pt.physical_type {
            PhysicalType::Boolean => encode_boolean_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::Int32 => encode_int32_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::Int64 => encode_int64_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::Float => encode_float_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::Double => encode_double_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::ByteArray => encode_byte_array_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::FixedLenByteArray(_) => encode_fixed_len_dispatch(
                encoding,
                pt,
                column_tag,
                columns,
                first_partition_start,
                last_partition_end,
                options,
                bloom_set,
            ),
            PhysicalType::Int96 => Err(fmt_err!(
                Unsupported,
                "Int96 encoding not supported on the write path"
            )),
        },
        ParquetType::GroupType { .. } => encode_group_dispatch(
            parquet_type,
            columns,
            first_partition_start,
            last_partition_end,
            options,
            encoding,
        ),
    }
}

fn unsupported(encoding: Encoding, tag: ColumnTypeTag, physical: &str) -> ParquetResult<Vec<Page>> {
    Err(fmt_err!(
        Unsupported,
        "encoding {:?} for column type {:?} on {}",
        encoding,
        tag,
        physical
    ))
}

fn encode_boolean_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    match (encoding, column_tag) {
        (Encoding::Plain, ColumnTypeTag::Boolean) => plain::encode_boolean(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        _ => unsupported(encoding, column_tag, "Boolean"),
    }
}

fn encode_int32_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    match (encoding, column_tag) {
        // Plain
        (Encoding::Plain, Int) => plain::encode_simd::<i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Byte) => plain::encode_int_notnull::<i8, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Short) => plain::encode_int_notnull::<i16, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Char) => plain::encode_int_notnull::<u16, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, IPv4) => {
            plain::encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, GeoByte) => {
            plain::encode_int_nullable::<crate::parquet_write::GeoByte, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, GeoShort) => {
            plain::encode_int_nullable::<crate::parquet_write::GeoShort, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, GeoInt) => {
            plain::encode_int_nullable::<crate::parquet_write::GeoInt, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        // DeltaBinaryPacked
        (Encoding::DeltaBinaryPacked, Int) => delta_binary_packed::encode_simd::<i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::DeltaBinaryPacked, Byte) => delta_binary_packed::encode_int_notnull::<i8, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::DeltaBinaryPacked, Short) => {
            delta_binary_packed::encode_int_notnull::<i16, i32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::DeltaBinaryPacked, Char) => delta_binary_packed::encode_int_notnull::<u16, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::DeltaBinaryPacked, IPv4) => {
            delta_binary_packed::encode_int_nullable::<crate::parquet_write::IPv4, i32, true>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::DeltaBinaryPacked, GeoByte) => {
            delta_binary_packed::encode_int_nullable::<crate::parquet_write::GeoByte, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::DeltaBinaryPacked, GeoShort) => {
            delta_binary_packed::encode_int_nullable::<crate::parquet_write::GeoShort, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::DeltaBinaryPacked, GeoInt) => {
            delta_binary_packed::encode_int_nullable::<crate::parquet_write::GeoInt, i32, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        // RleDictionary
        (Encoding::RleDictionary, Int) => rle_dictionary::encode_simd::<i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Byte) => rle_dictionary::encode_int_notnull::<i8, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Short) => rle_dictionary::encode_int_notnull::<i16, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Char) => rle_dictionary::encode_int_notnull::<u16, i32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, IPv4) => {
            rle_dictionary::encode_int_nullable::<crate::parquet_write::IPv4, i32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, GeoByte) => {
            rle_dictionary::encode_int_nullable::<crate::parquet_write::GeoByte, i32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, GeoShort) => {
            rle_dictionary::encode_int_nullable::<crate::parquet_write::GeoShort, i32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, GeoInt) => {
            rle_dictionary::encode_int_nullable::<crate::parquet_write::GeoInt, i32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        _ => unsupported(encoding, column_tag, "Int32"),
    }
}

fn encode_int64_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    // Designated timestamp columns are NOT NULL — they need the Required
    // (notnull) encoder paths even though their column_tag is `Timestamp`.
    let is_designated_timestamp = column_tag == Timestamp && columns[0].designated_timestamp;

    match (encoding, column_tag) {
        // Plain
        (Encoding::Plain, Long) | (Encoding::Plain, Date) => plain::encode_simd::<i64>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Timestamp) => {
            if is_designated_timestamp {
                plain::encode_int_notnull::<i64, i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            } else {
                plain::encode_simd::<i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            }
        }
        (Encoding::Plain, GeoLong) => {
            plain::encode_int_nullable::<crate::parquet_write::GeoLong, i64, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        // DeltaBinaryPacked
        (Encoding::DeltaBinaryPacked, Long) | (Encoding::DeltaBinaryPacked, Date) => {
            delta_binary_packed::encode_simd::<i64>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::DeltaBinaryPacked, Timestamp) => {
            if is_designated_timestamp {
                delta_binary_packed::encode_int_notnull::<i64, i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            } else {
                delta_binary_packed::encode_simd::<i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            }
        }
        (Encoding::DeltaBinaryPacked, GeoLong) => {
            delta_binary_packed::encode_int_nullable::<crate::parquet_write::GeoLong, i64, false>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        // RleDictionary
        (Encoding::RleDictionary, Long) | (Encoding::RleDictionary, Date) => {
            rle_dictionary::encode_simd::<i64>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Timestamp) => {
            if is_designated_timestamp {
                // Designated timestamps are Required: use the notnull dict path
                // so the data page is built with Required repetition.
                rle_dictionary::encode_int_notnull::<i64, i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            } else {
                rle_dictionary::encode_simd::<i64>(
                    columns,
                    first_partition_start,
                    last_partition_end,
                    pt,
                    options,
                    bloom_set,
                )
            }
        }
        (Encoding::RleDictionary, GeoLong) => {
            rle_dictionary::encode_int_nullable::<crate::parquet_write::GeoLong, i64>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        _ => unsupported(encoding, column_tag, "Int64"),
    }
}

fn encode_float_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    match (encoding, column_tag) {
        (Encoding::Plain, Float) => plain::encode_simd::<f32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Float) => rle_dictionary::encode_simd::<f32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        _ => unsupported(encoding, column_tag, "Float"),
    }
}

fn encode_double_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    match (encoding, column_tag) {
        (Encoding::Plain, Double) => plain::encode_simd::<f64>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Double) => rle_dictionary::encode_simd::<f64>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        _ => unsupported(encoding, column_tag, "Double"),
    }
}

fn encode_byte_array_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    match (encoding, column_tag) {
        // Plain
        (Encoding::Plain, String) => plain::encode_string(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Binary) => plain::encode_binary(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::Plain, Varchar) => plain::encode_varchar(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),

        // DeltaLengthByteArray
        (Encoding::DeltaLengthByteArray, String) => delta_length_array::encode_string(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::DeltaLengthByteArray, Binary) => delta_length_array::encode_binary(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::DeltaLengthByteArray, Varchar) => delta_length_array::encode_varchar(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),

        // RleDictionary
        (Encoding::RleDictionary, Symbol) => symbol::encode(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, String) => rle_dictionary::encode_string(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Binary) => rle_dictionary::encode_binary(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),
        (Encoding::RleDictionary, Varchar) => rle_dictionary::encode_varchar(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            bloom_set,
        ),

        // Raw-array encoding emits Array columns as PhysicalType::ByteArray.
        // Both Plain and DeltaLengthByteArray are accepted by `array_to_raw_page`.
        (Encoding::Plain, Array) | (Encoding::DeltaLengthByteArray, Array) => encode_array_raw(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            encoding,
        ),

        _ => unsupported(encoding, column_tag, "ByteArray"),
    }
}

fn encode_array_raw(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    pt: &PrimitiveType,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<Vec<Page>> {
    use crate::parquet_write::encoders::helpers::partition_chunk_slice;
    let num_partitions = columns.len();
    let mut pages = Vec::with_capacity(num_partitions);
    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        // SAFETY: JNI-backed, page-aligned, valid `[u8; 16]` aux entries.
        let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
        let page = array::array_to_raw_page(
            &aux[chunk.lower_bound..chunk.upper_bound],
            column.primary_data,
            chunk.adjusted_column_top,
            options,
            pt.clone(),
            encoding,
        )?;
        pages.push(page);
    }
    Ok(pages)
}

fn encode_fixed_len_dispatch(
    encoding: Encoding,
    pt: &PrimitiveType,
    column_tag: ColumnTypeTag,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<Page>> {
    use ColumnTypeTag::*;
    match (encoding, column_tag) {
        // Plain
        (Encoding::Plain, Long128) => plain::encode_fixed_len_bytes::<16>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            false,
            bloom_set,
        ),
        (Encoding::Plain, Uuid) => plain::encode_fixed_len_bytes::<16>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            true,
            bloom_set,
        ),
        (Encoding::Plain, Long256) => plain::encode_fixed_len_bytes::<32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            false,
            bloom_set,
        ),
        (Encoding::Plain, Decimal8) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal8>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, Decimal16) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal16>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, Decimal32) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, Decimal64) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal64>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, Decimal128) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal128>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::Plain, Decimal256) => {
            plain::encode_decimal::<crate::parquet_write::decimal::Decimal256>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        // RleDictionary
        (Encoding::RleDictionary, Long128) => rle_dictionary::encode_fixed_len_bytes::<16>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            false,
            bloom_set,
        ),
        (Encoding::RleDictionary, Uuid) => rle_dictionary::encode_fixed_len_bytes::<16>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            true,
            bloom_set,
        ),
        (Encoding::RleDictionary, Long256) => rle_dictionary::encode_fixed_len_bytes::<32>(
            columns,
            first_partition_start,
            last_partition_end,
            pt,
            options,
            false,
            bloom_set,
        ),
        (Encoding::RleDictionary, Decimal8) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal8>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Decimal16) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal16>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Decimal32) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal32>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Decimal64) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal64>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Decimal128) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal128>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }
        (Encoding::RleDictionary, Decimal256) => {
            rle_dictionary::encode_decimal::<crate::parquet_write::decimal::Decimal256>(
                columns,
                first_partition_start,
                last_partition_end,
                pt,
                options,
                bloom_set,
            )
        }

        _ => unsupported(encoding, column_tag, "FixedLenByteArray"),
    }
}

fn encode_group_dispatch(
    parquet_type: &ParquetType,
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    options: WriteOptions,
    encoding: Encoding,
) -> ParquetResult<Vec<Page>> {
    use crate::parquet_write::encoders::helpers::partition_chunk_slice;
    let num_partitions = columns.len();
    let column_tag = columns[0].data_type.tag();
    if column_tag != ColumnTypeTag::Array {
        return Err(fmt_err!(
            InvalidType,
            "encode_column_chunk: unsupported group type {:?}",
            column_tag
        ));
    }

    let primitive_type = match array_primitive_type(parquet_type) {
        Some(t) => t,
        None => {
            return Err(fmt_err!(
                InvalidType,
                "failed to find inner-most type for array column {}",
                columns[0].name
            ))
        }
    };
    let dim = columns[0].data_type.array_dimensionality()? as usize;

    let mut pages = Vec::with_capacity(num_partitions);
    for (part_idx, column) in columns.iter().enumerate() {
        let chunk = partition_chunk_slice(
            part_idx,
            num_partitions,
            column,
            first_partition_start,
            last_partition_end,
        );
        // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
        // page-aligned. The byte content represents valid `[u8; 16]` aux entries.
        let aux: &[[u8; 16]] = unsafe { transmute_slice(column.secondary_data) };
        let page = array::array_to_page(
            primitive_type.clone(),
            dim,
            &aux[chunk.lower_bound..chunk.upper_bound],
            column.primary_data,
            chunk.adjusted_column_top,
            options,
            encoding,
        )?;
        pages.push(page);
    }
    Ok(pages)
}

fn array_primitive_type(parquet_type: &ParquetType) -> Option<PrimitiveType> {
    let mut cur_type = parquet_type;
    let mut primitive_type = None;
    loop {
        match cur_type {
            ParquetType::PrimitiveType(t) => {
                primitive_type = Some(t);
                break;
            }
            ParquetType::GroupType { fields, .. } => {
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

// Mark imports as intentionally available even though they are only used via
// fully-qualified paths above (the unused warnings would otherwise fire).
const _: () = {
    let _ = std::mem::size_of::<Decimal8>();
    let _ = std::mem::size_of::<Decimal16>();
    let _ = std::mem::size_of::<Decimal32>();
    let _ = std::mem::size_of::<Decimal64>();
    let _ = std::mem::size_of::<Decimal128>();
    let _ = std::mem::size_of::<Decimal256>();
    let _ = std::mem::size_of::<IPv4>();
    let _ = std::mem::size_of::<GeoByte>();
    let _ = std::mem::size_of::<GeoShort>();
    let _ = std::mem::size_of::<GeoInt>();
    let _ = std::mem::size_of::<GeoLong>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet_write::schema::column_type_to_parquet_type;
    use crate::parquet_write::tests::make_column_with_top;
    use parquet2::compression::CompressionOptions;
    use parquet2::write::Version;
    use qdb_core::col_type::ColumnType;

    fn write_options() -> WriteOptions {
        WriteOptions {
            write_statistics: true,
            version: Version::V2,
            compression: CompressionOptions::Uncompressed,
            row_group_size: None,
            data_page_size: None,
            raw_array_encoding: false,
            bloom_filter_fpp: 0.01,
            min_compression_ratio: 0.0,
        }
    }

    /// Build a Column for the given tag with 100 minimal-but-valid rows.
    /// Returns the Column plus a Box owning the underlying Vec, which the
    /// caller must keep alive for the duration of the Column.
    fn build_column(tag: ColumnTypeTag) -> (Column, Box<dyn std::any::Any>) {
        match tag {
            ColumnTypeTag::Boolean => {
                let data: Vec<u8> = (0..100u8).map(|i| i % 2).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Byte => {
                let data: Vec<i8> = (0..100i8).map(|i| i.wrapping_add(1)).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Short => {
                let data: Vec<i16> = (0..100i16).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Char => {
                let data: Vec<u16> = (0..100u16).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Int => {
                let data: Vec<i32> = (0..100i32).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Long | ColumnTypeTag::Date | ColumnTypeTag::Timestamp => {
                let data: Vec<i64> = (0..100i64).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Float => {
                let data: Vec<f32> = (0..100).map(|i| i as f32).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Double => {
                let data: Vec<f64> = (0..100).map(|i| i as f64).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::IPv4 => {
                // Avoid the null sentinel (0).
                let data: Vec<i32> = (1..=100i32).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::GeoByte => {
                let data: Vec<i8> = (0..100i8).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::GeoShort => {
                let data: Vec<i16> = (0..100i16).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::GeoInt => {
                let data: Vec<i32> = (0..100i32).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::GeoLong => {
                let data: Vec<i64> = (0..100i64).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Long128 | ColumnTypeTag::Uuid => {
                let data: Vec<[u8; 16]> = (1u128..=100u128).map(|v| v.to_le_bytes()).collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            ColumnTypeTag::Long256 => {
                let data: Vec<[u8; 32]> = (1u128..=100u128)
                    .map(|v| {
                        let mut x = [0u8; 32];
                        x[..16].copy_from_slice(&v.to_le_bytes());
                        x
                    })
                    .collect();
                let col = make_column_with_top("col", tag, &data, 0, 0);
                (col, Box::new(data))
            }
            _ => panic!("unsupported tag in build_column: {:?}", tag),
        }
    }

    fn build_decimal_column(precision: u8, scale: u8) -> (Column, Box<dyn std::any::Any>) {
        // Use Decimal256 (largest width); it works for any precision/scale.
        let column_type = ColumnType::new_decimal(precision, scale).expect("decimal");
        let data: Vec<Decimal256> = (1..=100i64).map(|v| Decimal256(v, 0, 0, 0)).collect();
        let col = Column::from_raw_data(
            0,
            "col",
            column_type.code(),
            0,
            data.len(),
            data.as_ptr() as *const u8,
            data.len() * std::mem::size_of::<Decimal256>(),
            std::ptr::null(),
            0,
            std::ptr::null(),
            0,
            false,
            false,
            0,
        )
        .unwrap();
        (col, Box::new(data))
    }

    fn parquet_type_for(tag: ColumnTypeTag) -> ParquetType {
        let column_type = ColumnType::new(tag, 0);
        column_type_to_parquet_type(0, "col", column_type, false, false).expect("type")
    }

    /// Verify the dispatch supports a given (encoding, tag) pair: build a
    /// 100-row column, call `encode_column_chunk`, and assert it returns at
    /// least one page without erroring.
    fn assert_dispatch_supported(encoding: Encoding, tag: ColumnTypeTag) {
        let parquet_type = parquet_type_for(tag);
        let (col, _own) = build_column(tag);
        let pages = encode_column_chunk(
            encoding,
            &parquet_type,
            &[col],
            0,
            100,
            write_options(),
            None,
        )
        .unwrap_or_else(|e| panic!("dispatch ({:?}, {:?}) failed: {:?}", encoding, tag, e));
        assert!(
            !pages.is_empty(),
            "dispatch ({:?}, {:?}) returned no pages",
            encoding,
            tag
        );
    }

    fn assert_dispatch_decimal_supported(encoding: Encoding, precision: u8, scale: u8) {
        let column_type = ColumnType::new_decimal(precision, scale).expect("decimal");
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        let (col, _own) = build_decimal_column(precision, scale);
        let pages = encode_column_chunk(
            encoding,
            &parquet_type,
            &[col],
            0,
            100,
            write_options(),
            None,
        )
        .unwrap_or_else(|e| {
            panic!(
                "dispatch ({:?}, decimal({}, {})) failed: {:?}",
                encoding, precision, scale, e
            )
        });
        assert!(!pages.is_empty());
    }

    #[test]
    fn encode_column_chunk_dispatch_table() {
        // (encoding, column_tag) pairs the dispatch supports. Each entry
        // exercises one match arm in encode.rs's per-physical-type dispatchers.
        let cases: &[(Encoding, ColumnTypeTag)] = &[
            // Plain
            (Encoding::Plain, ColumnTypeTag::Boolean),
            (Encoding::Plain, ColumnTypeTag::Byte),
            (Encoding::Plain, ColumnTypeTag::Short),
            (Encoding::Plain, ColumnTypeTag::Char),
            (Encoding::Plain, ColumnTypeTag::Int),
            (Encoding::Plain, ColumnTypeTag::Long),
            (Encoding::Plain, ColumnTypeTag::Date),
            (Encoding::Plain, ColumnTypeTag::Timestamp),
            (Encoding::Plain, ColumnTypeTag::Float),
            (Encoding::Plain, ColumnTypeTag::Double),
            (Encoding::Plain, ColumnTypeTag::IPv4),
            (Encoding::Plain, ColumnTypeTag::GeoByte),
            (Encoding::Plain, ColumnTypeTag::GeoShort),
            (Encoding::Plain, ColumnTypeTag::GeoInt),
            (Encoding::Plain, ColumnTypeTag::GeoLong),
            (Encoding::Plain, ColumnTypeTag::Long128),
            (Encoding::Plain, ColumnTypeTag::Uuid),
            (Encoding::Plain, ColumnTypeTag::Long256),
            // DeltaBinaryPacked
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Byte),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Short),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Char),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Int),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Long),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Date),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Timestamp),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::IPv4),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::GeoByte),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::GeoShort),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::GeoInt),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::GeoLong),
            // RleDictionary
            (Encoding::RleDictionary, ColumnTypeTag::Byte),
            (Encoding::RleDictionary, ColumnTypeTag::Short),
            (Encoding::RleDictionary, ColumnTypeTag::Char),
            (Encoding::RleDictionary, ColumnTypeTag::Int),
            (Encoding::RleDictionary, ColumnTypeTag::Long),
            (Encoding::RleDictionary, ColumnTypeTag::Date),
            (Encoding::RleDictionary, ColumnTypeTag::Float),
            (Encoding::RleDictionary, ColumnTypeTag::Double),
            (Encoding::RleDictionary, ColumnTypeTag::IPv4),
            (Encoding::RleDictionary, ColumnTypeTag::GeoByte),
            (Encoding::RleDictionary, ColumnTypeTag::GeoShort),
            (Encoding::RleDictionary, ColumnTypeTag::GeoInt),
            (Encoding::RleDictionary, ColumnTypeTag::GeoLong),
            (Encoding::RleDictionary, ColumnTypeTag::Long128),
            (Encoding::RleDictionary, ColumnTypeTag::Uuid),
            (Encoding::RleDictionary, ColumnTypeTag::Long256),
        ];
        for &(encoding, tag) in cases {
            assert_dispatch_supported(encoding, tag);
        }

        // Decimal arms (parameterised separately because they need precision/scale).
        for &(precision, scale) in &[(2u8, 0u8), (4, 1), (9, 2), (18, 3), (30, 4), (60, 6)] {
            assert_dispatch_decimal_supported(Encoding::Plain, precision, scale);
            assert_dispatch_decimal_supported(Encoding::RleDictionary, precision, scale);
        }
    }

    #[test]
    fn encode_column_chunk_unsupported_combinations_error() {
        // Combinations that should return Err(Unsupported).
        // For each, build a representative column and verify the dispatch
        // rejects it.
        let cases: &[(Encoding, ColumnTypeTag)] = &[
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Boolean),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Float),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Double),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Long128),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Uuid),
            (Encoding::DeltaBinaryPacked, ColumnTypeTag::Long256),
            (Encoding::RleDictionary, ColumnTypeTag::Boolean),
            (Encoding::DeltaLengthByteArray, ColumnTypeTag::Int),
            (Encoding::DeltaLengthByteArray, ColumnTypeTag::Long),
            (Encoding::DeltaLengthByteArray, ColumnTypeTag::Boolean),
        ];
        for &(encoding, tag) in cases {
            let parquet_type = parquet_type_for(tag);
            let (col, _own) = build_column(tag);
            let result = encode_column_chunk(
                encoding,
                &parquet_type,
                &[col],
                0,
                100,
                write_options(),
                None,
            );
            assert!(
                result.is_err(),
                "expected Err for ({:?}, {:?}) but got Ok",
                encoding,
                tag
            );
        }
    }
}
