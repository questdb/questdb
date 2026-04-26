//! Numeric primitive encoders, organized around the `SimdEncodable` trait.
//!
//! This file holds the leaf-level page encoders for SIMD-encodable types
//! (i32, i64, f32, f64, plus widening helpers for Byte/Short/Char/IPv4/Geo*)
//! and the matching decimal encoders. Both the Plain and DeltaBinaryPacked
//! encoding paths route through here — `slice_to_page_simd` /
//! `int_slice_to_page_*` take an `Encoding` parameter and dispatch internally.

use std::collections::HashSet;
use std::fmt::Debug;

use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::util::{
    build_plain_page, encode_primitive_def_levels, ExactSizedIter, MaxMin,
};
use crate::parquet_write::Nullable;
use parquet2::bloom_filter::hash_native;
use parquet2::encoding::delta_bitpacked::{encode, encode_i32};
use parquet2::encoding::Encoding;
use parquet2::page::{DataPage, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::schema::Repetition;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;
use qdb_core::col_type::nulls;

pub fn int_slice_to_page_nullable<T, P, const UNSIGNED_STATS: bool>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
    MaxMin<P>: StatsUpdater<P, UNSIGNED_STATS>,
{
    match encoding {
        Encoding::Plain => slice_to_page_nullable_impl::<_, P, UNSIGNED_STATS, _>(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_plain_nullable,
            bloom_hashes,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_nullable_impl::<_, P, UNSIGNED_STATS, _>(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_nullable,
            bloom_hashes,
        ),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} while writing an int column"
            ))
        }
    }
    .map(Page::Data)
}

pub trait StatsUpdater<T, const UNSIGNED: bool> {
    fn update_stats(&mut self, v: T);
}

impl StatsUpdater<i32, false> for MaxMin<i32> {
    #[inline]
    fn update_stats(&mut self, v: i32) {
        self.update(v);
    }
}

impl StatsUpdater<i32, true> for MaxMin<i32> {
    #[inline]
    fn update_stats(&mut self, v: i32) {
        self.update_unsigned(v);
    }
}

impl<const UNSIGNED: bool> StatsUpdater<i64, UNSIGNED> for MaxMin<i64> {
    #[inline]
    fn update_stats(&mut self, v: i64) {
        self.update(v);
    }
}

fn slice_to_page_nullable_impl<T, P, const UNSIGNED_STATS: bool, F>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
    mut bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P> + Debug,
    F: Fn(&[T], usize, Vec<u8>) -> Vec<u8>,
    MaxMin<P>: StatsUpdater<P, UNSIGNED_STATS>,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();
    let mut null_count = 0;
    let write_stats = options.write_statistics;
    let mut statistics: MaxMin<P> = MaxMin::new();

    let def_levels_iter = (0..num_rows).map(|i| {
        if i < column_top {
            false
        } else {
            let value = slice[i - column_top];
            if value.is_null() {
                null_count += 1;
                false
            } else {
                let v: P = value.as_();
                if write_stats {
                    statistics.update_stats(v);
                }
                if let Some(ref mut h) = bloom_hashes {
                    h.insert(hash_native(v));
                }
                true
            }
        }
    });
    let mut buffer = vec![];
    encode_primitive_def_levels(&mut buffer, def_levels_iter, num_rows, options.version)?;

    let definition_levels_byte_length = buffer.len();
    let buffer = encode_fn(slice, null_count, buffer);

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some((column_top + null_count) as i64),
            statistics,
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top + null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        encoding,
        false,
    )
}

pub fn int_slice_to_page_notnull<T, P>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    match encoding {
        Encoding::Plain => slice_to_page_notnull(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_plain_notnull,
            bloom_hashes,
        ),
        Encoding::DeltaBinaryPacked => slice_to_page_notnull(
            slice,
            column_top,
            options,
            primitive_type,
            encoding,
            encode_delta_notnull,
            bloom_hashes,
        ),
        other => {
            return Err(fmt_err!(
                Unsupported,
                "unsupported encoding {other:?} while writing an int column"
            ))
        }
    }
    .map(Page::Data)
}

fn slice_to_page_notnull<T, P, F: Fn(&[T], usize) -> Vec<u8>>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    encode_fn: F,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<DataPage>
where
    P: NativeType,
    T: Default + num_traits::AsPrimitive<P> + Debug,
{
    assert_eq!(primitive_type.field_info.repetition, Repetition::Required);

    let statistics = match (options.write_statistics, bloom_hashes) {
        (true, Some(h)) => {
            let mut statistics = MaxMin::new();
            for value in slice {
                let v: P = value.as_();
                statistics.update(v);
                h.insert(hash_native(v));
            }
            Some(build_statistics(
                Some(column_top as i64),
                statistics,
                primitive_type.clone(),
            ))
        }
        (true, None) => {
            let mut statistics = MaxMin::new();
            for value in slice {
                statistics.update(value.as_());
            }
            Some(build_statistics(
                Some(column_top as i64),
                statistics,
                primitive_type.clone(),
            ))
        }
        (false, Some(h)) => {
            for value in slice {
                h.insert(hash_native(value.as_()));
            }
            None
        }
        (false, None) => None,
    };

    build_plain_page(
        encode_fn(slice, column_top),
        column_top + slice.len(),
        column_top,
        0,
        statistics,
        primitive_type,
        options,
        encoding,
        true,
    )
}

fn encode_plain_notnull<T, P>(slice: &[T], column_top: usize) -> Vec<u8>
where
    P: NativeType,
    T: Default + num_traits::AsPrimitive<P>,
{
    let mut buffer = Vec::with_capacity(size_of::<P>() * (column_top + slice.len()));
    for i in 0..column_top + slice.len() {
        let x = if i < column_top {
            T::default()
        } else {
            slice[i - column_top]
        };
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_bytes().as_ref())
    }
    buffer
}

fn encode_delta_notnull<T, P>(slice: &[T], column_top: usize) -> Vec<u8>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Default + num_traits::AsPrimitive<P>,
{
    let iterator = (0..column_top + slice.len()).map(|i| {
        let x = if i < column_top {
            T::default()
        } else {
            slice[i - column_top]
        };
        let parquet_native: P = x.as_();
        let integer: i64 = parquet_native.as_();
        integer
    });
    let mut buffer = vec![];
    if size_of::<P>() <= 4 {
        encode_i32(iterator, &mut buffer);
    } else {
        encode(iterator, &mut buffer);
    }
    buffer
}

fn encode_plain_nullable<T, P>(slice: &[T], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8>
where
    P: NativeType,
    T: Nullable + num_traits::AsPrimitive<P>,
{
    buffer.reserve(size_of::<P>() * (slice.len() - null_count));
    for x in slice.iter().filter(|x| !x.is_null()) {
        let parquet_native: P = x.as_();
        buffer.extend_from_slice(parquet_native.to_bytes().as_ref())
    }
    buffer
}

fn encode_delta_nullable<T, P>(slice: &[T], null_count: usize, mut buffer: Vec<u8>) -> Vec<u8>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    T: Nullable + num_traits::AsPrimitive<P>,
{
    let iterator = slice.iter().filter(|x| !x.is_null()).map(|x| {
        let parquet_native: P = x.as_();
        let integer: i64 = parquet_native.as_();
        integer
    });
    let iterator = ExactSizedIter::new(iterator, slice.len() - null_count);
    if size_of::<P>() <= 4 {
        encode_i32(iterator, &mut buffer);
    } else {
        encode(iterator, &mut buffer);
    }
    buffer
}

pub(crate) fn build_statistics<P: NativeType>(
    null_count: Option<i64>,
    statistics: MaxMin<P>,
    primitive_type: PrimitiveType,
) -> ParquetStatistics {
    let statistics = &PrimitiveStatistics::<P> {
        primitive_type,
        null_count,
        distinct_count: None,
        max_value: statistics.max,
        min_value: statistics.min,
    } as &dyn parquet2::statistics::Statistics;
    serialize_statistics(statistics)
}

use crate::parquet_write::simd::{
    encode_f32_def_levels, encode_f64_def_levels, encode_i32_def_levels, encode_i64_def_levels,
    DefLevelResult,
};

/// Trait for types that can be SIMD-encoded in Parquet pages.
pub trait SimdEncodable: NativeType {
    /// Encode definition levels using SIMD, returns null count and optional stats.
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
        bloom_hashes: Option<&mut HashSet<u64>>,
    ) -> std::io::Result<DefLevelResult<Self>>;

    /// Check if a value represents null.
    fn is_null(&self) -> bool;

    /// Encode data with delta encoding. Override for types that support it.
    fn encode_delta(_slice: &[Self], _non_null_count: usize, _buffer: &mut Vec<u8>) -> bool {
        false // Not supported by default
    }

    /// Encode data values, dispatching to Plain or Delta based on encoding.
    fn encode_data(
        slice: &[Self],
        null_count: usize,
        encoding: Encoding,
        mut buffer: Vec<u8>,
    ) -> ParquetResult<Vec<u8>> {
        let non_null_count = slice.len() - null_count;
        if non_null_count == 0 {
            return Ok(buffer);
        }

        match encoding {
            Encoding::Plain => {
                buffer.reserve(size_of::<Self>() * non_null_count);
                if null_count == 0 {
                    // Fast path: no nulls, memcpy entire slice
                    // SAFETY: Reinterprets a contiguous `&[T]` as raw bytes. Valid because slices are
                    // contiguous and `u8` has no alignment requirement.
                    let bytes = unsafe {
                        std::slice::from_raw_parts(slice.as_ptr() as *const u8, size_of_val(slice))
                    };
                    buffer.extend_from_slice(bytes);
                } else {
                    // Slow path: filter out nulls
                    for x in slice.iter().filter(|x| !x.is_null()) {
                        buffer.extend_from_slice(x.to_bytes().as_ref());
                    }
                }
                Ok(buffer)
            }
            Encoding::DeltaBinaryPacked => {
                if Self::encode_delta(slice, non_null_count, &mut buffer) {
                    Ok(buffer)
                } else {
                    Err(fmt_err!(
                        Unsupported,
                        "delta encoding not supported for this type"
                    ))
                }
            }
            other => Err(fmt_err!(Unsupported, "unsupported encoding {other:?}")),
        }
    }

    fn min() -> Self;

    fn max() -> Self;
}

impl SimdEncodable for i64 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
        bloom_hashes: Option<&mut HashSet<u64>>,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_i64_def_levels(buffer, slice, column_top, compute_stats, bloom_hashes)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        *self == nulls::LONG
    }

    fn encode_delta(slice: &[Self], non_null_count: usize, buffer: &mut Vec<u8>) -> bool {
        let iterator = slice.iter().filter(|&&x| x != nulls::LONG).copied();
        let iterator = ExactSizedIter::new(iterator, non_null_count);
        encode(iterator, buffer);
        true
    }

    fn min() -> Self {
        i64::MIN
    }

    fn max() -> Self {
        i64::MAX
    }
}

impl SimdEncodable for i32 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
        bloom_hashes: Option<&mut HashSet<u64>>,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_i32_def_levels(buffer, slice, column_top, compute_stats, bloom_hashes)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        *self == nulls::INT
    }

    fn encode_delta(slice: &[Self], non_null_count: usize, buffer: &mut Vec<u8>) -> bool {
        let iterator = slice
            .iter()
            .filter(|&&x| x != nulls::INT)
            .map(|&x| x as i64);
        let iterator = ExactSizedIter::new(iterator, non_null_count);
        encode_i32(iterator, buffer);
        true
    }

    fn min() -> Self {
        i32::MIN
    }

    fn max() -> Self {
        i32::MAX
    }
}

impl SimdEncodable for f64 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
        bloom_hashes: Option<&mut HashSet<u64>>,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_f64_def_levels(buffer, slice, column_top, compute_stats, bloom_hashes)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        self.is_nan()
    }

    fn min() -> Self {
        f64::MIN
    }

    fn max() -> Self {
        f64::MAX
    }
}

impl SimdEncodable for f32 {
    fn encode_def_levels(
        buffer: &mut Vec<u8>,
        slice: &[Self],
        column_top: usize,
        compute_stats: bool,
        bloom_hashes: Option<&mut HashSet<u64>>,
    ) -> std::io::Result<DefLevelResult<Self>> {
        encode_f32_def_levels(buffer, slice, column_top, compute_stats, bloom_hashes)
    }

    #[inline(always)]
    fn is_null(&self) -> bool {
        self.is_nan()
    }

    fn min() -> Self {
        f32::MIN
    }

    fn max() -> Self {
        f32::MAX
    }
}

/// Generic SIMD-optimized page encoder for nullable columns.
pub fn slice_to_page_simd<T: SimdEncodable>(
    slice: &[T],
    column_top: usize,
    options: WriteOptions,
    primitive_type: PrimitiveType,
    encoding: Encoding,
    bloom_hashes: Option<&mut HashSet<u64>>,
) -> ParquetResult<Page> {
    assert_eq!(primitive_type.field_info.repetition, Repetition::Optional);
    let num_rows = column_top + slice.len();

    let mut buffer = vec![];

    // For V1, write a 4-byte length prefix placeholder
    let def_levels_start = if matches!(options.version, parquet2::write::Version::V1) {
        buffer.extend_from_slice(&[0; 4]);
        4
    } else {
        0
    };

    let result = T::encode_def_levels(
        &mut buffer,
        slice,
        column_top,
        options.write_statistics,
        bloom_hashes,
    )
    .map_err(|e| {
        fmt_err!(
            Io(std::sync::Arc::new(e)),
            "failed to encode definition levels"
        )
    })?;

    // For V1, write the definition levels length
    if matches!(options.version, parquet2::write::Version::V1) {
        let def_levels_len = (buffer.len() - def_levels_start) as i32;
        buffer[0..4].copy_from_slice(&def_levels_len.to_le_bytes());
    }

    let definition_levels_byte_length = buffer.len();

    let buffer = T::encode_data(slice, result.null_count, encoding, buffer)?;

    let statistics = if options.write_statistics {
        Some(build_statistics(
            Some((column_top + result.null_count) as i64),
            MaxMin { max: result.max, min: result.min },
            primitive_type.clone(),
        ))
    } else {
        None
    };

    build_plain_page(
        buffer,
        num_rows,
        column_top + result.null_count,
        definition_levels_byte_length,
        statistics,
        primitive_type,
        options,
        encoding,
        false,
    )
    .map(Page::Data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parquet::error::ParquetErrorReason;
    use crate::parquet_write::schema::column_type_to_parquet_type;
    use parquet2::compression::CompressionOptions;
    use parquet2::page::DataPageHeader;
    use parquet2::schema::types::{ParquetType, PrimitiveType};
    use parquet2::write::Version;
    use qdb_core::col_type::{ColumnType, ColumnTypeTag};

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

    fn optional_type_for(tag: ColumnTypeTag) -> PrimitiveType {
        let column_type = ColumnType::new(tag, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, false, false).expect("type");
        match parquet_type {
            ParquetType::PrimitiveType(pt) => pt,
            _ => panic!("expected primitive type"),
        }
    }

    fn required_type_for(tag: ColumnTypeTag) -> PrimitiveType {
        let column_type = ColumnType::new(tag, 0);
        let parquet_type =
            column_type_to_parquet_type(0, "col", column_type, true, false).expect("type");
        match parquet_type {
            ParquetType::PrimitiveType(pt) => pt,
            _ => panic!("expected primitive type"),
        }
    }

    fn v2_header(page: &Page) -> (i32, i32, i32) {
        match page {
            Page::Data(d) => match &d.header {
                DataPageHeader::V2(h) => (h.num_values, h.num_nulls, h.encoding.0),
                DataPageHeader::V1(_) => panic!("expected V2 header"),
            },
            _ => panic!("expected data page"),
        }
    }

    #[test]
    fn int_slice_to_page_nullable_plain() {
        use crate::parquet_write::IPv4;
        // IPv4 null sentinel is 0
        let data: Vec<IPv4> = vec![IPv4(1), IPv4(0), IPv4(3)];
        let pt = optional_type_for(ColumnTypeTag::IPv4);
        let page = int_slice_to_page_nullable::<IPv4, i32, true>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::Plain,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn int_slice_to_page_nullable_delta() {
        use crate::parquet_write::IPv4;
        let data: Vec<IPv4> = vec![IPv4(1), IPv4(0), IPv4(3)];
        let pt = optional_type_for(ColumnTypeTag::IPv4);
        let page = int_slice_to_page_nullable::<IPv4, i32, true>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::DeltaBinaryPacked,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn int_slice_to_page_nullable_unsupported_encoding() {
        use crate::parquet_write::GeoByte;
        let data: Vec<GeoByte> = vec![GeoByte(1)];
        let pt = optional_type_for(ColumnTypeTag::GeoByte);
        let err = int_slice_to_page_nullable::<GeoByte, i32, false>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::RleDictionary,
            None,
        )
        .expect_err("expected error");
        assert!(matches!(err.reason(), ParquetErrorReason::Unsupported));
    }

    #[test]
    fn int_slice_to_page_nullable_no_stats() {
        use crate::parquet_write::GeoByte;
        let data: Vec<GeoByte> = vec![GeoByte(1), GeoByte(2)];
        let pt = optional_type_for(ColumnTypeTag::GeoByte);
        let opts = WriteOptions { write_statistics: false, ..write_options() };
        let page = int_slice_to_page_nullable::<GeoByte, i32, false>(
            &data,
            0,
            opts,
            pt,
            Encoding::Plain,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 2);
        assert_eq!(num_nulls, 0);
    }

    #[test]
    fn int_slice_to_page_notnull_plain() {
        let data: Vec<i8> = vec![1, 2, 3, 4, 5];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let page = int_slice_to_page_notnull::<i8, i32>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::Plain,
            None,
        )
        .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 5);
    }

    #[test]
    fn int_slice_to_page_notnull_delta() {
        let data: Vec<i8> = vec![1, 2, 3];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let page = int_slice_to_page_notnull::<i8, i32>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::DeltaBinaryPacked,
            None,
        )
        .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 3);
    }

    #[test]
    fn int_slice_to_page_notnull_unsupported_encoding() {
        let data: Vec<i8> = vec![1];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let err = int_slice_to_page_notnull::<i8, i32>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::RleDictionary,
            None,
        )
        .expect_err("expected error");
        assert!(matches!(err.reason(), ParquetErrorReason::Unsupported));
    }

    #[test]
    fn int_slice_to_page_notnull_with_bloom_and_stats() {
        let data: Vec<i8> = vec![1, 2, 3];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let mut bloom = HashSet::new();
        let page = int_slice_to_page_notnull::<i8, i32>(
            &data,
            0,
            write_options(),
            pt,
            Encoding::Plain,
            Some(&mut bloom),
        )
        .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 3);
        assert_eq!(bloom.len(), 3);
    }

    #[test]
    fn int_slice_to_page_notnull_bloom_no_stats() {
        let data: Vec<i8> = vec![1, 2];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let opts = WriteOptions { write_statistics: false, ..write_options() };
        let mut bloom = HashSet::new();
        let page = int_slice_to_page_notnull::<i8, i32>(
            &data,
            0,
            opts,
            pt,
            Encoding::Plain,
            Some(&mut bloom),
        )
        .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 2);
        assert_eq!(bloom.len(), 2);
    }

    #[test]
    fn int_slice_to_page_notnull_no_bloom_no_stats() {
        let data: Vec<i8> = vec![1, 2];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let opts = WriteOptions { write_statistics: false, ..write_options() };
        let page = int_slice_to_page_notnull::<i8, i32>(&data, 0, opts, pt, Encoding::Plain, None)
            .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 2);
    }

    #[test]
    fn int_slice_to_page_notnull_with_column_top() {
        let data: Vec<i8> = vec![1, 2];
        let pt = required_type_for(ColumnTypeTag::Byte);
        let page = int_slice_to_page_notnull::<i8, i32>(
            &data,
            3,
            write_options(),
            pt,
            Encoding::Plain,
            None,
        )
        .expect("encode");
        let (num_values, _, _) = v2_header(&page);
        assert_eq!(num_values, 5);
    }

    #[test]
    fn slice_to_page_simd_plain_i64() {
        let data: Vec<i64> = vec![1, 2, i64::MIN, 4];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let page = slice_to_page_simd(&data, 0, write_options(), pt, Encoding::Plain, None)
            .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn slice_to_page_simd_delta_i64() {
        let data: Vec<i64> = vec![1, 2, i64::MIN, 4];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let page = slice_to_page_simd(
            &data,
            0,
            write_options(),
            pt,
            Encoding::DeltaBinaryPacked,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn slice_to_page_simd_delta_i32() {
        let data: Vec<i32> = vec![1, 2, i32::MIN, 4];
        let pt = optional_type_for(ColumnTypeTag::Int);
        let page = slice_to_page_simd(
            &data,
            0,
            write_options(),
            pt,
            Encoding::DeltaBinaryPacked,
            None,
        )
        .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn slice_to_page_simd_delta_f64_unsupported() {
        let data: Vec<f64> = vec![1.0];
        let pt = optional_type_for(ColumnTypeTag::Double);
        let err = slice_to_page_simd(
            &data,
            0,
            write_options(),
            pt,
            Encoding::DeltaBinaryPacked,
            None,
        )
        .expect_err("expected error");
        assert!(matches!(err.reason(), ParquetErrorReason::Unsupported));
    }

    #[test]
    fn slice_to_page_simd_unsupported_encoding() {
        let data: Vec<i64> = vec![1];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let err = slice_to_page_simd(&data, 0, write_options(), pt, Encoding::RleDictionary, None)
            .expect_err("expected error");
        assert!(matches!(err.reason(), ParquetErrorReason::Unsupported));
    }

    #[test]
    fn slice_to_page_simd_no_stats() {
        let data: Vec<i64> = vec![1, 2, 3];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let opts = WriteOptions { write_statistics: false, ..write_options() };
        let page = slice_to_page_simd(&data, 0, opts, pt, Encoding::Plain, None).expect("ok");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 3);
        assert_eq!(num_nulls, 0);
    }

    #[test]
    fn slice_to_page_simd_f32_round_trip() {
        // Exercises f32 SimdEncodable impl including min/max.
        let data: Vec<f32> = vec![1.0, f32::NAN, 3.0, -1.5];
        let pt = optional_type_for(ColumnTypeTag::Float);
        let page = slice_to_page_simd(&data, 0, write_options(), pt, Encoding::Plain, None)
            .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 4);
        assert_eq!(num_nulls, 1);
    }

    #[test]
    fn slice_to_page_simd_with_column_top() {
        let data: Vec<i64> = vec![1, 2, 3];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let page = slice_to_page_simd(&data, 5, write_options(), pt, Encoding::Plain, None)
            .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 8);
        assert_eq!(num_nulls, 5);
    }

    #[test]
    fn slice_to_page_simd_all_nulls() {
        let data: Vec<i64> = vec![i64::MIN; 5];
        let pt = optional_type_for(ColumnTypeTag::Long);
        let page = slice_to_page_simd(&data, 0, write_options(), pt, Encoding::Plain, None)
            .expect("encode");
        let (num_values, num_nulls, _) = v2_header(&page);
        assert_eq!(num_values, 5);
        assert_eq!(num_nulls, 5);
    }
}
