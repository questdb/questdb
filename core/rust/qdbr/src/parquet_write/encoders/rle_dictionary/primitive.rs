use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use parquet2::schema::types::PrimitiveType;
use parquet2::types::NativeType;

use crate::parquet::error::ParquetResult;
use crate::parquet_write::file::WriteOptions;
use crate::parquet_write::schema::Column;
use crate::parquet_write::util::transmute_slice;
use crate::parquet_write::Nullable;

use super::{encode_primitive, Repetition};

/// Encode a SIMD-encodable primitive type (Int, Long, Float, Double, Date,
/// Timestamp) as RleDictionary pages: 1 DictPage + 1 DataPage.
pub fn encode_simd<T>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<parquet2::page::Page>>
where
    T: NativeType + Nullable + Copy + Debug,
    T::Bytes: Eq + Hash,
{
    encode_primitive::<T, T, _, _>(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
        Repetition::Optional,
        None,
        |column| -> ParquetResult<&[T]> {
            // SAFETY: Data originates from JNI/Java memory-mapped column data, which is
            // page-aligned. The byte content represents valid `T` values.
            Ok(unsafe { transmute_slice(column.primary_data) })
        },
        |value| value.is_null(),
        |value| value,
    )
}

/// Encode a notnull integer (Byte, Short, Char) as RleDictionary pages.
/// Column-top rows use `T::default()` (projected via the supplied function) as
/// the dict value.
pub fn encode_int_notnull<T, P>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<parquet2::page::Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    P::Bytes: Eq + Hash,
    T: Default + num_traits::AsPrimitive<P> + Copy + Debug,
{
    let column_top_default: P = T::default().as_();
    encode_primitive::<T, P, _, _>(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
        Repetition::Required,
        Some(column_top_default),
        |column| -> ParquetResult<&[T]> { Ok(unsafe { transmute_slice(column.primary_data) }) },
        |_value| false,
        |value| value.as_(),
    )
}

/// Encode a nullable integer (IPv4, GeoByte/Short/Int/Long) as RleDictionary
/// pages.
pub fn encode_int_nullable<T, P>(
    columns: &[Column],
    first_partition_start: usize,
    last_partition_end: usize,
    primitive_type: &PrimitiveType,
    options: WriteOptions,
    bloom_set: Option<Arc<Mutex<HashSet<u64>>>>,
) -> ParquetResult<Vec<parquet2::page::Page>>
where
    P: NativeType + num_traits::AsPrimitive<i64>,
    P::Bytes: Eq + Hash,
    T: Nullable + num_traits::AsPrimitive<P> + Copy + Debug,
{
    encode_primitive::<T, P, _, _>(
        columns,
        first_partition_start,
        last_partition_end,
        primitive_type,
        options,
        bloom_set,
        Repetition::Optional,
        None,
        |column| -> ParquetResult<&[T]> { Ok(unsafe { transmute_slice(column.primary_data) }) },
        |value| value.is_null(),
        |value| value.as_(),
    )
}
