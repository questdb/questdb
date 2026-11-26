use std::sync::Arc;

use parquet_format_safe::Statistics as ParquetStatistics;

use super::Statistics;
use crate::error::{Error, Result};
use crate::schema::types::{PhysicalType, PrimitiveType};
use crate::types;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveStatistics<T: types::NativeType> {
    pub primitive_type: PrimitiveType,
    pub null_count: Option<i64>,
    pub distinct_count: Option<i64>,
    pub min_value: Option<T>,
    pub max_value: Option<T>,
}

impl<T: types::NativeType> Statistics for PrimitiveStatistics<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn physical_type(&self) -> &PhysicalType {
        &T::TYPE
    }

    fn null_count(&self) -> Option<i64> {
        self.null_count
    }

    fn has_min_value(&self) -> bool {
        self.min_value.is_some()
    }

    fn has_max_value(&self) -> bool {
        self.max_value.is_some()
    }
}

pub fn read<T: types::NativeType>(
    v: &ParquetStatistics,
    primitive_type: PrimitiveType,
) -> Result<Arc<dyn Statistics>> {
    if let Some(ref v) = v.max_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::oos(
                "The max_value of statistics MUST be plain encoded",
            ));
        }
    };
    if let Some(ref v) = v.min_value {
        if v.len() != std::mem::size_of::<T>() {
            return Err(Error::oos(
                "The min_value of statistics MUST be plain encoded",
            ));
        }
    };

    Ok(Arc::new(PrimitiveStatistics::<T> {
        primitive_type,
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: v.max_value.as_ref().map(|x| types::decode(x)),
        min_value: v.min_value.as_ref().map(|x| types::decode(x)),
    }))
}

pub fn write<T: types::NativeType>(v: &PrimitiveStatistics<T>) -> ParquetStatistics {
    write_bounds(v, v.min_value, v.max_value)
}

/// Writes FLOAT statistics. parquet.thrift asks writers to write a zero min as -0.0 and a zero
/// max as +0.0, whichever zero the column holds, so that a reader that orders -0.0 before +0.0
/// never skips a page holding a zero.
pub fn write_f32(v: &PrimitiveStatistics<f32>) -> ParquetStatistics {
    write_bounds(
        v,
        v.min_value.map(|x| if x == 0.0 { -0.0 } else { x }),
        v.max_value.map(|x| if x == 0.0 { 0.0 } else { x }),
    )
}

/// Writes DOUBLE statistics, with the zero rule of [`write_f32`].
pub fn write_f64(v: &PrimitiveStatistics<f64>) -> ParquetStatistics {
    write_bounds(
        v,
        v.min_value.map(|x| if x == 0.0 { -0.0 } else { x }),
        v.max_value.map(|x| if x == 0.0 { 0.0 } else { x }),
    )
}

fn write_bounds<T: types::NativeType>(
    v: &PrimitiveStatistics<T>,
    min_value: Option<T>,
    max_value: Option<T>,
) -> ParquetStatistics {
    ParquetStatistics {
        null_count: v.null_count,
        distinct_count: v.distinct_count,
        max_value: max_value.map(|x| x.to_bytes().as_ref().to_vec()),
        min_value: min_value.map(|x| x.to_bytes().as_ref().to_vec()),
        min: None,
        max: None,
        is_max_value_exact: None,
        is_min_value_exact: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::statistics::serialize_statistics;

    fn serialize<T: types::NativeType>(min: Option<T>, max: Option<T>) -> ParquetStatistics {
        serialize_statistics(&PrimitiveStatistics::<T> {
            primitive_type: PrimitiveType::from_physical("c".to_string(), T::TYPE),
            null_count: Some(0),
            distinct_count: None,
            min_value: min,
            max_value: max,
        })
    }

    // (min, max) computed, then (min, max) as parquet.thrift asks writers to write them.
    const ZERO_CASES: [((f64, f64), (f64, f64)); 6] = [
        ((0.0, -0.0), (-0.0, 0.0)),
        ((-0.0, 0.0), (-0.0, 0.0)),
        ((0.0, 0.0), (-0.0, 0.0)),
        ((-0.0, -0.0), (-0.0, 0.0)),
        ((0.0, 1.0), (-0.0, 1.0)),
        ((-1.0, -0.0), (-1.0, 0.0)),
    ];

    #[test]
    fn double_zero_min_is_written_negative_and_zero_max_positive() {
        for ((min, max), (want_min, want_max)) in ZERO_CASES {
            let s = serialize(Some(min), Some(max));
            assert_eq!(
                s.min_value,
                Some(want_min.to_le_bytes().to_vec()),
                "min of ({min:?}, {max:?})"
            );
            assert_eq!(
                s.max_value,
                Some(want_max.to_le_bytes().to_vec()),
                "max of ({min:?}, {max:?})"
            );
        }
    }

    #[test]
    fn float_zero_min_is_written_negative_and_zero_max_positive() {
        for ((min, max), (want_min, want_max)) in ZERO_CASES {
            let (min, max) = (min as f32, max as f32);
            let (want_min, want_max) = (want_min as f32, want_max as f32);
            let s = serialize(Some(min), Some(max));
            assert_eq!(
                s.min_value,
                Some(want_min.to_le_bytes().to_vec()),
                "min of ({min:?}, {max:?})"
            );
            assert_eq!(
                s.max_value,
                Some(want_max.to_le_bytes().to_vec()),
                "max of ({min:?}, {max:?})"
            );
        }
    }

    #[test]
    fn nonzero_float_bounds_are_written_unchanged() {
        let doubles = [
            f64::NEG_INFINITY,
            f64::MIN,
            -f64::MIN_POSITIVE,
            -f64::from_bits(1),
            f64::from_bits(1),
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
        ];
        for v in doubles {
            let s = serialize(Some(v), Some(v));
            assert_eq!(s.min_value, Some(v.to_le_bytes().to_vec()), "min {v:?}");
            assert_eq!(s.max_value, Some(v.to_le_bytes().to_vec()), "max {v:?}");
        }
        let floats = [
            f32::NEG_INFINITY,
            f32::MIN,
            -f32::MIN_POSITIVE,
            -f32::from_bits(1),
            f32::from_bits(1),
            f32::MIN_POSITIVE,
            f32::MAX,
            f32::INFINITY,
        ];
        for v in floats {
            let s = serialize(Some(v), Some(v));
            assert_eq!(s.min_value, Some(v.to_le_bytes().to_vec()), "min {v:?}");
            assert_eq!(s.max_value, Some(v.to_le_bytes().to_vec()), "max {v:?}");
        }
    }

    #[test]
    fn absent_float_bounds_stay_absent() {
        let s = serialize::<f64>(None, None);
        assert_eq!((s.min_value, s.max_value), (None, None));
        let s = serialize::<f32>(None, None);
        assert_eq!((s.min_value, s.max_value), (None, None));
    }

    #[test]
    fn integer_zero_bounds_are_written_unchanged() {
        let s = serialize(Some(0i32), Some(0i32));
        assert_eq!(
            (s.min_value, s.max_value),
            (Some(vec![0; 4]), Some(vec![0; 4]))
        );
        let s = serialize(Some(0i64), Some(0i64));
        assert_eq!(
            (s.min_value, s.max_value),
            (Some(vec![0; 8]), Some(vec![0; 8]))
        );
    }
}
