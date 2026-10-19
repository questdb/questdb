use std::sync::Arc;

use crate::error::{Error, Result};
use crate::schema::types::{
    IntegerType, PhysicalType, PrimitiveConvertedType, PrimitiveLogicalType, PrimitiveType,
};
use crate::statistics::*;
use crate::types::NativeType;

#[inline]
fn reduce_single<T, F: Fn(T, T) -> T>(lhs: Option<T>, rhs: Option<T>, op: F) -> Option<T> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(x), None) => Some(x),
        (None, Some(x)) => Some(x),
        (Some(x), Some(y)) => Some(op(x, y)),
    }
}

#[inline]
fn reduce_vec8(
    lhs: Option<Vec<u8>>,
    rhs: &Option<Vec<u8>>,
    max: bool,
    ord: fn(Vec<u8>, Vec<u8>, bool) -> Vec<u8>,
) -> Option<Vec<u8>> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(x), None) => Some(x),
        (None, Some(x)) => Some(x.clone()),
        (Some(x), Some(y)) => Some(ord(x, y.clone(), max)),
    }
}

pub fn reduce(stats: &[&Option<Arc<dyn Statistics>>]) -> Result<Option<Arc<dyn Statistics>>> {
    if stats.is_empty() {
        return Ok(None);
    }
    let stats = stats
        .iter()
        .filter_map(|x| x.as_ref())
        .map(|x| x.as_ref())
        .collect::<Vec<&dyn Statistics>>();
    if stats.is_empty() {
        return Ok(None);
    };

    let same_type = stats
        .iter()
        .skip(1)
        .all(|x| x.physical_type() == stats[0].physical_type());
    if !same_type {
        return Err(Error::oos("The statistics do not have the same data_type"));
    };
    Ok(match stats[0].physical_type() {
        PhysicalType::Boolean => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_boolean(stats)))
        }
        PhysicalType::Int32 => Some(Arc::new(reduce_int_primitive::<i32>(&stats))),
        PhysicalType::Int64 => Some(Arc::new(reduce_int_primitive::<i64>(&stats))),
        PhysicalType::Float => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<f32, _, false>(stats)))
        }
        PhysicalType::Double => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_primitive::<f64, _, false>(stats)))
        }
        PhysicalType::ByteArray => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_binary(stats)))
        }
        PhysicalType::FixedLenByteArray(_) => {
            let stats = stats.iter().map(|x| x.as_any().downcast_ref().unwrap());
            Some(Arc::new(reduce_fix_len_binary(stats)))
        }
        _ => panic!("Unexpected physical type"),
    })
}

fn reduce_binary<'a, I: Iterator<Item = &'a BinaryStatistics>>(mut stats: I) -> BinaryStatistics {
    let initial = stats.next().unwrap().clone();
    // An opaque-Binary page whose max prefix is all 0xFF has no short upper bound, so
    // it emits max_value = None (with a present min) to mean "unbounded -- do not
    // prune". reduce_vec8 treats None as "no contribution", so a sibling page's
    // smaller bounded max would otherwise win and understate the chunk max. Carry the
    // sentinel up: if any contributing page omits its max, so does the chunk.
    let mut has_unbounded = has_unbounded_max(&initial);
    let mut reduced = stats.fold(initial, |mut acc, new| {
        has_unbounded |= has_unbounded_max(new);
        acc.min_value = reduce_vec8(acc.min_value, &new.min_value, false, ord_binary);
        acc.max_value = reduce_vec8(acc.max_value, &new.max_value, true, ord_binary);
        acc.null_count = reduce_single(acc.null_count, new.null_count, |x, y| x + y);
        acc.distinct_count = None;
        acc.is_min_value_exact = reduce_exact_flag(acc.is_min_value_exact, new.is_min_value_exact);
        acc.is_max_value_exact = reduce_exact_flag(acc.is_max_value_exact, new.is_max_value_exact);
        acc
    });
    if has_unbounded {
        reduced.max_value = None;
        reduced.is_max_value_exact = None;
    }
    reduced
}

/// A per-page binary stat that contributed a value (so it has a min) but no max: the
/// "unbounded max" sentinel for an opaque-Binary page whose all-0xFF prefix has no
/// short upper bound. An all-null page (no min, no max) is not one.
#[inline]
fn has_unbounded_max(stats: &BinaryStatistics) -> bool {
    stats.min_value.is_some() && stats.max_value.is_none()
}

fn reduce_exact_flag(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        _ => None,
    }
}

fn reduce_fix_len_binary<'a, I: Iterator<Item = &'a FixedLenStatistics>>(
    mut stats: I,
) -> FixedLenStatistics {
    let initial = stats.next().unwrap().clone();
    let ord = if is_signed_decimal_primitive_type(&initial.primitive_type) {
        ord_binary_signed
    } else {
        ord_binary
    };
    stats.fold(initial, |mut acc, new| {
        acc.min_value = reduce_vec8(acc.min_value, &new.min_value, false, ord);
        acc.max_value = reduce_vec8(acc.max_value, &new.max_value, true, ord);
        acc.null_count = reduce_single(acc.null_count, new.null_count, |x, y| x + y);
        acc.distinct_count = None;
        acc
    })
}

#[inline]
fn is_signed_decimal_primitive_type(primitive_type: &PrimitiveType) -> bool {
    matches!(
        primitive_type.logical_type,
        Some(PrimitiveLogicalType::Decimal(_, _))
    ) || matches!(
        primitive_type.converted_type,
        Some(PrimitiveConvertedType::Decimal(_, _))
    )
}

#[inline]
fn is_unsigned_integer_primitive_type(primitive_type: &PrimitiveType) -> bool {
    matches!(
        primitive_type.logical_type,
        Some(PrimitiveLogicalType::Integer(
            IntegerType::UInt8 | IntegerType::UInt16 | IntegerType::UInt32 | IntegerType::UInt64
        ))
    ) || matches!(
        primitive_type.converted_type,
        Some(
            PrimitiveConvertedType::Uint8
                | PrimitiveConvertedType::Uint16
                | PrimitiveConvertedType::Uint32
                | PrimitiveConvertedType::Uint64
        )
    )
}

// Orders two Binary/String values as unsigned bytes and returns the max (or the
// min). Vec<u8> ordering breaks a prefix tie by length ("ETH" < "ETHW"), which a
// byte loop stopping at the shorter operand misses -- understating a chunk's max
// or overstating its min when one page bound is a prefix of another's.
fn ord_binary(a: Vec<u8>, b: Vec<u8>, max: bool) -> Vec<u8> {
    match a.cmp(&b) {
        std::cmp::Ordering::Greater => {
            if max {
                a
            } else {
                b
            }
        }
        std::cmp::Ordering::Less => {
            if max {
                b
            } else {
                a
            }
        }
        std::cmp::Ordering::Equal => a,
    }
}

fn ord_binary_signed(a: Vec<u8>, b: Vec<u8>, max: bool) -> Vec<u8> {
    match cmp_signed_be_twos_complement(&a, &b) {
        std::cmp::Ordering::Greater => {
            if max {
                a
            } else {
                b
            }
        }
        std::cmp::Ordering::Less => {
            if max {
                b
            } else {
                a
            }
        }
        std::cmp::Ordering::Equal => a,
    }
}

#[inline]
fn cmp_signed_be_twos_complement(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    if let (Some(a0), Some(b0)) = (a.first(), b.first()) {
        let a_negative = (a0 & 0x80) != 0;
        let b_negative = (b0 & 0x80) != 0;
        if a_negative != b_negative {
            return if a_negative {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }
    }
    for (v1, v2) in a.iter().zip(b.iter()) {
        match v1.cmp(v2) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
    }
    a.len().cmp(&b.len())
}

fn reduce_boolean<'a, I: Iterator<Item = &'a BooleanStatistics>>(
    mut stats: I,
) -> BooleanStatistics {
    let initial = stats.next().unwrap().clone();
    stats.fold(initial, |mut acc, new| {
        acc.min_value = reduce_single(
            acc.min_value,
            new.min_value,
            |x, y| if x & !(y) { y } else { x },
        );
        acc.max_value = reduce_single(
            acc.max_value,
            new.max_value,
            |x, y| if x & !(y) { x } else { y },
        );
        acc.null_count = reduce_single(acc.null_count, new.null_count, |x, y| x + y);
        acc.distinct_count = None;
        acc
    })
}

/// Compares two native primitive values, reinterpreting them as unsigned when
/// `UNSIGNED` is set. Passing the choice as a const generic keeps each branch a
/// statically known comparison the fold can inline, unlike a runtime
/// function-pointer selection.
#[inline]
fn primitive_ord<T: NativeType, const UNSIGNED: bool>(x: &T, y: &T) -> std::cmp::Ordering {
    if UNSIGNED {
        x.ord_unsigned(y)
    } else {
        x.ord(y)
    }
}

fn reduce_primitive<'a, T, I, const UNSIGNED: bool>(mut stats: I) -> PrimitiveStatistics<T>
where
    T: NativeType,
    I: Iterator<Item = &'a PrimitiveStatistics<T>>,
{
    let initial = stats.next().unwrap().clone();
    stats.fold(initial, |mut acc, new| {
        acc.min_value = reduce_single(acc.min_value, new.min_value, |x, y| {
            std::cmp::min_by(x, y, primitive_ord::<T, UNSIGNED>)
        });
        acc.max_value = reduce_single(acc.max_value, new.max_value, |x, y| {
            std::cmp::max_by(x, y, primitive_ord::<T, UNSIGNED>)
        });
        acc.null_count = reduce_single(acc.null_count, new.null_count, |x, y| x + y);
        acc.distinct_count = None;
        acc
    })
}

/// Reduces Int32/Int64 chunk statistics, choosing signed or unsigned ordering
/// from the column's logical type. The decision selects a const-generic
/// monomorphization so the comparison inlines, mirroring the per-page
/// `StatsUpdater<T, UNSIGNED_STATS>` split. Every chunk of one column shares a
/// primitive type, so the first decides. CHAR (UInt16) is the one type whose
/// reduce runs unsigned here while its per-page stats are computed signed; the
/// two agree only because CHAR values are non-negative as i32, so a genuinely
/// signed 16-bit type must not reuse this pairing.
fn reduce_int_primitive<T: NativeType>(stats: &[&dyn Statistics]) -> PrimitiveStatistics<T> {
    let typed = || {
        stats
            .iter()
            .map(|x| x.as_any().downcast_ref::<PrimitiveStatistics<T>>().unwrap())
    };
    if is_unsigned_integer_primitive_type(&typed().next().unwrap().primitive_type) {
        reduce_primitive::<_, _, true>(typed())
    } else {
        reduce_primitive::<_, _, false>(typed())
    }
}

#[cfg(test)]
mod tests {
    use crate::schema::types::{
        IntegerType, PrimitiveConvertedType, PrimitiveLogicalType, PrimitiveType,
    };

    use super::*;

    #[test]
    fn binary() -> Result<()> {
        let iter = vec![
            BinaryStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::ByteArray,
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![1, 2]),
                max_value: Some(vec![3, 4]),
                is_max_value_exact: Some(false),
                is_min_value_exact: None,
            },
            BinaryStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::ByteArray,
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![4, 5]),
                max_value: None,
                is_max_value_exact: None,
                is_min_value_exact: Some(false),
            },
        ];
        // The second page has a value (min) but an absent max -- the "unbounded max"
        // sentinel -- so the reduced chunk max is None, not page one's [3, 4], and
        // is_max_value_exact clears with it.
        let a = reduce_binary(iter.iter());

        assert_eq!(
            a,
            BinaryStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::ByteArray,
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![1, 2]),
                max_value: None,
                is_max_value_exact: None,
                is_min_value_exact: Some(false),
            },
        );

        Ok(())
    }

    fn binary_page(min: Option<Vec<u8>>, max: Option<Vec<u8>>) -> BinaryStatistics {
        BinaryStatistics {
            primitive_type: PrimitiveType::from_physical(
                "blob".to_string(),
                PhysicalType::ByteArray,
            ),
            null_count: Some(0),
            distinct_count: None,
            min_value: min,
            max_value: max,
            is_min_value_exact: None,
            is_max_value_exact: None,
        }
    }

    #[test]
    fn reduce_binary_unbounded_max_page_forces_chunk_max_none() -> Result<()> {
        // An opaque-Binary page whose all-0xFF prefix has no short upper bound emits
        // max_value = None with a present min. A sibling page's smaller, bounded max
        // must not win the reduce: the chunk max stays None, and the min still reduces
        // normally. Order must not matter.
        let unbounded = binary_page(Some(vec![0xFF; 9]), None);
        let bounded = binary_page(Some(vec![0x10]), Some(vec![0x20]));

        let a = reduce_binary([&unbounded, &bounded].into_iter());
        assert_eq!(a.max_value, None, "unbounded page poisons the chunk max");
        assert_eq!(a.is_max_value_exact, None);
        assert_eq!(a.min_value, Some(vec![0x10]), "chunk min reduces normally");

        let b = reduce_binary([&bounded, &unbounded].into_iter());
        assert_eq!(b.max_value, None, "order does not matter");

        // The public `reduce` entry point that `build_column_chunk` calls.
        let page_a: Option<Arc<dyn Statistics>> = Some(Arc::new(unbounded));
        let page_b: Option<Arc<dyn Statistics>> = Some(Arc::new(bounded));
        let reduced = reduce(&[&page_a, &page_b])?.expect("reduced stats");
        let stats = reduced
            .as_any()
            .downcast_ref::<BinaryStatistics>()
            .expect("BinaryStatistics");
        assert_eq!(stats.max_value, None);
        Ok(())
    }

    #[test]
    fn reduce_binary_all_null_page_keeps_bounded_max() {
        // An all-null page (no min, no max) contributed no value -- it must not be
        // mistaken for the unbounded-max sentinel, or the text/all-null reduce would
        // wrongly drop a valid chunk max. The bounded sibling's max survives.
        let bounded = binary_page(Some(vec![0x10]), Some(vec![0x20]));
        let all_null = binary_page(None, None);

        let a = reduce_binary([&bounded, &all_null].into_iter());
        assert_eq!(a.max_value, Some(vec![0x20]), "all-null page keeps the max");
        let b = reduce_binary([&all_null, &bounded].into_iter());
        assert_eq!(b.max_value, Some(vec![0x20]), "order does not matter");
    }

    #[test]
    fn reduce_binary_prefix_bounds_apply_length_tiebreaker() {
        // Parquet orders BYTE_ARRAY values as unsigned bytes, so a proper prefix is
        // strictly less than the value that extends it ("app" < "apple"). The chunk
        // reduce must honour that length tiebreaker across pages, or the max
        // understates and the min overstates the true bounds and prunes matching rows.
        let short = binary_page(Some(b"app".to_vec()), Some(b"app".to_vec()));
        let long = binary_page(Some(b"apple".to_vec()), Some(b"apple".to_vec()));

        // Fold order [short, long] exercises the max branch (acc's shorter max must
        // yield to the longer one); [long, short] exercises the min branch.
        let a = reduce_binary([&short, &long].into_iter());
        assert_eq!(
            a.min_value,
            Some(b"app".to_vec()),
            "prefix is the chunk min"
        );
        assert_eq!(
            a.max_value,
            Some(b"apple".to_vec()),
            "extension is the chunk max"
        );

        let b = reduce_binary([&long, &short].into_iter());
        assert_eq!(b.min_value, Some(b"app".to_vec()), "order does not matter");
        assert_eq!(
            b.max_value,
            Some(b"apple".to_vec()),
            "order does not matter"
        );
    }

    #[test]
    fn fixed_len_binary() -> Result<()> {
        let iter = vec![
            FixedLenStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::FixedLenByteArray(2),
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![1, 2]),
                max_value: Some(vec![3, 4]),
            },
            FixedLenStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::FixedLenByteArray(2),
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![4, 5]),
                max_value: None,
            },
        ];
        let a = reduce_fix_len_binary(iter.iter());

        assert_eq!(
            a,
            FixedLenStatistics {
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::FixedLenByteArray(2),
                ),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![1, 2]),
                max_value: Some(vec![3, 4]),
            },
        );

        Ok(())
    }

    #[test]
    fn fixed_len_binary_decimal_logical_type_uses_signed_order() -> Result<()> {
        let mut primitive_type =
            PrimitiveType::from_physical("dec".to_string(), PhysicalType::FixedLenByteArray(2));
        primitive_type.logical_type = Some(PrimitiveLogicalType::Decimal(4, 0));
        let iter = vec![
            FixedLenStatistics {
                primitive_type: primitive_type.clone(),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![0x00, 0x01]), // 1
                max_value: Some(vec![0x00, 0x09]), // 9
            },
            FixedLenStatistics {
                primitive_type,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![0xFF, 0xFB]), // -5
                max_value: Some(vec![0xFF, 0xFE]), // -2
            },
        ];
        let a = reduce_fix_len_binary(iter.iter());

        assert_eq!(a.min_value, Some(vec![0xFF, 0xFB]));
        assert_eq!(a.max_value, Some(vec![0x00, 0x09]));
        Ok(())
    }

    #[test]
    fn fixed_len_binary_decimal_converted_type_uses_signed_order() -> Result<()> {
        let mut primitive_type =
            PrimitiveType::from_physical("dec".to_string(), PhysicalType::FixedLenByteArray(2));
        primitive_type.converted_type = Some(PrimitiveConvertedType::Decimal(4, 0));
        let iter = vec![
            FixedLenStatistics {
                primitive_type: primitive_type.clone(),
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![0x00, 0x01]), // 1
                max_value: Some(vec![0x00, 0x09]), // 9
            },
            FixedLenStatistics {
                primitive_type,
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(vec![0xFF, 0xFB]), // -5
                max_value: Some(vec![0xFF, 0xFE]), // -2
            },
        ];
        let a = reduce_fix_len_binary(iter.iter());

        assert_eq!(a.min_value, Some(vec![0xFF, 0xFB]));
        assert_eq!(a.max_value, Some(vec![0x00, 0x09]));
        Ok(())
    }

    #[test]
    fn boolean() -> Result<()> {
        let iter = vec![
            BooleanStatistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(false),
                max_value: Some(false),
            },
            BooleanStatistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(true),
                max_value: Some(true),
            },
        ];
        let a = reduce_boolean(iter.iter());

        assert_eq!(
            a,
            BooleanStatistics {
                null_count: Some(0),
                distinct_count: None,
                min_value: Some(false),
                max_value: Some(true),
            },
        );

        Ok(())
    }

    #[test]
    fn primitive() -> Result<()> {
        let iter = vec![PrimitiveStatistics {
            null_count: Some(2),
            distinct_count: None,
            min_value: Some(30),
            max_value: Some(70),
            primitive_type: PrimitiveType::from_physical("bla".to_string(), PhysicalType::Int32),
        }];
        let a = reduce_primitive::<i32, _, false>(iter.iter());

        assert_eq!(
            a,
            PrimitiveStatistics {
                null_count: Some(2),
                distinct_count: None,
                min_value: Some(30),
                max_value: Some(70),
                primitive_type: PrimitiveType::from_physical(
                    "bla".to_string(),
                    PhysicalType::Int32,
                ),
            },
        );

        Ok(())
    }

    // IPs straddling the i32 sign bit. 200.0.0.0 is a negative i32 yet the
    // unsigned-largest value, so a signed chunk reduce would understate the max.
    const IP_LOW: i32 = 0x0100_0000; // 1.0.0.0, positive i32
    const IP_MID: i32 = 0x4000_0000; // 64.0.0.0, positive i32
    const IP_HIGH: i32 = 0xC800_0000_u32 as i32; // 200.0.0.0, negative i32

    fn ipv4_logical_type() -> PrimitiveType {
        let mut pt = PrimitiveType::from_physical("ip".to_string(), PhysicalType::Int32);
        pt.logical_type = Some(PrimitiveLogicalType::Integer(IntegerType::UInt32));
        pt
    }

    fn ipv4_converted_type() -> PrimitiveType {
        let mut pt = PrimitiveType::from_physical("ip".to_string(), PhysicalType::Int32);
        pt.converted_type = Some(PrimitiveConvertedType::Uint32);
        pt
    }

    fn page<T: NativeType>(min: T, max: T, pt: &PrimitiveType) -> PrimitiveStatistics<T> {
        PrimitiveStatistics {
            primitive_type: pt.clone(),
            null_count: Some(0),
            distinct_count: None,
            min_value: Some(min),
            max_value: Some(max),
        }
    }

    /// Reduces the pages through the public `reduce` entry point that
    /// `build_column_chunk` calls, so each test exercises the real signed/unsigned
    /// dispatch rather than one comparison branch in isolation.
    fn reduce_pages<T: NativeType>(pages: Vec<PrimitiveStatistics<T>>) -> PrimitiveStatistics<T> {
        let owned: Vec<Option<Arc<dyn Statistics>>> = pages
            .into_iter()
            .map(|p| Some(Arc::new(p) as Arc<dyn Statistics>))
            .collect();
        let refs: Vec<&Option<Arc<dyn Statistics>>> = owned.iter().collect();
        reduce(&refs)
            .unwrap()
            .expect("reduced stats")
            .as_any()
            .downcast_ref::<PrimitiveStatistics<T>>()
            .expect("PrimitiveStatistics<T>")
            .clone()
    }

    #[test]
    fn primitive_ipv4_unsigned_logical_type_uses_unsigned_order() {
        // The unsigned extremes (low, high) are split across pages, so a signed
        // reduce would pick the wrong chunk min and max.
        let pt = ipv4_logical_type();
        let reduced = reduce_pages(vec![
            page(IP_HIGH, IP_HIGH, &pt),
            page(IP_LOW, IP_LOW, &pt),
            page(IP_MID, IP_MID, &pt),
        ]);
        assert_eq!(reduced.min_value, Some(IP_LOW), "unsigned chunk min");
        assert_eq!(reduced.max_value, Some(IP_HIGH), "unsigned chunk max");
        assert!(IP_LOW > 0 && IP_HIGH < 0, "high IP must be a negative i32");
    }

    #[test]
    fn primitive_ipv4_unsigned_converted_type_uses_unsigned_order() {
        let pt = ipv4_converted_type();
        let reduced = reduce_pages(vec![page(IP_HIGH, IP_HIGH, &pt), page(IP_LOW, IP_MID, &pt)]);
        assert_eq!(reduced.min_value, Some(IP_LOW));
        assert_eq!(reduced.max_value, Some(IP_HIGH));
    }

    #[test]
    fn primitive_signed_int32_keeps_signed_order() {
        // A plain INT32 column has no unsigned annotation: the negative value
        // must remain the min, not become the max.
        let pt = PrimitiveType::from_physical("n".to_string(), PhysicalType::Int32);
        let reduced = reduce_pages(vec![
            page(IP_HIGH, IP_HIGH, &pt),
            page(IP_LOW, IP_LOW, &pt),
            page(IP_MID, IP_MID, &pt),
        ]);
        assert_eq!(reduced.min_value, Some(IP_HIGH), "signed min is negative");
        assert_eq!(reduced.max_value, Some(IP_MID), "signed max is positive");
    }

    #[test]
    fn primitive_uint64_uses_unsigned_order() {
        // An INT64 carrying an unsigned annotation reduces unsigned: -1 is the
        // largest u64, so it must be the chunk max, not the min.
        let mut pt = PrimitiveType::from_physical("u".to_string(), PhysicalType::Int64);
        pt.logical_type = Some(PrimitiveLogicalType::Integer(IntegerType::UInt64));
        let low = 1_i64;
        let high = -1_i64; // 0xFFFF_FFFF_FFFF_FFFF == u64::MAX
        let reduced = reduce_pages(vec![page(high, high, &pt), page(low, low, &pt)]);
        assert_eq!(reduced.min_value, Some(low));
        assert_eq!(reduced.max_value, Some(high));
    }

    #[test]
    fn primitive_signed_int64_keeps_signed_order() {
        let pt = PrimitiveType::from_physical("n".to_string(), PhysicalType::Int64);
        let reduced = reduce_pages(vec![page(-5_i64, -5, &pt), page(9_i64, 9, &pt)]);
        assert_eq!(reduced.min_value, Some(-5));
        assert_eq!(reduced.max_value, Some(9));
    }

    #[test]
    fn primitive_double_reduces_by_value() {
        let pt = PrimitiveType::from_physical("d".to_string(), PhysicalType::Double);
        let reduced = reduce_pages(vec![page(2.5_f64, 9.0, &pt), page(-3.0, 4.0, &pt)]);
        assert_eq!(reduced.min_value, Some(-3.0));
        assert_eq!(reduced.max_value, Some(9.0));
    }

    #[test]
    fn primitive_float_reduces_by_value() {
        let pt = PrimitiveType::from_physical("f".to_string(), PhysicalType::Float);
        let reduced = reduce_pages(vec![page(1.0_f32, 7.0, &pt), page(-2.0, 3.0, &pt)]);
        assert_eq!(reduced.min_value, Some(-2.0));
        assert_eq!(reduced.max_value, Some(7.0));
    }
}
