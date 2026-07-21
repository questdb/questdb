use parquet_format_safe::BoundaryOrder;
use parquet_format_safe::ColumnIndex;
use parquet_format_safe::OffsetIndex;
use parquet_format_safe::PageLocation;

use crate::error::{Error, Result};
use crate::statistics::serialize_statistics;

use crate::write::page::{is_data_page, PageWriteSpec};

pub fn serialize_column_index(
    pages: &[PageWriteSpec],
    boundary_order: BoundaryOrder,
) -> Result<ColumnIndex> {
    let mut null_pages = Vec::with_capacity(pages.len());
    let mut min_values = Vec::with_capacity(pages.len());
    let mut max_values = Vec::with_capacity(pages.len());
    let mut null_counts = Vec::with_capacity(pages.len());

    pages
        .iter()
        .filter(|x| is_data_page(x))
        .try_for_each(|spec| {
            if let Some(stats) = &spec.statistics {
                let stats = serialize_statistics(stats.as_ref());

                let null_count = stats
                    .null_count
                    .ok_or_else(|| Error::oos("null count of a page is required"))?;
                null_counts.push(null_count);

                if let Some(min_value) = stats.min_value {
                    min_values.push(min_value);
                    max_values.push(
                        stats
                            .max_value
                            .ok_or_else(|| Error::oos("max value of a page is required"))?,
                    );
                    null_pages.push(false)
                } else {
                    min_values.push(vec![0]);
                    max_values.push(vec![0]);
                    null_pages.push(true)
                }

                Result::Ok(())
            } else {
                Err(Error::oos(
                    "options were set to write statistics but some pages miss them",
                ))
            }
        })?;
    Ok(ColumnIndex {
        null_pages,
        min_values,
        max_values,
        boundary_order,
        null_counts: Some(null_counts),
    })
}

/// True when these pages can produce a valid [`ColumnIndex`]. A data page is
/// representable only when it is all-null or carries both a min and a max, so two shapes
/// gate the ColumnIndex off file-wide (the OffsetIndex is still written). The first is a
/// min without a max: the opaque-Binary unbounded-max sentinel (`into_parquet_stats`),
/// whose all-`0xFF` prefix has no short upper bound. The second is values without a
/// min/max: array/LIST columns never carry bounds, and `serialize_column_index` would
/// misread the absent min as an all-null page and drop the live rows. A page that really
/// is all-null (`null_count == num_values`) stays representable, and missing statistics
/// is left for `serialize_column_index` to report.
pub fn pages_support_column_index(pages: &[PageWriteSpec]) -> bool {
    pages.iter().filter(|x| is_data_page(x)).all(|spec| {
        spec.statistics.as_ref().is_none_or(|stats| {
            let stats = stats.as_ref();
            if stats.has_min_value() {
                stats.has_max_value()
            } else {
                // No min: representable only if the page really is all null. A page
                // with values but no bounds (arrays) cannot become a ColumnIndex entry.
                stats.null_count() == Some(spec.num_values as i64)
            }
        })
    })
}

pub fn serialize_offset_index(pages: &[PageWriteSpec]) -> Result<OffsetIndex> {
    let mut first_row_index = 0;
    let page_locations = pages
        .iter()
        .filter(|x| is_data_page(x))
        .map(|spec| {
            let location = PageLocation {
                offset: spec.offset.try_into()?,
                compressed_page_size: spec.bytes_written.try_into()?,
                first_row_index,
            };
            let num_rows = spec.num_rows.ok_or_else(|| {
                Error::oos(
                    "options were set to write statistics but some data pages miss number of rows",
                )
            })?;
            first_row_index += num_rows as i64;
            Ok(location)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(OffsetIndex { page_locations })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use parquet_format_safe::PageType;

    use super::pages_support_column_index;
    use crate::compression::Compression;
    use crate::page::ParquetPageHeader;
    use crate::schema::types::{PhysicalType, PrimitiveType};
    use crate::statistics::{PrimitiveStatistics, Statistics};
    use crate::write::page::PageWriteSpec;

    fn data_page(num_values: usize, statistics: Option<Arc<dyn Statistics>>) -> PageWriteSpec {
        PageWriteSpec {
            header: ParquetPageHeader {
                type_: PageType::DATA_PAGE,
                uncompressed_page_size: 0,
                compressed_page_size: 0,
                crc: None,
                data_page_header: None,
                index_page_header: None,
                dictionary_page_header: None,
                data_page_header_v2: None,
            },
            num_values,
            num_rows: Some(num_values),
            header_size: 0,
            offset: 0,
            bytes_written: 0,
            compression: Compression::Uncompressed,
            statistics,
        }
    }

    fn stats(min: Option<i32>, max: Option<i32>, null_count: i64) -> Arc<dyn Statistics> {
        Arc::new(PrimitiveStatistics::<i32> {
            primitive_type: PrimitiveType::from_physical("c".to_string(), PhysicalType::Int32),
            null_count: Some(null_count),
            distinct_count: None,
            min_value: min,
            max_value: max,
        })
    }

    // A page with both bounds maps to a ColumnIndex entry; a genuinely all-null page
    // (null_count == num_values) has no bounds but stays representable as null_pages =
    // true; a page missing statistics is left for serialize_column_index to report.
    #[test]
    fn representable_pages_keep_the_column_index() {
        assert!(pages_support_column_index(&[data_page(
            10,
            Some(stats(Some(1), Some(9), 0))
        )]));
        assert!(pages_support_column_index(&[data_page(
            10,
            Some(stats(None, None, 10))
        )]));
        assert!(pages_support_column_index(&[data_page(10, None)]));
    }

    // An array/LIST page has values but never a min/max. serialize_column_index would
    // misread the absent min as an all-null page and mark live rows as null, so the
    // ColumnIndex must be gated off.
    #[test]
    fn array_page_with_values_gates_off_the_column_index() {
        assert!(!pages_support_column_index(&[data_page(
            10,
            Some(stats(None, None, 3))
        )]));
    }

    // The opaque-Binary unbounded-max sentinel keeps its min but drops its max, and so
    // cannot be represented either.
    #[test]
    fn unbounded_max_gates_off_the_column_index() {
        assert!(!pages_support_column_index(&[data_page(
            10,
            Some(stats(Some(1), None, 0))
        )]));
    }

    // Gating is all-or-nothing across the group: one unrepresentable page suppresses the
    // ColumnIndex even when its siblings would support one.
    #[test]
    fn one_unrepresentable_page_gates_off_the_group() {
        let good = data_page(10, Some(stats(Some(1), Some(9), 0)));
        let array = data_page(10, Some(stats(None, None, 3)));
        assert!(!pages_support_column_index(&[good, array]));
    }
}
