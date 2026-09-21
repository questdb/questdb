use std::io::Cursor;

use parquet2::compression::CompressionOptions;
use parquet2::error::Result;
use parquet2::indexes::{
    select_pages, BoundaryOrder, Index, Interval, NativeIndex, PageIndex, PageLocation,
};
use parquet2::metadata::{SchemaDescriptor, SortingColumn};
use parquet2::read::{
    read_columns_indexes, read_metadata, read_pages_locations, BasicDecompressor, IndexedPageReader,
};
use parquet2::schema::types::{ParquetType, PhysicalType, PrimitiveType};
use parquet2::write::WriteOptions;
use parquet2::write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version};

use crate::read::collect;
use crate::Array;

use super::primitive::array_to_page_v1;

fn write_int_file(
    page1: Vec<Option<i32>>,
    page2: Vec<Option<i32>>,
    sorting_columns: Option<Vec<SortingColumn>>,
) -> Result<Vec<u8>> {
    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
        bloom_filter_fpp: 0.01,
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col1".to_string(),
            PhysicalType::Int32,
        )],
    );

    let pages = vec![
        array_to_page_v1::<i32>(&page1, &options, &schema.columns()[0].descriptor),
        array_to_page_v1::<i32>(&page2, &options, &schema.columns()[0].descriptor),
    ];

    let pages = DynStreamingIterator::new(Compressor::new(
        DynIter::new(pages.into_iter()),
        CompressionOptions::Uncompressed,
        vec![],
        0.0,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer =
        FileWriter::with_sorting_columns(writer, schema, options, None, sorting_columns);

    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    Ok(writer.into_inner().into_inner())
}

fn write_file() -> Result<Vec<u8>> {
    write_int_file(
        vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)],
        vec![Some(10), Some(11)],
        None,
    )
}

/// Read the `boundary_order` of the single Int32 column's `ColumnIndex`.
fn read_int_boundary_order(data: &[u8]) -> Result<BoundaryOrder> {
    let mut reader = Cursor::new(data);
    let metadata = read_metadata(&mut reader)?;
    let columns = metadata.row_groups[0].columns();
    let indexes = read_columns_indexes(&mut reader, columns)?;
    let native = indexes[0]
        .as_any()
        .downcast_ref::<NativeIndex<i32>>()
        .expect("int32 column index");
    Ok(native.boundary_order)
}

#[test]
fn read_indexed_page() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let column = 0;
    let columns = &metadata.row_groups[0].columns();

    // selected the rows
    let intervals = &[Interval::new(2, 2)];

    let pages = read_pages_locations(&mut reader, columns)?;

    let pages = select_pages(intervals, &pages[column], metadata.row_groups[0].num_rows())?;

    let pages = IndexedPageReader::new(reader, &columns[column], pages, vec![], vec![]);

    let pages = BasicDecompressor::new(pages, vec![]);

    let arrays = collect(pages, columns[column].physical_type())?;

    // the second item and length 2
    assert_eq!(arrays, vec![Array::Int32(vec![None, Some(3)])]);

    Ok(())
}

#[test]
fn read_indexes_and_locations() -> Result<()> {
    let data = write_file()?;
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    let columns = &metadata.row_groups[0].columns();

    let expected_page_locations = vec![vec![
        PageLocation {
            offset: 4,
            compressed_page_size: 63,
            first_row_index: 0,
        },
        PageLocation {
            offset: 67,
            compressed_page_size: 47,
            first_row_index: 7,
        },
    ]];
    let expected_index = vec![Box::new(NativeIndex::<i32> {
        primitive_type: PrimitiveType::from_physical("col1".to_string(), PhysicalType::Int32),
        indexes: vec![
            PageIndex {
                min: Some(0),
                max: Some(6),
                null_count: Some(1),
            },
            PageIndex {
                min: Some(10),
                max: Some(11),
                null_count: Some(0),
            },
        ],
        boundary_order: BoundaryOrder::Unordered,
    }) as Box<dyn Index>];

    let indexes = read_columns_indexes(&mut reader, columns)?;
    assert_eq!(&indexes, &expected_index);

    let pages = read_pages_locations(&mut reader, columns)?;
    assert_eq!(pages, expected_page_locations);

    Ok(())
}

#[test]
fn ascending_sorting_column_sets_ascending_boundary_order() -> Result<()> {
    // col1 declared as an ascending sorting column -> its ColumnIndex must
    // advertise ASCENDING so readers can binary-search the page bounds.
    let data = write_int_file(
        vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)],
        vec![Some(10), Some(11)],
        Some(vec![SortingColumn::new(0, false, false)]),
    )?;
    assert_eq!(read_int_boundary_order(&data)?, BoundaryOrder::Ascending);
    Ok(())
}

#[test]
fn descending_sorting_column_sets_descending_boundary_order() -> Result<()> {
    // A descending sorting column propagates DESCENDING (the `descending` flag,
    // not the page values, drives the order).
    let data = write_int_file(
        vec![Some(11), Some(10), Some(9), Some(8)],
        vec![Some(4), Some(3), None, Some(1)],
        Some(vec![SortingColumn::new(0, true, false)]),
    )?;
    assert_eq!(read_int_boundary_order(&data)?, BoundaryOrder::Descending);
    Ok(())
}

#[test]
fn unsorted_column_keeps_unordered_boundary_order() -> Result<()> {
    // col1 is not the declared sorting column (index 1 does not exist here), so
    // it must keep UNORDERED rather than inheriting another column's order.
    let data = write_int_file(
        vec![Some(0), Some(1), None, Some(3)],
        vec![Some(10), Some(11)],
        Some(vec![SortingColumn::new(1, false, false)]),
    )?;
    assert_eq!(read_int_boundary_order(&data)?, BoundaryOrder::Unordered);
    Ok(())
}
