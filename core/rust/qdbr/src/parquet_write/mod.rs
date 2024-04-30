mod primitive;
mod schema;

use parquet2;
use parquet2::encoding::hybrid_rle::encode_bool;
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2, Page};
use parquet2::schema::types::{PrimitiveType};
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;
use parquet2::write::{DynIter, Version};
use std::io;
use std::io::Write;

const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;

pub fn column_chunk_to_pages(
    column_type: PrimitiveType,
    slice: &'static[i32],
    data_pagesize_limit: Option<usize>,
    encoding: Encoding,
) -> DynIter<'static, Page> {
    let number_of_rows = slice.len();
    let max_page_size = data_pagesize_limit.unwrap_or(DEFAULT_PAGE_SIZE);
    let max_page_size = max_page_size.min(2usize.pow(31) - 2usize.pow(25));
    let rows_per_page = (max_page_size / (std::mem::size_of::<i32>() + 1)).max(1);

    let rows = (0..number_of_rows)
        .step_by(rows_per_page)
        .map(move |offset| {
            let length = if offset + rows_per_page > number_of_rows {
                number_of_rows - offset
            } else {
                rows_per_page
            };
            (offset, length)
        });

    let pages = rows.map(move |(offset, length)| slice_to_page(column_type.clone(), &slice[offset..offset + length]));

    DynIter::new(pages)
}

pub fn slice_to_page(column_type: PrimitiveType, chunk: &[i32]) -> Page {
    let mut values = vec![];
    let mut null_count = 0;
    let mut max = i32::MIN;
    let mut min = i32::MAX;
    let nulls_iter = chunk.iter().map(|value| {
        max = max.max(*value);
        min = min.min(*value);
        if i32::MIN == *value {
            null_count += 1;
            false
        } else {
            // encode() values here
            values.extend_from_slice(value.to_le_bytes().as_ref());
            true
        }
    });

    let mut buffer = vec![];
    encode_iter(&mut buffer, nulls_iter, Version::V1).expect("nulls encoding");
    let definition_levels_byte_length = buffer.len();

    buffer.extend_from_slice(&values);

    let statistics = if true {
        Some(serialize_statistics(&PrimitiveStatistics::<i32> {
            primitive_type: column_type.clone(),
            null_count: Some(null_count as i64),
            distinct_count: None,
            min_value: Some(min),
            max_value: Some(max),
        }))
    } else {
        None
    };

    Page::Data(build_page_v1(
        buffer,
        chunk.len() - null_count,
        chunk.len(),
        statistics,
        column_type.clone(),
        Encoding::Plain
    ))
}

fn encode_iter_v1<I: Iterator<Item = bool>>(buffer: &mut Vec<u8>, iter: I) -> io::Result<()> {
    buffer.extend_from_slice(&[0; 4]);
    let start = buffer.len();
    encode_bool(buffer, iter)?;
    let end = buffer.len();
    let length = end - start;

    // write the first 4 bytes as length
    let length = (length as i32).to_le_bytes();
    (0..4).for_each(|i| buffer[start - 4 + i] = length[i]);
    Ok(())
}

fn encode_iter_v2<I: Iterator<Item = bool>>(writer: &mut Vec<u8>, iter: I) -> io::Result<()> {
    Ok(encode_bool(writer, iter)?)
}

fn encode_iter<I: Iterator<Item = bool>>(
    writer: &mut Vec<u8>,
    iter: I,
    version: Version,
) -> io::Result<()> {
    match version {
        Version::V1 => encode_iter_v1(writer, iter),
        Version::V2 => encode_iter_v2(writer, iter),
    }
}
#[allow(clippy::too_many_arguments)]
pub fn build_page_v1(
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> DataPage {
    let header = DataPageHeader::V1(DataPageHeaderV1 {
            num_values: num_values as i32,
            encoding: encoding.into(),
            definition_level_encoding: Encoding::Rle.into(),
            repetition_level_encoding: Encoding::Rle.into(),
            statistics,
        });

    DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn build_page_v2(
    buffer: Vec<u8>,
    num_values: usize,
    num_rows: usize,
    null_count: usize,
    repetition_levels_byte_length: usize,
    definition_levels_byte_length: usize,
    statistics: Option<ParquetStatistics>,
    primitive_type: PrimitiveType,
    encoding: Encoding,
) -> DataPage {
    let header = DataPageHeader::V2(DataPageHeaderV2 {
        num_values: num_values as i32,
        encoding: encoding.into(),
        num_nulls: null_count as i32,
        num_rows: num_rows as i32,
        definition_levels_byte_length: definition_levels_byte_length as i32,
        repetition_levels_byte_length: repetition_levels_byte_length as i32,
        is_compressed: Some(false),
        statistics,
    });

    DataPage::new(
        header,
        buffer,
        Descriptor {
            primitive_type,
            max_def_level: 0,
            max_rep_level: 0,
        },
        Some(num_rows),
    )
}
