mod primitive;
mod schema;

use parquet2::encoding::hybrid_rle::encode_bool;
use parquet2::encoding::Encoding;
use parquet2::metadata::Descriptor;
use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DataPageHeaderV2, Page};
use parquet2::schema::types::PrimitiveType;
use parquet2::statistics::{serialize_statistics, ParquetStatistics, PrimitiveStatistics};
use parquet2::types::NativeType;
use parquet2::write::{DynIter, Version};
use std::io;

const DEFAULT_PAGE_SIZE: usize = 1024 * 1024;

pub fn column_chunk_to_pages(
    column_type: PrimitiveType,
    slice: &[i32],
    data_pagesize_limit: Option<usize>,
    _encoding: Encoding,
) -> DynIter<'_, parquet2::error::Result<Page>> {
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

    let pages = rows.map(move |(offset, length)| {
        Ok(slice_to_page(
            column_type.clone(),
            &slice[offset..offset + length],
        ))
    });

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
    let _definition_levels_byte_length = buffer.len();

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
        chunk.len(),
        chunk.len(),
        statistics,
        column_type.clone(),
        Encoding::Plain,
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
    encode_bool(writer, iter)
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
pub fn _build_page_v2(
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

#[cfg(test)]
mod tests {
    use crate::parquet_write::column_chunk_to_pages;
    use parquet2::compression::CompressionOptions;
    use parquet2::deserialize::{
        native_cast, HybridEncoded, HybridRleDecoderIter, HybridRleIter, OptionalValues,
    };
    use parquet2::encoding::hybrid_rle::Decoder;
    use parquet2::encoding::{hybrid_rle, uleb128, Encoding};
    use parquet2::metadata::SchemaDescriptor;
    use parquet2::page::split_buffer;
    use parquet2::read::{
        read_metadata, BasicDecompressor, ColumnIterator, MutStreamingIterator, State,
    };
    use parquet2::schema::types::{ParquetType, PhysicalType};
    use parquet2::schema::Repetition;
    use parquet2::write::{
        Compressor, DynIter, DynStreamingIterator, FileWriter, Version, WriteOptions,
    };
    use parquet2::FallibleStreamingIterator;
    use std::io::Cursor;
    #[test]
    fn encode_column_tops() {
        let def_level_count: usize = 113_000_000;
        let mut buffer = vec![];
        let mut bb = [0u8; 10];
        let len = uleb128::encode((def_level_count << 1) as u64, &mut bb);
        buffer.extend_from_slice(&bb[..len]);
        buffer.extend_from_slice(&[1u8]);

        // assert!(encode_iter(&mut buffer, std::iter::repeat(true).take(def_level_count), Version::V1).is_ok());

        let iter = hybrid_rle::Decoder::new(buffer.as_slice(), 1);
        let iter = HybridRleIter::new(iter, def_level_count);
        for el in iter {
            assert!(el.is_ok());
            let he = el.unwrap();
            match he {
                HybridEncoded::Repeated(val, len) => {
                    assert_eq!(val, true);
                    assert_eq!(len, def_level_count);
                }
                _ => assert!(false),
            }
        }
    }

    #[test]
    fn write_i32_column() {
        let page1 = vec![0, 1, i32::MIN, 3, 4, 5, 6];
        let expected1 = vec![Some(0), Some(1), None, Some(3), Some(4), Some(5), Some(6)];

        let options = WriteOptions {
            write_statistics: true,
            version: Version::V1,
        };

        let schema = SchemaDescriptor::new(
            "schema".to_string(),
            vec![ParquetType::from_physical(
                "col_i32".to_string(),
                PhysicalType::Int32,
            )],
        );

        let pages = column_chunk_to_pages(
            schema.columns()[0].descriptor.primitive_type.clone(),
            &page1,
            Some(1024),
            Encoding::Plain,
        );

        let pages = DynStreamingIterator::new(Compressor::new(
            pages.into_iter(),
            CompressionOptions::Uncompressed,
            vec![],
        ));

        let columns = std::iter::once(Ok(pages));

        let writer = Cursor::new(vec![]);
        let mut writer = FileWriter::new(writer, schema, options, None);

        writer.write(DynIter::new(columns)).expect("write");
        writer.end(None).expect("end");
        let data = writer.into_inner().into_inner();
        let mut reader = Cursor::new(data);

        let metadata = read_metadata(&mut reader).expect("meta");

        let mut iter = ColumnIterator::new(
            reader,
            metadata.row_groups[0].columns().to_vec(),
            None,
            vec![],
            usize::MAX, // we trust the file is correct
        );

        loop {
            match iter.advance().expect("advance") {
                State::Some(mut new_iter) => {
                    if let Some((pages, _descriptor)) = new_iter.get() {
                        let mut iterator = BasicDecompressor::new(pages, vec![]);
                        while let Some(page) = iterator.next().unwrap() {
                            // do something with it
                            match page {
                                parquet2::page::Page::Data(page) => {
                                    let is_optional =
                                        page.descriptor.primitive_type.field_info.repetition
                                            == Repetition::Optional;
                                    match (page.encoding(), is_optional) {
                                        (Encoding::Plain, true) => {
                                            let (_, def_levels, _) =
                                                split_buffer(page).expect("split");
                                            let validity =
                                                HybridRleDecoderIter::new(HybridRleIter::new(
                                                    Decoder::new(def_levels, 1),
                                                    page.num_values(),
                                                ));
                                            let values = native_cast(page).expect("cast");
                                            let values: Result<
                                                Vec<Option<i32>>,
                                                parquet2::error::Error,
                                            > = OptionalValues::new(validity, values).collect();
                                            if let Ok(v) = values {
                                                assert_eq!(v, expected1);
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                        let _internal_buffer = iterator.into_inner();
                    }
                    iter = new_iter;
                }
                State::Finished(_buffer) => {
                    assert!(_buffer.is_empty()); // data is uncompressed => buffer is always moved
                    break;
                }
            }
        }
    }
}
