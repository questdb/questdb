mod binary;
mod indexes;
mod primitive;
mod sidecar;

use std::io::{Cursor, Read, Seek};
use std::sync::Arc;

use parquet2::compression::{BrotliLevel, CompressionOptions};
use parquet2::error::Result;
use parquet2::metadata::SchemaDescriptor;
use parquet2::read::read_metadata;
use parquet2::schema::types::{ParquetType, PhysicalType};
use parquet2::statistics::Statistics;
#[cfg(feature = "async")]
use parquet2::write::FileStreamer;
use parquet2::write::{Compressor, DynIter, DynStreamingIterator, FileWriter, Version};
use parquet2::{metadata::Descriptor, page::Page, write::WriteOptions};

use super::Array;
use super::{alltypes_plain, alltypes_statistics};
use primitive::array_to_page_v1;

pub fn array_to_page(
    array: &Array,
    options: &WriteOptions,
    descriptor: &Descriptor,
) -> Result<Page> {
    // using plain encoding format
    match array {
        Array::Int32(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Int64(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Int96(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Float(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Double(array) => primitive::array_to_page_v1(array, options, descriptor),
        Array::Binary(array) => binary::array_to_page_v1(array, options, descriptor),
        _ => todo!(),
    }
}

fn read_column<R: Read + Seek>(reader: &mut R) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
    let (a, statistics) = super::read::read_column(reader, 0, "col")?;
    Ok((a, statistics))
}

#[cfg(feature = "async")]
async fn read_column_async<
    R: futures::AsyncRead + futures::AsyncSeek + Send + std::marker::Unpin,
>(
    reader: &mut R,
) -> Result<(Array, Option<Arc<dyn Statistics>>)> {
    let (a, statistics) = super::read::read_column_async(reader, 0, "col").await?;
    Ok((a, statistics))
}

fn test_column(column: &str, compression: CompressionOptions) -> Result<()> {
    let array = alltypes_plain(column);

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
        bloom_filter_fpp: 0.01,
    };

    // prepare schema
    let type_ = match array {
        Array::Int32(_) => PhysicalType::Int32,
        Array::Int64(_) => PhysicalType::Int64,
        Array::Int96(_) => PhysicalType::Int96,
        Array::Float(_) => PhysicalType::Float,
        Array::Double(_) => PhysicalType::Double,
        Array::Binary(_) => PhysicalType::ByteArray,
        _ => todo!(),
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical("col".to_string(), type_)],
    );

    let a = schema.columns();

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page(
            &array,
            &options,
            &a[0].descriptor,
        ))),
        compression,
        vec![],
        0.0,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    let data = writer.into_inner().into_inner();

    let (result, statistics) = read_column(&mut Cursor::new(data))?;
    assert_eq!(array, result);
    let stats = alltypes_statistics(column);
    assert_eq!(
        statistics.as_ref().map(|x| x.as_ref()),
        Some(stats).as_ref().map(|x| x.as_ref())
    );
    Ok(())
}

#[test]
fn int32() -> Result<()> {
    test_column("id", CompressionOptions::Uncompressed)
}

#[test]
fn int32_snappy() -> Result<()> {
    test_column("id", CompressionOptions::Snappy)
}

#[test]
fn int32_lz4() -> Result<()> {
    test_column("id", CompressionOptions::Lz4Raw)
}

#[test]
fn int32_lz4_short_i32_array() -> Result<()> {
    test_column("id-short-array", CompressionOptions::Lz4Raw)
}

#[test]
fn int32_brotli() -> Result<()> {
    test_column(
        "id",
        CompressionOptions::Brotli(Some(BrotliLevel::default())),
    )
}

#[test]
#[ignore = "Native boolean writer not yet implemented"]
fn bool() -> Result<()> {
    test_column("bool_col", CompressionOptions::Uncompressed)
}

#[test]
fn tinyint() -> Result<()> {
    test_column("tinyint_col", CompressionOptions::Uncompressed)
}

#[test]
fn smallint_col() -> Result<()> {
    test_column("smallint_col", CompressionOptions::Uncompressed)
}

#[test]
fn int_col() -> Result<()> {
    test_column("int_col", CompressionOptions::Uncompressed)
}

#[test]
fn bigint_col() -> Result<()> {
    test_column("bigint_col", CompressionOptions::Uncompressed)
}

#[test]
fn float_col() -> Result<()> {
    test_column("float_col", CompressionOptions::Uncompressed)
}

#[test]
fn double_col() -> Result<()> {
    test_column("double_col", CompressionOptions::Uncompressed)
}

#[test]
fn string_col() -> Result<()> {
    test_column("string_col", CompressionOptions::Uncompressed)
}

#[test]
fn basic() -> Result<()> {
    let array = vec![
        Some(0),
        Some(1),
        Some(2),
        Some(3),
        Some(4),
        Some(5),
        Some(6),
    ];

    let options = WriteOptions {
        write_statistics: false,
        version: Version::V1,
        bloom_filter_fpp: 0.01,
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col".to_string(),
            PhysicalType::Int32,
        )],
    );

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page_v1(
            &array,
            &options,
            &schema.columns()[0].descriptor,
        ))),
        CompressionOptions::Uncompressed,
        vec![],
        0.0,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);

    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    let data = writer.into_inner().into_inner();
    let mut reader = Cursor::new(data);

    let metadata = read_metadata(&mut reader)?;

    // validated against an equivalent array produced by pyarrow.
    let expected = 51;
    assert_eq!(
        metadata.row_groups[0].columns()[0].uncompressed_size(),
        expected
    );

    Ok(())
}

#[cfg(feature = "async")]
async fn test_column_async(column: &str, compression: CompressionOptions) -> Result<()> {
    let array = alltypes_plain(column);

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
        bloom_filter_fpp: 0.01,
    };

    // prepare schema
    let type_ = match array {
        Array::Int32(_) => PhysicalType::Int32,
        Array::Int64(_) => PhysicalType::Int64,
        Array::Int96(_) => PhysicalType::Int96,
        Array::Float(_) => PhysicalType::Float,
        Array::Double(_) => PhysicalType::Double,
        Array::Binary(_) => PhysicalType::ByteArray,
        _ => todo!(),
    };

    let schema = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical("col".to_string(), type_)],
    );

    let a = schema.columns();

    let pages = DynStreamingIterator::new(Compressor::new_from_vec(
        DynIter::new(std::iter::once(array_to_page(
            &array,
            &options,
            &a[0].descriptor,
        ))),
        compression,
        vec![],
        0.0,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = futures::io::Cursor::new(vec![]);
    let mut writer = FileStreamer::new(writer, schema, options, None);

    writer.write(DynIter::new(columns)).await?;
    writer.end(None).await?;

    let data = writer.into_inner().into_inner();

    let (result, statistics) = read_column_async(&mut futures::io::Cursor::new(data)).await?;
    assert_eq!(array, result);
    let stats = alltypes_statistics(column);
    assert_eq!(
        statistics.as_ref().map(|x| x.as_ref()),
        Some(stats).as_ref().map(|x| x.as_ref())
    );
    Ok(())
}

#[cfg(feature = "async")]
#[tokio::test]
async fn test_async() -> Result<()> {
    test_column_async("float_col", CompressionOptions::Uncompressed).await
}

/// Helper: write a single Int32 column through a Compressor with the given
/// compression and min_compression_ratio, then return the column chunk codec
/// from the file metadata and the round-tripped data.
fn write_and_read_codec(
    data: &[i32],
    compression: CompressionOptions,
    min_compression_ratio: f64,
) -> Result<(parquet2::compression::Compression, Vec<Option<i32>>)> {
    use parquet2::schema::types::PhysicalType;

    let options = WriteOptions {
        write_statistics: true,
        version: Version::V1,
        bloom_filter_fpp: 0.0,
    };

    let type_ = ParquetType::from_physical("col".to_string(), PhysicalType::Int32);
    let schema = SchemaDescriptor::new("schema".to_string(), vec![type_]);
    let a = schema.columns();

    let array = Array::Int32(data.iter().copied().map(Some).collect());
    let page = array_to_page(&array, &options, &a[0].descriptor)?;

    let pages = DynStreamingIterator::new(Compressor::new(
        std::iter::once(Ok(page) as Result<Page>),
        compression,
        vec![],
        min_compression_ratio,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);
    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    let file_bytes = writer.into_inner().into_inner();
    let metadata = read_metadata(&mut Cursor::new(&file_bytes))?;

    let codec = metadata.row_groups[0].columns()[0].compression();

    let (array, _) = read_column(&mut Cursor::new(&file_bytes))?;
    let values = match array {
        Array::Int32(v) => v,
        _ => panic!("expected Int32 array"),
    };

    Ok((codec, values))
}

#[test]
fn min_compression_ratio_disabled() -> Result<()> {
    // ratio=0.0 disables the check — pages should stay compressed.
    let data: Vec<i32> = (0..10_000).collect();
    let (codec, values) = write_and_read_codec(&data, CompressionOptions::Snappy, 0.0)?;
    assert_eq!(codec, parquet2::compression::Compression::Snappy);
    assert_eq!(values, data.iter().copied().map(Some).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn min_compression_ratio_met() -> Result<()> {
    // Highly compressible data (all zeros) — ratio should easily be met.
    let data = vec![0i32; 10_000];
    let (codec, values) = write_and_read_codec(&data, CompressionOptions::Snappy, 1.2)?;
    assert_eq!(codec, parquet2::compression::Compression::Snappy);
    assert_eq!(values, data.iter().copied().map(Some).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn min_compression_ratio_not_met_falls_back_to_uncompressed() -> Result<()> {
    // Random-looking data that won't compress well with snappy,
    // combined with an impossibly high ratio threshold.
    let data: Vec<i32> = (0..10_000).map(|i: i32| i.wrapping_mul(0x9E3779B1u32 as i32)).collect();
    let (codec, values) = write_and_read_codec(&data, CompressionOptions::Snappy, 100.0)?;
    // With ratio=100.0 the check will always fail → fall back to uncompressed.
    assert_eq!(codec, parquet2::compression::Compression::Uncompressed);
    assert_eq!(values, data.iter().copied().map(Some).collect::<Vec<_>>());
    Ok(())
}

#[test]
fn min_compression_ratio_uncompressed_codec_is_noop() -> Result<()> {
    // When the requested codec is already Uncompressed, ratio check is irrelevant.
    let data: Vec<i32> = (0..1_000).collect();
    let (codec, values) =
        write_and_read_codec(&data, CompressionOptions::Uncompressed, 100.0)?;
    assert_eq!(codec, parquet2::compression::Compression::Uncompressed);
    assert_eq!(values, data.iter().copied().map(Some).collect::<Vec<_>>());
    Ok(())
}

/// Build a DictPage + DataPage pair for an Int32 column using dictionary encoding.
///
/// `dict_values` are the unique dictionary entries.
/// `indices` maps each row to an index into `dict_values`.
/// Returns a Vec of two pages: [DictPage, DataPage].
fn build_dict_pages(
    dict_values: &[i32],
    indices: &[u32],
    descriptor: &Descriptor,
) -> Result<Vec<Page>> {
    use parquet2::encoding::hybrid_rle::{encode_bool, encode_u32};
    use parquet2::encoding::Encoding;
    use parquet2::page::{DataPage, DataPageHeader, DataPageHeaderV1, DictPage};

    // Build the DictPage: raw little-endian i32 bytes.
    let mut dict_buffer = Vec::with_capacity(dict_values.len() * 4);
    for &v in dict_values {
        dict_buffer.extend_from_slice(&v.to_le_bytes());
    }
    let dict_page = Page::Dict(DictPage::new(dict_buffer, dict_values.len(), false));

    // Build the DataPage with RLE-encoded dictionary indices.
    // Definition levels: all values are present (non-null).
    let num_values = indices.len();
    let mut def_levels = std::io::Cursor::new(vec![0u8; 4]);
    def_levels.set_position(4);
    encode_bool(
        &mut def_levels,
        std::iter::repeat(true).take(num_values),
        num_values,
    )
    .unwrap();
    let mut def_buf = def_levels.into_inner();
    let def_len = (def_buf.len() - 4) as u32;
    def_buf[..4].copy_from_slice(&def_len.to_le_bytes());

    // Determine bit width needed for the indices.
    let max_index = dict_values.len().max(1) - 1;
    let bit_width = if max_index == 0 {
        0u8
    } else {
        (32 - (max_index as u32).leading_zeros()) as u8
    };

    // RLE/bitpack-encode the indices.
    let mut indices_buf = vec![bit_width];
    encode_u32(
        &mut indices_buf,
        indices.iter().copied(),
        num_values,
        bit_width as u32,
    )
    .unwrap();

    // Concatenate: def_levels ++ indices.
    let mut buffer = def_buf;
    buffer.extend_from_slice(&indices_buf);

    let header = DataPageHeaderV1 {
        num_values: num_values as i32,
        encoding: Encoding::RleDictionary.into(),
        definition_level_encoding: Encoding::Rle.into(),
        repetition_level_encoding: Encoding::Rle.into(),
        statistics: None,
    };

    let data_page = Page::Data(DataPage::new(
        DataPageHeader::V1(header),
        buffer,
        descriptor.clone(),
        Some(num_values),
    ));

    Ok(vec![dict_page, data_page])
}

/// Helper: write dict-encoded Int32 pages through a Compressor, then return
/// the column chunk codec and the round-tripped values.
fn write_and_read_dict_codec(
    dict_values: &[i32],
    indices: &[u32],
    compression: CompressionOptions,
    min_compression_ratio: f64,
) -> Result<(parquet2::compression::Compression, Vec<Option<i32>>)> {
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V1,
        bloom_filter_fpp: 0.0,
    };

    let type_ = ParquetType::from_physical("col".to_string(), PhysicalType::Int32);
    let schema = SchemaDescriptor::new("schema".to_string(), vec![type_]);
    let descriptor = &schema.columns()[0].descriptor;

    let pages = build_dict_pages(dict_values, indices, descriptor)?;
    let page_results: Vec<Result<Page>> = pages.into_iter().map(Ok).collect();

    let pages = DynStreamingIterator::new(Compressor::new(
        page_results.into_iter(),
        compression,
        vec![],
        min_compression_ratio,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema, options, None);
    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    let file_bytes = writer.into_inner().into_inner();
    let metadata = read_metadata(&mut Cursor::new(&file_bytes))?;
    let codec = metadata.row_groups[0].columns()[0].compression();

    let (array, _) = read_column(&mut Cursor::new(&file_bytes))?;
    let values = match array {
        Array::Int32(v) => v,
        _ => panic!("expected Int32 array"),
    };

    Ok((codec, values))
}

#[test]
fn min_compression_ratio_with_dict_page() -> Result<()> {
    // Dict-encoded column with a modest ratio threshold.
    // The dict has 4 entries; each row references one of them.
    // Highly repetitive indices compress well, so the ratio should be met.
    let dict_values: Vec<i32> = vec![10, 20, 30, 40];
    let indices: Vec<u32> = (0..10_000).map(|i| (i % 4) as u32).collect();

    let (codec, values) =
        write_and_read_dict_codec(&dict_values, &indices, CompressionOptions::Snappy, 1.2)?;

    assert_eq!(codec, parquet2::compression::Compression::Snappy);

    // Verify round-trip: each index maps back to the correct dict value.
    let expected: Vec<Option<i32>> = indices
        .iter()
        .map(|&idx| Some(dict_values[idx as usize]))
        .collect();
    assert_eq!(values, expected);
    Ok(())
}

#[test]
fn min_compression_ratio_dict_page_fallback() -> Result<()> {
    // Same dict-encoded column but with an impossibly high ratio threshold.
    // The compressor should fall back to uncompressed for all pages (dict + data).
    let dict_values: Vec<i32> = vec![10, 20, 30, 40];
    let indices: Vec<u32> = (0..10_000).map(|i| (i % 4) as u32).collect();

    let (codec, values) =
        write_and_read_dict_codec(&dict_values, &indices, CompressionOptions::Snappy, 100.0)?;

    assert_eq!(codec, parquet2::compression::Compression::Uncompressed);

    let expected: Vec<Option<i32>> = indices
        .iter()
        .map(|&idx| Some(dict_values[idx as usize]))
        .collect();
    assert_eq!(values, expected);
    Ok(())
}

#[test]
fn compressor_multiple_pages() -> Result<()> {
    // Feed 3 data pages into a Compressor with ratio checking enabled.
    // This exercises the collect_and_check_ratio() multi-page loop.
    let options = WriteOptions {
        write_statistics: false,
        version: Version::V1,
        bloom_filter_fpp: 0.0,
    };

    let num_pages = 3;
    let chunk_size = 5_000;

    // --- Case 1: ratio met, pages stay compressed ---
    let type_ = ParquetType::from_physical("col".to_string(), PhysicalType::Int32);
    let schema = SchemaDescriptor::new("schema".to_string(), vec![type_]);
    let descriptor = &schema.columns()[0].descriptor;

    // Build 3 separate data pages with highly compressible data (all zeros).
    let mut all_pages: Vec<Result<Page>> = Vec::new();
    for _ in 0..num_pages {
        let array: Vec<Option<i32>> = vec![Some(0); chunk_size];
        let page = array_to_page_v1(&array, &options, descriptor)?;
        all_pages.push(Ok(page));
    }

    // Use ratio=1.2 — all-zeros data compresses well, so the ratio should be met.
    let pages = DynStreamingIterator::new(Compressor::new(
        all_pages.into_iter(),
        CompressionOptions::Snappy,
        vec![],
        1.2,
    ));
    let columns = std::iter::once(Ok(pages));

    let writer = Cursor::new(vec![]);
    let mut writer = FileWriter::new(writer, schema.clone(), options, None);
    writer.write(DynIter::new(columns), &[])?;
    writer.end(None)?;

    let file_bytes = writer.into_inner().into_inner();
    let metadata = read_metadata(&mut Cursor::new(&file_bytes))?;
    let codec = metadata.row_groups[0].columns()[0].compression();
    assert_eq!(codec, parquet2::compression::Compression::Snappy);

    // Verify the file contains all rows across all 3 pages.
    assert_eq!(
        metadata.row_groups[0].num_rows(),
        (chunk_size * num_pages) as usize
    );

    // Verify data integrity by reading values back (the test reader returns
    // the last page's data for multi-page columns).
    let (array, _) = read_column(&mut Cursor::new(&file_bytes))?;
    let values = match array {
        Array::Int32(v) => v,
        _ => panic!("expected Int32 array"),
    };
    assert!(values.iter().all(|v| *v == Some(0)));

    // --- Case 2: impossibly high ratio, falls back to uncompressed ---
    let schema2 = SchemaDescriptor::new(
        "schema".to_string(),
        vec![ParquetType::from_physical(
            "col".to_string(),
            PhysicalType::Int32,
        )],
    );
    let descriptor2 = &schema2.columns()[0].descriptor;

    let mut all_pages2: Vec<Result<Page>> = Vec::new();
    for _ in 0..num_pages {
        let array: Vec<Option<i32>> = vec![Some(0); chunk_size];
        let page = array_to_page_v1(&array, &options, descriptor2)?;
        all_pages2.push(Ok(page));
    }

    let pages2 = DynStreamingIterator::new(Compressor::new(
        all_pages2.into_iter(),
        CompressionOptions::Snappy,
        vec![],
        100.0,
    ));
    let columns2 = std::iter::once(Ok(pages2));

    let writer2 = Cursor::new(vec![]);
    let mut writer2 = FileWriter::new(writer2, schema2, options, None);
    writer2.write(DynIter::new(columns2), &[])?;
    writer2.end(None)?;

    let file_bytes2 = writer2.into_inner().into_inner();
    let metadata2 = read_metadata(&mut Cursor::new(&file_bytes2))?;
    let codec2 = metadata2.row_groups[0].columns()[0].compression();
    assert_eq!(codec2, parquet2::compression::Compression::Uncompressed);

    assert_eq!(
        metadata2.row_groups[0].num_rows(),
        (chunk_size * num_pages) as usize
    );

    let (array2, _) = read_column(&mut Cursor::new(&file_bytes2))?;
    let values2 = match array2 {
        Array::Int32(v) => v,
        _ => panic!("expected Int32 array"),
    };
    assert!(values2.iter().all(|v| *v == Some(0)));

    Ok(())
}
