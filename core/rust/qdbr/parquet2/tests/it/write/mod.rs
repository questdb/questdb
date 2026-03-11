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
