use std::collections::HashSet;
use std::io::Write;
use std::sync::Mutex;

use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use parquet_format_safe::{
    BloomFilterAlgorithm, BloomFilterCompression, BloomFilterHash, BloomFilterHeader, ColumnChunk,
    ColumnMetaData, SplitBlockAlgorithm, Type, Uncompressed, XxHash,
};

use crate::bloom_filter;

#[cfg(feature = "async")]
use futures::AsyncWrite;
#[cfg(feature = "async")]
use parquet_format_safe::thrift::protocol::TCompactOutputStreamProtocol;

use crate::statistics::serialize_statistics;
use crate::FallibleStreamingIterator;
use crate::{
    compression::Compression,
    encoding::Encoding,
    error::{Error, Result},
    metadata::ColumnDescriptor,
    page::{CompressedPage, PageType},
};

#[cfg(feature = "async")]
use super::page::write_page_async;

use super::page::{write_page, PageWriteSpec};
use super::statistics::reduce;
use super::DynStreamingIterator;

pub fn write_column_chunk<W, E>(
    writer: &mut W,
    mut offset: u64,
    descriptor: &ColumnDescriptor,
    mut compressed_pages: DynStreamingIterator<'_, CompressedPage, E>,
    bloom_filter_fpp: f64,
    bloom_hashes: Option<&Mutex<HashSet<u64>>>,
) -> std::result::Result<(ColumnChunk, Vec<PageWriteSpec>, u64), E>
where
    W: Write,
    E: std::error::Error + From<Error>,
{
    // write every page

    let initial = offset;

    let mut specs = vec![];
    while let Some(compressed_page) = compressed_pages.next()? {
        let spec = write_page(writer, offset, compressed_page)?;
        offset += spec.bytes_written;
        specs.push(spec);
    }
    let mut bytes_written = offset - initial;

    let bloom_filter_offset = {
        let bloom_ref = bloom_hashes;
        if let Some(bloom_arc) = bloom_ref {
            let fpp = if bloom_filter_fpp <= 0.0 || bloom_filter_fpp >= 1.0 {
                DEFAULT_BLOOM_FILTER_FPP
            } else {
                bloom_filter_fpp
            };
            let hashes = bloom_arc.lock().unwrap();
            if hashes.is_empty() {
                None
            } else {
                let bitset_size = bloom_filter_bitset_size(hashes.len(), fpp);
                let mut bitset = vec![0u8; bitset_size];
                for &hash in hashes.iter() {
                    bloom_filter::insert(&mut bitset, hash);
                }
                let bf_offset = initial + bytes_written;
                let bf_bytes = write_bloom_filter(writer, &bitset)?;
                bytes_written += bf_bytes as u64;
                Some(bf_offset as i64)
            }
        } else {
            None
        }
    };

    let column_chunk = build_column_chunk(&specs, descriptor, bloom_filter_offset)?;

    // write metadata
    let mut protocol = TCompactOutputProtocol::new(writer);
    let column_chunk_bytes: Result<usize> = column_chunk
        .meta_data
        .as_ref()
        .unwrap()
        .write_to_out_protocol(&mut protocol)
        .map_err(|e| e.into());
    bytes_written += column_chunk_bytes? as u64;

    Ok((column_chunk, specs, bytes_written))
}

const DEFAULT_BLOOM_FILTER_FPP: f64 = 0.01;
const MINIMUM_BLOOM_FILTER_BYTES: usize = 32;
const MAXIMUM_BLOOM_FILTER_BYTES: usize = 128 * 1024 * 1024; // 128MB

/// Calculate optimal bloom filter size using the Split Block Bloom Filter formula.
/// This matches Apache Arrow/Parquet's implementation.
/// Formula: m = -8 * ndv / ln(1 - fpp^(1/8))
fn bloom_filter_bitset_size(ndv: usize, fpp: f64) -> usize {
    if ndv == 0 {
        return MINIMUM_BLOOM_FILTER_BYTES;
    }
    let fpp = if fpp <= 0.0 || fpp >= 1.0 {
        DEFAULT_BLOOM_FILTER_FPP
    } else {
        fpp
    };

    // Split Block Bloom Filter formula: m = -8 * ndv / ln(1 - fpp^(1/8))
    let num_bits = -8.0 * (ndv as f64) / (1.0 - fpp.powf(1.0 / 8.0)).ln();

    // Handle overflow
    let num_bits = if num_bits < 0.0 || num_bits > (MAXIMUM_BLOOM_FILTER_BYTES * 8) as f64 {
        MAXIMUM_BLOOM_FILTER_BYTES * 8
    } else {
        num_bits as usize
    };

    let num_bits = num_bits.max(MINIMUM_BLOOM_FILTER_BYTES * 8);
    let num_bytes = num_bits.div_ceil(8);
    let num_bytes = num_bytes.next_power_of_two();
    num_bytes.clamp(MINIMUM_BLOOM_FILTER_BYTES, MAXIMUM_BLOOM_FILTER_BYTES)
}

fn write_bloom_filter<W: Write>(writer: &mut W, bitset: &[u8]) -> Result<usize> {
    let header = BloomFilterHeader {
        num_bytes: bitset.len() as i32,
        algorithm: BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}),
        hash: BloomFilterHash::XXHASH(XxHash {}),
        compression: BloomFilterCompression::UNCOMPRESSED(Uncompressed {}),
    };

    let mut protocol = TCompactOutputProtocol::new(&mut *writer);
    let header_bytes = header
        .write_to_out_protocol(&mut protocol)
        .map_err(Error::from)?;

    writer.write_all(bitset)?;

    Ok(header_bytes + bitset.len())
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub async fn write_column_chunk_async<W, E>(
    writer: &mut W,
    mut offset: u64,
    descriptor: &ColumnDescriptor,
    mut compressed_pages: DynStreamingIterator<'_, CompressedPage, E>,
    _bloom_filter_fpp: f64,
    _bloom_hashes: Option<&Mutex<HashSet<u64>>>,
) -> Result<(ColumnChunk, Vec<PageWriteSpec>, u64)>
where
    W: AsyncWrite + Unpin + Send,
    Error: From<E>,
    E: std::error::Error,
{
    let initial = offset;
    // write every page
    let mut specs = vec![];
    while let Some(compressed_page) = compressed_pages.next()? {
        let spec = write_page_async(writer, offset, compressed_page).await?;
        offset += spec.bytes_written;
        specs.push(spec);
    }
    let mut bytes_written = offset - initial;

    // Note: bloom filter writing for async not implemented yet
    let bloom_filter_offset = None;

    let column_chunk = build_column_chunk(&specs, descriptor, bloom_filter_offset)?;

    // write metadata
    let mut protocol = TCompactOutputStreamProtocol::new(writer);
    bytes_written += column_chunk
        .meta_data
        .as_ref()
        .unwrap()
        .write_to_out_stream_protocol(&mut protocol)
        .await? as u64;

    Ok((column_chunk, specs, bytes_written))
}

fn build_column_chunk(
    specs: &[PageWriteSpec],
    descriptor: &ColumnDescriptor,
    bloom_filter_offset: Option<i64>,
) -> Result<ColumnChunk> {
    // compute stats to build header at the end of the chunk

    let compression = specs
        .iter()
        .map(|spec| spec.compression)
        .collect::<HashSet<_>>();
    if compression.len() > 1 {
        return Err(crate::error::Error::oos(
            "All pages within a column chunk must be compressed with the same codec",
        ));
    }
    let compression = compression
        .into_iter()
        .next()
        .unwrap_or(Compression::Uncompressed);

    // SPEC: the total compressed size is the total compressed size of each page + the header size
    let total_compressed_size = specs
        .iter()
        .map(|x| x.header_size as i64 + x.header.compressed_page_size as i64)
        .sum();
    // SPEC: the total compressed size is the total compressed size of each page + the header size
    let total_uncompressed_size = specs
        .iter()
        .map(|x| x.header_size as i64 + x.header.uncompressed_page_size as i64)
        .sum();
    let data_page_offset = specs.first().map(|spec| spec.offset).unwrap_or(0) as i64;
    let num_values = specs
        .iter()
        .map(|spec| {
            let type_ = spec.header.type_.try_into().unwrap();
            match type_ {
                PageType::DataPage => {
                    spec.header.data_page_header.as_ref().unwrap().num_values as i64
                }
                PageType::DataPageV2 => {
                    spec.header.data_page_header_v2.as_ref().unwrap().num_values as i64
                }
                _ => 0, // only data pages contribute
            }
        })
        .sum();
    let mut encodings = specs
        .iter()
        .flat_map(|spec| {
            let type_ = spec.header.type_.try_into().unwrap();
            match type_ {
                PageType::DataPage => vec![
                    spec.header.data_page_header.as_ref().unwrap().encoding,
                    Encoding::Rle.into(),
                ],
                PageType::DataPageV2 => {
                    vec![
                        spec.header.data_page_header_v2.as_ref().unwrap().encoding,
                        Encoding::Rle.into(),
                    ]
                }
                PageType::DictionaryPage => vec![
                    spec.header
                        .dictionary_page_header
                        .as_ref()
                        .unwrap()
                        .encoding,
                ],
            }
        })
        .collect::<HashSet<_>>() // unique
        .into_iter() // to vec
        .collect::<Vec<_>>();

    // Sort the encodings to have deterministic metadata
    encodings.sort();

    let statistics = specs.iter().map(|x| &x.statistics).collect::<Vec<_>>();
    let statistics = reduce(&statistics)?;
    let statistics = statistics.map(|x| serialize_statistics(x.as_ref()));

    let (type_, _): (Type, Option<i32>) = descriptor.descriptor.primitive_type.physical_type.into();

    let metadata = ColumnMetaData {
        type_,
        encodings,
        path_in_schema: descriptor.path_in_schema.clone(),
        codec: compression.into(),
        num_values,
        total_uncompressed_size,
        total_compressed_size,
        key_value_metadata: None,
        data_page_offset,
        index_page_offset: None,
        dictionary_page_offset: None,
        statistics,
        encoding_stats: None,
        bloom_filter_offset,
    };

    Ok(ColumnChunk {
        file_path: None, // same file for now.
        file_offset: data_page_offset + total_compressed_size,
        meta_data: Some(metadata),
        offset_index_offset: None,
        offset_index_length: None,
        column_index_offset: None,
        column_index_length: None,
        crypto_metadata: None,
        encrypted_column_metadata: None,
    })
}
