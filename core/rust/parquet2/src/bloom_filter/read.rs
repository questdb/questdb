use std::io::{Cursor, Read, Seek, SeekFrom};

use parquet_format_safe::{
    thrift::protocol::TCompactInputProtocol, BloomFilterAlgorithm, BloomFilterCompression,
    BloomFilterHeader, SplitBlockAlgorithm, Uncompressed,
};

use crate::{error::Error, metadata::ColumnChunkMetaData};

/// Reads the bloom filter associated to [`ColumnChunkMetaData`] into `bitset`.
/// Results in an empty `bitset` if there is no associated bloom filter or the algorithm is not supported.
/// # Error
/// Errors if the column contains no metadata or the filter can't be read or deserialized.
pub fn read<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    mut reader: &mut R,
    bitset: &mut Vec<u8>,
) -> Result<(), Error> {
    let offset = column_metadata.metadata().bloom_filter_offset;

    let offset = if let Some(offset) = offset {
        offset as u64
    } else {
        bitset.clear();
        return Ok(());
    };
    reader.seek(SeekFrom::Start(offset))?;

    // deserialize header
    let mut prot = TCompactInputProtocol::new(&mut reader, usize::MAX); // max is ok since `BloomFilterHeader` never allocates
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot)?;

    if header.algorithm != BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}) {
        bitset.clear();
        return Ok(());
    }
    if header.compression != BloomFilterCompression::UNCOMPRESSED(Uncompressed {}) {
        bitset.clear();
        return Ok(());
    }

    let length: usize = header.num_bytes.try_into()?;
    if length < 32 || length % 32 != 0 {
        bitset.clear();
        return Ok(());
    }

    bitset.clear();
    bitset.try_reserve(length)?;
    reader.by_ref().take(length as u64).read_to_end(bitset)?;

    if bitset.len() != length {
        return Err(Error::oos(format!(
            "bloom filter truncated: expected {} bytes, got {}",
            length,
            bitset.len()
        )));
    }

    Ok(())
}

/// Returns the total size in bytes (header + bitset) of the bloom filter for a column.
/// Returns `Ok(0)` if the column has no bloom filter.
pub fn total_size<R: Read + Seek>(
    column_metadata: &ColumnChunkMetaData,
    reader: &mut R,
) -> Result<u64, Error> {
    let offset = match column_metadata.metadata().bloom_filter_offset {
        Some(off) => off as u64,
        None => return Ok(0),
    };
    reader.seek(SeekFrom::Start(offset))?;
    let mut prot = TCompactInputProtocol::new(&mut *reader, usize::MAX);
    let header = BloomFilterHeader::read_from_in_protocol(&mut prot)?;
    let header_size = reader.stream_position()? - offset;
    let num_bytes: u64 = header.num_bytes.try_into()?;
    Ok(header_size + num_bytes)
}

pub fn read_from_slice<'a>(
    column_metadata: &ColumnChunkMetaData,
    data: &'a [u8],
) -> Result<&'a [u8], Error> {
    let Some(offset) = column_metadata.metadata().bloom_filter_offset else {
        return Ok(&[]);
    };
    read_from_slice_at_offset(offset as u64, data)
}

/// Reads the bloom filter bitset from `data` at the given byte `offset`.
/// Returns an empty slice if the bloom filter is absent or unsupported.
pub fn read_from_slice_at_offset(offset: u64, data: &[u8]) -> Result<&[u8], Error> {
    let offset: usize = offset
        .try_into()
        .map_err(|_| Error::oos("bloom filter offset overflow"))?;
    let remaining = data
        .get(offset..)
        .ok_or_else(|| Error::oos("bloom filter offset exceeds data length"))?;

    let mut cursor = Cursor::new(remaining);
    let header = {
        // max is ok since `BloomFilterHeader` never allocates
        let mut prot = TCompactInputProtocol::new(&mut cursor, usize::MAX);
        BloomFilterHeader::read_from_in_protocol(&mut prot)?
    };
    let header_size = cursor.position() as usize;

    if header.algorithm != BloomFilterAlgorithm::BLOCK(SplitBlockAlgorithm {}) {
        return Ok(&[]);
    }
    if header.compression != BloomFilterCompression::UNCOMPRESSED(Uncompressed {}) {
        return Ok(&[]);
    }

    let length: usize = header.num_bytes.try_into()?;
    if length < 32 || length % 32 != 0 {
        return Ok(&[]);
    }
    let start = offset
        .checked_add(header_size)
        .ok_or_else(|| Error::oos("bloom filter start offset overflow"))?;
    let end = start
        .checked_add(length)
        .ok_or_else(|| Error::oos("bloom filter end offset overflow"))?;
    data.get(start..end)
        .ok_or_else(|| Error::oos("bloom filter bitset exceeds data length"))
}
