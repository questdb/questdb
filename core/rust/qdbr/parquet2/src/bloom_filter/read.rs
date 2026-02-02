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

    bitset.clear();
    bitset.try_reserve(length)?;
    reader.by_ref().take(length as u64).read_to_end(bitset)?;

    Ok(())
}

pub fn read_from_slice<'a>(
    column_metadata: &ColumnChunkMetaData,
    data: &'a [u8],
) -> Result<&'a [u8], Error> {
    let Some(offset) = column_metadata.metadata().bloom_filter_offset else {
        return Ok(&[]);
    };
    let offset = offset as usize;

    let remaining = data.get(offset..).ok_or_else(|| {
        Error::oos("bloom filter offset exceeds data length")
    })?;

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
    let start = offset + header_size;
    data.get(start..start + length).ok_or_else(|| {
        Error::oos("bloom filter bitset exceeds data length")
    })
}
