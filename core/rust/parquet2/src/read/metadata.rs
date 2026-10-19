use std::convert::TryInto;
use std::{
    cmp::min,
    io::{Read, Seek, SeekFrom},
};

use parquet_format_safe::thrift::protocol::TCompactInputProtocol;
use parquet_format_safe::FileMetaData as TFileMetaData;

use super::super::{
    metadata::FileMetaData, DEFAULT_FOOTER_READ_SIZE, FOOTER_SIZE, HEADER_SIZE, PARQUET_MAGIC,
};

use crate::error::{Error, Result};

pub(super) fn metadata_len(buffer: &[u8], len: usize) -> i32 {
    i32::from_le_bytes(buffer[len - 8..len - 4].try_into().unwrap())
}

// see (unstable) Seek::stream_len
fn stream_len(seek: &mut impl Seek) -> std::result::Result<u64, std::io::Error> {
    let old_pos = seek.stream_position()?;
    let len = seek.seek(SeekFrom::End(0))?;

    // Avoid seeking a third time when we were already at the end of the
    // stream. The branch is usually way cheaper than a seek operation.
    if old_pos != len {
        seek.seek(SeekFrom::Start(old_pos))?;
    }

    Ok(len)
}

/// Reads a [`FileMetaData`] from the reader, located at the end of the file.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    // check file is large enough to hold footer
    let file_size = stream_len(reader)?;
    read_metadata_with_size(reader, file_size)
}

/// Reads a [`FileMetaData`] from the reader, located at the end of the file, with known file size.
pub fn read_metadata_with_size<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
) -> Result<FileMetaData> {
    // Ensure provided file_size is valid by comparing it with the actual file size
    let actual_file_size = reader.seek(SeekFrom::End(0))?;
    if file_size > actual_file_size {
        return Err(Error::oos(
            "Provided file_size is greater than the actual file size",
        ));
    }

    if file_size < HEADER_SIZE + FOOTER_SIZE {
        return Err(Error::oos(
            "A parquet file must contain a header and footer with at least 12 bytes",
        ));
    }

    // read and cache up to DEFAULT_FOOTER_READ_SIZE bytes from the end and process the footer
    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::Start(file_size - default_end_len as u64))?;

    let mut buffer = Vec::with_capacity(default_end_len);
    reader
        .by_ref()
        .take(default_end_len as u64)
        .read_to_end(&mut buffer)?;

    // check this is indeed a parquet file
    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(Error::oos("The file must end with PAR1"));
    }

    let metadata_len = metadata_len(&buffer, default_end_len);
    let metadata_len: u64 = metadata_len.try_into()?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if footer_len > file_size {
        return Err(Error::oos(
            "The footer size must be smaller or equal to the file's size",
        ));
    }

    let reader: &[u8] = if (footer_len as usize) < buffer.len() {
        // the whole metadata is in the bytes we already read
        let remaining = buffer.len() - footer_len as usize;
        &buffer[remaining..]
    } else {
        // the end of file read by default is not long enough, read again including the metadata.
        reader.seek(SeekFrom::Start(file_size - footer_len))?;
        // reader.seek(SeekFrom::End(-(footer_len as i64)))?;

        buffer.clear();
        buffer.try_reserve(footer_len as usize)?;
        reader.take(footer_len).read_to_end(&mut buffer)?;

        &buffer
    };

    // a highly nested but sparse struct could result in many allocations
    let max_size = reader.len() * 2 + 1024;

    deserialize_metadata(reader, max_size)
}

/// Parse loaded metadata bytes
pub fn deserialize_metadata<R: Read>(reader: R, max_size: usize) -> Result<FileMetaData> {
    let mut prot = TCompactInputProtocol::new(reader, max_size);
    let metadata = TFileMetaData::read_from_in_protocol(&mut prot)?;

    FileMetaData::try_from_thrift(metadata)
}

/// Reads a [`FileMetaData`] and returns the raw Thrift-serialized footer bytes
/// alongside the parsed metadata. The raw bytes can be used by
/// [`FooterCache`](crate::write::footer_cache::FooterCache) for incremental
/// footer serialization.
///
/// Returns `(metadata, raw_metadata_bytes, footer_size)` where `footer_size`
/// is the total footer size in bytes (metadata + 8-byte trailer).
pub fn read_metadata_with_footer_bytes<R: Read + Seek>(
    reader: &mut R,
    file_size: u64,
) -> Result<(FileMetaData, Vec<u8>, u64)> {
    let actual_file_size = reader.seek(SeekFrom::End(0))?;
    if file_size > actual_file_size {
        return Err(Error::oos(
            "Provided file_size is greater than the actual file size",
        ));
    }

    if file_size < HEADER_SIZE + FOOTER_SIZE {
        return Err(Error::oos(
            "A parquet file must contain a header and footer with at least 12 bytes",
        ));
    }

    let default_end_len = min(DEFAULT_FOOTER_READ_SIZE, file_size) as usize;
    reader.seek(SeekFrom::Start(file_size - default_end_len as u64))?;

    let mut buffer = Vec::with_capacity(default_end_len);
    reader
        .by_ref()
        .take(default_end_len as u64)
        .read_to_end(&mut buffer)?;

    if buffer[default_end_len - 4..] != PARQUET_MAGIC {
        return Err(Error::oos("The file must end with PAR1"));
    }

    let metadata_len = metadata_len(&buffer, default_end_len);
    let metadata_len: u64 = metadata_len.try_into()?;

    let footer_len = FOOTER_SIZE + metadata_len;
    if footer_len > file_size {
        return Err(Error::oos(
            "The footer size must be smaller or equal to the file's size",
        ));
    }

    if (footer_len as usize) < buffer.len() {
        // The whole footer fits in the speculative read. Deserialize from the
        // buffer slice, then drain the prefix so buffer holds only the raw
        // metadata bytes (no trailing 8-byte length+magic).
        let remaining = buffer.len() - footer_len as usize;
        let max_size = footer_len as usize * 2 + 1024;
        let metadata = deserialize_metadata(&buffer[remaining..], max_size)?;
        buffer.drain(..remaining);
        buffer.truncate(metadata_len as usize);
        Ok((metadata, buffer, footer_len))
    } else {
        // Need a second read for the full footer.
        reader.seek(SeekFrom::Start(file_size - footer_len))?;
        buffer.clear();
        buffer.try_reserve(footer_len as usize)?;
        reader.take(footer_len).read_to_end(&mut buffer)?;
        let max_size = buffer.len() * 2 + 1024;
        let metadata = deserialize_metadata(&buffer[..], max_size)?;
        buffer.truncate(metadata_len as usize);
        Ok((metadata, buffer, footer_len))
    }
}
