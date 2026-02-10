use parquet_format_safe::DataPageHeaderV2;

use crate::compression::{self, Compression};
use crate::error::{Error, Result};
use crate::page::sliced::{CompressedPageRef, DataPageRef, DictPageRef, PageRef};
use crate::page::DataPageHeader;

fn decompress_v1(compressed: &[u8], compression: Compression, buffer: &mut [u8]) -> Result<()> {
    compression::decompress(compression, compressed, buffer)
}

fn decompress_v2(
    compressed: &[u8],
    page_header: &DataPageHeaderV2,
    compression: Compression,
    buffer: &mut [u8],
) -> Result<()> {
    // When processing data page v2, depending on enabled compression for the
    // page, we should account for uncompressed data ('offset') of
    // repetition and definition levels.
    //
    // We always use 0 offset for other pages other than v2, `true` flag means
    // that compression will be applied if decompressor is defined
    let offset = (page_header.definition_levels_byte_length
        + page_header.repetition_levels_byte_length) as usize;
    // When is_compressed flag is missing the page is considered compressed
    let can_decompress = page_header.is_compressed.unwrap_or(true);

    if can_decompress {
        if offset > buffer.len() || offset > compressed.len() {
            return Err(Error::OutOfSpec(
                "V2 Page Header reported incorrect offset to compressed data".to_string(),
            ));
        }

        (buffer[..offset]).copy_from_slice(&compressed[..offset]);

        compression::decompress(compression, &compressed[offset..], &mut buffer[offset..])?;
    } else {
        if buffer.len() != compressed.len() {
            return Err(Error::OutOfSpec(
                "V2 Page Header reported incorrect decompressed size".to_string(),
            ));
        }
        buffer.copy_from_slice(compressed);
    }
    Ok(())
}

/// decompresses a [`CompressedDataPage`] into `buffer`.
/// If the page is un-compressed, `buffer` is swapped instead.
/// Returns whether the page was decompressed.
pub fn decompress_buffer<'a>(
    compressed_page: &CompressedPageRef<'a>,
    buffer: &'a mut Vec<u8>,
) -> Result<(bool, &'a [u8])> {
    if compressed_page.compression() != Compression::Uncompressed {
        // prepare the compression buffer
        let read_size = compressed_page.uncompressed_size();

        if read_size > buffer.capacity() {
            // dealloc and ignore region, replacing it by a new region.
            // This won't reallocate - it frees and calls `alloc_zeroed`
            buffer.reserve(read_size - buffer.capacity());
        }

        // SAFETY:
        // 1. we just reserved enough space
        // 2. the buffer doesn't need to be initialized for the decompression
        unsafe {
            buffer.set_len(read_size);
        }

        match compressed_page {
            CompressedPageRef::Data(compressed_page) => match compressed_page.header() {
                DataPageHeader::V1(_) => {
                    decompress_v1(&compressed_page.buffer, compressed_page.compression, buffer)?
                }
                DataPageHeader::V2(header) => decompress_v2(
                    &compressed_page.buffer,
                    header,
                    compressed_page.compression,
                    buffer,
                )?,
            },
            CompressedPageRef::Dict(page) => {
                decompress_v1(&page.buffer, page.compression(), buffer)?
            }
        }
        Ok((true, buffer))
    } else {
        // page.buffer is already decompressed => swap it with `buffer`, making `page.buffer` the
        // decompression buffer and `buffer` the decompressed buffer
        // std::mem::swap(, buffer);
        Ok((false, compressed_page.buffer()))
    }
}

fn create_page<'a>(compressed_page: CompressedPageRef, buffer: &'a mut Vec<u8>) -> PageRef<'a> {
    match compressed_page {
        CompressedPageRef::Data(page) => PageRef::Data(DataPageRef::new_read(
            page.header,
            buffer,
            page.descriptor,
            page.selected_rows,
        )),
        CompressedPageRef::Dict(page) => PageRef::Dict(DictPageRef {
            buffer,
            num_values: page.num_values,
            is_sorted: page.is_sorted,
        }),
    }
}

/// Decompresses the page, using `buffer` for decompression.
/// If `page.buffer.len() == 0`, there was no decompression and the buffer was moved.
/// Else, decompression took place.
pub fn decompress<'a>(
    mut compressed_page: CompressedPageRef<'a>,
    buffer: &'a mut Vec<u8>,
) -> Result<PageRef<'a>> {
    decompress_buffer(&mut compressed_page, buffer)?;
    Ok(create_page(compressed_page, buffer))
}
