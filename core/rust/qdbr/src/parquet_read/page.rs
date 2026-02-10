use parquet2::encoding::get_length;
use parquet2::error::{Error, Result};
use parquet2::page::DataPageHeaderExt;
use parquet2::{encoding::Encoding, metadata::Descriptor, page::DataPageHeader};

#[derive(Debug)]
pub struct DataPage<'a> {
    pub header: &'a DataPageHeader,
    pub descriptor: &'a Descriptor,
    pub buffer: &'a [u8],
}

impl DataPage<'_> {
    pub fn encoding(&self) -> Encoding {
        match &self.header {
            DataPageHeader::V1(d) => d.encoding(),
            DataPageHeader::V2(d) => d.encoding(),
        }
    }

    pub fn num_values(&self) -> usize {
        self.header.num_values()
    }
}
/// An uncompressed, encoded dictionary page.
#[derive(Debug)]
pub struct DictPage<'a> {
    pub buffer: &'a [u8],
    pub num_values: usize,
    pub is_sorted: bool,
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v1 pages.
#[inline]
pub fn split_buffer_v1(
    buffer: &[u8],
    has_rep: bool,
    has_def: bool,
) -> Result<(&[u8], &[u8], &[u8])> {
    let (rep, buffer) = if has_rep {
        let level_buffer_length = get_length(buffer).ok_or_else(|| {
            Error::oos("The number of bytes declared in v1 rep levels is higher than the page size")
        })?;
        (
            buffer.get(4..4 + level_buffer_length).ok_or_else(|| {
                Error::oos(
                    "The number of bytes declared in v1 rep levels is higher than the page size",
                )
            })?,
            buffer.get(4 + level_buffer_length..).ok_or_else(|| {
                Error::oos(
                    "The number of bytes declared in v1 rep levels is higher than the page size",
                )
            })?,
        )
    } else {
        (&[] as &[u8], buffer)
    };

    let (def, buffer) = if has_def {
        let level_buffer_length = get_length(buffer).ok_or_else(|| {
            Error::oos("The number of bytes declared in v1 rep levels is higher than the page size")
        })?;
        (
            buffer.get(4..4 + level_buffer_length).ok_or_else(|| {
                Error::oos(
                    "The number of bytes declared in v1 def levels is higher than the page size",
                )
            })?,
            buffer.get(4 + level_buffer_length..).ok_or_else(|| {
                Error::oos(
                    "The number of bytes declared in v1 def levels is higher than the page size",
                )
            })?,
        )
    } else {
        (&[] as &[u8], buffer)
    };

    Ok((rep, def, buffer))
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values) for v2 pages.
pub fn split_buffer_v2(
    buffer: &[u8],
    rep_level_buffer_length: usize,
    def_level_buffer_length: usize,
) -> Result<(&[u8], &[u8], &[u8])> {
    Ok((
        &buffer[..rep_level_buffer_length],
        &buffer[rep_level_buffer_length..rep_level_buffer_length + def_level_buffer_length],
        &buffer[rep_level_buffer_length + def_level_buffer_length..],
    ))
}

/// Splits the page buffer into 3 slices corresponding to (encoded rep levels, encoded def levels, encoded values).
pub fn split_buffer<'a>(page: &DataPage<'a>) -> Result<(&'a [u8], &'a [u8], &'a [u8])> {
    match page.header {
        DataPageHeader::V1(_) => split_buffer_v1(
            page.buffer,
            page.descriptor.max_rep_level > 0,
            page.descriptor.max_def_level > 0,
        ),
        DataPageHeader::V2(header) => {
            let def_level_buffer_length: usize = header.definition_levels_byte_length.try_into()?;
            let rep_level_buffer_length: usize = header.repetition_levels_byte_length.try_into()?;
            split_buffer_v2(
                page.buffer,
                rep_level_buffer_length,
                def_level_buffer_length,
            )
        }
    }
}
