use std::convert::TryInto;
use std::io::Cursor;

use parquet_format_safe::thrift::protocol::TCompactInputProtocol;

use crate::compression::Compression;
use crate::error::{Error, Result};
use crate::metadata::{ColumnChunkMetaData, Descriptor};
use crate::page::{DataPageHeader, PageType, ParquetPageHeader};
use crate::parquet_bridge::Encoding;

use super::reader::get_page_header;

#[derive(Debug)]
pub struct SlicedDictPage<'a> {
    pub buffer: &'a [u8],
    pub compression: Compression,
    pub uncompressed_size: usize,
    pub num_values: usize,
    pub is_sorted: bool,
}

#[derive(Debug)]
pub struct SlicedDataPage<'a> {
    pub header: DataPageHeader,
    pub buffer: &'a [u8],
    pub compression: Compression,
    pub uncompressed_size: usize,
    pub descriptor: Descriptor,
}

impl SlicedDataPage<'_> {
    pub fn num_values(&self) -> usize {
        self.header.num_values()
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SlicedPage<'a> {
    Dict(SlicedDictPage<'a>),
    Data(SlicedDataPage<'a>),
}

pub struct SlicePageReader<'a> {
    data: &'a [u8],
    offset: usize,
    end: usize,
    compression: Compression,
    descriptor: Descriptor,
    seen_num_values: i64,
    total_num_values: i64,
    max_page_size: usize,
}

impl<'a> SlicePageReader<'a> {
    pub fn new(data: &'a [u8], column: &ColumnChunkMetaData, max_page_size: usize) -> Result<Self> {
        let (col_start, col_len) = column.byte_range();
        let col_start = col_start as usize;
        let col_end = col_start + col_len as usize;
        if col_end > data.len() {
            return Err(Error::oos(format!(
                "Column chunk range {}..{} exceeds data length {}",
                col_start,
                col_end,
                data.len()
            )));
        }
        Ok(Self {
            data,
            offset: col_start,
            end: col_end,
            compression: column.compression(),
            descriptor: column.descriptor().descriptor.clone(),
            seen_num_values: 0,
            total_num_values: column.num_values(),
            max_page_size,
        })
    }

    fn read_next(&mut self) -> Result<Option<SlicedPage<'a>>> {
        if self.seen_num_values >= self.total_num_values {
            return Ok(None);
        }

        let remaining = &self.data[self.offset..self.end];
        let mut cursor = Cursor::new(remaining);
        let page_header = {
            let mut prot = TCompactInputProtocol::new(&mut cursor, self.max_page_size);
            ParquetPageHeader::read_from_in_protocol(&mut prot)?
        };
        let header_size = cursor.position() as usize;
        self.offset += header_size;

        self.seen_num_values += get_page_header(&page_header)?
            .map(|x| x.num_values() as i64)
            .unwrap_or_default();

        let read_size: usize = page_header.compressed_page_size.try_into()?;
        if read_size > self.max_page_size {
            return Err(Error::WouldOverAllocate);
        }

        if self.offset + read_size > self.end {
            return Err(Error::oos(
                "The page header reported the wrong page size".to_string(),
            ));
        }

        let page_data = &self.data[self.offset..self.offset + read_size];
        self.offset += read_size;

        let type_: PageType = page_header.type_.try_into()?;
        let uncompressed_page_size: usize = page_header.uncompressed_page_size.try_into()?;

        match type_ {
            PageType::DictionaryPage => {
                let dict_header =
                    page_header.dictionary_page_header.as_ref().ok_or_else(|| {
                        Error::oos(
                            "The page header type is a dictionary page but the dictionary header is empty",
                        )
                    })?;
                let is_sorted = dict_header.is_sorted.unwrap_or(false);

                Ok(Some(SlicedPage::Dict(SlicedDictPage {
                    buffer: page_data,
                    compression: self.compression,
                    uncompressed_size: uncompressed_page_size,
                    num_values: dict_header.num_values.try_into()?,
                    is_sorted,
                })))
            }
            PageType::DataPage => {
                let header = page_header.data_page_header.ok_or_else(|| {
                    Error::oos(
                        "The page header type is a v1 data page but the v1 data header is empty",
                    )
                })?;
                let _: Encoding = header.encoding.try_into()?;
                let _: Encoding = header.repetition_level_encoding.try_into()?;
                let _: Encoding = header.definition_level_encoding.try_into()?;

                Ok(Some(SlicedPage::Data(SlicedDataPage {
                    header: DataPageHeader::V1(header),
                    buffer: page_data,
                    compression: self.compression,
                    uncompressed_size: uncompressed_page_size,
                    descriptor: self.descriptor.clone(),
                })))
            }
            PageType::DataPageV2 => {
                let header = page_header.data_page_header_v2.ok_or_else(|| {
                    Error::oos(
                        "The page header type is a v2 data page but the v2 data header is empty",
                    )
                })?;
                let _: Encoding = header.encoding.try_into()?;

                Ok(Some(SlicedPage::Data(SlicedDataPage {
                    header: DataPageHeader::V2(header),
                    buffer: page_data,
                    compression: self.compression,
                    uncompressed_size: uncompressed_page_size,
                    descriptor: self.descriptor.clone(),
                })))
            }
        }
    }
}

impl<'a> Iterator for SlicePageReader<'a> {
    type Item = Result<SlicedPage<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next().transpose()
    }
}
