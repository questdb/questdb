use crate::compression::CompressionOptions;
use crate::error::{Error, Result};
use crate::page::{CompressedDictPage, CompressedPage, DataPageHeader, DictPage};
use crate::FallibleStreamingIterator;
use crate::{
    compression,
    page::{CompressedDataPage, DataPage, Page},
};

/// Compresses a [`DataPage`] into a [`CompressedDataPage`].
fn compress_data(
    page: DataPage,
    mut compressed_buffer: Vec<u8>,
    compression: CompressionOptions,
) -> Result<CompressedDataPage> {
    let DataPage {
        mut buffer,
        header,
        descriptor,
        selected_rows,
    } = page;
    let uncompressed_page_size = buffer.len();
    if compression != CompressionOptions::Uncompressed {
        match &header {
            DataPageHeader::V1(_) => {
                compression::compress(compression, &buffer, &mut compressed_buffer)?;
            }
            DataPageHeader::V2(header) => {
                let levels_byte_length = (header.repetition_levels_byte_length
                    + header.definition_levels_byte_length)
                    as usize;
                compressed_buffer.extend_from_slice(&buffer[..levels_byte_length]);
                compression::compress(
                    compression,
                    &buffer[levels_byte_length..],
                    &mut compressed_buffer,
                )?;
            }
        };
    } else {
        std::mem::swap(&mut buffer, &mut compressed_buffer);
    };
    Ok(CompressedDataPage::new_read(
        header,
        compressed_buffer,
        compression.into(),
        uncompressed_page_size,
        descriptor,
        selected_rows,
    ))
}

fn compress_dict(
    page: DictPage,
    mut compressed_buffer: Vec<u8>,
    compression: CompressionOptions,
) -> Result<CompressedDictPage> {
    let DictPage {
        mut buffer,
        num_values,
        is_sorted,
    } = page;
    let uncompressed_page_size = buffer.len();
    if compression != CompressionOptions::Uncompressed {
        compression::compress(compression, &buffer, &mut compressed_buffer)?;
    } else {
        std::mem::swap(&mut buffer, &mut compressed_buffer);
    }
    Ok(CompressedDictPage::new(
        compressed_buffer,
        compression.into(),
        uncompressed_page_size,
        num_values,
        is_sorted,
    ))
}

/// Compresses an [`EncodedPage`] into a [`CompressedPage`] using `compressed_buffer` as the
/// intermediary buffer.
///
/// `compressed_buffer` is taken by value because it becomes owned by [`CompressedPage`]
///
/// # Errors
/// Errors if the compressor fails
pub fn compress(
    page: Page,
    compressed_buffer: Vec<u8>,
    compression: CompressionOptions,
) -> Result<CompressedPage> {
    match page {
        Page::Data(page) => {
            compress_data(page, compressed_buffer, compression).map(CompressedPage::Data)
        }
        Page::Dict(page) => {
            compress_dict(page, compressed_buffer, compression).map(CompressedPage::Dict)
        }
    }
}

/// A [`FallibleStreamingIterator`] that compresses [`Page`]s and yields [`CompressedPage`]s.
///
/// When `min_compression_ratio > 0.0`, the compressor eagerly compresses all pages,
/// checks the aggregate compression ratio across the entire column chunk, and falls
/// back to uncompressed for ALL pages if the ratio is not met. This ensures all pages
/// within a column chunk use the same codec, as required by the Parquet specification.
pub struct Compressor<
    E: std::error::Error + From<Error>,
    I: Iterator<Item = std::result::Result<Page, E>>,
> {
    /// Pages source — used only in streaming mode (ratio check disabled).
    iter: I,
    compression: CompressionOptions,
    min_compression_ratio: f64,
    buffer: Vec<u8>,
    current: Option<CompressedPage>,
    /// Pre-collected pages when ratio check is active.
    collected: Option<std::vec::IntoIter<CompressedPage>>,
}

impl<E: std::error::Error + From<Error>, I: Iterator<Item = std::result::Result<Page, E>>>
    Compressor<E, I>
{
    /// Creates a new [`Compressor`]
    pub fn new(
        iter: I,
        compression: CompressionOptions,
        buffer: Vec<u8>,
        min_compression_ratio: f64,
    ) -> Self {
        Self {
            iter,
            compression,
            min_compression_ratio,
            buffer,
            current: None,
            collected: None,
        }
    }

    /// Creates a new [`Compressor`] (same as `new`)
    pub fn new_from_vec(
        iter: I,
        compression: CompressionOptions,
        buffer: Vec<u8>,
        min_compression_ratio: f64,
    ) -> Self {
        Self::new(iter, compression, buffer, min_compression_ratio)
    }

    /// Deconstructs itself into its iterator and scratch buffer.
    pub fn into_inner(mut self) -> (I, Vec<u8>) {
        let mut buffer = if let Some(page) = self.current.as_mut() {
            std::mem::take(page.buffer())
        } else {
            std::mem::take(&mut self.buffer)
        };
        buffer.clear();
        (self.iter, buffer)
    }

    /// Eagerly compress all pages from the iterator, then check the aggregate
    /// compression ratio. If the ratio is below `min_compression_ratio`,
    /// store all pages uncompressed so the entire column chunk uses a single
    /// codec, as required by the Parquet specification.
    fn collect_and_check_ratio(&mut self) -> std::result::Result<(), E> {
        // First pass: collect all uncompressed pages.
        let mut raw_pages = Vec::new();
        while let Some(result) = self.iter.next() {
            raw_pages.push(result?);
        }

        // Trial compression to measure aggregate ratio.
        let mut total_uncompressed: usize = 0;
        let mut total_compressed: usize = 0;
        let mut trial = Vec::with_capacity(raw_pages.len());
        for page in &raw_pages {
            let uncompressed_size = match page {
                Page::Data(p) => p.buffer.len(),
                Page::Dict(p) => p.buffer.len(),
            };
            total_uncompressed += uncompressed_size;
            let compressed = compress(page.clone(), vec![], self.compression).map_err(E::from)?;
            total_compressed += compressed.compressed_size();
            trial.push(compressed);
        }

        let ratio_failed = self.compression != CompressionOptions::Uncompressed
            && total_compressed > 0
            && (total_uncompressed as f64 / total_compressed as f64) < self.min_compression_ratio;

        let pages = if ratio_failed {
            // Ratio not met: store all pages uncompressed.
            let mut uncompressed = Vec::with_capacity(raw_pages.len());
            // We can reuse the trial compressed pages as buffers for the uncompressed pages to avoid extra allocations.
            for (page, mut buf) in raw_pages.into_iter().zip(trial) {
                uncompressed.push(
                    compress(
                        page,
                        std::mem::take(buf.buffer()),
                        CompressionOptions::Uncompressed,
                    )
                    .map_err(E::from)?,
                );
            }
            uncompressed
        } else {
            trial
        };

        self.collected = Some(pages.into_iter());
        Ok(())
    }
}

impl<E: std::error::Error + From<Error>, I: Iterator<Item = std::result::Result<Page, E>>>
    FallibleStreamingIterator for Compressor<E, I>
{
    type Item = CompressedPage;
    type Error = E;

    fn advance(&mut self) -> std::result::Result<(), Self::Error> {
        if self.min_compression_ratio > 0.0 {
            // Ratio check mode: eagerly collect all pages on first advance.
            if self.collected.is_none() {
                self.collect_and_check_ratio()?;
            }
            self.current = self.collected.as_mut().and_then(|iter| iter.next());
        } else {
            // Streaming mode: compress one page at a time (no ratio check).
            let mut compressed_buffer = if let Some(page) = self.current.as_mut() {
                std::mem::take(page.buffer())
            } else {
                std::mem::take(&mut self.buffer)
            };
            compressed_buffer.clear();

            let next = self
                .iter
                .next()
                .map(|x| {
                    x.and_then(|page| Ok(compress(page, compressed_buffer, self.compression)?))
                })
                .transpose()?;
            self.current = next;
        }
        Ok(())
    }

    fn get(&self) -> Option<&Self::Item> {
        self.current.as_ref()
    }
}
