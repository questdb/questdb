use std::collections::HashSet;
use std::io::Write;
use std::sync::{Arc, Mutex};

use parquet_format_safe::thrift::protocol::TCompactOutputProtocol;
use parquet_format_safe::{RowGroup, SortingColumn};

use crate::metadata::ThriftFileMetaData;
use crate::{
    error::{Error, Result},
    metadata::SchemaDescriptor,
    FOOTER_SIZE, PARQUET_MAGIC,
};

use super::footer_cache::FooterCache;
use super::indexes::{write_column_index, write_offset_index};
use super::page::PageWriteSpec;
use super::{row_group::write_row_group, RowGroupIter, WriteOptions};

pub use crate::metadata::KeyValue;
use crate::write::State;

pub fn start_file<W: Write>(writer: &mut W) -> Result<u64> {
    writer.write_all(&PARQUET_MAGIC)?;
    Ok(PARQUET_MAGIC.len() as u64)
}

/// Describes where a row group in the final list came from.
enum RowGroupSource {
    /// Unchanged original row group — use cached raw bytes if available.
    Cached(usize),
    /// Modified original row group — must be freshly serialized.
    Fresh,
    /// Newly inserted row group — must be freshly serialized.
    Inserted,
}

/// A streaming writer wrapper that supports both protocol-driven writes (via
/// the `Write` trait) and direct raw byte injection. Uses `RefCell` for
/// interior mutability so a `TCompactOutputProtocol` can hold a shared
/// reference while we also inject cached row group bytes. Streams directly
/// to the underlying writer, avoiding an intermediate buffer.
struct SharedWriter<'a, W: Write>(std::cell::RefCell<(&'a mut W, u64)>);

impl<'a, W: Write> SharedWriter<'a, W> {
    fn new(writer: &'a mut W) -> Self {
        SharedWriter(std::cell::RefCell::new((writer, 0)))
    }

    fn write_raw(&self, data: &[u8]) -> Result<()> {
        let mut guard = self.0.borrow_mut();
        guard.0.write_all(data)?;
        guard.1 += data.len() as u64;
        Ok(())
    }

    fn bytes_written(&self) -> u64 {
        self.0.borrow().1
    }

    fn flush(&self) -> std::io::Result<()> {
        self.0.borrow_mut().0.flush()
    }
}

impl<W: Write> std::io::Write for &SharedWriter<'_, W> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let mut guard = self.0.borrow_mut();
        guard.0.write_all(data)?;
        guard.1 += data.len() as u64;
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.0.borrow_mut().0.flush()
    }
}

/// Writes the Parquet footer incrementally. Unchanged row groups are written as
/// raw pre-serialized bytes from the `FooterCache`, while modified/inserted row
/// groups are serialized fresh. Falls back to full serialization when no cache
/// is available. Streams directly to the writer without an intermediate buffer.
fn end_file_incremental<W: Write>(
    writer: &mut W,
    metadata: &ThriftFileMetaData,
    footer_cache: &Option<FooterCache>,
    sources: &[RowGroupSource],
) -> Result<u64> {
    let cache = match footer_cache.as_ref() {
        Some(c) if sources.len() == metadata.row_groups.len() => c,
        _ => return end_file(writer, metadata),
    };

    use parquet_format_safe::thrift::protocol::{TCompactOutputProtocol, TOutputProtocol, TType};
    use parquet_format_safe::thrift::protocol::{
        TFieldIdentifier, TListIdentifier, TStructIdentifier,
    };

    // Use SharedWriter so a single protocol instance stays alive for the
    // entire serialization while we can also inject cached row group bytes.
    let shared = SharedWriter::new(writer);
    let mut prot = TCompactOutputProtocol::new(&shared);

    prot.write_struct_begin(&TStructIdentifier::new("FileMetaData"))?;

    // Field 1: version
    prot.write_field_begin(&TFieldIdentifier::new("version", TType::I32, 1))?;
    prot.write_i32(metadata.version)?;
    prot.write_field_end()?;

    // Field 2: schema
    prot.write_field_begin(&TFieldIdentifier::new("schema", TType::List, 2))?;
    prot.write_list_begin(&TListIdentifier::new(
        TType::Struct,
        metadata.schema.len() as u32,
    ))?;
    for elem in &metadata.schema {
        elem.write_to_out_protocol(&mut prot)?;
    }
    prot.write_list_end()?;
    prot.write_field_end()?;

    // Field 3: num_rows
    prot.write_field_begin(&TFieldIdentifier::new("num_rows", TType::I64, 3))?;
    prot.write_i64(metadata.num_rows)?;
    prot.write_field_end()?;

    // Field 4: row_groups
    prot.write_field_begin(&TFieldIdentifier::new("row_groups", TType::List, 4))?;
    prot.write_list_begin(&TListIdentifier::new(
        TType::Struct,
        metadata.row_groups.len() as u32,
    ))?;

    // Write row groups: cached raw bytes or freshly serialized.
    // Cached row group bytes are self-contained Thrift structs whose
    // serialization is independent of the outer protocol state, so injecting
    // them directly is safe. The protocol's field-delta tracking is unaffected
    // because struct_begin/struct_end (which would push/pop the stack) write
    // zero bytes and the push/pop is balanced.
    for (i, source) in sources.iter().enumerate() {
        match source {
            RowGroupSource::Cached(orig_idx) if *orig_idx < cache.row_group_count() => {
                shared.write_raw(cache.row_group_bytes(*orig_idx))?;
            }
            _ => {
                metadata.row_groups[i].write_to_out_protocol(&mut prot)?;
            }
        }
    }

    prot.write_list_end()?;
    prot.write_field_end()?;

    // Field 5: key_value_metadata (optional)
    if let Some(ref kv) = metadata.key_value_metadata {
        prot.write_field_begin(&TFieldIdentifier::new("key_value_metadata", TType::List, 5))?;
        prot.write_list_begin(&TListIdentifier::new(TType::Struct, kv.len() as u32))?;
        for entry in kv {
            entry.write_to_out_protocol(&mut prot)?;
        }
        prot.write_list_end()?;
        prot.write_field_end()?;
    }

    // Field 6: created_by (optional)
    if let Some(ref created_by) = metadata.created_by {
        prot.write_field_begin(&TFieldIdentifier::new("created_by", TType::String, 6))?;
        prot.write_string(created_by)?;
        prot.write_field_end()?;
    }

    // Field 7: column_orders (optional)
    if let Some(ref orders) = metadata.column_orders {
        prot.write_field_begin(&TFieldIdentifier::new("column_orders", TType::List, 7))?;
        prot.write_list_begin(&TListIdentifier::new(TType::Struct, orders.len() as u32))?;
        for order in orders {
            order.write_to_out_protocol(&mut prot)?;
        }
        prot.write_list_end()?;
        prot.write_field_end()?;
    }

    // Field 8: encryption_algorithm (optional)
    if let Some(ref algo) = metadata.encryption_algorithm {
        prot.write_field_begin(&TFieldIdentifier::new(
            "encryption_algorithm",
            TType::Struct,
            8,
        ))?;
        algo.write_to_out_protocol(&mut prot)?;
        prot.write_field_end()?;
    }

    // Field 9: footer_signing_key_metadata (optional)
    if let Some(ref key_meta) = metadata.footer_signing_key_metadata {
        prot.write_field_begin(&TFieldIdentifier::new(
            "footer_signing_key_metadata",
            TType::String,
            9,
        ))?;
        prot.write_bytes(key_meta)?;
        prot.write_field_end()?;
    }

    prot.write_field_stop()?;
    prot.write_struct_end()?;

    let bytes_written = shared.bytes_written();
    drop(prot);

    // Write footer (metadata length + PAR1 magic)
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    footer_buffer[..4].copy_from_slice(&(bytes_written as i32).to_le_bytes());
    footer_buffer[4..].copy_from_slice(&PARQUET_MAGIC);
    // Write footer through the raw writer (protocol is dropped, RefCell is
    // no longer borrowed).
    shared.write_raw(&footer_buffer)?;
    // No-op for unbuffered writers (e.g. File), but needed for consistency
    // with end_file() if the writer is ever wrapped in a BufWriter.
    shared.flush()?;

    Ok(bytes_written + FOOTER_SIZE)
}

pub fn end_file<W: Write>(mut writer: &mut W, metadata: &ThriftFileMetaData) -> Result<u64> {
    // Write metadata
    let mut protocol = TCompactOutputProtocol::new(&mut writer);
    let metadata_len = metadata.write_to_out_protocol(&mut protocol)? as i32;

    // Write footer
    let metadata_bytes = metadata_len.to_le_bytes();
    let mut footer_buffer = [0u8; FOOTER_SIZE as usize];
    (0..4).for_each(|i| {
        footer_buffer[i] = metadata_bytes[i];
    });

    (&mut footer_buffer[4..]).write_all(&PARQUET_MAGIC)?;
    writer.write_all(&footer_buffer)?;
    writer.flush()?;
    Ok(metadata_len as u64 + FOOTER_SIZE)
}

/// An interface to write a parquet file.
/// Use `start` to write the header, `write` to write a row group,
/// and `end` to write the footer.
pub struct FileWriter<W: Write> {
    writer: W,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    sorting_columns: Option<Vec<SortingColumn>>,
    offset: u64,
    row_groups: Vec<RowGroup>,
    page_specs: Vec<Vec<Vec<PageWriteSpec>>>,
    /// Used to store the current state for writing the file
    state: State,
    // when the file is written, metadata becomes available
    metadata: Option<ThriftFileMetaData>,
}

/// Writes a parquet file containing only the header and footer
///
/// This is used to write the metadata as a separate Parquet file, usually when data
/// is partitioned across multiple files.
///
/// Note: Recall that when combining row groups from [`ThriftFileMetaData`], the `file_path` on each
/// of their column chunks must be updated with their path relative to where they are written to.
pub fn write_metadata_sidecar<W: Write>(
    writer: &mut W,
    metadata: &ThriftFileMetaData,
) -> Result<u64> {
    let mut len = start_file(writer)?;
    len += end_file(writer, metadata)?;
    Ok(len)
}

// Accessors
impl<W: Write> FileWriter<W> {
    /// The options assigned to the file
    pub fn options(&self) -> &WriteOptions {
        &self.options
    }

    /// The [`SchemaDescriptor`] assigned to this file
    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Returns the [`ThriftFileMetaData`]. This is Some iff the [`Self::end`] has been called.
    ///
    /// This is used to write the metadata as a separate Parquet file, usually when data
    /// is partitioned across multiple files
    pub fn metadata(&self) -> Option<&ThriftFileMetaData> {
        self.metadata.as_ref()
    }
}

impl<W: Write> FileWriter<W> {
    /// Returns a new [`FileWriter`].
    pub fn new(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            sorting_columns: None,
            offset: 0,
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: None,
        }
    }

    pub fn with_sorting_columns(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
        sorting_columns: Option<Vec<SortingColumn>>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            sorting_columns,
            offset: 0,
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: None,
        }
    }

    /// Writes the header of the file.
    ///
    /// This is automatically called by [`Self::write`] if not called following [`Self::new`].
    ///
    /// # Errors
    /// Returns an error if data has been written to the file.
    fn start(&mut self) -> Result<()> {
        if self.offset == 0 {
            self.offset = start_file(&mut self.writer)?;
            self.state = State::Started;
            Ok(())
        } else {
            Err(Error::InvalidParameter(
                "Start cannot be called twice".to_string(),
            ))
        }
    }

    /// Writes a row group to the file.
    ///
    /// This call is IO-bounded
    pub fn write<E>(
        &mut self,
        row_group: RowGroupIter<'_, E>,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        if self.offset == 0 {
            self.start()?;
        }
        let ordinal = self.row_groups.len();
        let expected_cols = self.schema.columns().len();
        debug_assert!(
            bloom_hashes.is_empty() || bloom_hashes.len() == expected_cols,
            "bloom_hashes length mismatch: expected {}, got {}",
            expected_cols,
            bloom_hashes.len()
        );
        let default_bloom: Vec<Option<Arc<Mutex<HashSet<u64>>>>>;
        let bloom_hashes = if bloom_hashes.is_empty() {
            default_bloom = vec![None; expected_cols];
            &default_bloom
        } else {
            bloom_hashes
        };
        let (group, specs, size) = write_row_group(
            &mut self.writer,
            self.offset,
            self.schema.columns(),
            row_group,
            &self.sorting_columns,
            ordinal,
            self.options.bloom_filter_fpp,
            bloom_hashes,
        )?;
        self.offset += size;
        self.row_groups.push(group);
        self.page_specs.push(specs);
        Ok(())
    }

    /// Writes the footer of the parquet file. Returns the total size of the file and the
    /// underlying writer.
    pub fn end(&mut self, additional_meta: Option<Vec<KeyValue>>) -> Result<u64> {
        if self.offset == 0 {
            self.start()?;
        }

        if self.state != State::Started {
            return Err(Error::InvalidParameter(
                "End cannot be called twice".to_string(),
            ));
        }
        // compute file stats
        let num_rows = self.row_groups.iter().map(|group| group.num_rows).sum();

        if self.options.write_statistics {
            // write column indexes (require page statistics)
            self.row_groups
                .iter_mut()
                .zip(self.page_specs.iter())
                .try_for_each(|(group, pages)| {
                    group.columns.iter_mut().zip(pages.iter()).try_for_each(
                        |(column, pages)| {
                            let offset = self.offset;
                            column.column_index_offset = Some(offset as i64);
                            self.offset += write_column_index(&mut self.writer, pages)?;
                            let length = self.offset - offset;
                            column.column_index_length = Some(length as i32);
                            Result::Ok(())
                        },
                    )?;
                    Result::Ok(())
                })?;
        };

        // write offset index
        self.row_groups
            .iter_mut()
            .zip(self.page_specs.iter())
            .try_for_each(|(group, pages)| {
                group
                    .columns
                    .iter_mut()
                    .zip(pages.iter())
                    .try_for_each(|(column, pages)| {
                        let offset = self.offset;
                        column.offset_index_offset = Some(offset as i64);
                        self.offset += write_offset_index(&mut self.writer, pages)?;
                        column.offset_index_length = Some((self.offset - offset) as i32);
                        Result::Ok(())
                    })?;
                Result::Ok(())
            })?;

        let metadata = ThriftFileMetaData::new(
            self.options.version.into(),
            self.schema.clone().into_thrift(),
            num_rows,
            self.row_groups.clone(),
            additional_meta,
            self.created_by.clone(),
            None,
            None,
            None,
        );

        let len = end_file(&mut self.writer, &metadata)?;
        self.state = State::Finished;
        self.metadata = Some(metadata);
        Ok(self.offset + len)
    }

    /// Returns the underlying writer.
    pub fn into_inner(self) -> W {
        self.writer
    }

    /// Returns the underlying writer and [`ThriftFileMetaData`]
    /// # Panics
    /// This function panics if [`Self::end`] has not yet been called
    pub fn into_inner_and_metadata(self) -> (W, ThriftFileMetaData) {
        (self.writer, self.metadata.expect("File to have ended"))
    }
}

// TODO: replace `FileWriter` with `ParquetFile` and deprecate `FileWriter`
pub struct ParquetFile<W: Write> {
    writer: W,
    schema: SchemaDescriptor,
    options: WriteOptions,
    created_by: Option<String>,
    sorting_columns: Option<Vec<SortingColumn>>,
    offset: u64,
    row_groups: Vec<RowGroup>,
    page_specs: Vec<Vec<Vec<PageWriteSpec>>>,
    state: State,
    metadata: Option<ThriftFileMetaData>,
    mode: Mode,
    is_insert: Vec<bool>,
}

#[allow(clippy::large_enum_variant)]
pub enum Mode {
    Write,
    Update(ThriftFileMetaData, Option<FooterCache>),
}

impl<W: Write> ParquetFile<W> {
    pub fn new(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            sorting_columns: None,
            offset: 0,
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: None,
            mode: Mode::Write,
            is_insert: vec![],
        }
    }

    pub fn with_sorting_columns(
        writer: W,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
        sorting_columns: Option<Vec<SortingColumn>>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            sorting_columns,
            offset: 0,
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: None,
            mode: Mode::Write,
            is_insert: vec![],
        }
    }

    pub fn new_updater(
        writer: W,
        offset: u64,
        schema: SchemaDescriptor,
        options: WriteOptions,
        created_by: Option<String>,
        sorting_columns: Option<Vec<SortingColumn>>,
        metadata: ThriftFileMetaData,
        footer_cache: Option<FooterCache>,
    ) -> Self {
        Self {
            writer,
            schema,
            options,
            created_by,
            sorting_columns,
            offset,
            row_groups: vec![],
            page_specs: vec![],
            state: State::Initialised,
            metadata: Some(metadata.clone()), //TODO: do we need to keep it here?
            mode: Mode::Update(metadata, footer_cache),
            is_insert: vec![],
        }
    }

    pub fn options(&self) -> &WriteOptions {
        &self.options
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    pub fn set_schema(&mut self, schema: SchemaDescriptor) {
        self.schema = schema;
    }

    pub fn metadata(&self) -> Option<&ThriftFileMetaData> {
        self.metadata.as_ref()
    }

    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    fn start(&mut self) -> Result<()> {
        if self.offset == 0 {
            self.offset = start_file(&mut self.writer)?;
            self.state = State::Started;
            Ok(())
        } else {
            Err(Error::InvalidParameter(
                "Start cannot be called twice".to_string(),
            ))
        }
    }

    pub fn write<E>(
        &mut self,
        row_group: RowGroupIter<'_, E>,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        if self.offset == 0 {
            self.start()?;
        }
        let ordinal = self.row_groups.len();
        self.add_row_group(row_group, ordinal, bloom_hashes)?;
        self.is_insert.push(false);
        Ok(())
    }

    fn add_row_group<E>(
        &mut self,
        row_group: RowGroupIter<E>,
        ordinal: usize,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        let expected_cols = self.schema.columns().len();
        debug_assert!(
            bloom_hashes.is_empty() || bloom_hashes.len() == expected_cols,
            "bloom_hashes length mismatch: expected {}, got {}",
            expected_cols,
            bloom_hashes.len()
        );
        let default_bloom: Vec<Option<Arc<Mutex<HashSet<u64>>>>>;
        let bloom_hashes = if bloom_hashes.is_empty() {
            default_bloom = vec![None; expected_cols];
            &default_bloom
        } else {
            bloom_hashes
        };
        let (group, specs, size) = write_row_group(
            &mut self.writer,
            self.offset,
            self.schema.columns(),
            row_group,
            &self.sorting_columns,
            ordinal,
            self.options.bloom_filter_fpp,
            bloom_hashes,
        )?;

        self.offset += size;
        self.row_groups.push(group);
        self.page_specs.push(specs);
        Ok(())
    }

    pub fn replace<E>(
        &mut self,
        row_group: RowGroupIter<'_, E>,
        ordinal: Option<i32>,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        match &self.mode {
            Mode::Update(metadata, _) => {
                let ordinal = if let Some(ordinal) = ordinal {
                    if ordinal < 0 {
                        return Err(Error::InvalidParameter(format!(
                            "Row group ordinal must be non-negative, got {}",
                            ordinal
                        ))
                        .into());
                    }
                    let num_row_groups = metadata.row_groups.len();
                    let ordinal = ordinal as usize;
                    if ordinal >= num_row_groups {
                        return Err(Error::InvalidParameter(format!(
                            "Row group ordinal must be less then {}, got {}",
                            num_row_groups, ordinal
                        ))
                        .into());
                    }
                    ordinal
                } else {
                    metadata.row_groups.len() + self.row_groups.len()
                };
                self.add_row_group(row_group, ordinal, bloom_hashes)?;
                self.is_insert.push(false);
                Ok(())
            }
            _ => Err(Error::InvalidParameter(
                "Replace can only be called in update mode".to_string(),
            )
            .into()),
        }
    }

    pub fn insert<E>(
        &mut self,
        row_group: RowGroupIter<'_, E>,
        position: i32,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        if position < 0 {
            return Err(Error::InvalidParameter(format!(
                "Insert position must be non-negative, got {}",
                position
            ))
            .into());
        }
        match &self.mode {
            Mode::Update(_, _) => {
                self.add_row_group(row_group, position as usize, bloom_hashes)?;
                self.is_insert.push(true);
                Ok(())
            }
            _ => Err(Error::InvalidParameter(
                "Insert can only be called in update mode".to_string(),
            )
            .into()),
        }
    }

    pub fn append<E>(
        &mut self,
        row_group: RowGroupIter<'_, E>,
        bloom_hashes: &[Option<Arc<Mutex<HashSet<u64>>>>],
    ) -> std::result::Result<(), E>
    where
        E: std::error::Error + From<Error>,
    {
        self.replace(row_group, None, bloom_hashes)
    }

    /// Ensures the PAR1 file header has been written.
    /// Must be called before computing offsets for raw row group copies.
    pub fn ensure_started(&mut self) -> Result<()> {
        if self.offset == 0 {
            self.start()?;
        }
        Ok(())
    }

    /// Returns the current write offset in the file.
    pub fn current_offset(&self) -> u64 {
        self.offset
    }

    /// Write raw (pre-encoded) row group bytes and register the row group metadata.
    /// The `row_group` must have all offsets already adjusted for the new file.
    /// Callers must call `ensure_started()` before computing those offsets,
    /// otherwise the PAR1 header written here shifts data by 4 bytes.
    pub fn write_raw_row_group(&mut self, raw_bytes: &[u8], row_group: RowGroup) -> Result<()> {
        if self.offset == 0 {
            self.start()?;
        }
        self.writer.write_all(raw_bytes)?;
        self.offset += raw_bytes.len() as u64;
        self.row_groups.push(row_group);
        self.page_specs.push(vec![]);
        self.is_insert.push(false);
        Ok(())
    }

    pub fn end(&mut self, key_value_metadata: Option<Vec<KeyValue>>) -> Result<u64> {
        match &mut self.mode {
            Mode::Write => {
                if self.offset == 0 {
                    self.start()?;
                }

                if self.state != State::Started {
                    return Err(Error::InvalidParameter(
                        "End cannot be called twice".to_string(),
                    ));
                }

                let num_rows = self.row_groups.iter().map(|group| group.num_rows).sum();
                let metadata = ThriftFileMetaData::new(
                    self.options.version.into(),
                    self.schema.clone().into_thrift(),
                    num_rows,
                    self.row_groups.clone(),
                    key_value_metadata,
                    self.created_by.clone(),
                    None,
                    None,
                    None,
                );

                let len = end_file(&mut self.writer, &metadata)?;
                self.state = State::Finished;
                self.metadata = Some(metadata);
                Ok(self.offset + len)
            }
            Mode::Update(metadata, footer_cache) => {
                let mut num_rows = metadata.num_rows;
                let original_rg_count = metadata.row_groups.len();

                // Track which original row groups have been replaced.
                let mut modified = vec![false; original_rg_count];

                // Drain row_groups and is_insert so we can move instead of clone.
                let groups = std::mem::take(&mut self.row_groups);
                let is_insert_flags = std::mem::take(&mut self.is_insert);

                // Partition into replacements/appends and insertions.
                let mut insertion_groups = Vec::new();
                for (group, is_ins) in groups.into_iter().zip(is_insert_flags.iter()) {
                    if *is_ins {
                        insertion_groups.push(group);
                        continue;
                    }
                    let ordinal = group
                        .ordinal
                        .ok_or_else(|| Error::oos("Row group ordinal is missing"))?;
                    let new_rows = group.num_rows;
                    if (ordinal as usize) < original_rg_count {
                        num_rows -= metadata.row_groups[ordinal as usize].num_rows;
                        metadata.row_groups[ordinal as usize] = group;
                        modified[ordinal as usize] = true;
                    } else {
                        metadata.row_groups.push(group);
                    }
                    num_rows += new_rows;
                }

                // Build a mapping: for each position in the final row_groups list,
                // record whether it came from an original (possibly cached) entry
                // or is a freshly inserted/modified entry.
                let mut sources: Vec<RowGroupSource> = (0..metadata.row_groups.len())
                    .map(|i| {
                        if i < original_rg_count && !modified[i] {
                            RowGroupSource::Cached(i)
                        } else {
                            RowGroupSource::Fresh
                        }
                    })
                    .collect();

                // Phase 2: insertions in ascending ordinal order
                let mut insertions: Vec<_> = insertion_groups
                    .into_iter()
                    .map(|g| {
                        let pos = g.ordinal.unwrap_or(0) as usize;
                        (pos, g)
                    })
                    .collect();
                insertions.sort_by_key(|(pos, _)| *pos);

                for (pos, group) in insertions {
                    let adjusted_pos = pos.min(metadata.row_groups.len());
                    num_rows += group.num_rows;
                    metadata.row_groups.insert(adjusted_pos, group);
                    sources.insert(adjusted_pos, RowGroupSource::Inserted);
                }

                metadata.num_rows = num_rows;
                match metadata.key_value_metadata.take() {
                    None => metadata.key_value_metadata = key_value_metadata,
                    Some(mut kv) => {
                        if let Some(new_kv) = key_value_metadata {
                            for new_entry in new_kv {
                                if let Some(existing) =
                                    kv.iter_mut().find(|e| e.key == new_entry.key)
                                {
                                    *existing = new_entry;
                                } else {
                                    kv.push(new_entry);
                                }
                            }
                        }
                        metadata.key_value_metadata = Some(kv);
                    }
                }

                let len = end_file_incremental(&mut self.writer, metadata, footer_cache, &sources)?;
                self.state = State::Finished;
                Ok(self.offset + len)
            }
        }
    }

    pub fn into_inner(self) -> W {
        self.writer
    }

    pub fn into_inner_and_metadata(self) -> (W, ThriftFileMetaData) {
        (self.writer, self.metadata.expect("File to have ended"))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Cursor};

    use super::*;

    use crate::error::Result;
    use crate::read::read_metadata;
    use crate::tests::get_path;

    #[test]
    fn empty_file() -> Result<()> {
        let mut testdata = get_path();
        testdata.push("alltypes_plain.parquet");
        let mut file = File::open(testdata).unwrap();

        let mut metadata = read_metadata(&mut file)?;

        // take away all groups and rows
        metadata.row_groups = vec![];
        metadata.num_rows = 0;

        let mut writer = Cursor::new(vec![]);

        // write the file
        start_file(&mut writer)?;
        end_file(&mut writer, &metadata.into_thrift())?;

        let a = writer.into_inner();

        // read it again:
        let result = read_metadata(&mut Cursor::new(a));
        assert!(result.is_ok());

        Ok(())
    }
}
