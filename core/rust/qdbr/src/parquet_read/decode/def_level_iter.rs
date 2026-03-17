#![allow(dead_code)]
//! `DefLevelBatchIter` — micro-batch iterator over definition levels.
//!
//! Replaces the monolithic `decode_page0` / `decode_page0_filtered` approach
//! with an iterator that yields batches of N rows at a time, keeping working
//! sets in L1/L2 cache. Supports both unfiltered and filtered (row selection)
//! decode modes.

use crate::parquet::error::ParquetResult;
use crate::parquet_read::column_sink::Pushable;
use crate::parquet_read::page::DataPage;

use parquet2::deserialize::{HybridDecoderBitmapIter, HybridEncoded};

use super::{count_ones_in_bitmap, decode_null_bitmap, get_bit_at};

/// Partially consumed run from the definition level stream.
enum PendingRun<'a> {
    /// No pending run — need to fetch from the iterator.
    None,
    /// Run of `count` non-null values.
    Set(usize),
    /// Run of `count` null values.
    Null(usize),
    /// Bitmap with remaining bits to process.
    Bitmap {
        values: &'a [u8],
        bit_offset: usize,
        remaining: usize,
    },
}

/// State for filtered decode (skip-mode only, no FILL_NULLS).
struct FilterState<'a> {
    rows_filter: &'a [i64],
    filter_idx: usize,
    page_row_start: usize,
    row_group_lo: usize,
    /// Absolute row position within the page (0-based).
    current_page_row: usize,
}

/// Micro-batch iterator over definition levels.
///
/// Processes N rows at a time via `decode_batch`, driving a `Pushable<Sink>`
/// to write decoded values. Supports:
/// - **Unfiltered**: decode rows `[row_lo, row_hi)` from the page.
/// - **Filtered**: decode only rows in `rows_filter`, skip others.
pub struct DefLevelBatchIter<'a> {
    /// Hybrid-RLE def level iterator (None = all values are non-null).
    iter: Option<HybridDecoderBitmapIter<'a>>,
    /// Partially consumed run from the def level stream.
    pending: PendingRun<'a>,
    /// Non-null values to skip before producing output (row_lo offset).
    skip_remaining: usize,
    /// Total output rows remaining.
    output_remaining: usize,
    /// Optional row selection filter.
    filter: Option<FilterState<'a>>,
    /// True when all non-null values in skipped rows have been consumed.
    skip_done: bool,
}

impl<'a> DefLevelBatchIter<'a> {
    /// Create an unfiltered iterator for rows `[row_lo, row_hi)`.
    pub fn new(page: &DataPage<'a>, row_lo: usize, row_hi: usize) -> ParquetResult<Self> {
        let iter = decode_null_bitmap(page, row_hi)?;
        let has_nulls = iter.is_some();
        Ok(Self {
            iter,
            pending: PendingRun::None,
            skip_remaining: row_lo,
            output_remaining: row_hi - row_lo,
            filter: None,
            skip_done: !has_nulls && row_lo == 0,
        })
    }

    /// Create a filtered iterator that decodes only rows in `rows_filter`.
    pub fn new_filtered(
        page: &DataPage<'a>,
        page_row_start: usize,
        page_row_count: usize,
        row_group_lo: usize,
        rows_filter: &'a [i64],
    ) -> ParquetResult<Self> {
        let iter = decode_null_bitmap(page, page_row_count)?;
        let output_count = rows_filter.len();
        Ok(Self {
            iter,
            pending: PendingRun::None,
            skip_remaining: 0,
            output_remaining: output_count,
            filter: Some(FilterState {
                rows_filter,
                filter_idx: 0,
                page_row_start,
                row_group_lo,
                current_page_row: 0,
            }),
            skip_done: true,
        })
    }

    /// Number of output rows remaining.
    #[inline]
    pub fn output_count(&self) -> usize {
        self.output_remaining
    }

    /// True when all output rows have been produced.
    #[inline]
    pub fn is_exhausted(&self) -> bool {
        self.output_remaining == 0
    }

    /// Decode up to `batch_size` output rows.
    ///
    /// Returns the number of rows actually produced (may be less than
    /// `batch_size` if the page is exhausted).
    pub fn decode_batch<Sink, P: Pushable<Sink>>(
        &mut self,
        pushable: &mut P,
        sink: &mut Sink,
        batch_size: usize,
    ) -> ParquetResult<usize> {
        if self.filter.is_some() {
            self.decode_batch_filtered(pushable, sink, batch_size)
        } else {
            self.decode_batch_unfiltered(pushable, sink, batch_size)
        }
    }

    /// Unfiltered decode: process def levels up to `batch_size` rows.
    fn decode_batch_unfiltered<Sink, P: Pushable<Sink>>(
        &mut self,
        pushable: &mut P,
        sink: &mut Sink,
        batch_size: usize,
    ) -> ParquetResult<usize> {
        let budget = batch_size.min(self.output_remaining);
        if budget == 0 {
            return Ok(0);
        }

        // Phase 1: skip row_lo rows (advance decoder without output).
        if !self.skip_done {
            self.drain_skip(pushable)?;
        }

        // Phase 2: produce output rows.
        if self.iter.is_none() {
            // No null bitmap — all values are non-null.
            pushable.push_slice(sink, budget)?;
            self.output_remaining -= budget;
            return Ok(budget);
        }

        let mut produced = 0usize;
        while produced < budget {
            let remaining_budget = budget - produced;

            // Refill pending run if empty.
            if matches!(self.pending, PendingRun::None) {
                if !self.next_run()? {
                    break;
                }
            }

            match &mut self.pending {
                PendingRun::Set(count) => {
                    let n = remaining_budget.min(*count);
                    pushable.push_slice(sink, n)?;
                    *count -= n;
                    produced += n;
                    if *count == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Null(count) => {
                    let n = remaining_budget.min(*count);
                    pushable.push_nulls(sink, n)?;
                    *count -= n;
                    produced += n;
                    if *count == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Bitmap {
                    values,
                    bit_offset,
                    remaining,
                } => {
                    let n = remaining_budget.min(*remaining);
                    produced += Self::process_bitmap(values, bit_offset, remaining, n, pushable, sink)?;
                }
                PendingRun::None => unreachable!(),
            }
        }

        self.output_remaining -= produced;
        Ok(produced)
    }

    /// Filtered decode: intersect def levels with `rows_filter`.
    fn decode_batch_filtered<Sink, P: Pushable<Sink>>(
        &mut self,
        pushable: &mut P,
        sink: &mut Sink,
        batch_size: usize,
    ) -> ParquetResult<usize> {
        let budget = batch_size.min(self.output_remaining);
        if budget == 0 {
            return Ok(0);
        }

        let filter_state = self.filter.as_mut().unwrap();
        let rows_filter = filter_state.rows_filter;
        let filter_len = rows_filter.len();
        let page_row_start = filter_state.page_row_start;
        let row_group_lo = filter_state.row_group_lo;

        if self.iter.is_none() {
            // No null bitmap — all values non-null. Batch consecutive filter rows.
            let mut produced = 0usize;
            let filter_state = self.filter.as_mut().unwrap();

            while produced < budget && filter_state.filter_idx < filter_len {
                let first_row = rows_filter[filter_state.filter_idx] as usize
                    + row_group_lo
                    - page_row_start;

                // Skip values before this filter row.
                if filter_state.current_page_row < first_row {
                    pushable.skip(first_row - filter_state.current_page_row)?;
                    filter_state.current_page_row = first_row;
                }

                // Count consecutive filter rows.
                let mut consecutive = 1usize;
                let remaining_budget = budget - produced;
                while consecutive < remaining_budget
                    && filter_state.filter_idx + consecutive < filter_len
                {
                    let curr = rows_filter[filter_state.filter_idx + consecutive - 1] as usize;
                    let next = rows_filter[filter_state.filter_idx + consecutive] as usize;
                    if next != curr + 1 {
                        break;
                    }
                    consecutive += 1;
                }

                pushable.push_slice(sink, consecutive)?;
                filter_state.current_page_row += consecutive;
                filter_state.filter_idx += consecutive;
                produced += consecutive;
            }

            self.output_remaining -= produced;
            return Ok(produced);
        }

        // With null bitmap: process run-by-run, intersecting with filter.
        let mut produced = 0usize;

        while produced < budget {
            let filter_state = self.filter.as_mut().unwrap();
            if filter_state.filter_idx >= filter_len {
                break;
            }

            // Refill pending run if empty.
            if matches!(self.pending, PendingRun::None) {
                if !self.next_run()? {
                    break;
                }
            }

            let filter_state = self.filter.as_mut().unwrap();
            let target_page_row = rows_filter[filter_state.filter_idx] as usize
                + row_group_lo
                - page_row_start;

            match &mut self.pending {
                PendingRun::Set(count) => {
                    let run_start = filter_state.current_page_row;
                    let run_end = run_start + *count;

                    if target_page_row >= run_end {
                        // Entire run is before next filter row — skip it.
                        pushable.skip(*count)?;
                        filter_state.current_page_row += *count;
                        self.pending = PendingRun::None;
                        continue;
                    }

                    // Skip values before the target row within this run.
                    let skip = target_page_row - run_start;
                    if skip > 0 {
                        pushable.skip(skip)?;
                        *count -= skip;
                        filter_state.current_page_row += skip;
                    }

                    // Count consecutive filter rows within this run.
                    let remaining_budget = budget - produced;
                    let mut consecutive = 1usize;
                    while consecutive < remaining_budget
                        && consecutive < *count
                        && filter_state.filter_idx + consecutive < filter_len
                    {
                        let curr = rows_filter[filter_state.filter_idx + consecutive - 1] as usize;
                        let next = rows_filter[filter_state.filter_idx + consecutive] as usize;
                        if next != curr + 1 {
                            break;
                        }
                        // Check the next row is still within this run.
                        let next_page_row = next + row_group_lo - page_row_start;
                        if next_page_row >= run_end {
                            break;
                        }
                        consecutive += 1;
                    }

                    pushable.push_slice(sink, consecutive)?;
                    *count -= consecutive;
                    filter_state.current_page_row += consecutive;
                    filter_state.filter_idx += consecutive;
                    produced += consecutive;

                    if *count == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Null(count) => {
                    let run_start = filter_state.current_page_row;
                    let run_end = run_start + *count;

                    if target_page_row >= run_end {
                        // Entire null run is before next filter row — skip (no values to skip).
                        filter_state.current_page_row += *count;
                        self.pending = PendingRun::None;
                        continue;
                    }

                    // Count filter rows within this null run.
                    let remaining_budget = budget - produced;
                    let mut consecutive = 1usize;
                    while consecutive < remaining_budget
                        && filter_state.filter_idx + consecutive < filter_len
                    {
                        let next = rows_filter[filter_state.filter_idx + consecutive] as usize;
                        let next_page_row = next + row_group_lo - page_row_start;
                        if next_page_row >= run_end {
                            break;
                        }
                        // Null runs don't care about consecutiveness — all are null.
                        consecutive += 1;
                    }

                    pushable.push_nulls(sink, consecutive)?;
                    filter_state.filter_idx += consecutive;
                    produced += consecutive;

                    // Advance past this run (we've consumed all filter rows in it).
                    // Note: we don't decrement *count per-row since nulls have no
                    // decoder values to skip. Just advance page_row past the run.
                    let last_filter_row = rows_filter[filter_state.filter_idx - 1] as usize
                        + row_group_lo
                        - page_row_start;
                    filter_state.current_page_row = last_filter_row + 1;

                    // If we haven't consumed the whole run, keep it pending.
                    if filter_state.current_page_row < run_end {
                        *count = run_end - filter_state.current_page_row;
                    } else {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Bitmap {
                    values,
                    bit_offset,
                    remaining,
                } => {
                    // Process one filter row at a time within the bitmap.
                    while produced < budget && filter_state.filter_idx < filter_len && *remaining > 0
                    {
                        let target = rows_filter[filter_state.filter_idx] as usize
                            + row_group_lo
                            - page_row_start;

                        let bitmap_page_end =
                            filter_state.current_page_row + *remaining;

                        if target >= bitmap_page_end {
                            // Target is beyond this bitmap run.
                            break;
                        }

                        // Skip bits before the target row.
                        let skip_rows = target - filter_state.current_page_row;
                        if skip_rows > 0 {
                            let non_null_count =
                                count_ones_in_bitmap(values, *bit_offset, skip_rows);
                            pushable.skip(non_null_count)?;
                            *bit_offset += skip_rows;
                            *remaining -= skip_rows;
                            filter_state.current_page_row += skip_rows;
                        }

                        // Emit the target row.
                        if get_bit_at(values, *bit_offset) {
                            pushable.push(sink)?;
                        } else {
                            pushable.push_null(sink)?;
                        }
                        *bit_offset += 1;
                        *remaining -= 1;
                        filter_state.current_page_row += 1;
                        filter_state.filter_idx += 1;
                        produced += 1;
                    }

                    if *remaining == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::None => unreachable!(),
            }
        }

        self.output_remaining -= produced;
        Ok(produced)
    }

    /// Skip `self.skip_remaining` rows (unfiltered path).
    /// Advances the decoder without producing output.
    fn drain_skip<Sink, P: Pushable<Sink>>(&mut self, pushable: &mut P) -> ParquetResult<()> {
        if self.iter.is_none() {
            // No nulls — skip means advance the value stream.
            pushable.skip(self.skip_remaining)?;
            self.skip_remaining = 0;
            self.skip_done = true;
            return Ok(());
        }

        while self.skip_remaining > 0 {
            if matches!(self.pending, PendingRun::None) {
                if !self.next_run()? {
                    break;
                }
            }

            match &mut self.pending {
                PendingRun::Set(count) => {
                    let n = self.skip_remaining.min(*count);
                    pushable.skip(n)?;
                    *count -= n;
                    self.skip_remaining -= n;
                    if *count == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Null(count) => {
                    let n = self.skip_remaining.min(*count);
                    // Null rows don't consume decoder values.
                    *count -= n;
                    self.skip_remaining -= n;
                    if *count == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::Bitmap {
                    values,
                    bit_offset,
                    remaining,
                } => {
                    let n = self.skip_remaining.min(*remaining);
                    let non_null = count_ones_in_bitmap(values, *bit_offset, n);
                    pushable.skip(non_null)?;
                    *bit_offset += n;
                    *remaining -= n;
                    self.skip_remaining -= n;
                    if *remaining == 0 {
                        self.pending = PendingRun::None;
                    }
                }
                PendingRun::None => unreachable!(),
            }
        }

        self.skip_done = true;
        Ok(())
    }

    /// Fetch the next run from the HybridDecoderBitmapIter and store in `self.pending`.
    /// Returns false if the iterator is exhausted.
    fn next_run(&mut self) -> ParquetResult<bool> {
        let iter = match self.iter.as_mut() {
            Some(it) => it,
            None => return Ok(false),
        };

        match iter.next() {
            Some(Ok(HybridEncoded::Repeated(is_set, length))) => {
                if is_set {
                    self.pending = PendingRun::Set(length);
                } else {
                    self.pending = PendingRun::Null(length);
                }
                Ok(true)
            }
            Some(Ok(HybridEncoded::Bitmap(values, length))) => {
                self.pending = PendingRun::Bitmap {
                    values,
                    bit_offset: 0,
                    remaining: length,
                };
                Ok(true)
            }
            Some(Err(e)) => Err(e.into()),
            None => Ok(false),
        }
    }

    /// Process `n` bits from a bitmap, calling push_slice/push_nulls.
    /// Uses word-at-a-time processing when possible.
    /// Returns the number of output rows produced (always `n`).
    fn process_bitmap<Sink, P: Pushable<Sink>>(
        values: &[u8],
        bit_offset: &mut usize,
        remaining: &mut usize,
        n: usize,
        pushable: &mut P,
        sink: &mut Sink,
    ) -> ParquetResult<usize> {
        let mut pos = *bit_offset;
        let end = pos + n;
        let mut consecutive_true = 0usize;
        let mut consecutive_false = 0usize;

        // Handle unaligned start bits.
        let start_bit = pos & 7;
        if start_bit != 0 {
            let bits_in_byte = (8 - start_bit).min(end - pos);
            let byte = values[pos >> 3] >> start_bit;
            for i in 0..bits_in_byte {
                if (byte >> i) & 1 == 1 {
                    if consecutive_false > 0 {
                        pushable.push_nulls(sink, consecutive_false)?;
                        consecutive_false = 0;
                    }
                    consecutive_true += 1;
                } else {
                    if consecutive_true > 0 {
                        pushable.push_slice(sink, consecutive_true)?;
                        consecutive_true = 0;
                    }
                    consecutive_false += 1;
                }
            }
            pos += bits_in_byte;
        }

        // Process 64 bits at a time.
        while pos + 64 <= end && (pos >> 3) + 8 <= values.len() {
            let word = unsafe { (values.as_ptr().add(pos >> 3) as *const u64).read_unaligned() };

            if word == u64::MAX {
                if consecutive_false > 0 {
                    pushable.push_nulls(sink, consecutive_false)?;
                    consecutive_false = 0;
                }
                consecutive_true += 64;
            } else if word == 0 {
                if consecutive_true > 0 {
                    pushable.push_slice(sink, consecutive_true)?;
                    consecutive_true = 0;
                }
                consecutive_false += 64;
            } else {
                let mut w = word;
                let mut bits_left = 64usize;
                while bits_left > 0 {
                    if w & 1 == 1 {
                        let ones = (w.trailing_ones() as usize).min(bits_left);
                        if consecutive_false > 0 {
                            pushable.push_nulls(sink, consecutive_false)?;
                            consecutive_false = 0;
                        }
                        consecutive_true += ones;
                        w >>= ones;
                        bits_left -= ones;
                    } else {
                        let zeros = if w == 0 {
                            bits_left
                        } else {
                            (w.trailing_zeros() as usize).min(bits_left)
                        };
                        if consecutive_true > 0 {
                            pushable.push_slice(sink, consecutive_true)?;
                            consecutive_true = 0;
                        }
                        consecutive_false += zeros;
                        w >>= zeros;
                        bits_left -= zeros;
                    }
                }
            }
            pos += 64;
        }

        // Process remaining full bytes.
        while pos + 8 <= end {
            let byte = values[pos >> 3];
            if byte == 0xFF {
                if consecutive_false > 0 {
                    pushable.push_nulls(sink, consecutive_false)?;
                    consecutive_false = 0;
                }
                consecutive_true += 8;
            } else if byte == 0 {
                if consecutive_true > 0 {
                    pushable.push_slice(sink, consecutive_true)?;
                    consecutive_true = 0;
                }
                consecutive_false += 8;
            } else {
                for i in 0..8 {
                    if (byte >> i) & 1 == 1 {
                        if consecutive_false > 0 {
                            pushable.push_nulls(sink, consecutive_false)?;
                            consecutive_false = 0;
                        }
                        consecutive_true += 1;
                    } else {
                        if consecutive_true > 0 {
                            pushable.push_slice(sink, consecutive_true)?;
                            consecutive_true = 0;
                        }
                        consecutive_false += 1;
                    }
                }
            }
            pos += 8;
        }

        // Handle remaining bits.
        while pos < end {
            let byte = values[pos >> 3];
            let bit = (pos & 7) as u32;
            if (byte >> bit) & 1 == 1 {
                if consecutive_false > 0 {
                    pushable.push_nulls(sink, consecutive_false)?;
                    consecutive_false = 0;
                }
                consecutive_true += 1;
            } else {
                if consecutive_true > 0 {
                    pushable.push_slice(sink, consecutive_true)?;
                    consecutive_true = 0;
                }
                consecutive_false += 1;
            }
            pos += 1;
        }

        // Flush.
        if consecutive_true > 0 {
            pushable.push_slice(sink, consecutive_true)?;
        }
        if consecutive_false > 0 {
            pushable.push_nulls(sink, consecutive_false)?;
        }

        *bit_offset = pos;
        *remaining -= n;
        Ok(n)
    }
}

/// Convenience: decode all remaining rows in one shot.
pub fn decode_all<Sink, P: Pushable<Sink>>(
    iter: &mut DefLevelBatchIter<'_>,
    pushable: &mut P,
    sink: &mut Sink,
) -> ParquetResult<()> {
    pushable.reserve(sink, iter.output_count())?;
    while !iter.is_exhausted() {
        iter.decode_batch(pushable, sink, usize::MAX)?;
    }
    Ok(())
}
