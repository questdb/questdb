use crate::allocator::AcVec;
use crate::parquet::error::{fmt_err, ParquetResult};
use crate::parquet::util::{align8b, ARRAY_NDIMS_LIMIT};
use crate::parquet_read::column_sink::var::ARRAY_AUX_SIZE;
use crate::parquet_read::page::{split_buffer, DataPage};
use crate::parquet_read::slicer::DataPageSlicer;
use crate::parquet_read::ColumnChunkBuffers;
use crate::parquet_write::array::{
    append_array_null, append_array_nulls, calculate_array_shape, LevelsIterator,
};

#[allow(clippy::while_let_on_iterator)]
#[allow(clippy::too_many_arguments)]
pub(super) fn decode_array_page_filtered<T: DataPageSlicer, const FILL_NULLS: bool>(
    page: &DataPage<'_>,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if FILL_NULLS {
        let output_count = row_hi - row_lo;
        buffers.aux_vec.reserve(output_count * ARRAY_AUX_SIZE)?;
        if rows_filter.is_empty() {
            append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, output_count)?;
            return Ok(());
        }
    } else {
        if rows_filter.is_empty() {
            return Ok(());
        }
        buffers
            .aux_vec
            .reserve(rows_filter.len() * ARRAY_AUX_SIZE)?;
    }

    let (rep_levels, def_levels, _) = split_buffer(page)?;

    let max_rep_level = page.descriptor.max_rep_level;
    let max_def_level = page.descriptor.max_def_level;

    if max_rep_level > ARRAY_NDIMS_LIMIT as i16 {
        return Err(fmt_err!(
            Unsupported,
            "too large number of array dimensions {max_rep_level}"
        ));
    }

    let mut levels_iter = LevelsIterator::try_new(
        page.num_values(),
        max_rep_level,
        max_def_level,
        rep_levels,
        def_levels,
    )?;

    match max_rep_level {
        1 => decode_array_filtered_loop::<T, FILL_NULLS, 1>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
        2 => decode_array_filtered_loop::<T, FILL_NULLS, 2>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
        _ => decode_array_filtered_loop::<T, FILL_NULLS, 0>(
            &mut levels_iter,
            max_rep_level,
            max_def_level,
            page_row_start,
            page_row_count,
            row_group_lo,
            row_lo,
            row_hi,
            rows_filter,
            slicer,
            buffers,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_array_filtered_loop<T: DataPageSlicer, const FILL_NULLS: bool, const REP_LEVEL: i16>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: i16,
    max_def_level: i16,
    page_row_start: usize,
    page_row_count: usize,
    row_group_lo: usize,
    row_lo: usize,
    row_hi: usize,
    rows_filter: &[i64],
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let mut current_row = 0usize;
    let mut filter_idx = 0usize;
    let filter_len = rows_filter.len();
    let mut def_scratch: AcVec<u32> = AcVec::new_in(buffers.data_vec.allocator().clone());

    if FILL_NULLS && row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level as u32)?;
        slicer.skip(non_null_skipped)?;
        current_row = row_lo;
    }

    while current_row < page_row_count {
        if FILL_NULLS && current_row >= row_hi {
            break;
        }
        if !FILL_NULLS && filter_idx >= filter_len {
            break;
        }

        let row_pos = page_row_start + current_row;
        let in_filter =
            filter_idx < filter_len && (rows_filter[filter_idx] as usize + row_group_lo) == row_pos;

        if in_filter {
            let result = if REP_LEVEL == 1 {
                read_and_append_one_row_1d(
                    levels_iter,
                    max_def_level as u32,
                    slicer,
                    buffers,
                    &mut def_scratch,
                )?
            } else if REP_LEVEL == 2 {
                read_and_append_one_row_2d(
                    levels_iter,
                    max_def_level as u32,
                    slicer,
                    buffers,
                    &mut def_scratch,
                )?
            } else {
                read_and_append_one_row_generic(
                    levels_iter,
                    max_rep_level as u32,
                    max_def_level as u32,
                    slicer,
                    buffers,
                )?
            };
            let Some(first_vs) = result else {
                break;
            };

            filter_idx += 1;
            current_row += 1;
            if filter_idx == 1 && first_vs > 0 && filter_len > 1 {
                // estimate total size
                buffers.data_vec.reserve(first_vs * (filter_len - 1))?;
            }
        } else {
            let next_match_row = if filter_idx < filter_len {
                let abs_row = rows_filter[filter_idx] as usize + row_group_lo;
                abs_row.saturating_sub(page_row_start)
            } else {
                debug_assert!(FILL_NULLS);
                row_hi
            };
            let skip_count = next_match_row.saturating_sub(current_row);

            if skip_count > 0 {
                let non_null_skipped = levels_iter.skip_rows(skip_count, max_def_level as u32)?;
                slicer.skip(non_null_skipped)?;
                if FILL_NULLS {
                    append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, skip_count)?;
                }
                current_row += skip_count;
            }
        }
    }

    if FILL_NULLS && current_row < row_hi {
        let remaining = row_hi - current_row;
        append_array_nulls(&mut buffers.aux_vec, &buffers.data_vec, remaining)?;
    }

    Ok(())
}

pub(super) fn decode_array_page<T: DataPageSlicer>(
    page: &DataPage,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    let (rep_levels, def_levels, _) = split_buffer(page)?;

    let max_rep_level = page.descriptor.max_rep_level;
    let max_def_level = page.descriptor.max_def_level;

    if max_rep_level > ARRAY_NDIMS_LIMIT as i16 {
        return Err(fmt_err!(
            Unsupported,
            "too large number of array dimensions {max_rep_level}"
        ));
    }

    buffers
        .aux_vec
        .reserve((row_hi - row_lo) * ARRAY_AUX_SIZE)?;

    let mut levels_iter = LevelsIterator::try_new(
        page.num_values(),
        max_rep_level,
        max_def_level,
        rep_levels,
        def_levels,
    )?;

    match max_rep_level {
        1 => decode_array_rows_1d(
            &mut levels_iter,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
        2 => decode_array_rows_2d(
            &mut levels_iter,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
        _ => decode_array_rows_generic(
            &mut levels_iter,
            max_rep_level as u32,
            max_def_level as u32,
            row_lo,
            row_hi,
            slicer,
            buffers,
        ),
    }
}

fn decode_array_rows_1d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped)?;
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let mut def_scratch: AcVec<u32> = AcVec::new_in(buffers.data_vec.allocator().clone());

    let Some(first_vs) = read_and_append_one_row_1d(
        levels_iter,
        max_def_level,
        slicer,
        buffers,
        &mut def_scratch,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        // estimate total size
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_1d(
            levels_iter,
            max_def_level,
            slicer,
            buffers,
            &mut def_scratch,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

fn decode_array_rows_2d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped)?;
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let mut def_scratch: AcVec<u32> = AcVec::new_in(buffers.data_vec.allocator().clone());

    let Some(first_vs) = read_and_append_one_row_2d(
        levels_iter,
        max_def_level,
        slicer,
        buffers,
        &mut def_scratch,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        // estimate total size
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_2d(
            levels_iter,
            max_def_level,
            slicer,
            buffers,
            &mut def_scratch,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

#[inline]
fn read_and_append_one_row_1d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
    def_scratch: &mut AcVec<u32>,
) -> ParquetResult<Option<usize>> {
    let first_def = if levels_iter.has_lookahead() {
        let (_, def) = levels_iter.take_lookahead();
        Some(def)
    } else {
        match levels_iter.next_rep_def() {
            Some(Ok((_, def))) => Some(def),
            Some(Err(e)) => return Err(e),
            None => None,
        }
    };

    let Some(first_def) = first_def else {
        return Ok(None);
    };

    if first_def == 0 {
        append_array_null(&mut buffers.aux_vec, &buffers.data_vec)?;
        return Ok(Some(0));
    }

    let mut element_count = 1usize;
    let mut non_null_count: usize = if first_def == max_def_level { 1 } else { 0 };
    let mut has_nulls = first_def != max_def_level;

    def_scratch.clear();
    if has_nulls {
        def_scratch.push(first_def)?;
    }

    loop {
        match levels_iter.next_rep_def() {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok((rep, def))) => {
                if rep == 0 {
                    levels_iter.set_lookahead(rep, def);
                    break;
                }
                if def == max_def_level {
                    non_null_count += 1;
                } else if !has_nulls {
                    has_nulls = true;
                    def_scratch.reserve(element_count + 1)?;
                    def_scratch.resize(element_count, max_def_level)?;
                }
                if has_nulls {
                    def_scratch.push(def)?;
                }
                element_count += 1;
            }
        }
    }

    // 8 bytes shape ([element_count: u32, pad: u32]) + 8 bytes per f64 element.
    // Currently arrays only support f64 elements.
    let value_size = 8 + 8 * element_count;
    let data_start = buffers.data_vec.len();
    buffers.data_vec.reserve(value_size)?;

    buffers
        .aux_vec
        .extend_from_slice(&data_start.to_le_bytes())?;
    buffers
        .aux_vec
        .extend_from_slice(&value_size.to_le_bytes())?;

    buffers
        .data_vec
        .extend_from_slice(&(element_count as u32).to_le_bytes())?;
    buffers.data_vec.extend_from_slice(&[0u8; 4])?;

    if non_null_count == element_count {
        slicer.next_slice_into(element_count, &mut buffers.data_vec)?;
    } else {
        for &def in def_scratch.iter() {
            if def == max_def_level {
                slicer.next_into(&mut buffers.data_vec)?;
            } else {
                buffers
                    .data_vec
                    .extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }

    Ok(Some(value_size))
}

#[inline]
fn read_and_append_one_row_2d<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
    def_scratch: &mut AcVec<u32>,
) -> ParquetResult<Option<usize>> {
    let first_def = if levels_iter.has_lookahead() {
        let (_, def) = levels_iter.take_lookahead();
        Some(def)
    } else {
        match levels_iter.next_rep_def() {
            Some(Ok((_, def))) => Some(def),
            Some(Err(e)) => return Err(e),
            None => None,
        }
    };

    let Some(first_def) = first_def else {
        return Ok(None);
    };

    if first_def == 0 {
        append_array_null(&mut buffers.aux_vec, &buffers.data_vec)?;
        return Ok(Some(0));
    }

    let mut dim0 = 1u32;
    let mut cur_dim1 = 1u32;
    let mut max_dim1 = 0u32;
    let mut total_elements = 1usize;
    let mut non_null_count: usize = if first_def == max_def_level { 1 } else { 0 };
    let mut has_nulls = first_def != max_def_level;

    def_scratch.clear();
    if has_nulls {
        def_scratch.push(first_def)?;
    }

    loop {
        match levels_iter.next_rep_def() {
            None => break,
            Some(Err(e)) => return Err(e),
            Some(Ok((rep, def))) => {
                if rep == 0 {
                    levels_iter.set_lookahead(rep, def);
                    break;
                }
                if rep == 1 {
                    max_dim1 = max_dim1.max(cur_dim1);
                    cur_dim1 = 1;
                    dim0 += 1;
                } else {
                    cur_dim1 += 1;
                }
                if def == max_def_level {
                    non_null_count += 1;
                } else if !has_nulls {
                    has_nulls = true;
                    def_scratch.reserve(total_elements + 1)?;
                    def_scratch.resize(total_elements, max_def_level)?;
                }
                if has_nulls {
                    def_scratch.push(def)?;
                }
                total_elements += 1;
            }
        }
    }
    max_dim1 = max_dim1.max(cur_dim1);

    // 8 bytes shape ([dim0: u32, max_dim1: u32]) + 8 bytes per f64 element.
    // Currently arrays only support f64 elements.
    let value_size = 8 + 8 * total_elements;
    let data_start = buffers.data_vec.len();
    buffers.data_vec.reserve(value_size)?;

    buffers
        .aux_vec
        .extend_from_slice(&data_start.to_le_bytes())?;
    buffers
        .aux_vec
        .extend_from_slice(&value_size.to_le_bytes())?;

    buffers.data_vec.extend_from_slice(&dim0.to_le_bytes())?;
    buffers
        .data_vec
        .extend_from_slice(&max_dim1.to_le_bytes())?;

    if non_null_count == total_elements {
        slicer.next_slice_into(total_elements, &mut buffers.data_vec)?;
    } else {
        for &def in def_scratch.iter() {
            if def == max_def_level {
                slicer.next_into(&mut buffers.data_vec)?;
            } else {
                buffers
                    .data_vec
                    .extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }

    Ok(Some(value_size))
}

#[inline]
fn read_and_append_one_row_generic<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: u32,
    max_def_level: u32,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<Option<usize>> {
    match levels_iter.next_levels() {
        Some(Ok(levels)) => {
            let vs = append_array(
                &mut buffers.aux_vec,
                &mut buffers.data_vec,
                max_rep_level,
                max_def_level,
                &levels.rep_levels,
                &levels.def_levels,
                slicer,
            )?;
            Ok(Some(vs))
        }
        Some(Err(e)) => Err(e),
        None => Ok(None),
    }
}

fn decode_array_rows_generic<T: DataPageSlicer>(
    levels_iter: &mut LevelsIterator,
    max_rep_level: u32,
    max_def_level: u32,
    row_lo: usize,
    row_hi: usize,
    slicer: &mut T,
    buffers: &mut ColumnChunkBuffers,
) -> ParquetResult<()> {
    if row_lo > 0 {
        let non_null_skipped = levels_iter.skip_rows(row_lo, max_def_level)?;
        slicer.skip(non_null_skipped)?;
    }

    let target_rows = row_hi - row_lo;
    if target_rows == 0 {
        return Ok(());
    }

    let Some(first_vs) = read_and_append_one_row_generic(
        levels_iter,
        max_rep_level,
        max_def_level,
        slicer,
        buffers,
    )?
    else {
        return Ok(());
    };
    if target_rows > 1 && first_vs > 0 {
        buffers.data_vec.reserve(first_vs * (target_rows - 1))?;
    }
    for _ in 1..target_rows {
        if read_and_append_one_row_generic(
            levels_iter,
            max_rep_level,
            max_def_level,
            slicer,
            buffers,
        )?
        .is_none()
        {
            break;
        }
    }
    Ok(())
}

fn append_array<T: DataPageSlicer>(
    aux_mem: &mut AcVec<u8>,
    data_mem: &mut AcVec<u8>,
    max_rep_level: u32,
    max_def_level: u32,
    rep_levels: &[u32],
    def_levels: &[u32],
    slicer: &mut T,
) -> ParquetResult<usize> {
    if def_levels.len() == 1 && def_levels[0] == 0 {
        append_array_null(aux_mem, data_mem)?;
        return Ok(0);
    }

    let shape_size = align8b(4 * max_rep_level as usize);
    let value_size = shape_size + 8 * rep_levels.len();

    aux_mem.extend_from_slice(&data_mem.len().to_le_bytes())?;
    aux_mem.extend_from_slice(&value_size.to_le_bytes())?;

    // first, calculate and write shape
    let mut shape = [0_u32; ARRAY_NDIMS_LIMIT];
    calculate_array_shape(&mut shape, max_rep_level, rep_levels);
    data_mem.reserve(value_size)?;
    let mut num_elements: usize = 1;
    for &dim in shape.iter().take(max_rep_level as usize) {
        num_elements *= dim as usize;
        data_mem.extend_from_slice(&dim.to_le_bytes())?;
    }
    if num_elements != def_levels.len() {
        return Err(fmt_err!(
            InvalidLayout,
            "incomplete array structure: expected {} elements, present {}",
            num_elements,
            def_levels.len(),
        ));
    }
    // add an optional padding
    if !max_rep_level.is_multiple_of(2) {
        data_mem.extend_from_slice(&0_u32.to_le_bytes())?;
    }

    // next, copy elements
    let non_null_count = def_levels.iter().filter(|&&d| d == max_def_level).count();
    if non_null_count == def_levels.len() {
        // All non-null: batch copy.
        slicer.next_slice_into(def_levels.len(), data_mem)?;
    } else {
        for &def_level in def_levels {
            if def_level == max_def_level {
                slicer.next_into(data_mem)?;
            } else {
                data_mem.extend_from_slice(&f64::NAN.to_le_bytes())?;
            }
        }
    }
    Ok(value_size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::TestAllocatorState;
    use crate::parquet::error::ParquetErrorReason;
    use crate::parquet_read::slicer::DataPageFixedSlicer;
    use parquet2::encoding::hybrid_rle::encode_u32;
    use parquet2::read::levels::get_bit_width;

    // Encodes the rep/def level streams for a single 1D-array row of `n` elements
    // that are all present-but-null: def == 1 (one below max_def_level == 2), so
    // read_and_append_one_row_1d takes its has_nulls branch and grows def_scratch
    // to `n` entries. Per-element nulls only reach this path from a foreign
    // LIST<double> file -- QuestDB's own writer encodes a missing double as NaN,
    // never as a def-level null -- which is exactly the foreign-input case the
    // AcVec change hardens. All elements being null also means the values slicer
    // is never advanced, so the row decodes without any backing values buffer.
    fn encode_null_element_row_levels(n: usize) -> (Vec<u8>, Vec<u8>) {
        const MAX_REP: i16 = 1;
        const MAX_DEF: i16 = 2;
        // The first element opens the row (rep 0); the rest continue it (rep 1).
        let rep = std::iter::once(0u32).chain(std::iter::repeat_n(1u32, n - 1));
        // Every element is present-but-null (def 1, never max_def_level 2).
        let def = std::iter::repeat_n(1u32, n);
        let mut rep_buf = Vec::new();
        let mut def_buf = Vec::new();
        encode_u32(&mut rep_buf, rep, n, get_bit_width(MAX_REP)).unwrap();
        encode_u32(&mut def_buf, def, n, get_bit_width(MAX_DEF)).unwrap();
        (rep_buf, def_buf)
    }

    #[test]
    fn decode_array_rows_1d_null_elements_decodes() {
        // Coverage/sanity: with no memory limit a foreign 1D array row of all
        // present-but-null elements decodes cleanly, exercising def_scratch's
        // has_nulls growth path. Pins the level scaffolding so the allocation
        // failure test below cannot pass for the wrong reason.
        const N: usize = 256;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels(N);
        let mut levels_iter = LevelsIterator::try_new(N, 1, 2, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);

        decode_array_rows_1d(&mut levels_iter, 2, 0, 1, &mut slicer, &mut buffers).unwrap();

        // One aux entry for the row; data holds the shape header + N f64 NaNs.
        assert_eq!(buffers.aux_vec.len(), ARRAY_AUX_SIZE);
        assert_eq!(buffers.data_vec.len(), 8 + 8 * N);
    }

    #[test]
    fn decode_array_rows_1d_def_scratch_alloc_failure_errors_not_aborts() {
        // def_scratch is now an AcVec: a foreign array whose level stream drives
        // it large must surface a recoverable error, not abort the JVM through an
        // infallible Vec::push. Decode a row of N present-but-null elements
        // (forcing def_scratch to grow to N) under a memory limit set just above
        // the setup, so the growth fails. The decode must return a clean
        // OutOfMemory error rather than panic/abort.
        //
        // Scope: a memory limit proves abort-freedom of this path, not
        // fail-on-revert -- reverting def_scratch to an infallible std Vec would
        // merely shift the failure to the sibling data_vec reservation (also
        // fallible, and strictly larger per element). Abort-freedom is the
        // contract the AcVec change establishes and the prior infallible Vec
        // violated.
        const N: usize = 4096;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels(N);
        let mut levels_iter = LevelsIterator::try_new(N, 1, 2, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);

        // Cap memory just above the setup; def_scratch needs N * 4 bytes, far
        // beyond the headroom, so its growth fails deterministically.
        tas.set_mem_rss_limit(tas.rss_mem_used() + 256);
        let err = decode_array_rows_1d(&mut levels_iter, 2, 0, 1, &mut slicer, &mut buffers)
            .expect_err("def_scratch growth past the memory limit must error, not abort");
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory: {err:?}"
        );
        // The aborted decode must free everything it allocated before the limit
        // was hit -- the error path must not leak across JNI.
        drop(buffers);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }

    // Encodes the rep/def level streams for a single 2D-array row of `n` leaf
    // elements that are all present-but-null: def == 2 (one below max_def_level
    // == 3), so read_and_append_one_row_2d takes its has_nulls branch and grows
    // def_scratch to `n` entries. The first leaf opens the row (rep 0); the rest
    // continue a single inner list (rep 2). Mirrors encode_null_element_row_levels
    // for the 2D decode path, whose def_scratch callsite is otherwise untested.
    fn encode_null_element_row_levels_2d(n: usize) -> (Vec<u8>, Vec<u8>) {
        const MAX_REP: i16 = 2;
        const MAX_DEF: i16 = 3;
        let rep = std::iter::once(0u32).chain(std::iter::repeat_n(2u32, n - 1));
        let def = std::iter::repeat_n(2u32, n);
        let mut rep_buf = Vec::new();
        let mut def_buf = Vec::new();
        encode_u32(&mut rep_buf, rep, n, get_bit_width(MAX_REP)).unwrap();
        encode_u32(&mut def_buf, def, n, get_bit_width(MAX_DEF)).unwrap();
        (rep_buf, def_buf)
    }

    #[test]
    fn decode_array_rows_2d_null_elements_decodes() {
        // Coverage/sanity: with no memory limit a foreign 2D array row of all
        // present-but-null elements decodes cleanly, exercising the 2D
        // def_scratch has_nulls growth path. Pins the level scaffolding so the
        // allocation failure test below cannot pass for the wrong reason.
        const N: usize = 256;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels_2d(N);
        let mut levels_iter = LevelsIterator::try_new(N, 2, 3, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);

        decode_array_rows_2d(&mut levels_iter, 3, 0, 1, &mut slicer, &mut buffers).unwrap();

        // One aux entry for the row; data holds the shape header + N f64 NaNs.
        assert_eq!(buffers.aux_vec.len(), ARRAY_AUX_SIZE);
        assert_eq!(buffers.data_vec.len(), 8 + 8 * N);
    }

    #[test]
    fn decode_array_rows_2d_def_scratch_alloc_failure_errors_not_aborts() {
        // The 2D counterpart of the 1D def_scratch alloc-failure test: the 2D row
        // path uses its own AcVec<u32> def_scratch (read_and_append_one_row_2d),
        // whose fallible push/reserve/resize must surface a recoverable error
        // rather than abort the JVM. Same scope note applies -- a memory limit
        // proves abort-freedom, not fail-on-revert, because the sibling data_vec
        // reservation (also fallible, strictly larger per element) masks
        // def_scratch under any limit that trips it.
        const N: usize = 4096;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels_2d(N);
        let mut levels_iter = LevelsIterator::try_new(N, 2, 3, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);

        tas.set_mem_rss_limit(tas.rss_mem_used() + 256);
        let err = decode_array_rows_2d(&mut levels_iter, 3, 0, 1, &mut slicer, &mut buffers)
            .expect_err("def_scratch growth past the memory limit must error, not abort");
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory: {err:?}"
        );
        drop(buffers);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }

    #[test]
    fn decode_array_filtered_loop_1d_null_elements_decodes() {
        // Sanity for the FILTERED loop's def_scratch construction + iteration: a
        // row-filter that selects a foreign 1D array row of all present-but-null
        // elements decodes cleanly through decode_array_filtered_loop. Pins the
        // filter scaffolding so the alloc-failure test below cannot pass for the
        // wrong reason, and exercises the filtered AcVec callsite directly.
        const N: usize = 256;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels(N);
        let mut levels_iter = LevelsIterator::try_new(N, 1, 2, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);
        let rows_filter: [i64; 1] = [0];

        decode_array_filtered_loop::<_, false, 1>(
            &mut levels_iter,
            1,
            2,
            0,
            1,
            0,
            0,
            1,
            &rows_filter,
            &mut slicer,
            &mut buffers,
        )
        .unwrap();

        // One aux entry for the selected row; data holds the shape header + N NaNs.
        assert_eq!(buffers.aux_vec.len(), ARRAY_AUX_SIZE);
        assert_eq!(buffers.data_vec.len(), 8 + 8 * N);
    }

    #[test]
    fn decode_array_filtered_loop_1d_def_scratch_alloc_failure_errors_not_aborts() {
        // The FILTERED array decode loop builds and grows its OWN def_scratch
        // AcVec (decode_array_filtered_loop, REP_LEVEL == 1) through the same
        // read_and_append_one_row_1d helper as the unfiltered path. A foreign 1D
        // array row selected by a row filter must surface a recoverable
        // OutOfMemory error from that growth, not abort the JVM. This is the
        // filtered callsite of the def_scratch AcVec change, otherwise untested --
        // the row-filter counterpart of
        // decode_array_rows_1d_def_scratch_alloc_failure_errors_not_aborts. Same
        // scope note: a memory limit proves abort-freedom, not fail-on-revert.
        const N: usize = 4096;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels(N);
        let mut levels_iter = LevelsIterator::try_new(N, 1, 2, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);
        // Select the single array row (absolute row 0).
        let rows_filter: [i64; 1] = [0];

        tas.set_mem_rss_limit(tas.rss_mem_used() + 256);
        let err = decode_array_filtered_loop::<_, false, 1>(
            &mut levels_iter,
            1, // max_rep_level
            2, // max_def_level
            0, // page_row_start
            1, // page_row_count
            0, // row_group_lo
            0, // row_lo
            1, // row_hi
            &rows_filter,
            &mut slicer,
            &mut buffers,
        )
        .expect_err("filtered def_scratch growth past the memory limit must error, not abort");
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory: {err:?}"
        );
        drop(buffers);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }

    #[test]
    fn decode_array_filtered_loop_2d_null_elements_decodes() {
        // 2D sanity counterpart: decode_array_filtered_loop's REP_LEVEL == 2 branch
        // decodes a row-filter-selected foreign 2D array row of all present-but-null
        // leaves cleanly, pinning the filter scaffolding for the 2D alloc-failure
        // test below.
        const N: usize = 256;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels_2d(N);
        let mut levels_iter = LevelsIterator::try_new(N, 2, 3, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);
        let rows_filter: [i64; 1] = [0];

        decode_array_filtered_loop::<_, false, 2>(
            &mut levels_iter,
            2,
            3,
            0,
            1,
            0,
            0,
            1,
            &rows_filter,
            &mut slicer,
            &mut buffers,
        )
        .unwrap();

        assert_eq!(buffers.aux_vec.len(), ARRAY_AUX_SIZE);
        assert_eq!(buffers.data_vec.len(), 8 + 8 * N);
    }

    #[test]
    fn decode_array_filtered_loop_2d_def_scratch_alloc_failure_errors_not_aborts() {
        // The 2D counterpart: decode_array_filtered_loop's REP_LEVEL == 2 branch
        // grows its def_scratch through read_and_append_one_row_2d. Same contract --
        // a foreign 2D array row pulled in by a row filter must error cleanly, not
        // abort.
        const N: usize = 4096;
        let tas = TestAllocatorState::new();
        let allocator = tas.allocator();
        let (rep_buf, def_buf) = encode_null_element_row_levels_2d(N);
        let mut levels_iter = LevelsIterator::try_new(N, 2, 3, &rep_buf, &def_buf).unwrap();
        let mut buffers = ColumnChunkBuffers::new(allocator);
        let empty: &[u8] = &[];
        let mut slicer = DataPageFixedSlicer::<8>::new(empty, 0);
        let rows_filter: [i64; 1] = [0];

        tas.set_mem_rss_limit(tas.rss_mem_used() + 256);
        let err = decode_array_filtered_loop::<_, false, 2>(
            &mut levels_iter,
            2, // max_rep_level
            3, // max_def_level
            0, // page_row_start
            1, // page_row_count
            0, // row_group_lo
            0, // row_lo
            1, // row_hi
            &rows_filter,
            &mut slicer,
            &mut buffers,
        )
        .expect_err("filtered def_scratch growth past the memory limit must error, not abort");
        assert!(
            matches!(err.reason(), ParquetErrorReason::OutOfMemory(_)),
            "allocation failure must be classified OutOfMemory: {err:?}"
        );
        drop(buffers);
        assert_eq!(tas.rss_mem_used(), 0, "decode error path leaked memory");
    }
}
