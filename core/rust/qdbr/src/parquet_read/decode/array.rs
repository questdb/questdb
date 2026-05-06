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
    let mut def_scratch = Vec::new();

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

    let mut def_scratch = Vec::new();

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

    let mut def_scratch = Vec::new();

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
    def_scratch: &mut Vec<u32>,
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
        def_scratch.push(first_def);
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
                    def_scratch.reserve(element_count + 1);
                    def_scratch.resize(element_count, max_def_level);
                }
                if has_nulls {
                    def_scratch.push(def);
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
    def_scratch: &mut Vec<u32>,
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
        def_scratch.push(first_def);
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
                    def_scratch.reserve(total_elements + 1);
                    def_scratch.resize(total_elements, max_def_level);
                }
                if has_nulls {
                    def_scratch.push(def);
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
