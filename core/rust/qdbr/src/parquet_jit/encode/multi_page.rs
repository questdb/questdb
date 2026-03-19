//! Page-level multi-column IR emitter.
//!
//! Operates on **packed** parquet page values (only non-null values stored)
//! with an optional def_levels bitmap decoded to 1-byte-per-row format.
//!
//! The generated kernel uses **two counters**:
//! - `row_ctr`: iterates 0..row_count (indexes bitmap and output)
//! - `val_ctr`: advances only for non-null rows (indexes packed values)
//!
//! When `def_bitmap` is null, all rows are non-null and the kernel uses
//! a single counter (val_ctr == row_ctr).
//!
//! The with-nulls materialize path is **branchless**: it unconditionally
//! loads from `values[val_ctr]`, selects between value and sentinel based
//! on the bitmap byte, stores unconditionally, and advances `val_ctr` by
//! the bitmap byte (0 or 1). No branch mispredictions.

use super::super::ir::{CmpOp, IrType, ValRef};
use super::super::ir_builder::IrBuilder;
use super::super::multi::{
    column_to_pipeline_spec, ColumnRole, FilterCombine, MultiPipelineSpec,
    PAGE_CTX_DEF_BITMAP, PAGE_CTX_RESULT_PTR, PAGE_CTX_ROW_COUNT,
    PAGE_SLOT_FILTER_HI, PAGE_SLOT_FILTER_LO, PAGE_SLOT_VALUES_DATA,
    page_col_offset,
};
use super::super::values as ir_values;
use super::super::{JitError, JitFilterOp, JitOutputMode};

/// Emit IR for a page-level multi-column kernel.
pub fn emit_ir(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    match spec.output_mode {
        JitOutputMode::Aggregate => emit_ir_aggregate(b, spec),
        JitOutputMode::Materialize => emit_ir_materialize(b, spec),
    }
}

// =========================================================================
// Aggregate
// =========================================================================

fn emit_ir_aggregate(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    let cols = &spec.columns;

    let agg_idx = cols
        .iter()
        .position(|c| matches!(c.role, ColumnRole::Aggregate | ColumnRole::FilterAggregate))
        .ok_or_else(|| JitError::UnsupportedCombination("no aggregate column".into()))?;
    let agg_col = &cols[agg_idx];
    let agg_pt = agg_col.physical_type;
    let acc_ty = agg_pt.ir_acc_type();
    let agg_spec = column_to_pipeline_spec(agg_col, spec.output_mode);

    let filter_indices: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.role, ColumnRole::Filter | ColumnRole::FilterAggregate))
        .map(|(i, _)| i)
        .collect();

    // ---- Entry block ----
    let entry = b.create_block(&[]);
    b.switch_to(entry);

    let ctx = b.arg(0, IrType::Ptr);
    let row_count = b.load_field(IrType::I64, ctx, PAGE_CTX_ROW_COUNT);
    let result_ptr = b.load_field(IrType::Ptr, ctx, PAGE_CTX_RESULT_PTR);
    let def_bitmap = b.load_field(IrType::Ptr, ctx, PAGE_CTX_DEF_BITMAP);

    let init_count = b.load_field(IrType::I64, result_ptr, 0);
    let init_has = b.load_field(IrType::I64, result_ptr, 24);
    let init_acc = if agg_pt.is_float() {
        b.load_field(IrType::F64, result_ptr, 16)
    } else {
        b.load_field(acc_ty, result_ptr, 8)
    };

    let zero = b.iconst(0);

    // Block params: (row_ctr, val_ctr, acc, count, has)
    let loop_params = &[IrType::I64, IrType::I64, acc_ty, IrType::I64, IrType::I64];
    let init_args = &[zero, zero, init_acc, init_count, init_has];

    let nn_hdr = b.create_block(loop_params);
    let nn_body = b.create_block(loop_params);
    let wn_hdr = b.create_block(loop_params);
    let wn_check = b.create_block(loop_params);
    let wn_body = b.create_block(loop_params);
    let wn_skip = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    let bm_is_null = b.is_zero(def_bitmap);
    b.cond_br(bm_is_null, nn_hdr, init_args, wn_hdr, init_args);

    // ---- No-nulls path (val_ctr == row_ctr) ----
    b.switch_to(nn_hdr);
    let (row_ctr, _val_ctr, acc, count, has) = b.params_5();
    let done = b.icmp(CmpOp::Ge, row_ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[row_ctr, row_ctr, acc, count, has],
        nn_body, &[row_ctr, row_ctr, acc, count, has],
    );

    b.switch_to(nn_body);
    let (row_ctr, val_ctr, acc, count, has) = b.params_5();
    let combined_pass = emit_page_filter_chain(b, ctx, spec, &filter_indices, val_ctr);
    let agg_values = b.load_field(
        IrType::Ptr, ctx, page_col_offset(agg_idx, PAGE_SLOT_VALUES_DATA),
    );
    let agg_val = ir_values::emit_load_ir(b, agg_pt, agg_values, val_ctr);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, &agg_spec, agg_val, acc, count, has, combined_pass, None);
    let next = b.inc(row_ctr);
    b.br(nn_hdr, &[next, next, new_acc, new_count, new_has]);

    // ---- With-nulls path (branching, aggregate can skip nulls) ----
    b.switch_to(wn_hdr);
    let (row_ctr, val_ctr, acc, count, has) = b.params_5();
    let done = b.icmp(CmpOp::Ge, row_ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[row_ctr, val_ctr, acc, count, has],
        wn_check, &[row_ctr, val_ctr, acc, count, has],
    );

    b.switch_to(wn_check);
    let (row_ctr, val_ctr, acc, count, has) = b.params_5();
    let def_bm = b.load_field(IrType::Ptr, ctx, PAGE_CTX_DEF_BITMAP);
    let bm_byte = b.load_byte(def_bm, row_ctr);
    let is_nn = b.is_nonzero(bm_byte);
    b.cond_br(
        is_nn,
        wn_body, &[row_ctr, val_ctr, acc, count, has],
        wn_skip, &[row_ctr, val_ctr, acc, count, has],
    );

    b.switch_to(wn_body);
    let (row_ctr, val_ctr, acc, count, has) = b.params_5();
    let combined_pass = emit_page_filter_chain(b, ctx, spec, &filter_indices, val_ctr);
    let agg_values = b.load_field(
        IrType::Ptr, ctx, page_col_offset(agg_idx, PAGE_SLOT_VALUES_DATA),
    );
    let agg_val = ir_values::emit_load_ir(b, agg_pt, agg_values, val_ctr);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, &agg_spec, agg_val, acc, count, has, combined_pass, None);
    let next_row = b.inc(row_ctr);
    let next_val = b.inc(val_ctr);
    b.br(wn_hdr, &[next_row, next_val, new_acc, new_count, new_has]);

    b.switch_to(wn_skip);
    let (row_ctr, val_ctr, acc, count, has) = b.params_5();
    let next_row = b.inc(row_ctr);
    b.br(wn_hdr, &[next_row, val_ctr, acc, count, has]);

    // ---- Epilogue ----
    b.switch_to(epilogue);
    let (_row_ctr, _val_ctr, acc, count, has) = b.params_5();
    super::plain::emit_epilogue_stores(b, &agg_spec, result_ptr, acc, count, has);

    Ok(())
}

// =========================================================================
// Materialize
// =========================================================================

fn emit_ir_materialize(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    use super::super::JitPhysicalType;

    let cols = &spec.columns;

    let filter_indices: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, c)| matches!(c.role, ColumnRole::Filter | ColumnRole::FilterAggregate))
        .map(|(i, _)| i)
        .collect();

    let mat_indices: Vec<usize> = cols
        .iter()
        .enumerate()
        .filter(|(_, c)| c.role == ColumnRole::Materialize)
        .map(|(i, _)| i)
        .collect();

    if mat_indices.is_empty() {
        return Err(JitError::UnsupportedCombination(
            "Materialize mode requires at least one Materialize column".into(),
        ));
    }

    let n_cursors = mat_indices.len();

    // Block params: (row_ctr, val_ctr, cursor_0, ..., cursor_N-1, count)
    let mut loop_param_types = Vec::with_capacity(n_cursors + 3);
    loop_param_types.push(IrType::I64); // row_ctr
    loop_param_types.push(IrType::I64); // val_ctr
    for _ in 0..n_cursors {
        loop_param_types.push(IrType::I64); // cursor_i
    }
    loop_param_types.push(IrType::I64); // count
    let n_params = n_cursors + 3;

    // ---- Entry block ----
    let entry = b.create_block(&[]);
    b.switch_to(entry);

    let ctx = b.arg(0, IrType::Ptr);
    let row_count = b.load_field(IrType::I64, ctx, PAGE_CTX_ROW_COUNT);
    let result_ptr = b.load_field(IrType::Ptr, ctx, PAGE_CTX_RESULT_PTR);
    let def_bitmap = b.load_field(IrType::Ptr, ctx, PAGE_CTX_DEF_BITMAP);

    let zero = b.iconst(0);
    let init_count = b.load_field(IrType::I64, result_ptr, 0);

    let mut init_args = Vec::with_capacity(n_params);
    init_args.push(zero); // row_ctr
    init_args.push(zero); // val_ctr
    for &mi in &mat_indices {
        let out_ptr = b.load_field(IrType::Ptr, ctx, page_col_offset(mi, PAGE_SLOT_FILTER_LO));
        init_args.push(out_ptr);
    }
    init_args.push(init_count);

    // No-nulls loop also hoists values_data pointers as block params.
    // Layout: (row_ctr, val_ctr, cursor_0..N-1, count, values_data_0..N-1)
    let mut nn_param_types = loop_param_types.clone();
    for _ in &mat_indices {
        nn_param_types.push(IrType::Ptr); // values_data_i
    }
    let nn_n_params = n_params + n_cursors;

    let nn_hdr = b.create_block(&nn_param_types);
    let nn_body = b.create_block(&nn_param_types);
    let epilogue = b.create_block(&loop_param_types);

    // With-nulls loop has extra block params for hoisted loop-invariant
    // values: def_bitmap, per-column values_data, per-column sentinel.
    //
    // Layout: (row_ctr, val_ctr, cursor_0..N-1, count,
    //          def_bitmap, values_data_0..N-1, sentinel_0..N-1)
    let n_hoisted = 1 + n_cursors + n_cursors; // def_bitmap + values_data[] + sentinel[]
    let mut wn_param_types = loop_param_types.clone();
    wn_param_types.push(IrType::Ptr); // def_bitmap
    for _ in &mat_indices {
        wn_param_types.push(IrType::Ptr); // values_data_i
    }
    for &mi in &mat_indices {
        let pt = spec.columns[mi].physical_type;
        wn_param_types.push(pt.ir_acc_type()); // sentinel_i
    }
    let wn_n_params = n_params + n_hoisted;

    let wn_hdr = b.create_block(&wn_param_types);
    let wn_body = b.create_block(&wn_param_types);

    // Build with-nulls init args: base args + def_bitmap + values_data[] + sentinel[].
    let mut wn_init_args = init_args.clone();
    wn_init_args.push(def_bitmap);
    for &mi in &mat_indices {
        let vd = b.load_field(IrType::Ptr, ctx, page_col_offset(mi, PAGE_SLOT_VALUES_DATA));
        wn_init_args.push(vd);
    }
    for &mi in &mat_indices {
        let pt = spec.columns[mi].physical_type;
        let sentinel = match pt {
            JitPhysicalType::Int8 => b.iconst(i8::MIN as i64),
            JitPhysicalType::Int16 => b.iconst(i16::MIN as i64),
            JitPhysicalType::Int32 => b.iconst(i32::MIN as i64),
            JitPhysicalType::Int64 => b.iconst(i64::MIN),
            JitPhysicalType::Float => b.fconst(f64::NAN),
            JitPhysicalType::Double => b.fconst(f64::NAN),
            _ => unreachable!(),
        };
        wn_init_args.push(sentinel);
    }

    // Build nn init args: base args + values_data[].
    let mut nn_init_args = init_args.clone();
    for &mi in &mat_indices {
        let vd = b.load_field(IrType::Ptr, ctx, page_col_offset(mi, PAGE_SLOT_VALUES_DATA));
        nn_init_args.push(vd);
    }

    let bm_is_null = b.is_zero(def_bitmap);
    b.cond_br(bm_is_null, nn_hdr, &nn_init_args, wn_hdr, &wn_init_args);

    // ---- No-nulls path (values_data hoisted) ----
    b.switch_to(nn_hdr);
    let row_ctr = b.param(0);
    let all_nn: Vec<ValRef> = (0..nn_n_params).map(|i| b.param(i as u8)).collect();
    let base_p: Vec<ValRef> = (0..n_params).map(|i| b.param(i as u8)).collect();
    let done = b.icmp(CmpOp::Ge, row_ctr, row_count);
    b.cond_br(done, epilogue, &base_p, nn_body, &all_nn);

    b.switch_to(nn_body);
    emit_mat_body_nn(b, ctx, spec, &filter_indices, &mat_indices, &nn_param_types, n_cursors, nn_hdr);

    // ---- With-nulls path (BRANCHLESS, loop-invariants hoisted) ----
    b.switch_to(wn_hdr);
    let row_ctr = b.param(0);
    let all_wn: Vec<ValRef> = (0..wn_n_params).map(|i| b.param(i as u8)).collect();
    // Epilogue only needs the base params (first n_params).
    let base_p: Vec<ValRef> = (0..n_params).map(|i| b.param(i as u8)).collect();
    let done = b.icmp(CmpOp::Ge, row_ctr, row_count);
    b.cond_br(done, epilogue, &base_p, wn_body, &all_wn);

    // Branchless body using hoisted block params.
    b.switch_to(wn_body);
    let row_ctr = b.param(0);
    let val_ctr = b.param(1);
    let count = b.param((n_params - 1) as u8);

    // Read hoisted values from block params (no context reload).
    let hoisted_bm = b.param(n_params as u8); // def_bitmap
    let bm_byte = b.load_byte(hoisted_bm, row_ctr);
    let is_nn = b.is_nonzero(bm_byte);

    let mut new_cursors = Vec::with_capacity(n_cursors);
    for (cursor_idx, &col_idx) in mat_indices.iter().enumerate() {
        let col = &spec.columns[col_idx];
        let pt = col.physical_type;
        let cursor = b.param((cursor_idx + 2) as u8);

        // Hoisted values_data and sentinel from block params.
        let hoisted_values = b.param((n_params + 1 + cursor_idx) as u8);
        let hoisted_sentinel = b.param((n_params + 1 + n_cursors + cursor_idx) as u8);

        // Unconditionally load value at val_ctr.
        let val = ir_values::emit_load_ir(b, pt, hoisted_values, val_ctr);

        // Select: is_nn ? val : sentinel
        let acc_ty = pt.ir_acc_type();
        let store_val = b.select(acc_ty, is_nn, val, hoisted_sentinel);

        let width_log2: u8 = match pt {
            JitPhysicalType::Int8 => 0,
            JitPhysicalType::Int16 => 1,
            JitPhysicalType::Int32 | JitPhysicalType::Float => 2,
            JitPhysicalType::Int64 | JitPhysicalType::Double => 3,
            _ => unreachable!(),
        };

        let final_val = if pt == JitPhysicalType::Float {
            b.cvt_f64_f32(store_val)
        } else {
            store_val
        };

        b.store(cursor, final_val, width_log2);
        let size = b.iconst(pt.ir_type_size());
        new_cursors.push(b.add(cursor, size));
    }

    // val_ctr += bitmap_byte (0 or 1) — branchless conditional increment.
    let next_val = b.add(val_ctr, bm_byte);
    let new_count = b.inc(count);
    let next_row = b.inc(row_ctr);

    // Pass all wn params back including hoisted values (unchanged).
    let mut body_args = Vec::with_capacity(wn_n_params);
    body_args.push(next_row);
    body_args.push(next_val);
    body_args.extend_from_slice(&new_cursors);
    body_args.push(new_count);
    // Hoisted values pass through unchanged.
    body_args.push(hoisted_bm);
    for cursor_idx in 0..n_cursors {
        body_args.push(b.param((n_params + 1 + cursor_idx) as u8)); // values_data
    }
    for cursor_idx in 0..n_cursors {
        body_args.push(b.param((n_params + 1 + n_cursors + cursor_idx) as u8)); // sentinel
    }
    b.br(wn_hdr, &body_args);

    // ---- Epilogue ----
    b.switch_to(epilogue);
    let count = b.param((n_params - 1) as u8);
    b.store_field(result_ptr, 0, count);
    let ret_val = b.iconst(0);
    b.ret(ret_val);

    Ok(())
}

/// Emit no-nulls materialize body (single counter, values_data hoisted).
fn emit_mat_body_nn(
    b: &mut IrBuilder,
    ctx: ValRef,
    spec: &MultiPipelineSpec,
    filter_indices: &[usize],
    mat_indices: &[usize],
    loop_param_types: &[IrType],
    n_cursors: usize,
    loop_hdr: super::super::ir::BlockRef,
) {
    use super::super::JitPhysicalType;

    let base_n = n_cursors + 3; // row_ctr + val_ctr + cursors + count
    let nn_n = base_n + n_cursors; // + hoisted values_data[]
    let _row_ctr = b.param(0);
    let val_ctr = b.param(1); // == row_ctr in nn path

    let combined_pass = emit_page_filter_chain(b, ctx, spec, filter_indices, val_ctr);

    let store_block = b.create_block(loop_param_types);
    let skip_block = b.create_block(loop_param_types);

    let all_p: Vec<ValRef> = (0..nn_n).map(|i| b.param(i as u8)).collect();
    match combined_pass {
        Some(pass) => b.cond_br(pass, store_block, &all_p, skip_block, &all_p),
        None => b.br(store_block, &all_p),
    }

    // ---- Store block ----
    b.switch_to(store_block);
    let row_ctr = b.param(0);
    let val_ctr = b.param(1);
    let count = b.param((base_n - 1) as u8);

    let mut new_cursors = Vec::with_capacity(n_cursors);
    for (cursor_idx, &col_idx) in mat_indices.iter().enumerate() {
        let col = &spec.columns[col_idx];
        let pt = col.physical_type;
        let cursor = b.param((cursor_idx + 2) as u8);

        // Hoisted values_data from block param.
        let values = b.param((base_n + cursor_idx) as u8);
        let val = ir_values::emit_load_ir(b, pt, values, val_ctr);

        let width_log2: u8 = match pt {
            JitPhysicalType::Int8 => 0,
            JitPhysicalType::Int16 => 1,
            JitPhysicalType::Int32 | JitPhysicalType::Float => 2,
            JitPhysicalType::Int64 | JitPhysicalType::Double => 3,
            _ => unreachable!(),
        };

        let store_val = if pt == JitPhysicalType::Float {
            b.cvt_f64_f32(val)
        } else {
            val
        };

        b.store(cursor, store_val, width_log2);
        let size = b.iconst(pt.ir_type_size());
        new_cursors.push(b.add(cursor, size));
    }

    let new_count = b.inc(count);
    let next_row = b.inc(row_ctr);
    let mut store_args = Vec::with_capacity(nn_n);
    store_args.push(next_row);
    store_args.push(next_row); // val_ctr = row_ctr
    store_args.extend_from_slice(&new_cursors);
    store_args.push(new_count);
    // Pass through hoisted values_data.
    for cursor_idx in 0..n_cursors {
        store_args.push(b.param((base_n + cursor_idx) as u8));
    }
    b.br(loop_hdr, &store_args);

    // ---- Skip block (filter failed) ----
    b.switch_to(skip_block);
    let row_ctr = b.param(0);
    let next_row = b.inc(row_ctr);
    let mut skip_args: Vec<ValRef> = (0..nn_n).map(|i| b.param(i as u8)).collect();
    skip_args[0] = next_row;
    skip_args[1] = next_row;
    b.br(loop_hdr, &skip_args);
}

/// Reload column data from page context and evaluate all filter predicates.
fn emit_page_filter_chain(
    b: &mut IrBuilder,
    ctx: ValRef,
    spec: &MultiPipelineSpec,
    filter_indices: &[usize],
    val_ctr: ValRef,
) -> Option<ValRef> {
    if filter_indices.is_empty() {
        return None;
    }

    let mut combined: Option<ValRef> = None;

    for &idx in filter_indices {
        let col = &spec.columns[idx];

        if col.filter_op == JitFilterOp::None
            || col.filter_op == JitFilterOp::IsNull
            || col.filter_op == JitFilterOp::IsNotNull
        {
            continue;
        }

        let col_spec = column_to_pipeline_spec(col, spec.output_mode);

        let values = b.load_field(
            IrType::Ptr, ctx, page_col_offset(idx, PAGE_SLOT_VALUES_DATA),
        );
        let val = ir_values::emit_load_ir(b, col.physical_type, values, val_ctr);

        let is_between = col.filter_op == JitFilterOp::Between;

        let (flo, fhi) = if col.physical_type.is_float() {
            let flo_raw = b.load_field(
                IrType::I64, ctx, page_col_offset(idx, PAGE_SLOT_FILTER_LO),
            );
            let flo = b.bitcast_to_f64(flo_raw);
            let fhi = if is_between {
                let fhi_raw = b.load_field(
                    IrType::I64, ctx, page_col_offset(idx, PAGE_SLOT_FILTER_HI),
                );
                b.bitcast_to_f64(fhi_raw)
            } else {
                flo
            };
            (flo, fhi)
        } else {
            let flo = b.load_field(
                IrType::I64, ctx, page_col_offset(idx, PAGE_SLOT_FILTER_LO),
            );
            let fhi = if is_between {
                b.load_field(IrType::I64, ctx, page_col_offset(idx, PAGE_SLOT_FILTER_HI))
            } else {
                flo
            };
            (flo, fhi)
        };

        let pass = ir_values::emit_filter_ir(b, &col_spec, val, flo, fhi);

        if let Some(pass) = pass {
            combined = Some(match combined {
                None => pass,
                Some(prev) => match spec.filter_combine {
                    FilterCombine::And => b.and(prev, pass),
                    FilterCombine::Or => {
                        let not_a = b.not(prev);
                        let not_b = b.not(pass);
                        let and_nots = b.and(not_a, not_b);
                        b.not(and_nots)
                    }
                },
            });
        }
    }

    combined
}
