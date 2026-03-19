//! Multi-column IR emitter.
//!
//! Builds IR for multi-column filter+aggregate pipelines. Reuses per-column
//! helpers from `values.rs` (`emit_load_ir`, `emit_filter_ir`,
//! `emit_output_ir`).
//!
//! **Register strategy**: only three context fields are pre-assigned to
//! callee-saved registers (ctx→x0, row_count→x20, result_ptr→x26). All
//! per-column data (value pointers, bitmaps, filter constants) is reloaded
//! from ctx at the start of each block that needs it, using fresh scratch
//! registers. This removes any hard limit on column count — the only
//! constraint is the scratch pool depth within a single block (~12 GPRs).

use super::super::ir::{CmpOp, IrType, ValRef};
use super::super::ir_builder::IrBuilder;
use super::super::multi::{
    col_offset, column_to_pipeline_spec, ColumnRole, FilterCombine, MultiPipelineSpec,
    CTX_RESULT_PTR, CTX_ROW_COUNT, SLOT_FILTER_HI, SLOT_FILTER_LO, SLOT_NULL_BITMAP,
    SLOT_VALUES_DATA,
};
use super::super::values as ir_values;
use super::super::{JitError, JitFilterOp, JitOutputMode};

/// Emit IR for a multi-column pipeline.
pub fn emit_ir(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    match spec.output_mode {
        JitOutputMode::Aggregate => emit_ir_aggregate(b, spec),
        JitOutputMode::Materialize => emit_ir_materialize(b, spec),
    }
}

fn emit_ir_aggregate(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    let cols = &spec.columns;

    // Find aggregate column index.
    let agg_idx = cols
        .iter()
        .position(|c| matches!(c.role, ColumnRole::Aggregate | ColumnRole::FilterAggregate))
        .ok_or_else(|| JitError::UnsupportedCombination("no aggregate column".into()))?;
    let agg_col = &cols[agg_idx];
    let agg_pt = agg_col.physical_type;
    let acc_ty = agg_pt.ir_acc_type();
    let agg_spec = column_to_pipeline_spec(agg_col, spec.output_mode);

    // Collect filter column indices.
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
    let row_count = b.load_field(IrType::I64, ctx, CTX_ROW_COUNT);
    let result_ptr = b.load_field(IrType::Ptr, ctx, CTX_RESULT_PTR);

    let zero = b.iconst(0);
    let init_acc = if acc_ty == IrType::F64 {
        b.fconst(0.0)
    } else {
        zero
    };
    let init_count = b.iconst(0);
    let init_has = b.iconst(0);

    // Block params: (ctr: I64, acc: acc_ty, count: I64, has: I64)
    let loop_params = &[IrType::I64, acc_ty, IrType::I64, IrType::I64];
    let init_args = &[zero, init_acc, init_count, init_has];

    let wn_hdr = b.create_block(loop_params);
    let null_check = b.create_block(loop_params);
    let wn_body = b.create_block(loop_params);
    let wn_inc = b.create_block(loop_params);
    let nn_hdr = b.create_block(loop_params);
    let nn_body = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    // Check if ALL bitmaps are null → no-nulls path.
    // Bitmap pointers are loaded into scratch (consumed here, not persisted).
    let all_bm_null = {
        let bm0 = b.load_field(IrType::Ptr, ctx, col_offset(0, SLOT_NULL_BITMAP));
        let mut all = b.is_zero(bm0);
        for i in 1..cols.len() {
            let bm = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
            let z = b.is_zero(bm);
            all = b.and(all, z);
        }
        all
    };
    b.cond_br(all_bm_null, nn_hdr, init_args, wn_hdr, init_args);

    // ---- With-nulls path ----

    // wn_hdr: check counter < row_count
    b.switch_to(wn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue,
        &[ctr, acc, count, has],
        null_check,
        &[ctr, acc, count, has],
    );

    // Null check chain: one pair of blocks per column.
    //
    // For each column i:
    //   null_ptr_check_i: load bitmap pointer, if null → skip to next column
    //                     (that column has no nulls), else → bitmap_byte_check_i
    //   bitmap_byte_check_i: load bitmap byte, if zero (null row) → wn_inc,
    //                        else → next column's null_ptr_check
    //
    // After all columns pass: → wn_body.
    //
    // This avoids loading bytes from null bitmap pointers (which would
    // segfault) and also short-circuits: if any column's bitmap byte is
    // null for this row, we skip immediately without checking the rest.
    let n_cols = cols.len();
    let mut null_ptr_checks = Vec::with_capacity(n_cols);
    let mut bm_byte_checks = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        null_ptr_checks.push(b.create_block(loop_params));
        bm_byte_checks.push(b.create_block(loop_params));
    }

    // Wire null_check → first column's null_ptr_check.
    b.switch_to(null_check);
    let (ctr, acc, count, has) = b.params_4();
    b.br(null_ptr_checks[0], &[ctr, acc, count, has]);

    for i in 0..n_cols {
        let next = if i + 1 < n_cols {
            null_ptr_checks[i + 1]
        } else {
            wn_body // all columns passed
        };

        // null_ptr_check_i: is this column's bitmap pointer null?
        b.switch_to(null_ptr_checks[i]);
        let (ctr, acc, count, has) = b.params_4();
        let bm_ptr = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
        let ptr_is_null = b.is_zero(bm_ptr);
        b.cond_br(
            ptr_is_null,
            next,
            &[ctr, acc, count, has], // skip — no nulls for this column
            bm_byte_checks[i],
            &[ctr, acc, count, has],
        );

        // bitmap_byte_check_i: load byte, check if row is null.
        b.switch_to(bm_byte_checks[i]);
        let (ctr, acc, count, has) = b.params_4();
        let bm_ptr2 = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
        let bm_byte = b.load_byte(bm_ptr2, ctr);
        let is_nn = b.is_nonzero(bm_byte);
        b.cond_br(
            is_nn,
            next,
            &[ctr, acc, count, has], // non-null → continue chain
            wn_inc,
            &[ctr, acc, count, has], // null row → skip
        );
    }

    // wn_body: reload column data from ctx, filter, aggregate.
    b.switch_to(wn_body);
    let (ctr, acc, count, has) = b.params_4();
    let combined_pass = emit_filter_chain(b, ctx, spec, &filter_indices, ctr);
    let agg_values = b.load_field(IrType::Ptr, ctx, col_offset(agg_idx, SLOT_VALUES_DATA));
    let agg_val = ir_values::emit_load_ir(b, agg_pt, agg_values, ctr);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, &agg_spec, agg_val, acc, count, has, combined_pass, None);
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, new_acc, new_count, new_has]);

    // wn_inc: null row, skip
    b.switch_to(wn_inc);
    let (ctr, acc, count, has) = b.params_4();
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, acc, count, has]);

    // ---- No-nulls path ----

    b.switch_to(nn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue,
        &[ctr, acc, count, has],
        nn_body,
        &[ctr, acc, count, has],
    );

    b.switch_to(nn_body);
    let (ctr, acc, count, has) = b.params_4();
    let combined_pass = emit_filter_chain(b, ctx, spec, &filter_indices, ctr);
    let agg_values = b.load_field(IrType::Ptr, ctx, col_offset(agg_idx, SLOT_VALUES_DATA));
    let agg_val = ir_values::emit_load_ir(b, agg_pt, agg_values, ctr);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, &agg_spec, agg_val, acc, count, has, combined_pass, None);
    let next_ctr = b.inc(ctr);
    b.br(nn_hdr, &[next_ctr, new_acc, new_count, new_has]);

    // ---- Epilogue ----
    b.switch_to(epilogue);
    let (_ctr, acc, count, has) = b.params_4();
    super::plain::emit_epilogue_stores(b, &agg_spec, result_ptr, acc, count, has);

    Ok(())
}

/// Emit IR for multi-column materialize mode.
///
/// Filters work identically to aggregate mode. For each row that passes,
/// every Materialize column's value is written to its output buffer and
/// the cursor advances. Each Materialize column's output buffer pointer
/// comes from `ColumnSlot.filter_lo` (repurposed).
///
/// Block params: `(ctr, cursor_0, cursor_1, ..., cursor_N-1, count)`.
/// The result_ptr is used to write the final count.
fn emit_ir_materialize(b: &mut IrBuilder, spec: &MultiPipelineSpec) -> Result<(), JitError> {
    let cols = &spec.columns;

    // Collect filter and materialize column indices.
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

    // Block params: (ctr: I64, cursor_0: I64, ..., cursor_N-1: I64, count: I64)
    let mut loop_param_types = Vec::with_capacity(n_cursors + 2);
    loop_param_types.push(IrType::I64); // ctr
    for _ in 0..n_cursors {
        loop_param_types.push(IrType::I64); // cursor_i
    }
    loop_param_types.push(IrType::I64); // count

    // ---- Entry block ----
    let entry = b.create_block(&[]);
    b.switch_to(entry);

    let ctx = b.arg(0, IrType::Ptr);
    let row_count = b.load_field(IrType::I64, ctx, CTX_ROW_COUNT);
    let result_ptr = b.load_field(IrType::Ptr, ctx, CTX_RESULT_PTR);

    let zero = b.iconst(0);

    // Initial cursors: load each Materialize column's output buffer pointer
    // from ColumnSlot.filter_lo.
    let mut init_args = Vec::with_capacity(n_cursors + 2);
    init_args.push(zero); // ctr = 0
    for &mi in &mat_indices {
        let out_ptr = b.load_field(IrType::Ptr, ctx, col_offset(mi, SLOT_FILTER_LO));
        init_args.push(out_ptr);
    }
    init_args.push(zero); // count = 0

    let wn_hdr = b.create_block(&loop_param_types);
    let null_check = b.create_block(&loop_param_types);
    let wn_body = b.create_block(&loop_param_types);
    let wn_inc = b.create_block(&loop_param_types);
    let nn_hdr = b.create_block(&loop_param_types);
    let nn_body = b.create_block(&loop_param_types);
    let epilogue = b.create_block(&loop_param_types);

    // Check if ALL bitmaps are null → no-nulls path.
    let all_bm_null = {
        let bm0 = b.load_field(IrType::Ptr, ctx, col_offset(0, SLOT_NULL_BITMAP));
        let mut all = b.is_zero(bm0);
        for i in 1..cols.len() {
            let bm = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
            let z = b.is_zero(bm);
            all = b.and(all, z);
        }
        all
    };
    b.cond_br(all_bm_null, nn_hdr, &init_args, wn_hdr, &init_args);

    // ---- With-nulls path ----
    b.switch_to(wn_hdr);
    let ctr = b.param(0);
    let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(done, epilogue, &all_params, null_check, &all_params);

    // Null check chain (same as aggregate mode).
    let n_cols = cols.len();
    let mut null_ptr_checks = Vec::with_capacity(n_cols);
    let mut bm_byte_checks = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        null_ptr_checks.push(b.create_block(&loop_param_types));
        bm_byte_checks.push(b.create_block(&loop_param_types));
    }

    b.switch_to(null_check);
    let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();
    b.br(null_ptr_checks[0], &all_params);

    for i in 0..n_cols {
        let next = if i + 1 < n_cols {
            null_ptr_checks[i + 1]
        } else {
            wn_body
        };

        b.switch_to(null_ptr_checks[i]);
        let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|j| b.param(j as u8)).collect();
        let bm_ptr = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
        let ptr_is_null = b.is_zero(bm_ptr);
        b.cond_br(ptr_is_null, next, &all_params, bm_byte_checks[i], &all_params);

        b.switch_to(bm_byte_checks[i]);
        let ctr = b.param(0);
        let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|j| b.param(j as u8)).collect();
        let bm_ptr2 = b.load_field(IrType::Ptr, ctx, col_offset(i, SLOT_NULL_BITMAP));
        let bm_byte = b.load_byte(bm_ptr2, ctr);
        let is_nn = b.is_nonzero(bm_byte);
        b.cond_br(is_nn, next, &all_params, wn_inc, &all_params);
    }

    // wn_body: filter, then materialize each output column.
    b.switch_to(wn_body);
    emit_materialize_body(b, ctx, spec, &filter_indices, &mat_indices, &loop_param_types, n_cursors, wn_hdr);

    // wn_inc: null row, skip.
    b.switch_to(wn_inc);
    let ctr = b.param(0);
    let next_ctr = b.inc(ctr);
    let mut inc_args: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();
    inc_args[0] = next_ctr;
    b.br(wn_hdr, &inc_args);

    // ---- No-nulls path ----
    b.switch_to(nn_hdr);
    let ctr = b.param(0);
    let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(done, epilogue, &all_params, nn_body, &all_params);

    b.switch_to(nn_body);
    emit_materialize_body(b, ctx, spec, &filter_indices, &mat_indices, &loop_param_types, n_cursors, nn_hdr);

    // ---- Epilogue ----
    b.switch_to(epilogue);
    let count = b.param((n_cursors + 1) as u8);
    b.store_field(result_ptr, 0, count); // write count to result_ptr
    let ret_val = b.iconst(0);
    b.ret(ret_val);

    Ok(())
}

/// Emit the body block for materialize mode.
///
/// Uses a branch-split approach: the body block evaluates the filter chain
/// and conditionally branches to a "store" block (filter passed) or a
/// "skip" block (filter failed). This avoids using multiple Selects on the
/// same pass condition, which would exhaust the scratch register pool and
/// clobber the pass boolean for 2+ Materialize columns.
///
/// Must be called with the builder switched to a fresh body block (wn_body
/// or nn_body). Creates two extra blocks (store, skip) per call.
fn emit_materialize_body(
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

    let ctr = b.param(0);

    // Evaluate filter chain.
    let combined_pass = emit_filter_chain(b, ctx, spec, filter_indices, ctr);

    // If there's no filter, every row passes — go straight to stores.
    // If there's a filter, CondBr to a store block or skip block.
    let store_block = b.create_block(loop_param_types);
    let skip_block = b.create_block(loop_param_types);

    let all_params: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();

    match combined_pass {
        Some(pass) => {
            b.cond_br(pass, store_block, &all_params, skip_block, &all_params);
        }
        None => {
            b.br(store_block, &all_params);
        }
    }

    // ---- Store block: filter passed, store values and advance cursors ----
    b.switch_to(store_block);
    let ctr = b.param(0);
    let count = b.param((n_cursors + 1) as u8);

    let mut new_cursors: Vec<ValRef> = Vec::with_capacity(n_cursors);
    for (cursor_idx, &col_idx) in mat_indices.iter().enumerate() {
        let col = &spec.columns[col_idx];
        let pt = col.physical_type;
        let cursor = b.param((cursor_idx + 1) as u8);

        let values = b.load_field(IrType::Ptr, ctx, col_offset(col_idx, SLOT_VALUES_DATA));
        let val = ir_values::emit_load_ir(b, pt, values, ctr);

        let width_log2: u8 = match pt {
            JitPhysicalType::Int8 => 0,
            JitPhysicalType::Int16 => 1,
            JitPhysicalType::Int32 | JitPhysicalType::Float => 2,
            JitPhysicalType::Int64 | JitPhysicalType::Double => 3,
            _ => unreachable!("wide types rejected by validation"),
        };

        let store_val = if pt == JitPhysicalType::Float {
            b.cvt_f64_f32(val)
        } else {
            val
        };

        b.store(cursor, store_val, width_log2);
        let size = b.iconst(pt.ir_type_size());
        let new_cursor = b.add(cursor, size);
        new_cursors.push(new_cursor);
    }

    let new_count = b.inc(count);
    let next_ctr = b.inc(ctr);
    let mut store_args = Vec::with_capacity(n_cursors + 2);
    store_args.push(next_ctr);
    store_args.extend_from_slice(&new_cursors);
    store_args.push(new_count);
    b.br(loop_hdr, &store_args);

    // ---- Skip block: filter failed, just increment counter ----
    b.switch_to(skip_block);
    let ctr = b.param(0);
    let next_ctr = b.inc(ctr);
    let mut skip_args: Vec<ValRef> = (0..n_cursors + 2).map(|i| b.param(i as u8)).collect();
    skip_args[0] = next_ctr;
    b.br(loop_hdr, &skip_args);
}

/// Reload column data from ctx and evaluate all filter predicates,
/// combining results with AND/OR **incrementally**.
///
/// Each column's values and filter constants are loaded, compared, and
/// immediately combined with the running result before the next column's
/// loads. This ensures the backend's `ensure_materialized` consumes each
/// column's scratch registers before the next column's loads can clobber
/// them via round-robin reuse.
fn emit_filter_chain(
    b: &mut IrBuilder,
    ctx: ValRef,
    spec: &MultiPipelineSpec,
    filter_indices: &[usize],
    ctr: ValRef,
) -> Option<ValRef> {
    if filter_indices.is_empty() {
        return None;
    }

    let mut combined: Option<ValRef> = None;

    for &idx in filter_indices {
        let col = &spec.columns[idx];

        // IsNull/IsNotNull don't produce value-based filter predicates.
        // IsNull is rejected by validation; IsNotNull means "always pass"
        // (null rows are already skipped by the bitmap check chain).
        if col.filter_op == JitFilterOp::None
            || col.filter_op == JitFilterOp::IsNull
            || col.filter_op == JitFilterOp::IsNotNull
        {
            continue;
        }

        let col_spec = column_to_pipeline_spec(col, spec.output_mode);

        // Reload values pointer from ctx.
        let values = b.load_field(IrType::Ptr, ctx, col_offset(idx, SLOT_VALUES_DATA));
        let val = ir_values::emit_load_ir(b, col.physical_type, values, ctr);

        // Load only the filter constants actually needed.
        let is_between = col.filter_op == JitFilterOp::Between;

        // At this point we know we have a real value filter (Eq/Ne/Lt/Le/Gt/Ge/Between).
        let (flo, fhi) = if col.physical_type.is_float() {
            let flo_raw = b.load_field(IrType::I64, ctx, col_offset(idx, SLOT_FILTER_LO));
            let flo = b.bitcast_to_f64(flo_raw);
            let fhi = if is_between {
                let fhi_raw = b.load_field(IrType::I64, ctx, col_offset(idx, SLOT_FILTER_HI));
                b.bitcast_to_f64(fhi_raw)
            } else {
                flo // unused — emit_filter_ir ignores fhi for non-Between
            };
            (flo, fhi)
        } else {
            let flo = b.load_field(IrType::I64, ctx, col_offset(idx, SLOT_FILTER_LO));
            let fhi = if is_between {
                b.load_field(IrType::I64, ctx, col_offset(idx, SLOT_FILTER_HI))
            } else {
                flo // unused
            };
            (flo, fhi)
        };

        let pass = ir_values::emit_filter_ir(b, &col_spec, val, flo, fhi);

        // Combine incrementally: the And/Or forces the backend to
        // materialize the deferred ICmp *now*, before the next column's
        // loads can clobber the scratch registers holding its operands.
        if let Some(pass) = pass {
            combined = Some(match combined {
                None => pass,
                Some(prev) => match spec.filter_combine {
                    FilterCombine::And => b.and(prev, pass),
                    FilterCombine::Or => {
                        // De Morgan: a OR b = NOT(AND(NOT a, NOT b))
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
