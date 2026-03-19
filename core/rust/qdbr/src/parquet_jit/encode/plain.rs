//! PLAIN encoding emitter.
//!
//! Generates a simple indexed loop over a flat value array with null bitmap
//! handling, filter application, and aggregate accumulation.  This same loop
//! structure is reused by all encodings that pre-decode to flat arrays.

#[cfg(target_arch = "x86_64")]
use super::super::codegen::Codegen;
#[cfg(target_arch = "x86_64")]
use super::super::values;
use super::super::{JitError, JitFilterOp, PipelineSpec};

#[cfg(target_arch = "x86_64")]
pub fn emit(cg: &mut Codegen, spec: &PipelineSpec) -> Result<(), JitError> {
    let is_null_filter = spec.filter_op == JitFilterOp::IsNull;

    let lbl_loop_nulls = cg.label();
    let lbl_loop_no_nulls = cg.label();
    let lbl_done = cg.label();
    let lbl_skip_nulls = cg.label();
    let lbl_skip_no_nulls = cg.label();
    let lbl_null_row = cg.label();
    let lbl_set_n = cg.label();
    let lbl_after_n = cg.label();
    let lbl_set_nn = cg.label();
    let lbl_after_nn = cg.label();

    cg.check_bitmap_null(lbl_loop_no_nulls);

    // ---- with-nulls path ----
    if is_null_filter {
        emit_is_null_loop(cg, lbl_loop_nulls, lbl_null_row, lbl_done)?;
    } else {
        cg.bind(lbl_loop_nulls);
        cg.loop_check(lbl_done);
        cg.skip_if_null(lbl_null_row);

        values::emit_load(cg, spec.physical_type)?;
        values::emit_filter(cg, spec, lbl_skip_nulls)?;
        values::emit_output(cg, spec, lbl_set_n, lbl_after_n)?;

        cg.bind(lbl_skip_nulls);
        cg.bind(lbl_null_row);
        cg.loop_inc(lbl_loop_nulls);
    }

    // ---- no-nulls path ----
    if is_null_filter {
        cg.bind(lbl_loop_no_nulls);
        cg.jump(lbl_done);
    } else {
        cg.bind(lbl_loop_no_nulls);
        cg.loop_check(lbl_done);

        values::emit_load(cg, spec.physical_type)?;
        values::emit_filter(cg, spec, lbl_skip_no_nulls)?;
        values::emit_output(cg, spec, lbl_set_nn, lbl_after_nn)?;

        cg.bind(lbl_skip_no_nulls);
        cg.loop_inc(lbl_loop_no_nulls);
    }

    cg.bind(lbl_done);
    Ok(())
}

/// IS_NULL filter: count null rows.
#[cfg(target_arch = "x86_64")]
fn emit_is_null_loop(
    cg: &mut Codegen,
    lbl_loop: dynasmrt::DynamicLabel,
    lbl_not_null: dynasmrt::DynamicLabel,
    lbl_done: dynasmrt::DynamicLabel,
) -> Result<(), JitError> {
    cg.bind(lbl_loop);
    cg.loop_check(lbl_done);
    cg.skip_if_not_null(lbl_not_null);
    cg.inc_count();
    cg.bind(lbl_not_null);
    cg.loop_inc(lbl_loop);
    Ok(())
}

// =========================================================================
// IR-based PLAIN encoding emitter
// =========================================================================

use super::super::ir::{IrType, ValRef};
use super::super::ir_builder::IrBuilder;
use super::super::values as ir_values;
use super::super::{JitAggregateOp, JitOutputMode, JitPhysicalType};

/// Emit IR for the PLAIN encoding loop.
///
/// Function arguments (ABI):
///   arg0 = values_data (Ptr)
///   arg1 = null_bitmap (Ptr, may be null)
///   arg2 = row_count (I64)
///   arg3 = filter_lo (I64)
///   arg4 = filter_hi (I64)
///   arg5 = result_ptr (Ptr)
#[allow(dead_code)]
pub fn emit_ir(b: &mut IrBuilder, spec: &PipelineSpec) -> Result<(), JitError> {
    let is_null_filter = spec.filter_op == JitFilterOp::IsNull;
    let is_materialize = spec.output_mode == JitOutputMode::Materialize;
    let pt = spec.physical_type;
    let acc_ty = pt.ir_acc_type();

    // ---- entry block (no params) ----
    let entry = b.create_block(&[]);
    b.switch_to(entry);

    let values = b.arg(0, IrType::Ptr);
    let bitmap = b.arg(1, IrType::Ptr);
    let row_count = b.arg(2, IrType::I64);
    let filter_lo_raw = b.arg(3, IrType::I64);
    let filter_hi_raw = b.arg(4, IrType::I64);
    let result_ptr = b.arg(5, IrType::Ptr);

    // FP filter constants: bitcast from i64 args to f64.
    // For I128/I256: filter_lo/hi are pointers — keep them as raw I64 values.
    // The loop body loads from these pointers each iteration to avoid scratch
    // register clobbering across blocks (see emit_filter_ir).
    let (filter_lo, filter_hi) = if pt.is_float() {
        (b.bitcast_to_f64(filter_lo_raw), b.bitcast_to_f64(filter_hi_raw))
    } else {
        (filter_lo_raw, filter_hi_raw)
    };

    // Initial accumulator values.
    // For I128/I256: emit iconst(0) as I64. The backend zero-extends when the
    // target block param type is wider (both halves / all words set to 0).
    let zero_i64 = b.iconst(0);
    let init_acc = match acc_ty {
        IrType::F64 => b.fconst(0.0),
        _ => zero_i64,
    };
    let init_count = b.iconst(0);
    let init_has = b.iconst(0);

    // For materialize: cursor starts at result_ptr.
    // result_ptr is Ptr type, but we store cursor as I64 in block params.
    // The arg is loaded as Ptr — in the backend Ptr and I64 use the same register.

    // ---- build block parameter types ----
    if is_null_filter {
        emit_ir_is_null(b, spec, bitmap, row_count, result_ptr, zero_i64)?;
    } else if is_materialize {
        emit_ir_materialize(
            b, spec, values, bitmap, row_count, filter_lo, filter_hi,
            result_ptr, zero_i64, init_count,
        )?;
    } else {
        emit_ir_aggregate(
            b, spec, values, bitmap, row_count, filter_lo, filter_hi,
            result_ptr, zero_i64, init_acc, init_count, init_has,
        )?;
    }

    Ok(())
}

/// Emit IR for IS_NULL filter (only counts null rows).
fn emit_ir_is_null(
    b: &mut IrBuilder,
    _spec: &PipelineSpec,
    bitmap: ValRef,
    row_count: ValRef,
    result_ptr: ValRef,
    zero: ValRef,
) -> Result<(), JitError> {
    // Block params: (ctr: I64, count: I64)
    let loop_params = &[IrType::I64, IrType::I64];

    let wn_hdr = b.create_block(loop_params);
    let null_check = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    // Check if bitmap is null (no nulls → epilogue with count=0 directly).
    let bm_is_null = b.is_zero(bitmap);
    b.cond_br(
        bm_is_null,
        epilogue, &[zero, zero],
        wn_hdr, &[zero, zero],
    );

    // wn_hdr: loop header — check counter < row_count.
    b.switch_to(wn_hdr);
    let (ctr, count) = b.params_2();
    let done = b.icmp(super::super::ir::CmpOp::Ge, ctr, row_count);
    b.cond_br(done, epilogue, &[ctr, count], null_check, &[ctr, count]);

    // null_check: load bitmap byte, check if row is null (byte == 0).
    b.switch_to(null_check);
    let (ctr, count) = b.params_2();
    let bm_byte = b.load_byte(bitmap, ctr);
    let is_null = b.is_zero(bm_byte);
    let inc_count = b.inc(count);
    let new_count = b.select(IrType::I64, is_null, inc_count, count);
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, new_count]);

    // epilogue: store count to result, return 0.
    b.switch_to(epilogue);
    let (_ctr, count) = b.params_2();
    b.store_field(result_ptr, 0, count); // PipelineResult.count
    let ret_val = b.iconst(0);
    b.ret(ret_val);

    Ok(())
}

/// Emit IR for aggregate mode (non-IsNull).
fn emit_ir_aggregate(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    values: ValRef,
    bitmap: ValRef,
    row_count: ValRef,
    filter_lo: ValRef,
    filter_hi: ValRef,
    result_ptr: ValRef,
    zero: ValRef,
    init_acc: ValRef,
    init_count: ValRef,
    init_has: ValRef,
) -> Result<(), JitError> {
    let pt = spec.physical_type;
    let acc_ty = pt.ir_acc_type();

    // Block params: (ctr: I64, acc: acc_ty, count: I64, has: I64)
    let loop_params = &[IrType::I64, acc_ty, IrType::I64, IrType::I64];

    let wn_hdr = b.create_block(loop_params);
    let null_check = b.create_block(loop_params);
    let wn_body = b.create_block(loop_params);
    let wn_inc = b.create_block(loop_params);
    let nn_hdr = b.create_block(loop_params);
    let nn_body = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    // Entry: check bitmap null → branch to wn_hdr or nn_hdr.
    let bm_is_null = b.is_zero(bitmap);
    let init_args = &[zero, init_acc, init_count, init_has];
    b.cond_br(bm_is_null, nn_hdr, init_args, wn_hdr, init_args);

    // I256 Min/Max with a filter causes register pressure: the I256 data
    // value occupies 4 GPRs (two in callee-saved x27/x28, two in scratch),
    // the filter constant load consumes more scratch GPRs, and the two-Select
    // min/max pattern needs yet more scratch GPRs for destination registers.
    // The round-robin scratch pool (12 entries) wraps around and clobbers the
    // data value's scratch components before the second Select reads them.
    //
    // Fix: split the body block with CondBr so the aggregate runs in a
    // separate block with a fresh scratch pool. The agg block re-loads the
    // value (cheap: one LDP pair from L1) and performs the two-Select
    // min/max without any filter overhead.
    let needs_split = pt == JitPhysicalType::Int256
        && spec.filter_op != JitFilterOp::None
        && spec.filter_op != JitFilterOp::IsNotNull
        && spec.filter_op != JitFilterOp::IsNull
        && matches!(spec.aggregate_op, JitAggregateOp::Min | JitAggregateOp::Max);

    // Create extra agg/skip blocks when splitting is needed.
    let (wn_agg, wn_skip, nn_agg, nn_skip) = if needs_split {
        (
            Some(b.create_block(loop_params)),
            Some(b.create_block(loop_params)),
            Some(b.create_block(loop_params)),
            Some(b.create_block(loop_params)),
        )
    } else {
        (None, None, None, None)
    };

    // ---- with-nulls path ----

    // wn_hdr: check counter < row_count.
    b.switch_to(wn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(super::super::ir::CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, acc, count, has],
        null_check, &[ctr, acc, count, has],
    );

    // null_check: load bitmap byte, skip null rows.
    b.switch_to(null_check);
    let (ctr, acc, count, has) = b.params_4();
    let bm_byte = b.load_byte(bitmap, ctr);
    let is_null = b.is_zero(bm_byte);
    b.cond_br(
        is_null,
        wn_inc, &[ctr, acc, count, has],
        wn_body, &[ctr, acc, count, has],
    );

    // wn_body: load value, apply filter, aggregate.
    b.switch_to(wn_body);
    let (ctr, acc, count, has) = b.params_4();
    let val = ir_values::emit_load_ir(b, pt, values, ctr);
    let pass = ir_values::emit_filter_ir(b, spec, val, filter_lo, filter_hi);
    if needs_split {
        // CondBr split: branch to agg block (filter passed) or skip block.
        let p = pass.unwrap();
        b.cond_br(
            p,
            wn_agg.unwrap(), &[ctr, acc, count, has],
            wn_skip.unwrap(), &[ctr, acc, count, has],
        );
    } else {
        let (new_acc, new_count, new_has) =
            ir_values::emit_output_ir(b, spec, val, acc, count, has, pass, None);
        let next_ctr = b.inc(ctr);
        b.br(wn_hdr, &[next_ctr, new_acc, new_count, new_has]);
    }

    // wn_inc: skip row (null), just increment counter.
    b.switch_to(wn_inc);
    let (ctr, acc, count, has) = b.params_4();
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, acc, count, has]);

    // wn_agg / wn_skip: split blocks for I256 Min/Max with filter.
    if needs_split {
        // wn_agg: re-load val with fresh scratch pool, aggregate without filter.
        b.switch_to(wn_agg.unwrap());
        let (ctr, acc, count, has) = b.params_4();
        let val = ir_values::emit_load_ir(b, pt, values, ctr);
        let (new_acc, new_count, new_has) =
            ir_values::emit_output_ir(b, spec, val, acc, count, has, None, None);
        let next_ctr = b.inc(ctr);
        b.br(wn_hdr, &[next_ctr, new_acc, new_count, new_has]);

        // wn_skip: filter failed, just increment counter.
        b.switch_to(wn_skip.unwrap());
        let (ctr, acc, count, has) = b.params_4();
        let next_ctr = b.inc(ctr);
        b.br(wn_hdr, &[next_ctr, acc, count, has]);
    }

    // ---- no-nulls path ----

    // nn_hdr: check counter < row_count.
    b.switch_to(nn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(super::super::ir::CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, acc, count, has],
        nn_body, &[ctr, acc, count, has],
    );

    // nn_body: load value, filter, aggregate.
    b.switch_to(nn_body);
    let (ctr, acc, count, has) = b.params_4();
    let val = ir_values::emit_load_ir(b, pt, values, ctr);
    let pass = ir_values::emit_filter_ir(b, spec, val, filter_lo, filter_hi);
    if needs_split {
        // CondBr split: branch to agg block (filter passed) or skip block.
        let p = pass.unwrap();
        b.cond_br(
            p,
            nn_agg.unwrap(), &[ctr, acc, count, has],
            nn_skip.unwrap(), &[ctr, acc, count, has],
        );
    } else {
        let (new_acc, new_count, new_has) =
            ir_values::emit_output_ir(b, spec, val, acc, count, has, pass, None);
        let next_ctr = b.inc(ctr);
        b.br(nn_hdr, &[next_ctr, new_acc, new_count, new_has]);
    }

    // nn_agg / nn_skip: split blocks for I256 Min/Max with filter.
    if needs_split {
        // nn_agg: re-load val with fresh scratch pool, aggregate without filter.
        b.switch_to(nn_agg.unwrap());
        let (ctr, acc, count, has) = b.params_4();
        let val = ir_values::emit_load_ir(b, pt, values, ctr);
        let (new_acc, new_count, new_has) =
            ir_values::emit_output_ir(b, spec, val, acc, count, has, None, None);
        let next_ctr = b.inc(ctr);
        b.br(nn_hdr, &[next_ctr, new_acc, new_count, new_has]);

        // nn_skip: filter failed, just increment counter.
        b.switch_to(nn_skip.unwrap());
        let (ctr, acc, count, has) = b.params_4();
        let next_ctr = b.inc(ctr);
        b.br(nn_hdr, &[next_ctr, acc, count, has]);
    }

    // ---- epilogue: store results ----
    b.switch_to(epilogue);
    let (_ctr, acc, count, has) = b.params_4();
    emit_epilogue_stores(b, spec, result_ptr, acc, count, has);

    Ok(())
}

/// Emit IR for materialize mode.
fn emit_ir_materialize(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    values: ValRef,
    bitmap: ValRef,
    row_count: ValRef,
    filter_lo: ValRef,
    filter_hi: ValRef,
    result_ptr: ValRef,
    zero: ValRef,
    init_count: ValRef,
) -> Result<(), JitError> {
    let pt = spec.physical_type;

    // For materialize, filter_hi_raw (arg4) is repurposed as count_ptr.
    // But we already bitcast it for FP... Actually for materialize mode with FP,
    // the filter constants come from filter_lo only (single comparison or none).
    // filter_hi is the count_ptr. We need the raw arg4.
    // Re-read arg4 as Ptr.
    let count_ptr = b.arg(4, IrType::Ptr);

    // Block params: (ctr: I64, cursor: I64, count: I64)
    let loop_params = &[IrType::I64, IrType::I64, IrType::I64];

    let wn_hdr = b.create_block(loop_params);
    let null_check = b.create_block(loop_params);
    let wn_body = b.create_block(loop_params);
    let wn_inc = b.create_block(loop_params);
    let nn_hdr = b.create_block(loop_params);
    let nn_body = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    let init_args = &[zero, result_ptr, init_count];
    let bm_is_null = b.is_zero(bitmap);
    b.cond_br(bm_is_null, nn_hdr, init_args, wn_hdr, init_args);

    // ---- with-nulls path ----
    b.switch_to(wn_hdr);
    let (ctr, cursor, count) = b.params_3();
    let done = b.icmp(super::super::ir::CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, cursor, count],
        null_check, &[ctr, cursor, count],
    );

    b.switch_to(null_check);
    let (ctr, cursor, count) = b.params_3();
    let bm_byte = b.load_byte(bitmap, ctr);
    let is_null = b.is_zero(bm_byte);
    b.cond_br(
        is_null,
        wn_inc, &[ctr, cursor, count],
        wn_body, &[ctr, cursor, count],
    );

    b.switch_to(wn_body);
    let (ctr, cursor, count) = b.params_3();
    let val = ir_values::emit_load_ir(b, pt, values, ctr);
    let pass = ir_values::emit_filter_ir(b, spec, val, filter_lo, filter_hi);
    let dummy_has = b.iconst(0);
    let (new_cursor, new_count, _) =
        ir_values::emit_output_ir(b, spec, val, cursor, count, dummy_has, pass, Some(cursor));
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, new_cursor, new_count]);

    b.switch_to(wn_inc);
    let (ctr, cursor, count) = b.params_3();
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, cursor, count]);

    // ---- no-nulls path ----
    b.switch_to(nn_hdr);
    let (ctr, cursor, count) = b.params_3();
    let done = b.icmp(super::super::ir::CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, cursor, count],
        nn_body, &[ctr, cursor, count],
    );

    b.switch_to(nn_body);
    let (ctr, cursor, count) = b.params_3();
    let val = ir_values::emit_load_ir(b, pt, values, ctr);
    let pass = ir_values::emit_filter_ir(b, spec, val, filter_lo, filter_hi);
    let dummy_has = b.iconst(0);
    let (new_cursor, new_count, _) =
        ir_values::emit_output_ir(b, spec, val, cursor, count, dummy_has, pass, Some(cursor));
    let next_ctr = b.inc(ctr);
    b.br(nn_hdr, &[next_ctr, new_cursor, new_count]);

    // ---- epilogue ----
    b.switch_to(epilogue);
    let (_ctr, _cursor, count) = b.params_3();
    // Store count to the count_ptr.
    b.store_field(count_ptr, 0, count);
    let ret_val = b.iconst(0);
    b.ret(ret_val);

    Ok(())
}

/// Store aggregate results into PipelineResult.
#[allow(dead_code)]
pub fn emit_epilogue_stores(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    result_ptr: ValRef,
    acc: ValRef,
    count: ValRef,
    has: ValRef,
) {
    let pt = spec.physical_type;

    // PipelineResult layout:
    //   offset 0:  count (u64)
    //   offset 8:  value_i64 (i64) — lo word for I128/I256
    //   offset 16: value_f64 (f64)
    //   offset 24: has_value (u64)
    //   offset 32: value_hi (i64) — hi word for I128; w1 for I256
    //   offset 40: value_w2 (i64) — I256
    //   offset 48: value_w3 (i64) — I256

    b.store_field(result_ptr, 0, count);
    b.store_field(result_ptr, 24, has);

    if pt.is_float() {
        b.store_field(result_ptr, 16, acc);
    } else {
        // For I64, I128, I256: store acc at offset 8.
        // For I128/I256 the store instruction handles multi-word writes
        // based on the value type.
        b.store_field(result_ptr, 8, acc);
    }

    let ret_val = b.iconst(0);
    b.ret(ret_val);
}
