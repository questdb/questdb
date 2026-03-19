//! Dictionary-filtered encoding emitter.
//!
//! For RLE_DICTIONARY columns: the compiled Rust caller pre-evaluates the
//! filter on dictionary entries and produces a `match_bitmap`.  The JIT loop
//! walks a flat array of u32 dictionary indices, checks the bitmap, and only
//! decodes (loads from the dictionary) values whose entry passes.
//!
//! Function argument re-mapping for this mode:
//!   values_data (arg0) = flat u32 index array  (pre-decoded RLE/bitpacked)
//!   null_bitmap (arg1) = null bitmap           (1 byte/row, or NULL)
//!   row_count   (arg2) = number of rows
//!   filter_lo   (arg3) = pointer to match_bitmap  (1 byte per dict entry)
//!   filter_hi   (arg4) = pointer to dictionary values (typed array)
//!   result_ptr  (arg5) = output (PipelineResult for aggregate, buffer for materialize)

#[cfg(target_arch = "x86_64")]
use super::super::codegen::Codegen;
#[cfg(target_arch = "x86_64")]
use super::super::values;
#[cfg(target_arch = "x86_64")]
use super::super::JitPhysicalType;
use super::super::{JitError, JitFilterOp, PipelineSpec};
#[cfg(target_arch = "x86_64")]
#[allow(unused_imports)]
use dynasmrt::{DynasmApi, DynasmLabelApi, DynamicLabel};

#[cfg(target_arch = "x86_64")]
pub fn emit(cg: &mut Codegen, spec: &PipelineSpec) -> Result<(), JitError> {
    // IS_NULL doesn't need dictionary access at all — just count null rows.
    if spec.filter_op == JitFilterOp::IsNull {
        return super::plain::emit(cg, spec);
    }

    let lbl_loop_nulls = cg.label();
    let lbl_loop_no_nulls = cg.label();
    let lbl_done = cg.label();
    let lbl_skip_n = cg.label();
    let lbl_skip_nn = cg.label();
    let lbl_null_row = cg.label();
    let lbl_set_n = cg.label();
    let lbl_after_n = cg.label();
    let lbl_set_nn = cg.label();
    let lbl_after_nn = cg.label();

    cg.check_bitmap_null(lbl_loop_no_nulls);

    // ---- with-nulls path ----
    cg.bind(lbl_loop_nulls);
    cg.loop_check(lbl_done);
    cg.skip_if_null(lbl_null_row);

    emit_dict_body(cg, spec, lbl_skip_n, lbl_set_n, lbl_after_n)?;

    cg.bind(lbl_skip_n);
    cg.bind(lbl_null_row);
    cg.loop_inc(lbl_loop_nulls);

    // ---- no-nulls path ----
    cg.bind(lbl_loop_no_nulls);
    cg.loop_check(lbl_done);

    emit_dict_body(cg, spec, lbl_skip_nn, lbl_set_nn, lbl_after_nn)?;

    cg.bind(lbl_skip_nn);
    cg.loop_inc(lbl_loop_no_nulls);

    cg.bind(lbl_done);
    Ok(())
}

/// Load u32 index, check match_bitmap, load typed value from dict, output.
#[cfg(target_arch = "x86_64")]
fn emit_dict_body(
    cg: &mut Codegen,
    spec: &PipelineSpec,
    skip: DynamicLabel,
    set: DynamicLabel,
    after: DynamicLabel,
) -> Result<(), JitError> {
    // Step 1: load u32 index at indices[counter]
    //   AArch64: x0 = indices base, x19 = counter → index in w9
    //   x86_64:  rdi = indices base, rbx = counter → index in eax
    emit_load_index(cg);

    // Step 2: check match_bitmap[index]
    //   AArch64: x3 = match_bitmap ptr → ldrb from [x3 + x9]
    //   x86_64:  rcx = match_bitmap ptr → load byte from [rcx + rax]
    emit_check_match_bitmap(cg, skip);

    // Step 3: load typed value from dict_values[index]
    //   AArch64: x4 = dict_values ptr, index still in w9/x9
    //   x86_64:  r8 = dict_values ptr, index still in eax/rax
    emit_load_dict_value(cg, spec.physical_type)?;

    // Step 4: output (aggregate or materialize)
    values::emit_output(cg, spec, set, after)?;

    Ok(())
}

/// Load u32 dictionary index from `indices[counter]` into w9 (aarch64) / eax (x86_64).
#[cfg(target_arch = "x86_64")]
fn emit_load_index(cg: &mut Codegen) {
    #[cfg(target_arch = "aarch64")]
    {
        use super::super::codegen::dynasm;
        dynasm!(cg.ops ; .arch aarch64
            ; lsl x10, x19, 2          // offset = counter * 4
            ; ldr w9, [x0, x10]        // index = indices[counter] (u32, zero-extends to x9)
        );
    }
    #[cfg(target_arch = "x86_64")]
    {
        use super::super::codegen::dynasm;
        dynasm!(cg.ops ; .arch x64
            ; mov eax, DWORD [rdi + rbx * 4]   // index (u32, zero-extends to rax)
        );
    }
}

/// Check match_bitmap[index], skip if zero.
/// match_bitmap ptr is in x3 (aarch64) / rcx (x86_64).
#[cfg(target_arch = "x86_64")]
fn emit_check_match_bitmap(cg: &mut Codegen, skip: DynamicLabel) {
    #[cfg(target_arch = "aarch64")]
    {
        use super::super::codegen::dynasm;
        dynasm!(cg.ops ; .arch aarch64
            ; ldrb w10, [x3, x9]       // match_bitmap[index]
            ; cbz w10, =>skip
        );
    }
    #[cfg(target_arch = "x86_64")]
    {
        use super::super::codegen::dynasm;
        dynasm!(cg.ops ; .arch x64
            ; movzx r10d, BYTE [rcx + rax]
            ; test r10b, r10b
            ; jz =>skip
        );
    }
}

/// Load typed value from dict_values[index].
/// dict_values ptr is in x4 (aarch64) / r8 (x86_64).
/// Index is in x9/w9 (aarch64) / rax/eax (x86_64).
/// Result goes into the standard value register: x9 or d0 (aarch64), rax or xmm0 (x86_64).
#[cfg(target_arch = "x86_64")]
fn emit_load_dict_value(cg: &mut Codegen, pt: JitPhysicalType) -> Result<(), JitError> {
    #[cfg(target_arch = "aarch64")]
    {
        use super::super::codegen::dynasm;
        match pt {
            JitPhysicalType::Int8 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; ldrsb x9, [x4, x9]);  // sign-extend byte
            }
            JitPhysicalType::Int16 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 1
                    ; ldrsh x9, [x4, x10]);
            }
            JitPhysicalType::Int32 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 2
                    ; ldrsw x9, [x4, x10]);
            }
            JitPhysicalType::Int64 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 3
                    ; ldr x9, [x4, x10]);
            }
            JitPhysicalType::Float => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 2
                    ; ldr s0, [x4, x10]
                    ; fcvt d0, s0);
            }
            JitPhysicalType::Double => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 3
                    ; ldr d0, [x4, x10]);
            }
            JitPhysicalType::Int128 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x10, x9, 4
                    ; add x10, x4, x10
                    ; ldp x9, x10, [x10]);
            }
            JitPhysicalType::Int256 => {
                dynasm!(cg.ops ; .arch aarch64
                    ; lsl x13, x9, 5
                    ; add x13, x4, x13
                    ; ldp x9, x10, [x13]
                    ; ldp x11, x12, [x13, 16]);
            }
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        use super::super::codegen::dynasm;
        match pt {
            JitPhysicalType::Int8 => {
                dynasm!(cg.ops ; .arch x64
                    ; movsx rax, BYTE [r8 + rax]);
            }
            JitPhysicalType::Int16 => {
                dynasm!(cg.ops ; .arch x64
                    ; movsx rax, WORD [r8 + rax * 2]);
            }
            JitPhysicalType::Int32 => {
                dynasm!(cg.ops ; .arch x64
                    ; movsxd rax, DWORD [r8 + rax * 4]);
            }
            JitPhysicalType::Int64 => {
                dynasm!(cg.ops ; .arch x64
                    ; mov rax, QWORD [r8 + rax * 8]);
            }
            JitPhysicalType::Float => {
                dynasm!(cg.ops ; .arch x64
                    ; movss xmm0, [r8 + rax * 4]
                    ; cvtss2sd xmm0, xmm0);
            }
            JitPhysicalType::Double => {
                dynasm!(cg.ops ; .arch x64
                    ; movsd xmm0, [r8 + rax * 8]);
            }
            JitPhysicalType::Int128 => {
                dynasm!(cg.ops ; .arch x64
                    ; shl rax, 4
                    ; add rax, r8
                    ; mov rdx, QWORD [rax + 8]
                    ; mov rax, QWORD [rax]);
            }
            JitPhysicalType::Int256 => {
                dynasm!(cg.ops ; .arch x64
                    ; shl rax, 5
                    ; add rax, r8
                    ; mov r11, QWORD [rax + 24]
                    ; mov r10, QWORD [rax + 16]
                    ; mov rdx, QWORD [rax + 8]
                    ; mov rax, QWORD [rax]);
            }
        }
    }
    Ok(())
}

// =========================================================================
// IR-based dictionary-filtered encoding emitter
// =========================================================================

use super::super::ir::{CmpOp, IrType, ValRef};
use super::super::ir_builder::IrBuilder;
use super::super::values as ir_values;
use super::super::{JitOutputMode};

/// Emit IR for the RLE_DICTIONARY encoding loop.
///
/// Function argument re-mapping:
///   arg0 = flat u32 index array (pre-decoded RLE/bitpacked)
///   arg1 = null bitmap (1 byte/row, or NULL)
///   arg2 = row count
///   arg3 = match_bitmap pointer (1 byte per dict entry)
///   arg4 = dict_values pointer (typed array)
///   arg5 = result_ptr (PipelineResult for aggregate, buffer for materialize)
#[allow(dead_code)]
pub fn emit_ir(b: &mut IrBuilder, spec: &PipelineSpec) -> Result<(), JitError> {
    // IS_NULL: just delegate to plain — no dictionary access needed.
    if spec.filter_op == JitFilterOp::IsNull {
        return super::plain::emit_ir(b, spec);
    }

    let pt = spec.physical_type;
    let is_materialize = spec.output_mode == JitOutputMode::Materialize;

    // ---- entry block ----
    let entry = b.create_block(&[]);
    b.switch_to(entry);

    let indices = b.arg(0, IrType::Ptr);
    let bitmap = b.arg(1, IrType::Ptr);
    let row_count = b.arg(2, IrType::I64);
    let match_bm = b.arg(3, IrType::Ptr);   // filter_lo = match_bitmap
    let dict_values = b.arg(4, IrType::Ptr); // filter_hi = dict_values
    let result_ptr = b.arg(5, IrType::Ptr);

    let zero = b.iconst(0);

    if is_materialize {
        return emit_ir_dict_materialize(
            b, spec, indices, bitmap, row_count, match_bm, dict_values,
            result_ptr, zero,
        );
    }

    let init_acc = if pt.is_float() {
        b.fconst(0.0)
    } else {
        zero
    };
    let init_count = b.iconst(0);
    let init_has = b.iconst(0);

    emit_ir_dict_aggregate(
        b, spec, indices, bitmap, row_count, match_bm, dict_values,
        result_ptr, zero, init_acc, init_count, init_has,
    )
}

/// Emit IR for dict-filtered aggregate mode.
fn emit_ir_dict_aggregate(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    indices: ValRef,
    bitmap: ValRef,
    row_count: ValRef,
    match_bm: ValRef,
    dict_values: ValRef,
    result_ptr: ValRef,
    zero: ValRef,
    init_acc: ValRef,
    init_count: ValRef,
    init_has: ValRef,
) -> Result<(), JitError> {
    let pt = spec.physical_type;
    let acc_ty = pt.ir_acc_type();

    // Block params: (ctr: I64, acc: acc_ty, count: I64, has: I64)
    let loop_params: &[IrType] = &[IrType::I64, acc_ty, IrType::I64, IrType::I64];
    // Dict agg blocks have an extra param for the loaded dictionary index.
    let agg_params: &[IrType] = &[IrType::I64, acc_ty, IrType::I64, IrType::I64, IrType::I64];

    let wn_hdr = b.create_block(loop_params);
    let null_check = b.create_block(loop_params);
    let wn_dict_body = b.create_block(loop_params);
    let wn_dict_agg = b.create_block(agg_params);
    let wn_inc = b.create_block(loop_params);
    let nn_hdr = b.create_block(loop_params);
    let nn_dict_body = b.create_block(loop_params);
    let nn_dict_agg = b.create_block(agg_params);
    let nn_inc = b.create_block(loop_params);
    let epilogue = b.create_block(loop_params);

    // Entry: branch based on bitmap null.
    let bm_is_null = b.is_zero(bitmap);
    let init_args = &[zero, init_acc, init_count, init_has];
    b.cond_br(bm_is_null, nn_hdr, init_args, wn_hdr, init_args);

    // ---- with-nulls path ----

    b.switch_to(wn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, acc, count, has],
        null_check, &[ctr, acc, count, has],
    );

    // null_check: skip null rows via CondBr.
    b.switch_to(null_check);
    let (ctr, acc, count, has) = b.params_4();
    let bm_byte = b.load_byte(bitmap, ctr);
    let is_null = b.is_zero(bm_byte);
    b.cond_br(
        is_null,
        wn_inc, &[ctr, acc, count, has],
        wn_dict_body, &[ctr, acc, count, has],
    );

    // wn_dict_body: load u32 index, check match bitmap, CondBr to agg or inc.
    b.switch_to(wn_dict_body);
    let (ctr, acc, count, has) = b.params_4();
    let idx = b.load_u32(indices, ctr);
    let match_byte = b.load_byte(match_bm, idx);
    let is_match = b.is_nonzero(match_byte);
    b.cond_br(
        is_match,
        wn_dict_agg, &[ctr, acc, count, has, idx],
        wn_inc, &[ctr, acc, count, has],
    );

    // wn_dict_agg: load typed value from dict, aggregate.
    b.switch_to(wn_dict_agg);
    let (ctr, acc, count, has, idx) = b.params_5();
    let val = ir_values::emit_load_ir(b, pt, dict_values, idx);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, spec, val, acc, count, has, None, None);
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, new_acc, new_count, new_has]);

    // wn_inc: skip row, increment counter.
    b.switch_to(wn_inc);
    let (ctr, acc, count, has) = b.params_4();
    let next_ctr = b.inc(ctr);
    b.br(wn_hdr, &[next_ctr, acc, count, has]);

    // ---- no-nulls path ----

    b.switch_to(nn_hdr);
    let (ctr, acc, count, has) = b.params_4();
    let done = b.icmp(CmpOp::Ge, ctr, row_count);
    b.cond_br(
        done,
        epilogue, &[ctr, acc, count, has],
        nn_dict_body, &[ctr, acc, count, has],
    );

    // nn_dict_body: load index, check match bitmap.
    b.switch_to(nn_dict_body);
    let (ctr, acc, count, has) = b.params_4();
    let idx = b.load_u32(indices, ctr);
    let match_byte = b.load_byte(match_bm, idx);
    let is_match = b.is_nonzero(match_byte);
    b.cond_br(
        is_match,
        nn_dict_agg, &[ctr, acc, count, has, idx],
        nn_inc, &[ctr, acc, count, has],
    );

    // nn_dict_agg: load typed value, aggregate.
    b.switch_to(nn_dict_agg);
    let (ctr, acc, count, has, idx) = b.params_5();
    let val = ir_values::emit_load_ir(b, pt, dict_values, idx);
    let (new_acc, new_count, new_has) =
        ir_values::emit_output_ir(b, spec, val, acc, count, has, None, None);
    let next_ctr = b.inc(ctr);
    b.br(nn_hdr, &[next_ctr, new_acc, new_count, new_has]);

    // nn_inc: skip row.
    b.switch_to(nn_inc);
    let (ctr, acc, count, has) = b.params_4();
    let next_ctr = b.inc(ctr);
    b.br(nn_hdr, &[next_ctr, acc, count, has]);

    // ---- epilogue ----
    b.switch_to(epilogue);
    let (_ctr, acc, count, has) = b.params_4();
    super::plain::emit_epilogue_stores(b, spec, result_ptr, acc, count, has);

    Ok(())
}

/// Dict + materialize is not yet supported in the IR path.
fn emit_ir_dict_materialize(
    _b: &mut IrBuilder,
    _spec: &PipelineSpec,
    _indices: ValRef,
    _bitmap: ValRef,
    _row_count: ValRef,
    _match_bm: ValRef,
    _dict_values: ValRef,
    _result_ptr: ValRef,
    _zero: ValRef,
) -> Result<(), JitError> {
    Err(JitError::UnsupportedCombination(
        "dict + materialize not yet supported in IR".into(),
    ))
}
