//! AArch64 backend — compiles IrFunction to aarch64 machine code via dynasm-rs.
//!
//! Single-pass compilation from SSA IR to machine code with comparison-branch
//! fusion and flag caching.
//!
//! ## Register assignment (fixed, matching codegen.rs)
//!
//! ABI argument registers:
//!   arg(0) -> x0 (values_data)
//!   arg(1) -> x1 (null_bitmap)
//!   arg(2) -> x20 (row_count, copied from w2 in prologue, zero-extended)
//!   arg(3) -> x3 (filter_lo)
//!   arg(4) -> x4 (filter_hi)
//!   arg(5) -> x5 (result_ptr)
//!
//! Block param registers (role-based):
//!   param[0] (ctr)   -> x19
//!   param[1] (acc):
//!     I64            -> x21
//!     F64            -> d8
//!     I128           -> (x21=lo, x24=hi)
//!     I256           -> (x21=w0, x24=w1, x25=w2, x26=w3)
//!   param[2] (count) -> x22
//!   param[3] (has)   -> x23
//!   For IsNull (2 params): param[0]=x19, param[1]=x22
//!   For Materialize (3 params): param[0]=x19, param[1]=x24 (cursor), param[2]=x22
//!   Extra params beyond standard set -> x9 (first scratch)
//!
//! Scratch GPR pool: x6-x17 (round-robin, 12 registers)
//! Scratch FPR pool: d0, d1, d2, d3 (round-robin)

use dynasmrt::{dynasm, DynasmApi, DynasmLabelApi, DynamicLabel};

use super::super::ir::*;
use super::super::{
    CompiledPipeline, FusedPipelineFn, JitError, JitOutputMode, PipelineSpec,
};

// ---------------------------------------------------------------------------
// Location — where a value lives at runtime
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Loc {
    /// Single general-purpose register (I64, Bool, Ptr).
    Gpr(u8),
    /// Single floating-point register (F64, F32).
    Fpr(u8),
    /// Two GPRs for I128: (lo, hi).
    GprPair(u8, u8),
    /// Four GPRs for I256: (w0, w1, w2, w3).
    GprQuad(u8, u8, u8, u8),
    /// Immediate not yet materialized.
    Imm(i64),
}

// ---------------------------------------------------------------------------
// Scratch register pools
// ---------------------------------------------------------------------------

// Scratch GPR pool: x6-x17 (12 registers).
// x6-x8 are caller-saved and unused by the ABI arg assignments.
// x9-x15 are the standard caller-saved temporaries.
// x16-x17 (IP0/IP1) are caller-saved intra-procedure-call registers, safe
// to use since the JIT code makes no function calls or linker-veneer jumps.
//
// x27-x28 are callee-saved (saved/restored in prologue/epilogue) and reserved
// for the loaded data value (I128 lo/hi) to keep it safe from scratch clobber.
// For I256, only w0-w1 can be saved to x27-x28; w2-w3 remain in scratch regs
// but are protected by the interleaved filter-value load pattern.
const SCRATCH_GPRS: [u8; 12] = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17];
const SCRATCH_FPRS: [u8; 4] = [0, 1, 2, 3];

// ---------------------------------------------------------------------------
// Compiler
// ---------------------------------------------------------------------------

struct Compiler {
    ops: dynasmrt::aarch64::Assembler,
    start: dynasmrt::AssemblyOffset,
    /// Location of each ValRef.
    val_loc: Vec<Option<Loc>>,
    /// Remaining use count for each ValRef.
    use_count: Vec<u32>,
    /// Dynamic labels for each block.
    block_labels: Vec<DynamicLabel>,
    /// Round-robin index into SCRATCH_GPRS.
    scratch_gpr_idx: usize,
    /// Round-robin index into SCRATCH_FPRS.
    scratch_fpr_idx: usize,
    /// The ValRef whose comparison result currently lives in the condition flags.
    current_flags: Option<ValRef>,
    /// Whether the function uses floating-point callee-saved registers.
    uses_fp: bool,
    /// Counter for BitcastToF64 assignments (first -> d9, second -> d10).
    bitcast_count: u8,
    /// Pipeline specification.
    spec: PipelineSpec,
    /// Multi-column mode: BitcastToF64 always uses scratch FPRs instead of
    /// callee-saved d9/d10, since filter constants are block-local.
    multi_mode: bool,
    /// Page kernel mode: materialize blocks have (row_ctr, val_ctr, cursors..., count).
    page_kernel_mode: bool,
    /// Number of materialize columns (used for register assignment in page kernel mode).
    n_mat_columns: usize,
}

#[allow(dead_code)]
impl Compiler {
    fn new(ir: &IrFunction, spec: &PipelineSpec) -> Result<Self, JitError> {
        let ops = dynasmrt::aarch64::Assembler::new()
            .map_err(|e| JitError::AssemblerError(format!("{e}")))?;
        let n_vals = ir.val_types.len();
        let n_blocks = ir.blocks.len();

        // Determine if FP callee-saved registers are needed.
        let uses_fp = spec.physical_type.is_float();

        let mut c = Compiler {
            start: dynasmrt::AssemblyOffset(0),
            ops,
            val_loc: vec![None; n_vals],
            use_count: vec![0; n_vals],
            block_labels: Vec::with_capacity(n_blocks),
            scratch_gpr_idx: 0,
            scratch_fpr_idx: 0,
            current_flags: None,
            uses_fp,
            bitcast_count: 0,
            spec: *spec,
            multi_mode: false,
            page_kernel_mode: false,
            n_mat_columns: 0,
        };
        c.start = c.ops.offset();

        for _ in 0..n_blocks {
            c.block_labels.push(c.ops.new_dynamic_label());
        }

        Ok(c)
    }

    // -----------------------------------------------------------------------
    // Use counting
    // -----------------------------------------------------------------------

    fn count_uses(&mut self, ir: &IrFunction) {
        for block in &ir.blocks {
            for (_, inst) in &block.insts {
                match inst {
                    Inst::Arg(_) | Inst::IConst(_) | Inst::FConst(_) => {}
                    Inst::Load { base, index, .. } => {
                        self.use_count[base.0 as usize] += 1;
                        self.use_count[index.0 as usize] += 1;
                    }
                    Inst::LoadByte { base, index } | Inst::LoadU32 { base, index } => {
                        self.use_count[base.0 as usize] += 1;
                        self.use_count[index.0 as usize] += 1;
                    }
                    Inst::Store { ptr, val, .. } => {
                        self.use_count[ptr.0 as usize] += 1;
                        self.use_count[val.0 as usize] += 1;
                    }
                    Inst::StoreField { base, val, .. } => {
                        self.use_count[base.0 as usize] += 1;
                        self.use_count[val.0 as usize] += 1;
                    }
                    Inst::LoadField { base, .. } => {
                        self.use_count[base.0 as usize] += 1;
                    }
                    Inst::Add(a, b)
                    | Inst::Add128(a, b)
                    | Inst::Add256(a, b)
                    | Inst::AddF(a, b)
                    | Inst::ICmp(_, a, b)
                    | Inst::FCmp(_, a, b)
                    | Inst::ICmp128(_, a, b)
                    | Inst::ICmp256(_, a, b)
                    | Inst::And(a, b) => {
                        self.use_count[a.0 as usize] += 1;
                        self.use_count[b.0 as usize] += 1;
                    }
                    Inst::Inc(a)
                    | Inst::Shl(a, _)
                    | Inst::BitcastToF64(a)
                    | Inst::CvtF32F64(a)
                    | Inst::CvtF64F32(a)
                    | Inst::IsZero(a)
                    | Inst::IsNonZero(a)
                    | Inst::IsNaN(a)
                    | Inst::Not(a) => {
                        self.use_count[a.0 as usize] += 1;
                    }
                    Inst::Br { args, .. } => {
                        for a in args {
                            self.use_count[a.0 as usize] += 1;
                        }
                    }
                    Inst::CondBr {
                        cond,
                        then_args,
                        else_args,
                        ..
                    } => {
                        self.use_count[cond.0 as usize] += 1;
                        for a in then_args {
                            self.use_count[a.0 as usize] += 1;
                        }
                        for a in else_args {
                            self.use_count[a.0 as usize] += 1;
                        }
                    }
                    Inst::Ret(v) => {
                        self.use_count[v.0 as usize] += 1;
                    }
                    Inst::Select {
                        cond,
                        if_true,
                        if_false,
                    } => {
                        self.use_count[cond.0 as usize] += 1;
                        self.use_count[if_true.0 as usize] += 1;
                        self.use_count[if_false.0 as usize] += 1;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Register pre-assignment
    // -----------------------------------------------------------------------

    fn pre_assign_registers(&mut self, ir: &IrFunction) {
        // Walk entry block instructions for Arg() values.
        for block in &ir.blocks {
            for (val, inst) in &block.insts {
                if let Inst::Arg(n) = inst {
                    let loc = match n {
                        0 => Loc::Gpr(0),  // x0 = values_data
                        1 => Loc::Gpr(1),  // x1 = null_bitmap
                        2 => Loc::Gpr(20), // x20 = row_count (copied from w2)
                        3 => Loc::Gpr(3),  // x3 = filter_lo
                        4 => Loc::Gpr(4),  // x4 = filter_hi
                        5 => Loc::Gpr(5),  // x5 = result_ptr
                        _ => panic!("unexpected Arg index {n}"),
                    };
                    self.val_loc[val.0 as usize] = Some(loc);
                }
            }
        }

        // Assign block parameters based on role.
        for (block_idx, block) in ir.blocks.iter().enumerate() {
            if block_idx == 0 {
                // Entry block has no params (it's the setup block).
                continue;
            }
            let n_params = block.params.len();
            self.assign_block_params(block, n_params);
        }
    }

    fn assign_block_params(&mut self, block: &Block, n_params: usize) {
        let is_materialize = self.spec.output_mode == JitOutputMode::Materialize;
        let is_null_filter =
            self.spec.filter_op == super::super::JitFilterOp::IsNull;

        for (i, (val, ty)) in block.params.iter().enumerate() {
            let loc = if is_null_filter && n_params == 2 {
                // IsNull: param[0]=ctr(x19), param[1]=count(x22)
                match i {
                    0 => Loc::Gpr(19),
                    1 => Loc::Gpr(22),
                    _ => Loc::Gpr(SCRATCH_GPRS[0]),
                }
            } else if is_materialize && n_params == 3 {
                // Materialize: param[0]=ctr(x19), param[1]=cursor(x24), param[2]=count(x22)
                match i {
                    0 => Loc::Gpr(19),
                    1 => Loc::Gpr(24), // cursor
                    2 => Loc::Gpr(22),
                    _ => Loc::Gpr(SCRATCH_GPRS[0]),
                }
            } else if self.page_kernel_mode && n_params == 5 {
                // Page-kernel aggregate with bitmap:
                // param[0]=row_ctr, param[1]=val_ctr, param[2]=acc, param[3]=count, param[4]=has
                match i {
                    0 => Loc::Gpr(19), // row_ctr
                    1 => Loc::Gpr(23), // val_ctr
                    2 => {
                        // acc
                        match ty {
                            IrType::F64 => Loc::Fpr(8),
                            _ => Loc::Gpr(21),
                        }
                    }
                    3 => Loc::Gpr(22), // count
                    4 => Loc::Gpr(24), // has
                    _ => Loc::Gpr(SCRATCH_GPRS[0]),
                }
            } else {
                // Standard aggregate: param[0]=ctr, param[1]=acc, param[2]=count, param[3]=has
                match i {
                    0 => Loc::Gpr(19), // ctr
                    1 => {
                        // acc
                        match ty {
                            IrType::F64 => Loc::Fpr(8),
                            IrType::I128 => Loc::GprPair(21, 24),
                            IrType::I256 => Loc::GprQuad(21, 24, 25, 26),
                            _ => Loc::Gpr(21),
                        }
                    }
                    2 => Loc::Gpr(22), // count
                    3 => Loc::Gpr(23), // has
                    _ => {
                        // Extra params (e.g. dict_filter agg block's idx param)
                        Loc::Gpr(SCRATCH_GPRS[0])
                    }
                }
            };
            self.val_loc[val.0 as usize] = Some(loc);
        }
    }

    // -----------------------------------------------------------------------
    // Scratch register allocation (round-robin)
    // -----------------------------------------------------------------------

    fn alloc_scratch_gpr(&mut self) -> u8 {
        let r = SCRATCH_GPRS[self.scratch_gpr_idx % SCRATCH_GPRS.len()];
        self.scratch_gpr_idx += 1;
        r
    }

    fn alloc_scratch_fpr(&mut self) -> u8 {
        let r = SCRATCH_FPRS[self.scratch_fpr_idx % SCRATCH_FPRS.len()];
        self.scratch_fpr_idx += 1;
        r
    }

    fn alloc_for_type(&mut self, ty: IrType) -> Loc {
        match ty {
            IrType::F64 | IrType::F32 => Loc::Fpr(self.alloc_scratch_fpr()),
            IrType::I128 => {
                let lo = self.alloc_scratch_gpr();
                let hi = self.alloc_scratch_gpr();
                Loc::GprPair(lo, hi)
            }
            IrType::I256 => {
                let w0 = self.alloc_scratch_gpr();
                let w1 = self.alloc_scratch_gpr();
                let w2 = self.alloc_scratch_gpr();
                let w3 = self.alloc_scratch_gpr();
                Loc::GprQuad(w0, w1, w2, w3)
            }
            _ => Loc::Gpr(self.alloc_scratch_gpr()),
        }
    }

    // -----------------------------------------------------------------------
    // Location accessors
    // -----------------------------------------------------------------------

    fn loc(&self, v: ValRef) -> Loc {
        self.val_loc[v.0 as usize].expect("value has no location assigned")
    }

    fn gpr(&self, v: ValRef) -> u32 {
        match self.loc(v) {
            Loc::Gpr(r) => r as u32,
            Loc::Imm(0) => 31, // xzr
            other => panic!("expected Gpr for v{}, got {:?}", v.0, other),
        }
    }

    fn fpr(&self, v: ValRef) -> u32 {
        match self.loc(v) {
            Loc::Fpr(r) => r as u32,
            other => panic!("expected Fpr for v{}, got {:?}", v.0, other),
        }
    }

    fn set_loc(&mut self, v: ValRef, loc: Loc) {
        self.val_loc[v.0 as usize] = Some(loc);
    }

    fn use_val(&mut self, v: ValRef) {
        let c = &mut self.use_count[v.0 as usize];
        if *c > 0 {
            *c -= 1;
        }
    }

    fn is_single_use(&self, v: ValRef) -> bool {
        self.use_count[v.0 as usize] == 1
    }

    fn invalidate_flags(&mut self) {
        self.current_flags = None;
    }

    // -----------------------------------------------------------------------
    // Materialize an immediate into a register if needed
    // -----------------------------------------------------------------------

    fn materialize_if_imm(&mut self, v: ValRef) -> Loc {
        match self.loc(v) {
            Loc::Imm(val) => {
                if val == 0 {
                    // Use xzr — but for some operand positions we need a real register.
                    // Keep as Imm(0) and let the caller handle xzr.
                    Loc::Imm(0)
                } else {
                    let r = self.alloc_scratch_gpr();
                    self.emit_mov_imm(r, val);
                    let loc = Loc::Gpr(r);
                    self.set_loc(v, loc);
                    loc
                }
            }
            loc => loc,
        }
    }

    /// Force-materialize: always puts into a GPR (even for 0).
    fn materialize_to_gpr(&mut self, v: ValRef) -> u32 {
        match self.loc(v) {
            Loc::Gpr(r) => r as u32,
            Loc::Imm(val) => {
                if val == 0 {
                    // For zero, we can use wzr/xzr in many places,
                    // but if caller really needs a GPR, allocate one.
                    let r = self.alloc_scratch_gpr();
                    dynasm!(self.ops ; .arch aarch64 ; mov X(r as u32), xzr);
                    self.set_loc(v, Loc::Gpr(r));
                    r as u32
                } else {
                    let r = self.alloc_scratch_gpr();
                    self.emit_mov_imm(r, val);
                    self.set_loc(v, Loc::Gpr(r));
                    r as u32
                }
            }
            other => panic!("expected Gpr or Imm for v{}, got {:?}", v.0, other),
        }
    }

    /// Emit `ADD Xd, Xn, #imm12` (64-bit add-immediate, no shift).
    fn emit_add_imm(&mut self, rd: u8, rn: u8, imm12: u32) {
        debug_assert!(imm12 < 4096);
        // Encoding: sf=1, op=0, S=0 → 0b1001_0001_00 = 0x910
        // ADD Xd, Xn, #imm12 = 0x91000000 | (imm12 << 10) | (Rn << 5) | Rd
        let enc: u32 = 0x9100_0000 | (imm12 << 10) | ((rn as u32) << 5) | (rd as u32);
        dynasm!(self.ops ; .arch aarch64 ; .bytes enc.to_le_bytes());
    }

    fn emit_mov_imm(&mut self, r: u8, val: i64) {
        let rd = r as u32;
        // For values that fit in a movz/movn, use simple form.
        if val >= 0 && val <= 0xFFFF {
            dynasm!(self.ops ; .arch aarch64 ; movz X(rd), val as u32);
        } else if val < 0 && ((!val) as u64) <= 0xFFFF {
            dynasm!(self.ops ; .arch aarch64 ; movn X(rd), (!val) as u32);
        } else {
            // Use a sequence of movz/movk for wider immediates.
            // Variable names avoid w0-w3 which dynasm parses as registers.
            let uval = val as u64;
            let hw0 = (uval & 0xFFFF) as u32;
            let hw1 = ((uval >> 16) & 0xFFFF) as u32;
            let hw2 = ((uval >> 32) & 0xFFFF) as u32;
            let hw3 = ((uval >> 48) & 0xFFFF) as u32;

            // Check if all upper halfwords are 0xFFFF (negative small value).
            if hw1 == 0xFFFF && hw2 == 0xFFFF && hw3 == 0xFFFF {
                dynasm!(self.ops ; .arch aarch64 ; movn X(rd), (!val) as u32);
            } else {
                dynasm!(self.ops ; .arch aarch64 ; movz X(rd), hw0);
                if hw1 != 0 {
                    dynasm!(self.ops ; .arch aarch64 ; movk X(rd), hw1, LSL 16);
                }
                if hw2 != 0 {
                    dynasm!(self.ops ; .arch aarch64 ; movk X(rd), hw2, LSL 32);
                }
                if hw3 != 0 {
                    dynasm!(self.ops ; .arch aarch64 ; movk X(rd), hw3, LSL 48);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Prologue / Epilogue
    // -----------------------------------------------------------------------

    fn emit_prologue(&mut self) {
        // Save callee-saved registers.
        dynasm!(self.ops ; .arch aarch64
            ; stp x19, x20, [sp, -16]!
            ; stp x21, x22, [sp, -16]!
            ; stp x23, x24, [sp, -16]!
            ; stp x25, x26, [sp, -16]!
            ; stp x27, x28, [sp, -16]!
        );
        if self.uses_fp {
            dynasm!(self.ops ; .arch aarch64
                ; stp d8, d9, [sp, -16]!
                ; stp d10, d11, [sp, -16]!
            );
        }
        // Initialize callee-saved state.
        dynasm!(self.ops ; .arch aarch64
            ; mov w19, wzr
            ; mov w20, w2
            ; mov x21, xzr
            ; mov x22, xzr
            ; mov x23, xzr
            ; mov x24, xzr
            ; mov x25, xzr
            ; mov x26, xzr
        );
        if self.uses_fp {
            dynasm!(self.ops ; .arch aarch64 ; movi d8, 0);
        }
    }

    fn emit_epilogue(&mut self) {
        dynasm!(self.ops ; .arch aarch64 ; mov w0, wzr);
        if self.uses_fp {
            dynasm!(self.ops ; .arch aarch64
                ; ldp d10, d11, [sp], 16
                ; ldp d8, d9, [sp], 16
            );
        }
        dynasm!(self.ops ; .arch aarch64
            ; ldp x27, x28, [sp], 16
            ; ldp x25, x26, [sp], 16
            ; ldp x23, x24, [sp], 16
            ; ldp x21, x22, [sp], 16
            ; ldp x19, x20, [sp], 16
            ; ret
        );
    }

    // -----------------------------------------------------------------------
    // Block + instruction emission
    // -----------------------------------------------------------------------

    fn emit_block(&mut self, block_idx: usize, block: &Block, ir: &IrFunction) {
        // Bind block label.
        let label = self.block_labels[block_idx];
        dynasm!(self.ops ; .arch aarch64 ; =>label);

        // Reset scratch indices at block start for predictable allocation.
        self.scratch_gpr_idx = 0;
        self.scratch_fpr_idx = 0;

        for (val, inst) in &block.insts {
            self.emit_inst(*val, inst, block_idx, ir);
        }
    }

    fn emit_inst(
        &mut self,
        val: ValRef,
        inst: &Inst,
        block_idx: usize,
        ir: &IrFunction,
    ) {
        match inst {
            // -- Constants & arguments --
            Inst::Arg(_) => {
                // Already pre-assigned in pre_assign_registers.
            }
            Inst::IConst(v) => {
                self.set_loc(val, Loc::Imm(*v));
            }
            Inst::FConst(v) => {
                // Load float constant from bits.
                let bits = v.to_bits() as i64;
                let tmp = self.alloc_scratch_gpr();
                self.emit_mov_imm(tmp, bits);
                let fpr = self.alloc_scratch_fpr();
                dynasm!(self.ops ; .arch aarch64
                    ; fmov D(fpr as u32), X(tmp as u32)
                );
                self.set_loc(val, Loc::Fpr(fpr));
            }

            // -- Bitcast --
            Inst::BitcastToF64(src) => {
                self.use_val(*src);
                let src_r = self.gpr(*src);
                // In single-column mode the first two bitcasts go to callee-
                // saved d9/d10 so the filter constants persist across blocks.
                // In multi_mode filter constants are block-local, so always
                // use scratch FPRs.
                let dst_fpr = if self.multi_mode {
                    self.alloc_scratch_fpr()
                } else {
                    match self.bitcast_count {
                        0 => 9u8,
                        1 => 10u8,
                        _ => self.alloc_scratch_fpr(),
                    }
                };
                self.bitcast_count += 1;
                dynasm!(self.ops ; .arch aarch64
                    ; fmov D(dst_fpr as u32), X(src_r)
                );
                self.set_loc(val, Loc::Fpr(dst_fpr));
            }

            // -- Memory loads --
            Inst::Load {
                ty,
                base,
                index,
                stride_log2,
            } => {
                self.use_val(*base);
                self.use_val(*index);
                let base_r = self.gpr(*base);
                let idx_r = self.materialize_to_gpr(*index);

                match (ty, stride_log2) {
                    (IrType::I64, 0) => {
                        // ldrsb x_dst, [x_base, x_idx]
                        let dst = self.alloc_scratch_gpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; ldrsb X(dst as u32), [X(base_r), X(idx_r)]
                        );
                        self.set_loc(val, Loc::Gpr(dst));
                    }
                    (IrType::I64, 1) => {
                        let tmp = self.alloc_scratch_gpr();
                        let dst = self.alloc_scratch_gpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; lsl X(tmp as u32), X(idx_r), 1
                            ; ldrsh X(dst as u32), [X(base_r), X(tmp as u32)]
                        );
                        self.set_loc(val, Loc::Gpr(dst));
                    }
                    (IrType::I64, 2) => {
                        let tmp = self.alloc_scratch_gpr();
                        let dst = self.alloc_scratch_gpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; lsl X(tmp as u32), X(idx_r), 2
                            ; ldrsw X(dst as u32), [X(base_r), X(tmp as u32)]
                        );
                        self.set_loc(val, Loc::Gpr(dst));
                    }
                    (IrType::I64, 3) => {
                        let tmp = self.alloc_scratch_gpr();
                        let dst = self.alloc_scratch_gpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; lsl X(tmp as u32), X(idx_r), 3
                            ; ldr X(dst as u32), [X(base_r), X(tmp as u32)]
                        );
                        self.set_loc(val, Loc::Gpr(dst));
                    }
                    (IrType::F32, 2) => {
                        let tmp = self.alloc_scratch_gpr();
                        let dst = self.alloc_scratch_fpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; lsl X(tmp as u32), X(idx_r), 2
                            ; ldr S(dst as u32), [X(base_r), X(tmp as u32)]
                        );
                        // Note: F32 stays as F32 in the Fpr. CvtF32F64 will promote.
                        self.set_loc(val, Loc::Fpr(dst));
                    }
                    (IrType::F64, 3) => {
                        let tmp = self.alloc_scratch_gpr();
                        let dst = self.alloc_scratch_fpr();
                        dynasm!(self.ops ; .arch aarch64
                            ; lsl X(tmp as u32), X(idx_r), 3
                            ; ldr D(dst as u32), [X(base_r), X(tmp as u32)]
                        );
                        self.set_loc(val, Loc::Fpr(dst));
                    }
                    (IrType::I128, 4) => {
                        let tmp = self.alloc_scratch_gpr();
                        // Use callee-saved x27/x28 for the data value (loaded
                        // from arg0 = x0) to protect it from scratch clobber.
                        // Filter-value loads (from arg3/arg4) go to scratches.
                        let is_data_load = base_r == 0;
                        if is_data_load {
                            dynasm!(self.ops ; .arch aarch64
                                ; lsl X(tmp as u32), X(idx_r), 4
                                ; add X(tmp as u32), X(base_r), X(tmp as u32)
                                ; ldp x27, x28, [X(tmp as u32)]
                            );
                            self.set_loc(val, Loc::GprPair(27, 28));
                        } else {
                            let lo = self.alloc_scratch_gpr();
                            let hi = self.alloc_scratch_gpr();
                            dynasm!(self.ops ; .arch aarch64
                                ; lsl X(tmp as u32), X(idx_r), 4
                                ; add X(tmp as u32), X(base_r), X(tmp as u32)
                                ; ldp X(lo as u32), X(hi as u32), [X(tmp as u32)]
                            );
                            self.set_loc(val, Loc::GprPair(lo, hi));
                        }
                    }
                    (IrType::I256, 5) => {
                        let tmp = self.alloc_scratch_gpr();
                        let is_data_load = base_r == 0;
                        if is_data_load {
                            // Data value: w0/w1 → x27/x28 (safe), w2/w3 → scratch.
                            let w2 = self.alloc_scratch_gpr();
                            let w3 = self.alloc_scratch_gpr();
                            dynasm!(self.ops ; .arch aarch64
                                ; lsl X(tmp as u32), X(idx_r), 5
                                ; add X(tmp as u32), X(base_r), X(tmp as u32)
                                ; ldp x27, x28, [X(tmp as u32)]
                                ; ldp X(w2 as u32), X(w3 as u32), [X(tmp as u32), 16]
                            );
                            self.set_loc(val, Loc::GprQuad(27, 28, w2, w3));
                        } else {
                            let w0 = self.alloc_scratch_gpr();
                            let w1 = self.alloc_scratch_gpr();
                            let w2 = self.alloc_scratch_gpr();
                            let w3 = self.alloc_scratch_gpr();
                            dynasm!(self.ops ; .arch aarch64
                                ; lsl X(tmp as u32), X(idx_r), 5
                                ; add X(tmp as u32), X(base_r), X(tmp as u32)
                                ; ldp X(w0 as u32), X(w1 as u32), [X(tmp as u32)]
                                ; ldp X(w2 as u32), X(w3 as u32), [X(tmp as u32), 16]
                            );
                            self.set_loc(val, Loc::GprQuad(w0, w1, w2, w3));
                        }
                    }
                    _ => panic!(
                        "unsupported Load combination: ty={:?}, stride_log2={}",
                        ty, stride_log2
                    ),
                }
            }

            Inst::LoadByte { base, index } => {
                self.use_val(*base);
                self.use_val(*index);
                let base_r = self.gpr(*base);
                let idx_r = self.materialize_to_gpr(*index);
                let dst = self.alloc_scratch_gpr();
                dynasm!(self.ops ; .arch aarch64
                    ; ldrb W(dst as u32), [X(base_r), X(idx_r)]
                );
                self.set_loc(val, Loc::Gpr(dst));
            }

            Inst::LoadU32 { base, index } => {
                self.use_val(*base);
                self.use_val(*index);
                let base_r = self.gpr(*base);
                let idx_r = self.materialize_to_gpr(*index);
                let tmp = self.alloc_scratch_gpr();
                let dst = self.alloc_scratch_gpr();
                dynasm!(self.ops ; .arch aarch64
                    ; lsl X(tmp as u32), X(idx_r), 2
                    ; ldr W(dst as u32), [X(base_r), X(tmp as u32)]
                );
                self.set_loc(val, Loc::Gpr(dst));
            }

            // -- Memory stores --
            Inst::Store {
                ptr,
                val: store_val,
                width_log2,
            } => {
                self.use_val(*ptr);
                self.use_val(*store_val);
                let ptr_r = self.materialize_to_gpr(*ptr);
                let val_ty = ir.val_types[store_val.0 as usize];

                match (width_log2, val_ty) {
                    (0, _) => {
                        let vr = self.materialize_to_gpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; strb W(vr), [X(ptr_r)]
                        );
                    }
                    (1, _) => {
                        let vr = self.materialize_to_gpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; strh W(vr), [X(ptr_r)]
                        );
                    }
                    (2, IrType::F64 | IrType::F32) => {
                        let vr = self.fpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str S(vr), [X(ptr_r)]
                        );
                    }
                    (2, _) => {
                        let vr = self.materialize_to_gpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str W(vr), [X(ptr_r)]
                        );
                    }
                    (3, IrType::F64 | IrType::F32) => {
                        let vr = self.fpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str D(vr), [X(ptr_r)]
                        );
                    }
                    (3, _) => {
                        let vr = self.materialize_to_gpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str X(vr), [X(ptr_r)]
                        );
                    }
                    (4, _) => {
                        // I128
                        match self.loc(*store_val) {
                            Loc::GprPair(lo, hi) => {
                                dynasm!(self.ops ; .arch aarch64
                                    ; stp X(lo as u32), X(hi as u32), [X(ptr_r)]
                                );
                            }
                            _ => panic!("Store width 4 requires GprPair"),
                        }
                    }
                    (5, _) => {
                        // I256
                        match self.loc(*store_val) {
                            Loc::GprQuad(w0, w1, w2, w3) => {
                                dynasm!(self.ops ; .arch aarch64
                                    ; stp X(w0 as u32), X(w1 as u32), [X(ptr_r)]
                                    ; stp X(w2 as u32), X(w3 as u32), [X(ptr_r), 16]
                                );
                            }
                            _ => panic!("Store width 5 requires GprQuad"),
                        }
                    }
                    _ => panic!("unsupported Store width_log2={}", width_log2),
                }
            }

            Inst::StoreField {
                base,
                offset,
                val: store_val,
            } => {
                self.use_val(*base);
                self.use_val(*store_val);
                let base_r = self.gpr(*base);
                let off = *offset as u32;
                let val_ty = ir.val_types[store_val.0 as usize];

                match val_ty {
                    IrType::F64 => {
                        let vr = self.fpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str D(vr), [X(base_r), off]
                        );
                    }
                    IrType::I128 => {
                        // PipelineResult layout: value_i64 at off, value_hi at
                        // off+24 (skipping value_f64 and has_value).  The only
                        // caller that stores I128 via StoreField is the
                        // epilogue writing to PipelineResult.
                        match self.loc(*store_val) {
                            Loc::GprPair(lo, hi) => {
                                dynasm!(self.ops ; .arch aarch64
                                    ; str X(lo as u32), [X(base_r), off]
                                    ; str X(hi as u32), [X(base_r), off + 24]
                                );
                            }
                            _ => panic!("StoreField I128 requires GprPair"),
                        }
                    }
                    IrType::I256 => {
                        // PipelineResult layout: value_i64 at off, value_hi at
                        // off+24, value_w2 at off+32, value_w3 at off+40.
                        match self.loc(*store_val) {
                            Loc::GprQuad(w0, w1, w2, w3) => {
                                dynasm!(self.ops ; .arch aarch64
                                    ; str X(w0 as u32), [X(base_r), off]
                                    ; str X(w1 as u32), [X(base_r), off + 24]
                                    ; str X(w2 as u32), [X(base_r), off + 32]
                                    ; str X(w3 as u32), [X(base_r), off + 40]
                                );
                            }
                            _ => panic!("StoreField I256 requires GprQuad"),
                        }
                    }
                    _ => {
                        // I64, Ptr, Bool, I64
                        let vr = self.materialize_to_gpr(*store_val);
                        dynasm!(self.ops ; .arch aarch64
                            ; str X(vr), [X(base_r), off]
                        );
                    }
                }
            }

            // -- Context field load --
            Inst::LoadField { ty, base, offset } => {
                self.use_val(*base);
                let base_r = self.gpr(*base);
                let off = *offset as u32;

                if let Some(loc) = self.val_loc[val.0 as usize] {
                    // Pre-assigned location — load directly into it.
                    match loc {
                        Loc::Gpr(r) => {
                            dynasm!(self.ops ; .arch aarch64
                                ; ldr X(r as u32), [X(base_r), off]
                            );
                        }
                        Loc::Fpr(r) => {
                            dynasm!(self.ops ; .arch aarch64
                                ; ldr D(r as u32), [X(base_r), off]
                            );
                        }
                        _ => panic!("unexpected pre-assigned loc for LoadField: {:?}", loc),
                    }
                } else {
                    match ty {
                        IrType::F64 | IrType::F32 => {
                            let dst = self.alloc_scratch_fpr();
                            dynasm!(self.ops ; .arch aarch64
                                ; ldr D(dst as u32), [X(base_r), off]
                            );
                            self.set_loc(val, Loc::Fpr(dst));
                        }
                        _ => {
                            let dst = self.alloc_scratch_gpr();
                            dynasm!(self.ops ; .arch aarch64
                                ; ldr X(dst as u32), [X(base_r), off]
                            );
                            self.set_loc(val, Loc::Gpr(dst));
                        }
                    }
                }
            }

            // -- Integer arithmetic --
            Inst::Add(a, b) => {
                // Try immediate form: ADD Xd, Xn, #imm12 (0..4095).
                let loc_a = self.loc(*a);
                let loc_b = self.loc(*b);
                match (loc_a, loc_b) {
                    (_, Loc::Imm(imm)) if imm >= 0 && imm <= 4095 => {
                        let last_use_a = self.use_count[a.0 as usize] == 1;
                        self.use_val(*a);
                        self.use_val(*b);
                        let rn = self.materialize_to_gpr(*a) as u8;
                        let is_callee_saved = matches!(rn, 19..=28);
                        if last_use_a && is_callee_saved {
                            self.emit_add_imm(rn, rn, imm as u32);
                            self.set_loc(val, Loc::Gpr(rn));
                        } else {
                            let rd = self.alloc_scratch_gpr();
                            self.emit_add_imm(rd, rn, imm as u32);
                            self.set_loc(val, Loc::Gpr(rd));
                        }
                    }
                    (Loc::Imm(imm), _) if imm >= 0 && imm <= 4095 => {
                        let last_use_b = self.use_count[b.0 as usize] == 1;
                        self.use_val(*a);
                        self.use_val(*b);
                        let rn = self.materialize_to_gpr(*b) as u8;
                        let is_callee_saved = matches!(rn, 19..=28);
                        if last_use_b && is_callee_saved {
                            self.emit_add_imm(rn, rn, imm as u32);
                            self.set_loc(val, Loc::Gpr(rn));
                        } else {
                            let rd = self.alloc_scratch_gpr();
                            self.emit_add_imm(rd, rn, imm as u32);
                            self.set_loc(val, Loc::Gpr(rd));
                        }
                    }
                    _ => {
                        self.use_val(*a);
                        self.use_val(*b);
                        let ra = self.materialize_to_gpr(*a);
                        let rb = self.materialize_to_gpr(*b);
                        let dst = self.alloc_scratch_gpr() as u32;
                        dynasm!(self.ops ; .arch aarch64
                            ; add X(dst), X(ra), X(rb)
                        );
                        self.set_loc(val, Loc::Gpr(dst as u8));
                    }
                }
            }

            Inst::Inc(a) => {
                // If the source is a callee-saved register and this is its last
                // use, compute in-place to avoid a scratch→callee-saved mov later.
                let last_use = self.use_count[a.0 as usize] == 1;
                self.use_val(*a);
                let rn = self.materialize_to_gpr(*a) as u8;
                let is_callee_saved = matches!(rn, 19..=28);
                if last_use && is_callee_saved {
                    self.emit_add_imm(rn, rn, 1);
                    self.set_loc(val, Loc::Gpr(rn));
                } else {
                    let rd = self.alloc_scratch_gpr();
                    self.emit_add_imm(rd, rn, 1);
                    self.set_loc(val, Loc::Gpr(rd));
                }
            }

            Inst::Shl(a, imm) => {
                self.use_val(*a);
                let ra = self.materialize_to_gpr(*a);
                let dst = self.alloc_scratch_gpr() as u32;
                let shift = *imm as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; lsl X(dst), X(ra), shift
                );
                self.set_loc(val, Loc::Gpr(dst as u8));
            }

            Inst::Add128(a, b) => {
                self.use_val(*a);
                self.use_val(*b);
                let (a_lo, a_hi) = match self.loc(*a) {
                    Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
                    _ => panic!("Add128 requires GprPair"),
                };
                let (b_lo, b_hi) = match self.loc(*b) {
                    Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
                    _ => panic!("Add128 requires GprPair"),
                };
                let dst_lo = self.alloc_scratch_gpr() as u32;
                let dst_hi = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; adds X(dst_lo), X(a_lo), X(b_lo)
                    ; adc X(dst_hi), X(a_hi), X(b_hi)
                );
                self.invalidate_flags();
                self.set_loc(val, Loc::GprPair(dst_lo as u8, dst_hi as u8));
            }

            Inst::Add256(a, b) => {
                self.use_val(*a);
                self.use_val(*b);
                let (a0, a1, a2, a3) = match self.loc(*a) {
                    Loc::GprQuad(w0, w1, w2, w3) => {
                        (w0 as u32, w1 as u32, w2 as u32, w3 as u32)
                    }
                    _ => panic!("Add256 requires GprQuad"),
                };
                let (b0, b1, b2, b3) = match self.loc(*b) {
                    Loc::GprQuad(w0, w1, w2, w3) => {
                        (w0 as u32, w1 as u32, w2 as u32, w3 as u32)
                    }
                    _ => panic!("Add256 requires GprQuad"),
                };
                let d0 = self.alloc_scratch_gpr() as u32;
                let d1 = self.alloc_scratch_gpr() as u32;
                let d2 = self.alloc_scratch_gpr() as u32;
                let d3 = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; adds X(d0), X(a0), X(b0)
                    ; adcs X(d1), X(a1), X(b1)
                    ; adcs X(d2), X(a2), X(b2)
                    ; adc  X(d3), X(a3), X(b3)
                );
                self.invalidate_flags();
                self.set_loc(
                    val,
                    Loc::GprQuad(d0 as u8, d1 as u8, d2 as u8, d3 as u8),
                );
            }

            // -- Float arithmetic --
            Inst::AddF(a, b) => {
                self.use_val(*a);
                self.use_val(*b);
                let fa = self.fpr(*a);
                let fb = self.fpr(*b);
                let dst = self.alloc_scratch_fpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; fadd D(dst), D(fa), D(fb)
                );
                self.set_loc(val, Loc::Fpr(dst as u8));
            }

            Inst::CvtF32F64(a) => {
                self.use_val(*a);
                let fa = self.fpr(*a);
                let dst = self.alloc_scratch_fpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; fcvt D(dst), S(fa)
                );
                self.set_loc(val, Loc::Fpr(dst as u8));
            }

            Inst::CvtF64F32(a) => {
                self.use_val(*a);
                let fa = self.fpr(*a);
                let dst = self.alloc_scratch_fpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; fcvt S(dst), D(fa)
                );
                self.set_loc(val, Loc::Fpr(dst as u8));
            }

            // -- Comparison -> Bool --
            Inst::ICmp(op, a, b) => {
                self.emit_icmp(val, *op, *a, *b, ir);
            }
            Inst::FCmp(op, a, b) => {
                self.emit_fcmp(val, *op, *a, *b, ir);
            }
            Inst::ICmp128(op, a, b) => {
                self.emit_icmp128(val, *op, *a, *b, ir);
            }
            Inst::ICmp256(op, a, b) => {
                self.emit_icmp256(val, *op, *a, *b, ir);
            }
            Inst::IsZero(a) => {
                self.emit_is_zero(val, *a, ir);
            }
            Inst::IsNonZero(a) => {
                self.emit_is_nonzero(val, *a, ir);
            }
            Inst::IsNaN(a) => {
                self.emit_is_nan(val, *a, ir);
            }
            Inst::And(a, b) => {
                // Operands may be deferred comparisons (single-use with no
                // location). Force-materialize them before consuming uses.
                self.ensure_materialized(*a, ir);
                self.ensure_materialized(*b, ir);
                self.use_val(*a);
                self.use_val(*b);
                let ra = self.materialize_to_gpr(*a);
                let rb = self.materialize_to_gpr(*b);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; and X(dst), X(ra), X(rb)
                );
                self.set_loc(val, Loc::Gpr(dst as u8));
            }
            Inst::Not(a) => {
                // Operand may be a deferred comparison (single-use with no
                // location). Force-materialize it before consuming the use.
                self.ensure_materialized(*a, ir);
                self.use_val(*a);
                let ra = self.materialize_to_gpr(*a);
                let tmp = self.alloc_scratch_gpr() as u32;
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; movz X(tmp), 1
                    ; eor X(dst), X(ra), X(tmp)
                );
                self.set_loc(val, Loc::Gpr(dst as u8));
            }

            // -- Select (conditional move) --
            Inst::Select {
                cond,
                if_true,
                if_false,
            } => {
                self.emit_select(val, *cond, *if_true, *if_false, ir);
            }

            // -- Control flow --
            Inst::Br { target, args } => {
                self.emit_br_args(*target, args, ir);
                let next_block = block_idx + 1;
                if target.0 as usize != next_block {
                    let lbl = self.block_labels[target.0 as usize];
                    dynasm!(self.ops ; .arch aarch64 ; b =>lbl);
                }
            }

            Inst::CondBr {
                cond,
                then_block,
                then_args,
                else_block,
                else_args,
            } => {
                self.emit_cond_br(
                    val,
                    *cond,
                    *then_block,
                    then_args,
                    *else_block,
                    else_args,
                    block_idx,
                    ir,
                );
            }

            Inst::Ret(_v) => {
                self.emit_epilogue();
            }
        }
    }

    // -----------------------------------------------------------------------
    // Comparison emission (may defer if single-use for fusion)
    // -----------------------------------------------------------------------

    fn can_fuse_cmp(&self, v: ValRef, ir: &IrFunction) -> bool {
        if !self.is_single_use(v) {
            return false;
        }
        let idx = v.0 as usize;
        matches!(
            ir.val_types[idx],
            IrType::Bool
        ) && self.val_loc[idx].is_none()
    }

    fn emit_icmp(
        &mut self,
        val: ValRef,
        op: CmpOp,
        a: ValRef,
        b: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            // Defer — will be fused by CondBr or Select.
            // Don't consume operands yet.
            return;
        }
        // Materialize the boolean.
        self.use_val(a);
        self.use_val(b);
        let ra = self.materialize_to_gpr(a);
        let rb = self.materialize_to_gpr(b);
        let dst = self.alloc_scratch_gpr() as u32;
        dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), X(rb));
        self.emit_cset(dst, op);
        self.set_loc(val, Loc::Gpr(dst as u8));
    }

    fn emit_fcmp(
        &mut self,
        val: ValRef,
        op: CmpOp,
        a: ValRef,
        b: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        self.use_val(b);
        let fa = self.fpr(a);
        let fb = self.fpr(b);
        let dst = self.alloc_scratch_gpr() as u32;
        dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fb));
        self.emit_cset(dst, op);
        self.set_loc(val, Loc::Gpr(dst as u8));
    }

    fn emit_icmp128(
        &mut self,
        val: ValRef,
        op: CmpOp,
        a: ValRef,
        b: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        self.use_val(b);
        let dst = self.alloc_scratch_gpr();
        self.emit_icmp128_materialized(dst, op, a, b);
        self.set_loc(val, Loc::Gpr(dst));
    }

    /// Branchless 128-bit signed comparison.
    ///
    /// Uses `ccmp` for Eq/Ne and `subs+sbcs` for relational comparisons.
    /// No branches, no extra scratch registers, no risk of dst overlapping
    /// with an operand register.
    fn emit_icmp128_materialized(&mut self, dst: u8, op: CmpOp, a: ValRef, b: ValRef) {
        let (a_lo, a_hi) = match self.loc(a) {
            Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
            _ => panic!("ICmp128 requires GprPair"),
        };
        let (b_lo, b_hi) = match self.loc(b) {
            Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
            _ => panic!("ICmp128 requires GprPair"),
        };
        let d = dst as u32;

        match op {
            CmpOp::Eq => {
                // cmp hi, hi ; ccmp lo, lo, #0, eq → Z=1 iff both pairs match
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a_hi), X(b_hi)
                    ; ccmp X(a_lo), X(b_lo), 0, eq
                    ; cset X(d), eq
                );
            }
            CmpOp::Ne => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a_hi), X(b_hi)
                    ; ccmp X(a_lo), X(b_lo), 0, eq
                    ; cset X(d), ne
                );
            }
            CmpOp::Lt => {
                // subs+sbcs: signed 128-bit subtraction sets N,V correctly.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a_lo), X(b_lo)
                    ; sbcs xzr, X(a_hi), X(b_hi)
                    ; cset X(d), lt
                );
            }
            CmpOp::Ge => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a_lo), X(b_lo)
                    ; sbcs xzr, X(a_hi), X(b_hi)
                    ; cset X(d), ge
                );
            }
            CmpOp::Gt => {
                // a > b ↔ b < a: swap operands, use lt.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b_lo), X(a_lo)
                    ; sbcs xzr, X(b_hi), X(a_hi)
                    ; cset X(d), lt
                );
            }
            CmpOp::Le => {
                // a <= b ↔ b >= a: swap operands, use ge.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b_lo), X(a_lo)
                    ; sbcs xzr, X(b_hi), X(a_hi)
                    ; cset X(d), ge
                );
            }
        }
        self.invalidate_flags();
    }

    fn emit_icmp256(
        &mut self,
        val: ValRef,
        op: CmpOp,
        a: ValRef,
        b: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        self.use_val(b);
        let dst = self.alloc_scratch_gpr();
        self.emit_icmp256_materialized(dst, op, a, b);
        self.set_loc(val, Loc::Gpr(dst));
    }

    /// Branchless 256-bit signed comparison.
    ///
    /// Uses `ccmp` chains for Eq/Ne and `subs+sbcs` chains for relational.
    fn emit_icmp256_materialized(&mut self, dst: u8, op: CmpOp, a: ValRef, b: ValRef) {
        let (a0, a1, a2, a3) = match self.loc(a) {
            Loc::GprQuad(w0, w1, w2, w3) => (w0 as u32, w1 as u32, w2 as u32, w3 as u32),
            _ => panic!("ICmp256 requires GprQuad"),
        };
        let (b0, b1, b2, b3) = match self.loc(b) {
            Loc::GprQuad(w0, w1, w2, w3) => (w0 as u32, w1 as u32, w2 as u32, w3 as u32),
            _ => panic!("ICmp256 requires GprQuad"),
        };
        let d = dst as u32;

        match op {
            CmpOp::Eq => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a3), X(b3)
                    ; ccmp X(a2), X(b2), 0, eq
                    ; ccmp X(a1), X(b1), 0, eq
                    ; ccmp X(a0), X(b0), 0, eq
                    ; cset X(d), eq
                );
            }
            CmpOp::Ne => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a3), X(b3)
                    ; ccmp X(a2), X(b2), 0, eq
                    ; ccmp X(a1), X(b1), 0, eq
                    ; ccmp X(a0), X(b0), 0, eq
                    ; cset X(d), ne
                );
            }
            CmpOp::Lt => {
                // Signed 256-bit: subs+sbcs chain.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a0), X(b0)
                    ; sbcs xzr, X(a1), X(b1)
                    ; sbcs xzr, X(a2), X(b2)
                    ; sbcs xzr, X(a3), X(b3)
                    ; cset X(d), lt
                );
            }
            CmpOp::Ge => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a0), X(b0)
                    ; sbcs xzr, X(a1), X(b1)
                    ; sbcs xzr, X(a2), X(b2)
                    ; sbcs xzr, X(a3), X(b3)
                    ; cset X(d), ge
                );
            }
            CmpOp::Gt => {
                // a > b ↔ b < a: swap operands.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b0), X(a0)
                    ; sbcs xzr, X(b1), X(a1)
                    ; sbcs xzr, X(b2), X(a2)
                    ; sbcs xzr, X(b3), X(a3)
                    ; cset X(d), lt
                );
            }
            CmpOp::Le => {
                // a <= b ↔ b >= a: swap operands.
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b0), X(a0)
                    ; sbcs xzr, X(b1), X(a1)
                    ; sbcs xzr, X(b2), X(a2)
                    ; sbcs xzr, X(b3), X(a3)
                    ; cset X(d), ge
                );
            }
        }
        self.invalidate_flags();
    }

    fn emit_is_zero(
        &mut self,
        val: ValRef,
        a: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        let ra = self.materialize_to_gpr(a);
        let dst = self.alloc_scratch_gpr() as u32;
        dynasm!(self.ops ; .arch aarch64
            ; cmp X(ra), xzr
            ; cset X(dst), eq
        );
        self.set_loc(val, Loc::Gpr(dst as u8));
    }

    fn emit_is_nonzero(
        &mut self,
        val: ValRef,
        a: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        let ra = self.materialize_to_gpr(a);
        let dst = self.alloc_scratch_gpr() as u32;
        dynasm!(self.ops ; .arch aarch64
            ; cmp X(ra), xzr
            ; cset X(dst), ne
        );
        self.set_loc(val, Loc::Gpr(dst as u8));
    }

    fn emit_is_nan(
        &mut self,
        val: ValRef,
        a: ValRef,
        _ir: &IrFunction,
    ) {
        if self.is_single_use(val) {
            return;
        }
        self.use_val(a);
        let fa = self.fpr(a);
        let dst = self.alloc_scratch_gpr() as u32;
        dynasm!(self.ops ; .arch aarch64
            ; fcmp D(fa), D(fa)
            ; cset X(dst), vs
        );
        self.set_loc(val, Loc::Gpr(dst as u8));
    }

    fn emit_cset(&mut self, dst: u32, op: CmpOp) {
        match op {
            CmpOp::Eq => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), eq),
            CmpOp::Ne => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), ne),
            CmpOp::Lt => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), lt),
            CmpOp::Le => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), le),
            CmpOp::Gt => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), gt),
            CmpOp::Ge => dynasm!(self.ops ; .arch aarch64 ; cset X(dst), ge),
        }
    }

    // -----------------------------------------------------------------------
    // Select (conditional move) with fusion
    // -----------------------------------------------------------------------

    fn emit_select(
        &mut self,
        val: ValRef,
        cond: ValRef,
        if_true: ValRef,
        if_false: ValRef,
        ir: &IrFunction,
    ) {
        let val_ty = ir.val_types[val.0 as usize];

        // Try to fuse with the condition.
        let fused = self.try_fuse_select(val, cond, if_true, if_false, val_ty, ir);
        if fused {
            return;
        }

        // Not fused — materialize cond as a bool and use cmp+csel.
        self.ensure_materialized(cond, ir);
        self.use_val(cond);
        let rc = self.materialize_to_gpr(cond);
        dynasm!(self.ops ; .arch aarch64 ; cmp X(rc), xzr);

        self.use_val(if_true);
        self.use_val(if_false);
        self.emit_csel_for_type(val, if_true, if_false, val_ty, "ne");
    }

    fn try_fuse_select(
        &mut self,
        val: ValRef,
        cond: ValRef,
        if_true: ValRef,
        if_false: ValRef,
        val_ty: IrType,
        ir: &IrFunction,
    ) -> bool {
        // Check if cond is a single-use comparison that hasn't been emitted yet.
        if self.val_loc[cond.0 as usize].is_some() {
            return false;
        }
        if !self.is_single_use(cond) {
            return false;
        }

        // Find the instruction that defines cond.
        let cond_inst = self.find_inst(cond, ir);
        match cond_inst {
            Some(Inst::ICmp(op, a, b)) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let ra = self.materialize_to_gpr(a);
                let rb = self.materialize_to_gpr(b);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), X(rb));
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type_cmpop(val, if_true, if_false, val_ty, op);
                true
            }
            Some(Inst::FCmp(op, a, b)) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let fa = self.fpr(a);
                let fb = self.fpr(b);
                dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fb));
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type_cmpop(val, if_true, if_false, val_ty, op);
                true
            }
            Some(Inst::IsZero(a)) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), xzr);
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type(val, if_true, if_false, val_ty, "eq");
                true
            }
            Some(Inst::IsNonZero(a)) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), xzr);
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type(val, if_true, if_false, val_ty, "ne");
                true
            }
            Some(Inst::IsNaN(a)) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let fa = self.fpr(a);
                dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fa));
                self.use_val(if_true);
                self.use_val(if_false);
                // IsNaN is true when V flag set (vs).
                self.emit_csel_for_type(val, if_true, if_false, val_ty, "vs");
                true
            }
            Some(Inst::ICmp128(op, a, b)) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let cc = self.emit_icmp128_flags(op, a, b);
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type(val, if_true, if_false, val_ty, cc);
                true
            }
            Some(Inst::ICmp256(op, a, b)) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let cc = self.emit_icmp256_flags(op, a, b);
                self.use_val(if_true);
                self.use_val(if_false);
                self.emit_csel_for_type(val, if_true, if_false, val_ty, cc);
                true
            }
            _ => false,
        }
    }

    /// Emit a 128-bit comparison that sets condition flags (no scratch alloc).
    /// Returns the condition code string for use with csel/b.cc.
    fn emit_icmp128_flags(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> &'static str {
        let (a_lo, a_hi) = match self.loc(a) {
            Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
            _ => panic!("ICmp128 requires GprPair"),
        };
        let (b_lo, b_hi) = match self.loc(b) {
            Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
            _ => panic!("ICmp128 requires GprPair"),
        };
        match op {
            CmpOp::Eq => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a_hi), X(b_hi)
                    ; ccmp X(a_lo), X(b_lo), 0, eq
                );
                "eq"
            }
            CmpOp::Ne => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a_hi), X(b_hi)
                    ; ccmp X(a_lo), X(b_lo), 0, eq
                );
                "ne"
            }
            CmpOp::Lt => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a_lo), X(b_lo)
                    ; sbcs xzr, X(a_hi), X(b_hi)
                );
                "lt"
            }
            CmpOp::Ge => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a_lo), X(b_lo)
                    ; sbcs xzr, X(a_hi), X(b_hi)
                );
                "ge"
            }
            CmpOp::Gt => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b_lo), X(a_lo)
                    ; sbcs xzr, X(b_hi), X(a_hi)
                );
                "lt"
            }
            CmpOp::Le => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b_lo), X(a_lo)
                    ; sbcs xzr, X(b_hi), X(a_hi)
                );
                "ge"
            }
        }
    }

    /// Emit a 256-bit comparison that sets condition flags (no scratch alloc).
    fn emit_icmp256_flags(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> &'static str {
        let (a0, a1, a2, a3) = match self.loc(a) {
            Loc::GprQuad(w0, w1, w2, w3) => (w0 as u32, w1 as u32, w2 as u32, w3 as u32),
            _ => panic!("ICmp256 requires GprQuad"),
        };
        let (b0, b1, b2, b3) = match self.loc(b) {
            Loc::GprQuad(w0, w1, w2, w3) => (w0 as u32, w1 as u32, w2 as u32, w3 as u32),
            _ => panic!("ICmp256 requires GprQuad"),
        };
        match op {
            CmpOp::Eq => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a3), X(b3)
                    ; ccmp X(a2), X(b2), 0, eq
                    ; ccmp X(a1), X(b1), 0, eq
                    ; ccmp X(a0), X(b0), 0, eq
                );
                "eq"
            }
            CmpOp::Ne => {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(a3), X(b3)
                    ; ccmp X(a2), X(b2), 0, eq
                    ; ccmp X(a1), X(b1), 0, eq
                    ; ccmp X(a0), X(b0), 0, eq
                );
                "ne"
            }
            CmpOp::Lt => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a0), X(b0)
                    ; sbcs xzr, X(a1), X(b1)
                    ; sbcs xzr, X(a2), X(b2)
                    ; sbcs xzr, X(a3), X(b3)
                );
                "lt"
            }
            CmpOp::Ge => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(a0), X(b0)
                    ; sbcs xzr, X(a1), X(b1)
                    ; sbcs xzr, X(a2), X(b2)
                    ; sbcs xzr, X(a3), X(b3)
                );
                "ge"
            }
            CmpOp::Gt => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b0), X(a0)
                    ; sbcs xzr, X(b1), X(a1)
                    ; sbcs xzr, X(b2), X(a2)
                    ; sbcs xzr, X(b3), X(a3)
                );
                "lt"
            }
            CmpOp::Le => {
                dynasm!(self.ops ; .arch aarch64
                    ; subs xzr, X(b0), X(a0)
                    ; sbcs xzr, X(b1), X(a1)
                    ; sbcs xzr, X(b2), X(a2)
                    ; sbcs xzr, X(b3), X(a3)
                );
                "ge"
            }
        }
    }

    fn find_inst<'a>(&self, v: ValRef, ir: &'a IrFunction) -> Option<&'a Inst> {
        for block in &ir.blocks {
            for (vr, inst) in &block.insts {
                if *vr == v {
                    return Some(inst);
                }
            }
        }
        None
    }

    /// Force-materialize a deferred comparison into a GPR.
    ///
    /// When a comparison instruction (ICmp, FCmp, ICmp128, ICmp256, IsZero,
    /// IsNonZero, IsNaN) has a single use, the comparison emitters defer code
    /// generation — they return early without emitting code or setting a
    /// location, expecting the consumer (CondBr or Select) to fuse the
    /// comparison. But if the consumer is `And` or `Not`, fusion is not
    /// possible and the missing location causes a panic.
    ///
    /// This method checks whether `v` already has a location. If not, it looks
    /// up the defining instruction and emits the comparison + cset sequence,
    /// assigning a GPR location.
    fn ensure_materialized(&mut self, v: ValRef, ir: &IrFunction) {
        if self.val_loc[v.0 as usize].is_some() {
            return;
        }
        let inst = self
            .find_inst(v, ir)
            .unwrap_or_else(|| panic!("ensure_materialized: no instruction for v{}", v.0));
        match inst {
            Inst::ICmp(op, a, b) => {
                let (op, a, b) = (*op, *a, *b);
                self.use_val(a);
                self.use_val(b);
                let ra = self.materialize_to_gpr(a);
                let rb = self.materialize_to_gpr(b);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), X(rb));
                self.emit_cset(dst, op);
                self.set_loc(v, Loc::Gpr(dst as u8));
            }
            Inst::FCmp(op, a, b) => {
                let (op, a, b) = (*op, *a, *b);
                self.use_val(a);
                self.use_val(b);
                let fa = self.fpr(a);
                let fb = self.fpr(b);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fb));
                self.emit_cset(dst, op);
                self.set_loc(v, Loc::Gpr(dst as u8));
            }
            Inst::ICmp128(op, a, b) => {
                let (op, a, b) = (*op, *a, *b);
                self.use_val(a);
                self.use_val(b);
                let dst = self.alloc_scratch_gpr();
                self.emit_icmp128_materialized(dst, op, a, b);
                self.set_loc(v, Loc::Gpr(dst));
            }
            Inst::ICmp256(op, a, b) => {
                let (op, a, b) = (*op, *a, *b);
                self.use_val(a);
                self.use_val(b);
                let dst = self.alloc_scratch_gpr();
                self.emit_icmp256_materialized(dst, op, a, b);
                self.set_loc(v, Loc::Gpr(dst));
            }
            Inst::IsZero(a) => {
                let a = *a;
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(ra), xzr
                    ; cset X(dst), eq
                );
                self.set_loc(v, Loc::Gpr(dst as u8));
            }
            Inst::IsNonZero(a) => {
                let a = *a;
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; cmp X(ra), xzr
                    ; cset X(dst), ne
                );
                self.set_loc(v, Loc::Gpr(dst as u8));
            }
            Inst::IsNaN(a) => {
                let a = *a;
                self.use_val(a);
                let fa = self.fpr(a);
                let dst = self.alloc_scratch_gpr() as u32;
                dynasm!(self.ops ; .arch aarch64
                    ; fcmp D(fa), D(fa)
                    ; cset X(dst), vs
                );
                self.set_loc(v, Loc::Gpr(dst as u8));
            }
            _ => panic!(
                "ensure_materialized: unexpected instruction for v{}: {:?}",
                v.0, inst
            ),
        }
    }

    /// Emit csel for a CmpOp condition code.
    fn emit_csel_for_type_cmpop(
        &mut self,
        val: ValRef,
        if_true: ValRef,
        if_false: ValRef,
        val_ty: IrType,
        op: CmpOp,
    ) {
        match op {
            CmpOp::Eq => self.emit_csel_for_type(val, if_true, if_false, val_ty, "eq"),
            CmpOp::Ne => self.emit_csel_for_type(val, if_true, if_false, val_ty, "ne"),
            CmpOp::Lt => self.emit_csel_for_type(val, if_true, if_false, val_ty, "lt"),
            CmpOp::Le => self.emit_csel_for_type(val, if_true, if_false, val_ty, "le"),
            CmpOp::Gt => self.emit_csel_for_type(val, if_true, if_false, val_ty, "gt"),
            CmpOp::Ge => self.emit_csel_for_type(val, if_true, if_false, val_ty, "ge"),
        }
    }

    /// Emit csel with a string condition code.
    fn emit_csel_for_type(
        &mut self,
        val: ValRef,
        if_true: ValRef,
        if_false: ValRef,
        val_ty: IrType,
        cc: &str,
    ) {
        match val_ty {
            IrType::F64 | IrType::F32 => {
                let ft = self.fpr(if_true);
                let ff = self.fpr(if_false);
                let dst = self.alloc_scratch_fpr() as u32;
                match cc {
                    "eq" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), eq),
                    "ne" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), ne),
                    "lt" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), lt),
                    "le" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), le),
                    "gt" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), gt),
                    "ge" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), ge),
                    "vs" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), vs),
                    "vc" => dynasm!(self.ops ; .arch aarch64 ; fcsel D(dst), D(ft), D(ff), vc),
                    _ => panic!("unsupported condition code: {cc}"),
                }
                self.set_loc(val, Loc::Fpr(dst as u8));
            }
            IrType::I128 => {
                let (t_lo, t_hi) = match self.loc(if_true) {
                    Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
                    _ => panic!("Select I128 requires GprPair"),
                };
                let (f_lo, f_hi) = match self.loc(if_false) {
                    Loc::GprPair(lo, hi) => (lo as u32, hi as u32),
                    _ => panic!("Select I128 requires GprPair"),
                };
                let d_lo = self.alloc_scratch_gpr() as u32;
                let d_hi = self.alloc_scratch_gpr() as u32;
                self.emit_csel_gpr(d_lo, t_lo, f_lo, cc);
                self.emit_csel_gpr(d_hi, t_hi, f_hi, cc);
                self.set_loc(val, Loc::GprPair(d_lo as u8, d_hi as u8));
            }
            IrType::I256 => {
                let (t0, t1, t2, t3) = match self.loc(if_true) {
                    Loc::GprQuad(w0, w1, w2, w3) => {
                        (w0 as u32, w1 as u32, w2 as u32, w3 as u32)
                    }
                    _ => panic!("Select I256 requires GprQuad"),
                };
                let (f0, f1, f2, f3) = match self.loc(if_false) {
                    Loc::GprQuad(w0, w1, w2, w3) => {
                        (w0 as u32, w1 as u32, w2 as u32, w3 as u32)
                    }
                    _ => panic!("Select I256 requires GprQuad"),
                };
                let d0 = self.alloc_scratch_gpr() as u32;
                let d1 = self.alloc_scratch_gpr() as u32;
                let d2 = self.alloc_scratch_gpr() as u32;
                let d3 = self.alloc_scratch_gpr() as u32;
                self.emit_csel_gpr(d0, t0, f0, cc);
                self.emit_csel_gpr(d1, t1, f1, cc);
                self.emit_csel_gpr(d2, t2, f2, cc);
                self.emit_csel_gpr(d3, t3, f3, cc);
                self.set_loc(
                    val,
                    Loc::GprQuad(d0 as u8, d1 as u8, d2 as u8, d3 as u8),
                );
            }
            _ => {
                // I64, Bool, Ptr
                let rt = self.materialize_to_gpr(if_true);
                let rf = self.materialize_to_gpr(if_false);
                let dst = self.alloc_scratch_gpr() as u32;
                self.emit_csel_gpr(dst, rt, rf, cc);
                self.set_loc(val, Loc::Gpr(dst as u8));
            }
        }
    }

    fn emit_csel_gpr(&mut self, dst: u32, a: u32, b: u32, cc: &str) {
        match cc {
            "eq" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), eq),
            "ne" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), ne),
            "lt" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), lt),
            "le" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), le),
            "gt" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), gt),
            "ge" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), ge),
            "vs" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), vs),
            "vc" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), vc),
            "hi" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), hi),
            "hs" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), hs),
            "lo" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), lo),
            "ls" => dynasm!(self.ops ; .arch aarch64 ; csel X(dst), X(a), X(b), ls),
            _ => panic!("unsupported condition code for csel: {cc}"),
        }
    }

    // -----------------------------------------------------------------------
    // CondBr with fusion
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn emit_cond_br(
        &mut self,
        _val: ValRef,
        cond: ValRef,
        then_block: BlockRef,
        then_args: &[ValRef],
        else_block: BlockRef,
        else_args: &[ValRef],
        block_idx: usize,
        ir: &IrFunction,
    ) {
        let next_block = block_idx + 1;
        let then_lbl = self.block_labels[then_block.0 as usize];
        let else_lbl = self.block_labels[else_block.0 as usize];

        // We need to emit branch args for both targets.
        // The strategy: emit the "taken" branch target's args, branch conditionally,
        // then emit the "fall-through" target's args.
        //
        // Since branch args may include values produced by instructions in this block,
        // we need to be careful about register conflicts.
        //
        // For simplicity, we always:
        // 1. Emit the comparison
        // 2. Emit else_args (fall-through path args)
        // 3. Conditional branch to then_block
        // 4. Fall through to else_block (or unconditional branch if not next)
        //
        // Wait — that's wrong. We need then_args for the then path and else_args
        // for the else path. If then is not next and else is next, we:
        // 1. Emit comparison
        // 2. Emit then_args into then_block's params
        // 3. Branch cond -> then_block
        // 4. Emit else_args into else_block's params
        // 5. Fall through (or branch) to else_block
        //
        // But this is wrong if then_args and else_args both target the same params
        // — emitting then_args would clobber values needed for else_args.
        //
        // For our pipelines, a simpler approach works: the args are almost always
        // the same values or simple derived values that are already in scratch regs.
        // We'll use a label-based approach:
        //   1. Emit comparison
        //   2. If cond true, branch to a local then_stub
        //   3. Fall through: emit else_args, branch to else_block
        //   4. then_stub: emit then_args, branch to then_block
        //
        // But that adds extra branches. Let's use the observation that in our IR,
        // the else_block is typically the next block (fall-through), and for CondBr
        // the args for both branches often share the same slot assignments.
        //
        // Actually, let's look at how our IR typically works:
        // - CondBr at end of loop body: then=loop_top (back-edge), else=exit
        // - Loop top is NOT the next block in layout, exit IS the next block.
        //
        // So the typical pattern is: else_block = next block (fall-through).
        // We emit: compare, then emit else_args (into callee-saved regs),
        //          branch_if_true to then_stub,
        //          fall through to else_block.
        // then_stub: emit then_args, branch to then_block.
        //
        // However, if the args for both branches target the same registers and have
        // different source values, we need to be careful.
        //
        // For practical correctness, let's use a safe two-stub approach:

        // Step 1: emit the comparison.
        let fused = self.try_emit_cond_comparison(cond, ir);
        let cond_cc = if fused.is_some() {
            fused.unwrap()
        } else {
            // Not fused — use materialized bool.
            self.use_val(cond);
            let rc = self.materialize_to_gpr(cond);
            dynasm!(self.ops ; .arch aarch64 ; cmp X(rc), xzr);
            "ne" // branch if bool != 0
        };

        let else_is_next = else_block.0 as usize == next_block;
        let then_is_next = then_block.0 as usize == next_block;

        if else_is_next {
            // Common case: else is fall-through.
            // Branch to then if condition met.
            // But we need then_args emitted BEFORE the branch.
            // Use a stub if args are non-trivial.
            if then_args.is_empty()
                || self.args_are_identity(then_block, then_args, ir)
            {
                // No arg copies needed for then branch.
                self.emit_branch_cc(cond_cc, then_lbl);
                // Emit else_args for fall-through.
                self.emit_br_args(else_block, else_args, ir);
                // Fall through to else_block.
            } else if else_args.is_empty()
                || self.args_are_identity(else_block, else_args, ir)
            {
                // No arg copies for else path, so we can emit then_args and branch.
                let inv_cc = self.invert_cc(cond_cc);
                // If NOT cond, jump over then_args to else_block.
                let skip_then = self.ops.new_dynamic_label();
                self.emit_branch_cc(inv_cc, skip_then);
                self.emit_br_args(then_block, then_args, ir);
                dynasm!(self.ops ; .arch aarch64 ; b =>then_lbl);
                dynasm!(self.ops ; .arch aarch64 ; =>skip_then);
                // Fall through to else_block.
            } else {
                // Both have args. Use stub approach.
                let then_stub = self.ops.new_dynamic_label();
                self.emit_branch_cc(cond_cc, then_stub);
                // Fall-through: emit else_args, go to else_block (next).
                self.emit_br_args(else_block, else_args, ir);
                // else_block is next, so just fall through — but we need to skip
                // the then_stub. Actually the then_stub is after, so add a jump
                // over it if else_block is next.
                let after_stub = self.ops.new_dynamic_label();
                dynasm!(self.ops ; .arch aarch64 ; b =>after_stub);
                // then_stub:
                dynasm!(self.ops ; .arch aarch64 ; =>then_stub);
                self.emit_br_args(then_block, then_args, ir);
                dynasm!(self.ops ; .arch aarch64 ; b =>then_lbl);
                dynasm!(self.ops ; .arch aarch64 ; =>after_stub);
            }
        } else if then_is_next {
            // then is fall-through, else needs branch.
            let inv_cc = self.invert_cc(cond_cc);
            if else_args.is_empty()
                || self.args_are_identity(else_block, else_args, ir)
            {
                self.emit_branch_cc(inv_cc, else_lbl);
                self.emit_br_args(then_block, then_args, ir);
                // Fall through to then_block.
            } else {
                let else_stub = self.ops.new_dynamic_label();
                self.emit_branch_cc(inv_cc, else_stub);
                self.emit_br_args(then_block, then_args, ir);
                let after_stub = self.ops.new_dynamic_label();
                dynasm!(self.ops ; .arch aarch64 ; b =>after_stub);
                dynasm!(self.ops ; .arch aarch64 ; =>else_stub);
                self.emit_br_args(else_block, else_args, ir);
                dynasm!(self.ops ; .arch aarch64 ; b =>else_lbl);
                dynasm!(self.ops ; .arch aarch64 ; =>after_stub);
            }
        } else {
            // Neither is next — need explicit branches for both.
            let else_stub = self.ops.new_dynamic_label();
            self.emit_branch_cc(self.invert_cc(cond_cc), else_stub);
            self.emit_br_args(then_block, then_args, ir);
            dynasm!(self.ops ; .arch aarch64 ; b =>then_lbl);
            dynasm!(self.ops ; .arch aarch64 ; =>else_stub);
            self.emit_br_args(else_block, else_args, ir);
            dynasm!(self.ops ; .arch aarch64 ; b =>else_lbl);
        }
    }

    /// Try to fuse a comparison for CondBr. Returns the condition code string if fused.
    fn try_emit_cond_comparison(
        &mut self,
        cond: ValRef,
        ir: &IrFunction,
    ) -> Option<&'static str> {
        if self.val_loc[cond.0 as usize].is_some() {
            return None;
        }
        if !self.is_single_use(cond) {
            return None;
        }

        let inst = self.find_inst(cond, ir)?;
        match inst {
            Inst::ICmp(op, a, b) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let ra = self.materialize_to_gpr(a);
                let rb = self.materialize_to_gpr(b);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), X(rb));
                Some(self.cmpop_to_cc(op))
            }
            Inst::FCmp(op, a, b) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let fa = self.fpr(a);
                let fb = self.fpr(b);
                dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fb));
                Some(self.cmpop_to_cc(op))
            }
            Inst::IsZero(a) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), xzr);
                Some("eq")
            }
            Inst::IsNonZero(a) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let ra = self.materialize_to_gpr(a);
                dynasm!(self.ops ; .arch aarch64 ; cmp X(ra), xzr);
                Some("ne")
            }
            Inst::IsNaN(a) => {
                let a = *a;
                self.use_val(cond);
                self.use_val(a);
                let fa = self.fpr(a);
                dynasm!(self.ops ; .arch aarch64 ; fcmp D(fa), D(fa));
                Some("vs")
            }
            Inst::ICmp128(op, a, b) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let cc = self.emit_icmp128_flags(op, a, b);
                Some(cc)
            }
            Inst::ICmp256(op, a, b) => {
                let (a, b, op) = (*a, *b, *op);
                self.use_val(cond);
                self.use_val(a);
                self.use_val(b);
                let cc = self.emit_icmp256_flags(op, a, b);
                Some(cc)
            }
            _ => None,
        }
    }

    fn cmpop_to_cc(&self, op: CmpOp) -> &'static str {
        match op {
            CmpOp::Eq => "eq",
            CmpOp::Ne => "ne",
            CmpOp::Lt => "lt",
            CmpOp::Le => "le",
            CmpOp::Gt => "gt",
            CmpOp::Ge => "ge",
        }
    }

    fn invert_cc(&self, cc: &str) -> &'static str {
        match cc {
            "eq" => "ne",
            "ne" => "eq",
            "lt" => "ge",
            "le" => "gt",
            "gt" => "le",
            "ge" => "lt",
            "vs" => "vc",
            "vc" => "vs",
            "hi" => "ls",
            "hs" => "lo",
            "lo" => "hs",
            "ls" => "hi",
            _ => panic!("cannot invert cc: {cc}"),
        }
    }

    fn emit_branch_cc(&mut self, cc: &str, label: DynamicLabel) {
        match cc {
            "eq" => dynasm!(self.ops ; .arch aarch64 ; b.eq =>label),
            "ne" => dynasm!(self.ops ; .arch aarch64 ; b.ne =>label),
            "lt" => dynasm!(self.ops ; .arch aarch64 ; b.lt =>label),
            "le" => dynasm!(self.ops ; .arch aarch64 ; b.le =>label),
            "gt" => dynasm!(self.ops ; .arch aarch64 ; b.gt =>label),
            "ge" => dynasm!(self.ops ; .arch aarch64 ; b.ge =>label),
            "vs" => dynasm!(self.ops ; .arch aarch64 ; b.vs =>label),
            "vc" => dynasm!(self.ops ; .arch aarch64 ; b.vc =>label),
            "hi" => dynasm!(self.ops ; .arch aarch64 ; b.hi =>label),
            "hs" => dynasm!(self.ops ; .arch aarch64 ; b.hs =>label),
            "lo" => dynasm!(self.ops ; .arch aarch64 ; b.lo =>label),
            "ls" => dynasm!(self.ops ; .arch aarch64 ; b.ls =>label),
            _ => panic!("unsupported branch cc: {cc}"),
        }
    }

    // -----------------------------------------------------------------------
    // Branch argument emission (parallel copy)
    // -----------------------------------------------------------------------

    fn args_are_identity(
        &self,
        target: BlockRef,
        args: &[ValRef],
        ir: &IrFunction,
    ) -> bool {
        let target_block = &ir.blocks[target.0 as usize];
        if args.len() != target_block.params.len() {
            return false;
        }
        for (i, arg) in args.iter().enumerate() {
            let param_val = target_block.params[i].0;
            let arg_loc = self.val_loc[arg.0 as usize];
            let param_loc = self.val_loc[param_val.0 as usize];
            match (arg_loc, param_loc) {
                (Some(Loc::Gpr(a)), Some(Loc::Gpr(b))) if a == b => {}
                (Some(Loc::Fpr(a)), Some(Loc::Fpr(b))) if a == b => {}
                (Some(Loc::GprPair(a1, a2)), Some(Loc::GprPair(b1, b2)))
                    if a1 == b1 && a2 == b2 => {}
                (Some(Loc::GprQuad(a1, a2, a3, a4)), Some(Loc::GprQuad(b1, b2, b3, b4)))
                    if a1 == b1 && a2 == b2 && a3 == b3 && a4 == b4 => {}
                (Some(Loc::Imm(0)), Some(Loc::Gpr(_))) => {
                    // Zero immediate going to a GPR — need a mov.
                    return false;
                }
                _ => return false,
            }
        }
        true
    }

    fn emit_br_args(
        &mut self,
        target: BlockRef,
        args: &[ValRef],
        ir: &IrFunction,
    ) {
        let target_block = &ir.blocks[target.0 as usize];
        if args.len() != target_block.params.len() {
            panic!(
                "branch arg count ({}) != target param count ({})",
                args.len(),
                target_block.params.len()
            );
        }

        // Build list of moves: (src_loc, dst_loc).
        // First, collect all non-identity moves.
        struct Move {
            src: Loc,
            dst: Loc,
            _param_ty: IrType,
        }

        let mut moves: Vec<Move> = Vec::new();
        for (i, arg) in args.iter().enumerate() {
            let param_val = target_block.params[i].0;
            let param_ty = target_block.params[i].1;
            let src = self.val_loc[arg.0 as usize]
                .unwrap_or_else(|| panic!("branch arg v{} has no location", arg.0));
            let dst = self.val_loc[param_val.0 as usize]
                .unwrap_or_else(|| panic!("target param v{} has no location", param_val.0));

            // Check if this is an identity move.
            if self.locs_equal(src, dst) {
                continue;
            }

            moves.push(Move {
                src,
                dst,
                _param_ty: param_ty,
            });
        }

        // Emit moves. For simplicity, emit them in order — this works when there
        // are no cycles (which is the common case in our pipelines).
        // If there were cycles, we'd need a temp register, but for now let's
        // handle the common cases.
        for m in &moves {
            self.emit_move(m.src, m.dst);
        }
    }

    fn locs_equal(&self, a: Loc, b: Loc) -> bool {
        match (a, b) {
            (Loc::Gpr(a), Loc::Gpr(b)) => a == b,
            (Loc::Fpr(a), Loc::Fpr(b)) => a == b,
            (Loc::GprPair(a1, a2), Loc::GprPair(b1, b2)) => a1 == b1 && a2 == b2,
            (Loc::GprQuad(a1, a2, a3, a4), Loc::GprQuad(b1, b2, b3, b4)) => {
                a1 == b1 && a2 == b2 && a3 == b3 && a4 == b4
            }
            (Loc::Imm(a), Loc::Imm(b)) => a == b,
            _ => false,
        }
    }

    fn emit_move(&mut self, src: Loc, dst: Loc) {
        match (src, dst) {
            (Loc::Gpr(s), Loc::Gpr(d)) => {
                dynasm!(self.ops ; .arch aarch64 ; mov X(d as u32), X(s as u32));
            }
            (Loc::Fpr(s), Loc::Fpr(d)) => {
                dynasm!(self.ops ; .arch aarch64 ; fmov D(d as u32), D(s as u32));
            }
            (Loc::GprPair(s_lo, s_hi), Loc::GprPair(d_lo, d_hi)) => {
                // Handle potential overlap: if d_lo == s_hi, emit hi first.
                if d_lo == s_hi {
                    dynasm!(self.ops ; .arch aarch64
                        ; mov X(d_hi as u32), X(s_hi as u32)
                        ; mov X(d_lo as u32), X(s_lo as u32)
                    );
                } else {
                    dynasm!(self.ops ; .arch aarch64
                        ; mov X(d_lo as u32), X(s_lo as u32)
                        ; mov X(d_hi as u32), X(s_hi as u32)
                    );
                }
            }
            (Loc::GprQuad(s0, s1, s2, s3), Loc::GprQuad(d0, d1, d2, d3)) => {
                // For simplicity, emit all four moves.
                dynasm!(self.ops ; .arch aarch64
                    ; mov X(d0 as u32), X(s0 as u32)
                    ; mov X(d1 as u32), X(s1 as u32)
                    ; mov X(d2 as u32), X(s2 as u32)
                    ; mov X(d3 as u32), X(s3 as u32)
                );
            }
            (Loc::Imm(v), Loc::Gpr(d)) => {
                if v == 0 {
                    dynasm!(self.ops ; .arch aarch64 ; mov X(d as u32), xzr);
                } else {
                    self.emit_mov_imm(d, v);
                }
            }
            (Loc::Imm(_), Loc::Fpr(_)) => {
                panic!("cannot move immediate directly to FPR");
            }
            (Loc::Imm(v), Loc::GprPair(d_lo, d_hi)) => {
                // Treat as 128-bit: lo = v, hi = sign extension
                if v == 0 {
                    dynasm!(self.ops ; .arch aarch64
                        ; mov X(d_lo as u32), xzr
                        ; mov X(d_hi as u32), xzr
                    );
                } else {
                    self.emit_mov_imm(d_lo, v);
                    if v < 0 {
                        // Sign extend
                        dynasm!(self.ops ; .arch aarch64
                            ; movn X(d_hi as u32), 0
                        );
                    } else {
                        dynasm!(self.ops ; .arch aarch64
                            ; mov X(d_hi as u32), xzr
                        );
                    }
                }
            }
            (Loc::Imm(v), Loc::GprQuad(d0, d1, d2, d3)) => {
                // Treat as 256-bit: w0 = v, w1..w3 = sign extension
                if v == 0 {
                    dynasm!(self.ops ; .arch aarch64
                        ; mov X(d0 as u32), xzr
                        ; mov X(d1 as u32), xzr
                        ; mov X(d2 as u32), xzr
                        ; mov X(d3 as u32), xzr
                    );
                } else {
                    self.emit_mov_imm(d0, v);
                    if v < 0 {
                        dynasm!(self.ops ; .arch aarch64
                            ; movn X(d1 as u32), 0
                            ; movn X(d2 as u32), 0
                            ; movn X(d3 as u32), 0
                        );
                    } else {
                        dynasm!(self.ops ; .arch aarch64
                            ; mov X(d1 as u32), xzr
                            ; mov X(d2 as u32), xzr
                            ; mov X(d3 as u32), xzr
                        );
                    }
                }
            }
            _ => panic!("unsupported move: {:?} -> {:?}", src, dst),
        }
    }

    // -----------------------------------------------------------------------
    // Finalize
    // -----------------------------------------------------------------------

    fn finalize(self, spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
        let start = self.start;
        let code_size = self.ops.offset().0 - start.0;
        let buffer = self
            .ops
            .finalize()
            .map_err(|e| JitError::AssemblerError(format!("{e:?}")))?;
        let fn_ptr: FusedPipelineFn = unsafe { std::mem::transmute(buffer.ptr(start)) };
        Ok(CompiledPipeline {
            _buffer: buffer,
            fn_ptr,
            spec: *spec,
            code_size,
        })
    }

    fn finalize_multi(
        self,
        spec: &super::super::multi::MultiPipelineSpec,
    ) -> Result<super::super::multi::CompiledMultiPipeline, JitError> {
        let start = self.start;
        let code_size = self.ops.offset().0 - start.0;
        let buffer = self
            .ops
            .finalize()
            .map_err(|e| JitError::AssemblerError(format!("{e:?}")))?;
        let fn_ptr: super::super::multi::MultiPipelineFn =
            unsafe { std::mem::transmute(buffer.ptr(start)) };
        Ok(super::super::multi::CompiledMultiPipeline {
            _buffer: buffer,
            fn_ptr,
            spec: spec.clone(),
            code_size,
        })
    }

    fn finalize_page_kernel(
        self,
        spec: &super::super::multi::MultiPipelineSpec,
    ) -> Result<super::super::multi::CompiledPageKernel, JitError> {
        let start = self.start;
        let code_size = self.ops.offset().0 - start.0;
        let buffer = self
            .ops
            .finalize()
            .map_err(|e| JitError::AssemblerError(format!("{e:?}")))?;
        let fn_ptr: super::super::multi::PageKernelFn =
            unsafe { std::mem::transmute(buffer.ptr(start)) };
        Ok(super::super::multi::CompiledPageKernel {
            _buffer: buffer,
            fn_ptr,
            spec: spec.clone(),
            code_size,
        })
    }

    // -----------------------------------------------------------------------
    // Multi-column register pre-assignment
    // -----------------------------------------------------------------------

    /// Pre-assign registers for multi-column pipelines.
    ///
    /// Only three context fields get callee-saved registers:
    ///   x0:  ctx_ptr (Arg(0), persists since not in scratch pool)
    ///   x20: row_count (ctx+0)
    ///   x26: result_ptr (ctx+8)
    ///
    /// All per-column data (value pointers, bitmaps, filter constants) is
    /// reloaded from ctx in each block that needs it, using fresh scratch
    /// registers. This removes any hard limit on column count.
    fn pre_assign_multi_registers(&mut self, ir: &IrFunction) {
        let entry = &ir.blocks[0];

        for (val, inst) in &entry.insts {
            match inst {
                Inst::Arg(0) => {
                    self.val_loc[val.0 as usize] = Some(Loc::Gpr(0));
                }
                Inst::LoadField { base, offset, .. } => {
                    if matches!(self.val_loc[base.0 as usize], Some(Loc::Gpr(0))) {
                        let loc = match *offset {
                            0 => Some(Loc::Gpr(20)), // row_count
                            8 => Some(Loc::Gpr(26)), // result_ptr
                            _ => None,               // everything else → scratch
                        };
                        if let Some(l) = loc {
                            self.val_loc[val.0 as usize] = Some(l);
                        }
                    }
                }
                _ => {}
            }
        }

        // Assign block parameters.
        let is_multi_mat = self.spec.output_mode == JitOutputMode::Materialize;
        for (block_idx, block) in ir.blocks.iter().enumerate() {
            if block_idx == 0 {
                continue;
            }
            if is_multi_mat {
                self.assign_multi_materialize_params(block);
            } else {
                self.assign_block_params(block, block.params.len());
            }
        }
    }

    /// Assign block parameters for multi-column materialize mode.
    ///
    /// Two layouts supported:
    ///
    /// **Multi-column flat-array** (from encode/multi.rs):
    ///   `(ctr, cursor_0, ..., cursor_N-1, count)`
    ///   param[0]       = ctr     → x19
    ///   param[1..N]    = cursors → x21, x23, x24, x25, x27, x28
    ///   param[N+1]     = count   → x22
    ///
    /// **Page-kernel with bitmap** (from encode/multi_page.rs):
    ///   `(row_ctr, val_ctr, cursor_0, ..., cursor_N-1, count)`
    ///   param[0]       = row_ctr → x19
    ///   param[1]       = val_ctr → x23
    ///   param[2..N+1]  = cursors → x21, x24, x25, x27, x28
    ///   param[N+2]     = count   → x22
    ///
    /// We detect the page-kernel layout by checking if the block has
    /// the expected extra parameter (n_params = n_cursors + 3 vs + 2).
    fn assign_multi_materialize_params(&mut self, block: &Block) {
        let n = block.params.len();

        let is_page_kernel = self.page_kernel_mode;

        if is_page_kernel {
            // Page-kernel layout: (row_ctr, val_ctr, cursors..., count [, hoisted...])
            //
            // count is always at position (2 + n_mat_columns) where n_mat_columns
            // comes from the spec. After count come optional hoisted loop-invariants.
            let n_mat = self.n_mat_columns;
            let count_idx = 2 + n_mat; // row_ctr + val_ctr + n cursors

            const CURSOR_REGS: [u8; 5] = [21, 24, 25, 27, 28];

            for (i, (val, ty)) in block.params.iter().enumerate() {
                let loc = if i == 0 {
                    Loc::Gpr(19) // row_ctr
                } else if i == 1 {
                    Loc::Gpr(23) // val_ctr
                } else if i >= 2 && i < count_idx {
                    // Cursor.
                    let ci = i - 2;
                    if ci < CURSOR_REGS.len() {
                        Loc::Gpr(CURSOR_REGS[ci])
                    } else {
                        Loc::Gpr(SCRATCH_GPRS[0])
                    }
                } else if i == count_idx {
                    Loc::Gpr(22) // count
                } else {
                    // Hoisted loop-invariant. Use callee-saved registers not
                    // taken by cursors.
                    let hoisted_idx = i - count_idx - 1;
                    let avail: Vec<u8> = [24u8, 25, 27, 28]
                        .into_iter()
                        .filter(|r| {
                            !(0..n_mat).any(|ci| ci < CURSOR_REGS.len() && CURSOR_REGS[ci] == *r)
                        })
                        .collect();
                    if *ty == IrType::F64 || *ty == IrType::F32 {
                        Loc::Fpr(9) // d9 callee-saved
                    } else if hoisted_idx < avail.len() {
                        Loc::Gpr(avail[hoisted_idx])
                    } else {
                        Loc::Gpr(SCRATCH_GPRS[0])
                    }
                };
                self.val_loc[val.0 as usize] = Some(loc);
            }
        } else {
            // Flat-array layout: (ctr, cursors..., count)
            const CURSOR_REGS: [u8; 6] = [21, 23, 24, 25, 27, 28];
            for (i, (val, _ty)) in block.params.iter().enumerate() {
                let loc = if i == 0 {
                    Loc::Gpr(19) // ctr
                } else if i == n - 1 {
                    Loc::Gpr(22) // count
                } else {
                    let cursor_idx = i - 1;
                    if cursor_idx < CURSOR_REGS.len() {
                        Loc::Gpr(CURSOR_REGS[cursor_idx])
                    } else {
                        Loc::Gpr(SCRATCH_GPRS[0])
                    }
                };
                self.val_loc[val.0 as usize] = Some(loc);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub fn compile(ir: &IrFunction, spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
    let mut c = Compiler::new(ir, spec)?;
    c.count_uses(ir);
    c.pre_assign_registers(ir);
    c.emit_prologue();
    for (i, block) in ir.blocks.iter().enumerate() {
        c.emit_block(i, block, ir);
    }
    c.finalize(spec)
}

pub fn compile_multi(
    ir: &IrFunction,
    multi_spec: &super::super::multi::MultiPipelineSpec,
) -> Result<super::super::multi::CompiledMultiPipeline, JitError> {
    let backend_spec = multi_spec.backend_spec();
    let mut c = Compiler::new(ir, &backend_spec)?;
    c.uses_fp = multi_spec.uses_fp();
    c.multi_mode = true;
    c.count_uses(ir);
    c.pre_assign_multi_registers(ir);
    c.emit_prologue();
    for (i, block) in ir.blocks.iter().enumerate() {
        c.emit_block(i, block, ir);
    }
    c.finalize_multi(multi_spec)
}

pub fn compile_page_kernel(
    ir: &IrFunction,
    multi_spec: &super::super::multi::MultiPipelineSpec,
) -> Result<super::super::multi::CompiledPageKernel, JitError> {
    let backend_spec = multi_spec.backend_spec();
    let mut c = Compiler::new(ir, &backend_spec)?;
    c.uses_fp = multi_spec.uses_fp();
    c.multi_mode = true;
    c.page_kernel_mode = true;
    c.n_mat_columns = multi_spec.columns.iter()
        .filter(|col| col.role == super::super::multi::ColumnRole::Materialize)
        .count();
    c.count_uses(ir);
    c.pre_assign_multi_registers(ir);
    c.emit_prologue();
    for (i, block) in ir.blocks.iter().enumerate() {
        c.emit_block(i, block, ir);
    }
    c.finalize_page_kernel(multi_spec)
}
