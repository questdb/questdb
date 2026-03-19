//! Layer 5 (Codegen) — Platform-independent instruction emission.
//!
//! Follows the Flying Start approach: each semantic operation maps to a fixed
//! sequence of machine instructions with comparison-branch fusion.
//!
//! ## Register allocation
//!
//! ### AArch64 (AAPCS64)
//! Args:   x0=values  x1=bitmap  w2=rows  x3=flt_lo  x4=flt_hi  x5=result
//! Saved:  x19=ctr  x20=rows  x21=iacc/acc_w0  x22=cnt  x23=has
//!         x24=acc_hi/w1  x25=acc_w2  x26=acc_w3
//! FP:     d8=facc  d9=flt_lo  d10=flt_hi   (d8-d15 callee-saved)
//! Scratch: x9-x15, d0-d7
//!
//! ### x86-64 (System V)
//! Args:   rdi=values  rsi=bitmap  edx=rows  rcx=flt_lo  r8=flt_hi  r9=result
//! Saved:  rbx=ctr  r12=rows  r13=iacc/acc_w0  r14=cnt  r15=has
//! Stack:  [rsp+0]=acc_hi  [rsp+8]=acc_w2  [rsp+16]=acc_w3
//! FP:     xmm3=facc  xmm1=flt_lo  xmm2=flt_hi
//! Scratch: rax rdx r10 r11, xmm0 xmm4-xmm15
//!
//! For Int128/Int256, `filter_lo` and `filter_hi` are **pointers** to the
//! multi-word filter constants (not the constants themselves).

use dynasmrt::{DynasmApi, DynasmLabelApi, DynamicLabel};

use super::{
    CompiledPipeline, FusedPipelineFn, JitAggregateOp, JitError, JitFilterOp, JitPhysicalType,
    PipelineSpec,
};

pub struct Codegen {
    #[cfg(target_arch = "aarch64")]
    pub ops: dynasmrt::aarch64::Assembler,
    #[cfg(target_arch = "x86_64")]
    pub ops: dynasmrt::x64::Assembler,
    start: dynasmrt::AssemblyOffset,
}

impl Codegen {
    pub fn new() -> Result<Self, JitError> {
        #[cfg(target_arch = "aarch64")]
        let ops =
            dynasmrt::aarch64::Assembler::new().map_err(|e| JitError::AssemblerError(format!("{e}")))?;
        #[cfg(target_arch = "x86_64")]
        let ops =
            dynasmrt::x64::Assembler::new().map_err(|e| JitError::AssemblerError(format!("{e}")))?;
        let mut cg = Codegen { ops, start: dynasmrt::AssemblyOffset(0) };
        cg.start = cg.ops.offset();
        Ok(cg)
    }

    pub fn finalize(self, spec: &PipelineSpec) -> Result<CompiledPipeline, JitError> {
        let start = self.start;
        let code_size = self.ops.offset().0 - start.0;
        let buffer = self.ops.finalize().map_err(|e| JitError::AssemblerError(format!("{e:?}")))?;
        let fn_ptr: FusedPipelineFn = unsafe { std::mem::transmute(buffer.ptr(start)) };
        Ok(CompiledPipeline { _buffer: buffer, fn_ptr, spec: *spec, code_size })
    }

    // ---- Labels ----
    pub fn label(&mut self) -> DynamicLabel { self.ops.new_dynamic_label() }
    pub fn bind(&mut self, l: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; =>l);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; =>l);
    }
    pub fn jump(&mut self, l: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; b =>l);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; jmp =>l);
    }

    // ==================================================================
    // Prologue / Epilogue
    // ==================================================================

    pub fn emit_prologue(&mut self, uses_fp: bool) {
        #[cfg(target_arch = "aarch64")]
        {
            // Save x19-x26 (4 pairs) — enough for all types incl. Int256.
            dynasm!(self.ops ; .arch aarch64
                ; stp x19, x20, [sp, -16]!
                ; stp x21, x22, [sp, -16]!
                ; stp x23, x24, [sp, -16]!
                ; stp x25, x26, [sp, -16]!
            );
            if uses_fp {
                dynasm!(self.ops ; .arch aarch64
                    ; stp d8, d9, [sp, -16]!
                    ; stp d10, d11, [sp, -16]!
                );
            }
            dynasm!(self.ops ; .arch aarch64
                ; mov w19, wzr  ; mov w20, w2
                ; mov x21, xzr ; mov x22, xzr ; mov x23, xzr
                ; mov x24, xzr ; mov x25, xzr ; mov x26, xzr
            );
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64
                ; push rbx ; push r12 ; push r13 ; push r14 ; push r15
                ; sub rsp, 32  // 5 pushes (40) + sub 32 = 72; entry rsp = 16k-8 → 16k-80 = 16(k-5). Aligned.
                ; xor ebx, ebx ; mov r12d, edx
                ; xor r13, r13 ; xor r14, r14 ; xor r15, r15
                ; mov QWORD [rsp], 0 ; mov QWORD [rsp+8], 0 ; mov QWORD [rsp+16], 0
            );
        }
    }

    pub fn emit_epilogue(&mut self, spec: &PipelineSpec) {
        let pt = spec.physical_type;
        if spec.output_mode == super::JitOutputMode::Materialize {
            self.store_materialize_count();
        } else if pt.is_float() {
            self.store_fp_results();
        } else if pt.is_wide() {
            self.store_wide_results(pt);
        } else {
            self.store_int_results();
        }
        #[cfg(target_arch = "aarch64")]
        {
            dynasm!(self.ops ; .arch aarch64 ; mov w0, wzr);
            if pt.is_float() {
                dynasm!(self.ops ; .arch aarch64
                    ; ldp d10, d11, [sp], 16 ; ldp d8, d9, [sp], 16);
            }
            dynasm!(self.ops ; .arch aarch64
                ; ldp x25, x26, [sp], 16
                ; ldp x23, x24, [sp], 16
                ; ldp x21, x22, [sp], 16
                ; ldp x19, x20, [sp], 16
                ; ret
            );
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64
                ; xor eax, eax ; add rsp, 32
                ; pop r15 ; pop r14 ; pop r13 ; pop r12 ; pop rbx ; ret
            );
        }
    }

    // ==================================================================
    // Loop control
    // ==================================================================
    pub fn loop_check(&mut self, done: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; cmp w19, w20 ; b.ge =>done);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; cmp ebx, r12d ; jge =>done);
    }
    pub fn loop_inc(&mut self, top: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; add w19, w19, 1 ; b =>top);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; inc ebx ; jmp =>top);
    }

    // ==================================================================
    // Null bitmap
    // ==================================================================
    pub fn check_bitmap_null(&mut self, no_nulls: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; cbz x1, =>no_nulls);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; test rsi, rsi ; jz =>no_nulls);
    }
    pub fn skip_if_null(&mut self, l: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; ldrb w9, [x1, x19] ; cbz w9, =>l);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; movzx eax, BYTE [rsi + rbx] ; test al, al ; jz =>l);
    }
    pub fn skip_if_not_null(&mut self, l: DynamicLabel) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; ldrb w9, [x1, x19] ; cbnz w9, =>l);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; movzx eax, BYTE [rsi + rbx] ; test al, al ; jnz =>l);
    }
    pub fn inc_count(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; add x22, x22, 1);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; inc r14);
    }

    // ==================================================================
    // Integer (1–8 byte) value operations
    // ==================================================================

    /// Load value at `values[counter*stride]` into x9 (aarch64) / rax (x86_64).
    pub fn load_int_value(&mut self, pt: JitPhysicalType) {
        match pt {
            JitPhysicalType::Int8 => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; ldrsb x9, [x0, x19]);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; movsx rax, BYTE [rdi + rbx]);
            }
            JitPhysicalType::Int16 => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; lsl x10, x19, 1 ; ldrsh x9, [x0, x10]);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; movsx rax, WORD [rdi + rbx * 2]);
            }
            JitPhysicalType::Int32 => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; lsl x10, x19, 2 ; ldrsw x9, [x0, x10]);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; movsxd rax, DWORD [rdi + rbx * 4]);
            }
            JitPhysicalType::Int64 => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; lsl x10, x19, 3 ; ldr x9, [x0, x10]);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; mov rax, QWORD [rdi + rbx * 8]);
            }
            _ => unreachable!("load_int_value: not 1-8 byte int"),
        }
    }

    /// Compare i64 value vs filter_lo/filter_hi, skip when filter NOT satisfied.
    pub fn int_cmp_skip(&mut self, fop: JitFilterOp, skip: DynamicLabel) {
        match fop {
            JitFilterOp::None | JitFilterOp::IsNotNull => {}
            JitFilterOp::Eq => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.ne =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jne =>skip);
            }
            JitFilterOp::Ne => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.eq =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; je =>skip);
            }
            JitFilterOp::Lt => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.ge =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jge =>skip);
            }
            JitFilterOp::Le => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.gt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jg =>skip);
            }
            JitFilterOp::Gt => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.le =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jle =>skip);
            }
            JitFilterOp::Ge => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.lt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jl =>skip);
            }
            JitFilterOp::Between => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; cmp x9, x3 ; b.lt =>skip ; cmp x9, x4 ; b.gt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; cmp rax, rcx ; jl =>skip ; cmp rax, r8 ; jg =>skip);
            }
            JitFilterOp::IsNull => unreachable!(),
        }
    }

    /// Accumulate i64 value.
    pub fn int_aggregate(&mut self, aop: JitAggregateOp, set: DynamicLabel, after: DynamicLabel) {
        match aop {
            JitAggregateOp::Count => self.inc_count(),
            JitAggregateOp::Sum => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; add x21, x21, x9 ; add x22, x22, 1 ; mov x23, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; add r13, rax ; inc r14 ; mov r15, 1);
            }
            JitAggregateOp::Min => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; cbz x23, =>set ; cmp x9, x21 ; b.ge =>after
                    ; =>set ; mov x21, x9 ; mov x23, 1 ; =>after ; add x22, x22, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; test r15, r15 ; jz =>set ; cmp rax, r13 ; jge =>after
                    ; =>set ; mov r13, rax ; mov r15, 1 ; =>after ; inc r14);
            }
            JitAggregateOp::Max => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; cbz x23, =>set ; cmp x9, x21 ; b.le =>after
                    ; =>set ; mov x21, x9 ; mov x23, 1 ; =>after ; add x22, x22, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; test r15, r15 ; jz =>set ; cmp rax, r13 ; jle =>after
                    ; =>set ; mov r13, rax ; mov r15, 1 ; =>after ; inc r14);
            }
        }
    }

    // ==================================================================
    // Floating-point operations
    // ==================================================================

    pub fn init_fp_filters(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; fmov d9, x3 ; fmov d10, x4);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; movq xmm1, rcx ; movq xmm2, r8);
    }

    pub fn init_fp_acc(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; movi d8, 0);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; xorpd xmm3, xmm3);
    }

    pub fn load_fp_value(&mut self, pt: JitPhysicalType) {
        match pt {
            JitPhysicalType::Float => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; lsl x10, x19, 2 ; ldr s0, [x0, x10] ; fcvt d0, s0);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; movss xmm0, [rdi + rbx * 4] ; cvtss2sd xmm0, xmm0);
            }
            JitPhysicalType::Double => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; lsl x10, x19, 3 ; ldr d0, [x0, x10]);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; movsd xmm0, [rdi + rbx * 8]);
            }
            _ => unreachable!(),
        }
    }

    pub fn fp_cmp_skip(&mut self, fop: JitFilterOp, skip: DynamicLabel) {
        // NaN pre-check for any real comparison.
        if !matches!(fop, JitFilterOp::None | JitFilterOp::IsNotNull) {
            #[cfg(target_arch = "aarch64")]
            dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d0 ; b.vs =>skip);
            #[cfg(target_arch = "x86_64")]
            dynasm!(self.ops ; .arch x64 ; ucomisd xmm0, xmm0 ; jp =>skip);
        }
        match fop {
            JitFilterOp::None | JitFilterOp::IsNotNull => {}
            JitFilterOp::Eq => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.ne =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm0, xmm1 ; jne =>skip);
            }
            JitFilterOp::Ne => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.eq =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm0, xmm1 ; je =>skip);
            }
            JitFilterOp::Lt => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.ge =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm1, xmm0 ; jbe =>skip);
            }
            JitFilterOp::Le => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.gt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm1, xmm0 ; jb =>skip);
            }
            JitFilterOp::Gt => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.le =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm0, xmm1 ; jbe =>skip);
            }
            JitFilterOp::Ge => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.lt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; ucomisd xmm0, xmm1 ; jb =>skip);
            }
            JitFilterOp::Between => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fcmp d0, d9 ; b.lt =>skip ; fcmp d0, d10 ; b.gt =>skip);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; ucomisd xmm1, xmm0 ; ja =>skip
                    ; ucomisd xmm0, xmm2 ; ja =>skip);
            }
            JitFilterOp::IsNull => unreachable!(),
        }
    }

    pub fn fp_aggregate(&mut self, aop: JitAggregateOp, set: DynamicLabel, after: DynamicLabel) {
        match aop {
            JitAggregateOp::Count => self.inc_count(),
            JitAggregateOp::Sum => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64 ; fadd d8, d8, d0 ; add x22, x22, 1 ; mov x23, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64 ; addsd xmm3, xmm0 ; inc r14 ; mov r15, 1);
            }
            JitAggregateOp::Min => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; cbz x23, =>set ; fcmp d0, d8 ; b.ge =>after
                    ; =>set ; fmov d8, d0 ; mov x23, 1 ; =>after ; add x22, x22, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; test r15, r15 ; jz =>set ; ucomisd xmm3, xmm0 ; jbe =>after
                    ; =>set ; movsd xmm3, xmm0 ; mov r15, 1 ; =>after ; inc r14);
            }
            JitAggregateOp::Max => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; cbz x23, =>set ; fcmp d0, d8 ; b.le =>after
                    ; =>set ; fmov d8, d0 ; mov x23, 1 ; =>after ; add x22, x22, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; test r15, r15 ; jz =>set ; ucomisd xmm0, xmm3 ; jbe =>after
                    ; =>set ; movsd xmm3, xmm0 ; mov r15, 1 ; =>after ; inc r14);
            }
        }
    }

    // ==================================================================
    // Int128 (16-byte) operations
    // ==================================================================
    // Value: x9=lo x10=hi (aarch64)  rax=lo rdx=hi (x86_64)
    // Acc:   x21=lo x24=hi           r13=lo [rsp]=hi
    // Filter loaded from pointer in x3/rcx.

    /// Load 128-bit value at `values[counter*16]` into (x9=lo,x10=hi) / (rax=lo,rdx=hi).
    pub fn load_int128_value(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64
            ; lsl x11, x19, 4          // x11 = counter * 16
            ; add x11, x0, x11         // x11 = &values[counter*16]
            ; ldp x9, x10, [x11]       // x9=lo, x10=hi
        );
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov r10, rbx
            ; shl r10, 4               // r10 = counter * 16
            ; add r10, rdi             // r10 = &values[counter*16]
            ; mov rax, QWORD [r10]     // rax = lo
            ; mov rdx, QWORD [r10 + 8] // rdx = hi
        );
    }

    /// 128-bit comparison. filter_lo (x3/rcx) is a *pointer* to [lo, hi].
    pub fn int128_cmp_skip(&mut self, fop: JitFilterOp, skip: DynamicLabel) {
        match fop {
            JitFilterOp::None | JitFilterOp::IsNotNull => {}
            JitFilterOp::Eq => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; ldp x11, x12, [x3]
                    ; cmp x9, x11 ; b.ne =>skip
                    ; cmp x10, x12 ; b.ne =>skip
                );
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; cmp rax, QWORD [rcx] ; jne =>skip
                    ; cmp rdx, QWORD [rcx + 8] ; jne =>skip
                );
            }
            JitFilterOp::Ne => {
                let eq = self.label();
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; ldp x11, x12, [x3]
                    ; cmp x9, x11 ; b.ne =>eq
                    ; cmp x10, x12 ; b.eq =>skip
                    ; =>eq
                );
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; cmp rax, QWORD [rcx] ; jne =>eq
                    ; cmp rdx, QWORD [rcx + 8] ; je =>skip
                    ; =>eq
                );
            }
            JitFilterOp::Lt => { self.wide128_ord_skip(skip, true, true); }
            JitFilterOp::Le => { self.wide128_ord_skip(skip, true, false); }
            JitFilterOp::Gt => { self.wide128_ord_skip(skip, false, true); }
            JitFilterOp::Ge => { self.wide128_ord_skip(skip, false, false); }
            JitFilterOp::Between => {
                // skip if value < lo  OR  value > hi
                // For "skip if value < lo": use Ge negation (is_lt=false, strict=false)
                self.wide128_ord_skip(skip, false, false);  // skip if NOT(value >= lo) → skip if value < lo
                self.wide128_ord_skip_hi(skip);              // skip if value > hi
            }
            JitFilterOp::IsNull => unreachable!(),
        }
    }

    /// 128-bit aggregate.
    pub fn int128_aggregate(&mut self, aop: JitAggregateOp, set: DynamicLabel, after: DynamicLabel) {
        match aop {
            JitAggregateOp::Count => self.inc_count(),
            JitAggregateOp::Sum => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; adds x21, x21, x9 ; adc x24, x24, x10
                    ; add x22, x22, 1 ; mov x23, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; add r13, rax ; adc QWORD [rsp], rdx
                    ; inc r14 ; mov r15, 1);
            }
            JitAggregateOp::Min => { self.int128_minmax(set, after, true); }
            JitAggregateOp::Max => { self.int128_minmax(set, after, false); }
        }
    }

    /// Store 128-bit result at offsets 8 (lo) and 32 (hi).
    pub fn store_wide_results(&mut self, pt: JitPhysicalType) {
        #[cfg(target_arch = "aarch64")]
        {
            dynasm!(self.ops ; .arch aarch64
                ; str x22, [x5]     // count
                ; str x21, [x5, 8]  // lo
                ; str x23, [x5, 24] // has_value
                ; str x24, [x5, 32] // hi / w1
            );
            if pt == JitPhysicalType::Int256 {
                dynasm!(self.ops ; .arch aarch64
                    ; str x25, [x5, 40] ; str x26, [x5, 48]);
            }
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64
                ; mov QWORD [r9], r14
                ; mov QWORD [r9 + 8], r13
                ; mov QWORD [r9 + 24], r15
                ; mov rax, QWORD [rsp]
                ; mov QWORD [r9 + 32], rax
            );
            if pt == JitPhysicalType::Int256 {
                dynasm!(self.ops ; .arch x64
                    ; mov rax, QWORD [rsp + 8] ; mov QWORD [r9 + 40], rax
                    ; mov rax, QWORD [rsp + 16] ; mov QWORD [r9 + 48], rax
                );
            }
        }
    }

    // ==================================================================
    // Int256 (32-byte) operations
    // ==================================================================
    // Value: x9-x12 (aarch64)   rax,rdx,r10,r11 (x86_64)
    // Acc:   x21,x24,x25,x26   r13,[rsp],[rsp+8],[rsp+16]

    /// Load 256-bit value at `values[counter*32]` into
    /// (x9=w0, x10=w1, x11=w2, x12=w3) / (rax=w0, rdx=w1, r10=w2, r11=w3).
    pub fn load_int256_value(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64
            ; lsl x13, x19, 5           // x13 = counter * 32
            ; add x13, x0, x13          // x13 = &values[counter*32]
            ; ldp x9, x10, [x13]        // x9=w0, x10=w1
            ; ldp x11, x12, [x13, 16]   // x11=w2, x12=w3
        );
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov r10, rbx
            ; shl r10, 5                // r10 = counter * 32
            ; add r10, rdi              // r10 = &values[counter*32]
            ; mov rax, QWORD [r10]      // rax = w0
            ; mov rdx, QWORD [r10 + 8]  // rdx = w1
            ; mov r11, QWORD [r10 + 24] // r11 = w3 (load before r10 is overwritten)
            ; mov r10, QWORD [r10 + 16]  // r10 = w2
        );
    }

    pub fn int256_cmp_skip(&mut self, fop: JitFilterOp, skip: DynamicLabel) {
        match fop {
            JitFilterOp::None | JitFilterOp::IsNotNull => {}
            JitFilterOp::Eq => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; ldp x13, x14, [x3]
                    ; cmp x9, x13 ; b.ne =>skip
                    ; cmp x10, x14 ; b.ne =>skip
                    ; ldp x13, x14, [x3, 16]
                    ; cmp x11, x13 ; b.ne =>skip
                    ; cmp x12, x14 ; b.ne =>skip
                );
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; cmp rax, QWORD [rcx] ; jne =>skip
                    ; cmp rdx, QWORD [rcx + 8] ; jne =>skip
                    ; cmp r10, QWORD [rcx + 16] ; jne =>skip
                    ; cmp r11, QWORD [rcx + 24] ; jne =>skip
                );
            }
            JitFilterOp::Ne => {
                let ne = self.label();
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; ldp x13, x14, [x3]
                    ; cmp x9, x13 ; b.ne =>ne
                    ; cmp x10, x14 ; b.ne =>ne
                    ; ldp x13, x14, [x3, 16]
                    ; cmp x11, x13 ; b.ne =>ne
                    ; cmp x12, x14 ; b.eq =>skip
                    ; =>ne
                );
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; cmp rax, QWORD [rcx] ; jne =>ne
                    ; cmp rdx, QWORD [rcx + 8] ; jne =>ne
                    ; cmp r10, QWORD [rcx + 16] ; jne =>ne
                    ; cmp r11, QWORD [rcx + 24] ; je =>skip
                    ; =>ne
                );
            }
            // For ordered comparisons, compare most-significant word first (signed),
            // then cascade to lower words (unsigned).
            JitFilterOp::Lt | JitFilterOp::Le | JitFilterOp::Gt | JitFilterOp::Ge => {
                self.wide256_ord_skip(fop, skip, false);
            }
            JitFilterOp::Between => {
                // skip if value < lo: use Ge negation
                self.wide256_ord_skip(JitFilterOp::Ge, skip, false);
                // skip if value > hi: use Le negation
                self.wide256_ord_skip(JitFilterOp::Le, skip, true);
            }
            JitFilterOp::IsNull => unreachable!(),
        }
    }

    pub fn int256_aggregate(&mut self, aop: JitAggregateOp, set: DynamicLabel, after: DynamicLabel) {
        match aop {
            JitAggregateOp::Count => self.inc_count(),
            JitAggregateOp::Sum => {
                #[cfg(target_arch = "aarch64")]
                dynasm!(self.ops ; .arch aarch64
                    ; adds x21, x21, x9 ; adcs x24, x24, x10
                    ; adcs x25, x25, x11 ; adc x26, x26, x12
                    ; add x22, x22, 1 ; mov x23, 1);
                #[cfg(target_arch = "x86_64")]
                dynasm!(self.ops ; .arch x64
                    ; add r13, rax ; adc QWORD [rsp], rdx
                    ; adc QWORD [rsp+8], r10 ; adc QWORD [rsp+16], r11
                    ; inc r14 ; mov r15, 1);
            }
            JitAggregateOp::Min => { self.int256_minmax(set, after, true); }
            JitAggregateOp::Max => { self.int256_minmax(set, after, false); }
        }
    }

    // ==================================================================
    // Materialize — write matching values to output buffer
    // ==================================================================
    // AArch64: x24=write_cursor (starts at x5=result_ptr), x4=count_ptr
    // x86_64:  [rsp+0]=write_cursor (starts at r9), r8=count_ptr

    /// Set up write cursor from result_ptr.  For Materialize mode,
    /// `result_ptr` (x5/r9) is the output data buffer and
    /// `filter_hi` (x4/r8) is a `*mut u64` that receives the count.
    pub fn init_materialize(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; mov x24, x5); // write cursor = result_ptr
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; mov QWORD [rsp], r9); // write cursor = result_ptr
    }

    /// Store the loaded i64 value (x9 / rax) at the write cursor and advance
    /// by `type_size` bytes.
    pub fn materialize_int_value(&mut self, pt: JitPhysicalType) {
        let sz: i32 = match pt {
            JitPhysicalType::Int8  => 1,
            JitPhysicalType::Int16 => 2,
            JitPhysicalType::Int32 => 4,
            JitPhysicalType::Int64 => 8,
            _ => unreachable!(),
        };
        #[cfg(target_arch = "aarch64")]
        {
            match sz {
                1 => dynasm!(self.ops ; .arch aarch64 ; strb w9, [x24] ; add x24, x24, 1),
                2 => dynasm!(self.ops ; .arch aarch64 ; strh w9, [x24] ; add x24, x24, 2),
                4 => dynasm!(self.ops ; .arch aarch64 ; str  w9, [x24] ; add x24, x24, 4),
                8 => dynasm!(self.ops ; .arch aarch64 ; str  x9, [x24] ; add x24, x24, 8),
                _ => unreachable!(),
            }
            dynasm!(self.ops ; .arch aarch64 ; add x22, x22, 1);
        }
        #[cfg(target_arch = "x86_64")]
        {
            match sz {
                1 => dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp] ; mov BYTE [r10], al
                    ; add QWORD [rsp], 1),
                2 => dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp] ; mov WORD [r10], ax
                    ; add QWORD [rsp], 2),
                4 => dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp] ; mov DWORD [r10], eax
                    ; add QWORD [rsp], 4),
                8 => dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp] ; mov QWORD [r10], rax
                    ; add QWORD [rsp], 8),
                _ => unreachable!(),
            }
            dynasm!(self.ops ; .arch x64 ; inc r14);
        }
    }

    /// Store the loaded f64 value (d0 / xmm0) at the write cursor and advance.
    pub fn materialize_fp_value(&mut self, pt: JitPhysicalType) {
        #[cfg(target_arch = "aarch64")]
        {
            match pt {
                JitPhysicalType::Float => {
                    // Convert back from d0 (f64) to s0 (f32) before storing.
                    dynasm!(self.ops ; .arch aarch64
                        ; fcvt s0, d0 ; str s0, [x24] ; add x24, x24, 4);
                }
                JitPhysicalType::Double => {
                    dynasm!(self.ops ; .arch aarch64
                        ; str d0, [x24] ; add x24, x24, 8);
                }
                _ => unreachable!(),
            }
            dynasm!(self.ops ; .arch aarch64 ; add x22, x22, 1);
        }
        #[cfg(target_arch = "x86_64")]
        {
            match pt {
                JitPhysicalType::Float => {
                    dynasm!(self.ops ; .arch x64
                        ; cvtsd2ss xmm0, xmm0          // back to f32
                        ; mov r10, QWORD [rsp]
                        ; movss [r10], xmm0
                        ; add QWORD [rsp], 4);
                }
                JitPhysicalType::Double => {
                    dynasm!(self.ops ; .arch x64
                        ; mov r10, QWORD [rsp]
                        ; movsd [r10], xmm0
                        ; add QWORD [rsp], 8);
                }
                _ => unreachable!(),
            }
            dynasm!(self.ops ; .arch x64 ; inc r14);
        }
    }

    /// Store the loaded 128-bit value (x9,x10 / rax,rdx) at the write cursor.
    pub fn materialize_int128_value(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64
            ; stp x9, x10, [x24] ; add x24, x24, 16 ; add x22, x22, 1);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov r10, QWORD [rsp]
            ; mov QWORD [r10], rax ; mov QWORD [r10 + 8], rdx
            ; add QWORD [rsp], 16 ; inc r14);
    }

    /// Store the loaded 256-bit value (x9-x12 / rax,rdx,r10,r11) at the write cursor.
    pub fn materialize_int256_value(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64
            ; stp x9, x10, [x24] ; stp x11, x12, [x24, 16]
            ; add x24, x24, 32 ; add x22, x22, 1);
        #[cfg(target_arch = "x86_64")]
        {
            // r10 and r11 hold value w2 and w3, but we also need r10 for the cursor.
            // Save w2 (r10) first, then use r10 for cursor.
            dynasm!(self.ops ; .arch x64
                ; push r10                      // save w2
                ; mov r10, QWORD [rsp + 8]      // write cursor (rsp shifted by push)
                ; mov QWORD [r10], rax           // w0
                ; mov QWORD [r10 + 8], rdx       // w1
                ; pop rax                         // restore w2 into rax
                ; mov QWORD [r10 + 16], rax      // w2
                ; mov QWORD [r10 + 24], r11      // w3
                ; add r10, 32
                ; mov QWORD [rsp], r10           // update write cursor
                ; inc r14);
        }
    }

    /// Write final count to the count pointer (filter_hi = x4 / r8).
    fn store_materialize_count(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; str x22, [x4]);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64 ; mov QWORD [r8], r14);
    }

    // ==================================================================
    // Result storage
    // ==================================================================

    fn store_int_results(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; str x22, [x5] ; str x21, [x5, 8] ; str x23, [x5, 24]);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov QWORD [r9], r14 ; mov QWORD [r9 + 8], r13 ; mov QWORD [r9 + 24], r15);
    }

    fn store_fp_results(&mut self) {
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64 ; str x22, [x5] ; str d8, [x5, 16] ; str x23, [x5, 24]);
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov QWORD [r9], r14 ; movsd [r9 + 16], xmm3 ; mov QWORD [r9 + 24], r15);
    }

    // ==================================================================
    // Private helpers — 128-bit ordered comparison
    // ==================================================================

    /// Skip when value does NOT satisfy `< filter_lo` (if is_lt) or `> filter_lo` (if !is_lt).
    /// `strict`: true for LT/GT, false for LE/GE.
    fn wide128_ord_skip(&mut self, skip: DynamicLabel, is_lt: bool, strict: bool) {
        let pass = self.label();
        #[cfg(target_arch = "aarch64")]
        {
            dynasm!(self.ops ; .arch aarch64 ; ldp x11, x12, [x3]);
            if is_lt {
                // skip when value >= filter (i.e., NOT less)
                dynasm!(self.ops ; .arch aarch64 ; cmp x10, x12 ; b.gt =>skip ; b.lt =>pass);
                if strict { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x11 ; b.hs =>skip); }
                else       { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x11 ; b.hi =>skip); }
            } else {
                // skip when value <= filter (i.e., NOT greater)
                dynasm!(self.ops ; .arch aarch64 ; cmp x10, x12 ; b.lt =>skip ; b.gt =>pass);
                if strict { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x11 ; b.ls =>skip); }
                else       { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x11 ; b.lo =>skip); }
            }
            dynasm!(self.ops ; .arch aarch64 ; =>pass);
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64
                ; mov r10, QWORD [rcx] ; mov r11, QWORD [rcx + 8]);
            if is_lt {
                dynasm!(self.ops ; .arch x64 ; cmp rdx, r11 ; jg =>skip ; jl =>pass);
                if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, r10 ; jae =>skip); }
                else       { dynasm!(self.ops ; .arch x64 ; cmp rax, r10 ; ja =>skip); }
            } else {
                dynasm!(self.ops ; .arch x64 ; cmp rdx, r11 ; jl =>skip ; jg =>pass);
                if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, r10 ; jbe =>skip); }
                else       { dynasm!(self.ops ; .arch x64 ; cmp rax, r10 ; jb =>skip); }
            }
            dynasm!(self.ops ; .arch x64 ; =>pass);
        }
    }

    /// Skip when value > filter_hi (for BETWEEN upper bound).
    fn wide128_ord_skip_hi(&mut self, skip: DynamicLabel) {
        let pass = self.label();
        #[cfg(target_arch = "aarch64")]
        dynasm!(self.ops ; .arch aarch64
            ; ldp x11, x12, [x4]
            ; cmp x10, x12 ; b.lt =>pass ; b.gt =>skip
            ; cmp x9, x11 ; b.hi =>skip ; =>pass
        );
        #[cfg(target_arch = "x86_64")]
        dynasm!(self.ops ; .arch x64
            ; mov r10, QWORD [r8] ; mov r11, QWORD [r8 + 8]
            ; cmp rdx, r11 ; jl =>pass ; jg =>skip
            ; cmp rax, r10 ; ja =>skip ; =>pass
        );
    }

    /// 128-bit MIN or MAX update.
    fn int128_minmax(&mut self, set: DynamicLabel, after: DynamicLabel, is_min: bool) {
        let no_update = after;
        #[cfg(target_arch = "aarch64")]
        {
            dynasm!(self.ops ; .arch aarch64 ; cbz x23, =>set);
            // Compare value(x9,x10) vs acc(x21,x24) signed 128-bit.
            if is_min {
                // skip update if value >= acc
                dynasm!(self.ops ; .arch aarch64
                    ; cmp x10, x24 ; b.gt =>no_update ; b.lt =>set
                    ; cmp x9, x21 ; b.hs =>no_update);
            } else {
                dynasm!(self.ops ; .arch aarch64
                    ; cmp x10, x24 ; b.lt =>no_update ; b.gt =>set
                    ; cmp x9, x21 ; b.ls =>no_update);
            }
            dynasm!(self.ops ; .arch aarch64
                ; =>set ; mov x21, x9 ; mov x24, x10 ; mov x23, 1
                ; =>after ; add x22, x22, 1);
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64 ; test r15, r15 ; jz =>set);
            if is_min {
                dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp]   // acc_hi
                    ; cmp rdx, r10 ; jg =>no_update ; jl =>set
                    ; cmp rax, r13 ; jae =>no_update);
            } else {
                dynasm!(self.ops ; .arch x64
                    ; mov r10, QWORD [rsp]
                    ; cmp rdx, r10 ; jl =>no_update ; jg =>set
                    ; cmp rax, r13 ; jbe =>no_update);
            }
            dynasm!(self.ops ; .arch x64
                ; =>set ; mov r13, rax ; mov QWORD [rsp], rdx ; mov r15, 1
                ; =>after ; inc r14);
        }
    }

    // ==================================================================
    // Private helpers — 256-bit ordered comparison
    // ==================================================================

    fn wide256_ord_skip(&mut self, fop: JitFilterOp, skip: DynamicLabel, use_hi: bool) {
        // use_hi: use filter_hi (r8/x4) instead of filter_lo (rcx/x3) for BETWEEN upper bound.
        let pass = self.label();
        // Compare word-by-word from most significant (w3) to least (w0).
        // For LT: skip if NOT less; for GT: skip if NOT greater.
        let is_lt = matches!(fop, JitFilterOp::Lt | JitFilterOp::Le);
        let strict = matches!(fop, JitFilterOp::Lt | JitFilterOp::Gt);

        #[cfg(target_arch = "aarch64")]
        {
            if use_hi {
                dynasm!(self.ops ; .arch aarch64 ; ldp x13, x14, [x4] ; ldp x15, x6, [x4, 16]);
            } else {
                dynasm!(self.ops ; .arch aarch64 ; ldp x13, x14, [x3] ; ldp x15, x6, [x3, 16]);
            }
            // Compare w3 (most significant, signed)
            if is_lt {
                dynasm!(self.ops ; .arch aarch64 ; cmp x12, x6 ; b.gt =>skip ; b.lt =>pass);
            } else {
                dynasm!(self.ops ; .arch aarch64 ; cmp x12, x6 ; b.lt =>skip ; b.gt =>pass);
            }
            // w2 (unsigned)
            if is_lt {
                dynasm!(self.ops ; .arch aarch64 ; cmp x11, x15 ; b.hi =>skip ; b.lo =>pass);
            } else {
                dynasm!(self.ops ; .arch aarch64 ; cmp x11, x15 ; b.lo =>skip ; b.hi =>pass);
            }
            // w1 (unsigned)
            if is_lt {
                dynasm!(self.ops ; .arch aarch64 ; cmp x10, x14 ; b.hi =>skip ; b.lo =>pass);
            } else {
                dynasm!(self.ops ; .arch aarch64 ; cmp x10, x14 ; b.lo =>skip ; b.hi =>pass);
            }
            // w0 (unsigned, final)
            if is_lt {
                if strict { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x13 ; b.hs =>skip); }
                else       { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x13 ; b.hi =>skip); }
            } else {
                if strict { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x13 ; b.ls =>skip); }
                else       { dynasm!(self.ops ; .arch aarch64 ; cmp x9, x13 ; b.lo =>skip); }
            }
            dynasm!(self.ops ; .arch aarch64 ; =>pass);
        }
        #[cfg(target_arch = "x86_64")]
        {
            let fbase = if use_hi { 8 /*r8*/ } else { 1 /*rcx*/ };
            // Compare against memory operands. Use rcx or r8 as base.
            macro_rules! fld {
                ($off:expr) => { if use_hi { /* [r8+off] */ } else { /* [rcx+off] */ } }
            }
            // For x86_64 we compare word-by-word against memory.
            if use_hi {
                if is_lt {
                    dynasm!(self.ops ; .arch x64 ; cmp r11, QWORD [r8+24] ; jg =>skip ; jl =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp r10, QWORD [r8+16] ; ja =>skip ; jb =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp rdx, QWORD [r8+8]  ; ja =>skip ; jb =>pass);
                    if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [r8] ; jae =>skip); }
                    else       { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [r8] ; ja =>skip); }
                } else {
                    dynasm!(self.ops ; .arch x64 ; cmp r11, QWORD [r8+24] ; jl =>skip ; jg =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp r10, QWORD [r8+16] ; jb =>skip ; ja =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp rdx, QWORD [r8+8]  ; jb =>skip ; ja =>pass);
                    if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [r8] ; jbe =>skip); }
                    else       { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [r8] ; jb =>skip); }
                }
            } else {
                if is_lt {
                    dynasm!(self.ops ; .arch x64 ; cmp r11, QWORD [rcx+24] ; jg =>skip ; jl =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp r10, QWORD [rcx+16] ; ja =>skip ; jb =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp rdx, QWORD [rcx+8]  ; ja =>skip ; jb =>pass);
                    if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [rcx] ; jae =>skip); }
                    else       { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [rcx] ; ja =>skip); }
                } else {
                    dynasm!(self.ops ; .arch x64 ; cmp r11, QWORD [rcx+24] ; jl =>skip ; jg =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp r10, QWORD [rcx+16] ; jb =>skip ; ja =>pass);
                    dynasm!(self.ops ; .arch x64 ; cmp rdx, QWORD [rcx+8]  ; jb =>skip ; ja =>pass);
                    if strict { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [rcx] ; jbe =>skip); }
                    else       { dynasm!(self.ops ; .arch x64 ; cmp rax, QWORD [rcx] ; jb =>skip); }
                }
            }
            dynasm!(self.ops ; .arch x64 ; =>pass);
        }
    }

    fn int256_minmax(&mut self, set: DynamicLabel, after: DynamicLabel, is_min: bool) {
        // Compare value (x9-x12 / rax,rdx,r10,r11) vs acc (x21,x24,x25,x26 / r13,[rsp],[rsp+8],[rsp+16]).
        #[cfg(target_arch = "aarch64")]
        {
            dynasm!(self.ops ; .arch aarch64 ; cbz x23, =>set);
            // Most-significant word first (signed), then cascade unsigned.
            if is_min {
                let nu = after;
                dynasm!(self.ops ; .arch aarch64
                    ; cmp x12, x26 ; b.gt =>nu ; b.lt =>set
                    ; cmp x11, x25 ; b.hi =>nu ; b.lo =>set
                    ; cmp x10, x24 ; b.hi =>nu ; b.lo =>set
                    ; cmp x9, x21  ; b.hs =>nu
                );
            } else {
                let nu = after;
                dynasm!(self.ops ; .arch aarch64
                    ; cmp x12, x26 ; b.lt =>nu ; b.gt =>set
                    ; cmp x11, x25 ; b.lo =>nu ; b.hi =>set
                    ; cmp x10, x24 ; b.lo =>nu ; b.hi =>set
                    ; cmp x9, x21  ; b.ls =>nu
                );
            }
            dynasm!(self.ops ; .arch aarch64
                ; =>set
                ; mov x21, x9 ; mov x24, x10 ; mov x25, x11 ; mov x26, x12
                ; mov x23, 1
                ; =>after ; add x22, x22, 1
            );
        }
        #[cfg(target_arch = "x86_64")]
        {
            dynasm!(self.ops ; .arch x64 ; test r15, r15 ; jz =>set);
            // Load acc words from stack for comparison.
            // acc: r13=w0, [rsp]=w1, [rsp+8]=w2, [rsp+16]=w3
            if is_min {
                let nu = after;
                dynasm!(self.ops ; .arch x64
                    ; cmp r11, QWORD [rsp+16] ; jg =>nu ; jl =>set
                    ; cmp r10, QWORD [rsp+8]  ; ja =>nu ; jb =>set
                    ; cmp rdx, QWORD [rsp]    ; ja =>nu ; jb =>set
                    ; cmp rax, r13            ; jae =>nu
                );
            } else {
                let nu = after;
                dynasm!(self.ops ; .arch x64
                    ; cmp r11, QWORD [rsp+16] ; jl =>nu ; jg =>set
                    ; cmp r10, QWORD [rsp+8]  ; jb =>nu ; ja =>set
                    ; cmp rdx, QWORD [rsp]    ; jb =>nu ; ja =>set
                    ; cmp rax, r13            ; jbe =>nu
                );
            }
            dynasm!(self.ops ; .arch x64
                ; =>set
                ; mov r13, rax ; mov QWORD [rsp], rdx
                ; mov QWORD [rsp+8], r10 ; mov QWORD [rsp+16], r11
                ; mov r15, 1
                ; =>after ; inc r14
            );
        }
    }
}

pub(super) use dynasmrt::dynasm;
