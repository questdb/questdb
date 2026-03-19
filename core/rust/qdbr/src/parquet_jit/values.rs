//! Layer 4 (Values) — Type-dispatched operations on SQL values.

#[cfg(target_arch = "x86_64")]
use dynasmrt::DynamicLabel;

#[cfg(target_arch = "x86_64")]
use super::codegen::Codegen;
#[cfg(target_arch = "x86_64")]
use super::{JitError, JitOutputMode};
use super::{JitPhysicalType, PipelineSpec};

impl JitPhysicalType {
    pub fn is_float(self) -> bool {
        matches!(self, JitPhysicalType::Float | JitPhysicalType::Double)
    }
    pub fn is_integer(self) -> bool {
        matches!(self, JitPhysicalType::Int8 | JitPhysicalType::Int16
            | JitPhysicalType::Int32 | JitPhysicalType::Int64)
    }
    pub fn is_wide(self) -> bool {
        matches!(self, JitPhysicalType::Int128 | JitPhysicalType::Int256)
    }
}

#[cfg(target_arch = "x86_64")]
pub fn emit_load(cg: &mut Codegen, pt: JitPhysicalType) -> Result<(), JitError> {
    match pt {
        _ if pt.is_integer() => cg.load_int_value(pt),
        _ if pt.is_float() => cg.load_fp_value(pt),
        JitPhysicalType::Int128 => cg.load_int128_value(),
        JitPhysicalType::Int256 => cg.load_int256_value(),
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
pub fn emit_filter(cg: &mut Codegen, spec: &PipelineSpec, skip: DynamicLabel) -> Result<(), JitError> {
    match spec.physical_type {
        _ if spec.physical_type.is_integer() => cg.int_cmp_skip(spec.filter_op, skip),
        _ if spec.physical_type.is_float() => cg.fp_cmp_skip(spec.filter_op, skip),
        JitPhysicalType::Int128 => cg.int128_cmp_skip(spec.filter_op, skip),
        JitPhysicalType::Int256 => cg.int256_cmp_skip(spec.filter_op, skip),
        _ => unreachable!(),
    }
    Ok(())
}

/// Emit aggregate accumulation or materialization depending on output mode.
#[cfg(target_arch = "x86_64")]
pub fn emit_output(cg: &mut Codegen, spec: &PipelineSpec, set: DynamicLabel, after: DynamicLabel) -> Result<(), JitError> {
    if spec.output_mode == JitOutputMode::Materialize {
        return emit_materialize(cg, spec.physical_type);
    }
    match spec.physical_type {
        _ if spec.physical_type.is_integer() => cg.int_aggregate(spec.aggregate_op, set, after),
        _ if spec.physical_type.is_float() => cg.fp_aggregate(spec.aggregate_op, set, after),
        JitPhysicalType::Int128 => cg.int128_aggregate(spec.aggregate_op, set, after),
        JitPhysicalType::Int256 => cg.int256_aggregate(spec.aggregate_op, set, after),
        _ => unreachable!(),
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
fn emit_materialize(cg: &mut Codegen, pt: JitPhysicalType) -> Result<(), JitError> {
    match pt {
        _ if pt.is_integer() => cg.materialize_int_value(pt),
        _ if pt.is_float() => cg.materialize_fp_value(pt),
        JitPhysicalType::Int128 => cg.materialize_int128_value(),
        JitPhysicalType::Int256 => cg.materialize_int256_value(),
        _ => unreachable!(),
    }
    Ok(())
}

// =========================================================================
// IR-based helpers (portable, no architecture-specific code)
// =========================================================================

use super::ir::{CmpOp, IrType, ValRef};
use super::ir_builder::IrBuilder;

#[allow(dead_code)]
impl JitPhysicalType {
    /// IrType for the accumulator value in loop block params.
    pub fn ir_acc_type(self) -> IrType {
        match self {
            _ if self.is_float() => IrType::F64,
            JitPhysicalType::Int128 => IrType::I128,
            JitPhysicalType::Int256 => IrType::I256,
            _ => IrType::I64,
        }
    }

    /// (IrType, stride_log2) for Load instructions.
    pub fn ir_load_params(self) -> (IrType, u8) {
        match self {
            JitPhysicalType::Int8 => (IrType::I64, 0),
            JitPhysicalType::Int16 => (IrType::I64, 1),
            JitPhysicalType::Int32 => (IrType::I64, 2),
            JitPhysicalType::Int64 => (IrType::I64, 3),
            JitPhysicalType::Float => (IrType::F32, 2),
            JitPhysicalType::Double => (IrType::F64, 3),
            JitPhysicalType::Int128 => (IrType::I128, 4),
            JitPhysicalType::Int256 => (IrType::I256, 5),
        }
    }

    /// Byte size of one element.
    pub fn ir_type_size(self) -> i64 {
        match self {
            JitPhysicalType::Int8 => 1,
            JitPhysicalType::Int16 => 2,
            JitPhysicalType::Int32 | JitPhysicalType::Float => 4,
            JitPhysicalType::Int64 | JitPhysicalType::Double => 8,
            JitPhysicalType::Int128 => 16,
            JitPhysicalType::Int256 => 32,
        }
    }
}

/// Emit a typed load from `values[ctr]`.
/// For Float: loads f32 then promotes to f64.
#[allow(dead_code)]
pub fn emit_load_ir(
    b: &mut IrBuilder,
    pt: JitPhysicalType,
    values: ValRef,
    ctr: ValRef,
) -> ValRef {
    let (ty, stride) = pt.ir_load_params();
    let raw = b.load(ty, values, ctr, stride);
    if pt == JitPhysicalType::Float {
        // f32 → f64 promotion: load produces F32, promote to F64 for uniform accumulation.
        b.cvt_f32_f64(raw)
    } else {
        raw
    }
}

/// Map JitFilterOp to CmpOp (returns None for None/IsNull/IsNotNull).
#[allow(dead_code)]
fn filter_op_to_cmp(fop: super::JitFilterOp) -> Option<CmpOp> {
    use super::JitFilterOp;
    match fop {
        JitFilterOp::Eq => Some(CmpOp::Eq),
        JitFilterOp::Ne => Some(CmpOp::Ne),
        JitFilterOp::Lt => Some(CmpOp::Lt),
        JitFilterOp::Le => Some(CmpOp::Le),
        JitFilterOp::Gt => Some(CmpOp::Gt),
        JitFilterOp::Ge => Some(CmpOp::Ge),
        _ => None,
    }
}

/// Emit a branchless filter comparison.
///
/// Returns `Some(pass)` where `pass` is a Bool ValRef if the filter is active,
/// or `None` if the filter is None/IsNotNull (always passes).
///
/// For FP types: rejects NaN with `!IsNaN(val) && FCmp(op, val, threshold)`.
/// For Between: `ICmp(Ge, val, lo) && ICmp(Le, val, hi)`.
#[allow(dead_code)]
pub fn emit_filter_ir(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    val: ValRef,
    filter_lo: ValRef,
    filter_hi: ValRef,
) -> Option<ValRef> {
    use super::{JitFilterOp, JitPhysicalType};

    let pt = spec.physical_type;
    let fop = spec.filter_op;

    // For I128/I256 filters, filter_lo/hi are raw pointers (I64) that point
    // to the filter constant in memory.  We load them here (inside the loop
    // body) so the loaded values occupy scratch registers that are fresh at
    // the start of each block, avoiding cross-block clobber.
    //
    // For Between with wide types, we interleave load + compare to avoid
    // exhausting the scratch register pool: load flo, compare (Ge), then
    // load fhi, compare (Le), then And.  The backend's And handler saves
    // the first comparison result to a callee-saved register and rewinds
    // the scratch allocator before the second comparison, so each load
    // gets a fresh set of scratch registers.
    if (pt == JitPhysicalType::Int128 || pt == JitPhysicalType::Int256)
        && matches!(fop, JitFilterOp::Between)
    {
        let (load_ty, stride) = pt.ir_load_params();
        let zero_lo = b.iconst(0);
        let flo = b.load(load_ty, filter_lo, zero_lo, stride);
        let ge = if pt == JitPhysicalType::Int128 {
            b.icmp128(CmpOp::Ge, val, flo)
        } else {
            b.icmp256(CmpOp::Ge, val, flo)
        };
        let zero_hi = b.iconst(0);
        let fhi = b.load(load_ty, filter_hi, zero_hi, stride);
        let le = if pt == JitPhysicalType::Int128 {
            b.icmp128(CmpOp::Le, val, fhi)
        } else {
            b.icmp256(CmpOp::Le, val, fhi)
        };
        return Some(b.and(ge, le));
    }

    let (flo, fhi) = if pt == JitPhysicalType::Int128 || pt == JitPhysicalType::Int256 {
        match fop {
            JitFilterOp::None | JitFilterOp::IsNotNull | JitFilterOp::IsNull => {
                (filter_lo, filter_hi) // unused
            }
            _ => {
                let (load_ty, stride) = pt.ir_load_params();
                let zero = b.iconst(0);
                let lo = b.load(load_ty, filter_lo, zero, stride);
                (lo, filter_hi)
            }
        }
    } else {
        (filter_lo, filter_hi)
    };

    match fop {
        JitFilterOp::None | JitFilterOp::IsNotNull => None,
        JitFilterOp::IsNull => {
            unreachable!("emit_filter_ir called with IsNull")
        }
        JitFilterOp::Between => {
            if pt.is_float() {
                let is_nan = b.is_nan(val);
                let not_nan = b.not(is_nan);
                let ge = b.fcmp(CmpOp::Ge, val, flo);
                let le = b.fcmp(CmpOp::Le, val, fhi);
                let both = b.and(ge, le);
                Some(b.and(not_nan, both))
            } else {
                // I128/I256 Between handled above; only integer scalars here.
                let ge = b.icmp(CmpOp::Ge, val, flo);
                let le = b.icmp(CmpOp::Le, val, fhi);
                Some(b.and(ge, le))
            }
        }
        _ => {
            let cmp_op = filter_op_to_cmp(fop).unwrap();
            if pt.is_float() {
                let is_nan = b.is_nan(val);
                let not_nan = b.not(is_nan);
                let cmp = b.fcmp(cmp_op, val, flo);
                Some(b.and(not_nan, cmp))
            } else if pt == JitPhysicalType::Int128 {
                Some(b.icmp128(cmp_op, val, flo))
            } else if pt == JitPhysicalType::Int256 {
                Some(b.icmp256(cmp_op, val, flo))
            } else {
                Some(b.icmp(cmp_op, val, flo))
            }
        }
    }
}

/// Emit branchless aggregate/materialize output.
///
/// `pass` is `Some(Bool)` if there is a filter, `None` if all rows pass.
///
/// Returns `(new_acc, new_count, new_has)`.
#[allow(dead_code)]
pub fn emit_output_ir(
    b: &mut IrBuilder,
    spec: &PipelineSpec,
    val: ValRef,
    acc: ValRef,
    count: ValRef,
    has: ValRef,
    pass: Option<ValRef>,
    // Materialize-specific: cursor (pointer as I64) and type size constant.
    cursor: Option<ValRef>,
) -> (ValRef, ValRef, ValRef) {
    use super::{JitAggregateOp, JitOutputMode, JitPhysicalType};

    if spec.output_mode == JitOutputMode::Materialize {
        return emit_materialize_ir(b, spec.physical_type, val, acc, count, has, pass, cursor.unwrap());
    }

    let pt = spec.physical_type;
    let aop = spec.aggregate_op;
    let acc_ty = pt.ir_acc_type();

    match aop {
        JitAggregateOp::Count => {
            // new_count = count + 1, conditionally selected
            let inc_count = b.inc(count);
            let new_count = match pass {
                Some(p) => b.select(IrType::I64, p, inc_count, count),
                None => inc_count,
            };
            (acc, new_count, has)
        }
        JitAggregateOp::Sum => {
            let sum = if pt.is_float() {
                b.addf(acc, val)
            } else if pt == JitPhysicalType::Int128 {
                b.add128(acc, val)
            } else if pt == JitPhysicalType::Int256 {
                b.add256(acc, val)
            } else {
                b.add(acc, val)
            };
            let one = b.iconst(1);
            let inc_count = b.inc(count);
            match pass {
                Some(p) => {
                    let new_acc = b.select(acc_ty, p, sum, acc);
                    let new_count = b.select(IrType::I64, p, inc_count, count);
                    let new_has = b.select(IrType::I64, p, one, has);
                    (new_acc, new_count, new_has)
                }
                None => (sum, inc_count, one),
            }
        }
        JitAggregateOp::Min | JitAggregateOp::Max => {
            let cmp_op = if aop == JitAggregateOp::Min { CmpOp::Lt } else { CmpOp::Gt };
            let is_better = if pt.is_float() {
                b.fcmp(cmp_op, val, acc)
            } else if pt == JitPhysicalType::Int128 {
                b.icmp128(cmp_op, val, acc)
            } else if pt == JitPhysicalType::Int256 {
                b.icmp256(cmp_op, val, acc)
            } else {
                b.icmp(cmp_op, val, acc)
            };
            let is_first = b.is_zero(has);
            // Two-select min/max: first pick the better of val/acc, then
            // override with val if this is the first value (has==0).
            let better_acc = b.select(acc_ty, is_better, val, acc);
            let candidate_acc = b.select(acc_ty, is_first, val, better_acc);
            let one = b.iconst(1);
            let inc_count = b.inc(count);
            match pass {
                Some(p) => {
                    let new_acc = b.select(acc_ty, p, candidate_acc, acc);
                    let new_count = b.select(IrType::I64, p, inc_count, count);
                    let new_has = b.select(IrType::I64, p, one, has);
                    (new_acc, new_count, new_has)
                }
                None => {
                    (candidate_acc, inc_count, one)
                }
            }
        }
    }
}

/// Materialize output: store val at cursor, advance cursor, inc count.
fn emit_materialize_ir(
    b: &mut IrBuilder,
    pt: JitPhysicalType,
    val: ValRef,
    _acc: ValRef,  // unused for materialize (cursor is the "acc")
    count: ValRef,
    has: ValRef,
    pass: Option<ValRef>,
    cursor: ValRef,
) -> (ValRef, ValRef, ValRef) {
    use super::JitPhysicalType;

    // Determine store width_log2
    let width_log2: u8 = match pt {
        JitPhysicalType::Int8 => 0,
        JitPhysicalType::Int16 => 1,
        JitPhysicalType::Int32 | JitPhysicalType::Float => 2,
        JitPhysicalType::Int64 | JitPhysicalType::Double => 3,
        JitPhysicalType::Int128 => 4,
        JitPhysicalType::Int256 => 5,
    };

    // For float, we need to demote f64 back to f32 before storing.
    let store_val = if pt == JitPhysicalType::Float {
        b.cvt_f64_f32(val)
    } else {
        val
    };

    // Store val at cursor
    b.store(cursor, store_val, width_log2);

    // Advance cursor by type size
    let size = b.iconst(pt.ir_type_size());
    let new_cursor = b.add(cursor, size);
    let inc_count = b.inc(count);

    match pass {
        Some(p) => {
            let sel_cursor = b.select(IrType::I64, p, new_cursor, cursor);
            let sel_count = b.select(IrType::I64, p, inc_count, count);
            (sel_cursor, sel_count, has)
        }
        None => (new_cursor, inc_count, has),
    }
}
