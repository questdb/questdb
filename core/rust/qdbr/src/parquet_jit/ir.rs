//! IR types and data structures for the JIT pipeline.
//!
//! SSA IR with block parameters (no Phi nodes). Every instruction produces a
//! `ValRef` — a monotonically increasing index. Values are defined once, used
//! by reference. Branches pass values to the target block's parameters.

/// IR value reference — index into global value numbering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValRef(pub u32);

/// IR block reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockRef(pub u32);

/// IR value types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IrType {
    I64,
    F64,
    F32,
    Bool,
    Ptr,
    I128,
    I256,
}

/// Comparison operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// IR instruction. Each instruction in a block produces one ValRef (control
/// flow instructions produce a dummy value that is never used).
#[derive(Debug, Clone)]
pub enum Inst {
    // -- Constants & arguments --
    /// Function argument by ABI index (0..5).
    Arg(u8),
    /// 64-bit integer constant.
    IConst(i64),
    /// 64-bit float constant.
    FConst(f64),

    // -- Memory --
    /// Typed load: `*(base + index << stride_log2)`.
    /// Result type and stride_log2 together determine the memory access width
    /// and any sign-extension (see backend for details).
    Load { ty: IrType, base: ValRef, index: ValRef, stride_log2: u8 },
    /// Zero-extending byte load.
    LoadByte { base: ValRef, index: ValRef },
    /// Zero-extending u32 load at `base + index * 4` (for dictionary indices).
    LoadU32 { base: ValRef, index: ValRef },
    /// Store value at `ptr`. `width_log2` gives the store width in bytes
    /// (0=1B, 1=2B, 2=4B, 3=8B, 4=16B, 5=32B).
    Store { ptr: ValRef, val: ValRef, width_log2: u8 },
    /// Store value at `base + byte_offset`.
    StoreField { base: ValRef, offset: u16, val: ValRef },
    /// Load a value at `base + byte_offset` (for context struct field access).
    LoadField { ty: IrType, base: ValRef, offset: u16 },

    // -- Integer arithmetic --
    Add(ValRef, ValRef),
    Inc(ValRef),
    Shl(ValRef, u8),
    /// 128-bit add with carry (operands are I128).
    Add128(ValRef, ValRef),
    /// 256-bit add with carry chain (operands are I256).
    Add256(ValRef, ValRef),

    // -- Float --
    AddF(ValRef, ValRef),
    /// Reinterpret i64 bits as f64 (fmov / movq).
    BitcastToF64(ValRef),
    /// f32 → f64 promotion (only used after a stride_log2=2 float load).
    CvtF32F64(ValRef),
    /// f64 → f32 demotion (for float materialization).
    CvtF64F32(ValRef),

    // -- Comparison → Bool --
    /// Signed i64 compare.
    ICmp(CmpOp, ValRef, ValRef),
    /// Ordered f64 compare (NaN → false for all ops).
    FCmp(CmpOp, ValRef, ValRef),
    /// Signed 128-bit compare.
    ICmp128(CmpOp, ValRef, ValRef),
    /// Signed 256-bit compare.
    ICmp256(CmpOp, ValRef, ValRef),
    IsZero(ValRef),
    IsNonZero(ValRef),
    IsNaN(ValRef),
    And(ValRef, ValRef),
    Not(ValRef),

    // -- Control flow --
    /// Unconditional branch with block parameter args.
    Br { target: BlockRef, args: Vec<ValRef> },
    /// Conditional branch. If `cond` is true (non-zero), branch to
    /// `then_block` with `then_args`; otherwise fall through / branch to
    /// `else_block` with `else_args`.
    CondBr {
        cond: ValRef,
        then_block: BlockRef,
        then_args: Vec<ValRef>,
        else_block: BlockRef,
        else_args: Vec<ValRef>,
    },
    Ret(ValRef),

    // -- Select (conditional move) --
    /// Branchless ternary: `cond ? if_true : if_false`.
    Select { cond: ValRef, if_true: ValRef, if_false: ValRef },
}

// -----------------------------------------------------------------------
// Display impls
// -----------------------------------------------------------------------

impl std::fmt::Display for ValRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl std::fmt::Display for BlockRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bb{}", self.0)
    }
}

impl std::fmt::Display for IrType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IrType::I64 => write!(f, "i64"),
            IrType::F64 => write!(f, "f64"),
            IrType::F32 => write!(f, "f32"),
            IrType::Bool => write!(f, "bool"),
            IrType::Ptr => write!(f, "ptr"),
            IrType::I128 => write!(f, "i128"),
            IrType::I256 => write!(f, "i256"),
        }
    }
}

impl std::fmt::Display for CmpOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CmpOp::Eq => write!(f, "eq"),
            CmpOp::Ne => write!(f, "ne"),
            CmpOp::Lt => write!(f, "lt"),
            CmpOp::Le => write!(f, "le"),
            CmpOp::Gt => write!(f, "gt"),
            CmpOp::Ge => write!(f, "ge"),
        }
    }
}

fn fmt_args(args: &[ValRef]) -> String {
    args.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
}

impl std::fmt::Display for Inst {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Inst::Arg(n) => write!(f, "arg {n}"),
            Inst::IConst(v) => write!(f, "iconst {v}"),
            Inst::FConst(v) => write!(f, "fconst {v}"),
            Inst::Load { ty, base, index, stride_log2 } =>
                write!(f, "load.{ty} [{base} + {index} << {stride_log2}]"),
            Inst::LoadByte { base, index } =>
                write!(f, "load.u8 [{base} + {index}]"),
            Inst::LoadU32 { base, index } =>
                write!(f, "load.u32 [{base} + {index} * 4]"),
            Inst::Store { ptr, val, width_log2 } =>
                write!(f, "store.{} [{ptr}], {val}", 1u32 << width_log2),
            Inst::StoreField { base, offset, val } =>
                write!(f, "store [{base} + {offset}], {val}"),
            Inst::LoadField { ty, base, offset } =>
                write!(f, "load.{ty} [{base} + {offset}]"),
            Inst::Add(a, b) => write!(f, "add {a}, {b}"),
            Inst::Inc(v) => write!(f, "inc {v}"),
            Inst::Shl(v, n) => write!(f, "shl {v}, {n}"),
            Inst::Add128(a, b) => write!(f, "add128 {a}, {b}"),
            Inst::Add256(a, b) => write!(f, "add256 {a}, {b}"),
            Inst::AddF(a, b) => write!(f, "fadd {a}, {b}"),
            Inst::BitcastToF64(v) => write!(f, "bitcast.f64 {v}"),
            Inst::CvtF32F64(v) => write!(f, "cvt.f32.f64 {v}"),
            Inst::CvtF64F32(v) => write!(f, "cvt.f64.f32 {v}"),
            Inst::ICmp(op, a, b) => write!(f, "icmp.{op} {a}, {b}"),
            Inst::FCmp(op, a, b) => write!(f, "fcmp.{op} {a}, {b}"),
            Inst::ICmp128(op, a, b) => write!(f, "icmp128.{op} {a}, {b}"),
            Inst::ICmp256(op, a, b) => write!(f, "icmp256.{op} {a}, {b}"),
            Inst::IsZero(v) => write!(f, "is_zero {v}"),
            Inst::IsNonZero(v) => write!(f, "is_nonzero {v}"),
            Inst::IsNaN(v) => write!(f, "is_nan {v}"),
            Inst::And(a, b) => write!(f, "and {a}, {b}"),
            Inst::Not(v) => write!(f, "not {v}"),
            Inst::Br { target, args } =>
                write!(f, "br {target}({})", fmt_args(args)),
            Inst::CondBr { cond, then_block, then_args, else_block, else_args } =>
                write!(f, "condbr {cond}, {then_block}({}), {else_block}({})",
                    fmt_args(then_args), fmt_args(else_args)),
            Inst::Ret(v) => write!(f, "ret {v}"),
            Inst::Select { cond, if_true, if_false } =>
                write!(f, "select {cond}, {if_true}, {if_false}"),
        }
    }
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (val, inst) in &self.insts {
            writeln!(f, "  {val}: {ty} = {inst}",
                ty = "   ", // placeholder, type shown via val_types
            )?;
        }
        Ok(())
    }
}

impl IrFunction {
    /// Pretty-print the IR function.
    pub fn display(&self) -> String {
        let mut s = String::new();
        for (i, block) in self.blocks.iter().enumerate() {
            let params: Vec<String> = block.params.iter()
                .map(|(v, ty)| format!("{v}: {ty}"))
                .collect();
            s.push_str(&format!("bb{i}({}):\n", params.join(", ")));
            for (val, inst) in &block.insts {
                let ty = &self.val_types[val.0 as usize];
                s.push_str(&format!("  {val}: {ty} = {inst}\n"));
            }
            s.push('\n');
        }
        s
    }
}

/// A basic block with parameters and a sequence of instructions.
#[derive(Debug)]
pub struct Block {
    pub params: Vec<(ValRef, IrType)>,
    pub insts: Vec<(ValRef, Inst)>,
}

/// Complete IR function.
#[derive(Debug)]
pub struct IrFunction {
    pub blocks: Vec<Block>,
    pub entry: BlockRef,
    pub val_types: Vec<IrType>,
}
