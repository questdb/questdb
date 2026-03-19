//! Builder API for constructing IR functions.

use super::ir::*;

pub struct IrBuilder {
    blocks: Vec<Block>,
    val_types: Vec<IrType>,
    next_val: u32,
    current_block: Option<BlockRef>,
}

impl IrBuilder {
    pub fn new() -> Self {
        IrBuilder {
            blocks: Vec::new(),
            val_types: Vec::new(),
            next_val: 0,
            current_block: None,
        }
    }

    fn alloc_val(&mut self, ty: IrType) -> ValRef {
        let v = ValRef(self.next_val);
        self.next_val += 1;
        self.val_types.push(ty);
        v
    }

    fn emit(&mut self, ty: IrType, inst: Inst) -> ValRef {
        let v = self.alloc_val(ty);
        let block = self.current_block.expect("no current block");
        self.blocks[block.0 as usize].insts.push((v, inst));
        v
    }

    fn emit_void(&mut self, inst: Inst) {
        let v = self.alloc_val(IrType::I64); // dummy type for void instructions
        let block = self.current_block.expect("no current block");
        self.blocks[block.0 as usize].insts.push((v, inst));
    }

    // ----------------------------------------------------------------
    // Block management
    // ----------------------------------------------------------------

    /// Create a new block with the given parameter types.
    pub fn create_block(&mut self, param_types: &[IrType]) -> BlockRef {
        let br = BlockRef(self.blocks.len() as u32);
        let mut params = Vec::with_capacity(param_types.len());
        for &ty in param_types {
            let v = self.alloc_val(ty);
            params.push((v, ty));
        }
        self.blocks.push(Block {
            params,
            insts: Vec::new(),
        });
        br
    }

    /// Switch to emitting instructions in the given block.
    pub fn switch_to(&mut self, block: BlockRef) {
        self.current_block = Some(block);
    }

    /// Get block parameter by index.
    pub fn block_param(&self, block: BlockRef, index: u8) -> ValRef {
        self.blocks[block.0 as usize].params[index as usize].0
    }

    /// Get current block's parameter by index.
    pub fn param(&self, index: u8) -> ValRef {
        let block = self.current_block.expect("no current block");
        self.blocks[block.0 as usize].params[index as usize].0
    }

    pub fn params_2(&self) -> (ValRef, ValRef) {
        (self.param(0), self.param(1))
    }

    pub fn params_3(&self) -> (ValRef, ValRef, ValRef) {
        (self.param(0), self.param(1), self.param(2))
    }

    pub fn params_4(&self) -> (ValRef, ValRef, ValRef, ValRef) {
        (self.param(0), self.param(1), self.param(2), self.param(3))
    }

    pub fn params_5(&self) -> (ValRef, ValRef, ValRef, ValRef, ValRef) {
        (self.param(0), self.param(1), self.param(2), self.param(3), self.param(4))
    }

    #[allow(clippy::type_complexity)]
    pub fn params_7(
        &self,
    ) -> (ValRef, ValRef, ValRef, ValRef, ValRef, ValRef, ValRef) {
        (
            self.param(0),
            self.param(1),
            self.param(2),
            self.param(3),
            self.param(4),
            self.param(5),
            self.param(6),
        )
    }

    // ----------------------------------------------------------------
    // Constants & arguments
    // ----------------------------------------------------------------

    pub fn arg(&mut self, index: u8, ty: IrType) -> ValRef {
        self.emit(ty, Inst::Arg(index))
    }

    pub fn iconst(&mut self, val: i64) -> ValRef {
        self.emit(IrType::I64, Inst::IConst(val))
    }

    pub fn fconst(&mut self, val: f64) -> ValRef {
        self.emit(IrType::F64, Inst::FConst(val))
    }

    // ----------------------------------------------------------------
    // Memory
    // ----------------------------------------------------------------

    pub fn load(&mut self, ty: IrType, base: ValRef, index: ValRef, stride_log2: u8) -> ValRef {
        self.emit(ty, Inst::Load { ty, base, index, stride_log2 })
    }

    pub fn load_byte(&mut self, base: ValRef, index: ValRef) -> ValRef {
        self.emit(IrType::I64, Inst::LoadByte { base, index })
    }

    pub fn load_u32(&mut self, base: ValRef, index: ValRef) -> ValRef {
        self.emit(IrType::I64, Inst::LoadU32 { base, index })
    }

    pub fn store(&mut self, ptr: ValRef, val: ValRef, width_log2: u8) {
        self.emit_void(Inst::Store { ptr, val, width_log2 });
    }

    pub fn store_field(&mut self, base: ValRef, offset: u16, val: ValRef) {
        self.emit_void(Inst::StoreField { base, offset, val });
    }

    pub fn load_field(&mut self, ty: IrType, base: ValRef, offset: u16) -> ValRef {
        self.emit(ty, Inst::LoadField { ty, base, offset })
    }

    // ----------------------------------------------------------------
    // Integer arithmetic
    // ----------------------------------------------------------------

    pub fn add(&mut self, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::I64, Inst::Add(a, b))
    }

    pub fn inc(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::I64, Inst::Inc(a))
    }

    pub fn shl(&mut self, a: ValRef, imm: u8) -> ValRef {
        self.emit(IrType::I64, Inst::Shl(a, imm))
    }

    pub fn add128(&mut self, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::I128, Inst::Add128(a, b))
    }

    pub fn add256(&mut self, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::I256, Inst::Add256(a, b))
    }

    // ----------------------------------------------------------------
    // Float
    // ----------------------------------------------------------------

    pub fn addf(&mut self, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::F64, Inst::AddF(a, b))
    }

    pub fn bitcast_to_f64(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::F64, Inst::BitcastToF64(a))
    }

    pub fn cvt_f32_f64(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::F64, Inst::CvtF32F64(a))
    }

    pub fn cvt_f64_f32(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::F64, Inst::CvtF64F32(a))
    }

    // ----------------------------------------------------------------
    // Comparison → Bool
    // ----------------------------------------------------------------

    pub fn icmp(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::ICmp(op, a, b))
    }

    pub fn fcmp(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::FCmp(op, a, b))
    }

    pub fn icmp128(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::ICmp128(op, a, b))
    }

    pub fn icmp256(&mut self, op: CmpOp, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::ICmp256(op, a, b))
    }

    pub fn is_zero(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::IsZero(a))
    }

    pub fn is_nonzero(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::IsNonZero(a))
    }

    pub fn is_nan(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::IsNaN(a))
    }

    pub fn and(&mut self, a: ValRef, b: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::And(a, b))
    }

    pub fn not(&mut self, a: ValRef) -> ValRef {
        self.emit(IrType::Bool, Inst::Not(a))
    }

    // ----------------------------------------------------------------
    // Control flow
    // ----------------------------------------------------------------

    pub fn br(&mut self, target: BlockRef, args: &[ValRef]) {
        self.emit_void(Inst::Br { target, args: args.to_vec() });
    }

    pub fn cond_br(
        &mut self,
        cond: ValRef,
        then_block: BlockRef,
        then_args: &[ValRef],
        else_block: BlockRef,
        else_args: &[ValRef],
    ) {
        self.emit_void(Inst::CondBr {
            cond,
            then_block,
            then_args: then_args.to_vec(),
            else_block,
            else_args: else_args.to_vec(),
        });
    }

    pub fn ret(&mut self, val: ValRef) {
        self.emit_void(Inst::Ret(val));
    }

    // ----------------------------------------------------------------
    // Select (conditional move)
    // ----------------------------------------------------------------

    pub fn select(&mut self, ty: IrType, cond: ValRef, if_true: ValRef, if_false: ValRef) -> ValRef {
        self.emit(ty, Inst::Select { cond, if_true, if_false })
    }

    // ----------------------------------------------------------------
    // Finalize
    // ----------------------------------------------------------------

    pub fn finish(self) -> IrFunction {
        IrFunction {
            blocks: self.blocks,
            entry: BlockRef(0),
            val_types: self.val_types,
        }
    }
}
