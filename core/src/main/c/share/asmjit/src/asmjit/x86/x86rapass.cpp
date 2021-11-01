// AsmJit - Machine code generation for C++
//
//  * Official AsmJit Home Page: https://asmjit.com
//  * Official Github Repository: https://github.com/asmjit/asmjit
//
// Copyright (c) 2008-2020 The AsmJit Authors
//
// This software is provided 'as-is', without any express or implied
// warranty. In no event will the authors be held liable for any damages
// arising from the use of this software.
//
// Permission is granted to anyone to use this software for any purpose,
// including commercial applications, and to alter it and redistribute it
// freely, subject to the following restrictions:
//
// 1. The origin of this software must not be misrepresented; you must not
//    claim that you wrote the original software. If you use this software
//    in a product, an acknowledgment in the product documentation would be
//    appreciated but is not required.
// 2. Altered source versions must be plainly marked as such, and must not be
//    misrepresented as being the original software.
// 3. This notice may not be removed or altered from any source distribution.

#include "../core/api-build_p.h"
#if !defined(ASMJIT_NO_X86) && !defined(ASMJIT_NO_COMPILER)

#include "../core/cpuinfo.h"
#include "../core/support.h"
#include "../core/type.h"
#include "../x86/x86assembler.h"
#include "../x86/x86compiler.h"
#include "../x86/x86instapi_p.h"
#include "../x86/x86instdb_p.h"
#include "../x86/x86emithelper_p.h"
#include "../x86/x86rapass_p.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [asmjit::x86::X86RAPass - Helpers]
// ============================================================================

static ASMJIT_INLINE uint64_t raImmMaskFromSize(uint32_t size) noexcept {
  ASMJIT_ASSERT(size > 0 && size < 256);
  static const uint64_t masks[] = {
    0x00000000000000FFu, //   1
    0x000000000000FFFFu, //   2
    0x00000000FFFFFFFFu, //   4
    0xFFFFFFFFFFFFFFFFu, //   8
    0x0000000000000000u, //  16
    0x0000000000000000u, //  32
    0x0000000000000000u, //  64
    0x0000000000000000u, // 128
    0x0000000000000000u  // 256
  };
  return masks[Support::ctz(size)];
}

static ASMJIT_INLINE uint32_t raUseOutFlagsFromRWFlags(uint32_t rwFlags) noexcept {
  static const uint32_t map[] = {
    0,
    RATiedReg::kRead  | RATiedReg::kUse,                     // kRead
    RATiedReg::kWrite | RATiedReg::kOut,                     // kWrite
    RATiedReg::kRW    | RATiedReg::kUse,                     // kRW
    0,
    RATiedReg::kRead  | RATiedReg::kUse | RATiedReg::kUseRM, // kRead  | kRegMem
    RATiedReg::kWrite | RATiedReg::kOut | RATiedReg::kOutRM, // kWrite | kRegMem
    RATiedReg::kRW    | RATiedReg::kUse | RATiedReg::kUseRM  // kRW    | kRegMem
  };

  return map[rwFlags & (OpRWInfo::kRW | OpRWInfo::kRegMem)];
}

static ASMJIT_INLINE uint32_t raRegRwFlags(uint32_t flags) noexcept {
  return raUseOutFlagsFromRWFlags(flags);
}

static ASMJIT_INLINE uint32_t raMemBaseRwFlags(uint32_t flags) noexcept {
  constexpr uint32_t shift = Support::constCtz(OpRWInfo::kMemBaseRW);
  return raUseOutFlagsFromRWFlags((flags >> shift) & OpRWInfo::kRW);
}

static ASMJIT_INLINE uint32_t raMemIndexRwFlags(uint32_t flags) noexcept {
  constexpr uint32_t shift = Support::constCtz(OpRWInfo::kMemIndexRW);
  return raUseOutFlagsFromRWFlags((flags >> shift) & OpRWInfo::kRW);
}

// ============================================================================
// [asmjit::x86::RACFGBuilder]
// ============================================================================

class RACFGBuilder : public RACFGBuilderT<RACFGBuilder> {
public:
  uint32_t _arch;
  bool _is64Bit;
  bool _avxEnabled;

  inline RACFGBuilder(X86RAPass* pass) noexcept
    : RACFGBuilderT<RACFGBuilder>(pass),
      _arch(pass->cc()->arch()),
      _is64Bit(pass->registerSize() == 8),
      _avxEnabled(pass->avxEnabled()) {
  }

  inline Compiler* cc() const noexcept { return static_cast<Compiler*>(_cc); }

  inline uint32_t choose(uint32_t sseInst, uint32_t avxInst) const noexcept {
    return _avxEnabled ? avxInst : sseInst;
  }

  Error onInst(InstNode* inst, uint32_t& controlType, RAInstBuilder& ib) noexcept;

  Error onBeforeInvoke(InvokeNode* invokeNode) noexcept;
  Error onInvoke(InvokeNode* invokeNode, RAInstBuilder& ib) noexcept;

  Error moveVecToPtr(InvokeNode* invokeNode, const FuncValue& arg, const Vec& src, BaseReg* out) noexcept;
  Error moveImmToRegArg(InvokeNode* invokeNode, const FuncValue& arg, const Imm& imm_, BaseReg* out) noexcept;
  Error moveImmToStackArg(InvokeNode* invokeNode, const FuncValue& arg, const Imm& imm_) noexcept;
  Error moveRegToStackArg(InvokeNode* invokeNode, const FuncValue& arg, const BaseReg& reg) noexcept;

  Error onBeforeRet(FuncRetNode* funcRet) noexcept;
  Error onRet(FuncRetNode* funcRet, RAInstBuilder& ib) noexcept;
};

// ============================================================================
// [asmjit::x86::RACFGBuilder - OnInst]
// ============================================================================

Error RACFGBuilder::onInst(InstNode* inst, uint32_t& controlType, RAInstBuilder& ib) noexcept {
  InstRWInfo rwInfo;

  uint32_t instId = inst->id();
  if (Inst::isDefinedId(instId)) {
    uint32_t opCount = inst->opCount();
    const Operand* opArray = inst->operands();
    ASMJIT_PROPAGATE(InstInternal::queryRWInfo(_arch, inst->baseInst(), opArray, opCount, &rwInfo));

    const InstDB::InstInfo& instInfo = InstDB::infoById(instId);
    bool hasGpbHiConstraint = false;
    uint32_t singleRegOps = 0;

    if (opCount) {
      // The mask is for all registers, but we are mostly interested in AVX-512
      // registers at the moment. The mask will be combined with all available
      // registers of the Compiler at the end so we it never use more registers
      // than available.
      uint32_t instructionAllowedRegs = 0xFFFFFFFFu;

      if (instInfo.isEvex()) {
        // EVEX instruction and VEX instructions that can be encoded with EVEX
        // have the possibility to use 32 SIMD registers (XMM/YMM/ZMM).
        if (instInfo.isVex() && !instInfo.isEvexCompatible()) {
          if (instInfo.isEvexKRegOnly()) {
            // EVEX encodable only if the first operand is K register (compare instructions).
            if (!Reg::isKReg(opArray[0]))
              instructionAllowedRegs = 0xFFFFu;
          }
          else if (instInfo.isEvexTwoOpOnly()) {
            // EVEX encodable only if the instruction has two operands (gather instructions).
            if (opCount != 2)
              instructionAllowedRegs = 0xFFFFu;
          }
          else {
            instructionAllowedRegs = 0xFFFFu;
          }
        }
      }
      else if (instInfo.isEvexTransformable()) {
        ib.addAggregatedFlags(RAInst::kFlagIsTransformable);
      }
      else {
        // Not EVEX, restrict everything to [0-15] registers.
        instructionAllowedRegs = 0xFFFFu;
      }

      for (uint32_t i = 0; i < opCount; i++) {
        const Operand& op = opArray[i];
        const OpRWInfo& opRwInfo = rwInfo.operand(i);

        if (op.isReg()) {
          // Register Operand
          // ----------------
          const Reg& reg = op.as<Reg>();

          uint32_t flags = raRegRwFlags(opRwInfo.opFlags());
          uint32_t allowedRegs = instructionAllowedRegs;

          // X86-specific constraints related to LO|HI general purpose registers.
          // This is only required when the register is part of the encoding. If
          // the register is fixed we won't restrict anything as it doesn't restrict
          // encoding of other registers.
          if (reg.isGpb() && !(opRwInfo.opFlags() & OpRWInfo::kRegPhysId)) {
            flags |= RATiedReg::kX86Gpb;
            if (!_is64Bit) {
              // Restrict to first four - AL|AH|BL|BH|CL|CH|DL|DH. In 32-bit mode
              // it's not possible to access SIL|DIL, etc, so this is just enough.
              allowedRegs = 0x0Fu;
            }
            else {
              // If we encountered GPB-HI register the situation is much more
              // complicated than in 32-bit mode. We need to patch all registers
              // to not use ID higher than 7 and all GPB-LO registers to not use
              // index higher than 3. Instead of doing the patching here we just
              // set a flag and will do it later, to not complicate this loop.
              if (reg.isGpbHi()) {
                hasGpbHiConstraint = true;
                allowedRegs = 0x0Fu;
              }
            }
          }

          uint32_t vIndex = Operand::virtIdToIndex(reg.id());
          if (vIndex < Operand::kVirtIdCount) {
            RAWorkReg* workReg;
            ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

            // Use RW instead of Write in case that not the whole register is
            // overwritten. This is important for liveness as we cannot kill a
            // register that will be used. For example `mov al, 0xFF` is not a
            // write-only operation if user allocated the whole `rax` register.
            if ((flags & RATiedReg::kRW) == RATiedReg::kWrite) {
              if (workReg->regByteMask() & ~(opRwInfo.writeByteMask() | opRwInfo.extendByteMask())) {
                // Not write-only operation.
                flags = (flags & ~RATiedReg::kOut) | (RATiedReg::kRead | RATiedReg::kUse);
              }
            }

            // Do not use RegMem flag if changing Reg to Mem requires additional
            // CPU feature that may not be enabled.
            if (rwInfo.rmFeature() && (flags & (RATiedReg::kUseRM | RATiedReg::kOutRM))) {
              flags &= ~(RATiedReg::kUseRM | RATiedReg::kOutRM);
            }

            uint32_t group = workReg->group();
            uint32_t allocable = _pass->_availableRegs[group] & allowedRegs;

            uint32_t useId = BaseReg::kIdBad;
            uint32_t outId = BaseReg::kIdBad;

            uint32_t useRewriteMask = 0;
            uint32_t outRewriteMask = 0;

            if (flags & RATiedReg::kUse) {
              useRewriteMask = Support::bitMask(inst->getRewriteIndex(&reg._baseId));
              if (opRwInfo.opFlags() & OpRWInfo::kRegPhysId) {
                useId = opRwInfo.physId();
                flags |= RATiedReg::kUseFixed;
              }
            }
            else {
              outRewriteMask = Support::bitMask(inst->getRewriteIndex(&reg._baseId));
              if (opRwInfo.opFlags() & OpRWInfo::kRegPhysId) {
                outId = opRwInfo.physId();
                flags |= RATiedReg::kOutFixed;
              }
            }

            ASMJIT_PROPAGATE(ib.add(workReg, flags, allocable, useId, useRewriteMask, outId, outRewriteMask, opRwInfo.rmSize()));
            if (singleRegOps == i)
              singleRegOps++;
          }
        }
        else if (op.isMem()) {
          // Memory Operand
          // --------------
          const Mem& mem = op.as<Mem>();
          ib.addForbiddenFlags(RATiedReg::kUseRM | RATiedReg::kOutRM);

          if (mem.isRegHome()) {
            RAWorkReg* workReg;
            ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(Operand::virtIdToIndex(mem.baseId()), &workReg));
            _pass->getOrCreateStackSlot(workReg);
          }
          else if (mem.hasBaseReg()) {
            uint32_t vIndex = Operand::virtIdToIndex(mem.baseId());
            if (vIndex < Operand::kVirtIdCount) {
              RAWorkReg* workReg;
              ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

              uint32_t flags = raMemBaseRwFlags(opRwInfo.opFlags());
              uint32_t group = workReg->group();
              uint32_t allocable = _pass->_availableRegs[group];

              uint32_t useId = BaseReg::kIdBad;
              uint32_t outId = BaseReg::kIdBad;

              uint32_t useRewriteMask = 0;
              uint32_t outRewriteMask = 0;

              if (flags & RATiedReg::kUse) {
                useRewriteMask = Support::bitMask(inst->getRewriteIndex(&mem._baseId));
                if (opRwInfo.opFlags() & OpRWInfo::kMemPhysId) {
                  useId = opRwInfo.physId();
                  flags |= RATiedReg::kUseFixed;
                }
              }
              else {
                outRewriteMask = Support::bitMask(inst->getRewriteIndex(&mem._baseId));
                if (opRwInfo.opFlags() & OpRWInfo::kMemPhysId) {
                  outId = opRwInfo.physId();
                  flags |= RATiedReg::kOutFixed;
                }
              }

              ASMJIT_PROPAGATE(ib.add(workReg, flags, allocable, useId, useRewriteMask, outId, outRewriteMask));
            }
          }

          if (mem.hasIndexReg()) {
            uint32_t vIndex = Operand::virtIdToIndex(mem.indexId());
            if (vIndex < Operand::kVirtIdCount) {
              RAWorkReg* workReg;
              ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

              uint32_t flags = raMemIndexRwFlags(opRwInfo.opFlags());
              uint32_t group = workReg->group();
              uint32_t allocable = _pass->_availableRegs[group] & instructionAllowedRegs;

              // Index registers have never fixed id on X86/x64.
              const uint32_t useId = BaseReg::kIdBad;
              const uint32_t outId = BaseReg::kIdBad;

              uint32_t useRewriteMask = 0;
              uint32_t outRewriteMask = 0;

              if (flags & RATiedReg::kUse)
                useRewriteMask = Support::bitMask(inst->getRewriteIndex(&mem._data[Operand::kDataMemIndexId]));
              else
                outRewriteMask = Support::bitMask(inst->getRewriteIndex(&mem._data[Operand::kDataMemIndexId]));

              ASMJIT_PROPAGATE(ib.add(workReg, RATiedReg::kUse | RATiedReg::kRead, allocable, useId, useRewriteMask, outId, outRewriteMask));
            }
          }
        }
      }
    }

    // Handle extra operand (either REP {cx|ecx|rcx} or AVX-512 {k} selector).
    if (inst->hasExtraReg()) {
      uint32_t vIndex = Operand::virtIdToIndex(inst->extraReg().id());
      if (vIndex < Operand::kVirtIdCount) {
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

        uint32_t group = workReg->group();
        uint32_t rewriteMask = Support::bitMask(inst->getRewriteIndex(&inst->extraReg()._id));

        if (group == Gp::kGroupKReg) {
          // AVX-512 mask selector {k} register - read-only, allocable to any register except {k0}.
          uint32_t allocableRegs = _pass->_availableRegs[group];
          ASMJIT_PROPAGATE(ib.add(workReg, RATiedReg::kUse | RATiedReg::kRead, allocableRegs, BaseReg::kIdBad, rewriteMask, BaseReg::kIdBad, 0));
          singleRegOps = 0;
        }
        else {
          // REP {cx|ecx|rcx} register - read & write, allocable to {cx|ecx|rcx} only.
          ASMJIT_PROPAGATE(ib.add(workReg, RATiedReg::kUse | RATiedReg::kRW, 0, Gp::kIdCx, rewriteMask, Gp::kIdBad, 0));
        }
      }
      else {
        uint32_t group = inst->extraReg().group();
        if (group == Gp::kGroupKReg && inst->extraReg().id() != 0)
          singleRegOps = 0;
      }
    }

    // Handle X86 constraints.
    if (hasGpbHiConstraint) {
      for (RATiedReg& tiedReg : ib) {
        tiedReg._allocableRegs &= tiedReg.hasFlag(RATiedReg::kX86Gpb) ? 0x0Fu : 0xFFu;
      }
    }

    if (ib.tiedRegCount() == 1) {
      // Handle special cases of some instructions where all operands share the same
      // register. In such case the single operand becomes read-only or write-only.
      uint32_t singleRegCase = InstDB::kSingleRegNone;
      if (singleRegOps == opCount) {
        singleRegCase = instInfo.singleRegCase();
      }
      else if (opCount == 2 && inst->op(1).isImm()) {
        // Handle some tricks used by X86 asm.
        const BaseReg& reg = inst->op(0).as<BaseReg>();
        const Imm& imm = inst->op(1).as<Imm>();

        const RAWorkReg* workReg = _pass->workRegById(ib[0]->workId());
        uint32_t workRegSize = workReg->info().size();

        switch (inst->id()) {
          case Inst::kIdOr: {
            // Sets the value of the destination register to -1, previous content unused.
            if (reg.size() >= 4 || reg.size() >= workRegSize) {
              if (imm.value() == -1 || imm.valueAs<uint64_t>() == raImmMaskFromSize(reg.size()))
                singleRegCase = InstDB::kSingleRegWO;
            }
            ASMJIT_FALLTHROUGH;
          }

          case Inst::kIdAdd:
          case Inst::kIdAnd:
          case Inst::kIdRol:
          case Inst::kIdRor:
          case Inst::kIdSar:
          case Inst::kIdShl:
          case Inst::kIdShr:
          case Inst::kIdSub:
          case Inst::kIdXor: {
            // Updates [E|R]FLAGS without changing the content.
            if (reg.size() != 4 || reg.size() >= workRegSize) {
              if (imm.value() == 0)
                singleRegCase = InstDB::kSingleRegRO;
            }
            break;
          }
        }
      }

      switch (singleRegCase) {
        case InstDB::kSingleRegNone:
          break;
        case InstDB::kSingleRegRO:
          ib[0]->makeReadOnly();
          break;
        case InstDB::kSingleRegWO:
          ib[0]->makeWriteOnly();
          break;
      }
    }

    controlType = instInfo.controlType();
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - OnInvoke]
// ============================================================================

Error RACFGBuilder::onBeforeInvoke(InvokeNode* invokeNode) noexcept {
  const FuncDetail& fd = invokeNode->detail();
  uint32_t argCount = invokeNode->argCount();

  cc()->_setCursor(invokeNode->prev());
  uint32_t nativeRegType = cc()->_gpRegInfo.type();

  for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
    const FuncValuePack& argPack = fd.argPack(argIndex);
    for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
      if (!argPack[valueIndex])
        break;

      const FuncValue& arg = argPack[valueIndex];
      const Operand& op = invokeNode->arg(argIndex, valueIndex);

      if (op.isNone())
        continue;

      if (op.isReg()) {
        const Reg& reg = op.as<Reg>();
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(Operand::virtIdToIndex(reg.id()), &workReg));

        if (arg.isReg()) {
          uint32_t regGroup = workReg->group();
          uint32_t argGroup = Reg::groupOf(arg.regType());

          if (arg.isIndirect()) {
            if (reg.isGp()) {
              if (reg.type() != nativeRegType)
                return DebugUtils::errored(kErrorInvalidAssignment);
              // It's considered allocated if this is an indirect argument and the user used GP.
              continue;
            }

            BaseReg indirectReg;
            moveVecToPtr(invokeNode, arg, reg.as<Vec>(), &indirectReg);
            invokeNode->_args[argIndex][valueIndex] = indirectReg;
          }
          else {
            if (regGroup != argGroup) {
              // TODO: Conversion is not supported.
              return DebugUtils::errored(kErrorInvalidAssignment);
            }
          }
        }
        else {
          if (arg.isIndirect()) {
            if (reg.isGp()) {
              if (reg.type() != nativeRegType)
                return DebugUtils::errored(kErrorInvalidAssignment);

              ASMJIT_PROPAGATE(moveRegToStackArg(invokeNode, arg, reg));
              continue;
            }

            BaseReg indirectReg;
            moveVecToPtr(invokeNode, arg, reg.as<Vec>(), &indirectReg);
            ASMJIT_PROPAGATE(moveRegToStackArg(invokeNode, arg, indirectReg));
          }
          else {
            ASMJIT_PROPAGATE(moveRegToStackArg(invokeNode, arg, reg));
          }
        }
      }
      else if (op.isImm()) {
        if (arg.isReg()) {
          BaseReg reg;
          ASMJIT_PROPAGATE(moveImmToRegArg(invokeNode, arg, op.as<Imm>(), &reg));
          invokeNode->_args[argIndex][valueIndex] = reg;
        }
        else {
          ASMJIT_PROPAGATE(moveImmToStackArg(invokeNode, arg, op.as<Imm>()));
        }
      }
    }
  }

  cc()->_setCursor(invokeNode);
  if (fd.hasFlag(CallConv::kFlagCalleePopsStack) && fd.argStackSize() != 0)
    ASMJIT_PROPAGATE(cc()->sub(cc()->zsp(), fd.argStackSize()));

  if (fd.hasRet()) {
    for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
      const FuncValue& ret = fd.ret(valueIndex);
      if (!ret)
        break;

      const Operand& op = invokeNode->ret(valueIndex);
      if (op.isReg()) {
        const Reg& reg = op.as<Reg>();
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(Operand::virtIdToIndex(reg.id()), &workReg));

        if (ret.isReg()) {
          if (ret.regType() == Reg::kTypeSt) {
            if (workReg->group() != Reg::kGroupVec)
              return DebugUtils::errored(kErrorInvalidAssignment);

            Reg dst = Reg::fromSignatureAndId(workReg->signature(), workReg->virtId());
            Mem mem;

            uint32_t typeId = Type::baseOf(workReg->typeId());
            if (ret.hasTypeId())
              typeId = ret.typeId();

            switch (typeId) {
              case Type::kIdF32:
                ASMJIT_PROPAGATE(_pass->useTemporaryMem(mem, 4, 4));
                mem.setSize(4);
                ASMJIT_PROPAGATE(cc()->fstp(mem));
                ASMJIT_PROPAGATE(cc()->emit(choose(Inst::kIdMovss, Inst::kIdVmovss), dst.as<Xmm>(), mem));
                break;

              case Type::kIdF64:
                ASMJIT_PROPAGATE(_pass->useTemporaryMem(mem, 8, 4));
                mem.setSize(8);
                ASMJIT_PROPAGATE(cc()->fstp(mem));
                ASMJIT_PROPAGATE(cc()->emit(choose(Inst::kIdMovsd, Inst::kIdVmovsd), dst.as<Xmm>(), mem));
                break;

              default:
                return DebugUtils::errored(kErrorInvalidAssignment);
            }
          }
          else {
            uint32_t regGroup = workReg->group();
            uint32_t retGroup = Reg::groupOf(ret.regType());

            if (regGroup != retGroup) {
              // TODO: Conversion is not supported.
              return DebugUtils::errored(kErrorInvalidAssignment);
            }
          }
        }
      }
    }
  }

  // This block has function call(s).
  _curBlock->addFlags(RABlock::kFlagHasFuncCalls);
  _pass->func()->frame().addAttributes(FuncFrame::kAttrHasFuncCalls);
  _pass->func()->frame().updateCallStackSize(fd.argStackSize());

  return kErrorOk;
}

Error RACFGBuilder::onInvoke(InvokeNode* invokeNode, RAInstBuilder& ib) noexcept {
  uint32_t argCount = invokeNode->argCount();
  const FuncDetail& fd = invokeNode->detail();

  for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
    const FuncValuePack& argPack = fd.argPack(argIndex);
    for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
      if (!argPack[valueIndex])
        continue;

      const FuncValue& arg = argPack[valueIndex];
      const Operand& op = invokeNode->arg(argIndex, valueIndex);

      if (op.isNone())
        continue;

      if (op.isReg()) {
        const Reg& reg = op.as<Reg>();
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(Operand::virtIdToIndex(reg.id()), &workReg));

        if (arg.isIndirect()) {
          uint32_t regGroup = workReg->group();
          if (regGroup != BaseReg::kGroupGp)
            return DebugUtils::errored(kErrorInvalidState);
          ASMJIT_PROPAGATE(ib.addCallArg(workReg, arg.regId()));
        }
        else if (arg.isReg()) {
          uint32_t regGroup = workReg->group();
          uint32_t argGroup = Reg::groupOf(arg.regType());

          if (regGroup == argGroup) {
            ASMJIT_PROPAGATE(ib.addCallArg(workReg, arg.regId()));
          }
        }
      }
    }
  }

  for (uint32_t retIndex = 0; retIndex < Globals::kMaxValuePack; retIndex++) {
    const FuncValue& ret = fd.ret(retIndex);
    if (!ret)
      break;

    // Not handled here...
    const Operand& op = invokeNode->ret(retIndex);
    if (ret.regType() == Reg::kTypeSt)
      continue;

    if (op.isReg()) {
      const Reg& reg = op.as<Reg>();
      RAWorkReg* workReg;
      ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(Operand::virtIdToIndex(reg.id()), &workReg));

      if (ret.isReg()) {
        uint32_t regGroup = workReg->group();
        uint32_t retGroup = Reg::groupOf(ret.regType());

        if (regGroup == retGroup) {
          ASMJIT_PROPAGATE(ib.addCallRet(workReg, ret.regId()));
        }
      }
      else {
        return DebugUtils::errored(kErrorInvalidAssignment);
      }
    }
  }

  // Setup clobbered registers.
  ib._clobbered[0] = Support::lsbMask<uint32_t>(_pass->_physRegCount[0]) & ~fd.preservedRegs(0);
  ib._clobbered[1] = Support::lsbMask<uint32_t>(_pass->_physRegCount[1]) & ~fd.preservedRegs(1);
  ib._clobbered[2] = Support::lsbMask<uint32_t>(_pass->_physRegCount[2]) & ~fd.preservedRegs(2);
  ib._clobbered[3] = Support::lsbMask<uint32_t>(_pass->_physRegCount[3]) & ~fd.preservedRegs(3);

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - MoveVecToPtr]
// ============================================================================

static uint32_t x86VecRegSignatureBySize(uint32_t size) noexcept {
  if (size >= 64)
    return Zmm::kSignature;
  else if (size >= 32)
    return Ymm::kSignature;
  else
    return Xmm::kSignature;
}

Error RACFGBuilder::moveVecToPtr(InvokeNode* invokeNode, const FuncValue& arg, const Vec& src, BaseReg* out) noexcept {
  DebugUtils::unused(invokeNode);
  ASMJIT_ASSERT(arg.isReg());

  uint32_t argSize = Type::sizeOf(arg.typeId());
  if (argSize == 0)
    return DebugUtils::errored(kErrorInvalidState);

  if (argSize < 16)
    argSize = 16;

  uint32_t argStackOffset = Support::alignUp(invokeNode->detail()._argStackSize, argSize);
  _funcNode->frame().updateCallStackAlignment(argSize);
  invokeNode->detail()._argStackSize = argStackOffset + argSize;

  Vec vecReg = Vec::fromSignatureAndId(x86VecRegSignatureBySize(argSize), src.id());
  Mem vecPtr = ptr(_pass->_sp.as<Gp>(), int32_t(argStackOffset));

  uint32_t vMovInstId = choose(Inst::kIdMovaps, Inst::kIdVmovaps);
  if (argSize > 16)
    vMovInstId = Inst::kIdVmovaps;

  ASMJIT_PROPAGATE(cc()->_newReg(out, cc()->_gpRegInfo.type(), nullptr));

  VirtReg* vReg = cc()->virtRegById(out->id());
  vReg->setWeight(BaseRAPass::kCallArgWeight);

  ASMJIT_PROPAGATE(cc()->lea(out->as<Gp>(), vecPtr));
  ASMJIT_PROPAGATE(cc()->emit(vMovInstId, ptr(out->as<Gp>()), vecReg));

  if (arg.isStack()) {
    Mem stackPtr = ptr(_pass->_sp.as<Gp>(), arg.stackOffset());
    ASMJIT_PROPAGATE(cc()->mov(stackPtr, out->as<Gp>()));
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - MoveImmToRegArg]
// ============================================================================

Error RACFGBuilder::moveImmToRegArg(InvokeNode* invokeNode, const FuncValue& arg, const Imm& imm_, BaseReg* out) noexcept {
  DebugUtils::unused(invokeNode);
  ASMJIT_ASSERT(arg.isReg());

  Imm imm(imm_);
  uint32_t rTypeId = Type::kIdU32;

  switch (arg.typeId()) {
    case Type::kIdI8: imm.signExtend8Bits(); goto MovU32;
    case Type::kIdU8: imm.zeroExtend8Bits(); goto MovU32;
    case Type::kIdI16: imm.signExtend16Bits(); goto MovU32;
    case Type::kIdU16: imm.zeroExtend16Bits(); goto MovU32;

    case Type::kIdI32:
    case Type::kIdU32:
MovU32:
      imm.zeroExtend32Bits();
      break;

    case Type::kIdI64:
    case Type::kIdU64:
      // Moving to GPD automatically zero extends in 64-bit mode.
      if (imm.isUInt32()) {
        imm.zeroExtend32Bits();
        break;
      }

      rTypeId = Type::kIdU64;
      break;

    default:
      return DebugUtils::errored(kErrorInvalidAssignment);
  }

  ASMJIT_PROPAGATE(cc()->_newReg(out, rTypeId, nullptr));
  cc()->virtRegById(out->id())->setWeight(BaseRAPass::kCallArgWeight);

  return cc()->mov(out->as<x86::Gp>(), imm);
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - MoveImmToStackArg]
// ============================================================================

Error RACFGBuilder::moveImmToStackArg(InvokeNode* invokeNode, const FuncValue& arg, const Imm& imm_) noexcept {
  DebugUtils::unused(invokeNode);
  ASMJIT_ASSERT(arg.isStack());

  Mem stackPtr = ptr(_pass->_sp.as<Gp>(), arg.stackOffset());
  Imm imm[2];

  stackPtr.setSize(4);
  imm[0] = imm_;
  uint32_t nMovs = 0;

  // One stack entry has the same size as the native register size. That means
  // that if we want to move a 32-bit integer on the stack in 64-bit mode, we
  // need to extend it to a 64-bit integer first. In 32-bit mode, pushing a
  // 64-bit on stack is done in two steps by pushing low and high parts
  // separately.
  switch (arg.typeId()) {
    case Type::kIdI8: imm[0].signExtend8Bits(); goto MovU32;
    case Type::kIdU8: imm[0].zeroExtend8Bits(); goto MovU32;
    case Type::kIdI16: imm[0].signExtend16Bits(); goto MovU32;
    case Type::kIdU16: imm[0].zeroExtend16Bits(); goto MovU32;

    case Type::kIdI32:
    case Type::kIdU32:
    case Type::kIdF32:
MovU32:
      imm[0].zeroExtend32Bits();
      nMovs = 1;
      break;

    case Type::kIdI64:
    case Type::kIdU64:
    case Type::kIdF64:
    case Type::kIdMmx32:
    case Type::kIdMmx64:
      if (_is64Bit && imm[0].isInt32()) {
        stackPtr.setSize(8);
        nMovs = 1;
        break;
      }

      imm[1].setValue(imm[0].uint32Hi());
      imm[0].zeroExtend32Bits();
      nMovs = 2;
      break;

    default:
      return DebugUtils::errored(kErrorInvalidAssignment);
  }

  for (uint32_t i = 0; i < nMovs; i++) {
    ASMJIT_PROPAGATE(cc()->mov(stackPtr, imm[i]));
    stackPtr.addOffsetLo32(int32_t(stackPtr.size()));
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - MoveRegToStackArg]
// ============================================================================

Error RACFGBuilder::moveRegToStackArg(InvokeNode* invokeNode, const FuncValue& arg, const BaseReg& reg) noexcept {
  DebugUtils::unused(invokeNode);
  ASMJIT_ASSERT(arg.isStack());

  Mem stackPtr = ptr(_pass->_sp.as<Gp>(), arg.stackOffset());
  Reg r0, r1;

  VirtReg* vr = cc()->virtRegById(reg.id());
  uint32_t registerSize = cc()->registerSize();
  uint32_t instId = 0;

  uint32_t dstTypeId = arg.typeId();
  uint32_t srcTypeId = vr->typeId();

  switch (dstTypeId) {
    case Type::kIdI64:
    case Type::kIdU64:
      // Extend BYTE->QWORD (GP).
      if (Type::isGp8(srcTypeId)) {
        r1.setRegT<Reg::kTypeGpbLo>(reg.id());

        instId = (dstTypeId == Type::kIdI64 && srcTypeId == Type::kIdI8) ? Inst::kIdMovsx : Inst::kIdMovzx;
        goto ExtendMovGpXQ;
      }

      // Extend WORD->QWORD (GP).
      if (Type::isGp16(srcTypeId)) {
        r1.setRegT<Reg::kTypeGpw>(reg.id());

        instId = (dstTypeId == Type::kIdI64 && srcTypeId == Type::kIdI16) ? Inst::kIdMovsx : Inst::kIdMovzx;
        goto ExtendMovGpXQ;
      }

      // Extend DWORD->QWORD (GP).
      if (Type::isGp32(srcTypeId)) {
        r1.setRegT<Reg::kTypeGpd>(reg.id());

        instId = Inst::kIdMovsxd;
        if (dstTypeId == Type::kIdI64 && srcTypeId == Type::kIdI32)
          goto ExtendMovGpXQ;
        else
          goto ZeroExtendGpDQ;
      }

      // Move QWORD (GP).
      if (Type::isGp64(srcTypeId)) goto MovGpQ;
      if (Type::isMmx(srcTypeId)) goto MovMmQ;
      if (Type::isVec(srcTypeId)) goto MovXmmQ;
      break;

    case Type::kIdI32:
    case Type::kIdU32:
    case Type::kIdI16:
    case Type::kIdU16:
      // DWORD <- WORD (Zero|Sign Extend).
      if (Type::isGp16(srcTypeId)) {
        bool isDstSigned = dstTypeId == Type::kIdI16 || dstTypeId == Type::kIdI32;
        bool isSrcSigned = srcTypeId == Type::kIdI8  || srcTypeId == Type::kIdI16;

        r1.setRegT<Reg::kTypeGpw>(reg.id());
        instId = isDstSigned && isSrcSigned ? Inst::kIdMovsx : Inst::kIdMovzx;
        goto ExtendMovGpD;
      }

      // DWORD <- BYTE (Zero|Sign Extend).
      if (Type::isGp8(srcTypeId)) {
        bool isDstSigned = dstTypeId == Type::kIdI16 || dstTypeId == Type::kIdI32;
        bool isSrcSigned = srcTypeId == Type::kIdI8  || srcTypeId == Type::kIdI16;

        r1.setRegT<Reg::kTypeGpbLo>(reg.id());
        instId = isDstSigned && isSrcSigned ? Inst::kIdMovsx : Inst::kIdMovzx;
        goto ExtendMovGpD;
      }
      ASMJIT_FALLTHROUGH;

    case Type::kIdI8:
    case Type::kIdU8:
      if (Type::isInt(srcTypeId)) goto MovGpD;
      if (Type::isMmx(srcTypeId)) goto MovMmD;
      if (Type::isVec(srcTypeId)) goto MovXmmD;
      break;

    case Type::kIdMmx32:
    case Type::kIdMmx64:
      // Extend BYTE->QWORD (GP).
      if (Type::isGp8(srcTypeId)) {
        r1.setRegT<Reg::kTypeGpbLo>(reg.id());

        instId = Inst::kIdMovzx;
        goto ExtendMovGpXQ;
      }

      // Extend WORD->QWORD (GP).
      if (Type::isGp16(srcTypeId)) {
        r1.setRegT<Reg::kTypeGpw>(reg.id());

        instId = Inst::kIdMovzx;
        goto ExtendMovGpXQ;
      }

      if (Type::isGp32(srcTypeId)) goto ExtendMovGpDQ;
      if (Type::isGp64(srcTypeId)) goto MovGpQ;
      if (Type::isMmx(srcTypeId)) goto MovMmQ;
      if (Type::isVec(srcTypeId)) goto MovXmmQ;
      break;

    case Type::kIdF32:
    case Type::kIdF32x1:
      if (Type::isVec(srcTypeId)) goto MovXmmD;
      break;

    case Type::kIdF64:
    case Type::kIdF64x1:
      if (Type::isVec(srcTypeId)) goto MovXmmQ;
      break;

    default:
      if (Type::isVec(dstTypeId) && reg.as<Reg>().isVec()) {
        stackPtr.setSize(Type::sizeOf(dstTypeId));
        uint32_t vMovInstId = choose(Inst::kIdMovaps, Inst::kIdVmovaps);

        if (Type::isVec128(dstTypeId))
          r0.setRegT<Reg::kTypeXmm>(reg.id());
        else if (Type::isVec256(dstTypeId))
          r0.setRegT<Reg::kTypeYmm>(reg.id());
        else if (Type::isVec512(dstTypeId))
          r0.setRegT<Reg::kTypeZmm>(reg.id());
        else
          break;

        return cc()->emit(vMovInstId, stackPtr, r0);
      }
      break;
  }
  return DebugUtils::errored(kErrorInvalidAssignment);

  // Extend+Move Gp.
ExtendMovGpD:
  stackPtr.setSize(4);
  r0.setRegT<Reg::kTypeGpd>(reg.id());

  ASMJIT_PROPAGATE(cc()->emit(instId, r0, r1));
  ASMJIT_PROPAGATE(cc()->emit(Inst::kIdMov, stackPtr, r0));
  return kErrorOk;

ExtendMovGpXQ:
  if (registerSize == 8) {
    stackPtr.setSize(8);
    r0.setRegT<Reg::kTypeGpq>(reg.id());

    ASMJIT_PROPAGATE(cc()->emit(instId, r0, r1));
    ASMJIT_PROPAGATE(cc()->emit(Inst::kIdMov, stackPtr, r0));
  }
  else {
    stackPtr.setSize(4);
    r0.setRegT<Reg::kTypeGpd>(reg.id());

    ASMJIT_PROPAGATE(cc()->emit(instId, r0, r1));

ExtendMovGpDQ:
    ASMJIT_PROPAGATE(cc()->emit(Inst::kIdMov, stackPtr, r0));
    stackPtr.addOffsetLo32(4);
    ASMJIT_PROPAGATE(cc()->emit(Inst::kIdAnd, stackPtr, 0));
  }
  return kErrorOk;

ZeroExtendGpDQ:
  stackPtr.setSize(4);
  r0.setRegT<Reg::kTypeGpd>(reg.id());
  goto ExtendMovGpDQ;

MovGpD:
  stackPtr.setSize(4);
  r0.setRegT<Reg::kTypeGpd>(reg.id());
  return cc()->emit(Inst::kIdMov, stackPtr, r0);

MovGpQ:
  stackPtr.setSize(8);
  r0.setRegT<Reg::kTypeGpq>(reg.id());
  return cc()->emit(Inst::kIdMov, stackPtr, r0);

MovMmD:
  stackPtr.setSize(4);
  r0.setRegT<Reg::kTypeMm>(reg.id());
  return cc()->emit(choose(Inst::kIdMovd, Inst::kIdVmovd), stackPtr, r0);

MovMmQ:
  stackPtr.setSize(8);
  r0.setRegT<Reg::kTypeMm>(reg.id());
  return cc()->emit(choose(Inst::kIdMovq, Inst::kIdVmovq), stackPtr, r0);

MovXmmD:
  stackPtr.setSize(4);
  r0.setRegT<Reg::kTypeXmm>(reg.id());
  return cc()->emit(choose(Inst::kIdMovss, Inst::kIdVmovss), stackPtr, r0);

MovXmmQ:
  stackPtr.setSize(8);
  r0.setRegT<Reg::kTypeXmm>(reg.id());
  return cc()->emit(choose(Inst::kIdMovlps, Inst::kIdVmovlps), stackPtr, r0);
}

// ============================================================================
// [asmjit::x86::RACFGBuilder - OnReg]
// ============================================================================

Error RACFGBuilder::onBeforeRet(FuncRetNode* funcRet) noexcept {
  const FuncDetail& funcDetail = _pass->func()->detail();
  const Operand* opArray = funcRet->operands();
  uint32_t opCount = funcRet->opCount();

  cc()->_setCursor(funcRet->prev());

  for (uint32_t i = 0; i < opCount; i++) {
    const Operand& op = opArray[i];
    const FuncValue& ret = funcDetail.ret(i);

    if (!op.isReg())
      continue;

    if (ret.regType() == Reg::kTypeSt) {
      const Reg& reg = op.as<Reg>();
      uint32_t vIndex = Operand::virtIdToIndex(reg.id());

      if (vIndex < Operand::kVirtIdCount) {
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

        if (workReg->group() != Reg::kGroupVec)
          return DebugUtils::errored(kErrorInvalidAssignment);

        Reg src = Reg::fromSignatureAndId(workReg->signature(), workReg->virtId());
        Mem mem;

        uint32_t typeId = Type::baseOf(workReg->typeId());
        if (ret.hasTypeId())
          typeId = ret.typeId();

        switch (typeId) {
          case Type::kIdF32:
            ASMJIT_PROPAGATE(_pass->useTemporaryMem(mem, 4, 4));
            mem.setSize(4);
            ASMJIT_PROPAGATE(cc()->emit(choose(Inst::kIdMovss, Inst::kIdVmovss), mem, src.as<Xmm>()));
            ASMJIT_PROPAGATE(cc()->fld(mem));
            break;

          case Type::kIdF64:
            ASMJIT_PROPAGATE(_pass->useTemporaryMem(mem, 8, 4));
            mem.setSize(8);
            ASMJIT_PROPAGATE(cc()->emit(choose(Inst::kIdMovsd, Inst::kIdVmovsd), mem, src.as<Xmm>()));
            ASMJIT_PROPAGATE(cc()->fld(mem));
            break;

          default:
            return DebugUtils::errored(kErrorInvalidAssignment);
        }
      }
    }
  }

  return kErrorOk;
}

Error RACFGBuilder::onRet(FuncRetNode* funcRet, RAInstBuilder& ib) noexcept {
  const FuncDetail& funcDetail = _pass->func()->detail();
  const Operand* opArray = funcRet->operands();
  uint32_t opCount = funcRet->opCount();

  for (uint32_t i = 0; i < opCount; i++) {
    const Operand& op = opArray[i];
    if (op.isNone()) continue;

    const FuncValue& ret = funcDetail.ret(i);
    if (ASMJIT_UNLIKELY(!ret.isReg()))
      return DebugUtils::errored(kErrorInvalidAssignment);

    // Not handled here...
    if (ret.regType() == Reg::kTypeSt)
      continue;

    if (op.isReg()) {
      // Register return value.
      const Reg& reg = op.as<Reg>();
      uint32_t vIndex = Operand::virtIdToIndex(reg.id());

      if (vIndex < Operand::kVirtIdCount) {
        RAWorkReg* workReg;
        ASMJIT_PROPAGATE(_pass->virtIndexAsWorkReg(vIndex, &workReg));

        uint32_t group = workReg->group();
        uint32_t allocable = _pass->_availableRegs[group];
        ASMJIT_PROPAGATE(ib.add(workReg, RATiedReg::kUse | RATiedReg::kRead, allocable, ret.regId(), 0, BaseReg::kIdBad, 0));
      }
    }
    else {
      return DebugUtils::errored(kErrorInvalidAssignment);
    }
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::X86RAPass - Construction / Destruction]
// ============================================================================

X86RAPass::X86RAPass() noexcept
  : BaseRAPass() { _iEmitHelper = &_emitHelper; }
X86RAPass::~X86RAPass() noexcept {}

// ============================================================================
// [asmjit::x86::X86RAPass - OnInit / OnDone]
// ============================================================================

void X86RAPass::onInit() noexcept {
  uint32_t arch = cc()->arch();
  uint32_t baseRegCount = Environment::is32Bit(arch) ? 8u : 16u;
  uint32_t simdRegCount = baseRegCount;

  if (Environment::is64Bit(arch) && _func->frame().isAvx512Enabled())
    simdRegCount = 32u;

  bool avxEnabled = _func->frame().isAvxEnabled();
  bool avx512Enabled = _func->frame().isAvx512Enabled();

  _emitHelper._emitter = _cb;
  _emitHelper._avxEnabled = avxEnabled || avx512Enabled;
  _emitHelper._avx512Enabled = avx512Enabled;

  _archTraits = &ArchTraits::byArch(arch);
  _physRegCount.set(Reg::kGroupGp  , baseRegCount);
  _physRegCount.set(Reg::kGroupVec , simdRegCount);
  _physRegCount.set(Reg::kGroupMm  , 8);
  _physRegCount.set(Reg::kGroupKReg, 8);
  _buildPhysIndex();

  _availableRegCount = _physRegCount;
  _availableRegs[Reg::kGroupGp  ] = Support::lsbMask<uint32_t>(_physRegCount.get(Reg::kGroupGp  ));
  _availableRegs[Reg::kGroupVec ] = Support::lsbMask<uint32_t>(_physRegCount.get(Reg::kGroupVec ));
  _availableRegs[Reg::kGroupMm  ] = Support::lsbMask<uint32_t>(_physRegCount.get(Reg::kGroupMm  ));
  _availableRegs[Reg::kGroupKReg] = Support::lsbMask<uint32_t>(_physRegCount.get(Reg::kGroupKReg)) ^ 1u;

  _scratchRegIndexes[0] = uint8_t(Gp::kIdCx);
  _scratchRegIndexes[1] = uint8_t(baseRegCount - 1);

  // The architecture specific setup makes implicitly all registers available. So
  // make unavailable all registers that are special and cannot be used in general.
  bool hasFP = _func->frame().hasPreservedFP();

  makeUnavailable(Reg::kGroupGp, Gp::kIdSp);            // ESP|RSP used as a stack-pointer (SP).
  if (hasFP) makeUnavailable(Reg::kGroupGp, Gp::kIdBp); // EBP|RBP used as a frame-pointer (FP).

  _sp = cc()->zsp();
  _fp = cc()->zbp();
}

void X86RAPass::onDone() noexcept {}

// ============================================================================
// [asmjit::x86::X86RAPass - BuildCFG]
// ============================================================================

Error X86RAPass::buildCFG() noexcept {
  return RACFGBuilder(this).run();
}

// ============================================================================
// [asmjit::x86::X86RAPass - Rewrite]
// ============================================================================

static uint32_t transformVexToEvex(uint32_t instId) {
  switch (instId) {
    case Inst::kIdVbroadcastf128: return Inst::kIdVbroadcastf32x4;
    case Inst::kIdVbroadcasti128: return Inst::kIdVbroadcasti32x4;
    case Inst::kIdVextractf128: return Inst::kIdVextractf32x4;
    case Inst::kIdVextracti128: return Inst::kIdVextracti32x4;
    case Inst::kIdVinsertf128: return Inst::kIdVinsertf32x4;
    case Inst::kIdVinserti128: return Inst::kIdVinserti32x4;
    case Inst::kIdVmovdqa: return Inst::kIdVmovdqa32;
    case Inst::kIdVmovdqu: return Inst::kIdVmovdqu32;
    case Inst::kIdVpand: return Inst::kIdVpandd;
    case Inst::kIdVpandn: return Inst::kIdVpandnd;
    case Inst::kIdVpor: return Inst::kIdVpord;
    case Inst::kIdVpxor: return Inst::kIdVpxord;

    default:
      // This should never happen as only transformable instructions should go this path.
      ASMJIT_ASSERT(false);
      return 0;
  }
}

ASMJIT_FAVOR_SPEED Error X86RAPass::_rewrite(BaseNode* first, BaseNode* stop) noexcept {
  uint32_t virtCount = cc()->_vRegArray.size();

  BaseNode* node = first;
  while (node != stop) {
    BaseNode* next = node->next();
    if (node->isInst()) {
      InstNode* inst = node->as<InstNode>();
      RAInst* raInst = node->passData<RAInst>();

      Operand* operands = inst->operands();
      uint32_t opCount = inst->opCount();
      uint32_t maxRegId = 0;

      uint32_t i;

      // Rewrite virtual registers into physical registers.
      if (raInst) {
        // If the instruction contains pass data (raInst) then it was a subject
        // for register allocation and must be rewritten to use physical regs.
        RATiedReg* tiedRegs = raInst->tiedRegs();
        uint32_t tiedCount = raInst->tiedCount();

        for (i = 0; i < tiedCount; i++) {
          RATiedReg* tiedReg = &tiedRegs[i];

          Support::BitWordIterator<uint32_t> useIt(tiedReg->useRewriteMask());
          uint32_t useId = tiedReg->useId();
          while (useIt.hasNext()) {
            maxRegId = Support::max(maxRegId, useId);
            inst->rewriteIdAtIndex(useIt.next(), useId);
          }

          Support::BitWordIterator<uint32_t> outIt(tiedReg->outRewriteMask());
          uint32_t outId = tiedReg->outId();
          while (outIt.hasNext()) {
            maxRegId = Support::max(maxRegId, outId);
            inst->rewriteIdAtIndex(outIt.next(), outId);
          }
        }

        if (raInst->isTransformable()) {
          if (maxRegId > 15) {
            // Transform VEX instruction to EVEX.
            inst->setId(transformVexToEvex(inst->id()));
          }
        }

        // This data is allocated by Zone passed to `runOnFunction()`, which
        // will be reset after the RA pass finishes. So reset this data to
        // prevent having a dead pointer after the RA pass is complete.
        node->resetPassData();

        if (ASMJIT_UNLIKELY(node->type() != BaseNode::kNodeInst)) {
          // FuncRet terminates the flow, it must either be removed if the exit
          // label is next to it (optimization) or patched to an architecture
          // dependent jump instruction that jumps to the function's exit before
          // the epilog.
          if (node->type() == BaseNode::kNodeFuncRet) {
            RABlock* block = raInst->block();
            if (!isNextTo(node, _func->exitNode())) {
              cc()->_setCursor(node->prev());
              ASMJIT_PROPAGATE(emitJump(_func->exitNode()->label()));
            }

            BaseNode* prev = node->prev();
            cc()->removeNode(node);
            block->setLast(prev);
          }
        }
      }

      // Rewrite stack slot addresses.
      for (i = 0; i < opCount; i++) {
        Operand& op = operands[i];
        if (op.isMem()) {
          BaseMem& mem = op.as<BaseMem>();
          if (mem.isRegHome()) {
            uint32_t virtIndex = Operand::virtIdToIndex(mem.baseId());
            if (ASMJIT_UNLIKELY(virtIndex >= virtCount))
              return DebugUtils::errored(kErrorInvalidVirtId);

            VirtReg* virtReg = cc()->virtRegByIndex(virtIndex);
            RAWorkReg* workReg = virtReg->workReg();
            ASMJIT_ASSERT(workReg != nullptr);

            RAStackSlot* slot = workReg->stackSlot();
            int32_t offset = slot->offset();

            mem._setBase(_sp.type(), slot->baseRegId());
            mem.clearRegHome();
            mem.addOffsetLo32(offset);
          }
        }
      }
    }

    node = next;
  }

  return kErrorOk;
}

// ============================================================================
// [asmjit::x86::X86RAPass - OnEmit]
// ============================================================================

Error X86RAPass::emitMove(uint32_t workId, uint32_t dstPhysId, uint32_t srcPhysId) noexcept {
  RAWorkReg* wReg = workRegById(workId);
  BaseReg dst = BaseReg::fromSignatureAndId(wReg->info().signature(), dstPhysId);
  BaseReg src = BaseReg::fromSignatureAndId(wReg->info().signature(), srcPhysId);

  const char* comment = nullptr;

#ifndef ASMJIT_NO_LOGGING
  if (_loggerFlags & FormatOptions::kFlagAnnotations) {
    _tmpString.assignFormat("<MOVE> %s", workRegById(workId)->name());
    comment = _tmpString.data();
  }
#endif

  return _emitHelper.emitRegMove(dst, src, wReg->typeId(), comment);
}

Error X86RAPass::emitSwap(uint32_t aWorkId, uint32_t aPhysId, uint32_t bWorkId, uint32_t bPhysId) noexcept {
  RAWorkReg* waReg = workRegById(aWorkId);
  RAWorkReg* wbReg = workRegById(bWorkId);

  bool is64Bit = Support::max(waReg->typeId(), wbReg->typeId()) >= Type::kIdI64;
  uint32_t sign = is64Bit ? uint32_t(RegTraits<Reg::kTypeGpq>::kSignature)
                          : uint32_t(RegTraits<Reg::kTypeGpd>::kSignature);

#ifndef ASMJIT_NO_LOGGING
  if (_loggerFlags & FormatOptions::kFlagAnnotations) {
    _tmpString.assignFormat("<SWAP> %s, %s", waReg->name(), wbReg->name());
    cc()->setInlineComment(_tmpString.data());
  }
#endif

  return cc()->emit(Inst::kIdXchg,
                    Reg::fromSignatureAndId(sign, aPhysId),
                    Reg::fromSignatureAndId(sign, bPhysId));
}

Error X86RAPass::emitLoad(uint32_t workId, uint32_t dstPhysId) noexcept {
  RAWorkReg* wReg = workRegById(workId);
  BaseReg dstReg = BaseReg::fromSignatureAndId(wReg->info().signature(), dstPhysId);
  BaseMem srcMem = BaseMem(workRegAsMem(wReg));

  const char* comment = nullptr;

#ifndef ASMJIT_NO_LOGGING
  if (_loggerFlags & FormatOptions::kFlagAnnotations) {
    _tmpString.assignFormat("<LOAD> %s", workRegById(workId)->name());
    comment = _tmpString.data();
  }
#endif

  return _emitHelper.emitRegMove(dstReg, srcMem, wReg->typeId(), comment);
}

Error X86RAPass::emitSave(uint32_t workId, uint32_t srcPhysId) noexcept {
  RAWorkReg* wReg = workRegById(workId);
  BaseMem dstMem = BaseMem(workRegAsMem(wReg));
  BaseReg srcReg = BaseReg::fromSignatureAndId(wReg->info().signature(), srcPhysId);

  const char* comment = nullptr;

#ifndef ASMJIT_NO_LOGGING
  if (_loggerFlags & FormatOptions::kFlagAnnotations) {
    _tmpString.assignFormat("<SAVE> %s", workRegById(workId)->name());
    comment = _tmpString.data();
  }
#endif

  return _emitHelper.emitRegMove(dstMem, srcReg, wReg->typeId(), comment);
}

Error X86RAPass::emitJump(const Label& label) noexcept {
  return cc()->jmp(label);
}

Error X86RAPass::emitPreCall(InvokeNode* invokeNode) noexcept {
  if (invokeNode->detail().hasVarArgs() && cc()->is64Bit()) {
    const FuncDetail& fd = invokeNode->detail();
    uint32_t argCount = invokeNode->argCount();

    switch (invokeNode->detail().callConv().id()) {
      case CallConv::kIdX64SystemV: {
        // AL register contains the number of arguments passed in XMM register(s).
        uint32_t n = 0;
        for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
          const FuncValuePack& argPack = fd.argPack(argIndex);
          for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
            const FuncValue& arg = argPack[valueIndex];
            if (!arg)
              break;

            if (arg.isReg() && Reg::groupOf(arg.regType()) == Reg::kGroupVec)
              n++;
          }
        }

        if (!n)
          ASMJIT_PROPAGATE(cc()->xor_(eax, eax));
        else
          ASMJIT_PROPAGATE(cc()->mov(eax, n));
        break;
      }

      case CallConv::kIdX64Windows: {
        // Each double-precision argument passed in XMM must be also passed in GP.
        for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
          const FuncValuePack& argPack = fd.argPack(argIndex);
          for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
            const FuncValue& arg = argPack[valueIndex];
            if (!arg)
              break;

            if (arg.isReg() && Reg::groupOf(arg.regType()) == Reg::kGroupVec) {
              Gp dst = gpq(fd.callConv().passedOrder(Reg::kGroupGp)[argIndex]);
              Xmm src = xmm(arg.regId());
              ASMJIT_PROPAGATE(cc()->emit(choose(Inst::kIdMovq, Inst::kIdVmovq), dst, src));
            }
          }
        }
        break;
      }

      default:
        return DebugUtils::errored(kErrorInvalidState);
    }
  }

  return kErrorOk;
}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86 && !ASMJIT_NO_COMPILER
