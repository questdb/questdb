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
#if !defined(ASMJIT_NO_X86)

#include "../core/formatter.h"
#include "../core/funcargscontext_p.h"
#include "../core/string.h"
#include "../core/support.h"
#include "../core/type.h"
#include "../core/radefs_p.h"
#include "../x86/x86emithelper_p.h"
#include "../x86/x86emitter.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [asmjit::x86::Internal - Helpers]
// ============================================================================

static ASMJIT_INLINE uint32_t getXmmMovInst(const FuncFrame& frame) {
  bool avx = frame.isAvxEnabled();
  bool aligned = frame.hasAlignedVecSR();

  return aligned ? (avx ? Inst::kIdVmovaps : Inst::kIdMovaps)
                 : (avx ? Inst::kIdVmovups : Inst::kIdMovups);
}

//! Converts `size` to a 'kmov?' instructio.
static inline uint32_t kmovInstFromSize(uint32_t size) noexcept {
  switch (size) {
    case  1: return Inst::kIdKmovb;
    case  2: return Inst::kIdKmovw;
    case  4: return Inst::kIdKmovd;
    case  8: return Inst::kIdKmovq;
    default: return Inst::kIdNone;
  }
}

// ============================================================================
// [asmjit::X86Internal - Emit Helpers]
// ============================================================================

ASMJIT_FAVOR_SIZE Error EmitHelper::emitRegMove(
  const Operand_& dst_,
  const Operand_& src_, uint32_t typeId, const char* comment) {

  // Invalid or abstract TypeIds are not allowed.
  ASMJIT_ASSERT(Type::isValid(typeId) && !Type::isAbstract(typeId));

  Operand dst(dst_);
  Operand src(src_);

  uint32_t instId = Inst::kIdNone;
  uint32_t memFlags = 0;
  uint32_t overrideMemSize = 0;

  enum MemFlags : uint32_t {
    kDstMem = 0x1,
    kSrcMem = 0x2
  };

  // Detect memory operands and patch them to have the same size as the register.
  // BaseCompiler always sets memory size of allocs and spills, so it shouldn't
  // be really necessary, however, after this function was separated from Compiler
  // it's better to make sure that the size is always specified, as we can use
  // 'movzx' and 'movsx' that rely on it.
  if (dst.isMem()) { memFlags |= kDstMem; dst.as<Mem>().setSize(src.size()); }
  if (src.isMem()) { memFlags |= kSrcMem; src.as<Mem>().setSize(dst.size()); }

  switch (typeId) {
    case Type::kIdI8:
    case Type::kIdU8:
    case Type::kIdI16:
    case Type::kIdU16:
      // Special case - 'movzx' load.
      if (memFlags & kSrcMem) {
        instId = Inst::kIdMovzx;
        dst.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
      }
      else if (!memFlags) {
        // Change both destination and source registers to GPD (safer, no dependencies).
        dst.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        src.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
      }
      ASMJIT_FALLTHROUGH;

    case Type::kIdI32:
    case Type::kIdU32:
    case Type::kIdI64:
    case Type::kIdU64:
      instId = Inst::kIdMov;
      break;

    case Type::kIdMmx32:
      instId = Inst::kIdMovd;
      if (memFlags) break;
      ASMJIT_FALLTHROUGH;

    case Type::kIdMmx64 : instId = Inst::kIdMovq ; break;
    case Type::kIdMask8 : instId = Inst::kIdKmovb; break;
    case Type::kIdMask16: instId = Inst::kIdKmovw; break;
    case Type::kIdMask32: instId = Inst::kIdKmovd; break;
    case Type::kIdMask64: instId = Inst::kIdKmovq; break;

    default: {
      uint32_t elementTypeId = Type::baseOf(typeId);
      if (Type::isVec32(typeId) && memFlags) {
        overrideMemSize = 4;
        if (elementTypeId == Type::kIdF32)
          instId = _avxEnabled ? Inst::kIdVmovss : Inst::kIdMovss;
        else
          instId = _avxEnabled ? Inst::kIdVmovd : Inst::kIdMovd;
        break;
      }

      if (Type::isVec64(typeId) && memFlags) {
        overrideMemSize = 8;
        if (elementTypeId == Type::kIdF64)
          instId = _avxEnabled ? Inst::kIdVmovsd : Inst::kIdMovsd;
        else
          instId = _avxEnabled ? Inst::kIdVmovq : Inst::kIdMovq;
        break;
      }

      if (elementTypeId == Type::kIdF32)
        instId = _avxEnabled ? Inst::kIdVmovaps : Inst::kIdMovaps;
      else if (elementTypeId == Type::kIdF64)
        instId = _avxEnabled ? Inst::kIdVmovapd : Inst::kIdMovapd;
      else if (!_avx512Enabled)
        instId = _avxEnabled ? Inst::kIdVmovdqa : Inst::kIdMovdqa;
      else
        instId = Inst::kIdVmovdqa32;
      break;
    }
  }

  if (!instId)
    return DebugUtils::errored(kErrorInvalidState);

  if (overrideMemSize) {
    if (dst.isMem()) dst.as<Mem>().setSize(overrideMemSize);
    if (src.isMem()) src.as<Mem>().setSize(overrideMemSize);
  }

  _emitter->setInlineComment(comment);
  return _emitter->emit(instId, dst, src);
}

ASMJIT_FAVOR_SIZE Error EmitHelper::emitArgMove(
  const BaseReg& dst_, uint32_t dstTypeId,
  const Operand_& src_, uint32_t srcTypeId, const char* comment) {

  // Deduce optional `dstTypeId`, which may be `Type::kIdVoid` in some cases.
  if (!dstTypeId) {
    const ArchTraits& archTraits = ArchTraits::byArch(_emitter->arch());
    dstTypeId = archTraits.regTypeToTypeId(dst_.type());
  }

  // Invalid or abstract TypeIds are not allowed.
  ASMJIT_ASSERT(Type::isValid(dstTypeId) && !Type::isAbstract(dstTypeId));
  ASMJIT_ASSERT(Type::isValid(srcTypeId) && !Type::isAbstract(srcTypeId));

  Reg dst(dst_.as<Reg>());
  Operand src(src_);

  uint32_t dstSize = Type::sizeOf(dstTypeId);
  uint32_t srcSize = Type::sizeOf(srcTypeId);

  uint32_t instId = Inst::kIdNone;

  // Not a real loop, just 'break' is nicer than 'goto'.
  for (;;) {
    if (Type::isInt(dstTypeId)) {
      if (Type::isInt(srcTypeId)) {
        instId = Inst::kIdMovsx;
        uint32_t typeOp = (dstTypeId << 8) | srcTypeId;

        // Sign extend by using 'movsx'.
        if (typeOp == ((Type::kIdI16 << 8) | Type::kIdI8 ) ||
            typeOp == ((Type::kIdI32 << 8) | Type::kIdI8 ) ||
            typeOp == ((Type::kIdI32 << 8) | Type::kIdI16) ||
            typeOp == ((Type::kIdI64 << 8) | Type::kIdI8 ) ||
            typeOp == ((Type::kIdI64 << 8) | Type::kIdI16))
          break;

        // Sign extend by using 'movsxd'.
        instId = Inst::kIdMovsxd;
        if (typeOp == ((Type::kIdI64 << 8) | Type::kIdI32))
          break;
      }

      if (Type::isInt(srcTypeId) || src_.isMem()) {
        // Zero extend by using 'movzx' or 'mov'.
        if (dstSize <= 4 && srcSize < 4) {
          instId = Inst::kIdMovzx;
          dst.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        }
        else {
          // We should have caught all possibilities where `srcSize` is less
          // than 4, so we don't have to worry about 'movzx' anymore. Minimum
          // size is enough to determine if we want 32-bit or 64-bit move.
          instId = Inst::kIdMov;
          srcSize = Support::min(srcSize, dstSize);

          dst.setSignature(srcSize == 4 ? Reg::signatureOfT<Reg::kTypeGpd>()
                                        : Reg::signatureOfT<Reg::kTypeGpq>());
          if (src.isReg())
            src.setSignature(dst.signature());
        }
        break;
      }

      // NOTE: The previous branch caught all memory sources, from here it's
      // always register to register conversion, so catch the remaining cases.
      srcSize = Support::min(srcSize, dstSize);

      if (Type::isMmx(srcTypeId)) {
        // 64-bit move.
        instId = Inst::kIdMovq;
        if (srcSize == 8)
          break;

        // 32-bit move.
        instId = Inst::kIdMovd;
        dst.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        break;
      }

      if (Type::isMask(srcTypeId)) {
        instId = kmovInstFromSize(srcSize);
        dst.setSignature(srcSize <= 4 ? Reg::signatureOfT<Reg::kTypeGpd>()
                                      : Reg::signatureOfT<Reg::kTypeGpq>());
        break;
      }

      if (Type::isVec(srcTypeId)) {
        // 64-bit move.
        instId = _avxEnabled ? Inst::kIdVmovq : Inst::kIdMovq;
        if (srcSize == 8)
          break;

        // 32-bit move.
        instId = _avxEnabled ? Inst::kIdVmovd : Inst::kIdMovd;
        dst.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        break;
      }
    }

    if (Type::isMmx(dstTypeId)) {
      instId = Inst::kIdMovq;
      srcSize = Support::min(srcSize, dstSize);

      if (Type::isInt(srcTypeId) || src.isMem()) {
        // 64-bit move.
        if (srcSize == 8)
          break;

        // 32-bit move.
        instId = Inst::kIdMovd;
        if (src.isReg())
          src.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        break;
      }

      if (Type::isMmx(srcTypeId))
        break;

      // This will hurt if AVX is enabled.
      instId = Inst::kIdMovdq2q;
      if (Type::isVec(srcTypeId))
        break;
    }

    if (Type::isMask(dstTypeId)) {
      srcSize = Support::min(srcSize, dstSize);

      if (Type::isInt(srcTypeId) || Type::isMask(srcTypeId) || src.isMem()) {
        instId = kmovInstFromSize(srcSize);
        if (Reg::isGp(src) && srcSize <= 4)
          src.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
        break;
      }
    }

    if (Type::isVec(dstTypeId)) {
      // By default set destination to XMM, will be set to YMM|ZMM if needed.
      dst.setSignature(Reg::signatureOfT<Reg::kTypeXmm>());

      // This will hurt if AVX is enabled.
      if (Reg::isMm(src)) {
        // 64-bit move.
        instId = Inst::kIdMovq2dq;
        break;
      }

      // Argument conversion.
      uint32_t dstElement = Type::baseOf(dstTypeId);
      uint32_t srcElement = Type::baseOf(srcTypeId);

      if (dstElement == Type::kIdF32 && srcElement == Type::kIdF64) {
        srcSize = Support::min(dstSize * 2, srcSize);
        dstSize = srcSize / 2;

        if (srcSize <= 8)
          instId = _avxEnabled ? Inst::kIdVcvtss2sd : Inst::kIdCvtss2sd;
        else
          instId = _avxEnabled ? Inst::kIdVcvtps2pd : Inst::kIdCvtps2pd;

        if (dstSize == 32)
          dst.setSignature(Reg::signatureOfT<Reg::kTypeYmm>());
        if (src.isReg())
          src.setSignature(Reg::signatureOfVecBySize(srcSize));
        break;
      }

      if (dstElement == Type::kIdF64 && srcElement == Type::kIdF32) {
        srcSize = Support::min(dstSize, srcSize * 2) / 2;
        dstSize = srcSize * 2;

        if (srcSize <= 4)
          instId = _avxEnabled ? Inst::kIdVcvtsd2ss : Inst::kIdCvtsd2ss;
        else
          instId = _avxEnabled ? Inst::kIdVcvtpd2ps : Inst::kIdCvtpd2ps;

        dst.setSignature(Reg::signatureOfVecBySize(dstSize));
        if (src.isReg() && srcSize >= 32)
          src.setSignature(Reg::signatureOfT<Reg::kTypeYmm>());
        break;
      }

      srcSize = Support::min(srcSize, dstSize);
      if (Reg::isGp(src) || src.isMem()) {
        // 32-bit move.
        if (srcSize <= 4) {
          instId = _avxEnabled ? Inst::kIdVmovd : Inst::kIdMovd;
          if (src.isReg())
            src.setSignature(Reg::signatureOfT<Reg::kTypeGpd>());
          break;
        }

        // 64-bit move.
        if (srcSize == 8) {
          instId = _avxEnabled ? Inst::kIdVmovq : Inst::kIdMovq;
          break;
        }
      }

      if (Reg::isVec(src) || src.isMem()) {
        instId = _avxEnabled ? Inst::kIdVmovaps : Inst::kIdMovaps;

        if (src.isMem() && srcSize < _emitter->environment().stackAlignment())
          instId = _avxEnabled ? Inst::kIdVmovups : Inst::kIdMovups;

        uint32_t signature = Reg::signatureOfVecBySize(srcSize);
        dst.setSignature(signature);
        if (src.isReg())
          src.setSignature(signature);
        break;
      }
    }

    return DebugUtils::errored(kErrorInvalidState);
  }

  if (src.isMem())
    src.as<Mem>().setSize(srcSize);

  _emitter->setInlineComment(comment);
  return _emitter->emit(instId, dst, src);
}

Error EmitHelper::emitRegSwap(
  const BaseReg& a,
  const BaseReg& b, const char* comment) {

  if (a.isGp() && b.isGp()) {
    _emitter->setInlineComment(comment);
    return _emitter->emit(Inst::kIdXchg, a, b);
  }
  else
    return DebugUtils::errored(kErrorInvalidState);
}

// ============================================================================
// [asmjit::X86Internal - Emit Prolog & Epilog]
// ============================================================================

static ASMJIT_INLINE void X86Internal_setupSaveRestoreInfo(uint32_t group, const FuncFrame& frame, Reg& xReg, uint32_t& xInst, uint32_t& xSize) noexcept {
  switch (group) {
    case Reg::kGroupVec:
      xReg = xmm(0);
      xInst = getXmmMovInst(frame);
      xSize = xReg.size();
      break;
    case Reg::kGroupMm:
      xReg = mm(0);
      xInst = Inst::kIdMovq;
      xSize = xReg.size();
      break;
    case Reg::kGroupKReg:
      xReg = k(0);
      xInst = Inst::kIdKmovq;
      xSize = xReg.size();
      break;
  }
}

ASMJIT_FAVOR_SIZE Error EmitHelper::emitProlog(const FuncFrame& frame) {
  Emitter* emitter = _emitter->as<Emitter>();
  uint32_t gpSaved = frame.savedRegs(Reg::kGroupGp);

  Gp zsp = emitter->zsp();   // ESP|RSP register.
  Gp zbp = emitter->zbp();   // EBP|RBP register.
  Gp gpReg = zsp;            // General purpose register (temporary).
  Gp saReg = zsp;            // Stack-arguments base pointer.

  // Emit: 'push zbp'
  //       'mov  zbp, zsp'.
  if (frame.hasPreservedFP()) {
    gpSaved &= ~Support::bitMask(Gp::kIdBp);
    ASMJIT_PROPAGATE(emitter->push(zbp));
    ASMJIT_PROPAGATE(emitter->mov(zbp, zsp));
  }

  // Emit: 'push gp' sequence.
  {
    Support::BitWordIterator<uint32_t> it(gpSaved);
    while (it.hasNext()) {
      gpReg.setId(it.next());
      ASMJIT_PROPAGATE(emitter->push(gpReg));
    }
  }

  // Emit: 'mov saReg, zsp'.
  uint32_t saRegId = frame.saRegId();
  if (saRegId != BaseReg::kIdBad && saRegId != Gp::kIdSp) {
    saReg.setId(saRegId);
    if (frame.hasPreservedFP()) {
      if (saRegId != Gp::kIdBp)
        ASMJIT_PROPAGATE(emitter->mov(saReg, zbp));
    }
    else {
      ASMJIT_PROPAGATE(emitter->mov(saReg, zsp));
    }
  }

  // Emit: 'and zsp, StackAlignment'.
  if (frame.hasDynamicAlignment()) {
    ASMJIT_PROPAGATE(emitter->and_(zsp, -int32_t(frame.finalStackAlignment())));
  }

  // Emit: 'sub zsp, StackAdjustment'.
  if (frame.hasStackAdjustment()) {
    ASMJIT_PROPAGATE(emitter->sub(zsp, frame.stackAdjustment()));
  }

  // Emit: 'mov [zsp + DAOffset], saReg'.
  if (frame.hasDynamicAlignment() && frame.hasDAOffset()) {
    Mem saMem = ptr(zsp, int32_t(frame.daOffset()));
    ASMJIT_PROPAGATE(emitter->mov(saMem, saReg));
  }

  // Emit 'movxxx [zsp + X], {[x|y|z]mm, k}'.
  {
    Reg xReg;
    Mem xBase = ptr(zsp, int32_t(frame.extraRegSaveOffset()));

    uint32_t xInst;
    uint32_t xSize;

    for (uint32_t group = 1; group < BaseReg::kGroupVirt; group++) {
      Support::BitWordIterator<uint32_t> it(frame.savedRegs(group));
      if (it.hasNext()) {
        X86Internal_setupSaveRestoreInfo(group, frame, xReg, xInst, xSize);
        do {
          xReg.setId(it.next());
          ASMJIT_PROPAGATE(emitter->emit(xInst, xBase, xReg));
          xBase.addOffsetLo32(int32_t(xSize));
        } while (it.hasNext());
      }
    }
  }

  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error EmitHelper::emitEpilog(const FuncFrame& frame) {
  Emitter* emitter = _emitter->as<Emitter>();

  uint32_t i;
  uint32_t regId;

  uint32_t registerSize = emitter->registerSize();
  uint32_t gpSaved = frame.savedRegs(Reg::kGroupGp);

  Gp zsp = emitter->zsp();   // ESP|RSP register.
  Gp zbp = emitter->zbp();   // EBP|RBP register.
  Gp gpReg = emitter->zsp(); // General purpose register (temporary).

  // Don't emit 'pop zbp' in the pop sequence, this case is handled separately.
  if (frame.hasPreservedFP())
    gpSaved &= ~Support::bitMask(Gp::kIdBp);

  // Emit 'movxxx {[x|y|z]mm, k}, [zsp + X]'.
  {
    Reg xReg;
    Mem xBase = ptr(zsp, int32_t(frame.extraRegSaveOffset()));

    uint32_t xInst;
    uint32_t xSize;

    for (uint32_t group = 1; group < BaseReg::kGroupVirt; group++) {
      Support::BitWordIterator<uint32_t> it(frame.savedRegs(group));
      if (it.hasNext()) {
        X86Internal_setupSaveRestoreInfo(group, frame, xReg, xInst, xSize);
        do {
          xReg.setId(it.next());
          ASMJIT_PROPAGATE(emitter->emit(xInst, xReg, xBase));
          xBase.addOffsetLo32(int32_t(xSize));
        } while (it.hasNext());
      }
    }
  }

  // Emit 'emms' and/or 'vzeroupper'.
  if (frame.hasMmxCleanup()) ASMJIT_PROPAGATE(emitter->emms());
  if (frame.hasAvxCleanup()) ASMJIT_PROPAGATE(emitter->vzeroupper());

  if (frame.hasPreservedFP()) {
    // Emit 'mov zsp, zbp' or 'lea zsp, [zbp - x]'
    int32_t count = int32_t(frame.pushPopSaveSize() - registerSize);
    if (!count)
      ASMJIT_PROPAGATE(emitter->mov(zsp, zbp));
    else
      ASMJIT_PROPAGATE(emitter->lea(zsp, ptr(zbp, -count)));
  }
  else {
    if (frame.hasDynamicAlignment() && frame.hasDAOffset()) {
      // Emit 'mov zsp, [zsp + DsaSlot]'.
      Mem saMem = ptr(zsp, int32_t(frame.daOffset()));
      ASMJIT_PROPAGATE(emitter->mov(zsp, saMem));
    }
    else if (frame.hasStackAdjustment()) {
      // Emit 'add zsp, StackAdjustment'.
      ASMJIT_PROPAGATE(emitter->add(zsp, int32_t(frame.stackAdjustment())));
    }
  }

  // Emit 'pop gp' sequence.
  if (gpSaved) {
    i = gpSaved;
    regId = 16;

    do {
      regId--;
      if (i & 0x8000) {
        gpReg.setId(regId);
        ASMJIT_PROPAGATE(emitter->pop(gpReg));
      }
      i <<= 1;
    } while (regId != 0);
  }

  // Emit 'pop zbp'.
  if (frame.hasPreservedFP())
    ASMJIT_PROPAGATE(emitter->pop(zbp));

  // Emit 'ret' or 'ret x'.
  if (frame.hasCalleeStackCleanup())
    ASMJIT_PROPAGATE(emitter->emit(Inst::kIdRet, int(frame.calleeStackCleanup())));
  else
    ASMJIT_PROPAGATE(emitter->emit(Inst::kIdRet));

  return kErrorOk;
}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86
