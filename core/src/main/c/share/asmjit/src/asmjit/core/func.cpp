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
#include "../core/archtraits.h"
#include "../core/func.h"
#include "../core/operand.h"
#include "../core/type.h"
#include "../core/funcargscontext_p.h"

#if !defined(ASMJIT_NO_X86)
  #include "../x86/x86func_p.h"
#endif

#ifdef ASMJIT_BUILD_ARM
  #include "../arm/armfunc_p.h"
#endif

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::CallConv - Init / Reset]
// ============================================================================

ASMJIT_FAVOR_SIZE Error CallConv::init(uint32_t ccId, const Environment& environment) noexcept {
  reset();

#if !defined(ASMJIT_NO_X86)
  if (environment.isFamilyX86())
    return x86::FuncInternal::initCallConv(*this, ccId, environment);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (environment.isFamilyARM())
    return arm::FuncInternal::initCallConv(*this, ccId, environment);
#endif

  return DebugUtils::errored(kErrorInvalidArgument);
}

// ============================================================================
// [asmjit::FuncDetail - Init / Reset]
// ============================================================================

ASMJIT_FAVOR_SIZE Error FuncDetail::init(const FuncSignature& signature, const Environment& environment) noexcept {
  uint32_t ccId = signature.callConv();
  uint32_t argCount = signature.argCount();

  if (ASMJIT_UNLIKELY(argCount > Globals::kMaxFuncArgs))
    return DebugUtils::errored(kErrorInvalidArgument);

  CallConv& cc = _callConv;
  ASMJIT_PROPAGATE(cc.init(ccId, environment));

  uint32_t registerSize = Environment::registerSizeFromArch(cc.arch());
  uint32_t deabstractDelta = Type::deabstractDeltaOfSize(registerSize);

  const uint8_t* signatureArgs = signature.args();
  for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
    FuncValuePack& argPack = _args[argIndex];
    argPack[0].initTypeId(Type::deabstract(signatureArgs[argIndex], deabstractDelta));
  }
  _argCount = uint8_t(argCount);
  _vaIndex = uint8_t(signature.vaIndex());

  uint32_t ret = signature.ret();
  if (ret != Type::kIdVoid)
    _rets[0].initTypeId(Type::deabstract(ret, deabstractDelta));

#if !defined(ASMJIT_NO_X86)
  if (environment.isFamilyX86())
    return x86::FuncInternal::initFuncDetail(*this, signature, registerSize);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (environment.isFamilyARM())
    return arm::FuncInternal::initFuncDetail(*this, signature, registerSize);
#endif

  // We should never bubble here as if `cc.init()` succeeded then there has to
  // be an implementation for the current architecture. However, stay safe.
  return DebugUtils::errored(kErrorInvalidArgument);
}

// ============================================================================
// [asmjit::FuncFrame - Init / Finalize]
// ============================================================================

ASMJIT_FAVOR_SIZE Error FuncFrame::init(const FuncDetail& func) noexcept {
  uint32_t arch = func.callConv().arch();
  if (!Environment::isValidArch(arch))
    return DebugUtils::errored(kErrorInvalidArch);

  const ArchTraits& archTraits = ArchTraits::byArch(arch);

  // Initializing FuncFrame means making a copy of some properties of `func`.
  // Properties like `_localStackSize` will be set by the user before the frame
  // is finalized.
  reset();

  _arch = uint8_t(arch);
  _spRegId = uint8_t(archTraits.spRegId());
  _saRegId = uint8_t(BaseReg::kIdBad);

  uint32_t naturalStackAlignment = func.callConv().naturalStackAlignment();
  uint32_t minDynamicAlignment = Support::max<uint32_t>(naturalStackAlignment, 16);

  if (minDynamicAlignment == naturalStackAlignment)
    minDynamicAlignment <<= 1;

  _naturalStackAlignment = uint8_t(naturalStackAlignment);
  _minDynamicAlignment = uint8_t(minDynamicAlignment);
  _redZoneSize = uint8_t(func.redZoneSize());
  _spillZoneSize = uint8_t(func.spillZoneSize());
  _finalStackAlignment = uint8_t(_naturalStackAlignment);

  if (func.hasFlag(CallConv::kFlagCalleePopsStack)) {
    _calleeStackCleanup = uint16_t(func.argStackSize());
  }

  // Initial masks of dirty and preserved registers.
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++) {
    _dirtyRegs[group] = func.usedRegs(group);
    _preservedRegs[group] = func.preservedRegs(group);
  }

  // Exclude stack pointer - this register is never included in saved GP regs.
  _preservedRegs[BaseReg::kGroupGp] &= ~Support::bitMask(archTraits.spRegId());

  // The size and alignment of save/restore area of registers for each significant register group.
  memcpy(_saveRestoreRegSize, func.callConv()._saveRestoreRegSize, sizeof(_saveRestoreRegSize));
  memcpy(_saveRestoreAlignment, func.callConv()._saveRestoreAlignment, sizeof(_saveRestoreAlignment));

  return kErrorOk;
}

ASMJIT_FAVOR_SIZE Error FuncFrame::finalize() noexcept {
  if (!Environment::isValidArch(arch()))
    return DebugUtils::errored(kErrorInvalidArch);

  const ArchTraits& archTraits = ArchTraits::byArch(arch());

  uint32_t registerSize = _saveRestoreRegSize[BaseReg::kGroupGp];
  uint32_t vectorSize = _saveRestoreRegSize[BaseReg::kGroupVec];
  uint32_t returnAddressSize = archTraits.hasLinkReg() ? 0u : registerSize;

  // The final stack alignment must be updated accordingly to call and local stack alignments.
  uint32_t stackAlignment = _finalStackAlignment;
  ASMJIT_ASSERT(stackAlignment == Support::max(_naturalStackAlignment,
                                               _callStackAlignment,
                                               _localStackAlignment));

  bool hasFP = hasPreservedFP();
  bool hasDA = hasDynamicAlignment();

  uint32_t kSp = archTraits.spRegId();
  uint32_t kFp = archTraits.fpRegId();
  uint32_t kLr = archTraits.linkRegId();

  // Make frame pointer dirty if the function uses it.
  if (hasFP) {
    _dirtyRegs[BaseReg::kGroupGp] |= Support::bitMask(kFp);

    // Currently required by ARM, if this works differently across architectures
    // we would have to generalize most likely in CallConv.
    if (kLr != BaseReg::kIdBad)
      _dirtyRegs[BaseReg::kGroupGp] |= Support::bitMask(kLr);
  }

  // These two are identical if the function doesn't align its stack dynamically.
  uint32_t saRegId = _saRegId;
  if (saRegId == BaseReg::kIdBad)
    saRegId = kSp;

  // Fix stack arguments base-register from SP to FP in case it was not picked
  // before and the function performs dynamic stack alignment.
  if (hasDA && saRegId == kSp)
    saRegId = kFp;

  // Mark as dirty any register but SP if used as SA pointer.
  if (saRegId != kSp)
    _dirtyRegs[BaseReg::kGroupGp] |= Support::bitMask(saRegId);

  _spRegId = uint8_t(kSp);
  _saRegId = uint8_t(saRegId);

  // Setup stack size used to save preserved registers.
  uint32_t saveRestoreSizes[2] {};
  for (uint32_t group = 0; group < BaseReg::kGroupVirt; group++)
    saveRestoreSizes[size_t(!archTraits.hasPushPop(group))]
      += Support::alignUp(Support::popcnt(savedRegs(group)) * saveRestoreRegSize(group), saveRestoreAlignment(group));

  _pushPopSaveSize  = uint16_t(saveRestoreSizes[0]);
  _extraRegSaveSize = uint16_t(saveRestoreSizes[1]);

  uint32_t v = 0;                            // The beginning of the stack frame relative to SP after prolog.
  v += callStackSize();                      // Count 'callStackSize'      <- This is used to call functions.
  v  = Support::alignUp(v, stackAlignment);  // Align to function's stack alignment.

  _localStackOffset = v;                     // Store 'localStackOffset'   <- Function's local stack starts here.
  v += localStackSize();                     // Count 'localStackSize'     <- Function's local stack ends here.

  // If the function's stack must be aligned, calculate the alignment necessary
  // to store vector registers, and set `FuncFrame::kAttrAlignedVecSR` to inform
  // PEI that it can use instructions that perform aligned stores/loads.
  if (stackAlignment >= vectorSize && _extraRegSaveSize) {
    addAttributes(FuncFrame::kAttrAlignedVecSR);
    v = Support::alignUp(v, vectorSize);     // Align 'extraRegSaveOffset'.
  }

  _extraRegSaveOffset = v;                   // Store 'extraRegSaveOffset' <- Non-GP save/restore starts here.
  v += _extraRegSaveSize;                    // Count 'extraRegSaveSize'   <- Non-GP save/restore ends here.

  // Calculate if dynamic alignment (DA) slot (stored as offset relative to SP) is required and its offset.
  if (hasDA && !hasFP) {
    _daOffset = v;                           // Store 'daOffset'           <- DA pointer would be stored here.
    v += registerSize;                       // Count 'daOffset'.
  }
  else {
    _daOffset = FuncFrame::kTagInvalidOffset;
  }

  // Link Register
  // -------------
  //
  // The stack is aligned after the function call as the return address is
  // stored in a link register. Some architectures may require to always
  // have aligned stack after PUSH/POP operation, which is represented by
  // ArchTraits::stackAlignmentConstraint().
  //
  // No Link Register (X86/X64)
  // --------------------------
  //
  // The return address should be stored after GP save/restore regs. It has
  // the same size as `registerSize` (basically the native register/pointer
  // size). We don't adjust it now as `v` now contains the exact size that the
  // function requires to adjust (call frame + stack frame, vec stack size).
  // The stack (if we consider this size) is misaligned now, as it's always
  // aligned before the function call - when `call()` is executed it pushes
  // the current EIP|RIP onto the stack, and misaligns it by 12 or 8 bytes
  // (depending on the architecture). So count number of bytes needed to align
  // it up to the function's CallFrame (the beginning).
  if (v || hasFuncCalls() || !returnAddressSize)
    v += Support::alignUpDiff(v + pushPopSaveSize() + returnAddressSize, stackAlignment);

  _pushPopSaveOffset = v;                    // Store 'pushPopSaveOffset'  <- Function's push/pop save/restore starts here.
  _stackAdjustment = v;                      // Store 'stackAdjustment'    <- SA used by 'add SP, SA' and 'sub SP, SA'.
  v += _pushPopSaveSize;                     // Count 'pushPopSaveSize'    <- Function's push/pop save/restore ends here.
  _finalStackSize = v;                       // Store 'finalStackSize'     <- Final stack used by the function.

  if (!archTraits.hasLinkReg())
    v += registerSize;                       // Count 'ReturnAddress'      <- As CALL pushes onto stack.

  // If the function performs dynamic stack alignment then the stack-adjustment must be aligned.
  if (hasDA)
    _stackAdjustment = Support::alignUp(_stackAdjustment, stackAlignment);

  // Calculate where the function arguments start relative to SP.
  _saOffsetFromSP = hasDA ? FuncFrame::kTagInvalidOffset : v;

  // Calculate where the function arguments start relative to FP or user-provided register.
  _saOffsetFromSA = hasFP ? returnAddressSize + registerSize      // Return address + frame pointer.
                          : returnAddressSize + _pushPopSaveSize; // Return address + all push/pop regs.

  return kErrorOk;
}

// ============================================================================
// [asmjit::FuncArgsAssignment]
// ============================================================================

ASMJIT_FAVOR_SIZE Error FuncArgsAssignment::updateFuncFrame(FuncFrame& frame) const noexcept {
  uint32_t arch = frame.arch();
  const FuncDetail* func = funcDetail();

  if (!func)
    return DebugUtils::errored(kErrorInvalidState);

  RAConstraints constraints;
  ASMJIT_PROPAGATE(constraints.init(arch));

  FuncArgsContext ctx;
  ASMJIT_PROPAGATE(ctx.initWorkData(frame, *this, &constraints));
  ASMJIT_PROPAGATE(ctx.markDstRegsDirty(frame));
  ASMJIT_PROPAGATE(ctx.markScratchRegs(frame));
  ASMJIT_PROPAGATE(ctx.markStackArgsReg(frame));
  return kErrorOk;
}

ASMJIT_END_NAMESPACE
