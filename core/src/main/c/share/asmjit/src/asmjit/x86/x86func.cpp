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

#include "../x86/x86func_p.h"
#include "../x86/x86emithelper_p.h"
#include "../x86/x86operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [asmjit::x86::FuncInternal - Init]
// ============================================================================

namespace FuncInternal {

static inline bool shouldThreatAsCDeclIn64BitMode(uint32_t ccId) noexcept {
  return ccId == CallConv::kIdCDecl ||
         ccId == CallConv::kIdStdCall ||
         ccId == CallConv::kIdThisCall ||
         ccId == CallConv::kIdFastCall ||
         ccId == CallConv::kIdRegParm1 ||
         ccId == CallConv::kIdRegParm2 ||
         ccId == CallConv::kIdRegParm3;
}

ASMJIT_FAVOR_SIZE Error initCallConv(CallConv& cc, uint32_t ccId, const Environment& environment) noexcept {
  constexpr uint32_t kGroupGp   = Reg::kGroupGp;
  constexpr uint32_t kGroupVec  = Reg::kGroupVec;
  constexpr uint32_t kGroupMm   = Reg::kGroupMm;
  constexpr uint32_t kGroupKReg = Reg::kGroupKReg;

  constexpr uint32_t kZax = Gp::kIdAx;
  constexpr uint32_t kZbx = Gp::kIdBx;
  constexpr uint32_t kZcx = Gp::kIdCx;
  constexpr uint32_t kZdx = Gp::kIdDx;
  constexpr uint32_t kZsp = Gp::kIdSp;
  constexpr uint32_t kZbp = Gp::kIdBp;
  constexpr uint32_t kZsi = Gp::kIdSi;
  constexpr uint32_t kZdi = Gp::kIdDi;

  bool winABI = environment.isPlatformWindows() || environment.isAbiMSVC();

  cc.setArch(environment.arch());
  cc.setSaveRestoreRegSize(Reg::kGroupVec, 16);
  cc.setSaveRestoreRegSize(Reg::kGroupMm, 8);
  cc.setSaveRestoreRegSize(Reg::kGroupKReg, 8);
  cc.setSaveRestoreAlignment(Reg::kGroupVec, 16);
  cc.setSaveRestoreAlignment(Reg::kGroupMm, 8);
  cc.setSaveRestoreAlignment(Reg::kGroupKReg, 8);

  if (environment.is32Bit()) {
    bool isStandardCallConv = true;

    cc.setSaveRestoreRegSize(Reg::kGroupGp, 4);
    cc.setSaveRestoreAlignment(Reg::kGroupGp, 4);

    cc.setPreservedRegs(Reg::kGroupGp, Support::bitMask(Gp::kIdBx, Gp::kIdSp, Gp::kIdBp, Gp::kIdSi, Gp::kIdDi));
    cc.setNaturalStackAlignment(4);

    switch (ccId) {
      case CallConv::kIdCDecl:
        break;

      case CallConv::kIdStdCall:
        cc.setFlags(CallConv::kFlagCalleePopsStack);
        break;

      case CallConv::kIdFastCall:
        cc.setFlags(CallConv::kFlagCalleePopsStack);
        cc.setPassedOrder(kGroupGp, kZcx, kZdx);
        break;

      case CallConv::kIdVectorCall:
        cc.setFlags(CallConv::kFlagCalleePopsStack);
        cc.setPassedOrder(kGroupGp, kZcx, kZdx);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3, 4, 5);
        break;

      case CallConv::kIdThisCall:
        // NOTE: Even MINGW (starting with GCC 4.7.0) now uses __thiscall on MS Windows,
        // so we won't bail to any other calling convention if __thiscall was specified.
        if (winABI) {
          cc.setFlags(CallConv::kFlagCalleePopsStack);
          cc.setPassedOrder(kGroupGp, kZcx);
        }
        else {
          ccId = CallConv::kIdCDecl;
        }
        break;

      case CallConv::kIdRegParm1:
        cc.setPassedOrder(kGroupGp, kZax);
        break;

      case CallConv::kIdRegParm2:
        cc.setPassedOrder(kGroupGp, kZax, kZdx);
        break;

      case CallConv::kIdRegParm3:
        cc.setPassedOrder(kGroupGp, kZax, kZdx, kZcx);
        break;

      case CallConv::kIdLightCall2:
      case CallConv::kIdLightCall3:
      case CallConv::kIdLightCall4: {
        uint32_t n = (ccId - CallConv::kIdLightCall2) + 2;

        cc.setFlags(CallConv::kFlagPassFloatsByVec);
        cc.setPassedOrder(kGroupGp, kZax, kZdx, kZcx, kZsi, kZdi);
        cc.setPassedOrder(kGroupMm, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPassedOrder(kGroupKReg, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPreservedRegs(kGroupGp, Support::lsbMask<uint32_t>(8));
        cc.setPreservedRegs(kGroupVec, Support::lsbMask<uint32_t>(8) & ~Support::lsbMask<uint32_t>(n));

        cc.setNaturalStackAlignment(16);
        isStandardCallConv = false;
        break;
      }

      default:
        return DebugUtils::errored(kErrorInvalidArgument);
    }

    if (isStandardCallConv) {
      // MMX arguments is something where compiler vendors disagree. For example
      // GCC and MSVC would pass first three via registers and the rest via stack,
      // however Clang passes all via stack. Returning MMX registers is even more
      // fun, where GCC uses MM0, but Clang uses EAX:EDX pair. I'm not sure it's
      // something we should be worried about as MMX is deprecated anyway.
      cc.setPassedOrder(kGroupMm, 0, 1, 2);

      // Vector arguments (XMM|YMM|ZMM) are passed via registers. However, if the
      // function is variadic then they have to be passed via stack.
      cc.setPassedOrder(kGroupVec, 0, 1, 2);

      // Functions with variable arguments always use stack for MM and vector
      // arguments.
      cc.addFlags(CallConv::kFlagPassVecByStackIfVA);
    }

    if (ccId == CallConv::kIdCDecl) {
      cc.addFlags(CallConv::kFlagVarArgCompatible);
    }
  }
  else {
    cc.setSaveRestoreRegSize(Reg::kGroupGp, 8);
    cc.setSaveRestoreAlignment(Reg::kGroupGp, 8);

    // Preprocess the calling convention into a common id as many conventions
    // are normally ignored even by C/C++ compilers and treated as `__cdecl`.
    if (shouldThreatAsCDeclIn64BitMode(ccId))
      ccId = winABI ? CallConv::kIdX64Windows : CallConv::kIdX64SystemV;

    switch (ccId) {
      case CallConv::kIdX64SystemV: {
        cc.setFlags(CallConv::kFlagPassFloatsByVec |
                    CallConv::kFlagPassMmxByXmm    |
                    CallConv::kFlagVarArgCompatible);
        cc.setNaturalStackAlignment(16);
        cc.setRedZoneSize(128);
        cc.setPassedOrder(kGroupGp, kZdi, kZsi, kZdx, kZcx, 8, 9);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPreservedRegs(kGroupGp, Support::bitMask(kZbx, kZsp, kZbp, 12, 13, 14, 15));
        break;
      }

      case CallConv::kIdX64Windows: {
        cc.setStrategy(CallConv::kStrategyX64Windows);
        cc.setFlags(CallConv::kFlagPassFloatsByVec |
                    CallConv::kFlagIndirectVecArgs |
                    CallConv::kFlagPassMmxByGp     |
                    CallConv::kFlagVarArgCompatible);
        cc.setNaturalStackAlignment(16);
        // Maximum 4 arguments in registers, each adds 8 bytes to the spill zone.
        cc.setSpillZoneSize(4 * 8);
        cc.setPassedOrder(kGroupGp, kZcx, kZdx, 8, 9);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3);
        cc.setPreservedRegs(kGroupGp, Support::bitMask(kZbx, kZsp, kZbp, kZsi, kZdi, 12, 13, 14, 15));
        cc.setPreservedRegs(kGroupVec, Support::bitMask(6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
        break;
      }

      case CallConv::kIdVectorCall: {
        cc.setStrategy(CallConv::kStrategyX64VectorCall);
        cc.setFlags(CallConv::kFlagPassFloatsByVec |
                    CallConv::kFlagPassMmxByGp     );
        cc.setNaturalStackAlignment(16);
        // Maximum 6 arguments in registers, each adds 8 bytes to the spill zone.
        cc.setSpillZoneSize(6 * 8);
        cc.setPassedOrder(kGroupGp, kZcx, kZdx, 8, 9);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3, 4, 5);
        cc.setPreservedRegs(kGroupGp, Support::bitMask(kZbx, kZsp, kZbp, kZsi, kZdi, 12, 13, 14, 15));
        cc.setPreservedRegs(kGroupVec, Support::bitMask(6, 7, 8, 9, 10, 11, 12, 13, 14, 15));
        break;
      }

      case CallConv::kIdLightCall2:
      case CallConv::kIdLightCall3:
      case CallConv::kIdLightCall4: {
        uint32_t n = (ccId - CallConv::kIdLightCall2) + 2;

        cc.setFlags(CallConv::kFlagPassFloatsByVec);
        cc.setNaturalStackAlignment(16);
        cc.setPassedOrder(kGroupGp, kZax, kZdx, kZcx, kZsi, kZdi);
        cc.setPassedOrder(kGroupMm, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPassedOrder(kGroupVec, 0, 1, 2, 3, 4, 5, 6, 7);
        cc.setPassedOrder(kGroupKReg, 0, 1, 2, 3, 4, 5, 6, 7);

        cc.setPreservedRegs(kGroupGp, Support::lsbMask<uint32_t>(16));
        cc.setPreservedRegs(kGroupVec, ~Support::lsbMask<uint32_t>(n));
        break;
      }

      default:
        return DebugUtils::errored(kErrorInvalidArgument);
    }
  }

  cc.setId(ccId);
  return kErrorOk;
}

ASMJIT_FAVOR_SIZE void unpackValues(FuncDetail& func, FuncValuePack& pack) noexcept {
  uint32_t typeId = pack[0].typeId();
  switch (typeId) {
    case Type::kIdI64:
    case Type::kIdU64: {
      if (Environment::is32Bit(func.callConv().arch())) {
        // Convert a 64-bit return value to two 32-bit return values.
        pack[0].initTypeId(Type::kIdU32);
        pack[1].initTypeId(typeId - 2);
        break;
      }
      break;
    }
  }
}

ASMJIT_FAVOR_SIZE Error initFuncDetail(FuncDetail& func, const FuncSignature& signature, uint32_t registerSize) noexcept {
  const CallConv& cc = func.callConv();
  uint32_t arch = cc.arch();
  uint32_t stackOffset = cc._spillZoneSize;
  uint32_t argCount = func.argCount();

  // Up to two return values can be returned in GP registers.
  static const uint8_t gpReturnIndexes[4] = {
    uint8_t(Gp::kIdAx),
    uint8_t(Gp::kIdDx),
    uint8_t(BaseReg::kIdBad),
    uint8_t(BaseReg::kIdBad)
  };

  if (func.hasRet()) {
    unpackValues(func, func._rets);
    for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
      uint32_t typeId = func._rets[valueIndex].typeId();

      // Terminate at the first void type (end of the pack).
      if (!typeId)
        break;

      switch (typeId) {
        case Type::kIdI64:
        case Type::kIdU64: {
          if (gpReturnIndexes[valueIndex] != BaseReg::kIdBad)
            func._rets[valueIndex].initReg(Reg::kTypeGpq, gpReturnIndexes[valueIndex], typeId);
          else
            return DebugUtils::errored(kErrorInvalidState);
          break;
        }

        case Type::kIdI8:
        case Type::kIdI16:
        case Type::kIdI32: {
          if (gpReturnIndexes[valueIndex] != BaseReg::kIdBad)
            func._rets[valueIndex].initReg(Reg::kTypeGpd, gpReturnIndexes[valueIndex], Type::kIdI32);
          else
            return DebugUtils::errored(kErrorInvalidState);
          break;
        }

        case Type::kIdU8:
        case Type::kIdU16:
        case Type::kIdU32: {
          if (gpReturnIndexes[valueIndex] != BaseReg::kIdBad)
            func._rets[valueIndex].initReg(Reg::kTypeGpd, gpReturnIndexes[valueIndex], Type::kIdU32);
          else
            return DebugUtils::errored(kErrorInvalidState);
          break;
        }

        case Type::kIdF32:
        case Type::kIdF64: {
          uint32_t regType = Environment::is32Bit(arch) ? Reg::kTypeSt : Reg::kTypeXmm;
          func._rets[valueIndex].initReg(regType, valueIndex, typeId);
          break;
        }

        case Type::kIdF80: {
          // 80-bit floats are always returned by FP0.
          func._rets[valueIndex].initReg(Reg::kTypeSt, valueIndex, typeId);
          break;
        }

        case Type::kIdMmx32:
        case Type::kIdMmx64: {
          // MM registers are returned through XMM (SystemV) or GPQ (Win64).
          uint32_t regType = Reg::kTypeMm;
          uint32_t regIndex = valueIndex;
          if (Environment::is64Bit(arch)) {
            regType = cc.strategy() == CallConv::kStrategyDefault ? Reg::kTypeXmm : Reg::kTypeGpq;
            regIndex = cc.strategy() == CallConv::kStrategyDefault ? valueIndex : gpReturnIndexes[valueIndex];

            if (regIndex == BaseReg::kIdBad)
              return DebugUtils::errored(kErrorInvalidState);
          }

          func._rets[valueIndex].initReg(regType, regIndex, typeId);
          break;
        }

        default: {
          func._rets[valueIndex].initReg(vecTypeIdToRegType(typeId), valueIndex, typeId);
          break;
        }
      }
    }
  }

  switch (cc.strategy()) {
    case CallConv::kStrategyDefault: {
      uint32_t gpzPos = 0;
      uint32_t vecPos = 0;

      for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
        unpackValues(func, func._args[argIndex]);

        for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
          FuncValue& arg = func._args[argIndex][valueIndex];

          // Terminate if there are no more arguments in the pack.
          if (!arg)
            break;

          uint32_t typeId = arg.typeId();

          if (Type::isInt(typeId)) {
            uint32_t regId = BaseReg::kIdBad;

            if (gpzPos < CallConv::kMaxRegArgsPerGroup)
              regId = cc._passedOrder[Reg::kGroupGp].id[gpzPos];

            if (regId != BaseReg::kIdBad) {
              uint32_t regType = (typeId <= Type::kIdU32) ? Reg::kTypeGpd : Reg::kTypeGpq;
              arg.assignRegData(regType, regId);
              func.addUsedRegs(Reg::kGroupGp, Support::bitMask(regId));
              gpzPos++;
            }
            else {
              uint32_t size = Support::max<uint32_t>(Type::sizeOf(typeId), registerSize);
              arg.assignStackOffset(int32_t(stackOffset));
              stackOffset += size;
            }
            continue;
          }

          if (Type::isFloat(typeId) || Type::isVec(typeId)) {
            uint32_t regId = BaseReg::kIdBad;

            if (vecPos < CallConv::kMaxRegArgsPerGroup)
              regId = cc._passedOrder[Reg::kGroupVec].id[vecPos];

            if (Type::isFloat(typeId)) {
              // If this is a float, but `kFlagPassFloatsByVec` is false, we have
              // to use stack instead. This should be only used by 32-bit calling
              // conventions.
              if (!cc.hasFlag(CallConv::kFlagPassFloatsByVec))
                regId = BaseReg::kIdBad;
            }
            else {
              // Pass vector registers via stack if this is a variable arguments
              // function. This should be only used by 32-bit calling conventions.
              if (signature.hasVarArgs() && cc.hasFlag(CallConv::kFlagPassVecByStackIfVA))
                regId = BaseReg::kIdBad;
            }

            if (regId != BaseReg::kIdBad) {
              arg.initTypeId(typeId);
              arg.assignRegData(vecTypeIdToRegType(typeId), regId);
              func.addUsedRegs(Reg::kGroupVec, Support::bitMask(regId));
              vecPos++;
            }
            else {
              uint32_t size = Type::sizeOf(typeId);
              arg.assignStackOffset(int32_t(stackOffset));
              stackOffset += size;
            }
            continue;
          }
        }
      }
      break;
    }

    case CallConv::kStrategyX64Windows:
    case CallConv::kStrategyX64VectorCall: {
      // Both X64 and VectorCall behave similarly - arguments are indexed
      // from left to right. The position of the argument determines in
      // which register the argument is allocated, so it's either GP or
      // one of XMM/YMM/ZMM registers.
      //
      //       [       X64       ] [VecCall]
      // Index: #0   #1   #2   #3   #4   #5
      //
      // GP   : RCX  RDX  R8   R9
      // VEC  : XMM0 XMM1 XMM2 XMM3 XMM4 XMM5
      //
      // For example function `f(int a, double b, int c, double d)` will be:
      //
      //        (a)  (b)  (c)  (d)
      //        RCX  XMM1 R8   XMM3
      //
      // Unused vector registers are used by HVA.
      bool isVectorCall = (cc.strategy() == CallConv::kStrategyX64VectorCall);

      for (uint32_t argIndex = 0; argIndex < argCount; argIndex++) {
        unpackValues(func, func._args[argIndex]);

        for (uint32_t valueIndex = 0; valueIndex < Globals::kMaxValuePack; valueIndex++) {
          FuncValue& arg = func._args[argIndex][valueIndex];

          // Terminate if there are no more arguments in the pack.
          if (!arg)
            break;

          uint32_t typeId = arg.typeId();
          uint32_t size = Type::sizeOf(typeId);

          if (Type::isInt(typeId) || Type::isMmx(typeId)) {
            uint32_t regId = BaseReg::kIdBad;

            if (argIndex < CallConv::kMaxRegArgsPerGroup)
              regId = cc._passedOrder[Reg::kGroupGp].id[argIndex];

            if (regId != BaseReg::kIdBad) {
              uint32_t regType = (size <= 4 && !Type::isMmx(typeId)) ? Reg::kTypeGpd : Reg::kTypeGpq;
              arg.assignRegData(regType, regId);
              func.addUsedRegs(Reg::kGroupGp, Support::bitMask(regId));
            }
            else {
              arg.assignStackOffset(int32_t(stackOffset));
              stackOffset += 8;
            }
            continue;
          }

          if (Type::isFloat(typeId) || Type::isVec(typeId)) {
            uint32_t regId = BaseReg::kIdBad;

            if (argIndex < CallConv::kMaxRegArgsPerGroup)
              regId = cc._passedOrder[Reg::kGroupVec].id[argIndex];

            if (regId != BaseReg::kIdBad) {
              // X64-ABI doesn't allow vector types (XMM|YMM|ZMM) to be passed
              // via registers, however, VectorCall was designed for that purpose.
              if (Type::isFloat(typeId) || isVectorCall) {
                uint32_t regType = vecTypeIdToRegType(typeId);
                arg.assignRegData(regType, regId);
                func.addUsedRegs(Reg::kGroupVec, Support::bitMask(regId));
                continue;
              }
            }

            // Passed via stack if the argument is float/double or indirectly.
            // The trap is - if the argument is passed indirectly, the address
            // can be passed via register, if the argument's index has GP one.
            if (Type::isFloat(typeId)) {
              arg.assignStackOffset(int32_t(stackOffset));
            }
            else {
              uint32_t gpRegId = cc._passedOrder[Reg::kGroupGp].id[argIndex];
              if (gpRegId != BaseReg::kIdBad)
                arg.assignRegData(Reg::kTypeGpq, gpRegId);
              else
                arg.assignStackOffset(int32_t(stackOffset));
              arg.addFlags(FuncValue::kFlagIsIndirect);
            }

            // Always 8 bytes (float/double/pointer).
            stackOffset += 8;
            continue;
          }
        }
      }
      break;
    }
  }

  func._argStackSize = stackOffset;
  return kErrorOk;
}

} // {FuncInternal}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86
