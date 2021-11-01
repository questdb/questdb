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
#include "../core/misc_p.h"

#if !defined(ASMJIT_NO_X86)
  #include "../x86/x86archtraits_p.h"
#endif

#ifdef ASMJIT_BUILD_ARM
  #include "../arm/armarchtraits_p.h"
#endif

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::ArchTraits]
// ============================================================================

static const constexpr ArchTraits noArchTraits = {
  // SP/FP/LR/PC.
  0xFF, 0xFF, 0xFF, 0xFF,

  // Reserved,
  { 0, 0, 0 },

  // HW stack alignment.
  0,

  // Min/Max stack offset.
  0, 0,

  // ISA features [Gp, Vec, Other0, Other1].
  { 0, 0, 0, 0},

  // RegTypeToSignature.
  { { 0 } },

  // RegTypeToTypeId.
  { 0 },

  // TypeIdToRegType.
  { 0 },

  // Word names of 8-bit, 16-bit, 32-bit, and 64-bit quantities.
  {
    ISAWordNameId::kByte,
    ISAWordNameId::kHalf,
    ISAWordNameId::kWord,
    ISAWordNameId::kQuad
  }
};

ASMJIT_VARAPI const ArchTraits _archTraits[Environment::kArchCount] = {
  // No architecture.
  noArchTraits,

  // X86/X86 architectures.
#if !defined(ASMJIT_NO_X86)
  x86::x86ArchTraits,
  x86::x64ArchTraits,
#else
  noArchTraits,
  noArchTraits,
#endif

  // RISCV32/RISCV64 architectures.
  noArchTraits,
  noArchTraits,

  // ARM architecture
  noArchTraits,

  // AArch64 architecture.
#ifdef ASMJIT_BUILD_ARM
  arm::a64ArchTraits,
#else
  noArchTraits,
#endif

  // ARM/Thumb architecture.
  noArchTraits,

  // Reserved.
  noArchTraits,

  // MIPS32/MIPS64
  noArchTraits,
  noArchTraits
};

// ============================================================================
// [asmjit::ArchUtils]
// ============================================================================

ASMJIT_FAVOR_SIZE Error ArchUtils::typeIdToRegInfo(uint32_t arch, uint32_t typeId, uint32_t* typeIdOut, RegInfo* regInfoOut) noexcept {
  const ArchTraits& archTraits = ArchTraits::byArch(arch);

  // Passed RegType instead of TypeId?
  if (typeId <= BaseReg::kTypeMax)
    typeId = archTraits.regTypeToTypeId(typeId);

  if (ASMJIT_UNLIKELY(!Type::isValid(typeId)))
    return DebugUtils::errored(kErrorInvalidTypeId);

  // First normalize architecture dependent types.
  if (Type::isAbstract(typeId)) {
    bool is32Bit = Environment::is32Bit(arch);
    if (typeId == Type::kIdIntPtr)
      typeId = is32Bit ? Type::kIdI32 : Type::kIdI64;
    else
      typeId = is32Bit ? Type::kIdU32 : Type::kIdU64;
  }

  // Type size helps to construct all groups of registers.
  // TypeId is invalid if the size is zero.
  uint32_t size = Type::sizeOf(typeId);
  if (ASMJIT_UNLIKELY(!size))
    return DebugUtils::errored(kErrorInvalidTypeId);

  if (ASMJIT_UNLIKELY(typeId == Type::kIdF80))
    return DebugUtils::errored(kErrorInvalidUseOfF80);

  uint32_t regType = 0;
  if (typeId >= Type::_kIdBaseStart && typeId < Type::_kIdVec32Start) {
    regType = archTraits._typeIdToRegType[typeId - Type::_kIdBaseStart];
    if (!regType) {
      if (typeId == Type::kIdI64 || typeId == Type::kIdU64)
        return DebugUtils::errored(kErrorInvalidUseOfGpq);
      else
        return DebugUtils::errored(kErrorInvalidTypeId);
    }
  }
  else {
    if (size <= 8 && archTraits._regInfo[BaseReg::kTypeVec64].isValid())
      regType = BaseReg::kTypeVec64;
    else if (size <= 16 && archTraits._regInfo[BaseReg::kTypeVec128].isValid())
      regType = BaseReg::kTypeVec128;
    else if (size == 32 && archTraits._regInfo[BaseReg::kTypeVec256].isValid())
      regType = BaseReg::kTypeVec256;
    else if (archTraits._regInfo[BaseReg::kTypeVec512].isValid())
      regType = BaseReg::kTypeVec512;
    else
      return DebugUtils::errored(kErrorInvalidTypeId);
  }

  *typeIdOut = typeId;
  regInfoOut->reset(archTraits.regTypeToSignature(regType));
  return kErrorOk;
}

ASMJIT_END_NAMESPACE
