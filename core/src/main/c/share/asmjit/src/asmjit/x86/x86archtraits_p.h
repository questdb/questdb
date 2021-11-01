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

#ifndef ASMJIT_X86_X86ARCHTRAITS_P_H_INCLUDED
#define ASMJIT_X86_X86ARCHTRAITS_P_H_INCLUDED

#include "../core/archtraits.h"
#include "../core/misc_p.h"
#include "../x86/x86operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \cond INTERNAL
//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::x86ArchTraits
// ============================================================================

static const constexpr ArchTraits x86ArchTraits = {
  // SP/FP/LR/PC.
  Gp::kIdSp, Gp::kIdBp, 0xFF, 0xFF,

  // Reserved.
  { 0, 0, 0 },

  // HW stack alignment.
  1,

  // Min/Max stack offset
  0x7FFFFFFFu, 0x7FFFFFFFu,

  // ISA features [Gp, Vec, Other0, Other1].
  { ArchTraits::kIsaFeatureSwap | ArchTraits::kIsaFeaturePushPop, 0, 0, 0 },

  // RegInfo.
  #define V(index) { x86::RegTraits<index>::kSignature }
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // RegTypeToTypeId.
  #define V(index) x86::RegTraits<index>::kTypeId
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // TypeIdToRegType.
  #define V(index) (index + Type::_kIdBaseStart == Type::kIdI8      ? Reg::kTypeGpbLo : \
                    index + Type::_kIdBaseStart == Type::kIdU8      ? Reg::kTypeGpbLo : \
                    index + Type::_kIdBaseStart == Type::kIdI16     ? Reg::kTypeGpw   : \
                    index + Type::_kIdBaseStart == Type::kIdU16     ? Reg::kTypeGpw   : \
                    index + Type::_kIdBaseStart == Type::kIdI32     ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdU32     ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdIntPtr  ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdUIntPtr ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdF32     ? Reg::kTypeXmm   : \
                    index + Type::_kIdBaseStart == Type::kIdF64     ? Reg::kTypeXmm   : \
                    index + Type::_kIdBaseStart == Type::kIdMask8   ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask16  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask32  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask64  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMmx32   ? Reg::kTypeMm    : \
                    index + Type::_kIdBaseStart == Type::kIdMmx64   ? Reg::kTypeMm    : Reg::kTypeNone)
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // Word names of 8-bit, 16-bit, 32-bit, and 64-bit quantities.
  {
    ISAWordNameId::kDB,
    ISAWordNameId::kDW,
    ISAWordNameId::kDD,
    ISAWordNameId::kDQ
  }
};

// ============================================================================
// [asmjit::x86::x64ArchTraits
// ============================================================================

static const constexpr ArchTraits x64ArchTraits = {
  // SP/FP/LR/PC.
  Gp::kIdSp, Gp::kIdBp, 0xFF, 0xFF,

  // Reserved.
  { 0, 0, 0 },

  // HW stack alignment.
  1,

  // Min/Max stack offset
  0x7FFFFFFFu, 0x7FFFFFFFu,

  // ISA features [Gp, Vec, Other0, Other1].
  { ArchTraits::kIsaFeatureSwap | ArchTraits::kIsaFeaturePushPop, 0, 0, 0 },

  // RegInfo.
  #define V(index) { x86::RegTraits<index>::kSignature }
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // RegTypeToTypeId.
  #define V(index) x86::RegTraits<index>::kTypeId
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // TypeIdToRegType.
  #define V(index) (index + Type::_kIdBaseStart == Type::kIdI8      ? Reg::kTypeGpbLo : \
                    index + Type::_kIdBaseStart == Type::kIdU8      ? Reg::kTypeGpbLo : \
                    index + Type::_kIdBaseStart == Type::kIdI16     ? Reg::kTypeGpw   : \
                    index + Type::_kIdBaseStart == Type::kIdU16     ? Reg::kTypeGpw   : \
                    index + Type::_kIdBaseStart == Type::kIdI32     ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdU32     ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdI64     ? Reg::kTypeGpq   : \
                    index + Type::_kIdBaseStart == Type::kIdU64     ? Reg::kTypeGpq   : \
                    index + Type::_kIdBaseStart == Type::kIdIntPtr  ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdUIntPtr ? Reg::kTypeGpd   : \
                    index + Type::_kIdBaseStart == Type::kIdF32     ? Reg::kTypeXmm   : \
                    index + Type::_kIdBaseStart == Type::kIdF64     ? Reg::kTypeXmm   : \
                    index + Type::_kIdBaseStart == Type::kIdMask8   ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask16  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask32  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMask64  ? Reg::kTypeKReg  : \
                    index + Type::_kIdBaseStart == Type::kIdMmx32   ? Reg::kTypeMm    : \
                    index + Type::_kIdBaseStart == Type::kIdMmx64   ? Reg::kTypeMm    : Reg::kTypeNone)
  { ASMJIT_LOOKUP_TABLE_32(V, 0) },
  #undef V

  // Word names of 8-bit, 16-bit, 32-bit, and 64-bit quantities.
  {
    ISAWordNameId::kDB,
    ISAWordNameId::kDW,
    ISAWordNameId::kDD,
    ISAWordNameId::kDQ
  }
};

//! \}
//! \endcond

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86ARCHTRAITS_P_H_INCLUDED
