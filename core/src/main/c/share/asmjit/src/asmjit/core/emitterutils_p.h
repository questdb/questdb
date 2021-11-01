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

#ifndef ASMJIT_CORE_EMITTERUTILS_P_H_INCLUDED
#define ASMJIT_CORE_EMITTERUTILS_P_H_INCLUDED

#include "../core/emitter.h"
#include "../core/operand.h"

ASMJIT_BEGIN_NAMESPACE

class BaseAssembler;

//! \cond INTERNAL
//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::EmitterUtils]
// ============================================================================

namespace EmitterUtils {

static const Operand_ noExt[3] {};

enum kOpIndex {
  kOp3 = 0,
  kOp4 = 1,
  kOp5 = 2
};

static ASMJIT_INLINE uint32_t opCountFromEmitArgs(const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt) noexcept {
  uint32_t opCount = 0;

  if (opExt[kOp3].isNone()) {
    if (!o0.isNone()) opCount = 1;
    if (!o1.isNone()) opCount = 2;
    if (!o2.isNone()) opCount = 3;
  }
  else {
    opCount = 4;
    if (!opExt[kOp4].isNone()) {
      opCount = 5 + uint32_t(!opExt[kOp5].isNone());
    }
  }

  return opCount;
}

static ASMJIT_INLINE void opArrayFromEmitArgs(Operand_ dst[Globals::kMaxOpCount], const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt) noexcept {
  dst[0].copyFrom(o0);
  dst[1].copyFrom(o1);
  dst[2].copyFrom(o2);
  dst[3].copyFrom(opExt[kOp3]);
  dst[4].copyFrom(opExt[kOp4]);
  dst[5].copyFrom(opExt[kOp5]);
}

#ifndef ASMJIT_NO_LOGGING
enum : uint32_t {
  // Has to be big to be able to hold all metadata compiler can assign to a
  // single instruction.
  kMaxInstLineSize = 44,
  kMaxBinarySize = 26
};

Error formatLine(String& sb, const uint8_t* binData, size_t binSize, size_t dispSize, size_t immSize, const char* comment) noexcept;

void logLabelBound(BaseAssembler* self, const Label& label) noexcept;

void logInstructionEmitted(
  BaseAssembler* self,
  uint32_t instId, uint32_t options, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt,
  uint32_t relSize, uint32_t immSize, uint8_t* afterCursor);

Error logInstructionFailed(
  BaseAssembler* self,
  Error err, uint32_t instId, uint32_t options, const Operand_& o0, const Operand_& o1, const Operand_& o2, const Operand_* opExt);
#endif

}

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_EMITTERUTILS_P_H_INCLUDED

