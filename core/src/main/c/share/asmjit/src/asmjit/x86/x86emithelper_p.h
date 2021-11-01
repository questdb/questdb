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

#ifndef ASMJIT_X86_X86EMITHELPER_P_H_INCLUDED
#define ASMJIT_X86_X86EMITHELPER_P_H_INCLUDED

#include "../core/api-config.h"

#include "../core/emithelper_p.h"
#include "../core/func.h"
#include "../x86/x86emitter.h"
#include "../x86/x86operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \cond INTERNAL
//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::EmitHelper]
// ============================================================================

static ASMJIT_INLINE uint32_t vecTypeIdToRegType(uint32_t typeId) noexcept {
  return typeId <= Type::_kIdVec128End ? Reg::kTypeXmm :
         typeId <= Type::_kIdVec256End ? Reg::kTypeYmm : Reg::kTypeZmm;
}

class EmitHelper : public BaseEmitHelper {
public:
  bool _avxEnabled;
  bool _avx512Enabled;

  inline explicit EmitHelper(BaseEmitter* emitter = nullptr, bool avxEnabled = false, bool avx512Enabled = false) noexcept
    : BaseEmitHelper(emitter),
      _avxEnabled(avxEnabled || avx512Enabled),
      _avx512Enabled(avx512Enabled) {}

  Error emitRegMove(
    const Operand_& dst_,
    const Operand_& src_, uint32_t typeId, const char* comment = nullptr) override;

  Error emitArgMove(
    const BaseReg& dst_, uint32_t dstTypeId,
    const Operand_& src_, uint32_t srcTypeId, const char* comment = nullptr) override;

  Error emitRegSwap(
    const BaseReg& a,
    const BaseReg& b, const char* comment = nullptr) override;

  Error emitProlog(const FuncFrame& frame);
  Error emitEpilog(const FuncFrame& frame);
};

//! \}
//! \endcond

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86EMITHELPER_P_H_INCLUDED
