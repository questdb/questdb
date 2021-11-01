
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

#ifndef ASMJIT_CORE_EMITHELPER_P_H_INCLUDED
#define ASMJIT_CORE_EMITHELPER_P_H_INCLUDED

#include "../core/emitter.h"
#include "../core/operand.h"
#include "../core/type.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::BaseEmitHelper]
// ============================================================================

//! Helper class that provides utilities for each supported architecture.
class BaseEmitHelper {
public:
  BaseEmitter* _emitter;

  inline explicit BaseEmitHelper(BaseEmitter* emitter = nullptr) noexcept
    : _emitter(emitter) {}

  inline BaseEmitter* emitter() const noexcept { return _emitter; }
  inline void setEmitter(BaseEmitter* emitter) noexcept { _emitter = emitter; }

  //! Emits a pure move operation between two registers or the same type or
  //! between a register and its home slot. This function does not handle
  //! register conversion.
  virtual Error emitRegMove(
    const Operand_& dst_,
    const Operand_& src_, uint32_t typeId, const char* comment = nullptr) = 0;

  //! Emits swap between two registers.
  virtual Error emitRegSwap(
    const BaseReg& a,
    const BaseReg& b, const char* comment = nullptr) = 0;

  //! Emits move from a function argument (either register or stack) to a register.
  //!
  //! This function can handle the necessary conversion from one argument to
  //! another, and from one register type to another, if it's possible. Any
  //! attempt of conversion that requires third register of a different group
  //! (for example conversion from K to MMX on X86/X64) will fail.
  virtual Error emitArgMove(
    const BaseReg& dst_, uint32_t dstTypeId,
    const Operand_& src_, uint32_t srcTypeId, const char* comment = nullptr) = 0;

  Error emitArgsAssignment(const FuncFrame& frame, const FuncArgsAssignment& args);
};

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_EMITHELPER_P_H_INCLUDED
