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

#ifndef ASMJIT_X86_X86FUNC_P_H_INCLUDED
#define ASMJIT_X86_X86FUNC_P_H_INCLUDED

#include "../core/func.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \cond INTERNAL
//! \addtogroup asmjit_x86
//! \{

// ============================================================================
// [asmjit::x86::FuncInternal]
// ============================================================================

//! X86-specific function API (calling conventions and other utilities).
namespace FuncInternal {

//! Initialize `CallConv` structure (X86 specific).
Error initCallConv(CallConv& cc, uint32_t ccId, const Environment& environment) noexcept;

//! Initialize `FuncDetail` (X86 specific).
Error initFuncDetail(FuncDetail& func, const FuncSignature& signature, uint32_t registerSize) noexcept;

} // {FuncInternal}

//! \}
//! \endcond

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86FUNC_P_H_INCLUDED
