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

#ifndef ASMJIT_X86_X86INSTAPI_P_H_INCLUDED
#define ASMJIT_X86_X86INSTAPI_P_H_INCLUDED

#include "../core/inst.h"
#include "../core/operand.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

//! \cond INTERNAL
//! \addtogroup asmjit_x86
//! \{

namespace InstInternal {

#ifndef ASMJIT_NO_TEXT
Error instIdToString(uint32_t arch, uint32_t instId, String& output) noexcept;
uint32_t stringToInstId(uint32_t arch, const char* s, size_t len) noexcept;
#endif // !ASMJIT_NO_TEXT

#ifndef ASMJIT_NO_VALIDATION
Error validate(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, uint32_t validationFlags) noexcept;
#endif // !ASMJIT_NO_VALIDATION

#ifndef ASMJIT_NO_INTROSPECTION
Error queryRWInfo(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, InstRWInfo* out) noexcept;
Error queryFeatures(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, BaseFeatures* out) noexcept;
#endif // !ASMJIT_NO_INTROSPECTION

} // {InstInternal}

//! \}
//! \endcond

ASMJIT_END_SUB_NAMESPACE

#endif // ASMJIT_X86_X86INSTAPI_P_H_INCLUDED
