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
#if !defined(ASMJIT_NO_X86) && !defined(ASMJIT_NO_COMPILER)

#include "../x86/x86assembler.h"
#include "../x86/x86compiler.h"
#include "../x86/x86rapass_p.h"

ASMJIT_BEGIN_SUB_NAMESPACE(x86)

// ============================================================================
// [asmjit::x86::Compiler - Construction / Destruction]
// ============================================================================

Compiler::Compiler(CodeHolder* code) noexcept : BaseCompiler() {
  if (code)
    code->attach(this);
}
Compiler::~Compiler() noexcept {}

// ============================================================================
// [asmjit::x86::Compiler - Finalize]
// ============================================================================

Error Compiler::finalize() {
  ASMJIT_PROPAGATE(runPasses());
  Assembler a(_code);
  a.addEncodingOptions(encodingOptions());
  a.addValidationOptions(validationOptions());
  return serializeTo(&a);
}

// ============================================================================
// [asmjit::x86::Compiler - Events]
// ============================================================================

Error Compiler::onAttach(CodeHolder* code) noexcept {
  uint32_t arch = code->arch();
  if (!Environment::isFamilyX86(arch))
    return DebugUtils::errored(kErrorInvalidArch);

  ASMJIT_PROPAGATE(Base::onAttach(code));
  Error err = addPassT<X86RAPass>();

  if (ASMJIT_UNLIKELY(err)) {
    onDetach(code);
    return err;
  }

  return kErrorOk;
}

ASMJIT_END_SUB_NAMESPACE

#endif // !ASMJIT_NO_X86 && !ASMJIT_NO_COMPILER
