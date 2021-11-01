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
#include "../core/inst.h"

#if !defined(ASMJIT_NO_X86)
  #include "../x86/x86instapi_p.h"
#endif

#ifdef ASMJIT_BUILD_ARM
  #include "../arm/a64instapi_p.h"
#endif

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::InstAPI - Text]
// ============================================================================

#ifndef ASMJIT_NO_TEXT
Error InstAPI::instIdToString(uint32_t arch, uint32_t instId, String& output) noexcept {
#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::InstInternal::instIdToString(arch, instId, output);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isArchAArch64(arch))
    return a64::InstInternal::instIdToString(arch, instId, output);
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}

uint32_t InstAPI::stringToInstId(uint32_t arch, const char* s, size_t len) noexcept {
#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::InstInternal::stringToInstId(arch, s, len);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isArchAArch64(arch))
    return a64::InstInternal::stringToInstId(arch, s, len);
#endif

  return 0;
}
#endif // !ASMJIT_NO_TEXT

// ============================================================================
// [asmjit::InstAPI - Validate]
// ============================================================================

#ifndef ASMJIT_NO_VALIDATION
Error InstAPI::validate(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, uint32_t validationFlags) noexcept {
#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::InstInternal::validate(arch, inst, operands, opCount, validationFlags);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isArchAArch64(arch))
    return a64::InstInternal::validate(arch, inst, operands, opCount, validationFlags);
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}
#endif // !ASMJIT_NO_VALIDATION

// ============================================================================
// [asmjit::InstAPI - QueryRWInfo]
// ============================================================================

#ifndef ASMJIT_NO_INTROSPECTION
Error InstAPI::queryRWInfo(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, InstRWInfo* out) noexcept {
  if (ASMJIT_UNLIKELY(opCount > Globals::kMaxOpCount))
    return DebugUtils::errored(kErrorInvalidArgument);

#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::InstInternal::queryRWInfo(arch, inst, operands, opCount, out);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isArchAArch64(arch))
    return a64::InstInternal::queryRWInfo(arch, inst, operands, opCount, out);
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}
#endif // !ASMJIT_NO_INTROSPECTION

// ============================================================================
// [asmjit::InstAPI - QueryFeatures]
// ============================================================================

#ifndef ASMJIT_NO_INTROSPECTION
Error InstAPI::queryFeatures(uint32_t arch, const BaseInst& inst, const Operand_* operands, size_t opCount, BaseFeatures* out) noexcept {
#if !defined(ASMJIT_NO_X86)
  if (Environment::isFamilyX86(arch))
    return x86::InstInternal::queryFeatures(arch, inst, operands, opCount, out);
#endif

#ifdef ASMJIT_BUILD_ARM
  if (Environment::isArchAArch64(arch))
    return a64::InstInternal::queryFeatures(arch, inst, operands, opCount, out);
#endif

  return DebugUtils::errored(kErrorInvalidArch);
}
#endif // !ASMJIT_NO_INTROSPECTION

ASMJIT_END_NAMESPACE
