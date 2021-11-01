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

#ifndef ASMJIT_CORE_API_BUILD_P_H_INCLUDED
#define ASMJIT_CORE_API_BUILD_P_H_INCLUDED

#define ASMJIT_EXPORTS

// Only turn-off these warnings when building asmjit itself.
#ifdef _MSC_VER
  #ifndef _CRT_SECURE_NO_DEPRECATE
    #define _CRT_SECURE_NO_DEPRECATE
  #endif
  #ifndef _CRT_SECURE_NO_WARNINGS
    #define _CRT_SECURE_NO_WARNINGS
  #endif
#endif

// Dependencies only required for asmjit build, but never exposed through public headers.
#ifdef _WIN32
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <windows.h>
#endif

// ============================================================================
// [asmjit::Build - Globals - Build-Only]
// ============================================================================

#include "./api-config.h"

#if !defined(ASMJIT_BUILD_DEBUG) && defined(__GNUC__) && !defined(__clang__)
  #define ASMJIT_FAVOR_SIZE  __attribute__((__optimize__("Os")))
  #define ASMJIT_FAVOR_SPEED __attribute__((__optimize__("O3")))
#elif ASMJIT_CXX_HAS_ATTRIBUTE(__minsize__, 0)
  #define ASMJIT_FAVOR_SIZE __attribute__((__minsize__))
  #define ASMJIT_FAVOR_SPEED
#else
  #define ASMJIT_FAVOR_SIZE
  #define ASMJIT_FAVOR_SPEED
#endif

// Make sure '#ifdef'ed unit tests are properly highlighted in IDE.
#if !defined(ASMJIT_TEST) && defined(__INTELLISENSE__)
  #define ASMJIT_TEST
#endif

// Include a unit testing package if this is a `asmjit_test_unit` build.
#if defined(ASMJIT_TEST)
  #include "../../../test/broken.h"
#endif

#endif // ASMJIT_CORE_API_BUILD_P_H_INCLUDED
