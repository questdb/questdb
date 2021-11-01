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
#include "../core/environment.h"

ASMJIT_BEGIN_NAMESPACE

// X86 Target
// ----------
//
//   - 32-bit - Linux, OSX, BSD, and apparently also Haiku guarantee 16-byte
//              stack alignment. Other operating systems are assumed to have
//              4-byte alignment by default for safety reasons.
//   - 64-bit - stack must be aligned to 16 bytes.
//
// ARM Target
// ----------
//
//   - 32-bit - Stack must be aligned to 8 bytes.
//   - 64-bit - Stack must be aligned to 16 bytes (hardware requirement).
uint32_t Environment::stackAlignment() const noexcept {
  if (is64Bit()) {
    // Assume 16-byte alignment on any 64-bit target.
    return 16;
  }
  else {
    // The following platforms use 16-byte alignment in 32-bit mode.
    if (isPlatformLinux() ||
        isPlatformBSD() ||
        isPlatformApple() ||
        isPlatformHaiku()) {
      return 16u;
    }

    if (isFamilyARM())
      return 8;

    // Bail to 4-byte alignment if we don't know.
    return 4;
  }
}

ASMJIT_END_NAMESPACE
