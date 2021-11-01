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

#ifndef ASMJIT_CORE_MISC_P_H_INCLUDED
#define ASMJIT_CORE_MISC_P_H_INCLUDED

#include "../core/api-config.h"

ASMJIT_BEGIN_NAMESPACE

//! \cond INTERNAL
//! \addtogroup asmjit_utilities
//! \{

#define ASMJIT_LOOKUP_TABLE_4(T, I) T((I)), T((I+1)), T((I+2)), T((I+3))
#define ASMJIT_LOOKUP_TABLE_8(T, I) ASMJIT_LOOKUP_TABLE_4(T, I), ASMJIT_LOOKUP_TABLE_4(T, I + 4)
#define ASMJIT_LOOKUP_TABLE_16(T, I) ASMJIT_LOOKUP_TABLE_8(T, I), ASMJIT_LOOKUP_TABLE_8(T, I + 8)
#define ASMJIT_LOOKUP_TABLE_32(T, I) ASMJIT_LOOKUP_TABLE_16(T, I), ASMJIT_LOOKUP_TABLE_16(T, I + 16)
#define ASMJIT_LOOKUP_TABLE_40(T, I) ASMJIT_LOOKUP_TABLE_16(T, I), ASMJIT_LOOKUP_TABLE_16(T, I + 16), ASMJIT_LOOKUP_TABLE_8(T, I + 32)
#define ASMJIT_LOOKUP_TABLE_64(T, I) ASMJIT_LOOKUP_TABLE_32(T, I), ASMJIT_LOOKUP_TABLE_32(T, I + 32)
#define ASMJIT_LOOKUP_TABLE_128(T, I) ASMJIT_LOOKUP_TABLE_64(T, I), ASMJIT_LOOKUP_TABLE_64(T, I + 64)
#define ASMJIT_LOOKUP_TABLE_256(T, I) ASMJIT_LOOKUP_TABLE_128(T, I), ASMJIT_LOOKUP_TABLE_128(T, I + 128)
#define ASMJIT_LOOKUP_TABLE_512(T, I) ASMJIT_LOOKUP_TABLE_256(T, I), ASMJIT_LOOKUP_TABLE_256(T, I + 256)
#define ASMJIT_LOOKUP_TABLE_1024(T, I) ASMJIT_LOOKUP_TABLE_512(T, I), ASMJIT_LOOKUP_TABLE_512(T, I + 512)

//! \}
//! \endcond

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_MISC_P_H_INCLUDED
