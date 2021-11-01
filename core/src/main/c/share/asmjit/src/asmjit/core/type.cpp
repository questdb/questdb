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
#include "../core/misc_p.h"
#include "../core/type.h"

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Type]
// ============================================================================

namespace Type {

template<uint32_t TYPE_ID>
struct BaseOfTypeId {
  static constexpr uint32_t kTypeId =
    isBase  (TYPE_ID) ? TYPE_ID :
    isMask8 (TYPE_ID) ? kIdU8   :
    isMask16(TYPE_ID) ? kIdU16  :
    isMask32(TYPE_ID) ? kIdU32  :
    isMask64(TYPE_ID) ? kIdU64  :
    isMmx32 (TYPE_ID) ? kIdI32  :
    isMmx64 (TYPE_ID) ? kIdI64  :
    isVec32 (TYPE_ID) ? TYPE_ID + kIdI8 - _kIdVec32Start  :
    isVec64 (TYPE_ID) ? TYPE_ID + kIdI8 - _kIdVec64Start  :
    isVec128(TYPE_ID) ? TYPE_ID + kIdI8 - _kIdVec128Start :
    isVec256(TYPE_ID) ? TYPE_ID + kIdI8 - _kIdVec256Start :
    isVec512(TYPE_ID) ? TYPE_ID + kIdI8 - _kIdVec512Start : 0;
};

template<uint32_t TYPE_ID>
struct SizeOfTypeId {
  static constexpr uint32_t kTypeSize =
    isInt8   (TYPE_ID) ?  1 :
    isUInt8  (TYPE_ID) ?  1 :
    isInt16  (TYPE_ID) ?  2 :
    isUInt16 (TYPE_ID) ?  2 :
    isInt32  (TYPE_ID) ?  4 :
    isUInt32 (TYPE_ID) ?  4 :
    isInt64  (TYPE_ID) ?  8 :
    isUInt64 (TYPE_ID) ?  8 :
    isFloat32(TYPE_ID) ?  4 :
    isFloat64(TYPE_ID) ?  8 :
    isFloat80(TYPE_ID) ? 10 :
    isMask8  (TYPE_ID) ?  1 :
    isMask16 (TYPE_ID) ?  2 :
    isMask32 (TYPE_ID) ?  4 :
    isMask64 (TYPE_ID) ?  8 :
    isMmx32  (TYPE_ID) ?  4 :
    isMmx64  (TYPE_ID) ?  8 :
    isVec32  (TYPE_ID) ?  4 :
    isVec64  (TYPE_ID) ?  8 :
    isVec128 (TYPE_ID) ? 16 :
    isVec256 (TYPE_ID) ? 32 :
    isVec512 (TYPE_ID) ? 64 : 0;
};

const TypeData _typeData = {
  #define VALUE(x) BaseOfTypeId<x>::kTypeId
  { ASMJIT_LOOKUP_TABLE_256(VALUE, 0) },
  #undef VALUE

  #define VALUE(x) SizeOfTypeId<x>::kTypeSize
  { ASMJIT_LOOKUP_TABLE_256(VALUE, 0) }
  #undef VALUE
};

} // {Type}

ASMJIT_END_NAMESPACE
