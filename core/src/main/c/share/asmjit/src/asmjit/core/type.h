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

#ifndef ASMJIT_CORE_TYPE_H_INCLUDED
#define ASMJIT_CORE_TYPE_H_INCLUDED

#include "../core/globals.h"

ASMJIT_BEGIN_NAMESPACE

//! \addtogroup asmjit_core
//! \{

// ============================================================================
// [asmjit::Type]
// ============================================================================

//! Provides a minimalist type-system that is used by Asmjit library.
namespace Type {

//! TypeId.
//!
//! This is an additional information that can be used to describe a value-type
//! of physical or virtual register. it's used mostly by BaseCompiler to describe
//! register representation (the group of data stored in the register and the
//! width used) and it's also used by APIs that allow to describe and work with
//! function signatures.
enum Id : uint32_t {
  kIdVoid         = 0,  //!< Void type.

  _kIdBaseStart   = 32,
  _kIdBaseEnd     = 44,

  _kIdIntStart    = 32,
  _kIdIntEnd      = 41,

  kIdIntPtr       = 32, //!< Abstract signed integer type that has a native size.
  kIdUIntPtr      = 33, //!< Abstract unsigned integer type that has a native size.

  kIdI8           = 34, //!< 8-bit signed integer type.
  kIdU8           = 35, //!< 8-bit unsigned integer type.
  kIdI16          = 36, //!< 16-bit signed integer type.
  kIdU16          = 37, //!< 16-bit unsigned integer type.
  kIdI32          = 38, //!< 32-bit signed integer type.
  kIdU32          = 39, //!< 32-bit unsigned integer type.
  kIdI64          = 40, //!< 64-bit signed integer type.
  kIdU64          = 41, //!< 64-bit unsigned integer type.

  _kIdFloatStart  = 42,
  _kIdFloatEnd    = 44,

  kIdF32          = 42, //!< 32-bit floating point type.
  kIdF64          = 43, //!< 64-bit floating point type.
  kIdF80          = 44, //!< 80-bit floating point type.

  _kIdMaskStart   = 45,
  _kIdMaskEnd     = 48,

  kIdMask8        = 45, //!< 8-bit opmask register (K).
  kIdMask16       = 46, //!< 16-bit opmask register (K).
  kIdMask32       = 47, //!< 32-bit opmask register (K).
  kIdMask64       = 48, //!< 64-bit opmask register (K).

  _kIdMmxStart    = 49,
  _kIdMmxEnd      = 50,

  kIdMmx32        = 49, //!< 64-bit MMX register only used for 32 bits.
  kIdMmx64        = 50, //!< 64-bit MMX register.

  _kIdVec32Start  = 51,
  _kIdVec32End    = 60,

  kIdI8x4         = 51,
  kIdU8x4         = 52,
  kIdI16x2        = 53,
  kIdU16x2        = 54,
  kIdI32x1        = 55,
  kIdU32x1        = 56,
  kIdF32x1        = 59,

  _kIdVec64Start  = 61,
  _kIdVec64End    = 70,

  kIdI8x8         = 61,
  kIdU8x8         = 62,
  kIdI16x4        = 63,
  kIdU16x4        = 64,
  kIdI32x2        = 65,
  kIdU32x2        = 66,
  kIdI64x1        = 67,
  kIdU64x1        = 68,
  kIdF32x2        = 69,
  kIdF64x1        = 70,

  _kIdVec128Start = 71,
  _kIdVec128End   = 80,

  kIdI8x16        = 71,
  kIdU8x16        = 72,
  kIdI16x8        = 73,
  kIdU16x8        = 74,
  kIdI32x4        = 75,
  kIdU32x4        = 76,
  kIdI64x2        = 77,
  kIdU64x2        = 78,
  kIdF32x4        = 79,
  kIdF64x2        = 80,

  _kIdVec256Start = 81,
  _kIdVec256End   = 90,

  kIdI8x32        = 81,
  kIdU8x32        = 82,
  kIdI16x16       = 83,
  kIdU16x16       = 84,
  kIdI32x8        = 85,
  kIdU32x8        = 86,
  kIdI64x4        = 87,
  kIdU64x4        = 88,
  kIdF32x8        = 89,
  kIdF64x4        = 90,

  _kIdVec512Start = 91,
  _kIdVec512End   = 100,

  kIdI8x64        = 91,
  kIdU8x64        = 92,
  kIdI16x32       = 93,
  kIdU16x32       = 94,
  kIdI32x16       = 95,
  kIdU32x16       = 96,
  kIdI64x8        = 97,
  kIdU64x8        = 98,
  kIdF32x16       = 99,
  kIdF64x8        = 100,

  kIdCount        = 101,
  kIdMax          = 255
};

struct TypeData {
  uint8_t baseOf[kIdMax + 1];
  uint8_t sizeOf[kIdMax + 1];
};
ASMJIT_VARAPI const TypeData _typeData;

static constexpr bool isVoid(uint32_t typeId) noexcept { return typeId == 0; }
static constexpr bool isValid(uint32_t typeId) noexcept { return typeId >= _kIdIntStart && typeId <= _kIdVec512End; }
static constexpr bool isBase(uint32_t typeId) noexcept { return typeId >= _kIdBaseStart && typeId <= _kIdBaseEnd; }
static constexpr bool isAbstract(uint32_t typeId) noexcept { return typeId >= kIdIntPtr && typeId <= kIdUIntPtr; }

static constexpr bool isInt(uint32_t typeId) noexcept { return typeId >= _kIdIntStart && typeId <= _kIdIntEnd; }
static constexpr bool isInt8(uint32_t typeId) noexcept { return typeId == kIdI8; }
static constexpr bool isUInt8(uint32_t typeId) noexcept { return typeId == kIdU8; }
static constexpr bool isInt16(uint32_t typeId) noexcept { return typeId == kIdI16; }
static constexpr bool isUInt16(uint32_t typeId) noexcept { return typeId == kIdU16; }
static constexpr bool isInt32(uint32_t typeId) noexcept { return typeId == kIdI32; }
static constexpr bool isUInt32(uint32_t typeId) noexcept { return typeId == kIdU32; }
static constexpr bool isInt64(uint32_t typeId) noexcept { return typeId == kIdI64; }
static constexpr bool isUInt64(uint32_t typeId) noexcept { return typeId == kIdU64; }

static constexpr bool isGp8(uint32_t typeId) noexcept { return typeId >= kIdI8 && typeId <= kIdU8; }
static constexpr bool isGp16(uint32_t typeId) noexcept { return typeId >= kIdI16 && typeId <= kIdU16; }
static constexpr bool isGp32(uint32_t typeId) noexcept { return typeId >= kIdI32 && typeId <= kIdU32; }
static constexpr bool isGp64(uint32_t typeId) noexcept { return typeId >= kIdI64 && typeId <= kIdU64; }

static constexpr bool isFloat(uint32_t typeId) noexcept { return typeId >= _kIdFloatStart && typeId <= _kIdFloatEnd; }
static constexpr bool isFloat32(uint32_t typeId) noexcept { return typeId == kIdF32; }
static constexpr bool isFloat64(uint32_t typeId) noexcept { return typeId == kIdF64; }
static constexpr bool isFloat80(uint32_t typeId) noexcept { return typeId == kIdF80; }

static constexpr bool isMask(uint32_t typeId) noexcept { return typeId >= _kIdMaskStart && typeId <= _kIdMaskEnd; }
static constexpr bool isMask8(uint32_t typeId) noexcept { return typeId == kIdMask8; }
static constexpr bool isMask16(uint32_t typeId) noexcept { return typeId == kIdMask16; }
static constexpr bool isMask32(uint32_t typeId) noexcept { return typeId == kIdMask32; }
static constexpr bool isMask64(uint32_t typeId) noexcept { return typeId == kIdMask64; }

static constexpr bool isMmx(uint32_t typeId) noexcept { return typeId >= _kIdMmxStart && typeId <= _kIdMmxEnd; }
static constexpr bool isMmx32(uint32_t typeId) noexcept { return typeId == kIdMmx32; }
static constexpr bool isMmx64(uint32_t typeId) noexcept { return typeId == kIdMmx64; }

static constexpr bool isVec(uint32_t typeId) noexcept { return typeId >= _kIdVec32Start && typeId <= _kIdVec512End; }
static constexpr bool isVec32(uint32_t typeId) noexcept { return typeId >= _kIdVec32Start && typeId <= _kIdVec32End; }
static constexpr bool isVec64(uint32_t typeId) noexcept { return typeId >= _kIdVec64Start && typeId <= _kIdVec64End; }
static constexpr bool isVec128(uint32_t typeId) noexcept { return typeId >= _kIdVec128Start && typeId <= _kIdVec128End; }
static constexpr bool isVec256(uint32_t typeId) noexcept { return typeId >= _kIdVec256Start && typeId <= _kIdVec256End; }
static constexpr bool isVec512(uint32_t typeId) noexcept { return typeId >= _kIdVec512Start && typeId <= _kIdVec512End; }

//! \cond
enum TypeCategory : uint32_t {
  kTypeCategoryUnknown = 0,
  kTypeCategoryEnum = 1,
  kTypeCategoryIntegral = 2,
  kTypeCategoryFloatingPoint = 3,
  kTypeCategoryFunction = 4
};

template<typename T, uint32_t Category>
struct IdOfT_ByCategory {}; // Fails if not specialized.

template<typename T>
struct IdOfT_ByCategory<T, kTypeCategoryIntegral> {
  enum : uint32_t {
    kTypeId = (sizeof(T) == 1 &&  std::is_signed<T>::value) ? kIdI8 :
              (sizeof(T) == 1 && !std::is_signed<T>::value) ? kIdU8 :
              (sizeof(T) == 2 &&  std::is_signed<T>::value) ? kIdI16 :
              (sizeof(T) == 2 && !std::is_signed<T>::value) ? kIdU16 :
              (sizeof(T) == 4 &&  std::is_signed<T>::value) ? kIdI32 :
              (sizeof(T) == 4 && !std::is_signed<T>::value) ? kIdU32 :
              (sizeof(T) == 8 &&  std::is_signed<T>::value) ? kIdI64 :
              (sizeof(T) == 8 && !std::is_signed<T>::value) ? kIdU64 : kIdVoid
  };
};

template<typename T>
struct IdOfT_ByCategory<T, kTypeCategoryFloatingPoint> {
  enum : uint32_t {
    kTypeId = (sizeof(T) == 4 ) ? kIdF32 :
              (sizeof(T) == 8 ) ? kIdF64 :
              (sizeof(T) >= 10) ? kIdF80 : kIdVoid
  };
};

template<typename T>
struct IdOfT_ByCategory<T, kTypeCategoryEnum>
  : public IdOfT_ByCategory<typename std::underlying_type<T>::type, kTypeCategoryIntegral> {};

template<typename T>
struct IdOfT_ByCategory<T, kTypeCategoryFunction> {
  enum: uint32_t { kTypeId = kIdUIntPtr };
};
//! \endcond

//! IdOfT<> template allows to get a TypeId from a C++ type `T`.
template<typename T>
struct IdOfT
#ifdef _DOXYGEN
  //! TypeId of C++ type `T`.
  static constexpr uint32_t kTypeId = _TypeIdDeducedAtCompileTime_;
#else
  : public IdOfT_ByCategory<T,
    std::is_enum<T>::value           ? kTypeCategoryEnum          :
    std::is_integral<T>::value       ? kTypeCategoryIntegral      :
    std::is_floating_point<T>::value ? kTypeCategoryFloatingPoint :
    std::is_function<T>::value       ? kTypeCategoryFunction      : kTypeCategoryUnknown>
#endif
{};

//! \cond
template<typename T>
struct IdOfT<T*> { enum : uint32_t { kTypeId = kIdUIntPtr }; };

template<typename T>
struct IdOfT<T&> { enum : uint32_t { kTypeId = kIdUIntPtr }; };
//! \endcond

static inline uint32_t baseOf(uint32_t typeId) noexcept {
  ASMJIT_ASSERT(typeId <= kIdMax);
  return _typeData.baseOf[typeId];
}

static inline uint32_t sizeOf(uint32_t typeId) noexcept {
  ASMJIT_ASSERT(typeId <= kIdMax);
  return _typeData.sizeOf[typeId];
}

//! Returns offset needed to convert a `kIntPtr` and `kUIntPtr` TypeId
//! into a type that matches `registerSize` (general-purpose register size).
//! If you find such TypeId it's then only about adding the offset to it.
//!
//! For example:
//!
//! ```
//! uint32_t registerSize = '4' or '8';
//! uint32_t deabstractDelta = Type::deabstractDeltaOfSize(registerSize);
//!
//! uint32_t typeId = 'some type-id';
//!
//! // Normalize some typeId into a non-abstract typeId.
//! if (Type::isAbstract(typeId)) typeId += deabstractDelta;
//!
//! // The same, but by using Type::deabstract() function.
//! typeId = Type::deabstract(typeId, deabstractDelta);
//! ```
static constexpr uint32_t deabstractDeltaOfSize(uint32_t registerSize) noexcept {
  return registerSize >= 8 ? kIdI64 - kIdIntPtr : kIdI32 - kIdIntPtr;
}

static constexpr uint32_t deabstract(uint32_t typeId, uint32_t deabstractDelta) noexcept {
  return isAbstract(typeId) ? typeId + deabstractDelta : typeId;
}

//! bool as C++ type-name.
struct Bool {};
//! int8_t as C++ type-name.
struct I8 {};
//! uint8_t as C++ type-name.
struct U8 {};
//! int16_t as C++ type-name.
struct I16 {};
//! uint16_t as C++ type-name.
struct U16 {};
//! int32_t as C++ type-name.
struct I32 {};
//! uint32_t as C++ type-name.
struct U32 {};
//! int64_t as C++ type-name.
struct I64 {};
//! uint64_t as C++ type-name.
struct U64 {};
//! intptr_t as C++ type-name.
struct IPtr {};
//! uintptr_t as C++ type-name.
struct UPtr {};
//! float as C++ type-name.
struct F32 {};
//! double as C++ type-name.
struct F64 {};

} // {Type}

// ============================================================================
// [ASMJIT_DEFINE_TYPE_ID]
// ============================================================================

//! \cond
#define ASMJIT_DEFINE_TYPE_ID(T, TYPE_ID)  \
namespace Type {                           \
  template<>                               \
  struct IdOfT<T> {                        \
    enum : uint32_t { kTypeId = TYPE_ID }; \
  };                                       \
}

ASMJIT_DEFINE_TYPE_ID(void, kIdVoid);
ASMJIT_DEFINE_TYPE_ID(Bool, kIdU8);
ASMJIT_DEFINE_TYPE_ID(I8  , kIdI8);
ASMJIT_DEFINE_TYPE_ID(U8  , kIdU8);
ASMJIT_DEFINE_TYPE_ID(I16 , kIdI16);
ASMJIT_DEFINE_TYPE_ID(U16 , kIdU16);
ASMJIT_DEFINE_TYPE_ID(I32 , kIdI32);
ASMJIT_DEFINE_TYPE_ID(U32 , kIdU32);
ASMJIT_DEFINE_TYPE_ID(I64 , kIdI64);
ASMJIT_DEFINE_TYPE_ID(U64 , kIdU64);
ASMJIT_DEFINE_TYPE_ID(IPtr, kIdIntPtr);
ASMJIT_DEFINE_TYPE_ID(UPtr, kIdUIntPtr);
ASMJIT_DEFINE_TYPE_ID(F32 , kIdF32);
ASMJIT_DEFINE_TYPE_ID(F64 , kIdF64);
//! \endcond

//! \}

ASMJIT_END_NAMESPACE

#endif // ASMJIT_CORE_TYPE_H_INCLUDED
