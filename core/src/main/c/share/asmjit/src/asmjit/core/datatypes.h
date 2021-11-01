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

#ifndef ASMJIT_CORE_DATATYPES_H_INCLUDED
#define ASMJIT_CORE_DATATYPES_H_INCLUDED

#include "../core/globals.h"

#ifndef ASMJIT_NO_DEPRECATED

ASMJIT_BEGIN_NAMESPACE

// ============================================================================
// [asmjit::Data64]
// ============================================================================

//! 64-bit data useful for creating SIMD constants.
union ASMJIT_DEPRECATED_STRUCT("Data64 is deprecated and will be removed in the future") Data64 {
  //! Array of eight 8-bit signed integers.
  int8_t sb[8];
  //! Array of eight 8-bit unsigned integers.
  uint8_t ub[8];
  //! Array of four 16-bit signed integers.
  int16_t sw[4];
  //! Array of four 16-bit unsigned integers.
  uint16_t uw[4];
  //! Array of two 32-bit signed integers.
  int32_t sd[2];
  //! Array of two 32-bit unsigned integers.
  uint32_t ud[2];
  //! Array of one 64-bit signed integer.
  int64_t sq[1];
  //! Array of one 64-bit unsigned integer.
  uint64_t uq[1];

  //! Array of two SP-FP values.
  float sf[2];
  //! Array of one DP-FP value.
  double df[1];

  //! \name Construction & Destruction
  //! \{

  //! Sets all eight 8-bit signed integers.
  static inline Data64 fromI8(int8_t x0) noexcept {
    Data64 self;
    self.setI8(x0);
    return self;
  }

  //! Sets all eight 8-bit unsigned integers.
  static inline Data64 fromU8(uint8_t x0) noexcept {
    Data64 self;
    self.setU8(x0);
    return self;
  }

  //! Sets all eight 8-bit signed integers.
  static inline Data64 fromI8(
    int8_t x0, int8_t x1, int8_t x2, int8_t x3, int8_t x4, int8_t x5, int8_t x6, int8_t x7) noexcept {

    Data64 self;
    self.setI8(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all eight 8-bit unsigned integers.
  static inline Data64 fromU8(
    uint8_t x0, uint8_t x1, uint8_t x2, uint8_t x3, uint8_t x4, uint8_t x5, uint8_t x6, uint8_t x7) noexcept {

    Data64 self;
    self.setU8(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all four 16-bit signed integers.
  static inline Data64 fromI16(int16_t x0) noexcept {
    Data64 self;
    self.setI16(x0);
    return self;
  }

  //! Sets all four 16-bit unsigned integers.
  static inline Data64 fromU16(uint16_t x0) noexcept {
    Data64 self;
    self.setU16(x0);
    return self;
  }

  //! Sets all four 16-bit signed integers.
  static inline Data64 fromI16(int16_t x0, int16_t x1, int16_t x2, int16_t x3) noexcept {
    Data64 self;
    self.setI16(x0, x1, x2, x3);
    return self;
  }

  //! Sets all four 16-bit unsigned integers.
  static inline Data64 fromU16(uint16_t x0, uint16_t x1, uint16_t x2, uint16_t x3) noexcept {
    Data64 self;
    self.setU16(x0, x1, x2, x3);
    return self;
  }

  //! Sets all two 32-bit signed integers.
  static inline Data64 fromI32(int32_t x0) noexcept {
    Data64 self;
    self.setI32(x0);
    return self;
  }

  //! Sets all two 32-bit unsigned integers.
  static inline Data64 fromU32(uint32_t x0) noexcept {
    Data64 self;
    self.setU32(x0);
    return self;
  }

  //! Sets all two 32-bit signed integers.
  static inline Data64 fromI32(int32_t x0, int32_t x1) noexcept {
    Data64 self;
    self.setI32(x0, x1);
    return self;
  }

  //! Sets all two 32-bit unsigned integers.
  static inline Data64 fromU32(uint32_t x0, uint32_t x1) noexcept {
    Data64 self;
    self.setU32(x0, x1);
    return self;
  }

  //! Sets 64-bit signed integer.
  static inline Data64 fromI64(int64_t x0) noexcept {
    Data64 self;
    self.setI64(x0);
    return self;
  }

  //! Sets 64-bit unsigned integer.
  static inline Data64 fromU64(uint64_t x0) noexcept {
    Data64 self;
    self.setU64(x0);
    return self;
  }

  //! Sets all two SP-FP values.
  static inline Data64 fromF32(float x0) noexcept {
    Data64 self;
    self.setF32(x0);
    return self;
  }

  //! Sets all two SP-FP values.
  static inline Data64 fromF32(float x0, float x1) noexcept {
    Data64 self;
    self.setF32(x0, x1);
    return self;
  }

  //! Sets all two SP-FP values.
  static inline Data64 fromF64(double x0) noexcept {
    Data64 self;
    self.setF64(x0);
    return self;
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Sets all eight 8-bit signed integers.
  inline void setI8(int8_t x0) noexcept {
    setU8(uint8_t(x0));
  }

  //! Sets all eight 8-bit unsigned integers.
  inline void setU8(uint8_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0101010101010101u;
      uq[0] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x01010101u;
      ud[0] = xd;
      ud[1] = xd;
    }
  }

  //! Sets all eight 8-bit signed integers.
  inline void setI8(
    int8_t x0, int8_t x1, int8_t x2, int8_t x3, int8_t x4, int8_t x5, int8_t x6, int8_t x7) noexcept {

    sb[0] = x0; sb[1] = x1; sb[2] = x2; sb[3] = x3;
    sb[4] = x4; sb[5] = x5; sb[6] = x6; sb[7] = x7;
  }

  //! Sets all eight 8-bit unsigned integers.
  inline void setU8(
    uint8_t x0, uint8_t x1, uint8_t x2, uint8_t x3, uint8_t x4, uint8_t x5, uint8_t x6, uint8_t x7) noexcept {

    ub[0] = x0; ub[1] = x1; ub[2] = x2; ub[3] = x3;
    ub[4] = x4; ub[5] = x5; ub[6] = x6; ub[7] = x7;
  }

  //! Sets all four 16-bit signed integers.
  inline void setI16(int16_t x0) noexcept {
    setU16(uint16_t(x0));
  }

  //! Sets all four 16-bit unsigned integers.
  inline void setU16(uint16_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0001000100010001u;
      uq[0] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x00010001u;
      ud[0] = xd;
      ud[1] = xd;
    }
  }

  //! Sets all four 16-bit signed integers.
  inline void setI16(int16_t x0, int16_t x1, int16_t x2, int16_t x3) noexcept {
    sw[0] = x0; sw[1] = x1; sw[2] = x2; sw[3] = x3;
  }

  //! Sets all four 16-bit unsigned integers.
  inline void setU16(uint16_t x0, uint16_t x1, uint16_t x2, uint16_t x3) noexcept {
    uw[0] = x0; uw[1] = x1; uw[2] = x2; uw[3] = x3;
  }

  //! Sets all two 32-bit signed integers.
  inline void setI32(int32_t x0) noexcept {
    sd[0] = x0; sd[1] = x0;
  }

  //! Sets all two 32-bit unsigned integers.
  inline void setU32(uint32_t x0) noexcept {
    ud[0] = x0; ud[1] = x0;
  }

  //! Sets all two 32-bit signed integers.
  inline void setI32(int32_t x0, int32_t x1) noexcept {
    sd[0] = x0; sd[1] = x1;
  }

  //! Sets all two 32-bit unsigned integers.
  inline void setU32(uint32_t x0, uint32_t x1) noexcept {
    ud[0] = x0; ud[1] = x1;
  }

  //! Sets 64-bit signed integer.
  inline void setI64(int64_t x0) noexcept {
    sq[0] = x0;
  }

  //! Sets 64-bit unsigned integer.
  inline void setU64(uint64_t x0) noexcept {
    uq[0] = x0;
  }

  //! Sets all two SP-FP values.
  inline void setF32(float x0) noexcept {
    sf[0] = x0; sf[1] = x0;
  }

  //! Sets all two SP-FP values.
  inline void setF32(float x0, float x1) noexcept {
    sf[0] = x0; sf[1] = x1;
  }

  //! Sets all two SP-FP values.
  inline void setF64(double x0) noexcept {
    df[0] = x0;
  }
};

// ============================================================================
// [asmjit::Data128]
// ============================================================================

//! 128-bit data useful for creating SIMD constants.
union ASMJIT_DEPRECATED_STRUCT("Data128 is deprecated and will be removed in the future") Data128 {
  //! Array of sixteen 8-bit signed integers.
  int8_t sb[16];
  //! Array of sixteen 8-bit unsigned integers.
  uint8_t ub[16];
  //! Array of eight 16-bit signed integers.
  int16_t sw[8];
  //! Array of eight 16-bit unsigned integers.
  uint16_t uw[8];
  //! Array of four 32-bit signed integers.
  int32_t sd[4];
  //! Array of four 32-bit unsigned integers.
  uint32_t ud[4];
  //! Array of two 64-bit signed integers.
  int64_t sq[2];
  //! Array of two 64-bit unsigned integers.
  uint64_t uq[2];

  //! Array of four 32-bit single precision floating points.
  float sf[4];
  //! Array of two 64-bit double precision floating points.
  double df[2];

  //! \name Construction & Destruction
  //! \{

  //! Sets all sixteen 8-bit signed integers.
  static inline Data128 fromI8(int8_t x0) noexcept {
    Data128 self;
    self.setI8(x0);
    return self;
  }

  //! Sets all sixteen 8-bit unsigned integers.
  static inline Data128 fromU8(uint8_t x0) noexcept {
    Data128 self;
    self.setU8(x0);
    return self;
  }

  //! Sets all sixteen 8-bit signed integers.
  static inline Data128 fromI8(
    int8_t x0 , int8_t x1 , int8_t x2 , int8_t x3 ,
    int8_t x4 , int8_t x5 , int8_t x6 , int8_t x7 ,
    int8_t x8 , int8_t x9 , int8_t x10, int8_t x11,
    int8_t x12, int8_t x13, int8_t x14, int8_t x15) noexcept {

    Data128 self;
    self.setI8(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
    return self;
  }

  //! Sets all sixteen 8-bit unsigned integers.
  static inline Data128 fromU8(
    uint8_t x0 , uint8_t x1 , uint8_t x2 , uint8_t x3 ,
    uint8_t x4 , uint8_t x5 , uint8_t x6 , uint8_t x7 ,
    uint8_t x8 , uint8_t x9 , uint8_t x10, uint8_t x11,
    uint8_t x12, uint8_t x13, uint8_t x14, uint8_t x15) noexcept {

    Data128 self;
    self.setU8(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
    return self;
  }

  //! Sets all eight 16-bit signed integers.
  static inline Data128 fromI16(int16_t x0) noexcept {
    Data128 self;
    self.setI16(x0);
    return self;
  }

  //! Sets all eight 16-bit unsigned integers.
  static inline Data128 fromU16(uint16_t x0) noexcept {
    Data128 self;
    self.setU16(x0);
    return self;
  }

  //! Sets all eight 16-bit signed integers.
  static inline Data128 fromI16(
    int16_t x0, int16_t x1, int16_t x2, int16_t x3, int16_t x4, int16_t x5, int16_t x6, int16_t x7) noexcept {

    Data128 self;
    self.setI16(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all eight 16-bit unsigned integers.
  static inline Data128 fromU16(
    uint16_t x0, uint16_t x1, uint16_t x2, uint16_t x3, uint16_t x4, uint16_t x5, uint16_t x6, uint16_t x7) noexcept {

    Data128 self;
    self.setU16(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all four 32-bit signed integers.
  static inline Data128 fromI32(int32_t x0) noexcept {
    Data128 self;
    self.setI32(x0);
    return self;
  }

  //! Sets all four 32-bit unsigned integers.
  static inline Data128 fromU32(uint32_t x0) noexcept {
    Data128 self;
    self.setU32(x0);
    return self;
  }

  //! Sets all four 32-bit signed integers.
  static inline Data128 fromI32(int32_t x0, int32_t x1, int32_t x2, int32_t x3) noexcept {
    Data128 self;
    self.setI32(x0, x1, x2, x3);
    return self;
  }

  //! Sets all four 32-bit unsigned integers.
  static inline Data128 fromU32(uint32_t x0, uint32_t x1, uint32_t x2, uint32_t x3) noexcept {
    Data128 self;
    self.setU32(x0, x1, x2, x3);
    return self;
  }

  //! Sets all two 64-bit signed integers.
  static inline Data128 fromI64(int64_t x0) noexcept {
    Data128 self;
    self.setI64(x0);
    return self;
  }

  //! Sets all two 64-bit unsigned integers.
  static inline Data128 fromU64(uint64_t x0) noexcept {
    Data128 self;
    self.setU64(x0);
    return self;
  }

  //! Sets all two 64-bit signed integers.
  static inline Data128 fromI64(int64_t x0, int64_t x1) noexcept {
    Data128 self;
    self.setI64(x0, x1);
    return self;
  }

  //! Sets all two 64-bit unsigned integers.
  static inline Data128 fromU64(uint64_t x0, uint64_t x1) noexcept {
    Data128 self;
    self.setU64(x0, x1);
    return self;
  }

  //! Sets all four SP-FP floats.
  static inline Data128 fromF32(float x0) noexcept {
    Data128 self;
    self.setF32(x0);
    return self;
  }

  //! Sets all four SP-FP floats.
  static inline Data128 fromF32(float x0, float x1, float x2, float x3) noexcept {
    Data128 self;
    self.setF32(x0, x1, x2, x3);
    return self;
  }

  //! Sets all two DP-FP floats.
  static inline Data128 fromF64(double x0) noexcept {
    Data128 self;
    self.setF64(x0);
    return self;
  }

  //! Sets all two DP-FP floats.
  static inline Data128 fromF64(double x0, double x1) noexcept {
    Data128 self;
    self.setF64(x0, x1);
    return self;
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Sets all sixteen 8-bit signed integers.
  inline void setI8(int8_t x0) noexcept {
    setU8(uint8_t(x0));
  }

  //! Sets all sixteen 8-bit unsigned integers.
  inline void setU8(uint8_t x0) noexcept  {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0101010101010101u;
      uq[0] = xq;
      uq[1] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x01010101u;
      ud[0] = xd;
      ud[1] = xd;
      ud[2] = xd;
      ud[3] = xd;
    }
  }

  //! Sets all sixteen 8-bit signed integers.
  inline void setI8(
    int8_t x0 , int8_t x1 , int8_t x2 , int8_t x3 ,
    int8_t x4 , int8_t x5 , int8_t x6 , int8_t x7 ,
    int8_t x8 , int8_t x9 , int8_t x10, int8_t x11,
    int8_t x12, int8_t x13, int8_t x14, int8_t x15) noexcept  {

    sb[0 ] = x0 ; sb[1 ] = x1 ; sb[2 ] = x2 ; sb[3 ] = x3 ;
    sb[4 ] = x4 ; sb[5 ] = x5 ; sb[6 ] = x6 ; sb[7 ] = x7 ;
    sb[8 ] = x8 ; sb[9 ] = x9 ; sb[10] = x10; sb[11] = x11;
    sb[12] = x12; sb[13] = x13; sb[14] = x14; sb[15] = x15;
  }

  //! Sets all sixteen 8-bit unsigned integers.
  inline void setU8(
    uint8_t x0 , uint8_t x1 , uint8_t x2 , uint8_t x3 ,
    uint8_t x4 , uint8_t x5 , uint8_t x6 , uint8_t x7 ,
    uint8_t x8 , uint8_t x9 , uint8_t x10, uint8_t x11,
    uint8_t x12, uint8_t x13, uint8_t x14, uint8_t x15) noexcept {

    ub[0 ] = x0 ; ub[1 ] = x1 ; ub[2 ] = x2 ; ub[3 ] = x3 ;
    ub[4 ] = x4 ; ub[5 ] = x5 ; ub[6 ] = x6 ; ub[7 ] = x7 ;
    ub[8 ] = x8 ; ub[9 ] = x9 ; ub[10] = x10; ub[11] = x11;
    ub[12] = x12; ub[13] = x13; ub[14] = x14; ub[15] = x15;
  }

  //! Sets all eight 16-bit signed integers.
  inline void setI16(int16_t x0) noexcept {
    setU16(uint16_t(x0));
  }

  //! Sets all eight 16-bit unsigned integers.
  inline void setU16(uint16_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0001000100010001u;
      uq[0] = xq;
      uq[1] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x00010001u;
      ud[0] = xd;
      ud[1] = xd;
      ud[2] = xd;
      ud[3] = xd;
    }
  }

  //! Sets all eight 16-bit signed integers.
  inline void setI16(
    int16_t x0, int16_t x1, int16_t x2, int16_t x3, int16_t x4, int16_t x5, int16_t x6, int16_t x7) noexcept {

    sw[0] = x0; sw[1] = x1; sw[2] = x2; sw[3] = x3;
    sw[4] = x4; sw[5] = x5; sw[6] = x6; sw[7] = x7;
  }

  //! Sets all eight 16-bit unsigned integers.
  inline void setU16(
    uint16_t x0, uint16_t x1, uint16_t x2, uint16_t x3, uint16_t x4, uint16_t x5, uint16_t x6, uint16_t x7) noexcept {

    uw[0] = x0; uw[1] = x1; uw[2] = x2; uw[3] = x3;
    uw[4] = x4; uw[5] = x5; uw[6] = x6; uw[7] = x7;
  }

  //! Sets all four 32-bit signed integers.
  inline void setI32(int32_t x0) noexcept {
    setU32(uint32_t(x0));
  }

  //! Sets all four 32-bit unsigned integers.
  inline void setU32(uint32_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t t = (uint64_t(x0) << 32) + x0;
      uq[0] = t;
      uq[1] = t;
    }
    else {
      ud[0] = x0;
      ud[1] = x0;
      ud[2] = x0;
      ud[3] = x0;
    }
  }

  //! Sets all four 32-bit signed integers.
  inline void setI32(int32_t x0, int32_t x1, int32_t x2, int32_t x3) noexcept {
    sd[0] = x0; sd[1] = x1; sd[2] = x2; sd[3] = x3;
  }

  //! Sets all four 32-bit unsigned integers.
  inline void setU32(uint32_t x0, uint32_t x1, uint32_t x2, uint32_t x3) noexcept {
    ud[0] = x0; ud[1] = x1; ud[2] = x2; ud[3] = x3;
  }

  //! Sets all two 64-bit signed integers.
  inline void setI64(int64_t x0) noexcept {
    sq[0] = x0; sq[1] = x0;
  }

  //! Sets all two 64-bit unsigned integers.
  inline void setU64(uint64_t x0) noexcept {
    uq[0] = x0; uq[1] = x0;
  }

  //! Sets all two 64-bit signed integers.
  inline void setI64(int64_t x0, int64_t x1) noexcept {
    sq[0] = x0; sq[1] = x1;
  }

  //! Sets all two 64-bit unsigned integers.
  inline void setU64(uint64_t x0, uint64_t x1) noexcept {
    uq[0] = x0; uq[1] = x1;
  }

  //! Sets all four SP-FP floats.
  inline void setF32(float x0) noexcept {
    sf[0] = x0; sf[1] = x0; sf[2] = x0; sf[3] = x0;
  }

  //! Sets all four SP-FP floats.
  inline void setF32(float x0, float x1, float x2, float x3) noexcept {
    sf[0] = x0; sf[1] = x1; sf[2] = x2; sf[3] = x3;
  }

  //! Sets all two DP-FP floats.
  inline void setF64(double x0) noexcept {
    df[0] = x0; df[1] = x0;
  }

  //! Sets all two DP-FP floats.
  inline void setF64(double x0, double x1) noexcept {
    df[0] = x0; df[1] = x1;
  }
};

// ============================================================================
// [asmjit::Data256]
// ============================================================================

//! 256-bit data useful for creating SIMD constants.
union ASMJIT_DEPRECATED_STRUCT("Data256 is deprecated and will be removed in the future") Data256 {
  //! Array of thirty two 8-bit signed integers.
  int8_t sb[32];
  //! Array of thirty two 8-bit unsigned integers.
  uint8_t ub[32];
  //! Array of sixteen 16-bit signed integers.
  int16_t sw[16];
  //! Array of sixteen 16-bit unsigned integers.
  uint16_t uw[16];
  //! Array of eight 32-bit signed integers.
  int32_t sd[8];
  //! Array of eight 32-bit unsigned integers.
  uint32_t ud[8];
  //! Array of four 64-bit signed integers.
  int64_t sq[4];
  //! Array of four 64-bit unsigned integers.
  uint64_t uq[4];

  //! Array of eight 32-bit single precision floating points.
  float sf[8];
  //! Array of four 64-bit double precision floating points.
  double df[4];

  //! \name Construction & Destruction
  //! \{

  //! Sets all thirty two 8-bit signed integers.
  static inline Data256 fromI8(int8_t x0) noexcept {
    Data256 self;
    self.setI8(x0);
    return self;
  }

  //! Sets all thirty two 8-bit unsigned integers.
  static inline Data256 fromU8(uint8_t x0) noexcept {
    Data256 self;
    self.setU8(x0);
    return self;
  }

  //! Sets all thirty two 8-bit signed integers.
  static inline Data256 fromI8(
    int8_t x0 , int8_t x1 , int8_t x2 , int8_t x3 ,
    int8_t x4 , int8_t x5 , int8_t x6 , int8_t x7 ,
    int8_t x8 , int8_t x9 , int8_t x10, int8_t x11,
    int8_t x12, int8_t x13, int8_t x14, int8_t x15,
    int8_t x16, int8_t x17, int8_t x18, int8_t x19,
    int8_t x20, int8_t x21, int8_t x22, int8_t x23,
    int8_t x24, int8_t x25, int8_t x26, int8_t x27,
    int8_t x28, int8_t x29, int8_t x30, int8_t x31) noexcept {

    Data256 self;
    self.setI8(
      x0,  x1 , x2 , x3 , x4 , x5 , x6 , x7 , x8 , x9 , x10, x11, x12, x13, x14, x15,
      x16, x17, x18, x19, x20, x21, x22, x23, x24, x25, x26, x27, x28, x29, x30, x31);
    return self;
  }

  //! Sets all thirty two 8-bit unsigned integers.
  static inline Data256 fromU8(
    uint8_t x0 , uint8_t x1 , uint8_t x2 , uint8_t x3 ,
    uint8_t x4 , uint8_t x5 , uint8_t x6 , uint8_t x7 ,
    uint8_t x8 , uint8_t x9 , uint8_t x10, uint8_t x11,
    uint8_t x12, uint8_t x13, uint8_t x14, uint8_t x15,
    uint8_t x16, uint8_t x17, uint8_t x18, uint8_t x19,
    uint8_t x20, uint8_t x21, uint8_t x22, uint8_t x23,
    uint8_t x24, uint8_t x25, uint8_t x26, uint8_t x27,
    uint8_t x28, uint8_t x29, uint8_t x30, uint8_t x31) noexcept {

    Data256 self;
    self.setU8(
      x0,  x1 , x2 , x3 , x4 , x5 , x6 , x7 , x8 , x9 , x10, x11, x12, x13, x14, x15,
      x16, x17, x18, x19, x20, x21, x22, x23, x24, x25, x26, x27, x28, x29, x30, x31);
    return self;
  }

  //! Sets all sixteen 16-bit signed integers.
  static inline Data256 fromI16(int16_t x0) noexcept {
    Data256 self;
    self.setI16(x0);
    return self;
  }

  //! Sets all sixteen 16-bit unsigned integers.
  static inline Data256 fromU16(uint16_t x0) noexcept {
    Data256 self;
    self.setU16(x0);
    return self;
  }

  //! Sets all sixteen 16-bit signed integers.
  static inline Data256 fromI16(
    int16_t x0, int16_t x1, int16_t x2 , int16_t x3 , int16_t x4 , int16_t x5 , int16_t x6 , int16_t x7 ,
    int16_t x8, int16_t x9, int16_t x10, int16_t x11, int16_t x12, int16_t x13, int16_t x14, int16_t x15) noexcept {

    Data256 self;
    self.setI16(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
    return self;
  }

  //! Sets all sixteen 16-bit unsigned integers.
  static inline Data256 fromU16(
    uint16_t x0, uint16_t x1, uint16_t x2 , uint16_t x3 , uint16_t x4 , uint16_t x5 , uint16_t x6 , uint16_t x7 ,
    uint16_t x8, uint16_t x9, uint16_t x10, uint16_t x11, uint16_t x12, uint16_t x13, uint16_t x14, uint16_t x15) noexcept {

    Data256 self;
    self.setU16(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15);
    return self;
  }

  //! Sets all eight 32-bit signed integers.
  static inline Data256 fromI32(int32_t x0) noexcept {
    Data256 self;
    self.setI32(x0);
    return self;
  }

  //! Sets all eight 32-bit unsigned integers.
  static inline Data256 fromU32(uint32_t x0) noexcept {
    Data256 self;
    self.setU32(x0);
    return self;
  }

  //! Sets all eight 32-bit signed integers.
  static inline Data256 fromI32(
    int32_t x0, int32_t x1, int32_t x2, int32_t x3,
    int32_t x4, int32_t x5, int32_t x6, int32_t x7) noexcept {

    Data256 self;
    self.setI32(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all eight 32-bit unsigned integers.
  static inline Data256 fromU32(
    uint32_t x0, uint32_t x1, uint32_t x2, uint32_t x3,
    uint32_t x4, uint32_t x5, uint32_t x6, uint32_t x7) noexcept {

    Data256 self;
    self.setU32(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all four 64-bit signed integers.
  static inline Data256 fromI64(int64_t x0) noexcept {
    Data256 self;
    self.setI64(x0);
    return self;
  }

  //! Sets all four 64-bit unsigned integers.
  static inline Data256 fromU64(uint64_t x0) noexcept {
    Data256 self;
    self.setU64(x0);
    return self;
  }

  //! Sets all four 64-bit signed integers.
  static inline Data256 fromI64(int64_t x0, int64_t x1, int64_t x2, int64_t x3) noexcept {
    Data256 self;
    self.setI64(x0, x1, x2, x3);
    return self;
  }

  //! Sets all four 64-bit unsigned integers.
  static inline Data256 fromU64(uint64_t x0, uint64_t x1, uint64_t x2, uint64_t x3) noexcept {
    Data256 self;
    self.setU64(x0, x1, x2, x3);
    return self;
  }

  //! Sets all eight SP-FP floats.
  static inline Data256 fromF32(float x0) noexcept {
    Data256 self;
    self.setF32(x0);
    return self;
  }

  //! Sets all eight SP-FP floats.
  static inline Data256 fromF32(
    float x0, float x1, float x2, float x3,
    float x4, float x5, float x6, float x7) noexcept {

    Data256 self;
    self.setF32(x0, x1, x2, x3, x4, x5, x6, x7);
    return self;
  }

  //! Sets all four DP-FP floats.
  static inline Data256 fromF64(double x0) noexcept {
    Data256 self;
    self.setF64(x0);
    return self;
  }

  //! Sets all four DP-FP floats.
  static inline Data256 fromF64(double x0, double x1, double x2, double x3) noexcept {
    Data256 self;
    self.setF64(x0, x1, x2, x3);
    return self;
  }

  //! \}

  //! \name Accessors
  //! \{

  //! Sets all thirty two 8-bit signed integers.
  inline void setI8(int8_t x0) noexcept {
    setU8(uint8_t(x0));
  }

  //! Sets all thirty two 8-bit unsigned integers.
  inline void setU8(uint8_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0101010101010101u;
      uq[0] = xq;
      uq[1] = xq;
      uq[2] = xq;
      uq[3] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x01010101u;
      ud[0] = xd;
      ud[1] = xd;
      ud[2] = xd;
      ud[3] = xd;
      ud[4] = xd;
      ud[5] = xd;
      ud[6] = xd;
      ud[7] = xd;
    }
  }

  //! Sets all thirty two 8-bit signed integers.
  inline void setI8(
    int8_t x0 , int8_t x1 , int8_t x2 , int8_t x3 ,
    int8_t x4 , int8_t x5 , int8_t x6 , int8_t x7 ,
    int8_t x8 , int8_t x9 , int8_t x10, int8_t x11,
    int8_t x12, int8_t x13, int8_t x14, int8_t x15,
    int8_t x16, int8_t x17, int8_t x18, int8_t x19,
    int8_t x20, int8_t x21, int8_t x22, int8_t x23,
    int8_t x24, int8_t x25, int8_t x26, int8_t x27,
    int8_t x28, int8_t x29, int8_t x30, int8_t x31) noexcept {

    sb[0 ] = x0 ; sb[1 ] = x1 ; sb[2 ] = x2 ; sb[3 ] = x3 ;
    sb[4 ] = x4 ; sb[5 ] = x5 ; sb[6 ] = x6 ; sb[7 ] = x7 ;
    sb[8 ] = x8 ; sb[9 ] = x9 ; sb[10] = x10; sb[11] = x11;
    sb[12] = x12; sb[13] = x13; sb[14] = x14; sb[15] = x15;
    sb[16] = x16; sb[17] = x17; sb[18] = x18; sb[19] = x19;
    sb[20] = x20; sb[21] = x21; sb[22] = x22; sb[23] = x23;
    sb[24] = x24; sb[25] = x25; sb[26] = x26; sb[27] = x27;
    sb[28] = x28; sb[29] = x29; sb[30] = x30; sb[31] = x31;
  }

  //! Sets all thirty two 8-bit unsigned integers.
  inline void setU8(
    uint8_t x0 , uint8_t x1 , uint8_t x2 , uint8_t x3 ,
    uint8_t x4 , uint8_t x5 , uint8_t x6 , uint8_t x7 ,
    uint8_t x8 , uint8_t x9 , uint8_t x10, uint8_t x11,
    uint8_t x12, uint8_t x13, uint8_t x14, uint8_t x15,
    uint8_t x16, uint8_t x17, uint8_t x18, uint8_t x19,
    uint8_t x20, uint8_t x21, uint8_t x22, uint8_t x23,
    uint8_t x24, uint8_t x25, uint8_t x26, uint8_t x27,
    uint8_t x28, uint8_t x29, uint8_t x30, uint8_t x31) noexcept {

    ub[0 ] = x0 ; ub[1 ] = x1 ; ub[2 ] = x2 ; ub[3 ] = x3 ;
    ub[4 ] = x4 ; ub[5 ] = x5 ; ub[6 ] = x6 ; ub[7 ] = x7 ;
    ub[8 ] = x8 ; ub[9 ] = x9 ; ub[10] = x10; ub[11] = x11;
    ub[12] = x12; ub[13] = x13; ub[14] = x14; ub[15] = x15;
    ub[16] = x16; ub[17] = x17; ub[18] = x18; ub[19] = x19;
    ub[20] = x20; ub[21] = x21; ub[22] = x22; ub[23] = x23;
    ub[24] = x24; ub[25] = x25; ub[26] = x26; ub[27] = x27;
    ub[28] = x28; ub[29] = x29; ub[30] = x30; ub[31] = x31;
  }

  //! Sets all sixteen 16-bit signed integers.
  inline void setI16(int16_t x0) noexcept {
    setU16(uint16_t(x0));
  }

  //! Sets all eight 16-bit unsigned integers.
  inline void setU16(uint16_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = uint64_t(x0) * 0x0001000100010001u;
      uq[0] = xq;
      uq[1] = xq;
      uq[2] = xq;
      uq[3] = xq;
    }
    else {
      uint32_t xd = uint32_t(x0) * 0x00010001u;
      ud[0] = xd;
      ud[1] = xd;
      ud[2] = xd;
      ud[3] = xd;
      ud[4] = xd;
      ud[5] = xd;
      ud[6] = xd;
      ud[7] = xd;
    }
  }

  //! Sets all sixteen 16-bit signed integers.
  inline void setI16(
    int16_t x0, int16_t x1, int16_t x2 , int16_t x3 , int16_t x4 , int16_t x5 , int16_t x6 , int16_t x7,
    int16_t x8, int16_t x9, int16_t x10, int16_t x11, int16_t x12, int16_t x13, int16_t x14, int16_t x15) noexcept {

    sw[0 ] = x0 ; sw[1 ] = x1 ; sw[2 ] = x2 ; sw[3 ] = x3 ;
    sw[4 ] = x4 ; sw[5 ] = x5 ; sw[6 ] = x6 ; sw[7 ] = x7 ;
    sw[8 ] = x8 ; sw[9 ] = x9 ; sw[10] = x10; sw[11] = x11;
    sw[12] = x12; sw[13] = x13; sw[14] = x14; sw[15] = x15;
  }

  //! Sets all sixteen 16-bit unsigned integers.
  inline void setU16(
    uint16_t x0, uint16_t x1, uint16_t x2 , uint16_t x3 , uint16_t x4 , uint16_t x5 , uint16_t x6 , uint16_t x7,
    uint16_t x8, uint16_t x9, uint16_t x10, uint16_t x11, uint16_t x12, uint16_t x13, uint16_t x14, uint16_t x15) noexcept {

    uw[0 ] = x0 ; uw[1 ] = x1 ; uw[2 ] = x2 ; uw[3 ] = x3 ;
    uw[4 ] = x4 ; uw[5 ] = x5 ; uw[6 ] = x6 ; uw[7 ] = x7 ;
    uw[8 ] = x8 ; uw[9 ] = x9 ; uw[10] = x10; uw[11] = x11;
    uw[12] = x12; uw[13] = x13; uw[14] = x14; uw[15] = x15;
  }

  //! Sets all eight 32-bit signed integers.
  inline void setI32(int32_t x0) noexcept {
    setU32(uint32_t(x0));
  }

  //! Sets all eight 32-bit unsigned integers.
  inline void setU32(uint32_t x0) noexcept {
    if (ASMJIT_ARCH_BITS >= 64) {
      uint64_t xq = (uint64_t(x0) << 32) + x0;
      uq[0] = xq;
      uq[1] = xq;
      uq[2] = xq;
      uq[3] = xq;
    }
    else {
      ud[0] = x0;
      ud[1] = x0;
      ud[2] = x0;
      ud[3] = x0;
      ud[4] = x0;
      ud[5] = x0;
      ud[6] = x0;
      ud[7] = x0;
    }
  }

  //! Sets all eight 32-bit signed integers.
  inline void setI32(
    int32_t x0, int32_t x1, int32_t x2, int32_t x3,
    int32_t x4, int32_t x5, int32_t x6, int32_t x7) noexcept {

    sd[0] = x0; sd[1] = x1; sd[2] = x2; sd[3] = x3;
    sd[4] = x4; sd[5] = x5; sd[6] = x6; sd[7] = x7;
  }

  //! Sets all eight 32-bit unsigned integers.
  inline void setU32(
    uint32_t x0, uint32_t x1, uint32_t x2, uint32_t x3,
    uint32_t x4, uint32_t x5, uint32_t x6, uint32_t x7) noexcept {

    ud[0] = x0; ud[1] = x1; ud[2] = x2; ud[3] = x3;
    ud[4] = x4; ud[5] = x5; ud[6] = x6; ud[7] = x7;
  }

  //! Sets all four 64-bit signed integers.
  inline void setI64(int64_t x0) noexcept {
    sq[0] = x0; sq[1] = x0; sq[2] = x0; sq[3] = x0;
  }

  //! Sets all four 64-bit unsigned integers.
  inline void setU64(uint64_t x0) noexcept {
    uq[0] = x0; uq[1] = x0; uq[2] = x0; uq[3] = x0;
  }

  //! Sets all four 64-bit signed integers.
  inline void setI64(int64_t x0, int64_t x1, int64_t x2, int64_t x3) noexcept {
    sq[0] = x0; sq[1] = x1; sq[2] = x2; sq[3] = x3;
  }

  //! Sets all four 64-bit unsigned integers.
  inline void setU64(uint64_t x0, uint64_t x1, uint64_t x2, uint64_t x3) noexcept {
    uq[0] = x0; uq[1] = x1; uq[2] = x2; uq[3] = x3;
  }

  //! Sets all eight SP-FP floats.
  inline void setF32(float x0) noexcept {
    sf[0] = x0; sf[1] = x0; sf[2] = x0; sf[3] = x0;
    sf[4] = x0; sf[5] = x0; sf[6] = x0; sf[7] = x0;
  }

  //! Sets all eight SP-FP floats.
  inline void setF32(
    float x0, float x1, float x2, float x3,
    float x4, float x5, float x6, float x7) noexcept {

    sf[0] = x0; sf[1] = x1; sf[2] = x2; sf[3] = x3;
    sf[4] = x4; sf[5] = x5; sf[6] = x6; sf[7] = x7;
  }

  //! Sets all four DP-FP floats.
  inline void setF64(double x0) noexcept {
    df[0] = x0; df[1] = x0; df[2] = x0; df[3] = x0;
  }

  //! Sets all four DP-FP floats.
  inline void setF64(double x0, double x1, double x2, double x3) noexcept {
    df[0] = x0; df[1] = x1; df[2] = x2; df[3] = x3;
  }

  //! \}
};

ASMJIT_END_NAMESPACE

#endif // !ASMJIT_NO_DEPRECATED
#endif // ASMJIT_CORE_DATATYPES_H_INCLUDED
