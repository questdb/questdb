/****************************  vectori512s.h   ********************************
* Author:        Agner Fog
* Date created:  2019-04-20
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector classes
* Description:
* Header file defining 512-bit integer vector classes for 8 and 16 bit integers.
* For x86 microprocessors with AVX512BW and later instruction sets.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec64c    Vector of  64  8-bit  signed   integers
* Vec64uc   Vector of  64  8-bit  unsigned integers
* Vec64cb   Vector of  64  booleans for use with Vec64c and Vec64uc
* Vec32s    Vector of  32  16-bit signed   integers
* Vec32us   Vector of  32  16-bit unsigned integers
* Vec32sb   Vector of  32  booleans for use with Vec32s and Vec32us
* Other 512-bit integer vectors are defined in Vectori512.h
*
* Each vector object is represented internally in the CPU as a 512-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef VECTORI512S_H
#define VECTORI512S_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#ifdef VECTORI512SE_H
#error Two different versions of vectorf256.h included
#endif 


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Vector of 64 8-bit signed integers
*
*****************************************************************************/

class Vec64c: public Vec512b {
public:
    // Default constructor:
    Vec64c() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec64c(int8_t i) {
        zmm = _mm512_set1_epi8(i);
    } 
    // Constructor to build from all elements:
    Vec64c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8,  int8_t i9,  int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31,
        int8_t i32, int8_t i33, int8_t i34, int8_t i35, int8_t i36, int8_t i37, int8_t i38, int8_t i39,        
        int8_t i40, int8_t i41, int8_t i42, int8_t i43, int8_t i44, int8_t i45, int8_t i46, int8_t i47,        
        int8_t i48, int8_t i49, int8_t i50, int8_t i51, int8_t i52, int8_t i53, int8_t i54, int8_t i55,        
        int8_t i56, int8_t i57, int8_t i58, int8_t i59, int8_t i60, int8_t i61, int8_t i62, int8_t i63) {
        // _mm512_set_epi8 and _mm512_set_epi16 missing in GCC 7.4.0
        int8_t aa[64] = {
            i0, i1, i2, i3, i4, i5, i6, i7,i8, i9, i10, i11, i12, i13, i14, i15,        
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31,
            i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,        
            i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63 };
        load(aa);
    } 
    // Constructor to build from two Vec32c:
    Vec64c(Vec32c const a0, Vec32c const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec64c(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec64c & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec64c & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec64c & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec64c & load_partial(int n, void const * p) {
        if (n >= 64) {
            zmm = _mm512_loadu_si512(p);
        }
        else {        
            zmm = _mm512_maskz_loadu_epi8(__mmask64(((uint64_t)1 << n) - 1), p);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n >= 64) {
            // _mm512_storeu_epi8(p, zmm);
            _mm512_storeu_si512(p, zmm);
        }
        else {        
            _mm512_mask_storeu_epi8(p, __mmask64(((uint64_t)1 << n) - 1), zmm);
        }
    } 
    // cut off vector to n elements. The last 64-n elements are set to zero
    Vec64c & cutoff(int n) {
        if (n < 64) {
            zmm = _mm512_maskz_mov_epi8(__mmask64(((uint64_t)1 << n) - 1), zmm);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec64c const insert(int index, int8_t value) {
        zmm = _mm512_mask_set1_epi8(zmm, __mmask64((uint64_t)1 << index), value);
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m512i x = _mm512_maskz_compress_epi8(__mmask64((uint64_t)1 << index), zmm);
        return (int8_t)_mm_cvtsi128_si32(_mm512_castsi512_si128(x));        
#else 
        int8_t a[64];
        store(a);
        return a[index & 63];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec32c:
    Vec32c get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec32c get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static constexpr int size() {
        return 64;
    }
    static constexpr int elementtype() {
        return 4;
    }
};


/*****************************************************************************
*
*          Vec64b: Vector of 64 Booleans for use with Vec64c and Vec64uc
*
*****************************************************************************/

class Vec64b {
protected:
    __mmask64  mm; // Boolean vector
public:
    // Default constructor:
    Vec64b () {
    }
    // Constructor to build from all elements:
    /*
    Vec64b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7,
        bool b8,  bool b9,  bool b10, bool b11, bool b12, bool b13, bool b14, bool b15, 
        bool b16, bool b17, bool b18, bool b19, bool b20, bool b21, bool b22, bool b23,
        bool b24, bool b25, bool b26, bool b27, bool b28, bool b29, bool b30, bool b31,
        bool b32, bool b33, bool b34, bool b35, bool b36, bool b37, bool b38, bool b39,
        bool b40, bool b41, bool b42, bool b43, bool b44, bool b45, bool b46, bool b47,
        bool b48, bool b49, bool b50, bool b51, bool b52, bool b53, bool b54, bool b55,
        bool b56, bool b57, bool b58, bool b59, bool b60, bool b61, bool b62, bool b63) {
        mm = uint64_t(
            (uint64_t)b0        | (uint64_t)b1  << 1  | (uint64_t)b2  << 2  | (uint64_t)b3  << 3  |
            (uint64_t)b4  << 4  | (uint64_t)b5  << 5  | (uint64_t)b6  << 6  | (uint64_t)b7  << 7  |
            (uint64_t)b8  << 8  | (uint64_t)b9  << 9  | (uint64_t)b10 << 10 | (uint64_t)b11 << 11 |
            (uint64_t)b12 << 12 | (uint64_t)b13 << 13 | (uint64_t)b14 << 14 | (uint64_t)b15 << 15 |        
            (uint64_t)b16 << 16 | (uint64_t)b17 << 17 | (uint64_t)b18 << 18 | (uint64_t)b19 << 19 |
            (uint64_t)b20 << 20 | (uint64_t)b21 << 21 | (uint64_t)b22 << 22 | (uint64_t)b23 << 23 |
            (uint64_t)b24 << 24 | (uint64_t)b25 << 25 | (uint64_t)b26 << 26 | (uint64_t)b27 << 27 |
            (uint64_t)b28 << 28 | (uint64_t)b29 << 29 | (uint64_t)b30 << 30 | (uint64_t)b31 << 31 |        
            (uint64_t)b32 << 32 | (uint64_t)b33 << 33 | (uint64_t)b34 << 34 | (uint64_t)b35 << 35 |
            (uint64_t)b36 << 36 | (uint64_t)b37 << 37 | (uint64_t)b38 << 38 | (uint64_t)b39 << 39 |
            (uint64_t)b40 << 40 | (uint64_t)b41 << 41 | (uint64_t)b42 << 42 | (uint64_t)b43 << 43 |
            (uint64_t)b44 << 44 | (uint64_t)b45 << 45 | (uint64_t)b46 << 46 | (uint64_t)b47 << 47 |
            (uint64_t)b48 << 48 | (uint64_t)b49 << 49 | (uint64_t)b50 << 50 | (uint64_t)b51 << 51 |
            (uint64_t)b52 << 52 | (uint64_t)b53 << 53 | (uint64_t)b54 << 54 | (uint64_t)b55 << 55 |
            (uint64_t)b56 << 56 | (uint64_t)b57 << 57 | (uint64_t)b58 << 58 | (uint64_t)b59 << 59 |
            (uint64_t)b60 << 60 | (uint64_t)b61 << 61 | (uint64_t)b62 << 62 | (uint64_t)b63 << 63);
    } */
    // Constructor to convert from type __mmask64 used in intrinsics:
    Vec64b (__mmask64 x) {
        mm = x;
    }
    // Constructor to broadcast single value:
    Vec64b(bool b) {
        mm = __mmask64(-int64_t(b));    
    }
    // Constructor to make from two halves
    Vec64b(Vec32b const x0, Vec32b const x1) {
        mm = uint32_t(__mmask32(x0)) | uint64_t(__mmask32(x1)) << 32;
    }
    // Assignment operator to convert from type __mmask64 used in intrinsics:
    Vec64b & operator = (__mmask64 x) {
        mm = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec64b & operator = (bool b) {
        mm = Vec64b(b);
        return *this;
    }
    // split into two halves
    Vec32b get_low() const {
        return Vec32b(__mmask32(mm));
    }
    Vec32b get_high() const {
        return Vec32b(__mmask32(mm >> 32));
    }
    // Member function to change a single element in vector
    Vec64b & insert (uint32_t index, bool a) {
        uint64_t mask = uint64_t(1) << index;
        mm = (mm & ~mask) | uint64_t(a) << index;
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const { 
        return ((mm >> index) & 1) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Type cast operator to convert to __mmask64 used in intrinsics
    operator __mmask64() const {
        return mm;
    }
    // Member function to change a bitfield to a boolean vector
    Vec64b & load_bits(uint64_t a) {
        mm = __mmask64(a);
        return *this;
    }
    static constexpr int size() {
        return 64;
    }
    static constexpr int elementtype() {
        return 2;
    }
};

typedef Vec64b Vec64cb;   // compact boolean vector
typedef Vec64b Vec64ucb;  // compact boolean vector


/*****************************************************************************
*
*          Define operators and functions for Vec64cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec64cb operator & (Vec64cb const a, Vec64cb const b) {
    //return _kand_mask64(a, b);
    return __mmask64(a) & __mmask64(b);
}
static inline Vec64cb operator && (Vec64cb const a, Vec64cb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec64cb & operator &= (Vec64cb & a, Vec64cb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec64cb operator | (Vec64cb const a, Vec64cb const b) {
    //return _kor_mask64(a, b);
    return __mmask64(a) | __mmask64(b);
}
static inline Vec64cb operator || (Vec64cb const a, Vec64cb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec64cb & operator |= (Vec64cb & a, Vec64cb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec64cb operator ^ (Vec64cb const a, Vec64cb const b) {
    //return _kxor_mask64(a, b);
    return __mmask64(a) ^ __mmask64(b);
}
// vector operator ^= : bitwise xor
static inline Vec64cb & operator ^= (Vec64cb & a, Vec64cb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec64cb operator == (Vec64cb const a, Vec64cb const b) {
    return __mmask64(a) ^ ~ __mmask64(b);
    //return _kxnor_mask64(a, b); // not all compilers have this intrinsic
}

// vector operator != : xor
static inline Vec64cb operator != (Vec64cb const a, Vec64cb const b) {
    //return _kxor_mask64(a, b);
    return __mmask64(a) ^ __mmask64(b);
}

// vector operator ~ : bitwise not
static inline Vec64cb operator ~ (Vec64cb const a) {
    //return _knot_mask64(a);
    return ~ __mmask64(a);
}

// vector operator ! : element not
static inline Vec64cb operator ! (Vec64cb const a) {
    return ~a;
}

// vector function andnot
static inline Vec64cb andnot (Vec64cb const a, Vec64cb const b) {
    //return  _kxnor_mask64(b, a);
    return __mmask64(a) & ~ __mmask64(b);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec64cb const a) {
    return int64_t(__mmask64(a)) == -(int64_t)(1);
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec64cb const a) {
    return int64_t(__mmask64(a)) != 0;
}

// to_bits: convert boolean vector to integer bitfield
static inline uint64_t to_bits(Vec64cb x) {
    return uint64_t(__mmask64(x));
}


/*****************************************************************************
*
*          Define operators for Vec64c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec64c operator + (Vec64c const a, Vec64c const b) {
    return _mm512_add_epi8(a, b);
}
// vector operator += : add
static inline Vec64c & operator += (Vec64c & a, Vec64c const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec64c operator ++ (Vec64c & a, int) {
    Vec64c a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec64c & operator ++ (Vec64c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec64c operator - (Vec64c const a, Vec64c const b) {
    return _mm512_sub_epi8(a, b);
}
// vector operator - : unary minus
static inline Vec64c operator - (Vec64c const a) {
    return _mm512_sub_epi8(_mm512_setzero_epi32(), a);
}
// vector operator -= : subtract
static inline Vec64c & operator -= (Vec64c & a, Vec64c const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec64c operator -- (Vec64c & a, int) {
    Vec64c a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec64c & operator -- (Vec64c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec64c operator * (Vec64c const a, Vec64c const b) {
    // There is no 8-bit multiply. Split into two 16-bit multiplies
    __m512i aodd    = _mm512_srli_epi16(a,8);              // odd numbered elements of a
    __m512i bodd    = _mm512_srli_epi16(b,8);              // odd numbered elements of b
    __m512i muleven = _mm512_mullo_epi16(a,b);             // product of even numbered elements
    __m512i mulodd  = _mm512_mullo_epi16(aodd,bodd);       // product of odd  numbered elements
    mulodd          = _mm512_slli_epi16(mulodd,8);         // put odd numbered elements back in place
    __m512i product = _mm512_mask_mov_epi8(muleven, 0xAAAAAAAAAAAAAAAA, mulodd); // interleave even and odd
    return product;
}

// vector operator *= : multiply
static inline Vec64c & operator *= (Vec64c & a, Vec64c const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec64c operator << (Vec64c const a, int32_t b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;                   // mask to remove bits that are shifted out
    __m512i am    = _mm512_and_si512(a,_mm512_set1_epi8((char)mask));// remove bits that will overflow
    __m512i res   = _mm512_sll_epi16(am,_mm_cvtsi32_si128(b));       // 16-bit shifts
    return res;
}

// vector operator <<= : shift left
static inline Vec64c & operator <<= (Vec64c & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec64c operator >> (Vec64c const a, int32_t b) {
    __m512i aeven = _mm512_slli_epi16(a, 8);                            // even numbered elements of a. get sign bit in position
    aeven         = _mm512_sra_epi16(aeven, _mm_cvtsi32_si128(b + 8));  // shift arithmetic, back to position
    __m512i aodd  = _mm512_sra_epi16(a, _mm_cvtsi32_si128(b));          // shift odd numbered elements arithmetic
    __m512i res = _mm512_mask_mov_epi8(aeven, 0xAAAAAAAAAAAAAAAA, aodd);// interleave even and odd
    return  res;
}
// vector operator >>= : shift right arithmetic
static inline Vec64c & operator >>= (Vec64c & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec64cb operator == (Vec64c const a, Vec64c const b) {
    return _mm512_cmpeq_epi8_mask(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec64cb operator != (Vec64c const a, Vec64c const b) {
    return _mm512_cmpneq_epi8_mask(a, b);
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec64cb operator > (Vec64c const a, Vec64c const b) {
    return _mm512_cmp_epi8_mask(a, b, 6);
}

// vector operator < : returns true for elements for which a < b
static inline Vec64cb operator < (Vec64c const a, Vec64c const b) {
    return _mm512_cmp_epi8_mask(a, b, 1);
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec64cb operator >= (Vec64c const a, Vec64c const b) {
    return _mm512_cmp_epi8_mask(a, b, 5);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec64cb operator <= (Vec64c const a, Vec64c const b) {
    return _mm512_cmp_epi8_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec64c operator & (Vec64c const a, Vec64c const b) {
    return _mm512_and_epi32(a, b);
}

// vector operator &= : bitwise and
static inline Vec64c & operator &= (Vec64c & a, Vec64c const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec64c operator | (Vec64c const a, Vec64c const b) {
    return _mm512_or_epi32(a, b);
}

// vector operator |= : bitwise or
static inline Vec64c & operator |= (Vec64c & a, Vec64c const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec64c operator ^ (Vec64c const a, Vec64c const b) {
    return _mm512_xor_epi32(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec64c & operator ^= (Vec64c & a, Vec64c const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec64c operator ~ (Vec64c const a) {
    return Vec64c(~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec64c select (Vec64cb const s, Vec64c const a, Vec64c const b) {
    return _mm512_mask_mov_epi8(b, s, a);  // conditional move may be optimized better by the compiler than blend
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec64c if_add (Vec64cb const f, Vec64c const a, Vec64c const b) {
    return _mm512_mask_add_epi8(a, f, a, b);
}

// Conditional subtract
static inline Vec64c if_sub (Vec64cb const f, Vec64c const a, Vec64c const b) {
    return _mm512_mask_sub_epi8(a, f, a, b);
}

// Conditional multiply
static inline Vec64c if_mul (Vec64cb const f, Vec64c const a, Vec64c const b) {
    Vec64c m = a * b;
    return select(f, m, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int8_t horizontal_add (Vec64c const a) {
    __m512i sum1 = _mm512_sad_epu8(a,_mm512_setzero_si512());
    return (int8_t)horizontal_add(Vec8q(sum1));
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec64c const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec64c add_saturated(Vec64c const a, Vec64c const b) {
    return _mm512_adds_epi8(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec64c sub_saturated(Vec64c const a, Vec64c const b) {
    return _mm512_subs_epi8(a, b);
}

// function max: a > b ? a : b
static inline Vec64c max(Vec64c const a, Vec64c const b) {
    return _mm512_max_epi8(a,b);
}

// function min: a < b ? a : b
static inline Vec64c min(Vec64c const a, Vec64c const b) {
    return _mm512_min_epi8(a,b);

}

// function abs: a >= 0 ? a : -a
static inline Vec64c abs(Vec64c const a) {
    return _mm512_abs_epi8(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec64c abs_saturated(Vec64c const a) {
    return _mm512_min_epu8(abs(a), Vec64c(0x7F));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec64c rotate_left(Vec64c const a, int b) {
    uint8_t mask  = 0xFFu << b;                    // mask off overflow bits
    __m512i m     = _mm512_set1_epi8(mask);
    __m128i bb    = _mm_cvtsi32_si128(b & 7);      // b modulo 8
    __m128i mbb   = _mm_cvtsi32_si128((- b) & 7);  // 8-b modulo 8
    __m512i left  = _mm512_sll_epi16(a, bb);       // a << b
    __m512i right = _mm512_srl_epi16(a, mbb);      // a >> 8-b
            left  = _mm512_and_si512(m, left);     // mask off overflow bits
            right = _mm512_andnot_si512(m, right);
    return  _mm512_or_si512(left, right);          // combine left and right shifted bits
}


/*****************************************************************************
*
*          Vector of 64 8-bit unsigned integers
*
*****************************************************************************/

class Vec64uc : public Vec64c {
public:
    // Default constructor:
    Vec64uc() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec64uc(uint8_t i) {
        zmm = _mm512_set1_epi8((int8_t)i);
    }
    // Constructor to build from all elements:
    Vec64uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31,
        uint8_t i32, uint8_t i33, uint8_t i34, uint8_t i35, uint8_t i36, uint8_t i37, uint8_t i38, uint8_t i39,        
        uint8_t i40, uint8_t i41, uint8_t i42, uint8_t i43, uint8_t i44, uint8_t i45, uint8_t i46, uint8_t i47,        
        uint8_t i48, uint8_t i49, uint8_t i50, uint8_t i51, uint8_t i52, uint8_t i53, uint8_t i54, uint8_t i55,        
        uint8_t i56, uint8_t i57, uint8_t i58, uint8_t i59, uint8_t i60, uint8_t i61, uint8_t i62, uint8_t i63) 
        : Vec64c(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,        
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31,
            i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,        
            i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63) {}

    // Constructor to build from two Vec32uc:
    Vec64uc(Vec32uc const a0, Vec32uc const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec64uc(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec64uc & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec64uc & load(void const * p) {
        Vec64c::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec64uc & load_a(void const * p) {
        Vec64c::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec64uc const insert(int index, uint8_t value) {
        Vec64c::insert(index, (int8_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(int index) const {
        return (uint8_t)Vec64c::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (int index) const {
        return (uint8_t)Vec64c::extract(index);
    }
    // Member functions to split into two Vec32uc:
    Vec32uc get_low() const {
        return Vec32uc(Vec64c::get_low());
    }
    Vec32uc get_high() const {
        return Vec32uc(Vec64c::get_high());
    }
    static constexpr int elementtype() {
        return 5;
    }
};

// Define operators for this class

// vector operator + : add element by element
static inline Vec64uc operator + (Vec64uc const a, Vec64uc const b) {
    return _mm512_add_epi8(a, b);
}

// vector operator - : subtract element by element
static inline Vec64uc operator - (Vec64uc const a, Vec64uc const b) {
    return _mm512_sub_epi8(a, b);
}

// vector operator ' : multiply element by element
static inline Vec64uc operator * (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) * Vec64c(b));
}

// vector operator / : divide. See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec64uc operator >> (Vec64uc const a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF << (uint32_t)b;                     // mask to remove bits that are shifted out
    __m512i am    = _mm512_and_si512(a,_mm512_set1_epi8((char)mask));  // remove bits that will overflow
    __m512i res   = _mm512_srl_epi16(am,_mm_cvtsi32_si128((int32_t)b));// 16-bit shifts
    return res;
}
static inline Vec64uc operator >> (Vec64uc const a, int b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec64uc & operator >>= (Vec64uc & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical (signed b)
static inline Vec64uc & operator >>= (Vec64uc & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec64uc operator << (Vec64uc const a, uint32_t b) {
    return Vec64uc(Vec64c(a) << int32_t(b));
}
static inline Vec64uc operator << (Vec64uc const a, int b) {
    return a << (uint32_t)b;
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec64cb operator < (Vec64uc const a, Vec64uc const b) {
    return _mm512_cmp_epu8_mask(a, b, 1);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec64cb operator > (Vec64uc const a, Vec64uc const b) {
    return _mm512_cmp_epu8_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec64cb operator >= (Vec64uc const a, Vec64uc const b) {
    return _mm512_cmp_epu8_mask(a, b, 5);
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec64cb operator <= (Vec64uc const a, Vec64uc const b) {
    return _mm512_cmp_epu8_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec64uc operator & (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) & Vec64c(b));
}

// vector operator | : bitwise or
static inline Vec64uc operator | (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) | Vec64c(b));
}

// vector operator ^ : bitwise xor
static inline Vec64uc operator ^ (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) ^ Vec64c(b));
}

// vector operator ~ : bitwise not
static inline Vec64uc operator ~ (Vec64uc const a) {
    return Vec64uc( ~ Vec64c(a));
}


// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec64uc select (Vec64cb const s, Vec64uc const a, Vec64uc const b) {
    return Vec64uc(select(s, Vec64c(a), Vec64c(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec64uc if_add (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    return _mm512_mask_add_epi8(a, f, a, b);
}

// Conditional subtract
static inline Vec64uc if_sub (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    return _mm512_mask_sub_epi8(a, f, a, b);
}

// Conditional multiply
static inline Vec64uc if_mul (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    Vec64uc m = a * b;
    return select(f, m, a);
} 

// function add_saturated: add element by element, unsigned with saturation
static inline Vec64uc add_saturated(Vec64uc const a, Vec64uc const b) {
    return _mm512_adds_epu8(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec64uc sub_saturated(Vec64uc const a, Vec64uc const b) {
    return _mm512_subs_epu8(a, b);
}

// function max: a > b ? a : b
static inline Vec64uc max(Vec64uc const a, Vec64uc const b) {
    return _mm512_max_epu8(a,b);
}

// function min: a < b ? a : b
static inline Vec64uc min(Vec64uc const a, Vec64uc const b) {
    return _mm512_min_epu8(a,b);
}


/*****************************************************************************
*
*          Vector of 32 16-bit signed integers
*
*****************************************************************************/

class Vec32s: public Vec512b {
public:
    // Default constructor:
    Vec32s() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec32s(int16_t i) {
        zmm = _mm512_set1_epi16(i);
    }
    // Constructor to build from all elements:
    Vec32s(int16_t i0, int16_t i1, int16_t i2, int16_t i3, int16_t i4, int16_t i5, int16_t i6, int16_t i7,
        int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15,        
        int16_t i16, int16_t i17, int16_t i18, int16_t i19, int16_t i20, int16_t i21, int16_t i22, int16_t i23,
        int16_t i24, int16_t i25, int16_t i26, int16_t i27, int16_t i28, int16_t i29, int16_t i30, int16_t i31) {
#if true
        // _mm512_set_epi16 missing in GCC 7.4.0. This may be more efficient after all:
        int16_t aa[32] = {
            i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,        
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31 };
        load(aa);
#else
        zmm = _mm512_set_epi16(
            i31, i30, i29, i28, i27, i26, i25, i24, i23, i22, i21, i20, i19, i18, i17, i16,
            i15, i14, i13, i12, i11, i10, i9,  i8,  i7,  i6,  i5,  i4,  i3,  i2,  i1,  i0);
#endif
    }
    // Constructor to build from two Vec16s:
    Vec32s(Vec16s const a0, Vec16s const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec32s(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec32s & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec32s & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec32s & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec32s & load_partial(int n, void const * p) {
        zmm = _mm512_maskz_loadu_epi16(__mmask32(((uint64_t)1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        _mm512_mask_storeu_epi16(p, __mmask32(((uint64_t)1 << n) - 1), zmm);
    } 
    // cut off vector to n elements. The last 32-n elements are set to zero
    Vec32s & cutoff(int n) {
        zmm = _mm512_maskz_mov_epi16(__mmask32(((uint64_t)1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec32s const insert(int index, int16_t value) {
        zmm = _mm512_mask_set1_epi16(zmm, __mmask64((uint64_t)1 << index), value);
        return *this;
    }
    // Member function extract a single element from vector
    int16_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m512i x = _mm512_maskz_compress_epi16(__mmask32(1u << index), zmm);
        return (int16_t)_mm_cvtsi128_si32(_mm512_castsi512_si128(x));        
#else 
        int16_t a[32];
        store(a);
        return a[index & 31];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16s:
    Vec16s get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec16s get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static constexpr int size() {
        return 32;
    }
    static constexpr int elementtype() {
        return 6;
    }
};


/*****************************************************************************
*
*          Vec32sb: Vector of 64 Booleans for use with Vec32s and Vec32us
*
*****************************************************************************/

typedef Vec32b Vec32sb;  // compact boolean vector


/*****************************************************************************
*
*          Define operators for Vec32s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec32s operator + (Vec32s const a, Vec32s const b) {
    return _mm512_add_epi16(a, b);
}
// vector operator += : add
static inline Vec32s & operator += (Vec32s & a, Vec32s const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec32s operator ++ (Vec32s & a, int) {
    Vec32s a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec32s & operator ++ (Vec32s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec32s operator - (Vec32s const a, Vec32s const b) {
    return _mm512_sub_epi16(a, b);
}
// vector operator - : unary minus
static inline Vec32s operator - (Vec32s const a) {
    return _mm512_sub_epi16(_mm512_setzero_epi32(), a);
}
// vector operator -= : subtract
static inline Vec32s & operator -= (Vec32s & a, Vec32s const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec32s operator -- (Vec32s & a, int) {
    Vec32s a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec32s & operator -- (Vec32s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec32s operator * (Vec32s const a, Vec32s const b) {
    return _mm512_mullo_epi16(a, b);
}

// vector operator *= : multiply
static inline Vec32s & operator *= (Vec32s & a, Vec32s const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec32s operator << (Vec32s const a, int32_t b) {
    return _mm512_sll_epi16(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec32s & operator <<= (Vec32s & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec32s operator >> (Vec32s const a, int32_t b) {
    return _mm512_sra_epi16(a, _mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec32s & operator >>= (Vec32s & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32sb operator == (Vec32s const a, Vec32s const b) {
    return _mm512_cmpeq_epi16_mask(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec32sb operator != (Vec32s const a, Vec32s const b) {
    return _mm512_cmpneq_epi16_mask(a, b);
}

// vector operator > : returns true for elements for which a > b
static inline Vec32sb operator > (Vec32s const a, Vec32s const b) {
    return _mm512_cmp_epi16_mask(a, b, 6);
}

// vector operator < : returns true for elements for which a < b
static inline Vec32sb operator < (Vec32s const a, Vec32s const b) {
    return _mm512_cmp_epi16_mask(a, b, 1);
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32sb operator >= (Vec32s const a, Vec32s const b) {
    return _mm512_cmp_epi16_mask(a, b, 5);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32sb operator <= (Vec32s const a, Vec32s const b) {
    return _mm512_cmp_epi16_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec32s operator & (Vec32s const a, Vec32s const b) {
    return _mm512_and_epi32(a, b);
}

// vector operator &= : bitwise and
static inline Vec32s & operator &= (Vec32s & a, Vec32s const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32s operator | (Vec32s const a, Vec32s const b) {
    return _mm512_or_epi32(a, b);
}

// vector operator |= : bitwise or
static inline Vec32s & operator |= (Vec32s & a, Vec32s const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32s operator ^ (Vec32s const a, Vec32s const b) {
    return _mm512_xor_epi32(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec32s & operator ^= (Vec32s & a, Vec32s const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32s operator ~ (Vec32s const a) {
    return Vec32s(~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32s select (Vec32sb const s, Vec32s const a, Vec32s const b) {
    return _mm512_mask_mov_epi16(b, s, a);  // conditional move may be optimized better by the compiler than blend
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32s if_add (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return _mm512_mask_add_epi16(a, f, a, b);
}

// Conditional subtract
static inline Vec32s if_sub (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return _mm512_mask_sub_epi16(a, f, a, b);
}

// Conditional multiply
static inline Vec32s if_mul (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return _mm512_mask_mullo_epi16(a, f, a, b);    
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline int16_t horizontal_add (Vec32s const a) {
    Vec16s s = a.get_low() + a.get_high();
    return horizontal_add(s);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32s const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec32s add_saturated(Vec32s const a, Vec32s const b) {
    return _mm512_adds_epi16(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32s sub_saturated(Vec32s const a, Vec32s const b) {
    return _mm512_subs_epi16(a, b);
}

// function max: a > b ? a : b
static inline Vec32s max(Vec32s const a, Vec32s const b) {
    return _mm512_max_epi16(a,b);
}

// function min: a < b ? a : b
static inline Vec32s min(Vec32s const a, Vec32s const b) {
    return _mm512_min_epi16(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec32s abs(Vec32s const a) {
    return _mm512_abs_epi16(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32s abs_saturated(Vec32s const a) {
    return _mm512_min_epu16(abs(a), Vec32s(0x7FFF));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32s rotate_left(Vec32s const a, int b) {
    __m512i left  = _mm512_sll_epi16(a,_mm_cvtsi32_si128(b & 0xF));      // a << b 
    __m512i right = _mm512_srl_epi16(a,_mm_cvtsi32_si128((16-b) & 0xF)); // a >> (32 - b)
    __m512i rot   = _mm512_or_si512(left,right);                         // or
    return  rot;
} 


/*****************************************************************************
*
*          Vector of 32 16-bit unsigned integers
*
*****************************************************************************/

class Vec32us : public Vec32s {
public:
    // Default constructor:
    Vec32us() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec32us(uint16_t i) {
        zmm = _mm512_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements. Inherit from Vec32s
    Vec32us(uint16_t i0, uint16_t i1, uint16_t i2, uint16_t i3, uint16_t i4, uint16_t i5, uint16_t i6, uint16_t i7,
        uint16_t i8,  uint16_t i9,  uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15,        
        uint16_t i16, uint16_t i17, uint16_t i18, uint16_t i19, uint16_t i20, uint16_t i21, uint16_t i22, uint16_t i23,
        uint16_t i24, uint16_t i25, uint16_t i26, uint16_t i27, uint16_t i28, uint16_t i29, uint16_t i30, uint16_t i31) 
    : Vec32s(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,        
         i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31) {}

    // Constructor to build from two Vec16us:
    Vec32us(Vec16us const a0, Vec16us const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec32us(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec32us & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec32us & load(void const * p) {
        Vec32s::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec32us & load_a(void const * p) {
        Vec32s::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec32us const insert(int index, uint16_t value) {
        Vec32s::insert(index, (int16_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint16_t extract(int index) const {
        return (uint16_t)Vec32s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (int index) const {
        return (uint16_t)Vec32s::extract(index);
    }
    // Member functions to split into two Vec16us:
    Vec16us get_low() const {
        return Vec16us(Vec32s::get_low());
    }
    Vec16us get_high() const {
        return Vec16us(Vec32s::get_high());
    }
    static constexpr int elementtype() {
        return 7;
    }
};

// Define operators for this class

// vector operator + : add element by element
static inline Vec32us operator + (Vec32us const a, Vec32us const b) {
    return _mm512_add_epi16(a, b);
}

// vector operator - : subtract element by element
static inline Vec32us operator - (Vec32us const a, Vec32us const b) {
    return _mm512_sub_epi16(a, b);
}

// vector operator * : multiply element by element
static inline Vec32us operator * (Vec32us const a, Vec32us const b) {
    return _mm512_mullo_epi16(a, b);
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec32us operator >> (Vec32us const a, uint32_t b) {
    return _mm512_srl_epi16(a, _mm_cvtsi32_si128((int)b));
}
static inline Vec32us operator >> (Vec32us const a, int b) {
    return a >> uint32_t(b);
}

// vector operator >>= : shift right logical
static inline Vec32us & operator >>= (Vec32us & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical (signed b)
static inline Vec32us & operator >>= (Vec32us & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec32us operator << (Vec32us const a, uint32_t b) {
    return _mm512_sll_epi16(a, _mm_cvtsi32_si128((int)b));
}
static inline Vec32us operator << (Vec32us const a, int b) {
    return a << uint32_t(b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32sb operator < (Vec32us const a, Vec32us const b) {
    return _mm512_cmp_epu16_mask(a, b, 1);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32sb operator > (Vec32us const a, Vec32us const b) {
    return _mm512_cmp_epu16_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec32sb operator >= (Vec32us const a, Vec32us const b) {
    return _mm512_cmp_epu16_mask(a, b, 5);
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32sb operator <= (Vec32us const a, Vec32us const b) {
    return _mm512_cmp_epu16_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec32us operator & (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) & Vec32s(b));
}

// vector operator | : bitwise or
static inline Vec32us operator | (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) | Vec32s(b));
}

// vector operator ^ : bitwise xor
static inline Vec32us operator ^ (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) ^ Vec32s(b));
}

// vector operator ~ : bitwise not
static inline Vec32us operator ~ (Vec32us const a) {
    return Vec32us( ~ Vec32s(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32us select (Vec32sb const s, Vec32us const a, Vec32us const b) {
    return Vec32us(select(s, Vec32s(a), Vec32s(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32us if_add (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return _mm512_mask_add_epi16(a, f, a, b);
}

// Conditional subtract
static inline Vec32us if_sub (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return _mm512_mask_sub_epi16(a, f, a, b);
}

// Conditional multiply
static inline Vec32us if_mul (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return _mm512_mask_mullo_epi16(a, f, a, b);    
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32us add_saturated(Vec32us const a, Vec32us const b) {
    return _mm512_adds_epu16(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32us sub_saturated(Vec32us const a, Vec32us const b) {
    return _mm512_subs_epu16(a, b);
}

// function max: a > b ? a : b
static inline Vec32us max(Vec32us const a, Vec32us const b) {
    return _mm512_max_epu16(a,b);
}

// function min: a < b ? a : b
static inline Vec32us min(Vec32us const a, Vec32us const b) {
    return _mm512_min_epu16(a,b);
}


/*****************************************************************************
*
*          Vector permute functions
*
******************************************************************************
*
* These permute functions can reorder the elements of a vector and optionally
* set some elements to zero. See Vectori128.h for description
*
*****************************************************************************/

// Permute vector of 32 16-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int... i0 >
    static inline Vec32s permute32(Vec32s const a) {
    int constexpr indexs[32] = { i0... };    
    __m512i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec32s>(indexs);

    static_assert(sizeof... (i0) == 32, "permute32 must have 32 indexes");
    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {                   // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {         // use larger permutation
            constexpr EList<int, 16> L = largeblock_perm<32>(indexs); // permutation pattern
            y = permute16 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7],
                L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]> (Vec16i(a));
            if (!(flags & perm_addz)) return y;                 // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in all lanes
            if constexpr ((flags & perm_rotate) != 0) {         // fits palignr. rotate within lanes
                y = _mm512_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF);
            }
            else { // use pshufb
                const EList <int8_t, 64> bm = pshufb_mask<Vec32s>(indexs);
                return _mm512_shuffle_epi8(a, Vec32s().load(bm.a));
            }
        } 
        else {  // different patterns in all lanes
            if constexpr ((flags & perm_cross_lane) == 0) {     // no lane crossing. Use pshufb
                const EList <int8_t, 64> bm = pshufb_mask<Vec32s>(indexs);
                return _mm512_shuffle_epi8(a, Vec32s().load(bm.a));
            }
            else if constexpr ((flags & perm_rotate_big) != 0) {// fits full rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count) * 2; // rotate count
                constexpr uint8_t r1 = (rot >> 4 << 1) & 7;
                constexpr uint8_t r2 = (r1 + 2) & 7;
                __m512i y1 = a, y2 = a;
                if constexpr (r1 != 0) y1 = _mm512_alignr_epi64 (a, a, r1); // rotate 128-bit blocks
                if constexpr (r2 != 0) y2 = _mm512_alignr_epi64 (a, a, r2); // rotate 128-bit blocks
                y = _mm512_alignr_epi8(y2, y1, rot & 15);
            }
            else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
                y = _mm512_broadcastw_epi16(_mm512_castsi512_si128(y));     // broadcast first element
            }
            else if constexpr ((flags & perm_zext) != 0) {     // fits zero extension
                y = _mm512_cvtepu16_epi32(_mm512_castsi512_si256(y));  // zero extension
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#if defined (__AVX512VBMI2__)
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_epi16(__mmask32(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_epi16(__mmask32(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#endif  // AVX512VBMI2
            else {  // full permute needed
                const EList <int16_t, 32> bm = perm_mask_broad<Vec32s>(indexs);
                y = _mm512_permutexvar_epi16 (Vec32s().load(bm.a), y);
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {           // additional zeroing needed
        y = _mm512_maskz_mov_epi16(zero_mask<32>(indexs), y);
    }
    return y;
}

template <int... i0 >
    static inline Vec32us permute32(Vec32us const a) {
    return Vec32us (permute32<i0...> (Vec32s(a)));
}


// Permute vector of 64 8-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int... i0 >
static inline Vec64c permute64(Vec64c const a) {
    int constexpr indexs[64] = { i0... };
    __m512i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec64c>(indexs);

    static_assert(sizeof... (i0) == 64, "permute64 must have 64 indexes");
    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) {
        return _mm512_setzero_si512();                                    // just return zero
    }
    if constexpr ((flags & perm_perm) != 0) {                             // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {                   // use larger permutation
            constexpr EList<int, 32> L = largeblock_perm<64>(indexs);      // permutation pattern
            y = permute32 <
                L.a[0],  L.a[1],  L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
                L.a[8],  L.a[9],  L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15], 
                L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23], 
                L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31]> 
                (Vec32s(a));
            if (!(flags & perm_addz)) return y;                           // no remaining zeroing
        }
        else {
            if constexpr ((flags & perm_cross_lane) == 0) {               // no lane crossing. Use pshufb
                const EList <int8_t, 64> bm = pshufb_mask<Vec64c>(indexs);
                return _mm512_shuffle_epi8(a, Vec64c().load(bm.a));
            }
            else if constexpr ((flags & perm_rotate_big) != 0) {          // fits full rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotate count
                constexpr uint8_t r1 = (rot >> 4 << 1) & 7;
                constexpr uint8_t r2 = (r1 + 2) & 7;
                __m512i y1 = a, y2 = a;
                if constexpr (r1 != 0) y1 = _mm512_alignr_epi64(y, y, r1);// rotate 128-bit blocks
                if constexpr (r2 != 0) y2 = _mm512_alignr_epi64(a, a, r2);// rotate 128-bit blocks
                y = _mm512_alignr_epi8(y2, y1, rot & 15);
            }
            else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
                y = _mm512_broadcastb_epi8(_mm512_castsi512_si128(y));    // broadcast first element
            }
            else if constexpr ((flags & perm_zext) != 0) {                // fits zero extension
                y = _mm512_cvtepu8_epi16(_mm512_castsi512_si256(y));      // zero extension
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#if defined (__AVX512VBMI2__)
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_epi8(__mmask64(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_epi8(__mmask64(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#endif  // AVX512VBMI2
            else {      // full permute needed
#ifdef __AVX512VBMI__   // full permute instruction available
                const EList <int8_t, 64> bm = perm_mask_broad<Vec64c>(indexs);
                y = _mm512_permutexvar_epi8(Vec64c().load(bm.a), y);
#else
                // There is no 8-bit full permute. Use 16-bit permute
                // getevenmask: get permutation mask for destination bytes with even position
                auto getevenmask = [](int const (&indexs)[64]) constexpr {
                    EList<uint16_t, 32> u = {{0}};       // list to return
                    for (int i = 0; i < 64; i += 2) {    // loop through even indexes
                        uint16_t ix = indexs[i] & 63;
                        // source bytes with odd position are in opposite 16-bit word because of 32-bit rotation
                        u.a[i>>1] = ((ix >> 1) ^ (ix & 1)) | (((ix & 1) ^ 1) << 5); 
                    }
                    return u;
                };
                // getoddmask: get permutation mask for destination bytes with odd position
                auto getoddmask = [](int const (&indexs)[64]) constexpr {
                    EList<uint16_t, 32> u = {{0}};       // list to return
                    for (int i = 1; i < 64; i += 2) {  // loop through odd indexes
                        uint16_t ix = indexs[i] & 63;
                        u.a[i>>1] = (ix >> 1) | ((ix & 1) << 5);
                    }
                    return u;
                };
                EList<uint16_t, 32> evenmask = getevenmask(indexs);
                EList<uint16_t, 32> oddmask  = getoddmask (indexs);
                // Rotate to get odd bytes into even position, and vice versa.
                // There is no 16-bit rotate, use 32-bit rotate.
                // The wrong position of the odd bytes is compensated for in getevenmask
                __m512i ro    = _mm512_rol_epi32 (a, 8);                     // rotate
                __m512i yeven = _mm512_permutex2var_epi16(ro, Vec32s().load(evenmask.a), a);  // destination bytes with even position
                __m512i yodd  = _mm512_permutex2var_epi16(ro, Vec32s().load(oddmask.a),  a);  // destination bytes with odd  position
                __mmask64 maske = 0x5555555555555555;                        // mask for even position
                y = _mm512_mask_mov_epi8(yodd, maske, yeven);                // interleave even and odd position bytes
#endif         
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {      // additional zeroing needed
        y = _mm512_maskz_mov_epi8(zero_mask<64>(indexs), y);
    }
    return y;
}

template <int... i0 >
static inline Vec64uc permute64(Vec64uc const a) {
    return Vec64uc(permute64<i0...>(Vec64c(a)));
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

// permute and blend Vec32s
template <int ... i0 >
static inline Vec32s blend32(Vec32s const a, Vec32s const b) {
    int constexpr indexs[32] = { i0 ... }; // indexes as array
    static_assert(sizeof... (i0) == 32, "blend32 must have 32 indexes");
    __m512i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec32s>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute32 <i0 ... >(a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        constexpr EList<int, 64> L = blend_perm_indexes<32, 2>(indexs); // get permutation indexes
        return permute32 <
            L.a[32], L.a[33], L.a[34], L.a[35], L.a[36], L.a[37], L.a[38], L.a[39],
            L.a[40], L.a[41], L.a[42], L.a[43], L.a[44], L.a[45], L.a[46], L.a[47],
            L.a[48], L.a[49], L.a[50], L.a[51], L.a[52], L.a[53], L.a[54], L.a[55],
            L.a[56], L.a[57], L.a[58], L.a[59], L.a[60], L.a[61], L.a[62], L.a[63] > (b);
    }
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint32_t mb = (uint32_t)make_bit_mask<32, 0x305>(indexs);  // blend mask
        y = _mm512_mask_mov_epi16(a, mb, b);
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 32-bit blocks
        constexpr EList<int, 16> L = largeblock_perm<32>(indexs); // get 32-bit blend pattern
        y = blend16 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7],
            L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] >
            (Vec16i(a), Vec16i(b));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing
    }
    else { // No special cases
        const EList <int16_t, 32> bm = perm_mask_broad<Vec32s>(indexs);      // full permute
        y = _mm512_permutex2var_epi16(a, Vec32s().load(bm.a), b);
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_epi16(zero_mask<32>(indexs), y);
    }
    return y;
}

template <int ... i0 > 
    static inline Vec32us blend32(Vec32us const a, Vec32us const b) {
    return Vec32us(blend32<i0 ...> (Vec32s(a),Vec32s(b)));
}

    // permute and blend Vec64c
template <int ... i0 >
static inline Vec64c blend64(Vec64c const a, Vec64c const b) {
    int constexpr indexs[64] = { i0 ... }; // indexes as array
    static_assert(sizeof... (i0) == 64, "blend64 must have 64 indexes");
    __m512i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec64c>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute64 <i0 ... >(a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        constexpr EList<int, 128> L = blend_perm_indexes<64, 2>(indexs); // get permutation indexes
        return permute64 <
            L.a[64],  L.a[65],  L.a[66],  L.a[67],  L.a[68],  L.a[69],  L.a[70],  L.a[71],
            L.a[72],  L.a[73],  L.a[74],  L.a[75],  L.a[76],  L.a[77],  L.a[78],  L.a[79],
            L.a[80],  L.a[81],  L.a[82],  L.a[83],  L.a[84],  L.a[85],  L.a[86],  L.a[87],
            L.a[88],  L.a[89],  L.a[90],  L.a[91],  L.a[92],  L.a[93],  L.a[94],  L.a[95],
            L.a[96],  L.a[97],  L.a[98],  L.a[99],  L.a[100], L.a[101], L.a[102], L.a[103],
            L.a[104], L.a[105], L.a[106], L.a[107], L.a[108], L.a[109], L.a[110], L.a[111],
            L.a[112], L.a[113], L.a[114], L.a[115], L.a[116], L.a[117], L.a[118], L.a[119],
            L.a[120], L.a[121], L.a[122], L.a[123], L.a[124], L.a[125], L.a[126], L.a[127]
        > (b);
    }
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint64_t mb = make_bit_mask<64, 0x306>(indexs);  // blend mask
        y = _mm512_mask_mov_epi8(a, mb, b);
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 16-bit blocks
        constexpr EList<int, 32> L = largeblock_perm<64>(indexs); // get 16-bit blend pattern
        y = blend32 <
            L.a[0],  L.a[1],  L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
            L.a[8],  L.a[9],  L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15],
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31]
        > (Vec32s(a), Vec32s(b));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing
    }
    else { // No special cases
#ifdef  __AVX512VBMI__   // AVX512VBMI
        const EList <int8_t, 64> bm = perm_mask_broad<Vec64c>(indexs);      // full permute
        y = _mm512_permutex2var_epi8(a, Vec64c().load(bm.a), b);
#else   // split into two permutes
        constexpr EList<int, 128> L = blend_perm_indexes<64, 0> (indexs);
        __m512i ya = permute64 <
            L.a[0],  L.a[1],  L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
            L.a[8],  L.a[9],  L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15],
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31],
            L.a[32], L.a[33], L.a[34], L.a[35], L.a[36], L.a[37], L.a[38], L.a[39],
            L.a[40], L.a[41], L.a[42], L.a[43], L.a[44], L.a[45], L.a[46], L.a[47],
            L.a[48], L.a[49], L.a[50], L.a[51], L.a[52], L.a[53], L.a[54], L.a[55],
            L.a[56], L.a[57], L.a[58], L.a[59], L.a[60], L.a[61], L.a[62], L.a[63]
        > (a);
        __m512i yb = permute64 <
            L.a[64],  L.a[65],  L.a[66],  L.a[67],  L.a[68],  L.a[69],  L.a[70],  L.a[71],
            L.a[72],  L.a[73],  L.a[74],  L.a[75],  L.a[76],  L.a[77],  L.a[78],  L.a[79],
            L.a[80],  L.a[81],  L.a[82],  L.a[83],  L.a[84],  L.a[85],  L.a[86],  L.a[87],
            L.a[88],  L.a[89],  L.a[90],  L.a[91],  L.a[92],  L.a[93],  L.a[94],  L.a[95],
            L.a[96],  L.a[97],  L.a[98],  L.a[99],  L.a[100], L.a[101], L.a[102], L.a[103],
            L.a[104], L.a[105], L.a[106], L.a[107], L.a[108], L.a[109], L.a[110], L.a[111],
            L.a[112], L.a[113], L.a[114], L.a[115], L.a[116], L.a[117], L.a[118], L.a[119],
            L.a[120], L.a[121], L.a[122], L.a[123], L.a[124], L.a[125], L.a[126], L.a[127]
        > (b);
        uint64_t bm = make_bit_mask<64, 0x306> (indexs);
        y = _mm512_mask_mov_epi8(ya, bm, yb);
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_epi8(zero_mask<64>(indexs), y);
    }
    return y;
}

template <int ... i0 >
static inline Vec64uc blend64(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(blend64 <i0 ...>(Vec64c(a), Vec64c(b)));
}


/*****************************************************************************
*
*          Vector lookup functions
*
******************************************************************************
*
* These functions use vector elements as indexes into a table.
* The table is given as one or more vectors
*
*****************************************************************************/

// lookup in table of 64 int8_t values
static inline Vec64c lookup64(Vec64c const index, Vec64c const table) {
#ifdef  __AVX512VBMI__   // AVX512VBMI instruction set not supported yet (April 2019)
    return _mm512_permutexvar_epi8(index, table);
#else 
    // broadcast each 128-bit lane, because int8_t shuffle is only within 128-bit lanes
    __m512i lane0 = _mm512_broadcast_i32x4(_mm512_castsi512_si128(table));
    __m512i lane1 = _mm512_shuffle_i64x2(table, table, 0x55);
    __m512i lane2 = _mm512_shuffle_i64x2(table, table, 0xAA);
    __m512i lane3 = _mm512_shuffle_i64x2(table, table, 0xFF);
    Vec64c  laneselect = index >> 4;  // upper part of index selects lane
    // select and permute from each lane
    Vec64c  dat0  = _mm512_maskz_shuffle_epi8(      laneselect==0, lane0, index);
    Vec64c  dat1  = _mm512_mask_shuffle_epi8 (dat0, laneselect==1, lane1, index);
    Vec64c  dat2  = _mm512_maskz_shuffle_epi8(      laneselect==2, lane2, index);
    Vec64c  dat3  = _mm512_mask_shuffle_epi8 (dat2, laneselect==3, lane3, index);
    return dat1 | dat3;
#endif
}

// lookup in table of 128 int8_t values
static inline Vec64c lookup128(Vec64c const index, Vec64c const table1, Vec64c const table2) {
#ifdef  __AVX512VBMI__   // AVX512VBMI instruction set not supported yet (April 2019)
    return _mm512_permutex2var_epi8(table1, index, table2);

#else
    // use 16-bits permute, which is included in AVX512BW
    __m512i ieven2 = _mm512_srli_epi16 (index, 1);              // even pos bytes of index / 2 (extra bits will be ignored)
    __m512i e1 = _mm512_permutex2var_epi16(table1, ieven2, table2); // 16-bits results for even pos index
    __mmask32 me1 = (Vec32s(index) & 1) != 0;                   // even pos indexes are odd value
    __m512i e2 = _mm512_mask_srli_epi16(e1, me1, e1, 8);        // combined results for even pos index. get upper 8 bits down if index was odd
    __m512i iodd2  = _mm512_srli_epi16 (index, 9);              // odd  pos bytes of index / 2
    __m512i o1 = _mm512_permutex2var_epi16(table1, iodd2, table2); // 16-bits results for odd pos index
    __mmask32 mo1 = (Vec32s(index) & 0x100) == 0;               // odd pos indexes have even value
    __m512i o2 = _mm512_mask_slli_epi16(o1, mo1, o1, 8);        // combined results for odd pos index. get lower 8 bits up if index was even
    __mmask64 maske = 0x5555555555555555;                       // mask for even position
    return  _mm512_mask_mov_epi8(o2, maske, e2);                // interleave even and odd position result
#endif
}

// lookup in table of 256 int8_t values.
// The complete table of all possible 256 byte values is contained in four vectors
// The index is treated as unsigned
static inline Vec64c lookup256(Vec64c const index, Vec64c const table1, Vec64c const table2, Vec64c const table3, Vec64c const table4) {
#ifdef  __AVX512VBMI__   // AVX512VBMI instruction set not supported yet (April 2019)
    Vec64c d12 = _mm512_permutex2var_epi8(table1, index, table2);
    Vec64c d34 = _mm512_permutex2var_epi8(table3, index, table4);
    return select(index < 0, d34, d12);  // use sign bit to select
#else
    // the AVX512BW version of lookup128 ignores upper bytes of index
    // (the compiler will optimize away common subexpressions of the two lookup128)
    Vec64c d12 = lookup128(index, table1, table2);
    Vec64c d34 = lookup128(index, table3, table4);
    return select(index < 0, d34, d12);
#endif
}


// lookup in table of 32 values
static inline Vec32s lookup32(Vec32s const index, Vec32s const table) {
    return _mm512_permutexvar_epi16(index, table);
}

// lookup in table of 64 values
static inline Vec32s lookup64(Vec32s const index, Vec32s const table1, Vec32s const table2) {
    return _mm512_permutex2var_epi16(table1, index, table2);
}

// lookup in table of 128 values
static inline Vec32s lookup128(Vec32s const index, Vec32s const table1, Vec32s const table2, Vec32s const table3, Vec32s const table4) {
    Vec32s d12 = _mm512_permutex2var_epi16(table1, index, table2);
    Vec32s d34 = _mm512_permutex2var_epi16(table3, index, table4);
    return select((index >> 6) != 0, d34, d12);
}


/*****************************************************************************
*
*          Byte shifts
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
template <unsigned int b>
static inline Vec64c shift_bytes_up(Vec64c const a) {
    __m512i ahi, alo;
    if constexpr (b == 0) return a;
    else if constexpr ((b & 3) == 0) {  // b is divisible by 4
        return _mm512_alignr_epi32(a, _mm512_setzero_si512(), (16 - (b >> 2)) & 15);
    }     
    else if constexpr (b < 16) {    
        alo = a;
        ahi = _mm512_maskz_shuffle_i64x2(0xFC, a, a, 0x90);  // shift a 16 bytes up, zero lower part
    }
    else if constexpr (b < 32) {    
        alo = _mm512_maskz_shuffle_i64x2(0xFC, a, a, 0x90);  // shift a 16 bytes up, zero lower part
        ahi = _mm512_maskz_shuffle_i64x2(0xF0, a, a, 0x40);  // shift a 32 bytes up, zero lower part
    }
    else if constexpr (b < 48) { 
        alo = _mm512_maskz_shuffle_i64x2(0xF0, a, a, 0x40);  // shift a 32 bytes up, zero lower part
        ahi = _mm512_maskz_shuffle_i64x2(0xC0, a, a, 0x00);  // shift a 48 bytes up, zero lower part
    }
    else if constexpr (b < 64) { 
        alo = _mm512_maskz_shuffle_i64x2(0xC0, a, a, 0x00);  // shift a 48 bytes up, zero lower part
        ahi = _mm512_setzero_si512();                        // zero
    }
    else {
        return _mm512_setzero_si512();                       // zero
    }
    return _mm512_alignr_epi8(alo, ahi, 16-(b & 0xF));       // shift within 16-bytes lane
} 

// Function shift_bytes_down: shift whole vector right by b bytes
template <unsigned int b>
static inline Vec64c shift_bytes_down(Vec64c const a) {
    if constexpr ((b & 3) == 0) {  // b is divisible by 4
        return _mm512_alignr_epi32(_mm512_setzero_si512(), a, ((b >> 2) & 15));
    }     
    __m512i ahi, alo;
    if constexpr (b < 16) {
        alo =  _mm512_maskz_shuffle_i64x2(0x3F, a, a, 0x39);  // shift a 16 bytes down, zero upper part
        ahi = a;
    }
    else if constexpr (b < 32) {
        alo = _mm512_maskz_shuffle_i64x2(0x0F, a, a, 0x0E);  // shift a 32 bytes down, zero upper part
        ahi = _mm512_maskz_shuffle_i64x2(0x3F, a, a, 0x39);  // shift a 16 bytes down, zero upper part
    }
    else if constexpr (b < 48) { 
        alo = _mm512_maskz_shuffle_i64x2(0x03, a, a, 0x03);  // shift a 48 bytes down, zero upper part            
        ahi = _mm512_maskz_shuffle_i64x2(0x0F, a, a, 0x0E);  // shift a 32 bytes down, zero upper part
    }
    else if constexpr (b < 64) { 
        alo = _mm512_setzero_si512();            
        ahi = _mm512_maskz_shuffle_i64x2(0x03, a, a, 0x03);  // shift a 48 bytes down, zero upper part
    }
    else {
        return _mm512_setzero_si512();                       // zero
    }
    return _mm512_alignr_epi8(alo, ahi, b & 0xF);            // shift within 16-bytes lane
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 32 elements to 16 bits with sign extension
static inline Vec32s extend_low (Vec64c const a) {
    __m512i a2   = permute8<0,V_DC,1,V_DC,2,V_DC,3,V_DC>(Vec8q(a));  // get low 64-bit blocks
    Vec64cb sign = _mm512_cmpgt_epi8_mask(_mm512_setzero_si512(),a2);// 0 > a2
    __m512i ss   = _mm512_maskz_set1_epi8(sign, -1);
    return         _mm512_unpacklo_epi8(a2, ss);                     // interleave with sign extensions
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec32s extend_high (Vec64c const a) {
    __m512i a2   = permute8<4,V_DC,5,V_DC,6,V_DC,7,V_DC>(Vec8q(a));  // get low 64-bit blocks
    Vec64cb sign = _mm512_cmpgt_epi8_mask(_mm512_setzero_si512(),a2);// 0 > a2
    __m512i ss   = _mm512_maskz_set1_epi8(sign, -1);
    return         _mm512_unpacklo_epi8(a2, ss);                     // interleave with sign extensions
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec32us extend_low (Vec64uc const a) {
    __m512i a2   = permute8<0,V_DC,1,V_DC,2,V_DC,3,V_DC>(Vec8q(a));  // get low 64-bit blocks
    return    _mm512_unpacklo_epi8(a2, _mm512_setzero_si512());      // interleave with zero extensions
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec32us extend_high (Vec64uc const a) {
    __m512i a2   = permute8<4,V_DC,5,V_DC,6,V_DC,7,V_DC>(Vec8q(a));  // get low 64-bit blocks
    return    _mm512_unpacklo_epi8(a2, _mm512_setzero_si512());      // interleave with zero extensions
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec16i extend_low (Vec32s const a) {
    __m512i a2   = permute8<0,V_DC,1,V_DC,2,V_DC,3,V_DC>(Vec8q(a));  // get low 64-bit blocks
    Vec32sb sign = _mm512_cmpgt_epi16_mask(_mm512_setzero_si512(),a2);// 0 > a2
    __m512i ss   = _mm512_maskz_set1_epi16(sign, -1);
    return         _mm512_unpacklo_epi16(a2, ss);                    // interleave with sign extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec16i extend_high (Vec32s const a) {
    __m512i a2   = permute8<4,V_DC,5,V_DC,6,V_DC,7,V_DC>(Vec8q(a));  // get low 64-bit blocks
    Vec32sb sign = _mm512_cmpgt_epi16_mask(_mm512_setzero_si512(),a2);// 0 > a2
    __m512i ss   = _mm512_maskz_set1_epi16(sign, -1);
    return         _mm512_unpacklo_epi16(a2, ss);                    // interleave with sign extensions
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec16ui extend_low (Vec32us const a) {
    __m512i a2   = permute8<0,V_DC,1,V_DC,2,V_DC,3,V_DC>(Vec8q(a));  // get low 64-bit blocks
    return    _mm512_unpacklo_epi16(a2, _mm512_setzero_si512());     // interleave with zero extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec16ui extend_high (Vec32us const a) {
    __m512i a2   = permute8<4,V_DC,5,V_DC,6,V_DC,7,V_DC>(Vec8q(a));  // get low 64-bit blocks
    return    _mm512_unpacklo_epi16(a2, _mm512_setzero_si512());     // interleave with zero extensions
}


// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec64c compress (Vec32s const low, Vec32s const high) {
    __mmask64 mask = 0x5555555555555555;
    __m512i lowm  = _mm512_maskz_mov_epi8 (mask, low);     // bytes of low
    __m512i highm = _mm512_maskz_mov_epi8 (mask, high);    // bytes of high
    __m512i pk    = _mm512_packus_epi16(lowm, highm);      // unsigned pack
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec64c compress_saturated (Vec32s const low, Vec32s const high) {
    __m512i pk    = _mm512_packs_epi16(low,high);          // packed with signed saturation
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec64uc compress (Vec32us const low, Vec32us const high) {
    return  Vec64uc (compress((Vec32s)low, (Vec32s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec64uc compress_saturated (Vec32us const low, Vec32us const high) {
    __m512i maxval  = _mm512_set1_epi32(0x00FF00FF);       // maximum value
    __m512i low1    = _mm512_min_epu16(low,maxval);        // upper limit
    __m512i high1   = _mm512_min_epu16(high,maxval);       // upper limit
    __m512i pk      = _mm512_packus_epi16(low1,high1);     // this instruction saturates from signed 32 bit to unsigned 16 bit
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec32s compress (Vec16i const low, Vec16i const high) {
    __mmask32 mask = 0x55555555;
    __m512i lowm  = _mm512_maskz_mov_epi16 (mask, low);    // words of low
    __m512i highm = _mm512_maskz_mov_epi16 (mask, high);   // words of high
    __m512i pk    = _mm512_packus_epi32(lowm, highm);      // unsigned pack
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec32s compress_saturated (Vec16i const low, Vec16i const high) {
    __m512i pk    =  _mm512_packs_epi32(low,high);         // pack with signed saturation
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec32us compress (Vec16ui const low, Vec16ui const high) {
    return Vec32us (compress((Vec16i)low, (Vec16i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec32us compress_saturated (Vec16ui const low, Vec16ui const high) {
    __m512i maxval  = _mm512_set1_epi32(0x0000FFFF);       // maximum value
    __m512i low1    = _mm512_min_epu32(low,maxval);        // upper limit
    __m512i high1   = _mm512_min_epu32(high,maxval);       // upper limit
    __m512i pk      = _mm512_packus_epi32(low1,high1);     // this instruction saturates from signed 32 bit to unsigned 16 bit
    __m512i in    = constant16ui<0,0,2,0,4,0,6,0,1,0,3,0,5,0,7,0>();
    return  _mm512_permutexvar_epi64(in, pk);              // put in right place
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 32 16-bit signed integers
static inline Vec32s operator / (Vec32s const a, Divisor_s const d) {
    __m512i m   = _mm512_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m512i sgn = _mm512_broadcastq_epi64(d.getsign());    // broadcast sign of d
    __m512i t1  = _mm512_mulhi_epi16(a, m);                // multiply high signed words
    __m512i t2  = _mm512_add_epi16(t1,a);                  // + a
    __m512i t3  = _mm512_sra_epi16(t2,d.gets1());          // shift right arithmetic
    __m512i t4  = _mm512_srai_epi16(a,15);                 // sign of a
    __m512i t5  = _mm512_sub_epi16(t4,sgn);                // sign of a - sign of d
    __m512i t6  = _mm512_sub_epi16(t3,t5);                 // + 1 if a < 0, -1 if d < 0
    return        _mm512_xor_si512(t6,sgn);                // change sign if divisor negative
}

// vector of 16 16-bit unsigned integers
static inline Vec32us operator / (Vec32us const a, Divisor_us const d) {
    __m512i m   = _mm512_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m512i t1  = _mm512_mulhi_epu16(a, m);                // multiply high signed words
    __m512i t2  = _mm512_sub_epi16(a,t1);                  // subtract
    __m512i t3  = _mm512_srl_epi16(t2,d.gets1());          // shift right logical
    __m512i t4  = _mm512_add_epi16(t1,t3);                 // add
    return        _mm512_srl_epi16(t4,d.gets2());          // shift right logical 
}

// vector of 32 8-bit signed integers
static inline Vec64c operator / (Vec64c const a, Divisor_s const d) {
    // sign-extend even-numbered and odd-numbered elements to 16 bits
    Vec32s  even = _mm512_srai_epi16(_mm512_slli_epi16(a, 8),8);
    Vec32s  odd  = _mm512_srai_epi16(a, 8);
    Vec32s  evend = even / d;         // divide even-numbered elements
    Vec32s  oddd  = odd  / d;         // divide odd-numbered  elements
            oddd  = _mm512_slli_epi16(oddd, 8); // shift left to put back in place
    __m512i res  = _mm512_mask_mov_epi8(evend, 0xAAAAAAAAAAAAAAAA, oddd); // interleave even and odd
    return res;
}

// vector of 32 8-bit unsigned integers
static inline Vec64uc operator / (Vec64uc const a, Divisor_us const d) {
    // zero-extend even-numbered and odd-numbered elements to 16 bits
    Vec32us  even = _mm512_maskz_mov_epi8(__mmask64(0x5555555555555555), a);
    Vec32us  odd  = _mm512_srli_epi16(a, 8);
    Vec32us  evend = even / d;         // divide even-numbered elements
    Vec32us  oddd  = odd  / d;         // divide odd-numbered  elements
    oddd  = _mm512_slli_epi16(oddd, 8); // shift left to put back in place
    __m512i res  = _mm512_mask_mov_epi8(evend, 0xAAAAAAAAAAAAAAAA, oddd); // interleave even and odd
    return res;
}

// vector operator /= : divide
static inline Vec32s & operator /= (Vec32s & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec32us & operator /= (Vec32us & a, Divisor_us const d) {
    a = a / d;
    return a;

}

// vector operator /= : divide
static inline Vec64c & operator /= (Vec64c & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec64uc & operator /= (Vec64uc & a, Divisor_us const d) {
    a = a / d;
    return a;
}

/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec32s by compile-time constant 
template <int d>
static inline Vec32s divide_by_i(Vec32s const x) {
    constexpr int16_t d0 = int16_t(d);                               // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");
    if constexpr (d0 ==  1) return  x;                               // divide by  1
    if constexpr (d0 == -1) return -x;                               // divide by -1
    if constexpr (uint16_t(d0) == 0x8000u) {
        return _mm512_maskz_set1_epi16(x == Vec32s((int16_t)0x8000u), 1); // avoid overflow of abs(d). return (x == 0x80000000) ? 1 : 0;        
    }
    constexpr uint16_t d1 = d0 > 0 ? d0 : -d0;                       // compile-time abs(d0)
    if constexpr ((d1 & (d1-1)) == 0) {
        // d is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(uint32_t(d1));
        __m512i sign;
        if (k > 1) sign = _mm512_srai_epi16(x, reinterpret_cast<int>(k-1)); else sign = x;  // k copies of sign bit
        __m512i bias    = _mm512_srli_epi16(sign, 16-k);             // bias = x >= 0 ? 0 : k-1
        __m512i xpbias  = _mm512_add_epi16 (x, bias);                // x + bias
        __m512i q       = _mm512_srai_epi16(xpbias, k);              // (x + bias) >> k
        if (d0 > 0)  return q;                                       // d0 > 0: return  q
        return _mm512_sub_epi16(_mm512_setzero_si512(), q);          // d0 < 0: return -q
    }
    // general case
    constexpr int L = bit_scan_reverse_const(uint16_t(d1-1)) + 1;        // ceil(log2(d)). (d < 2 handled above)
    constexpr int16_t mult = int16_t(1 + (1u << (15+L)) / uint32_t(d1) - 0x10000);// multiplier
    constexpr int shift1 = L - 1;
    const Divisor_s div(mult, shift1, d0 > 0 ? 0 : -1);
    return x / div;
}

// define Vec32s a / const_int(d)
template <int d>
static inline Vec32s operator / (Vec32s const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec32s a / const_uint(d)
template <uint32_t d>
static inline Vec32s operator / (Vec32s const a, Const_uint_t<d>) {
    static_assert(d < 0x8000u, "Dividing signed integer by overflowing unsigned");
    return divide_by_i<int(d)>(a);                                   // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32s & operator /= (Vec32s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32s & operator /= (Vec32s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// Divide Vec32us by compile-time constant
template <uint32_t d>
static inline Vec32us divide_by_ui(Vec32us const x) {
    constexpr uint16_t d0 = uint16_t(d);                             // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");
    if constexpr (d0 == 1) return x;                                 // divide by 1
    constexpr int b = bit_scan_reverse_const(d0);                    // floor(log2(d))
    if constexpr ((d0 & (d0-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm512_srli_epi16(x, b);                             // x >> b
    }
    // general case (d > 2)
    constexpr uint16_t mult = uint16_t((uint32_t(1) << (b+16)) / d0);// multiplier = 2^(32+b) / d
    constexpr uint32_t rem = (uint32_t(1) << (b+16)) - uint32_t(d0)*mult;// remainder 2^(32+b) % d
    constexpr bool round_down = (2*rem < d0);                        // check if fraction is less than 0.5
    Vec32us x1 = x;
    if constexpr (round_down) {
        x1 = x1 + 1;                                                 // round down mult and compensate by adding 1 to x
    }
    constexpr uint16_t mult1 = round_down ? mult : mult + 1;
    const __m512i multv = _mm512_set1_epi16(mult1);                  // broadcast mult
    __m512i xm = _mm512_mulhi_epu16(x1, multv);                      // high part of 16x16->32 bit unsigned multiplication
    Vec32us q    = _mm512_srli_epi16(xm, b);                         // shift right by b
    if constexpr (round_down) {
        Vec32sb overfl = (x1 == Vec32us(_mm512_setzero_si512()));    // check for overflow of x+1
        return select(overfl, Vec32us(uint32_t(mult1 >> b)), q);     // deal with overflow (rarely needed)
    }
    else {
        return q;                                                    // no overflow possible
    }
}

// define Vec32us a / const_uint(d)
template <uint32_t d>
static inline Vec32us operator / (Vec32us const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec32us a / const_int(d)
template <int d>
static inline Vec32us operator / (Vec32us const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32us & operator /= (Vec32us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32us & operator /= (Vec32us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// define Vec64c a / const_int(d)
template <int d>
static inline Vec64c operator / (Vec64c const a, Const_int_t<d>) {
    // expand into two Vec32s
    Vec32s low  = extend_low(a)  / Const_int_t<d>();
    Vec32s high = extend_high(a) / Const_int_t<d>();
    return compress(low,high);
}

// define Vec64c a / const_uint(d)
template <uint32_t d>
static inline Vec64c operator / (Vec64c const a, Const_uint_t<d>) {
    static_assert(uint8_t(d) < 0x80u, "Dividing signed integer by overflowing unsigned");
    return a / Const_int_t<d>();                           // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec64c & operator /= (Vec64c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec64c & operator /= (Vec64c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec64uc a / const_uint(d)
template <uint32_t d>
static inline Vec64uc operator / (Vec64uc const a, Const_uint_t<d>) {
    // expand into two Vec32us
    Vec32us low  = extend_low(a)  / Const_uint_t<d>();
    Vec32us high = extend_high(a) / Const_uint_t<d>();
    return compress(low,high);
}

// define Vec64uc a / const_int(d)
template <int d>
static inline Vec64uc operator / (Vec64uc const a, Const_int_t<d>) {
    static_assert(int8_t(d) >= 0, "Dividing unsigned integer by negative is ambiguous");
    return a / Const_uint_t<d>();                          // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec64uc & operator /= (Vec64uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec64uc & operator /= (Vec64uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI512S_H
