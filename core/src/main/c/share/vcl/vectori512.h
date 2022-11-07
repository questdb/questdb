/****************************  vectori512.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 512-bit integer vector classes for 32 and 64 bit integers.
* For x86 microprocessors with AVX512F and later instruction sets.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec16i    Vector of  16  32-bit signed   integers
* Vec16ui   Vector of  16  32-bit unsigned integers
* Vec16ib   Vector of  16  Booleans for use with Vec16i and Vec16ui
* Vec8q     Vector of   8  64-bit signed   integers
* Vec8uq    Vector of   8  64-bit unsigned integers
* Vec8qb    Vector of   8  Booleans for use with Vec8q and Vec8uq
* Other 512-bit integer vectors are defined in Vectori512s.h
*
* Each vector object is represented internally in the CPU as a 512-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORI512_H
#define VECTORI512_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#ifdef VECTORI512E_H
#error Two different versions of vectori512.h included
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

// Generate a constant vector of 16 integers stored in memory.
// Can be converted to any integer vector type
template <uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7, 
uint32_t i8, uint32_t i9, uint32_t i10, uint32_t i11, uint32_t i12, uint32_t i13, uint32_t i14, uint32_t i15>
static inline __m512i constant16ui() {
    /*
    const union {
        uint32_t i[16];
        __m512i zmm;
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15}};
    return u.zmm;
    */
    return _mm512_setr_epi32(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15);
}


/*****************************************************************************
*
*          Boolean vector classes for AVX512
*
*****************************************************************************/

typedef Vec16b Vec16ib;
typedef Vec16b Vec16uib;
typedef Vec8b Vec8qb;
typedef Vec8b Vec8uqb;


/*****************************************************************************
*
*          Vector of 512 bits. Used as base class for Vec16i and Vec8q
*
*****************************************************************************/
class Vec512b {
protected:
    __m512i zmm; // Integer vector
public:
    // Default constructor:
    Vec512b() {
    }
    // Constructor to build from two Vec256b:
    Vec512b(Vec256b const a0, Vec256b const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec512b(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec512b & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec512b & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64, but there is hardly any speed advantage of load_a on modern processors
    Vec512b & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm512_storeu_si512(p, zmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(void * p) const {
        _mm512_stream_si512((__m512i *)p, zmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 64;
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64, but there is hardly any speed advantage of store_a on modern processors
    void store_a(void * p) const {
        _mm512_store_si512(p, zmm);
    }
    // Member functions to split into two Vec256b:
    Vec256b get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec256b get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static constexpr int size() {
        return 512;
    }
    static constexpr int elementtype() {
        return 1;
    }
    typedef __m512i registertype;
};

// Define operators and functions for this class

// vector operator & : bitwise and
static inline Vec512b operator & (Vec512b const a, Vec512b const b) {
    return _mm512_and_epi32(a, b);
}
static inline Vec512b operator && (Vec512b const a, Vec512b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec512b operator | (Vec512b const a, Vec512b const b) {
    return _mm512_or_epi32(a, b);
}
static inline Vec512b operator || (Vec512b const a, Vec512b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec512b operator ^ (Vec512b const a, Vec512b const b) {
    return _mm512_xor_epi32(a, b);
}

// vector operator ~ : bitwise not
static inline Vec512b operator ~ (Vec512b const a) {
    return _mm512_xor_epi32(a, _mm512_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec512b & operator &= (Vec512b & a, Vec512b const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec512b & operator |= (Vec512b & a, Vec512b const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec512b & operator ^= (Vec512b & a, Vec512b const b) {
    a = a ^ b;
    return a;
}

// function andnot: a & ~ b
static inline Vec512b andnot (Vec512b const a, Vec512b const b) {
    return _mm512_andnot_epi32(b, a);
}


/*****************************************************************************
*
*          Vector of 16 32-bit signed integers
*
*****************************************************************************/

class Vec16i: public Vec512b {
public:
    // Default constructor:
    Vec16i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16i(int i) {
        zmm = _mm512_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec16i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7,
        int32_t i8, int32_t i9, int32_t i10, int32_t i11, int32_t i12, int32_t i13, int32_t i14, int32_t i15) {
        zmm = _mm512_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to build from two Vec8i:
    Vec16i(Vec8i const a0, Vec8i const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec16i(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec16i & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec16i & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16i & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16i & load_partial(int n, void const * p) {
        zmm = _mm512_maskz_loadu_epi32(__mmask16((1u << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        _mm512_mask_storeu_epi32(p, __mmask16((1u << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16i & cutoff(int n) {
        zmm = _mm512_maskz_mov_epi32(__mmask16((1u << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16i const insert(int index, int32_t value) {
        zmm = _mm512_mask_set1_epi32(zmm, __mmask16(1u << index), value);
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(int index) const {
        __m512i x = _mm512_maskz_compress_epi32(__mmask16(1u << index), zmm);
        return _mm_cvtsi128_si32(_mm512_castsi512_si128(x));        
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8i:
    Vec8i get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec8i get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 8;
    }
};


// Define operators for Vec16i

// vector operator + : add element by element
static inline Vec16i operator + (Vec16i const a, Vec16i const b) {
    return _mm512_add_epi32(a, b);
}
// vector operator += : add
static inline Vec16i & operator += (Vec16i & a, Vec16i const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16i operator ++ (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec16i & operator ++ (Vec16i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16i operator - (Vec16i const a, Vec16i const b) {
    return _mm512_sub_epi32(a, b);
}
// vector operator - : unary minus
static inline Vec16i operator - (Vec16i const a) {
    return _mm512_sub_epi32(_mm512_setzero_epi32(), a);
}
// vector operator -= : subtract
static inline Vec16i & operator -= (Vec16i & a, Vec16i const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16i operator -- (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec16i & operator -- (Vec16i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16i operator * (Vec16i const a, Vec16i const b) {
    return _mm512_mullo_epi32(a, b);
}
// vector operator *= : multiply
static inline Vec16i & operator *= (Vec16i & a, Vec16i const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec16i operator << (Vec16i const a, int32_t b) {
    return _mm512_sll_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec16i & operator <<= (Vec16i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16i operator >> (Vec16i const a, int32_t b) {
    return _mm512_sra_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec16i & operator >>= (Vec16i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16ib operator == (Vec16i const a, Vec16i const b) {
    return _mm512_cmpeq_epi32_mask(a, b);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16ib operator != (Vec16i const a, Vec16i const b) {
    return _mm512_cmpneq_epi32_mask(a, b);
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec16ib operator > (Vec16i const a, Vec16i const b) {
    return  _mm512_cmp_epi32_mask(a, b, 6);
}

// vector operator < : returns true for elements for which a < b
static inline Vec16ib operator < (Vec16i const a, Vec16i const b) {
    return  _mm512_cmp_epi32_mask(a, b, 1);
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16ib operator >= (Vec16i const a, Vec16i const b) {
    return _mm512_cmp_epi32_mask(a, b, 5);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16ib operator <= (Vec16i const a, Vec16i const b) {
    return _mm512_cmp_epi32_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec16i operator & (Vec16i const a, Vec16i const b) {
    return _mm512_and_epi32(a, b);
}
// vector operator &= : bitwise and
static inline Vec16i & operator &= (Vec16i & a, Vec16i const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16i operator | (Vec16i const a, Vec16i const b) {
    return _mm512_or_epi32(a, b);
}
// vector operator |= : bitwise or
static inline Vec16i & operator |= (Vec16i & a, Vec16i const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16i operator ^ (Vec16i const a, Vec16i const b) {
    return _mm512_xor_epi32(a, b);
}
// vector operator ^= : bitwise xor
static inline Vec16i & operator ^= (Vec16i & a, Vec16i const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16i operator ~ (Vec16i const a) {
    return a ^ Vec16i(-1);
    // This is potentially faster, but not on any current compiler:
    //return _mm512_ternarylogic_epi32(_mm512_undefined_epi32(), _mm512_undefined_epi32(), a, 0x55);
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16i select (Vec16ib const s, Vec16i const a, Vec16i const b) {
    return _mm512_mask_mov_epi32(b, s, a);  // conditional move may be optimized better by the compiler than blend
    // return _mm512_mask_blend_epi32(s, b, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16i if_add (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return _mm512_mask_add_epi32(a, f, a, b);
}

// Conditional subtract
static inline Vec16i if_sub (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return _mm512_mask_sub_epi32(a, f, a, b);
}

// Conditional multiply
static inline Vec16i if_mul (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return _mm512_mask_mullo_epi32(a, f, a, b);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add (Vec16i const a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_epi32(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function add_saturated: add element by element, signed with saturation
// (is it faster to up-convert to 64 bit integers, and then downconvert the sum with saturation?)
static inline Vec16i add_saturated(Vec16i const a, Vec16i const b) {
    __m512i sum    = _mm512_add_epi32(a, b);               // a + b
    __m512i axb    = _mm512_xor_epi32(a, b);               // check if a and b have different sign
    __m512i axs    = _mm512_xor_epi32(a, sum);             // check if a and sum have different sign
    __m512i ovf1   = _mm512_andnot_epi32(axb,axs);         // check if sum has wrong sign
    __m512i ovf2   = _mm512_srai_epi32(ovf1,31);           // -1 if overflow
    __mmask16 ovf3 = _mm512_cmpneq_epi32_mask(ovf2, _mm512_setzero_epi32()); // same, as mask
    __m512i asign  = _mm512_srli_epi32(a,31);              // 1  if a < 0
    __m512i sat1   = _mm512_srli_epi32(ovf2,1);            // 7FFFFFFF if overflow
    __m512i sat2   = _mm512_add_epi32(sat1,asign);         // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return _mm512_mask_blend_epi32(ovf3, sum, sat2);       // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16i sub_saturated(Vec16i const a, Vec16i const b) {
    __m512i diff   = _mm512_sub_epi32(a, b);               // a + b
    __m512i axb    = _mm512_xor_si512(a, b);               // check if a and b have different sign
    __m512i axs    = _mm512_xor_si512(a, diff);            // check if a and sum have different sign
    __m512i ovf1   = _mm512_and_si512(axb,axs);            // check if sum has wrong sign
    __m512i ovf2   = _mm512_srai_epi32(ovf1,31);           // -1 if overflow
    __mmask16 ovf3 = _mm512_cmpneq_epi32_mask(ovf2, _mm512_setzero_epi32()); // same, as mask
    __m512i asign  = _mm512_srli_epi32(a,31);              // 1  if a < 0
    __m512i sat1   = _mm512_srli_epi32(ovf2,1);            // 7FFFFFFF if overflow
    __m512i sat2   = _mm512_add_epi32(sat1,asign);         // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return _mm512_mask_blend_epi32(ovf3, diff, sat2);      // sum if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec16i max(Vec16i const a, Vec16i const b) {
    return _mm512_max_epi32(a,b);
}

// function min: a < b ? a : b
static inline Vec16i min(Vec16i const a, Vec16i const b) {
    return _mm512_min_epi32(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec16i abs(Vec16i const a) {
    return _mm512_abs_epi32(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16i abs_saturated(Vec16i const a) {
    return _mm512_min_epu32(abs(a), Vec16i(0x7FFFFFFF));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16i rotate_left(Vec16i const a, int b) {
    return _mm512_rolv_epi32(a, Vec16i(b));
}


/*****************************************************************************
*
*          Vector of 16 32-bit unsigned integers
*
*****************************************************************************/

class Vec16ui : public Vec16i {
public:
    // Default constructor:
    Vec16ui() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16ui(uint32_t i) {
        zmm = _mm512_set1_epi32((int32_t)i);
    }
    // Constructor to build from all elements:
    Vec16ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7, 
        uint32_t i8, uint32_t i9, uint32_t i10, uint32_t i11, uint32_t i12, uint32_t i13, uint32_t i14, uint32_t i15) {
        zmm = _mm512_setr_epi32((int32_t)i0, (int32_t)i1, (int32_t)i2, (int32_t)i3, (int32_t)i4, (int32_t)i5, (int32_t)i6, (int32_t)i7, 
            (int32_t)i8, (int32_t)i9, (int32_t)i10, (int32_t)i11, (int32_t)i12, (int32_t)i13, (int32_t)i14, (int32_t)i15);
    }
    // Constructor to build from two Vec8ui:
    Vec16ui(Vec8ui const a0, Vec8ui const a1) {
        zmm = Vec16i(Vec8i(a0), Vec8i(a1));
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec16ui(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec16ui & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16ui & load(void const * p) {
        Vec16i::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16ui & load_a(void const * p) {
        Vec16i::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16ui const insert(int index, uint32_t value) {
        Vec16i::insert(index, (int32_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(int index) const {
        return (uint32_t)Vec16i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4ui:
    Vec8ui get_low() const {
        return Vec8ui(Vec16i::get_low());
    }
    Vec8ui get_high() const {
        return Vec8ui(Vec16i::get_high());
    }
    static constexpr int elementtype() {
        return 9;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16ui operator + (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) + Vec16i(b));
}

// vector operator - : subtract
static inline Vec16ui operator - (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) - Vec16i(b));
}

// vector operator * : multiply
static inline Vec16ui operator * (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) * Vec16i(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec16ui operator >> (Vec16ui const a, uint32_t b) {
    return _mm512_srl_epi32(a, _mm_cvtsi32_si128((int32_t)b)); 
}
static inline Vec16ui operator >> (Vec16ui const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const a, uint32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const a, int32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16ib operator < (Vec16ui const a, Vec16ui const b) {
    return _mm512_cmp_epu32_mask(a, b, 1);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16ib operator > (Vec16ui const a, Vec16ui const b) {
    return _mm512_cmp_epu32_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16ib operator >= (Vec16ui const a, Vec16ui const b) {
    return _mm512_cmp_epu32_mask(a, b, 5);
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16ib operator <= (Vec16ui const a, Vec16ui const b) {
    return _mm512_cmp_epu32_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec16ui operator & (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) & Vec16i(b));
}

// vector operator | : bitwise or
static inline Vec16ui operator | (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) | Vec16i(b));
}

// vector operator ^ : bitwise xor
static inline Vec16ui operator ^ (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) ^ Vec16i(b));
}

// vector operator ~ : bitwise not
static inline Vec16ui operator ~ (Vec16ui const a) {
    return Vec16ui( ~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16ui select (Vec16ib const s, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(select(s, Vec16i(a), Vec16i(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16ui if_add (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_add(f, Vec16i(a), Vec16i(b)));
}

// Conditional subtract
static inline Vec16ui if_sub (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_sub(f, Vec16i(a), Vec16i(b)));
}

// Conditional multiply
static inline Vec16ui if_mul (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_mul(f, Vec16i(a), Vec16i(b)));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add (Vec16ui const a) {
    return (uint32_t)horizontal_add((Vec16i)a);
}

// horizontal_add_x: Horizontal add extended: Calculates the sum of all vector elements. Defined later in this file

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16ui add_saturated(Vec16ui const a, Vec16ui const b) {
    Vec16ui sum      = a + b;
    Vec16ib overflow = sum < (a | b);                      // overflow if (a + b) < (a | b)
    return _mm512_mask_set1_epi32(sum, overflow, -1);      // 0xFFFFFFFF if overflow
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16ui sub_saturated(Vec16ui const a, Vec16ui const b) {
    Vec16ui diff      = a - b;
    return _mm512_maskz_mov_epi32(diff <= a, diff);        // underflow if diff > a gives zero
}

// function max: a > b ? a : b
static inline Vec16ui max(Vec16ui const a, Vec16ui const b) {
    return _mm512_max_epu32(a,b);
}

// function min: a < b ? a : b
static inline Vec16ui min(Vec16ui const a, Vec16ui const b) {
    return _mm512_min_epu32(a,b);
}


/*****************************************************************************
*
*          Vector of 8 64-bit signed integers
*
*****************************************************************************/

class Vec8q : public Vec512b {
public:
    // Default constructor:
    Vec8q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8q(int64_t i) {
        zmm = _mm512_set1_epi64(i);
    }
    // Constructor to build from all elements:
    Vec8q(int64_t i0, int64_t i1, int64_t i2, int64_t i3, int64_t i4, int64_t i5, int64_t i6, int64_t i7) {
        zmm = _mm512_setr_epi64(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4q:
    Vec8q(Vec4q const a0, Vec4q const a1) {
        zmm = _mm512_inserti64x4(_mm512_castsi256_si512(a0), a1, 1);
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec8q(__m512i const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec8q & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec8q & load(void const * p) {
        zmm = _mm512_loadu_si512(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec8q & load_a(void const * p) {
        zmm = _mm512_load_si512(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8q & load_partial(int n, void const * p) {
        zmm = _mm512_maskz_loadu_epi64(__mmask16((1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        _mm512_mask_storeu_epi64(p, __mmask16((1 << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8q & cutoff(int n) {
        zmm = _mm512_maskz_mov_epi64(__mmask16((1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8q const insert(int index, int64_t value) {
#ifdef __x86_64__
        zmm = _mm512_mask_set1_epi64(zmm, __mmask16(1 << index), value);
#else
        __m512i v = Vec8q(value);
        zmm = _mm512_mask_mov_epi64(zmm, __mmask16(1 << index), v);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(int index) const {
        __m512i x = _mm512_maskz_compress_epi64(__mmask8(1u << index), zmm);
        return _emulate_movq(_mm512_castsi512_si128(x));        
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec4q get_low() const {
        return _mm512_castsi512_si256(zmm);
    }
    Vec4q get_high() const {
        return _mm512_extracti64x4_epi64(zmm,1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 10;
    }
};


// Define operators for Vec8q

// vector operator + : add element by element
static inline Vec8q operator + (Vec8q const a, Vec8q const b) {
    return _mm512_add_epi64(a, b);
}
// vector operator += : add
static inline Vec8q & operator += (Vec8q & a, Vec8q const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8q operator ++ (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec8q & operator ++ (Vec8q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8q operator - (Vec8q const a, Vec8q const b) {
    return _mm512_sub_epi64(a, b);
}
// vector operator - : unary minus
static inline Vec8q operator - (Vec8q const a) {
    return _mm512_sub_epi64(_mm512_setzero_epi32(), a);
}
// vector operator -= : subtract
static inline Vec8q & operator -= (Vec8q & a, Vec8q const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8q operator -- (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec8q & operator -- (Vec8q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8q operator * (Vec8q const a, Vec8q const b) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_mullo_epi64(a, b);
#elif defined (__INTEL_COMPILER)
    return _mm512_mullox_epi64(a, b);                      // _mm512_mullox_epi64 missing in gcc
#else
    // instruction does not exist. Split into 32-bit multiplications
    //__m512i ahigh = _mm512_shuffle_epi32(a, 0xB1);       // swap H<->L
    __m512i ahigh   = _mm512_srli_epi64(a, 32);            // high 32 bits of each a
    __m512i bhigh   = _mm512_srli_epi64(b, 32);            // high 32 bits of each b
    __m512i prodahb = _mm512_mul_epu32(ahigh, b);          // ahigh*b
    __m512i prodbha = _mm512_mul_epu32(bhigh, a);          // bhigh*a
    __m512i prodhl  = _mm512_add_epi64(prodahb, prodbha);  // sum of high*low products
    __m512i prodhi  = _mm512_slli_epi64(prodhl, 32);       // same, shifted high
    __m512i prodll  = _mm512_mul_epu32(a, b);              // alow*blow = 64 bit unsigned products
    __m512i prod    = _mm512_add_epi64(prodll, prodhi);    // low*low+(high*low)<<32
    return  prod;
#endif
}

// vector operator *= : multiply
static inline Vec8q & operator *= (Vec8q & a, Vec8q const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec8q operator << (Vec8q const a, int32_t b) {
    return _mm512_sll_epi64(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec8q & operator <<= (Vec8q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8q operator >> (Vec8q const a, int32_t b) {
    return _mm512_sra_epi64(a, _mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec8q & operator >>= (Vec8q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8qb operator == (Vec8q const a, Vec8q const b) {
    return Vec8qb(_mm512_cmpeq_epi64_mask(a, b));
}

// vector operator != : returns true for elements for which a != b
static inline Vec8qb operator != (Vec8q const a, Vec8q const b) {
    return Vec8qb(_mm512_cmpneq_epi64_mask(a, b));
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec8qb operator < (Vec8q const a, Vec8q const b) {
    return _mm512_cmp_epi64_mask(a, b, 1);
}

// vector operator > : returns true for elements for which a > b
static inline Vec8qb operator > (Vec8q const a, Vec8q const b) {
    return _mm512_cmp_epi64_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8qb operator >= (Vec8q const a, Vec8q const b) {
    return _mm512_cmp_epi64_mask(a, b, 5);
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8qb operator <= (Vec8q const a, Vec8q const b) {
    return _mm512_cmp_epi64_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec8q operator & (Vec8q const a, Vec8q const b) {
    return _mm512_and_epi32(a, b);
}
// vector operator &= : bitwise and
static inline Vec8q & operator &= (Vec8q & a, Vec8q const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8q operator | (Vec8q const a, Vec8q const b) {
    return _mm512_or_epi32(a, b);
}
// vector operator |= : bitwise or
static inline Vec8q & operator |= (Vec8q & a, Vec8q const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8q operator ^ (Vec8q const a, Vec8q const b) {
    return _mm512_xor_epi32(a, b);
}
// vector operator ^= : bitwise xor
static inline Vec8q & operator ^= (Vec8q & a, Vec8q const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8q operator ~ (Vec8q const a) {
    return Vec8q(~ Vec16i(a));
    //return _mm512_ternarylogic_epi64(_mm512_undefined_epi32(), _mm512_undefined_epi32(), a, 0x55);
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8q select (Vec8qb const s, Vec8q const a, Vec8q const b) {
    // avoid warning in MS compiler if INSTRSET = 9 by casting mask to uint8_t, while __mmask8 is not supported in AVX512F
    return _mm512_mask_mov_epi64(b, (uint8_t)s, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8q if_add (Vec8qb const f, Vec8q const a, Vec8q const b) {
    return _mm512_mask_add_epi64(a, (uint8_t)f, a, b);
}

// Conditional subtract
static inline Vec8q if_sub (Vec8qb const f, Vec8q const a, Vec8q const b) {
    return _mm512_mask_sub_epi64(a, (uint8_t)f, a, b);
}

// Conditional multiply
static inline Vec8q if_mul (Vec8qb const f, Vec8q const a, Vec8q const b) {
#if INSTRSET >= 10
    return _mm512_mask_mullo_epi64(a, f, a, b);  // AVX512DQ
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int64_t horizontal_add (Vec8q const a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_epi64(a);
#else
    return horizontal_add(a.get_low()+a.get_high());
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec16i const x) {
    Vec8q a = _mm512_cvtepi32_epi64(x.get_low());
    Vec8q b = _mm512_cvtepi32_epi64(x.get_high());
    return horizontal_add(a+b);
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec16ui const x) {
    Vec8q a = _mm512_cvtepu32_epi64(x.get_low());
    Vec8q b = _mm512_cvtepu32_epi64(x.get_high());
    return (uint64_t)horizontal_add(a+b);
}

// function max: a > b ? a : b
static inline Vec8q max(Vec8q const a, Vec8q const b) {
    return _mm512_max_epi64(a, b);
}

// function min: a < b ? a : b
static inline Vec8q min(Vec8q const a, Vec8q const b) {
    return _mm512_min_epi64(a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8q abs(Vec8q const a) {
    return _mm512_abs_epi64(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8q abs_saturated(Vec8q const a) {
    return _mm512_min_epu64(abs(a), Vec8q(0x7FFFFFFFFFFFFFFF));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8q rotate_left(Vec8q const a, int b) {
    return _mm512_rolv_epi64(a, Vec8q(b));
}


/*****************************************************************************
*
*          Vector of 8 64-bit unsigned integers
*
*****************************************************************************/

class Vec8uq : public Vec8q {
public:
    // Default constructor:
    Vec8uq() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8uq(uint64_t i) {
        zmm = Vec8q((int64_t)i);
    }
    // Constructor to convert from Vec8q:
    Vec8uq(Vec8q const x) {
        zmm = x;
    }
    // Constructor to convert from type __m512i used in intrinsics:
    Vec8uq(__m512i const x) {
        zmm = x;
    }
    // Constructor to build from all elements:
    Vec8uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3, uint64_t i4, uint64_t i5, uint64_t i6, uint64_t i7) {
        zmm = Vec8q((int64_t)i0, (int64_t)i1, (int64_t)i2, (int64_t)i3, (int64_t)i4, (int64_t)i5, (int64_t)i6, (int64_t)i7);
    }
    // Constructor to build from two Vec4uq:
    Vec8uq(Vec4uq const a0, Vec4uq const a1) {
        zmm = Vec8q(Vec4q(a0), Vec4q(a1));
    }
    // Assignment operator to convert from Vec8q:
    Vec8uq  & operator = (Vec8q const x) {
        zmm = x;
        return *this;
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec8uq & operator = (__m512i const x) {
        zmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8uq & load(void const * p) {
        Vec8q::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8uq & load_a(void const * p) {
        Vec8q::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8uq const insert(int index, uint64_t value) {
        Vec8q::insert(index, (int64_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(int index) const {
        return (uint64_t)Vec8q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2uq:
    Vec4uq get_low() const {
        return Vec4uq(Vec8q::get_low());
    }
    Vec4uq get_high() const {
        return Vec4uq(Vec8q::get_high());
    }
    static constexpr int elementtype() {
        return 10;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8uq operator + (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) + Vec8q(b));
}

// vector operator - : subtract
static inline Vec8uq operator - (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) - Vec8q(b));
}

// vector operator * : multiply element by element
static inline Vec8uq operator * (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) * Vec8q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec8uq operator >> (Vec8uq const a, uint32_t b) {
    return _mm512_srl_epi64(a,_mm_cvtsi32_si128((int32_t)b)); 
}
static inline Vec8uq operator >> (Vec8uq const a, int32_t b) {
    return a >> (uint32_t)b;
}
// vector operator >>= : shift right arithmetic
static inline Vec8uq & operator >>= (Vec8uq & a, uint32_t b) {
    a = a >> b;
    return a;
}
// vector operator >>= : shift right logical
static inline Vec8uq & operator >>= (Vec8uq & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const a, uint32_t b) {
    return Vec8uq ((Vec8q)a << (int32_t)b);
}
// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const a, int32_t b) {
    return Vec8uq ((Vec8q)a << b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8qb operator < (Vec8uq const a, Vec8uq const b) {
    return _mm512_cmp_epu64_mask(a, b, 1);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8qb operator > (Vec8uq const a, Vec8uq const b) {
    return _mm512_cmp_epu64_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8qb operator >= (Vec8uq const a, Vec8uq const b) {
    return _mm512_cmp_epu64_mask(a, b, 5);
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8qb operator <= (Vec8uq const a, Vec8uq const b) {
    return _mm512_cmp_epu64_mask(a, b, 2);
}

// vector operator & : bitwise and
static inline Vec8uq operator & (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) & Vec8q(b));
}

// vector operator | : bitwise or
static inline Vec8uq operator | (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) | Vec8q(b));
}

// vector operator ^ : bitwise xor
static inline Vec8uq operator ^ (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) ^ Vec8q(b));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8uq select (Vec8qb const s, Vec8uq const a, Vec8uq const b) {
    return Vec8uq(select(s, Vec8q(a), Vec8q(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8uq if_add (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
    return _mm512_mask_add_epi64(a, (uint8_t)f, a, b);
}

// Conditional subtract
static inline Vec8uq if_sub (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
    return _mm512_mask_sub_epi64(a, (uint8_t)f, a, b);
}

// Conditional multiply
static inline Vec8uq if_mul (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
#if INSTRSET >= 10
    return _mm512_mask_mullo_epi64(a, f, a, b);  // AVX512DQ
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint64_t horizontal_add (Vec8uq const a) {
    return (uint64_t)horizontal_add(Vec8q(a));
}

// function max: a > b ? a : b
static inline Vec8uq max(Vec8uq const a, Vec8uq const b) {
    return _mm512_max_epu64(a, b);
}

// function min: a < b ? a : b
static inline Vec8uq min(Vec8uq const a, Vec8uq const b) {
    return _mm512_min_epu64(a, b);
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

// Permute vector of 8 64-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q permute8(Vec8q const a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m512i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec8q>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // permutation pattern
            constexpr uint8_t  ppat = (L.a[0] & 3) | (L.a[1]<<2 & 0xC) | (L.a[2]<<4 & 0x30) | (L.a[3]<<6 & 0xC0);
            y = _mm512_shuffle_i64x2(a, a, ppat);
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in all lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm512_unpackhi_epi64(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm512_unpacklo_epi64(y, y);
            }
            else { // general permute
                y = _mm512_shuffle_epi32(a, (_MM_PERM_ENUM)uint8_t(flags >> perm_ipattern));
            }
        }
        else {  // different patterns in all lanes
            if constexpr ((flags & perm_rotate_big) != 0) {// fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                y = _mm512_alignr_epi64 (y, y, rot);
            } 
            else if constexpr ((flags & perm_broadcast) != 0) { // broadcast one element
                constexpr int e = flags >> perm_rot_count;
                if constexpr(e != 0) {
                    y = _mm512_alignr_epi64(y, y, e);
                }
                y = _mm512_broadcastq_epi64(_mm512_castsi512_si128(y));
            }
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_epi64(__mmask8(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_epi64(__mmask8(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_cross_lane) == 0) {  // no lane crossing. Use pshufb
                const EList <int8_t, 64> bm = pshufb_mask<Vec8q>(indexs);
                return _mm512_shuffle_epi8(y, Vec8q().load(bm.a));
            } 
            else {
                // full permute needed
                const __m512i pmask = constant16ui <
                    i0 & 7, 0, i1 & 7, 0, i2 & 7, 0, i3 & 7, 0, i4 & 7, 0, i5 & 7, 0, i6 & 7, 0, i7 & 7, 0>();
                y = _mm512_permutexvar_epi64(pmask, y);
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {           // additional zeroing needed
        y = _mm512_maskz_mov_epi64(zero_mask<8>(indexs), y);
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8uq permute8(Vec8uq const a) {
    return Vec8uq (permute8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8q(a)));
}


// Permute vector of 16 32-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i permute16(Vec16i const a) {
    int constexpr indexs[16] = {  // indexes as array
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };    
    __m512i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec16i>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // permutation pattern
            y = permute8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> (Vec8q(a));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in all lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm512_unpackhi_epi32(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm512_unpacklo_epi32(y, y);
            }
            else { // general permute
                y = _mm512_shuffle_epi32(a, (_MM_PERM_ENUM)uint8_t(flags >> perm_ipattern));
            }
        }
        else {  // different patterns in all lanes
            if constexpr ((flags & perm_rotate_big) != 0) {// fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                return _mm512_maskz_alignr_epi32 (zero_mask<16>(indexs), y, y, rot);
            }
            else if constexpr ((flags & perm_broadcast) != 0) { // broadcast one element
                constexpr int e = flags >> perm_rot_count; // element index
                if constexpr(e != 0) {
                    y = _mm512_alignr_epi32(y, y, e);
                }
                y = _mm512_broadcastd_epi32(_mm512_castsi512_si128(y));
            }
            else if constexpr ((flags & perm_zext) != 0) {        
                y = _mm512_cvtepu32_epi64(_mm512_castsi512_si256(y)); // zero extension
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_epi32(__mmask16(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_epi32(__mmask16(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_cross_lane) == 0) { // no lane crossing. Use pshufb
                const EList <int8_t, 64> bm = pshufb_mask<Vec16i>(indexs);
                return _mm512_shuffle_epi8(a, Vec16i().load(bm.a));
            }
            else {
                // full permute needed
                const __m512i pmask = constant16ui <
                    i0 & 15, i1 & 15, i2 & 15, i3 & 15, i4 & 15, i5 & 15, i6 & 15, i7 & 15,
                    i8 & 15, i9 & 15, i10 & 15, i11 & 15, i12 & 15, i13 & 15, i14 & 15, i15 & 15>();
                return _mm512_maskz_permutexvar_epi32(zero_mask<16>(indexs), pmask, a);
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {           // additional zeroing needed
        y = _mm512_maskz_mov_epi32(zero_mask<16>(indexs), y);
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16ui permute16(Vec16ui const a) {
    return Vec16ui (permute16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16i(a)));
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

// permute and blend Vec8q
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8q blend8(Vec8q const a, Vec8q const b) { 
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m512i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec8q>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_si512(); // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute8 <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        constexpr EList<int, 16> L = blend_perm_indexes<8, 2>(indexs); // get permutation indexes
        return permute8 < L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] > (b);
    } 
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint8_t mb = (uint8_t)make_bit_mask<8, 0x303>(indexs);  // blend mask
        y = _mm512_mask_mov_epi64 (a, mb, b);
    }
    else if constexpr ((flags & blend_rotate_big) != 0) {  // full rotate
        constexpr uint8_t rot = uint8_t(flags >> blend_rotpattern); // rotate count
        if constexpr (rot < 8) {
            y = _mm512_alignr_epi64(b, a, rot);
        }
        else {
            y = _mm512_alignr_epi64(a, b, rot & 7);
        }
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 128-bit blocks
        constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // get 128-bit blend pattern
        constexpr uint8_t shuf = (L.a[0] & 3) | (L.a[1] & 3) << 2 | (L.a[2] & 3) << 4 | (L.a[3] & 3) << 6;
        if constexpr (make_bit_mask<8, 0x103>(indexs) == 0) {  // fits vshufi64x2 (a,b)
            y = _mm512_shuffle_i64x2(a, b, shuf);
        }
        else if constexpr (make_bit_mask<8, 0x203>(indexs) == 0) { // fits vshufi64x2 (b,a)
            y = _mm512_shuffle_i64x2(b, a, shuf);
        }
        else {
            const EList <int64_t, 8> bm = perm_mask_broad<Vec8q>(indexs);   // full permute
            y = _mm512_permutex2var_epi64(a, Vec8q().load(bm.a), b);
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm512_unpacklo_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm512_unpacklo_epi64 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm512_unpackhi_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm512_unpackhi_epi64 (b, a);
    }
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm512_castpd_si512(_mm512_shuffle_pd(_mm512_castsi512_pd(a), _mm512_castsi512_pd(b), uint8_t(flags >> blend_shufpattern)));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm512_castpd_si512(_mm512_shuffle_pd(_mm512_castsi512_pd(b), _mm512_castsi512_pd(a), uint8_t(flags >> blend_shufpattern)));
    }
#else
    // we might use 2 x _mm512_mask(z)_shuffle_epi32 like in blend16 below
#endif
    else { // No special cases
        const EList <int64_t, 8> bm = perm_mask_broad<Vec8q>(indexs);   // full permute
        y = _mm512_permutex2var_epi64(a, Vec8q().load(bm.a), b);
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_epi64(zero_mask<8>(indexs), y);
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8uq blend8(Vec8uq const a, Vec8uq const b) {
    return Vec8uq( blend8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8q(a),Vec8q(b)));
}


// permute and blend Vec16i
template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16i blend16(Vec16i const a, Vec16i const b) {  
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15}; // indexes as array
    __m512i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec16i>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_si512();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute16 <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        constexpr EList<int, 32> L = blend_perm_indexes<16, 2>(indexs); // get permutation indexes
        return permute16 < 
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31] > (b);
    } 
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint16_t mb = (uint16_t)make_bit_mask<16, 0x304>(indexs);  // blend mask
        y = _mm512_mask_mov_epi32 (a, mb, b);
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 64-bit blocks
        constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // get 64-bit blend pattern
        y = blend8<L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7] >
            (Vec8q(a), Vec8q(b));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing
    }
    else if constexpr ((flags & blend_same_pattern) != 0) { 
        // same pattern in all 128-bit lanes. check if pattern fits special cases
        if constexpr ((flags & blend_punpcklab) != 0) {
            y = _mm512_unpacklo_epi32(a, b);
        }
        else if constexpr ((flags & blend_punpcklba) != 0) {
            y = _mm512_unpacklo_epi32(b, a);
        }
        else if constexpr ((flags & blend_punpckhab) != 0) {
            y = _mm512_unpackhi_epi32(a, b);
        }
        else if constexpr ((flags & blend_punpckhba) != 0) {
            y = _mm512_unpackhi_epi32(b, a);
        }
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
        else if constexpr ((flags & blend_shufab) != 0) {  // use floating point instruction shufpd
            y = _mm512_castps_si512(_mm512_shuffle_ps(_mm512_castsi512_ps(a), _mm512_castsi512_ps(b), uint8_t(flags >> blend_shufpattern)));
        }
        else if constexpr ((flags & blend_shufba) != 0) {  // use floating point instruction shufpd
            y = _mm512_castps_si512(_mm512_shuffle_ps(_mm512_castsi512_ps(b), _mm512_castsi512_ps(a), uint8_t(flags >> blend_shufpattern)));
        }
#endif
        else {
            // Use vpshufd twice. This generates two instructions in the dependency chain, 
            // but we are avoiding the slower lane-crossing instruction, and saving 64 
            // bytes of data cache.
            auto shuf = [](int const (&a)[16]) constexpr { // get pattern for vpshufd
                int pat[4] = {-1,-1,-1,-1}; 
                for (int i = 0; i < 16; i++) {
                    int ix = a[i];
                    if (ix >= 0 && pat[i&3] < 0) {
                        pat[i&3] = ix;
                    }
                }
                return (pat[0] & 3) | (pat[1] & 3) << 2 | (pat[2] & 3) << 4 | (pat[3] & 3) << 6;
            };
            constexpr uint8_t  pattern = uint8_t(shuf(indexs));                    // permute pattern
            constexpr uint16_t froma = (uint16_t)make_bit_mask<16, 0x004>(indexs); // elements from a
            constexpr uint16_t fromb = (uint16_t)make_bit_mask<16, 0x304>(indexs); // elements from b
            y = _mm512_maskz_shuffle_epi32(   froma, a, (_MM_PERM_ENUM) pattern);
            y = _mm512_mask_shuffle_epi32 (y, fromb, b, (_MM_PERM_ENUM) pattern);
            return y;  // we have already zeroed any unused elements
        }
    }
    else if constexpr ((flags & blend_rotate_big) != 0) {  // full rotate
        constexpr uint8_t rot = uint8_t(flags >> blend_rotpattern); // rotate count
        if constexpr (rot < 16) {
            y = _mm512_alignr_epi32(b, a, rot);
        }
        else {
            y = _mm512_alignr_epi32(a, b, rot & 0x0F);
        }
    }

    else { // No special cases
        const EList <int32_t, 16> bm = perm_mask_broad<Vec16i>(indexs);   // full permute
        y = _mm512_permutex2var_epi32(a, Vec16i().load(bm.a), b);
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_epi32(zero_mask<16>(indexs), y);
    }
    return y;
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16ui blend16(Vec16ui const a, Vec16ui const b) {
    return Vec16ui( blend16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16i(a),Vec16i(b)));
}


/*****************************************************************************
*
*          Vector lookup functions
*
******************************************************************************
*
* These functions use vector elements as indexes into a table.
* The table is given as one or more vectors or as an array.
*
*****************************************************************************/

static inline Vec16i lookup16(Vec16i const index, Vec16i const table) {
    return _mm512_permutexvar_epi32(index, table);
}

static inline Vec16i lookup32(Vec16i const index, Vec16i const table1, Vec16i const table2) {
    return _mm512_permutex2var_epi32(table1, index, table2);
}

static inline Vec16i lookup64(Vec16i const index, Vec16i const table1, Vec16i const table2, Vec16i const table3, Vec16i const table4) {
    Vec16i d12 = _mm512_permutex2var_epi32(table1, index, table2);
    Vec16i d34 = _mm512_permutex2var_epi32(table3, index, table4);
    return select((index >> 5) != 0, d34, d12);
}

template <int n>
static inline Vec16i lookup(Vec16i const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 16) {
        Vec16i table1 = Vec16i().load(table);
        return lookup16(index, table1);
    }
    if (n <= 32) {
        Vec16i table1 = Vec16i().load(table);
        Vec16i table2 = Vec16i().load((int8_t*)table + 64);
        return _mm512_permutex2var_epi32(table1, index, table2);
    }
    // n > 32. Limit index
    Vec16ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16ui(index), uint32_t(n-1));
    }
    return _mm512_i32gather_epi32(index1, (const int*)table, 4);
    // return  _mm512_i32gather_epi32(index1, table, _MM_UPCONV_EPI32_NONE, 4, 0);
}


static inline Vec8q lookup8(Vec8q const index, Vec8q const table) {
    return _mm512_permutexvar_epi64(index, table);
}

template <int n>
static inline Vec8q lookup(Vec8q const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8q table1 = Vec8q().load(table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8q table1 = Vec8q().load(table);
        Vec8q table2 = Vec8q().load((int8_t*)table + 64);
        return _mm512_permutex2var_epi64(table1, index, table2);
    }
    // n > 16. Limit index
    Vec8uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8uq(index), uint32_t(n-1));
    }
    return _mm512_i64gather_epi64(index1, (const long long*)table, 8);
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i gather16i(void const * a) {
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 15) {
        // load one contiguous block and permute
        if constexpr (imax > 15) {
            // make sure we don't read past the end of the array
            Vec16i b = Vec16i().load((int32_t const *)a + imax-15);
            return permute16<i0-imax+15, i1-imax+15, i2-imax+15, i3-imax+15, i4-imax+15, i5-imax+15, i6-imax+15, i7-imax+15,
                i8-imax+15, i9-imax+15, i10-imax+15, i11-imax+15, i12-imax+15, i13-imax+15, i14-imax+15, i15-imax+15> (b);
        }
        else {
            Vec16i b = Vec16i().load((int32_t const *)a + imin);
            return permute16<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin,
                i8-imin, i9-imin, i10-imin, i11-imin, i12-imin, i13-imin, i14-imin, i15-imin> (b);
        }
    }
    if constexpr ((i0<imin+16  || i0>imax-16)  && (i1<imin+16  || i1>imax-16)  && (i2<imin+16  || i2>imax-16)  && (i3<imin+16  || i3>imax-16)
    &&  (i4<imin+16  || i4>imax-16)  && (i5<imin+16  || i5>imax-16)  && (i6<imin+16  || i6>imax-16)  && (i7<imin+16  || i7>imax-16)    
    &&  (i8<imin+16  || i8>imax-16)  && (i9<imin+16  || i9>imax-16)  && (i10<imin+16 || i10>imax-16) && (i11<imin+16 || i11>imax-16)
    &&  (i12<imin+16 || i12>imax-16) && (i13<imin+16 || i13>imax-16) && (i14<imin+16 || i14>imax-16) && (i15<imin+16 || i15>imax-16) ) {
        // load two contiguous blocks and blend
        Vec16i b = Vec16i().load((int32_t const *)a + imin);
        Vec16i c = Vec16i().load((int32_t const *)a + imax-15);
        const int j0  = i0 <imin+16 ? i0 -imin : 31-imax+i0;
        const int j1  = i1 <imin+16 ? i1 -imin : 31-imax+i1;
        const int j2  = i2 <imin+16 ? i2 -imin : 31-imax+i2;
        const int j3  = i3 <imin+16 ? i3 -imin : 31-imax+i3;
        const int j4  = i4 <imin+16 ? i4 -imin : 31-imax+i4;
        const int j5  = i5 <imin+16 ? i5 -imin : 31-imax+i5;
        const int j6  = i6 <imin+16 ? i6 -imin : 31-imax+i6;
        const int j7  = i7 <imin+16 ? i7 -imin : 31-imax+i7;
        const int j8  = i8 <imin+16 ? i8 -imin : 31-imax+i8;
        const int j9  = i9 <imin+16 ? i9 -imin : 31-imax+i9;
        const int j10 = i10<imin+16 ? i10-imin : 31-imax+i10;
        const int j11 = i11<imin+16 ? i11-imin : 31-imax+i11;
        const int j12 = i12<imin+16 ? i12-imin : 31-imax+i12;
        const int j13 = i13<imin+16 ? i13-imin : 31-imax+i13;
        const int j14 = i14<imin+16 ? i14-imin : 31-imax+i14;
        const int j15 = i15<imin+16 ? i15-imin : 31-imax+i15;
        return blend16<j0,j1,j2,j3,j4,j5,j6,j7,j8,j9,j10,j11,j12,j13,j14,j15>(b, c);
    }
    // use gather instruction
    return _mm512_i32gather_epi32(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const int *)a, 4);
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q gather8q(void const * a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 7) {
        // load one contiguous block and permute
        if constexpr (imax > 7) {
            // make sure we don't read past the end of the array
            Vec8q b = Vec8q().load((int64_t const *)a + imax-7);
            return permute8<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7> (b);
        }
        else {
            Vec8q b = Vec8q().load((int64_t const *)a + imin);
            return permute8<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin> (b);
        }
    }
    if constexpr ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8q b = Vec8q().load((int64_t const *)a + imin);
        Vec8q c = Vec8q().load((int64_t const *)a + imax-7);
        const int j0 = i0<imin+8 ? i0-imin : 15-imax+i0;
        const int j1 = i1<imin+8 ? i1-imin : 15-imax+i1;
        const int j2 = i2<imin+8 ? i2-imin : 15-imax+i2;
        const int j3 = i3<imin+8 ? i3-imin : 15-imax+i3;
        const int j4 = i4<imin+8 ? i4-imin : 15-imax+i4;
        const int j5 = i5<imin+8 ? i5-imin : 15-imax+i5;
        const int j6 = i6<imin+8 ? i6-imin : 15-imax+i6;
        const int j7 = i7<imin+8 ? i7-imin : 15-imax+i7;
        return blend8<j0, j1, j2, j3, j4, j5, j6, j7>(b, c);
    }
    // use gather instruction
    return _mm512_i64gather_epi64(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const long long *)a, 8);
}

/*****************************************************************************
*
*          Vector scatter functions
*
******************************************************************************
*
* These functions write the elements of a vector to arbitrary positions in an
* array in memory. Each vector element is written to an array position 
* determined by an index. An element is not written if the corresponding
* index is out of range.
* The indexes can be specified as constant template parameters or as an
* integer vector.
* 
*****************************************************************************/

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
    static inline void scatter(Vec16i const data, void * array) {
    __m512i indx = constant16ui<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15>();
    Vec16ib mask(i0>=0, i1>=0, i2>=0, i3>=0, i4>=0, i5>=0, i6>=0, i7>=0,
        i8>=0, i9>=0, i10>=0, i11>=0, i12>=0, i13>=0, i14>=0, i15>=0);
    _mm512_mask_i32scatter_epi32((int*)array, mask, indx, data, 4);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline void scatter(Vec8q const data, void * array) {
    __m256i indx = constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>();
    Vec8qb mask(i0>=0, i1>=0, i2>=0, i3>=0, i4>=0, i5>=0, i6>=0, i7>=0);
    _mm512_mask_i32scatter_epi64((long long *)array, mask, indx, data, 8);
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec16i const index, uint32_t limit, Vec16i const data, void * destination) {
    Vec16ib mask = Vec16ui(index) < limit;
    _mm512_mask_i32scatter_epi32((int*)destination, mask, index, data, 4);
}

static inline void scatter(Vec8q const index, uint32_t limit, Vec8q const data, void * destination) {
    Vec8qb mask = Vec8uq(index) < uint64_t(limit);
    _mm512_mask_i64scatter_epi64((long long *)destination, (uint8_t)mask, index, data, 8);
}

static inline void scatter(Vec8i const index, uint32_t limit, Vec8q const data, void * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask16 mask = _mm256_cmplt_epu32_mask(index, Vec8ui(limit));
#else
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xFFu, _mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec8ui(limit)));
#endif
    _mm512_mask_i32scatter_epi64((long long *)destination, (uint8_t)mask, index, data, 8);
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 64 bits with sign extension
static inline Vec8q extend_low (Vec16i const a) {
    return _mm512_cvtepi32_epi64(a.get_low());
}

// Function extend_high : extends the high 8 elements to 64 bits with sign extension
static inline Vec8q extend_high (Vec16i const a) {
    return _mm512_cvtepi32_epi64(a.get_high());
}

// Function extend_low : extends the low 8 elements to 64 bits with zero extension
static inline Vec8uq extend_low (Vec16ui const a) {
    return _mm512_cvtepu32_epi64(a.get_low());
}

// Function extend_high : extends the high 8 elements to 64 bits with zero extension
static inline Vec8uq extend_high (Vec16ui const a) {
    return _mm512_cvtepu32_epi64(a.get_high());
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec16i compress (Vec8q const low, Vec8q const high) {
    Vec8i low2   = _mm512_cvtepi64_epi32(low);
    Vec8i high2  = _mm512_cvtepi64_epi32(high);
    return Vec16i(low2, high2);
}
static inline Vec16ui compress (Vec8uq const low, Vec8uq const high) {
    return Vec16ui(compress(Vec8q(low), Vec8q(high)));
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec16i compress_saturated (Vec8q const low, Vec8q const high) {
    Vec8i low2   = _mm512_cvtsepi64_epi32(low);
    Vec8i high2  = _mm512_cvtsepi64_epi32(high);
    return Vec16i(low2, high2);
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec16ui compress_saturated (Vec8uq const low, Vec8uq const high) {
    Vec8ui low2   = _mm512_cvtusepi64_epi32(low);
    Vec8ui high2  = _mm512_cvtusepi64_epi32(high);
    return Vec16ui(low2, high2);
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 16 32-bit signed integers
static inline Vec16i operator / (Vec16i const a, Divisor_i const d) {
    __m512i m   = _mm512_broadcast_i32x4(d.getm());        // broadcast multiplier
    __m512i sgn = _mm512_broadcast_i32x4(d.getsign());     // broadcast sign of d
    __m512i t1  = _mm512_mul_epi32(a,m);                   // 32x32->64 bit signed multiplication of even elements of a
    __m512i t3  = _mm512_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m512i t4  = _mm512_mul_epi32(t3,m);                  // 32x32->64 bit signed multiplication of odd elements
    __m512i t2  = _mm512_srli_epi64(t1,32);                // dword of even index results
    __m512i t7  = _mm512_mask_mov_epi32(t2, 0xAAAA, t4);   // blend two results
    __m512i t8  = _mm512_add_epi32(t7,a);                  // add
    __m512i t9  = _mm512_sra_epi32(t8,d.gets1());          // shift right arithmetic
    __m512i t10 = _mm512_srai_epi32(a,31);                 // sign of a
    __m512i t11 = _mm512_sub_epi32(t10,sgn);               // sign of a - sign of d
    __m512i t12 = _mm512_sub_epi32(t9,t11);                // + 1 if a < 0, -1 if d < 0
    return        _mm512_xor_si512(t12,sgn);               // change sign if divisor negative
}

// vector of 16 32-bit unsigned integers
static inline Vec16ui operator / (Vec16ui const a, Divisor_ui const d) {
    __m512i m   = _mm512_broadcast_i32x4(d.getm());        // broadcast multiplier
    __m512i t1  = _mm512_mul_epu32(a,m);                   // 32x32->64 bit unsigned multiplication of even elements of a
    __m512i t3  = _mm512_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m512i t4  = _mm512_mul_epu32(t3,m);                  // 32x32->64 bit unsigned multiplication of odd elements
    __m512i t2  = _mm512_srli_epi64(t1,32);                // high dword of even index results
    __m512i t7  = _mm512_mask_mov_epi32(t2, 0xAAAA, t4);   // blend two results
    __m512i t8  = _mm512_sub_epi32(a,t7);                  // subtract
    __m512i t9  = _mm512_srl_epi32(t8,d.gets1());          // shift right logical
    __m512i t10 = _mm512_add_epi32(t7,t9);                 // add
    return        _mm512_srl_epi32(t10,d.gets2());         // shift right logical 
}

// vector operator /= : divide
static inline Vec16i & operator /= (Vec16i & a, Divisor_i const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16ui & operator /= (Vec16ui & a, Divisor_ui const d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec16i by compile-time constant
template <int32_t d>
static inline Vec16i divide_by_i(Vec16i const x) {
    static_assert(d != 0, "Integer division by zero");
    if constexpr (d ==  1) return  x;
    if constexpr (d == -1) return -x;
    if constexpr (uint32_t(d) == 0x80000000u) {
        return _mm512_maskz_set1_epi32(x == Vec16i(0x80000000), 1);  // avoid overflow of abs(d). return (x == 0x80000000) ? 1 : 0;
    }
    constexpr uint32_t d1 = d > 0 ? uint32_t(d) : uint32_t(-d);      // compile-time abs(d). (force compiler to treat d as 32 bits, not 64 bits)
    if constexpr ((d1 & (d1-1)) == 0) {
        // d1 is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(d1);
        __m512i sign;
        if constexpr (k > 1) sign = _mm512_srai_epi32(x, k-1); else sign = x;  // k copies of sign bit
        __m512i bias    = _mm512_srli_epi32(sign, 32-k);             // bias = x >= 0 ? 0 : k-1
        __m512i xpbias  = _mm512_add_epi32 (x, bias);                // x + bias
        __m512i q       = _mm512_srai_epi32(xpbias, k);              // (x + bias) >> k
        if (d > 0)      return q;                                    // d > 0: return  q
        return _mm512_sub_epi32(_mm512_setzero_epi32(), q);          // d < 0: return -q
    }
    // general case
    constexpr int32_t sh = bit_scan_reverse_const(uint32_t(d1)-1);   // ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    constexpr int32_t mult = int(1 + (uint64_t(1) << (32+sh)) / uint32_t(d1) - (int64_t(1) << 32));   // multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec8i a / const_int(d)
template <int32_t d>
static inline Vec16i operator / (Vec16i const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec16i a / const_uint(d)
template <uint32_t d>
static inline Vec16i operator / (Vec16i const a, Const_uint_t<d>) {
    static_assert(d < 0x80000000u, "Dividing signed integer by overflowing unsigned");
    return divide_by_i<int32_t(d)>(a);                     // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16ui by compile-time constant
template <uint32_t d>
static inline Vec16ui divide_by_ui(Vec16ui const x) {
    static_assert(d != 0, "Integer division by zero");
    if constexpr (d == 1) return x;                        // divide by 1
    constexpr  int b = bit_scan_reverse_const(d);          // floor(log2(d))
    if constexpr ((uint32_t(d) & (uint32_t(d)-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm512_srli_epi32(x, b);                   // x >> b
    }
    // general case (d > 2)
    constexpr uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d); // multiplier = 2^(32+b) / d
    constexpr  uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    constexpr  bool round_down = (2*rem < d);                        // check if fraction is less than 0.5
    constexpr uint32_t mult1 = round_down ? mult : mult + 1;

    // do 32*32->64 bit unsigned multiplication and get high part of result
    const __m512i multv = _mm512_maskz_set1_epi32(0x5555, mult1); // zero-extend mult and broadcast
    __m512i t1 = _mm512_mul_epu32(x,multv);                // 32x32->64 bit unsigned multiplication of even elements
    if constexpr (round_down) {
        t1     = _mm512_add_epi64(t1,multv);               // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m512i t2 = _mm512_srli_epi64(t1,32);                 // high dword of result 0 and 2
    __m512i t3 = _mm512_srli_epi64(x,32);                  // get odd elements into position for multiplication
    __m512i t4 = _mm512_mul_epu32(t3,multv);               // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if constexpr (round_down) {
        t4     = _mm512_add_epi64(t4,multv);               // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m512i t7 = _mm512_mask_mov_epi32(t2, 0xAAAA, t4);    // blend two results
    Vec16ui q  = _mm512_srli_epi32(t7, b);                 // shift right by b
    return q;                                              // no overflow possible
}

// define Vec8ui a / const_uint(d)
template <uint32_t d>
static inline Vec16ui operator / (Vec16ui const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8ui a / const_int(d)
template <int32_t d>
static inline Vec16ui operator / (Vec16ui const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI512_H
