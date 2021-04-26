/****************************  vectorf256.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 256-bit floating point vector classes
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec8f     Vector of 8 single precision floating point numbers
* Vec8fb    Vector of 8 Booleans for use with Vec8f
* Vec4d     Vector of 4 double precision floating point numbers
* Vec4db    Vector of 4 Booleans for use with Vec4d
*
* Each vector object is represented internally in the CPU as a 256-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORF256_H
#define VECTORF256_H  1

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

#ifdef VECTORF256E_H
#error Two different versions of vectorf256.h included
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Generate compile-time constant vector
*
*****************************************************************************/

// Generate a constant vector of 8 integers stored in memory
template <uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7>
inline __m256 constant8f() {
    /*
    const union {
        uint32_t i[8];
        __m256   ymm;
    } u = {{i0,i1,i2,i3,i4,i5,i6,i7}};
    return u.ymm;
    */
    return _mm256_castsi256_ps(_mm256_setr_epi32(i0,i1,i2,i3,i4,i5,i6,i7));
}


//    Join two 128-bit vectors. Used below
#define set_m128r(lo,hi) _mm256_insertf128_ps(_mm256_castps128_ps256(lo),(hi),1)
// _mm256_set_m128(hi,lo); // not defined in all versions of immintrin.h


/*****************************************************************************
*
*          Vec8fb: Vector of 8 Booleans for use with Vec8f
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

class Vec8fb {
protected:
    __m256 ymm; // Float vector
public:
    // Default constructor:
    Vec8fb() {
    }
    // Constructor to build from all elements:
    Vec8fb(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_ps(_mm256_setr_epi32(-(int)b0, -(int)b1, -(int)b2, -(int)b3, -(int)b4, -(int)b5, -(int)b6, -(int)b7)); 
#else
        __m128 blo = _mm_castsi128_ps(_mm_setr_epi32(-(int)b0, -(int)b1, -(int)b2, -(int)b3));
        __m128 bhi = _mm_castsi128_ps(_mm_setr_epi32(-(int)b4, -(int)b5, -(int)b6, -(int)b7));
        ymm = set_m128r(blo,bhi);
#endif
    }
    // Constructor to build from two Vec4fb:
    Vec8fb(Vec4fb const a0, Vec4fb const a1) {
        ymm = set_m128r(a0, a1);
    }
    // Constructor to convert from type __m256 used in intrinsics:
    Vec8fb(__m256 const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256 used in intrinsics:
    Vec8fb & operator = (__m256 const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec8fb(bool b) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_ps(_mm256_set1_epi32(-(int)b));
#else
        __m128 b1 = _mm_castsi128_ps(_mm_set1_epi32(-(int)b));
        //ymm = _mm256_set_m128(b1,b1);
        ymm = set_m128r(b1,b1);
#endif
    }
    // Assignment operator to broadcast scalar value:
    Vec8fb & operator = (bool b) {
        *this = Vec8fb(b);
        return *this;
    }
    // Type cast operator to convert to __m256 used in intrinsics
    operator __m256() const {
        return ymm;
    }
#if INSTRSET >= 8  // AVX2
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const x) {
        ymm = _mm256_castsi256_ps(x);
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const x) {
        ymm = _mm256_castsi256_ps(x);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec8fb & load_bits(uint8_t a) {
        Vec8ib b;  b.load_bits(a);
        ymm = _mm256_castsi256_ps(b);
        return *this;
    }
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return _mm256_castps_si256(ymm);
    }
#endif
#else  // AVX version
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const x) {
        ymm = set_m128r(_mm_castsi128_ps(x.get_low()), _mm_castsi128_ps(x.get_high()));
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const x) {
        ymm = set_m128r(_mm_castsi128_ps(x.get_low()), _mm_castsi128_ps(x.get_high()));
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    // AVX version. Use float instructions, treating integers as subnormal values
    Vec8fb & load_bits(uint8_t a) {
        __m256 b1 = _mm256_castsi256_ps(_mm256_set1_epi32((int32_t)a));  // broadcast a
        __m256 m2 = constant8f<1,2,4,8,0x10,0x20,0x40,0x80>(); 
        __m256 d1 = _mm256_and_ps(b1, m2); // isolate one bit in each dword
        ymm = _mm256_cmp_ps(d1, _mm256_setzero_ps(), 4);  // compare subnormal values with 0
        return *this;
    }
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return Vec8i(_mm_castps_si128(get_low()), _mm_castps_si128(get_high()));
    }
#endif // AVX2
    // Member function to change a single element in vector
    Vec8fb const insert(int index, bool value) {
        const int32_t maskl[16] = {0,0,0,0,0,0,0,0,-1,0,0,0,0,0,0,0};
        __m256 mask  = _mm256_loadu_ps((float const*)(maskl+8-(index & 7))); // mask with FFFFFFFF at index position
        if (value) {
            ymm = _mm256_or_ps(ymm,mask);
        }
        else {
            ymm = _mm256_andnot_ps(mask,ymm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        union {
            float   f[8];
            int32_t i[8];
        } u;
        _mm256_storeu_ps(u.f, ymm);
        return u.i[index & 7] != 0;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec4fb get_low() const {
        return _mm256_castps256_ps128(ymm);
    }
    Vec4fb get_high() const {
        return _mm256_extractf128_ps(ymm,1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec8fb(int b) = delete;
    Vec8fb & operator = (int x) = delete;
    };

#else

typedef Vec8b Vec8fb;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Operators and functions for Vec8fb
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

// vector operator & : bitwise and
static inline Vec8fb operator & (Vec8fb const a, Vec8fb const b) {
    return _mm256_and_ps(a, b);
}
static inline Vec8fb operator && (Vec8fb const a, Vec8fb const b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec8fb & operator &= (Vec8fb & a, Vec8fb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8fb operator | (Vec8fb const a, Vec8fb const b) {
    return _mm256_or_ps(a, b);
}
static inline Vec8fb operator || (Vec8fb const a, Vec8fb const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec8fb & operator |= (Vec8fb & a, Vec8fb const b) {
    a = a | b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8fb operator ~ (Vec8fb const a) {
    return _mm256_xor_ps(a, constant8f<0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu>());
}

// vector operator ^ : bitwise xor
static inline Vec8fb operator ^ (Vec8fb const a, Vec8fb const b) {
    return _mm256_xor_ps(a, b);
}

// vector operator == : xnor
static inline Vec8fb operator == (Vec8fb const a, Vec8fb const b) {
    return Vec8fb(a ^ Vec8fb(~b));
}

// vector operator != : xor
static inline Vec8fb operator != (Vec8fb const a, Vec8fb const b) {
    return _mm256_xor_ps(a, b);
} 

// vector operator ^= : bitwise xor
static inline Vec8fb & operator ^= (Vec8fb & a, Vec8fb const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not all bits in an element are the same)
static inline Vec8fb operator ! (Vec8fb const a) {
return Vec8fb( !Vec8ib(a));
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec8fb andnot(Vec8fb const a, Vec8fb const b) {
    return _mm256_andnot_ps(b, a);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec8fb const a) {
    return _mm256_testc_ps(a,constant8f<0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu>()) != 0;
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec8fb const a) {
    return ! _mm256_testz_ps(a,a);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8fb const x) {
    return to_bits(Vec8ib(x));
}

#endif


/*****************************************************************************
*
*          Vec4db: Vector of 4 Booleans for use with Vec4d
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

class Vec4db {
protected:
    __m256d ymm; // double vector
public:
    // Default constructor:
    Vec4db() {
    }
    // Constructor to build from all elements:
    Vec4db(bool b0, bool b1, bool b2, bool b3) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_pd(_mm256_setr_epi64x(-(int64_t)b0, -(int64_t)b1, -(int64_t)b2, -(int64_t)b3)); 
#else
        __m128 blo = _mm_castsi128_ps(_mm_setr_epi32(-(int)b0, -(int)b0, -(int)b1, -(int)b1));
        __m128 bhi = _mm_castsi128_ps(_mm_setr_epi32(-(int)b2, -(int)b2, -(int)b3, -(int)b3));
        ymm = _mm256_castps_pd(set_m128r(blo, bhi));
#endif
    }
    // Constructor to build from two Vec2db:
    Vec4db(Vec2db const a0, Vec2db const a1) {
        ymm = _mm256_castps_pd(set_m128r(_mm_castpd_ps(a0),_mm_castpd_ps(a1)));
        //ymm = _mm256_set_m128d(a1, a0);
    }
    // Constructor to convert from type __m256d used in intrinsics:
    Vec4db(__m256d const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256d used in intrinsics:
    Vec4db & operator = (__m256d const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast the same value into all elements:
    Vec4db(bool b) {
#if INSTRSET >= 8  // AVX2
        ymm = _mm256_castsi256_pd(_mm256_set1_epi64x(-(int64_t)b));
#else
        __m128 b1 = _mm_castsi128_ps(_mm_set1_epi32(-(int)b));
        ymm = _mm256_castps_pd(set_m128r(b1,b1));
#endif
    }
    // Assignment operator to broadcast scalar value:
    Vec4db & operator = (bool b) {
        ymm = _mm256_castsi256_pd(_mm256_set1_epi32(-int32_t(b)));
        return *this;
    }
    // Type cast operator to convert to __m256d used in intrinsics
    operator __m256d() const {
        return ymm;
    }
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const x) {
        ymm = _mm256_castsi256_pd(x);
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const x) {
        ymm = _mm256_castsi256_pd(x);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec4db & load_bits(uint8_t a) {
        Vec4qb b; b.load_bits(a);
        ymm = _mm256_castsi256_pd(b);
        return *this;
    }
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return _mm256_castpd_si256(ymm);
    }
#endif
#else   // 256 bit integer vectors emulated without AVX2
    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const x) {
        *this = Vec4db(_mm_castsi128_pd(x.get_low()), _mm_castsi128_pd(x.get_high()));
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const x) {
        *this = Vec4db(_mm_castsi128_pd(x.get_low()), _mm_castsi128_pd(x.get_high()));
        return *this;
    }
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return Vec4q(_mm_castpd_si128(get_low()), _mm_castpd_si128(get_high()));
    }
    // Member function to change a bitfield to a boolean vector
    // AVX version. Use float instructions, treating integers as subnormal values
    Vec4db & load_bits(uint8_t a) {
        __m256d b1 = _mm256_castsi256_pd(_mm256_set1_epi32((int32_t)a));  // broadcast a
        __m256d m2 = _mm256_castps_pd(constant8f<1,0,2,0,4,0,8,0>()); 
        __m256d d1 = _mm256_and_pd(b1, m2); // isolate one bit in each dword
        ymm = _mm256_cmp_pd(d1, _mm256_setzero_pd(), 4);  // compare subnormal values with 0
        return *this;
    }
#endif // AVX2
    // Member function to change a single element in vector
    Vec4db const insert(int index, bool value) {
        const int32_t maskl[16] = {0,0,0,0,0,0,0,0,-1,-1,0,0,0,0,0,0};
        __m256d mask = _mm256_loadu_pd((double const*)(maskl+8-(index&3)*2)); // mask with FFFFFFFFFFFFFFFF at index position
        if (value) {
            ymm = _mm256_or_pd(ymm,mask);
        }
        else {
            ymm = _mm256_andnot_pd(mask,ymm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        union {
            double  f[8];
            int32_t i[16];
        } u;
        _mm256_storeu_pd(u.f, ymm);
        return u.i[(index & 3) * 2 + 1] != 0;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec2db get_low() const {
        return _mm256_castpd256_pd128(ymm);
    }
    Vec2db get_high() const {
        return _mm256_extractf128_pd(ymm,1);
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec4db(int b) = delete;
    Vec4db & operator = (int x) = delete;
};

#else

typedef Vec4b Vec4db;  // compact boolean vector

#endif

/*****************************************************************************
*
*          Operators and functions for Vec4db
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

// vector operator & : bitwise and
static inline Vec4db operator & (Vec4db const a, Vec4db const b) {
    return _mm256_and_pd(a, b);
}
static inline Vec4db operator && (Vec4db const a, Vec4db const b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec4db & operator &= (Vec4db & a, Vec4db const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4db operator | (Vec4db const a, Vec4db const b) {
    return _mm256_or_pd(a, b);
}
static inline Vec4db operator || (Vec4db const a, Vec4db const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec4db & operator |= (Vec4db & a, Vec4db const b) {
    a = a | b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4db operator ~ (Vec4db const a) {
    return _mm256_xor_pd(a, _mm256_castps_pd (constant8f<0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu,0xFFFFFFFFu>()));
}

// vector operator ^ : bitwise xor
static inline Vec4db operator ^ (Vec4db const a, Vec4db const b) {
    return _mm256_xor_pd(a, b);
}

// vector operator == : xnor
static inline Vec4db operator == (Vec4db const a, Vec4db const b) {
    return Vec4db(a ^ Vec4db(~b));
}

// vector operator != : xor
static inline Vec4db operator != (Vec4db const a, Vec4db const b) {
    return _mm256_xor_pd(a, b);
} 

// vector operator ^= : bitwise xor
static inline Vec4db & operator ^= (Vec4db & a, Vec4db const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not all bits in an element are the same)
static inline Vec4db operator ! (Vec4db const a) {
return Vec4db( ! Vec4qb(a));
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec4db andnot(Vec4db const a, Vec4db const b) {
    return _mm256_andnot_pd(b, a);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec4db const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    return horizontal_and(Vec256b(_mm256_castpd_si256(a)));
#else  // split into 128 bit vectors
    return horizontal_and(a.get_low() & a.get_high());
#endif
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec4db const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    return horizontal_or(Vec256b(_mm256_castpd_si256(a)));
#else  // split into 128 bit vectors
    return horizontal_or(a.get_low() | a.get_high());
#endif
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4db const x) {
    return to_bits(Vec4qb(x));
}

#endif


 /*****************************************************************************
*
*          Vec8f: Vector of 8 single precision floating point values
*
*****************************************************************************/

class Vec8f {
protected:
    __m256 ymm; // Float vector
public:
    // Default constructor:
    Vec8f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8f(float f) {
        ymm = _mm256_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec8f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7) {
        ymm = _mm256_setr_ps(f0, f1, f2, f3, f4, f5, f6, f7); 
    }
    // Constructor to build from two Vec4f:
    Vec8f(Vec4f const a0, Vec4f const a1) {
        ymm = set_m128r(a0, a1);
        //ymm = _mm256_set_m128(a1, a0);
    }
    // Constructor to convert from type __m256 used in intrinsics:
    Vec8f(__m256 const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256 used in intrinsics:
    Vec8f & operator = (__m256 const x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256 used in intrinsics
    operator __m256() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec8f & load(float const * p) {
        ymm = _mm256_loadu_ps(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address divisible by 32
    Vec8f & load_a(float const * p) {
        ymm = _mm256_load_ps(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm256_storeu_ps(p, ymm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(float * p) const {
        _mm256_stream_ps(p, ymm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 32;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address divisible by 32
    void store_a(float * p) const {
        _mm256_store_ps(p, ymm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8f & load_partial(int n, float const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_ps(__mmask8((1u << n) - 1), p);
#else 
        if (n > 0 && n <= 4) {
            *this = Vec8f(Vec4f().load_partial(n, p), _mm_setzero_ps());
        }
        else if (n > 4 && n <= 8) {
            *this = Vec8f(Vec4f().load(p), Vec4f().load_partial(n - 4, p + 4));
        }
        else {
            ymm = _mm256_setzero_ps();
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm256_mask_storeu_ps(p, __mmask8((1u << n) - 1), ymm);
#else 
        if (n <= 4) {
            get_low().store_partial(n, p);
        }
        else if (n <= 8) {
            get_low().store(p);
            get_high().store_partial(n - 4, p + 4);
        }
#endif
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8f & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_ps(__mmask8((1u << n) - 1), ymm);
#else  
        if (uint32_t(n) >= 8) return *this;
        const union {        
            int32_t i[16];
            float   f[16];
        } mask = {{-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0}};
        *this = Vec8fb(*this) & Vec8fb(Vec8f().load(mask.f + 8 - n));
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec8f const insert(int index, float value) {
#if INSTRSET >= 10   // AVX512VL         
        ymm = _mm256_mask_broadcastss_ps (ymm, __mmask8(1u << index), _mm_set_ss(value));
#else
        __m256 v0 = _mm256_broadcast_ss(&value);
        switch (index) {
        case 0:
            ymm = _mm256_blend_ps (ymm, v0, 1);  break;
        case 1:
            ymm = _mm256_blend_ps (ymm, v0, 2);  break;
        case 2:
            ymm = _mm256_blend_ps (ymm, v0, 4);  break;
        case 3:
            ymm = _mm256_blend_ps (ymm, v0, 8);  break;
        case 4:
            ymm = _mm256_blend_ps (ymm, v0, 0x10);  break;
        case 5:
            ymm = _mm256_blend_ps (ymm, v0, 0x20);  break;
        case 6:
            ymm = _mm256_blend_ps (ymm, v0, 0x40);  break;
        default:
            ymm = _mm256_blend_ps (ymm, v0, 0x80);  break;
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    float extract(int index) const {
#if INSTRSET >= 10
        __m256 x = _mm256_maskz_compress_ps(__mmask8(1u << index), ymm);
        return _mm256_cvtss_f32(x);        
#else 
        float x[8];
        store(x);
        return x[index & 7];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec4f get_low() const {
        return _mm256_castps256_ps128(ymm);
    }
    Vec4f get_high() const {
        return _mm256_extractf128_ps(ymm,1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 16;
    }
    typedef __m256 registertype;
};


/*****************************************************************************
*
*          Operators for Vec8f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8f operator + (Vec8f const a, Vec8f const b) {
    return _mm256_add_ps(a, b);
}

// vector operator + : add vector and scalar
static inline Vec8f operator + (Vec8f const a, float b) {
    return a + Vec8f(b);
}
static inline Vec8f operator + (float a, Vec8f const b) {
    return Vec8f(a) + b;
}

// vector operator += : add
static inline Vec8f & operator += (Vec8f & a, Vec8f const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8f operator ++ (Vec8f & a, int) {
    Vec8f a0 = a;
    a = a + 1.0f;
    return a0;
}

// prefix operator ++
static inline Vec8f & operator ++ (Vec8f & a) {
    a = a + 1.0f;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8f operator - (Vec8f const a, Vec8f const b) {
    return _mm256_sub_ps(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec8f operator - (Vec8f const a, float b) {
    return a - Vec8f(b);
}
static inline Vec8f operator - (float a, Vec8f const b) {
    return Vec8f(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec8f operator - (Vec8f const a) {
    return _mm256_xor_ps(a, Vec8f(-0.0f));
}

// vector operator -= : subtract
static inline Vec8f & operator -= (Vec8f & a, Vec8f const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8f operator -- (Vec8f & a, int) {
    Vec8f a0 = a;
    a = a - 1.0f;
    return a0;
}

// prefix operator --
static inline Vec8f & operator -- (Vec8f & a) {
    a = a - 1.0f;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8f operator * (Vec8f const a, Vec8f const b) {
    return _mm256_mul_ps(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec8f operator * (Vec8f const a, float b) {
    return a * Vec8f(b);
}
static inline Vec8f operator * (float a, Vec8f const b) {
    return Vec8f(a) * b;
}

// vector operator *= : multiply
static inline Vec8f & operator *= (Vec8f & a, Vec8f const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8f operator / (Vec8f const a, Vec8f const b) {
    return _mm256_div_ps(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec8f operator / (Vec8f const a, float b) {
    return a / Vec8f(b);
}
static inline Vec8f operator / (float a, Vec8f const b) {
    return Vec8f(a) / b;
}

// vector operator /= : divide
static inline Vec8f & operator /= (Vec8f & a, Vec8f const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8fb operator == (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 0);
#else
    return _mm256_cmp_ps(a, b, 0);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec8fb operator != (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 4);
#else
    return _mm256_cmp_ps(a, b, 4);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec8fb operator < (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 1);
#else
    return _mm256_cmp_ps(a, b, 1);
#endif
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8fb operator <= (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 2);
#else
    return _mm256_cmp_ps(a, b, 2);
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec8fb operator > (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 6);
#else
    return b < a;
#endif
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8fb operator >= (Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_ps_mask(a, b, 5);
#else
    return b <= a;
#endif
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8f operator & (Vec8f const a, Vec8f const b) {
    return _mm256_and_ps(a, b);
}

// vector operator &= : bitwise and
static inline Vec8f & operator &= (Vec8f & a, Vec8f const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8f and Vec8fb
static inline Vec8f operator & (Vec8f const a, Vec8fb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_maskz_mov_ps(b, a);
#else
    return _mm256_and_ps(a, b);
#endif
}
static inline Vec8f operator & (Vec8fb const a, Vec8f const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec8f operator | (Vec8f const a, Vec8f const b) {
    return _mm256_or_ps(a, b);
}

// vector operator |= : bitwise or
static inline Vec8f & operator |= (Vec8f & a, Vec8f const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8f operator ^ (Vec8f const a, Vec8f const b) {
    return _mm256_xor_ps(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec8f & operator ^= (Vec8f & a, Vec8f const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8fb operator ! (Vec8f const a) {
    return a == Vec8f(0.0f);
}


/*****************************************************************************
*
*          Functions for Vec8f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8f select (Vec8fb const s, Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_ps(b, s, a);
#else
    return _mm256_blendv_ps (b, a, s);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8f if_add (Vec8fb const f, Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10
    return _mm256_mask_add_ps (a, f, a, b);
#else
    return a + (Vec8f(f) & b);
#endif
}

// Conditional subtract
static inline Vec8f if_sub (Vec8fb const f, Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10
    return _mm256_mask_sub_ps (a, f, a, b);
#else
    return a - (Vec8f(f) & b);
#endif
}

// Conditional multiply
static inline Vec8f if_mul (Vec8fb const f, Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10
    return _mm256_mask_mul_ps (a, f, a, b);
#else
    return a * select(f, b, 1.f);
#endif
}

// Conditional divide
static inline Vec8f if_div (Vec8fb const f, Vec8f const a, Vec8f const b) {
#if INSTRSET >= 10
    return _mm256_mask_div_ps (a, f, a, b);
#else
    return a / select(f, b, 1.f);
#endif
}

// Sign functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb sign_bit(Vec8f const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);    // reinterpret as 32-bit integer
    Vec8i t2 = t1 >> 31;                  // extend sign bit
#if INSTRSET >= 10
    return t2 != 0;
#else
    return _mm256_castsi256_ps(t2);       // reinterpret as 32-bit Boolean
#endif
#else
    return Vec8fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
#endif
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8f sign_combine(Vec8f const a, Vec8f const b) {
#if INSTRSET < 10
    return a ^ (b & Vec8f(-0.0f));
#else
    return _mm256_castsi256_ps (_mm256_ternarylogic_epi32(
        _mm256_castps_si256(a), _mm256_castps_si256(b), Vec8i(0x80000000), 0x78));
#endif
}

// Categorization functions

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_finite(Vec8f const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask8(~ _mm256_fpclass_ps_mask (a, 0x99));
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);    // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                // shift out sign bit
    Vec8ib t3 = Vec8i(t2 & 0xFF000000) != 0xFF000000; // exponent field is not all 1s
    return t3;
#else
    return Vec8fb(is_finite(a.get_low()), is_finite(a.get_high()));
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_inf(Vec8f const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_ps_mask (a, 0x18);
#elif INSTRSET >= 8  //  256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a); // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                // shift out sign bit
    return t2 == 0xFF000000;           // exponent is all 1s, fraction is 0
#else
    return Vec8fb(is_inf(a.get_low()), is_inf(a.get_high()));
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec8fb is_nan(Vec8f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm256_fpclass_ps_mask (a, 0x81);
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) 
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec8fb is_nan(Vec8f const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
static inline Vec8fb is_nan(Vec8f const a) {
    __m256 aa = a;
    __m256 unordered;
    __asm volatile("vcmpps $3, %1, %1, %0" : "=v" (unordered) :  "v" (aa) );
    return Vec8fb(unordered);
}
#else
static inline Vec8fb is_nan(Vec8f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm256_cmp_ps(a, a, 3); // compare unordered
    // return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif


// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8fb is_subnormal(Vec8f const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_ps_mask (a, 0x20);
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = _mm256_castps_si256(a);      // reinterpret as 32-bit integer
    Vec8i t2 = t1 << 1;                     // shift out sign bit
    Vec8i t3 = 0xFF000000;                  // exponent mask
    Vec8i t4 = t2 & t3;                     // exponent
    Vec8i t5 = _mm256_andnot_si256(t3,t2);  // fraction
    return Vec8ib(t4 == 0 && t5 != 0);      // exponent = 0 and fraction != 0
#else
    return Vec8fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8fb is_zero_or_subnormal(Vec8f const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_ps_mask (a, 0x26);
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2    Vec8i t = _mm256_castps_si256(a);            // reinterpret as 32-bit integer
    Vec8i t = _mm256_castps_si256(a);       // reinterpret as 32-bit integer
    t &= 0x7F800000;                        // isolate exponent
    return t == 0;                          // exponent = 0
#else
    return Vec8fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
#endif
}

// change signs on vectors Vec8f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
inline Vec8f change_sign(Vec8f const a) {
    if ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) == 0) return a;
    __m256 mask = constant8f<
        (i0 ? 0x80000000u : 0u), (i1 ? 0x80000000u : 0u), (i2 ? 0x80000000u : 0u), (i3 ? 0x80000000u : 0u),
        (i4 ? 0x80000000u : 0u), (i5 ? 0x80000000u : 0u), (i6 ? 0x80000000u : 0u), (i7 ? 0x80000000u : 0u)> ();
    return _mm256_xor_ps(a, mask);
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec8f const a) {
    return horizontal_add(a.get_low()+a.get_high());
}

// function max: a > b ? a : b
static inline Vec8f max(Vec8f const a, Vec8f const b) {
    return _mm256_max_ps(a,b);
}

// function min: a < b ? a : b
static inline Vec8f min(Vec8f const a, Vec8f const b) {
    return _mm256_min_ps(a,b);
} 
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec8f abs(Vec8f const a) {
#if INSTRSET >= 10  // AVX512VL
    return _mm256_range_ps(a, a, 8);
#else
    __m256 mask = constant8f<0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu,0x7FFFFFFFu> ();
    return _mm256_and_ps(a,mask);
#endif
}

// function sqrt: square root
static inline Vec8f sqrt(Vec8f const a) {
    return _mm256_sqrt_ps(a);
}

// function square: a * a
static inline Vec8f square(Vec8f const a) {
    return a * a;
}

// The purpose of this template is to prevent implicit conversion of a float
// exponent to int when calling pow(vector, float) and vectormath_exp.h is not included 
template <typename TT> static Vec8f pow(Vec8f const a, TT const n);

// Raise floating point numbers to integer power n
template <>
inline Vec8f pow<int>(Vec8f const x0, int const n) {
    return pow_template_i<Vec8f>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec8f pow<uint32_t>(Vec8f const x0, uint32_t const n) {
    return pow_template_i<Vec8f>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec8f pow(Vec8f const a, Const_int_t<n>) {
    return pow_n<Vec8f, n>(a);
} 

// function round: round to nearest integer (even). (result as float vector)
static inline Vec8f round(Vec8f const a) {
    return _mm256_round_ps(a, 0+8);
}

// function truncate: round towards zero. (result as float vector)
static inline Vec8f truncate(Vec8f const a) {
    return _mm256_round_ps(a, 3+8);
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec8f floor(Vec8f const a) {
    return _mm256_round_ps(a, 1+8);
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec8f ceil(Vec8f const a) {
    return _mm256_round_ps(a, 2+8);
}

#if INSTRSET >= 8  // 256 bit integer vectors are available

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec8i roundi(Vec8f const a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm256_cvtps_epi32(a);
}

// function truncatei: round towards zero. (result as integer vector)
static inline Vec8i truncatei(Vec8f const a) {
    return _mm256_cvttps_epi32(a);
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const a) {
    return _mm256_cvtepi32_ps(a);
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec8f to_float(Vec8ui const a) {
#if INSTRSET >= 10 && !defined (_MSC_VER)  // _mm256_cvtepu32_ps missing in VS2019
    return _mm256_cvtepu32_ps(a);
#elif INSTRSET >= 9  // __AVX512F__
    return _mm512_castps512_ps256(_mm512_cvtepu32_ps(_mm512_castsi256_si512(a)));
#else
    Vec8f b = to_float(Vec8i(a & 0xFFFFF));             // 20 bits
    Vec8f c = to_float(Vec8i(a >> 20));                 // remaining bits
    Vec8f d = b + c * 1048576.f;  // 2^20
    return d;
#endif
}

#else // no AVX2

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec8i roundi(Vec8f const a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8i(_mm_cvtps_epi32(a.get_low()), _mm_cvtps_epi32(a.get_high()));
}

// function truncatei: round towards zero. (result as integer vector)
static inline Vec8i truncatei(Vec8f const a) {
    return Vec8i(_mm_cvttps_epi32(a.get_low()), _mm_cvttps_epi32(a.get_high()));
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const a) {
    return Vec8f(_mm_cvtepi32_ps(a.get_low()), _mm_cvtepi32_ps(a.get_high()));
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec8f to_float(Vec8ui const a) {
    return Vec8f(to_float(a.get_low()), to_float(a.get_high()));
}
#endif // AVX2


// Fused multiply and add functions

// Multiply and add
static inline Vec8f mul_add(Vec8f const a, Vec8f const b, Vec8f const c) {
#ifdef __FMA__
    return _mm256_fmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_macc_ps(a, b, c);
#else
    return a * b + c;
#endif    
}

// Multiply and subtract
static inline Vec8f mul_sub(Vec8f const a, Vec8f const b, Vec8f const c) {
#ifdef __FMA__
    return _mm256_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_ps(a, b, c);
#else
    return a * b - c;
#endif    
}

// Multiply and inverse subtract
static inline Vec8f nmul_add(Vec8f const a, Vec8f const b, Vec8f const c) {
#ifdef __FMA__
    return _mm256_fnmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_nmacc_ps(a, b, c);
#else
    return c - a * b;
#endif
}


// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
// This is used in mathematical functions. Do not use it in general code 
// because it is inaccurate in certain cases
static inline Vec8f mul_sub_x(Vec8f const a, Vec8f const b, Vec8f const c) {
#ifdef __FMA__
    return _mm256_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_ps(a, b, c);
#else
    // calculate a * b - c with extra precision
    const uint32_t b12 = uint32_t(-(1 << 12));   // mask to remove lower 12 bits
    Vec8f upper_mask = constant8f<b12,b12,b12,b12,b12,b12,b12,b12>();
    Vec8f a_high = a & upper_mask;               // split into high and low parts
    Vec8f b_high = b & upper_mask;
    Vec8f a_low  = a - a_high;
    Vec8f b_low  = b - b_high;
    Vec8f r1 = a_high * b_high;                  // this product is exact
    Vec8f r2 = r1 - c;                           // subtract c from high product
    Vec8f r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a. relative accuracy better than 2^-11)
static inline Vec8f approx_recipr(Vec8f const a) {
#ifdef __AVX512ER__  // AVX512ER: full precision
    // todo: if future processors have both AVX512ER and AVX512VL: _mm256_rcp28_round_ps(a, _MM_FROUND_NO_EXC);
    return _mm512_castps512_ps256(_mm512_rcp28_round_ps(_mm512_castps256_ps512(a), _MM_FROUND_NO_EXC));
#elif INSTRSET >= 10  // AVX512VL: 14 bit precision
    return _mm256_rcp14_ps(a);
#elif INSTRSET >= 9   // AVX512F: 14 bit precision
    return _mm512_castps512_ps256(_mm512_rcp14_ps(_mm512_castps256_ps512(a)));
#else  // AVX: 11 bit precision
    return _mm256_rcp_ps(a);
#endif
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a). Relative accuracy better than 2^-11)
static inline Vec8f approx_rsqrt(Vec8f const a) {
// use more accurate version if available. (none of these will raise exceptions on zero)
#ifdef __AVX512ER__  // AVX512ER: full precision
    // todo: if future processors have both AVX512ER and AVX521VL: _mm256_rsqrt28_round_ps(a, _MM_FROUND_NO_EXC);
    return _mm512_castps512_ps256(_mm512_rsqrt28_round_ps(_mm512_castps256_ps512(a), _MM_FROUND_NO_EXC));
#elif INSTRSET >= 10 && defined(_mm256_rsqrt14_ps)  // missing in VS2019
    return _mm256_rsqrt14_ps(a);
#elif INSTRSET >= 9  // AVX512F: 14 bit precision
    return _mm512_castps512_ps256(_mm512_rsqrt14_ps(_mm512_castps256_ps512(a)));
#else  // AVX: 11 bit precision
    return _mm256_rsqrt_ps(a);
#endif
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec8i exponent(Vec8f const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec8ui t1 = _mm256_castps_si256(a);// reinterpret as 32-bit integer
    Vec8ui t2 = t1 << 1;               // shift out sign bit
    Vec8ui t3 = t2 >> 24;              // shift down logical to position 0
    Vec8i  t4 = Vec8i(t3) - 0x7F;      // subtract bias from exponent
    return t4;
#else  // no AVX2
    return Vec8i(exponent(a.get_low()), exponent(a.get_high()));
#endif
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec8f fraction(Vec8f const a) {
#if INSTRSET >= 10
    return _mm256_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
#elif INSTRSET >= 8 // AVX2. 256 bit integer vectors are available
    Vec8ui t1 = _mm256_castps_si256(a);   // reinterpret as 32-bit integer
    Vec8ui t2 = (t1 & 0x007FFFFF) | 0x3F800000; // set exponent to 0 + bias
    return _mm256_castsi256_ps(t2);
#else
    return Vec8f(fraction(a.get_low()), fraction(a.get_high()));
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec8f exp2(Vec8i const n) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec8i t1 = max(n,  -0x7F);         // limit to allowed range
    Vec8i t2 = min(t1,  0x80);
    Vec8i t3 = t2 + 0x7F;              // add bias
    Vec8i t4 = t3 << 23;               // put exponent into position 23
    return _mm256_castsi256_ps(t4);    // reinterpret as float
#else
    return Vec8f(exp2(n.get_low()), exp2(n.get_high()));
#endif // AVX2
}
//static inline Vec8f exp2(Vec8f const x); // defined in vectormath_exp.h



/*****************************************************************************
*
*          Vec4d: Vector of 4 double precision floating point values
*
*****************************************************************************/

class Vec4d {
protected:
    __m256d ymm; // double vector
public:
    // Default constructor:
    Vec4d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4d(double d) {
        ymm = _mm256_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec4d(double d0, double d1, double d2, double d3) {
        ymm = _mm256_setr_pd(d0, d1, d2, d3); 
    }
    // Constructor to build from two Vec2d:
    Vec4d(Vec2d const a0, Vec2d const a1) {
        ymm = _mm256_castps_pd(set_m128r(_mm_castpd_ps(a0), _mm_castpd_ps(a1)));
        //ymm = _mm256_set_m128d(a1, a0);
    }
    // Constructor to convert from type __m256d used in intrinsics:
    Vec4d(__m256d const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256d used in intrinsics:
    Vec4d & operator = (__m256d const x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256d used in intrinsics
    operator __m256d() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec4d & load(double const * p) {
        ymm = _mm256_loadu_pd(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32
    Vec4d & load_a(double const * p) {
        ymm = _mm256_load_pd(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm256_storeu_pd(p, ymm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(double * p) const {
        _mm256_stream_pd(p, ymm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 32;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32
    void store_a(double * p) const {
        _mm256_store_pd(p, ymm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4d & load_partial(int n, double const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_pd(__mmask8((1u << n) - 1), p);
#else 
        if (n > 0 && n <= 2) {
            *this = Vec4d(Vec2d().load_partial(n, p), _mm_setzero_pd());
        }
        else if (n > 2 && n <= 4) {
            *this = Vec4d(Vec2d().load(p), Vec2d().load_partial(n - 2, p + 2));
        }
        else {
            ymm = _mm256_setzero_pd();
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm256_mask_storeu_pd(p, __mmask8((1u << n) - 1), ymm);
#else 
        if (n <= 2) {
            get_low().store_partial(n, p);
        }
        else if (n <= 4) {
            get_low().store(p);
            get_high().store_partial(n - 2, p + 2);
        }
#endif
    }
    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec4d & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_pd(__mmask8((1u << n) - 1), ymm);
#else  
        ymm = _mm256_castps_pd(Vec8f(_mm256_castpd_ps(ymm)).cutoff(n*2));
#endif
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec4d const insert(int index, double value) {
#if INSTRSET >= 10   // AVX512VL         
        ymm = _mm256_mask_broadcastsd_pd (ymm, __mmask8(1u << index), _mm_set_sd(value));
#else
        __m256d v0 = _mm256_broadcast_sd(&value);
        switch (index) {
        case 0:
            ymm = _mm256_blend_pd (ymm, v0, 1);  break;
        case 1:
            ymm = _mm256_blend_pd (ymm, v0, 2);  break;
        case 2:
            ymm = _mm256_blend_pd (ymm, v0, 4);  break;
        default:
            ymm = _mm256_blend_pd (ymm, v0, 8);  break;
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    double extract(int index) const {
#if INSTRSET >= 10
        __m256d x = _mm256_maskz_compress_pd(__mmask8(1u << index), ymm);
        return _mm256_cvtsd_f64(x);        
#else 
        double x[4];
        store(x);
        return x[index & 3];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2d:
    Vec2d get_low() const {
        return _mm256_castpd256_pd128(ymm);
    }
    Vec2d get_high() const {
        return _mm256_extractf128_pd(ymm,1);
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 17;
    }
    typedef __m256d registertype;
};


/*****************************************************************************
*
*          Operators for Vec4d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4d operator + (Vec4d const a, Vec4d const b) {
    return _mm256_add_pd(a, b);
}

// vector operator + : add vector and scalar
static inline Vec4d operator + (Vec4d const a, double b) {
    return a + Vec4d(b);
}
static inline Vec4d operator + (double a, Vec4d const b) {
    return Vec4d(a) + b;
}

// vector operator += : add
static inline Vec4d & operator += (Vec4d & a, Vec4d const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4d operator ++ (Vec4d & a, int) {
    Vec4d a0 = a;
    a = a + 1.0;
    return a0;
}

// prefix operator ++
static inline Vec4d & operator ++ (Vec4d & a) {
    a = a + 1.0;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4d operator - (Vec4d const a, Vec4d const b) {
    return _mm256_sub_pd(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec4d operator - (Vec4d const a, double b) {
    return a - Vec4d(b);
}
static inline Vec4d operator - (double a, Vec4d const b) {
    return Vec4d(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec4d operator - (Vec4d const a) {
    return _mm256_xor_pd(a, _mm256_castps_pd(constant8f<0u,0x80000000u,0u,0x80000000u,0u,0x80000000u,0u,0x80000000u> ()));
}

// vector operator -= : subtract
static inline Vec4d & operator -= (Vec4d & a, Vec4d const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4d operator -- (Vec4d & a, int) {
    Vec4d a0 = a;
    a = a - 1.0;
    return a0;
}

// prefix operator --
static inline Vec4d & operator -- (Vec4d & a) {
    a = a - 1.0;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4d operator * (Vec4d const a, Vec4d const b) {
    return _mm256_mul_pd(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec4d operator * (Vec4d const a, double b) {
    return a * Vec4d(b);
}
static inline Vec4d operator * (double a, Vec4d const b) {
    return Vec4d(a) * b;
}

// vector operator *= : multiply
static inline Vec4d & operator *= (Vec4d & a, Vec4d const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec4d operator / (Vec4d const a, Vec4d const b) {
    return _mm256_div_pd(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec4d operator / (Vec4d const a, double b) {
    return a / Vec4d(b);
}
static inline Vec4d operator / (double a, Vec4d const b) {
    return Vec4d(a) / b;
}

// vector operator /= : divide
static inline Vec4d & operator /= (Vec4d & a, Vec4d const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4db operator == (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 0);
#else
    return _mm256_cmp_pd(a, b, 0);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec4db operator != (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 4);
#else
    return _mm256_cmp_pd(a, b, 4);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec4db operator < (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 1);
#else
    return _mm256_cmp_pd(a, b, 1);
#endif
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec4db operator <= (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 2);
#else
    return _mm256_cmp_pd(a, b, 2);
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec4db operator > (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 6);
#else
    return b < a;
#endif
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec4db operator >= (Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_pd_mask(a, b, 5);
#else
    return b <= a;
#endif
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec4d operator & (Vec4d const a, Vec4d const b) {
    return _mm256_and_pd(a, b);
}

// vector operator &= : bitwise and
static inline Vec4d & operator &= (Vec4d & a, Vec4d const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec4d and Vec4db
static inline Vec4d operator & (Vec4d const a, Vec4db const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_maskz_mov_pd(b, a);
#else
    return _mm256_and_pd(a, b);
#endif
}
static inline Vec4d operator & (Vec4db const a, Vec4d const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec4d operator | (Vec4d const a, Vec4d const b) {
    return _mm256_or_pd(a, b);
}

// vector operator |= : bitwise or
static inline Vec4d & operator |= (Vec4d & a, Vec4d const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4d operator ^ (Vec4d const a, Vec4d const b) {
    return _mm256_xor_pd(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec4d & operator ^= (Vec4d & a, Vec4d const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec4db operator ! (Vec4d const a) {
    return a == Vec4d(0.0);
}


/*****************************************************************************
*
*          Functions for Vec4d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec4d select (Vec4db const s, Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_pd(b, s, a);
#else
    return _mm256_blendv_pd(b, a, s);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4d if_add (Vec4db const f, Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10
    return _mm256_mask_add_pd (a, f, a, b);
#else
    return a + (Vec4d(f) & b);
#endif
}

// Conditional subtract
static inline Vec4d if_sub (Vec4db const f, Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10
    return _mm256_mask_sub_pd (a, f, a, b);
#else
    return a - (Vec4d(f) & b);
#endif
}

// Conditional multiply
static inline Vec4d if_mul (Vec4db const f, Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10
    return _mm256_mask_mul_pd (a, f, a, b);
#else
    return a * select(f, b, 1.);
#endif
}

// Conditional divide
static inline Vec4d if_div (Vec4db const f, Vec4d const a, Vec4d const b) {
#if INSTRSET >= 10
    return _mm256_mask_div_pd (a, f, a, b);
#else
    return a / select(f, b, 1.);
#endif
}

// sign functions

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec4d sign_combine(Vec4d const a, Vec4d const b) {
#if INSTRSET < 10
    return a ^ (b & Vec4d(-0.0));
#else
    return _mm256_castsi256_pd (_mm256_ternarylogic_epi64(
        _mm256_castpd_si256(a), _mm256_castpd_si256(b), Vec4q(0x8000000000000000), 0x78));
#endif
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec4db is_finite(Vec4d const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask8(~ _mm256_fpclass_pd_mask (a, 0x99));
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    Vec4q t3 = 0xFFE0000000000000;     // exponent mask
    Vec4qb t4 = Vec4q(t2 & t3) != t3;  // exponent field is not all 1s
    return t4;
#else
    return Vec4db(is_finite(a.get_low()),is_finite(a.get_high()));
#endif
}

// categorization functions

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec4db is_inf(Vec4d const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_pd_mask (a, 0x18);
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    return t2 == 0xFFE0000000000000;   // exponent is all 1s, fraction is 0
#else
    return Vec4db(is_inf(a.get_low()),is_inf(a.get_high()));
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec4db is_nan(Vec4d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm256_fpclass_pd_mask (a, 0x81);
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) 
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec4db is_nan(Vec4d const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
static inline Vec4db is_nan(Vec4d const a) {
    __m256d aa = a;
    __m256d unordered;
    __asm volatile("vcmppd $3, %1, %1, %0" : "=v" (unordered) :  "v" (aa) );
    return Vec4db(unordered);
}
#else
static inline Vec4db is_nan(Vec4d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm256_cmp_pd(a, a, 3); // compare unordered
    // return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif


// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec4db is_subnormal(Vec4d const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_pd_mask (a, 0x20);
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a); // reinterpret as 64-bit integer
    Vec4q t2 = t1 << 1;                // shift out sign bit
    Vec4q t3 = 0xFFE0000000000000;     // exponent mask
    Vec4q t4 = t2 & t3;                // exponent
    Vec4q t5 = _mm256_andnot_si256(t3,t2);// fraction
    return Vec4qb(t4 == 0 && t5 != 0); // exponent = 0 and fraction != 0
#else
    return Vec4db(is_subnormal(a.get_low()),is_subnormal(a.get_high()));
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec4db is_zero_or_subnormal(Vec4d const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_fpclass_pd_mask (a, 0x26);
#elif INSTRSET >= 8  // 256 bit integer vectors are available, AVX2    Vec8i t = _mm256_castps_si256(a);            // reinterpret as 32-bit integer
    Vec4q t = _mm256_castpd_si256(a);     // reinterpret as 32-bit integer
    t &= 0x7FF0000000000000ll;   // isolate exponent
    return t == 0;                     // exponent = 0
#else
    return Vec4db(is_zero_or_subnormal(a.get_low()),is_zero_or_subnormal(a.get_high()));
#endif
} 

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec4d const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec4d max(Vec4d const a, Vec4d const b) {
    return _mm256_max_pd(a,b);
}

// function min: a < b ? a : b
static inline Vec4d min(Vec4d const a, Vec4d const b) {
    return _mm256_min_pd(a,b);
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec4d abs(Vec4d const a) {
#if INSTRSET >= 10  // AVX512VL
    return _mm256_range_pd(a, a, 8);
#else
    __m256d mask = _mm256_castps_pd(constant8f<0xFFFFFFFFu,0x7FFFFFFFu,0xFFFFFFFFu,0x7FFFFFFFu,0xFFFFFFFFu,0x7FFFFFFFu,0xFFFFFFFFu,0x7FFFFFFFu> ());
    return _mm256_and_pd(a,mask);
#endif
}

// function sqrt: square root
static inline Vec4d sqrt(Vec4d const a) {
    return _mm256_sqrt_pd(a);
}

// function square: a * a
static inline Vec4d square(Vec4d const a) {
    return a * a;
}

// The purpose of this template is to prevent implicit conversion of a float
// exponent to int when calling pow(vector, float) and vectormath_exp.h is not included 
template <typename TT> static Vec4d pow(Vec4d const a, TT const n);

// Raise floating point numbers to integer power n
template <>
inline Vec4d pow<int>(Vec4d const x0, int const n) {
    return pow_template_i<Vec4d>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec4d pow<uint32_t>(Vec4d const x0, uint32_t const n) {
    return pow_template_i<Vec4d>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec4d pow(Vec4d const a, Const_int_t<n>) {
    return pow_n<Vec4d, n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec4d round(Vec4d const a) {
    return _mm256_round_pd(a, 0+8);
}

// function truncate: round towards zero. (result as double vector)
static inline Vec4d truncate(Vec4d const a) {
    return _mm256_round_pd(a, 3+8);
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec4d floor(Vec4d const a) {
    return _mm256_round_pd(a, 1+8);
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec4d ceil(Vec4d const a) {
    return _mm256_round_pd(a, 2+8);
}

// function round_to_int32: round to nearest integer (even). (result as integer vector)
static inline Vec4i round_to_int32(Vec4d const a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm256_cvtpd_epi32(a);
}

// function truncate_to_int32: round towards zero. (result as integer vector)
static inline Vec4i truncate_to_int32(Vec4d const a) {
    return _mm256_cvttpd_epi32(a);
}

#if INSTRSET >= 8  // 256 bit integer vectors are available. AVX2

// function truncatei: round towards zero
static inline Vec4q truncatei(Vec4d const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm256_cvttpd_epi64(a);
#else
    double aa[4];    // inefficient
    a.store(aa);
    return Vec4q(int64_t(aa[0]), int64_t(aa[1]), int64_t(aa[2]), int64_t(aa[3]));
#endif
}

// function roundi: round to nearest or even
static inline Vec4q roundi(Vec4d const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm256_cvtpd_epi64(a);
#else
    return truncatei(round(a));  // inefficient
#endif
}

// function to_double: convert integer vector elements to double vector
static inline Vec4d to_double(Vec4q const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
        return _mm256_maskz_cvtepi64_pd( __mmask16(0xFF), a);
#else
        int64_t aa[4];      // inefficient
        a.store(aa);
        return Vec4d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]));
#endif
}

static inline Vec4d to_double(Vec4uq const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm256_cvtepu64_pd(a);
#else
    uint64_t aa[4];      // inefficient
    a.store(aa);
    return Vec4d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]));
#endif
}

#else  // no 256 bit integer vectors

// function truncatei: round towards zero. (inefficient)
static inline Vec4q truncatei(Vec4d const a) {
    return Vec4q(truncatei(a.get_low()), truncatei(a.get_high()));
}

// function roundi: round to nearest or even. (inefficient)
static inline Vec4q roundi(Vec4d const a) {
    return Vec4q(roundi(a.get_low()), roundi(a.get_high()));
}

// function to_double: convert integer vector elements to double vector
static inline Vec4d to_double(Vec4q const a) {
    return Vec4d(to_double(a.get_low()), to_double(a.get_high()));
}

static inline Vec4d to_double(Vec4uq const a) {
    return Vec4d(to_double(a.get_low()), to_double(a.get_high()));
}

#endif // AVX2


// function to_double: convert integer vector to double vector
static inline Vec4d to_double(Vec4i const a) {
    return _mm256_cvtepi32_pd(a);
}

// function compress: convert two Vec4d to one Vec8f
static inline Vec8f compress (Vec4d const low, Vec4d const high) {
    __m128 t1 = _mm256_cvtpd_ps(low);
    __m128 t2 = _mm256_cvtpd_ps(high);
    return Vec8f(t1, t2);
}

// Function extend_low : convert Vec8f vector elements 0 - 3 to Vec4d
static inline Vec4d extend_low(Vec8f const a) {
    return _mm256_cvtps_pd(_mm256_castps256_ps128(a));
}

// Function extend_high : convert Vec8f vector elements 4 - 7 to Vec4d
static inline Vec4d extend_high (Vec8f const a) {
    return _mm256_cvtps_pd(_mm256_extractf128_ps(a,1));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec4d mul_add(Vec4d const a, Vec4d const b, Vec4d const c) {
#ifdef __FMA__
    return _mm256_fmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_macc_pd(a, b, c);
#else
    return a * b + c;
#endif
    
} 

// Multiply and subtract
static inline Vec4d mul_sub(Vec4d const a, Vec4d const b, Vec4d const c) {
#ifdef __FMA__
    return _mm256_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_pd(a, b, c);
#else
    return a * b - c;
#endif   
}

// Multiply and inverse subtract
static inline Vec4d nmul_add(Vec4d const a, Vec4d const b, Vec4d const c) {
#ifdef __FMA__
    return _mm256_fnmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_nmacc_pd(a, b, c);
#else
    return c - a * b;
#endif
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split.
// This is used in mathematical functions. Do not use it in general code 
// because it is inaccurate in certain cases
static inline Vec4d mul_sub_x(Vec4d const a, Vec4d const b, Vec4d const c) {
#ifdef __FMA__
    return _mm256_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm256_msub_pd(a, b, c);
#else
    // calculate a * b - c with extra precision
    // mask to remove lower 27 bits
    Vec4d upper_mask = _mm256_castps_pd(constant8f<0xF8000000u,0xFFFFFFFFu,0xF8000000u,0xFFFFFFFFu,0xF8000000u,0xFFFFFFFFu,0xF8000000u,0xFFFFFFFFu>());
    Vec4d a_high = a & upper_mask;               // split into high and low parts
    Vec4d b_high = b & upper_mask;
    Vec4d a_low  = a - a_high;
    Vec4d b_low  = b - b_high;
    Vec4d r1 = a_high * b_high;                  // this product is exact
    Vec4d r2 = r1 - c;                           // subtract c from high product
    Vec4d r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec4q exponent(Vec4d const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available
    Vec4uq t1 = _mm256_castpd_si256(a);// reinterpret as 64-bit integer
    Vec4uq t2 = t1 << 1;               // shift out sign bit
    Vec4uq t3 = t2 >> 53;              // shift down logical to position 0
    Vec4q  t4 = Vec4q(t3) - 0x3FF;     // subtract bias from exponent
    return t4;
#else
    return Vec4q(exponent(a.get_low()), exponent(a.get_high()));
#endif
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec4d fraction(Vec4d const a) {
#if INSTRSET >= 10
    return _mm256_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
#elif INSTRSET >= 8 // AVX2. 256 bit integer vectors are available
    Vec4uq t1 = _mm256_castpd_si256(a);   // reinterpret as 64-bit integer
    Vec4uq t2 = Vec4uq((t1 & 0x000FFFFFFFFFFFFF) | 0x3FF0000000000000); // set exponent to 0 + bias
    return _mm256_castsi256_pd(t2);
#else
    return Vec4d(fraction(a.get_low()), fraction(a.get_high()));
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec4d exp2(Vec4q const n) {
#if INSTRSET >= 8  // 256 bit integer vectors are available
    Vec4q t1 = max(n,  -0x3FF);        // limit to allowed range
    Vec4q t2 = min(t1,  0x400);
    Vec4q t3 = t2 + 0x3FF;             // add bias
    Vec4q t4 = t3 << 52;               // put exponent into position 52
    return _mm256_castsi256_pd(t4);    // reinterpret as double
#else
    return Vec4d(exp2(n.get_low()), exp2(n.get_high()));
#endif
}
//static inline Vec4d exp2(Vec4d const x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec4d(-0.0)) gives true, while Vec4d(-0.0) < Vec4d(0.0) gives false
static inline Vec4db sign_bit(Vec4d const a) {
#if INSTRSET >= 8  // 256 bit integer vectors are available, AVX2
    Vec4q t1 = _mm256_castpd_si256(a);    // reinterpret as 64-bit integer
    Vec4q t2 = t1 >> 63;                  // extend sign bit
#if INSTRSET >= 10
    return t2 != 0;
#else
    return _mm256_castsi256_pd(t2);       // reinterpret as 64-bit Boolean
#endif
#else
    return Vec4db(sign_bit(a.get_low()),sign_bit(a.get_high()));
#endif
}

// change signs on vectors Vec4d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3>
inline Vec4d change_sign(Vec4d const a) {
    if ((i0 | i1 | i2 | i3) == 0) return a;
    __m256d mask = _mm256_castps_pd(constant8f <
        0u, (i0 ? 0x80000000u : 0u), 0u, (i1 ? 0x80000000u : 0u), 0u, (i2 ? 0x80000000u : 0u), 0u, (i3 ? 0x80000000u : 0u)> ());
    return _mm256_xor_pd(a, mask);
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

#if INSTRSET >= 8  // AVX2

// ABI version 4 or later needed on Gcc for correct mangling of 256-bit intrinsic vectors.
// If necessary, compile with -fabi-version=0 to get the latest abi version
//#if !defined (GCC_VERSION) || (defined (__GXX_ABI_VERSION) && __GXX_ABI_VERSION >= 1004)  
static inline __m256i reinterpret_i (__m256i const x) {
    return x;
}

static inline __m256i reinterpret_i (__m256  const x) {
    return _mm256_castps_si256(x);
}

static inline __m256i reinterpret_i (__m256d const x) {
    return _mm256_castpd_si256(x);
}

static inline __m256  reinterpret_f (__m256i const x) {
    return _mm256_castsi256_ps(x);
}

static inline __m256  reinterpret_f (__m256  const x) {
    return x;
}

static inline __m256  reinterpret_f (__m256d const x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (__m256i const x) {
    return _mm256_castsi256_pd(x);
}

static inline __m256d reinterpret_d (__m256  const x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (__m256d const x) {
    return x;
}

#else  // AVX2 emulated in vectori256e.h, AVX supported

// ABI version 4 or later needed on Gcc for correct mangling of 256-bit intrinsic vectors.
// If necessary, compile with -fabi-version=0 to get the latest abi version

static inline Vec256b reinterpret_i (__m256  const x) {
    Vec8f xx(x);
    return Vec256b(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline Vec256b reinterpret_i (__m256d const x) {
    Vec4d xx(x);
    return Vec256b(reinterpret_i(xx.get_low()), reinterpret_i(xx.get_high()));
}

static inline __m256  reinterpret_f (__m256  const x) {
    return x;
}

static inline __m256  reinterpret_f (__m256d const x) {
    return _mm256_castpd_ps(x);
}

static inline __m256d reinterpret_d (__m256  const x) {
    return _mm256_castps_pd(x);
}

static inline __m256d reinterpret_d (__m256d const x) {
    return x;
}

static inline Vec256b reinterpret_i (Vec256b const x) {
    return x;
}

static inline __m256  reinterpret_f (Vec256b const x) {
    return Vec8f(Vec4f(reinterpret_f(x.get_low())), Vec4f(reinterpret_f(x.get_high())));
}

static inline __m256d reinterpret_d (Vec256b const x) {
    return Vec4d(Vec2d(reinterpret_d(x.get_low())), Vec2d(reinterpret_d(x.get_high())));
}

#endif  // AVX2

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec8f infinite8f() {
    return reinterpret_f(Vec8i(0x7F800000));
}

// Function nan8f: returns a vector where all elements are +NAN (quiet)
static inline Vec8f nan8f(int n = 0x10) {
    return nan_vec<Vec8f>(n);
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec4d infinite4d() {
    return reinterpret_d(Vec4q(0x7FF0000000000000));
}

// Function nan4d: returns a vector where all elements are +NAN (quiet)
static inline Vec4d nan4d(int n = 0x10) {        
    return nan_vec<Vec4d>(n);
}


/*****************************************************************************
*
*          Vector permute and blend functions
*
******************************************************************************
*
* These permute functions can reorder the elements of a vector and optionally
* set some elements to zero. See Vectori128.h for description
*
*****************************************************************************/

// permute vector Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d permute4(Vec4d const a) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m256d y = a;                                         // result
    constexpr uint64_t flags = perm_flags<Vec4d>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_pd(); // just return zero

    if constexpr ((flags & perm_largeblock) != 0) {        // permute 128-bit blocks
        constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // permutation pattern
        constexpr int j0 = L.a[0];
        constexpr int j1 = L.a[1];
#ifndef ZEXT_MISSING
        if constexpr (j0 == 0 && j1 == -1 && !(flags & perm_addz)) { // zero extend
            return _mm256_zextpd128_pd256(_mm256_castpd256_pd128(y)); 
        }
        if constexpr (j0 == 1 && j1 < 0 && !(flags & perm_addz)) {   // extract upper part, zero extend
            return _mm256_zextpd128_pd256(_mm256_extractf128_pd(y, 1)); 
        }
#endif
        if constexpr ((flags & perm_perm) != 0  && !(flags & perm_zeroing)) {
            return _mm256_permute2f128_pd(y, y, (j0 & 1) | (j1 & 1) << 4);
        }
    } 
    if constexpr ((flags & perm_perm) != 0) {              // permutation needed
        if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm256_unpackhi_pd(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm256_unpacklo_pd(y, y);
            }
            else { // general permute
                constexpr uint8_t mm0 = (i0 & 1) | (i1 & 1) << 1 | (i2 & 1) << 2 | (i3 & 1) << 3;
                y = _mm256_permute_pd(a, mm0);             // select within same lane
            }
        }
#if INSTRSET >= 8  // AVX2 
        else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
            y = _mm256_broadcastsd_pd(_mm256_castpd256_pd128(y)); // broadcast first element
        }
#endif
        else {     // different patterns in two lanes
#if INSTRSET >= 10 // AVX512VL 
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                constexpr uint8_t zm = zero_mask<4>(indexs);
                return _mm256_castsi256_pd(_mm256_maskz_alignr_epi64 (zm, _mm256_castpd_si256(y), _mm256_castpd_si256(y), rot));
            }
#endif
            if constexpr ((flags & perm_cross_lane) == 0){ // no lane crossing
                constexpr uint8_t mm0 = (i0 & 1) | (i1 & 1) << 1 | (i2 & 1) << 2 | (i3 & 1) << 3;
                y = _mm256_permute_pd(a, mm0);             // select within same lane
            }
            else {
#if INSTRSET >= 8  // AVX2 
                // full permute
                constexpr uint8_t mms = (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6;
                y = _mm256_permute4x64_pd(a, mms);
#else
                // permute lanes separately
                __m256d sw = _mm256_permute2f128_pd(a,a,1);// swap the two 128-bit lanes
                constexpr uint8_t mml = (i0 & 1) | (i1 & 1) << 1 | (i2 & 1) << 2 | (i3 & 1) << 3;
                __m256d y1 = _mm256_permute_pd(a, mml);    // select from same lane
                __m256d y2 = _mm256_permute_pd(sw, mml);   // select from opposite lane
                constexpr uint64_t blendm = make_bit_mask<4, 0x101>(indexs);  // blend mask
                y = _mm256_blend_pd(y1, y2, uint8_t(blendm));
#endif
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {           // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_pd(zero_mask<4>(indexs), y);
#else               // use broad mask
        const EList <int64_t, 4> bm = zero_mask_broad<Vec4q>(indexs);
        //y = _mm256_and_pd(_mm256_castsi256_pd( Vec4q().load(bm.a) ), y);  // does not work with INSTRSET = 7
        __m256i bm1 = _mm256_loadu_si256((const __m256i*)(bm.a));
        y = _mm256_and_pd(_mm256_castsi256_pd(bm1), y);

#endif
    }
    return y;
}


// permute vector Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f permute8(Vec8f const a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m256 y = a;                                         // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec8f>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_ps();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // permutation pattern
            y = _mm256_castpd_ps(permute4 <L.a[0], L.a[1], L.a[2], L.a[3]> 
                (Vec4d(_mm256_castps_pd(a))));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm256_unpackhi_ps(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm256_unpacklo_ps(y, y);
            }
            else { // general permute, same pattern in both lanes
                y = _mm256_shuffle_ps(a, a, uint8_t(flags >> perm_ipattern));
            }
        }
#if INSTRSET >= 10
        else if constexpr ((flags & perm_broadcast) != 0) {
            constexpr uint8_t e = flags >> perm_rot_count & 0xF; // broadcast one element
            if constexpr (e > 0) {
                y =  _mm256_castsi256_ps(_mm256_alignr_epi32( _mm256_castps_si256(y),  _mm256_castps_si256(y), e));
            }
            y = _mm256_broadcastss_ps(_mm256_castps256_ps128(y));
        }
#elif INSTRSET >= 8 // AVX2
        else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count == 0)) {
            y = _mm256_broadcastss_ps(_mm256_castps256_ps128(y)); // broadcast first element
        }
#endif
#if INSTRSET >= 8  // avx2
        else if constexpr ((flags & perm_zext) != 0) {  // zero extension      
            y = _mm256_castsi256_ps(_mm256_cvtepu32_epi64(_mm256_castsi256_si128(_mm256_castps_si256(y))));  // zero extension
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#endif
#if INSTRSET >= 10  // AVX512VL 
        else if constexpr ((flags & perm_compress) != 0) {
            y = _mm256_maskz_compress_ps(__mmask8(compress_mask(indexs)), y); // compress
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
        else if constexpr ((flags & perm_expand) != 0) {
            y = _mm256_maskz_expand_ps(__mmask8(expand_mask(indexs)), y); // expand
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#endif
        else {  // different patterns in two lanes
#if INSTRSET >= 10  // AVX512VL 
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                y = _mm256_castsi256_ps(_mm256_alignr_epi32(_mm256_castps_si256(y), _mm256_castps_si256(y), rot));
            }
            else
#endif
            if constexpr ((flags & perm_cross_lane) == 0) {  // no lane crossing. Use vpermilps 
                __m256 m = constant8f<i0 & 3, i1 & 3, i2 & 3, i3 & 3, i4 & 3, i5 & 3, i6 & 3, i7 & 3>();
                y = _mm256_permutevar_ps(a, _mm256_castps_si256(m));
            }
            else {
                // full permute needed
                __m256i permmask = _mm256_castps_si256(
                    constant8f <i0 & 7, i1 & 7, i2 & 7, i3 & 7, i4 & 7, i5 & 7, i6 & 7, i7 & 7 >());
#if INSTRSET >= 8  // AVX2
                y = _mm256_permutevar8x32_ps(a, permmask);
#else           
                // permute lanes separately
                __m256 sw = _mm256_permute2f128_ps(a, a, 1);  // swap the two 128-bit lanes
                __m256 y1 = _mm256_permutevar_ps(a,  permmask);   // select from same lane
                __m256 y2 = _mm256_permutevar_ps(sw, permmask);   // select from opposite lane
                constexpr uint64_t blendm = make_bit_mask<8, 0x102>(indexs);  // blend mask
                y = _mm256_blend_ps(y1, y2, uint8_t(blendm));
#endif
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_ps(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 8> bm = zero_mask_broad<Vec8i>(indexs);
        __m256i bm1 = _mm256_loadu_si256((const __m256i*)(bm.a));
        y = _mm256_and_ps(_mm256_castsi256_ps(bm1), y);
#endif
    }
    return y;
}


// blend vectors Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d blend4(Vec4d const a, Vec4d const b) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m256d y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec4d>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_pd();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute4 <i0, i1, i2, i3> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute4 <i0<0?i0:i0&3, i1<0?i1:i1&3, i2<0?i2:i2&3, i3<0?i3:i3&3> (b);
    } 
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint8_t mb = (uint8_t)make_bit_mask<4, 0x302>(indexs);  // blend mask
#if INSTRSET >= 10 // AVX512VL
        y = _mm256_mask_mov_pd (a, mb, b);
#else  // AVX
        y = _mm256_blend_pd(a, b, mb); // duplicate each bit
#endif        
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 128-bit blocks
        constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // get 128-bit blend pattern
        constexpr uint8_t pp = (L.a[0] & 0xF) | uint8_t(L.a[1] & 0xF) << 4;
        y = _mm256_permute2f128_pd(a, b, pp);
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm256_unpacklo_pd (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm256_unpacklo_pd (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm256_unpackhi_pd(a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm256_unpackhi_pd(b, a);
    }
    else if constexpr ((flags & blend_shufab) != 0) { 
        y = _mm256_shuffle_pd(a, b, (flags >> blend_shufpattern) & 0xF);
    }
    else if constexpr ((flags & blend_shufba) != 0) { 
        y = _mm256_shuffle_pd(b, a, (flags >> blend_shufpattern) & 0xF);
    }
    else { // No special cases
#if INSTRSET >= 10  // AVX512VL. use vpermi2pd
        __m256i const maskp = constant8ui<i0 & 7, 0, i1 & 7, 0, i2 & 7, 0, i3 & 7, 0>();
        return _mm256_maskz_permutex2var_pd (zero_mask<4>(indexs), a, maskp, b);
#else   // permute a and b separately, then blend.
        constexpr EList<int, 8> L = blend_perm_indexes<4, 0>(indexs); // get permutation indexes
        __m256d ya = permute4<L.a[0], L.a[1], L.a[2], L.a[3]>(a);
        __m256d yb = permute4<L.a[4], L.a[5], L.a[6], L.a[7]>(b);
        constexpr uint8_t mb = (uint8_t)make_bit_mask<4, 0x302>(indexs);  // blend mask
        y = _mm256_blend_pd(ya, yb, mb); 
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_pd(zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int64_t, 4> bm = zero_mask_broad<Vec4q>(indexs);
        __m256i bm1 = _mm256_loadu_si256((const __m256i*)(bm.a));
        y = _mm256_and_pd(_mm256_castsi256_pd(bm1), y);
#endif
    }
    return y;
}


// blend vectors Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f blend8(Vec8f const a, Vec8f const b) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m256 y = a;                                          // result
    constexpr uint64_t flags = blend_flags<Vec8f>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_ps();  // just return zero

    if constexpr ((flags & blend_largeblock) != 0) {       // blend and permute 32-bit blocks
        constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // get 32-bit blend pattern
        y = _mm256_castpd_ps(blend4 <L.a[0], L.a[1], L.a[2], L.a[3]> 
            (Vec4d(_mm256_castps_pd(a)), Vec4d(_mm256_castps_pd(b))));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing        
    } 
    else if constexpr ((flags & blend_b) == 0) {           // nothing from b. just permute a
        return permute8 <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }
    else if constexpr ((flags & blend_a) == 0) {           // nothing from a. just permute b
        constexpr EList<int, 16> L = blend_perm_indexes<8, 2>(indexs); // get permutation indexes
        return permute8 < L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] > (b);
    } 
    else if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint8_t mb = (uint8_t)make_bit_mask<8, 0x303>(indexs);  // blend mask
#if INSTRSET >= 10 // AVX512VL
        y = _mm256_mask_mov_ps(a, mb, b);
#else  // AVX2
        y = _mm256_blend_ps(a, b, mb); 
#endif        
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm256_unpacklo_ps(a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm256_unpacklo_ps(b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm256_unpackhi_ps(a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm256_unpackhi_ps(b, a);
    }
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm256_shuffle_ps(a, b, uint8_t(flags >> blend_shufpattern));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm256_shuffle_ps(b, a, uint8_t(flags >> blend_shufpattern));
    }
    else { // No special cases
#if INSTRSET >= 10  // AVX512VL. use vpermi2d
        __m256i const maskp = constant8ui<i0 & 15, i1 & 15, i2 & 15, i3 & 15, i4 & 15, i5 & 15, i6 & 15, i7 & 15> ();
        return _mm256_maskz_permutex2var_ps(zero_mask<8>(indexs), a, maskp, b);
#else   // permute a and b separately, then blend.
        constexpr EList<int, 16> L = blend_perm_indexes<8, 0>(indexs); // get permutation indexes
        __m256 ya = permute8<L.a[0], L.a[1], L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7] >(a);
        __m256 yb = permute8<L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]>(b);
        constexpr uint8_t mb = (uint8_t)make_bit_mask<8, 0x303>(indexs);  // blend mask
        y = _mm256_blend_ps(ya, yb, mb); 
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_ps(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 8> bm = zero_mask_broad<Vec8i>(indexs);
        __m256i bm1 = _mm256_loadu_si256((const __m256i*)(bm.a));
        y = _mm256_and_ps(_mm256_castsi256_ps(bm1), y);
#endif
    }
    return y;
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

static inline Vec8f lookup8(Vec8i const index, Vec8f const table) {
#if INSTRSET >= 8  // AVX2
    return _mm256_permutevar8x32_ps(table, index);

#else // AVX
    // swap low and high part of table
    __m256  sw = _mm256_permute2f128_ps(table, table, 1);  // swap the two 128-bit lanes
    // join index parts
    __m256i index2 = _mm256_insertf128_si256(_mm256_castsi128_si256(index.get_low()), index.get_high(), 1);
    // permute within each 128-bit part
    __m256  r0 = _mm256_permutevar_ps(table, index2);
    __m256  r1 = _mm256_permutevar_ps(sw,    index2);
    // high index bit for blend
    __m128i k1 = _mm_slli_epi32(index.get_high() ^ 4, 29);
    __m128i k0 = _mm_slli_epi32(index.get_low(),      29);
    __m256  kk = _mm256_insertf128_ps(_mm256_castps128_ps256(_mm_castsi128_ps(k0)), _mm_castsi128_ps(k1), 1);
    // blend the two permutes
    return _mm256_blendv_ps(r0, r1, kk);
#endif
}

template <int n>
static inline Vec8f lookup(Vec8i const index, float const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4f table1 = Vec4f().load(table);        
        return Vec8f(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
#if INSTRSET < 8  // not AVX2
    if (n <= 8) {
        return lookup8(index, Vec8f().load(table));
    }
#endif
    // Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), n-1);
    }
#if INSTRSET >= 8  // AVX2
    return _mm256_i32gather_ps(table, index1, 4);
#else // AVX
    return Vec8f(table[index1[0]],table[index1[1]],table[index1[2]],table[index1[3]],
    table[index1[4]],table[index1[5]],table[index1[6]],table[index1[7]]);
#endif
}

static inline Vec4d lookup4(Vec4q const index, Vec4d const table) {
#if INSTRSET >= 10  // AVX512VL
    return _mm256_permutexvar_pd(index, table);

#elif INSTRSET >= 8  // AVX2
    // We can't use VPERMPD because it has constant indexes, vpermilpd can permute only within 128-bit lanes
    // Convert the index to fit VPERMPS
    Vec8i index1 = permute8<0,0,2,2,4,4,6,6> (Vec8i(index+index));
    Vec8i index2 = index1 + Vec8i(constant8ui<0,1,0,1,0,1,0,1>());
    return _mm256_castps_pd(_mm256_permutevar8x32_ps(_mm256_castpd_ps(table), index2));

#else // AVX
    // swap low and high part of table
    __m256d sw = _mm256_permute2f128_pd(table, table, 1);// swap the two 128-bit lanes
    // index << 1
    __m128i index2lo = index.get_low()  + index.get_low();
    __m128i index2hi = index.get_high() + index.get_high();
    // join index parts
    __m256i index3 = _mm256_insertf128_si256(_mm256_castsi128_si256(index2lo), index2hi, 1);
    // permute within each 128-bit part
    __m256d r0 = _mm256_permutevar_pd(table, index3);  // permutevar_pd selects by bit 1 !
    __m256d r1 = _mm256_permutevar_pd(sw,    index3);
    // high index bit for blend
    __m128i k1 = _mm_slli_epi64(index.get_high() ^ 2, 62);
    __m128i k0 = _mm_slli_epi64(index.get_low(),      62);
    __m256d kk = _mm256_insertf128_pd(_mm256_castpd128_pd256(_mm_castsi128_pd(k0)), _mm_castsi128_pd(k1), 1);
    // blend the two permutes
    return _mm256_blendv_pd(r0, r1, kk);
#endif
}


template <int n>
static inline Vec4d lookup(Vec4q const index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 2) {
        Vec2d table1 = Vec2d().load(table);        
        return Vec4d(       
            lookup2 (index.get_low(),  table1),
            lookup2 (index.get_high(), table1));
    }
#if INSTRSET < 8  // not AVX2
    if (n <= 4) {
        return lookup4(index, Vec4d().load(table));
    }
#endif
    // Limit index
    Vec4uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec4uq(index) & Vec4uq(n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec4uq(index), n-1);
    }
#if INSTRSET >= 8  // AVX2
    return _mm256_i64gather_pd(table, index1, 8);
#else // AVX
    Vec4q index2 = Vec4q(index1);
    return Vec4d(table[index2[0]],table[index2[1]],table[index2[2]],table[index2[3]]);
#endif
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3, ..
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f gather8f(void const * a) {
    return reinterpret_f(gather8i<i0, i1, i2, i3, i4, i5, i6, i7>(a));
}

// Load elements from array a with indices i0, i1, i2, i3
template <int i0, int i1, int i2, int i3>
static inline Vec4d gather4d(void const * a) {
    return reinterpret_d(gather4q<i0, i1, i2, i3>(a));
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

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline void scatter(Vec8f const data, float * array) {
#if INSTRSET >= 10 //  __AVX512VL__
    __m256i indx = constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>();
    __mmask8 mask = uint16_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3) | 
        ((i4>=0)<<4) | ((i5>=0)<<5) | ((i6>=0)<<6) | ((i7>=0)<<7));
    _mm256_mask_i32scatter_ps(array, mask, indx, data, 4);
#elif INSTRSET >= 9  //  __AVX512F__
    __m512i indx = _mm512_castsi256_si512(constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>());
    __mmask16 mask = uint16_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3) | 
        ((i4>=0)<<4) | ((i5>=0)<<5) | ((i6>=0)<<6) | ((i7>=0)<<7));
    _mm512_mask_i32scatter_ps(array, mask, indx, _mm512_castps256_ps512(data), 4);
#else
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
#endif
}

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4d const data, double * array) {
#if INSTRSET >= 10 //  __AVX512VL__
    __m128i indx = constant4ui<i0,i1,i2,i3>();
    __mmask8 mask = uint8_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3));
    _mm256_mask_i32scatter_pd(array, mask, indx, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    __m256i indx = _mm256_castsi128_si256(constant4ui<i0,i1,i2,i3>());
    __mmask16 mask = uint16_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3));
    _mm512_mask_i32scatter_pd(array, (__mmask8)mask, indx, _mm512_castpd256_pd512(data), 8);
#else
    const int index[4] = {i0,i1,i2,i3};
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
#endif
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec8i const index, uint32_t limit, Vec8f const data, float * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm256_cmplt_epu32_mask(index, Vec8ui(limit));
    _mm256_mask_i32scatter_ps(destination, mask, index, data, 4);
#elif INSTRSET >= 9  //  __AVX512F__
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xFFu, _mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec8ui(limit)));
    _mm512_mask_i32scatter_ps(destination, mask, _mm512_castsi256_si512(index), _mm512_castps256_ps512(data), 4);
#else
    for (int i = 0; i < 8; i++) {
        if (uint32_t(index[i]) < limit) destination[index[i]] = data[i];
    }
#endif
}

static inline void scatter(Vec4q const index, uint32_t limit, Vec4d const data, double * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm256_cmplt_epu64_mask(index, Vec4uq(uint64_t(limit)));
    _mm256_mask_i64scatter_pd(destination, mask, index, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    __mmask16 mask = _mm512_mask_cmplt_epu64_mask(0xF, _mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec4uq(uint64_t(limit))));
    _mm512_mask_i64scatter_pd(destination, (__mmask8)mask, _mm512_castsi256_si512(index), _mm512_castpd256_pd512(data), 8);
#else
    for (int i = 0; i < 4; i++) {
        if (uint64_t(index[i]) < uint64_t(limit)) destination[index[i]] = data[i];
    }
#endif
} 

static inline void scatter(Vec4i const index, uint32_t limit, Vec4d const data, double * destination) {
#if INSTRSET >= 10   //  __AVX512VL__
    __mmask8 mask = _mm_cmplt_epu32_mask(index, Vec4ui(limit));
    _mm256_mask_i32scatter_pd(destination, mask, index, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xF, _mm512_castsi128_si512(index), _mm512_castsi128_si512(Vec4ui(limit)));
    _mm512_mask_i32scatter_pd(destination, (__mmask8)mask, _mm256_castsi128_si256(index), _mm512_castpd256_pd512(data), 8);
#else
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) destination[index[i]] = data[i];
    }
#endif
} 


#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORF256_H
