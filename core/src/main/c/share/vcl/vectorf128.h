/****************************  vectorf128.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-23
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 128-bit floating point vector classes
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec4f     Vector of 4 single precision floating point numbers
* Vec4fb    Vector of 4 Booleans for use with Vec4f
* Vec2d     Vector of 2 double precision floating point numbers
* Vec2db    Vector of 2 Booleans for use with Vec2d
*
* Each vector object is represented internally in the CPU as a 128-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORF128_H
#define VECTORF128_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

/*****************************************************************************
*
*          select functions
*
*****************************************************************************/
// Select between two __m128 sources, element by element, with broad boolean vector.
// Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFF (true). 
// No other values are allowed for broad boolean vectors. 
// The implementation depends on the instruction set: 
// If SSE4.1 is supported then only bit 31 in each dword of s is checked, 
// otherwise all bits in s are used.
static inline __m128 selectf(__m128 const s, __m128 const a, __m128 const b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_blendv_ps(b, a, s);
#else
    return _mm_or_ps(
        _mm_and_ps(s, a),
        _mm_andnot_ps(s, b));
#endif
}

// Same, with two __m128d sources.
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). No other 
// No other values are allowed for broad boolean vectors. 
// The implementation depends on the instruction set: 
// If SSE4.1 is supported then only bit 63 in each dword of s is checked, 
// otherwise all bits in s are used.
static inline __m128d selectd(__m128d const s, __m128d const a, __m128d const b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_blendv_pd(b, a, s);
#else
    return _mm_or_pd(
        _mm_and_pd(s, a),
        _mm_andnot_pd(s, b));
#endif
}


/*****************************************************************************
*
*          Vec4fb: Vector of 4 Booleans for use with Vec4f
*
*****************************************************************************/

#if INSTRSET < 10 // broad boolean vectors

class Vec4fb {
protected:
    __m128 xmm; // Float vector
public:
    // Default constructor:
    Vec4fb() {
    }
    // Constructor to build from all elements:
    Vec4fb(bool b0, bool b1, bool b2, bool b3) {
        xmm = _mm_castsi128_ps(_mm_setr_epi32(-(int)b0, -(int)b1, -(int)b2, -(int)b3));
    }
    // Constructor to convert from type __m128 used in intrinsics:
    Vec4fb(__m128 const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128 used in intrinsics:
    Vec4fb & operator = (__m128 const x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec4fb(bool b) {
        xmm = _mm_castsi128_ps(_mm_set1_epi32(-int32_t(b)));
    }
    // Assignment operator to broadcast scalar value:
    Vec4fb & operator = (bool b) {
        *this = Vec4fb(b);
        return *this;
    }
    // Constructor to convert from type Vec4ib used as Boolean for integer vectors
    Vec4fb(Vec4ib const x) {
        xmm = _mm_castsi128_ps(x);
    }
    // Assignment operator to convert from type Vec4ib used as Boolean for integer vectors
    Vec4fb & operator = (Vec4ib const x) {
        xmm = _mm_castsi128_ps(x);
        return *this;
    }
    // Type cast operator to convert to __m128 used in intrinsics
    operator __m128() const {
        return xmm;
    }
    /* Clang problem:
    The Clang compiler treats the intrinsic vector types __m128, __m128i, and __m128f as identical.
    I have reported this problem in 2013 but it is still not fixed in 2019!
    See the bug report at https://bugs.llvm.org/show_bug.cgi?id=17164
    Additional problem: The version number is not consistent across platforms. The Apple build has
    different version numbers. We have to rely on __apple_build_version__ on the Mac platform:
    http://llvm.org/bugs/show_bug.cgi?id=12643
    I have received reports that there was no aliasing of vector types on __apple_build_version__ = 6020053
    but apparently the problem has come back. The aliasing of vector types has been reported on
    __apple_build_version__ = 8000042
    We have to make switches here when - hopefully - the error some day has been fixed.
    We need different version checks with and whithout __apple_build_version__
    */
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY  
    // Type cast operator to convert to type Vec4ib used as Boolean for integer vectors
    operator Vec4ib() const {
        return _mm_castps_si128(xmm);
    }
#endif
    // Member function to change a single element in vector
    Vec4fb const insert(int index, bool value) {
        const int32_t maskl[8] = { 0,0,0,0,-1,0,0,0 };
        __m128 mask = _mm_loadu_ps((float const*)(maskl + 4 - (index & 3))); // mask with FFFFFFFF at index position
        if (value) {
            xmm = _mm_or_ps(xmm, mask);
        }
        else {
            xmm = _mm_andnot_ps(mask, xmm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec4ib(_mm_castps_si128(xmm)).extract(index);
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }

    // Member function to change a bitfield to a boolean vector
    Vec4fb & load_bits(uint8_t a) {
        Vec4ib b;  b.load_bits(a);
        xmm = _mm_castsi128_ps(b);
        return *this;
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec4fb(int b) = delete;
    Vec4fb & operator = (int x) = delete;
};

#else

typedef Vec4b Vec4fb;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Operators for Vec4fb
*
*****************************************************************************/

#if INSTRSET < 10 // broad boolean vectors

// vector operator & : bitwise and
static inline Vec4fb operator & (Vec4fb const a, Vec4fb const b) {
    return _mm_and_ps(a, b);
}
static inline Vec4fb operator && (Vec4fb const a, Vec4fb const b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec4fb & operator &= (Vec4fb & a, Vec4fb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4fb operator | (Vec4fb const a, Vec4fb const b) {
    return _mm_or_ps(a, b);
}
static inline Vec4fb operator || (Vec4fb const a, Vec4fb const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec4fb & operator |= (Vec4fb & a, Vec4fb const b) {
    a = a | b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4fb operator ~ (Vec4fb const a) {
    return _mm_xor_ps(a, _mm_castsi128_ps(_mm_set1_epi32(-1)));
}

// vector operator ^ : bitwise xor
static inline Vec4fb operator ^ (Vec4fb const a, Vec4fb const b) {
    return _mm_xor_ps(a, b);
}

// vector operator == : xnor
static inline Vec4fb operator == (Vec4fb const a, Vec4fb const b) {
    return Vec4fb(a ^ Vec4fb(~b));
}

// vector operator != : xor
static inline Vec4fb operator != (Vec4fb const a, Vec4fb const b) {
    return Vec4fb(a ^ b);
}

// vector operator ^= : bitwise xor
static inline Vec4fb & operator ^= (Vec4fb & a, Vec4fb const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not all bits in an element are the same)
static inline Vec4fb operator ! (Vec4fb const a) {
    return Vec4fb(!Vec4ib(a));
}

// Functions for Vec4fb

// andnot: a & ~ b
static inline Vec4fb andnot(Vec4fb const a, Vec4fb const b) {
    return _mm_andnot_ps(b, a);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and(Vec4fb const a) {
    return _mm_movemask_ps(a) == 0x0F;
    //return horizontal_and(Vec128b(_mm_castps_si128(a)));
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or(Vec4fb const a) {
    return _mm_movemask_ps(a) != 0;
    //return horizontal_or(Vec128b(_mm_castps_si128(a)));
}

#endif


/*****************************************************************************
*
*          Vec2db: Vector of 2 Booleans for use with Vec2d
*
*****************************************************************************/

#if INSTRSET < 10 // broad boolean vectors

class Vec2db {
protected:
    __m128d xmm; // Double vector
public:
    // Default constructor:
    Vec2db() {
    }
    // Constructor to broadcast scalar value:
    Vec2db(bool b) {
        xmm = _mm_castsi128_pd(_mm_set1_epi32(-int32_t(b)));
    }
    // Constructor to build from all elements:
    Vec2db(bool b0, bool b1) {
        xmm = _mm_castsi128_pd(_mm_setr_epi32(-(int)b0, -(int)b0, -(int)b1, -(int)b1));
    }
    // Constructor to convert from type __m128d used in intrinsics:
    Vec2db(__m128d const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128d used in intrinsics:
    Vec2db & operator = (__m128d const x) {
        xmm = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec2db & operator = (bool b) {
        *this = Vec2db(b);
        return *this;
    }
    // Constructor to convert from type Vec2qb used as Boolean for integer vectors
    Vec2db(Vec2qb const x) {
        xmm = _mm_castsi128_pd(x);
    }
    // Assignment operator to convert from type Vec2qb used as Boolean for integer vectors
    Vec2db & operator = (Vec2qb const x) {
        xmm = _mm_castsi128_pd(x);
        return *this;
    }
    // Type cast operator to convert to __m128d used in intrinsics
    operator __m128d() const {
        return xmm;
    }
#ifndef FIX_CLANG_VECTOR_ALIAS_AMBIGUITY
    // Type cast operator to convert to type Vec2qb used as Boolean for integer vectors
    operator Vec2qb() const {
        return _mm_castpd_si128(xmm);
    }
#endif
    // Member function to change a single element in vector
    Vec2db const insert(int index, bool value) {
        const int32_t maskl[8] = { 0,0,0,0,-1,-1,0,0 };
        __m128 mask = _mm_loadu_ps((float const*)(maskl + 4 - (index & 1) * 2)); // mask with FFFFFFFFFFFFFFFF at index position
        if (value) {
            xmm = _mm_or_pd(xmm, _mm_castps_pd(mask));
        }
        else {
            xmm = _mm_andnot_pd(_mm_castps_pd(mask), xmm);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec2qb(_mm_castpd_si128(xmm)).extract(index);
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec2db & load_bits(uint8_t a) {
        Vec2qb b; b.load_bits(a);
        xmm = _mm_castsi128_pd(b);
        return *this;
    }
    static constexpr int size() {
        return 2;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec2db(int b) = delete;
    Vec2db & operator = (int x) = delete;
};

#else

typedef Vec2b Vec2db;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Operators for Vec2db
*
*****************************************************************************/

#if INSTRSET < 10 // broad boolean vectors

// vector operator & : bitwise and
static inline Vec2db operator & (Vec2db const a, Vec2db const b) {
    return _mm_and_pd(a, b);
}
static inline Vec2db operator && (Vec2db const a, Vec2db const b) {
    return a & b;
}

// vector operator &= : bitwise and
static inline Vec2db & operator &= (Vec2db & a, Vec2db const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec2db operator | (Vec2db const a, Vec2db const b) {
    return _mm_or_pd(a, b);
}
static inline Vec2db operator || (Vec2db const a, Vec2db const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec2db & operator |= (Vec2db & a, Vec2db const b) {
    a = a | b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec2db operator ~ (Vec2db const a) {
    return _mm_xor_pd(a, _mm_castsi128_pd(_mm_set1_epi32(-1)));
}

// vector operator ^ : bitwise xor
static inline Vec2db operator ^ (Vec2db const a, Vec2db const b) {
    return _mm_xor_pd(a, b);
}

// vector operator == : xnor
static inline Vec2db operator == (Vec2db const a, Vec2db const b) {
    return Vec2db(a ^ Vec2db(~b));
}

// vector operator != : xor
static inline Vec2db operator != (Vec2db const a, Vec2db const b) {
    return Vec2db(a ^ b);
}

// vector operator ^= : bitwise xor
static inline Vec2db & operator ^= (Vec2db & a, Vec2db const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not all bits in an element are the same)
static inline Vec2db operator ! (Vec2db const a) {
    return Vec2db(!Vec2qb(a));
}

// Functions for Vec2db

// andnot: a & ~ b
static inline Vec2db andnot(Vec2db const a, Vec2db const b) {
    return _mm_andnot_pd(b, a);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and(Vec2db const a) {
    return _mm_movemask_pd(a) == 3;
    //return horizontal_and(Vec128b(_mm_castpd_si128(a)));
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or(Vec2db const a) {
    return _mm_movemask_pd(a) != 0;
    //return horizontal_or(Vec128b(_mm_castpd_si128(a)));
}

#endif


/*****************************************************************************
*
*          Vec4f: Vector of 4 single precision floating point values
*
*****************************************************************************/

class Vec4f {
protected:
    __m128 xmm; // Float vector
public:
    // Default constructor:
    Vec4f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4f(float f) {
        xmm = _mm_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec4f(float f0, float f1, float f2, float f3) {
        xmm = _mm_setr_ps(f0, f1, f2, f3);
    }
    // Constructor to convert from type __m128 used in intrinsics:
    Vec4f(__m128 const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128 used in intrinsics:
    Vec4f & operator = (__m128 const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128 used in intrinsics
    operator __m128() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec4f & load(float const * p) {
        xmm = _mm_loadu_ps(p);
        return *this;
    }
    // Member function to load from array, aligned by 16
    // "load_a" is faster than "load" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 16.
    Vec4f & load_a(float const * p) {
        xmm = _mm_load_ps(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm_storeu_ps(p, xmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(float * p) const {
        _mm_stream_ps(p, xmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 16
    // "store_a" is faster than "store" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 16.
    void store_a(float * p) const {
        _mm_store_ps(p, xmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4f & load_partial(int n, float const * p) {
#if INSTRSET >= 10  // AVX512VL
        xmm = _mm_maskz_loadu_ps(__mmask8((1u << n) - 1), p);
#else 
        __m128 t1, t2;
        switch (n) {
        case 1:
            xmm = _mm_load_ss(p); break;
        case 2:
            xmm = _mm_castpd_ps(_mm_load_sd((double const*)p)); break;
        case 3:
            t1 = _mm_castpd_ps(_mm_load_sd((double const*)p));
            t2 = _mm_load_ss(p + 2);
            xmm = _mm_movelh_ps(t1, t2); break;
        case 4:
            load(p); break;
        default:
            xmm = _mm_setzero_ps();
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm_mask_storeu_ps(p, __mmask8((1u << n) - 1), xmm);
#else 
        __m128 t1;
        switch (n) {
        case 1:
            _mm_store_ss(p, xmm); break;
        case 2:
            _mm_store_sd((double*)p, _mm_castps_pd(xmm)); break;
        case 3:
            _mm_store_sd((double*)p, _mm_castps_pd(xmm));
            t1 = _mm_movehl_ps(xmm, xmm);
            _mm_store_ss(p + 2, t1); break;
        case 4:
            store(p); break;
        default:;
        }
#endif
    }
    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec4f & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_ps(__mmask8((1u << n) - 1), xmm);
#else 
        if (uint32_t(n) >= 4) return *this;
        const union {
            int32_t i[8];
            float   f[8];
        } mask = { {1,-1,-1,-1,0,0,0,0} };
        xmm = _mm_and_ps(xmm, Vec4f().load(mask.f + 4 - n));
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec4f const insert(int index, float value) {
#if INSTRSET >= 10   // AVX512VL         
        xmm = _mm_mask_broadcastss_ps(xmm, __mmask8(1u << index), _mm_set_ss(value));
#elif INSTRSET >= 5   // SSE4.1
        switch (index & 3) {
        case 0:
            xmm = _mm_insert_ps(xmm, _mm_set_ss(value), 0 << 4);  break;
        case 1:
            xmm = _mm_insert_ps(xmm, _mm_set_ss(value), 1 << 4);  break;
        case 2:
            xmm = _mm_insert_ps(xmm, _mm_set_ss(value), 2 << 4);  break;
        default:
            xmm = _mm_insert_ps(xmm, _mm_set_ss(value), 3 << 4);  break;
        }
#else
        const int32_t maskl[8] = { 0,0,0,0,-1,0,0,0 };
        __m128 broad = _mm_set1_ps(value);  // broadcast value into all elements
        __m128 mask = _mm_loadu_ps((float const*)(maskl + 4 - (index & 3))); // mask with FFFFFFFF at index position
        xmm = selectf(mask, broad, xmm);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    float extract(int index) const {
#if INSTRSET >= 10
        __m128 x = _mm_maskz_compress_ps(__mmask8(1u << index), xmm);
        return _mm_cvtss_f32(x);
#else 
        float x[4];
        store(x);
        return x[index & 3];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 16;
    }
    typedef __m128 registertype;
};


/*****************************************************************************
*
*          Operators for Vec4f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4f operator + (Vec4f const a, Vec4f const b) {
    return _mm_add_ps(a, b);
}

// vector operator + : add vector and scalar
static inline Vec4f operator + (Vec4f const a, float b) {
    return a + Vec4f(b);
}
static inline Vec4f operator + (float a, Vec4f const b) {
    return Vec4f(a) + b;
}

// vector operator += : add
static inline Vec4f & operator += (Vec4f & a, Vec4f const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4f operator ++ (Vec4f & a, int) {
    Vec4f a0 = a;
    a = a + 1.0f;
    return a0;
}

// prefix operator ++
static inline Vec4f & operator ++ (Vec4f & a) {
    a = a + 1.0f;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4f operator - (Vec4f const a, Vec4f const b) {
    return _mm_sub_ps(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec4f operator - (Vec4f const a, float b) {
    return a - Vec4f(b);
}
static inline Vec4f operator - (float a, Vec4f const b) {
    return Vec4f(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec4f operator - (Vec4f const a) {
    return _mm_xor_ps(a, _mm_castsi128_ps(_mm_set1_epi32(0x80000000)));
}

// vector operator -= : subtract
static inline Vec4f & operator -= (Vec4f & a, Vec4f const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4f operator -- (Vec4f & a, int) {
    Vec4f a0 = a;
    a = a - 1.0f;
    return a0;
}

// prefix operator --
static inline Vec4f & operator -- (Vec4f & a) {
    a = a - 1.0f;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4f operator * (Vec4f const a, Vec4f const b) {
    return _mm_mul_ps(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec4f operator * (Vec4f const a, float b) {
    return a * Vec4f(b);
}
static inline Vec4f operator * (float a, Vec4f const b) {
    return Vec4f(a) * b;
}

// vector operator *= : multiply
static inline Vec4f & operator *= (Vec4f & a, Vec4f const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec4f operator / (Vec4f const a, Vec4f const b) {
    return _mm_div_ps(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec4f operator / (Vec4f const a, float b) {
    return a / Vec4f(b);
}
static inline Vec4f operator / (float a, Vec4f const b) {
    return Vec4f(a) / b;
}

// vector operator /= : divide
static inline Vec4f & operator /= (Vec4f & a, Vec4f const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4fb operator == (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 0);
#else
    return _mm_cmpeq_ps(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec4fb operator != (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 4);
#else
    return _mm_cmpneq_ps(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec4fb operator < (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 1);
#else
    return _mm_cmplt_ps(a, b);
#endif 
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec4fb operator <= (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 2);
#else
    return _mm_cmple_ps(a, b);
#endif 
}

// vector operator > : returns true for elements for which a > b
static inline Vec4fb operator > (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 6);
#else
    return b < a;
#endif 
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec4fb operator >= (Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_ps_mask(a, b, 5);
#else
    return b <= a;
#endif 
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec4f operator & (Vec4f const a, Vec4f const b) {
    return _mm_and_ps(a, b);
}

// vector operator &= : bitwise and
static inline Vec4f & operator &= (Vec4f & a, Vec4f const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec4f and Vec4fb
static inline Vec4f operator & (Vec4f const a, Vec4fb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_maskz_mov_ps(b, a);
#else
    return _mm_and_ps(a, b);
#endif
}
static inline Vec4f operator & (Vec4fb const a, Vec4f const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec4f operator | (Vec4f const a, Vec4f const b) {
    return _mm_or_ps(a, b);
}

// vector operator |= : bitwise or
static inline Vec4f & operator |= (Vec4f & a, Vec4f const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4f operator ^ (Vec4f const a, Vec4f const b) {
    return _mm_xor_ps(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec4f & operator ^= (Vec4f & a, Vec4f const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec4fb operator ! (Vec4f const a) {
    return a == Vec4f(0.0f);
}


/*****************************************************************************
*
*          Functions for Vec4f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec4f select(Vec4fb const s, Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_ps(b, s, a);
#else
    return selectf(s, a, b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4f if_add(Vec4fb const f, Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10
    return _mm_mask_add_ps (a, f, a, b);
#else
    return a + (Vec4f(f) & b);
#endif
}

// Conditional subtract: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec4f if_sub(Vec4fb const f, Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10
    return _mm_mask_sub_ps (a, f, a, b);
#else
    return a - (Vec4f(f) & b);
#endif
}

// Conditional multiply: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec4f if_mul(Vec4fb const f, Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10
    return _mm_mask_mul_ps (a, f, a, b);
#else
    return a * select(f, b, 1.f);
#endif
}

// Conditional divide: For all vector elements i: result[i] = f[i] ? (a[i] / b[i]) : a[i]
static inline Vec4f if_div(Vec4fb const f, Vec4f const a, Vec4f const b) {
#if INSTRSET >= 10
    return _mm_mask_div_ps (a, f, a, b);
#else
    return a / select(f, b, 1.f);
#endif
}

// Sign functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec4f(-0.0f)) gives true, while Vec4f(-0.0f) < Vec4f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec4fb sign_bit(Vec4f const a) {
    Vec4i t1 = _mm_castps_si128(a);    // reinterpret as 32-bit integer
    Vec4i t2 = t1 >> 31;               // extend sign bit
#if INSTRSET >= 10
    return t2 != 0;
#else
    return _mm_castsi128_ps(t2);       // reinterpret as 32-bit Boolean
#endif
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec4f sign_combine(Vec4f const a, Vec4f const b) {
#if INSTRSET < 10
    return a ^ (b & Vec4f(-0.0f));
#else
    return _mm_castsi128_ps (_mm_ternarylogic_epi32(
        _mm_castps_si128(a), _mm_castps_si128(b), Vec4i(0x80000000), 0x78));
#endif
}

// Categorization functions

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec4fb is_finite(Vec4f const a) {
#if INSTRSET >= 10
    return __mmask8(_mm_fpclass_ps_mask(a, 0x99) ^ 0x0F);
#else
    Vec4i t1 = _mm_castps_si128(a);    // reinterpret as 32-bit integer
    Vec4i t2 = t1 << 1;                // shift out sign bit
    Vec4i t3 = Vec4i(t2 & 0xFF000000) != 0xFF000000; // exponent field is not all 1s
    return Vec4ib(t3);
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec4fb is_inf(Vec4f const a) {
#if INSTRSET >= 10
    return __mmask8(_mm_fpclass_ps_mask(a, 0x18));
#else
    Vec4i t1 = _mm_castps_si128(a);    // reinterpret as 32-bit integer
    Vec4i t2 = t1 << 1;                // shift out sign bit
    return t2 == Vec4i(0xFF000000);    // exponent is all 1s, fraction is 0
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec4fb is_nan(Vec4f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return Vec4fb(_mm_fpclass_ps_mask(a, 0x81));
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) 
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec4fb is_nan(Vec4f const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
static inline Vec4fb is_nan(Vec4f const a) {
    __m128 aa = a;
    __m128i unordered;
    __asm volatile("vcmpps $3,  %1, %1, %0" : "=x" (unordered) :  "x" (aa) );
    return Vec4fb(unordered);
}
#else
static inline Vec4fb is_nan(Vec4f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm_cmp_ps(a, a, 3); // compare unordered
    // return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec4fb is_subnormal(Vec4f const a) {
#if INSTRSET >= 10
    return Vec4fb(_mm_fpclass_ps_mask(a, 0x20));
#else
    Vec4i t1 = _mm_castps_si128(a);              // reinterpret as 32-bit integer
    Vec4i t2 = t1 << 1;                          // shift out sign bit
    Vec4i t3 = 0xFF000000;                       // exponent mask
    Vec4i t4 = t2 & t3;                          // exponent
    Vec4i t5 = _mm_andnot_si128(t3, t2);         // fraction
    return Vec4ib((t4 == 0) & (t5 != 0));        // exponent = 0 and fraction != 0
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec4fb is_zero_or_subnormal(Vec4f const a) {
#if INSTRSET >= 10
    return Vec4fb(_mm_fpclass_ps_mask(a, 0x26));
#else
    Vec4i t = _mm_castps_si128(a);     // reinterpret as 32-bit integer
    t &= 0x7F800000;                   // isolate exponent
    return t == 0;                     // exponent = 0
#endif
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec4f infinite4f() {
    return _mm_castsi128_ps(_mm_set1_epi32(0x7F800000));
}

// Function nan4f: returns a vector where all elements are NAN (quiet)
static inline Vec4f nan4f(int n = 0x10) {
    return nan_vec<Vec4f>(n);
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add(Vec4f const a) {
#if  INSTRSET >= 3 && false // SSE3
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128 t1 = _mm_hadd_ps(a, a);
    __m128 t2 = _mm_hadd_ps(t1, t1);
    return _mm_cvtss_f32(t2);
#else
    __m128 t1 = _mm_movehl_ps(a, a);
    __m128 t2 = _mm_add_ps(a, t1);
    __m128 t3 = _mm_shuffle_ps(t2, t2, 1);
    __m128 t4 = _mm_add_ss(t2, t3);
    return _mm_cvtss_f32(t4);
#endif
}

// function max: a > b ? a : b
static inline Vec4f max(Vec4f const a, Vec4f const b) {
    return _mm_max_ps(a, b);
}

// function min: a < b ? a : b
static inline Vec4f min(Vec4f const a, Vec4f const b) {
    return _mm_min_ps(a, b);
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec4f abs(Vec4f const a) {
#if INSTRSET >= 10  // AVX512VL
    return _mm_range_ps(a, a, 8);
#else
    __m128 mask = _mm_castsi128_ps(_mm_set1_epi32(0x7FFFFFFF));
    return _mm_and_ps(a, mask);
#endif
}

// function sqrt: square root
static inline Vec4f sqrt(Vec4f const a) {
    return _mm_sqrt_ps(a);
}

// function square: a * a
static inline Vec4f square(Vec4f const a) {
    return a * a;
}

// pow(vector,int) function template
template <typename VTYPE>
static inline VTYPE pow_template_i(VTYPE const x0, int n) {
    VTYPE x = x0;                      // a^(2^i)
    VTYPE y(1.0f);                     // accumulator
    if (n >= 0) {                      // make sure n is not negative
        while (true) {                 // loop for each bit in n
            if (n & 1) y *= x;         // multiply if bit = 1
            n >>= 1;                   // get next bit of n
            if (n == 0) return y;      // finished
            x *= x;                    // x = a^2, a^4, a^8, etc.
        }
    }
    else {
        // n < 0
        if (uint32_t(n) == 0x80000000u) return nan_vec<VTYPE>();  // integer overflow
        return VTYPE(1.0f) / pow_template_i<VTYPE>(x0, -n);       // reciprocal
    }
}

// The purpose of this template is to prevent implicit conversion of a float
// exponent to int when calling pow(vector, float) and vectormath_exp.h is not included 
template <typename TT> static Vec4f pow(Vec4f const a, TT const n);  // = delete

// Raise floating point numbers to integer power n
template <>
inline Vec4f pow<int>(Vec4f const x0, int const n) {
    return pow_template_i<Vec4f>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec4f pow<uint32_t>(Vec4f const x0, uint32_t const n) {
    return pow_template_i<Vec4f>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant

// gcc can optimize pow_template_i to generate the same as the code below. MS and Clang can not.
// Therefore, this code is kept
// to do: test on Intel compiler
template <typename V, int n>
static inline V pow_n(V const a) {
    if (n == 0x80000000) return nan_vec<V>();    // integer overflow
    if (n < 0)    return V(1.0f) / pow_n<V, -n>(a);
    if (n == 0)   return V(1.0f);
    if (n >= 256) return pow(a, n);
    V x = a;                                     // a^(2^i)
    V y;                                         // accumulator
    const int lowest = n - (n & (n - 1));        // lowest set bit in n
    if (n & 1) y = x;
    if (n < 2) return y;
    x = x * x;                                   // x^2
    if (n & 2) {
        if (lowest == 2) y = x; else y *= x;
    }
    if (n < 4) return y;
    x = x * x;                                   // x^4
    if (n & 4) {
        if (lowest == 4) y = x; else y *= x;
    }
    if (n < 8) return y;
    x = x * x;                                   // x^8
    if (n & 8) {
        if (lowest == 8) y = x; else y *= x;
    }
    if (n < 16) return y;
    x = x * x;                                   // x^16
    if (n & 16) {
        if (lowest == 16) y = x; else y *= x;
    }
    if (n < 32) return y;
    x = x * x;                                   // x^32
    if (n & 32) {
        if (lowest == 32) y = x; else y *= x;
    }
    if (n < 64) return y;
    x = x * x;                                   // x^64
    if (n & 64) {
        if (lowest == 64) y = x; else y *= x;
    }
    if (n < 128) return y;
    x = x * x;                                   // x^128
    if (n & 128) {
        if (lowest == 128) y = x; else y *= x;
    }
    return y;
}

// implement as function pow(vector, const_int)
template <int n>
static inline Vec4f pow(Vec4f const a, Const_int_t<n>) {
    return pow_n<Vec4f, n>(a);
}

// implement the same as macro pow_const(vector, int)
//#define pow_const(x,n) pow_n<n>(x)
#define pow_const(x,n) pow(x,Const_int_t<n>())

static inline Vec4f round(Vec4f const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_ps(a, 8);
#else  // SSE2
    Vec4i y1 = _mm_cvtps_epi32(a);           // convert to integer
    Vec4f y2 = _mm_cvtepi32_ps(y1);          // convert back to float
#ifdef SIGNED_ZERO
    y2 |= (a & Vec4f(-0.0f));                // sign of zero
#endif
    return select(y1 != 0x80000000, y2, a);  // use original value if integer overflows
#endif
}

// function truncate: round towards zero. (result as float vector)
static inline Vec4f truncate(Vec4f const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_ps(a, 3 + 8);
#else  // SSE2
    Vec4i y1 = _mm_cvttps_epi32(a);          // truncate to integer
    Vec4f y2 = _mm_cvtepi32_ps(y1);          // convert back to float
#ifdef SIGNED_ZERO
    y2 |= (a & Vec4f(-0.0f));                // sign of zero
#endif
    return select(y1 != 0x80000000, y2, a);  // use original value if integer overflows
#endif
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec4f floor(Vec4f const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_ps(a, 1 + 8);
#else  // SSE2
    Vec4f y = round(a);                      // round
    y -= Vec4f(1.f) & (y > a);               // subtract 1 if bigger
#ifdef SIGNED_ZERO
    y |= (a & Vec4f(-0.0f));                 // sign of zero
#endif
    return y;
#endif
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec4f ceil(Vec4f const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_ps(a, 2 + 8);
#else  // SSE2
    Vec4f y = round(a);                      // round
    y += Vec4f(1.f) & (y < a);               // add 1 if bigger
#ifdef SIGNED_ZERO
    y |= (a & Vec4f(-0.0f));                 // sign of zero
#endif
    return y;
#endif
}

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec4i roundi(Vec4f const a) {
    // Note: assume MXCSR control register is set to rounding
    return _mm_cvtps_epi32(a);
}
//static inline Vec4i round_to_int(Vec4f const a) { return roundi(a); } // deprecated

// function truncatei: round towards zero. (result as integer vector)
static inline Vec4i truncatei(Vec4f const a) {
    return _mm_cvttps_epi32(a);
}
//static inline Vec4i truncate_to_int(Vec4f const a) { return truncatei(a); } // deprecated

// function to_float: convert integer vector to float vector
static inline Vec4f to_float(Vec4i const a) {
    return _mm_cvtepi32_ps(a);
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec4f to_float(Vec4ui const a) {
#if INSTRSET >= 10 && (!defined(_MSC_VER) || defined(__INTEL_COMPILER)) // _mm_cvtepu32_ps missing in MS VS2019
    return _mm_cvtepu32_ps(a);
#elif INSTRSET >= 9  // __AVX512F__
    return _mm512_castps512_ps128(_mm512_cvtepu32_ps(_mm512_castsi128_si512(a)));
#else
    Vec4f b = to_float(Vec4i(a & 0xFFFFF));             // 20 bits
    Vec4f c = to_float(Vec4i(a >> 20));                 // remaining bits
    Vec4f d = b + c * 1048576.f;  // 2^20
    return d;
#endif
}

// Approximate math functions

// approximate reciprocal (Faster than 1.f / a. relative accuracy better than 2^-11)
static inline Vec4f approx_recipr(Vec4f const a) {
#ifdef __AVX512ER__  // AVX512ER: full precision
    // todo: if future processors have both AVX512ER and AVX512VL: _mm128_rcp28_round_ps(a, _MM_FROUND_NO_EXC);
    return _mm512_castps512_ps128(_mm512_rcp28_round_ps(_mm512_castps128_ps512(a), _MM_FROUND_NO_EXC));
#elif INSTRSET >= 10   // AVX512VL: 14 bit precision
    return _mm_rcp14_ps(a);
#elif INSTRSET >= 9    // AVX512F: 14 bit precision
    return _mm512_castps512_ps128(_mm512_rcp14_ps(_mm512_castps128_ps512(a)));
#else  // AVX: 11 bit precision
    return _mm_rcp_ps(a);
#endif
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a). Relative accuracy better than 2^-11)
static inline Vec4f approx_rsqrt(Vec4f const a) {
    // use more accurate version if available. (none of these will raise exceptions on zero)
#ifdef __AVX512ER__  // AVX512ER: full precision
    // todo: if future processors have both AVX512ER and AVX521VL: _mm128_rsqrt28_round_ps(a, _MM_FROUND_NO_EXC);
    return _mm512_castps512_ps128(_mm512_rsqrt28_round_ps(_mm512_castps128_ps512(a), _MM_FROUND_NO_EXC));
#elif INSTRSET >= 10 && !defined(_MSC_VER)  // missing in VS2019
    return _mm_rsqrt14_ps(a);
#elif INSTRSET >= 9  // AVX512F: 14 bit precision
    return _mm512_castps512_ps128(_mm512_rsqrt14_ps(_mm512_castps128_ps512(a)));
#else  // SSE: 11 bit precision
    return _mm_rsqrt_ps(a);
#endif
}

// Fused multiply and add functions

// Multiply and add
static inline Vec4f mul_add(Vec4f const a, Vec4f const b, Vec4f const c) {
#ifdef __FMA__
    return _mm_fmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm_macc_ps(a, b, c);
#else
    return a * b + c;
#endif
}

// Multiply and subtract
static inline Vec4f mul_sub(Vec4f const a, Vec4f const b, Vec4f const c) {
#ifdef __FMA__
    return _mm_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm_msub_ps(a, b, c);
#else
    return a * b - c;
#endif
}

// Multiply and inverse subtract
static inline Vec4f nmul_add(Vec4f const a, Vec4f const b, Vec4f const c) {
#ifdef __FMA__
    return _mm_fnmadd_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm_nmacc_ps(a, b, c);
#else
    return c - a * b;
#endif
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split.
// This is used in mathematical functions. Do not use it in general code 
// because it is inaccurate in certain cases
static inline Vec4f mul_sub_x(Vec4f const a, Vec4f const b, Vec4f const c) {
#ifdef __FMA__
    return _mm_fmsub_ps(a, b, c);
#elif defined (__FMA4__)
    return _mm_msub_ps(a, b, c);
#else
    // calculate a * b - c with extra precision
    Vec4i upper_mask = -(1 << 12);                         // mask to remove lower 12 bits
    Vec4f a_high = a & Vec4f(_mm_castsi128_ps(upper_mask));// split into high and low parts
    Vec4f b_high = b & Vec4f(_mm_castsi128_ps(upper_mask));
    Vec4f a_low = a - a_high;
    Vec4f b_low = b - b_high;
    Vec4f r1 = a_high * b_high;                            // this product is exact
    Vec4f r2 = r1 - c;                                     // subtract c from high product
    Vec4f r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}

// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec4i exponent(Vec4f const a) {
    Vec4ui t1 = _mm_castps_si128(a);   // reinterpret as 32-bit integer
    Vec4ui t2 = t1 << 1;               // shift out sign bit
    Vec4ui t3 = t2 >> 24;              // shift down logical to position 0
    Vec4i  t4 = Vec4i(t3) - 0x7F;      // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f
// NOTE: The name fraction clashes with an ENUM in MAC XCode CarbonCore script.h !
static inline Vec4f fraction(Vec4f const a) {
#if INSTRSET >= 10
    return _mm_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
#else
    Vec4ui t1 = _mm_castps_si128(a);   // reinterpret as 32-bit integer
    Vec4ui t2 = Vec4ui((t1 & 0x007FFFFF) | 0x3F800000); // set exponent to 0 + bias
    return _mm_castsi128_ps(t2);
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec4f exp2(Vec4i const n) {
    Vec4i t1 = max(n, -0x7F);         // limit to allowed range
    Vec4i t2 = min(t1, 0x80);
    Vec4i t3 = t2 + 0x7F;              // add bias
    Vec4i t4 = t3 << 23;               // put exponent into position 23
    return _mm_castsi128_ps(t4);       // reinterpret as float
}
//static Vec4f exp2(Vec4f const x);    // defined in vectormath_exp.h


// Control word manipulaton
// ------------------------
// The MXCSR control word has the following bits:
//  0:    Invalid Operation Flag
//  1:    Denormal Flag (=subnormal)
//  2:    Divide-by-Zero Flag
//  3:    Overflow Flag
//  4:    Underflow Flag
//  5:    Precision Flag
//  6:    Denormals Are Zeros (=subnormals)
//  7:    Invalid Operation Mask
//  8:    Denormal Operation Mask (=subnormal)
//  9:    Divide-by-Zero Mask
// 10:    Overflow Mask
// 11:    Underflow Mask
// 12:    Precision Mask
// 13-14: Rounding control
//        00: round to nearest or even
//        01: round down towards -infinity
//        10: round up   towards +infinity
//        11: round towards zero (truncate)
// 15: Flush to Zero

// Function get_control_word:
// Read the MXCSR control word
static inline uint32_t get_control_word() {
    return _mm_getcsr();
}

// Function set_control_word:
// Write the MXCSR control word
static inline void set_control_word(uint32_t w) {
    _mm_setcsr(w);
}

// Function no_subnormals:
// Set "Denormals Are Zeros" and "Flush to Zero" mode to avoid the extremely
// time-consuming denormals in case of underflow
static inline void no_subnormals() {
    uint32_t t1 = get_control_word();
    t1 |= (1 << 6) | (1 << 15);     // set bit 6 and 15 in MXCSR
    set_control_word(t1);
}

// Function reset_control_word:
// Set the MXCSR control word to the default value 0x1F80.
// This will mask floating point exceptions, set rounding mode to nearest (or even),
// and allow denormals.
static inline void reset_control_word() {
    set_control_word(0x1F80);
}


// change signs on vectors Vec4f
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3>
static inline Vec4f change_sign(Vec4f const a) {
    if ((i0 | i1 | i2 | i3) == 0) return a;
    __m128i mask = constant4ui<i0 ? 0x80000000 : 0, i1 ? 0x80000000 : 0, i2 ? 0x80000000 : 0, i3 ? 0x80000000 : 0>();
    return  _mm_xor_ps(a, _mm_castsi128_ps(mask));     // flip sign bits
}


/*****************************************************************************
*
*          Vec2d: Vector of 2 double precision floating point values
*
*****************************************************************************/

class Vec2d {
protected:
    __m128d xmm; // double vector
public:
    // Default constructor:
    Vec2d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec2d(double d) {
        xmm = _mm_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec2d(double d0, double d1) {
        xmm = _mm_setr_pd(d0, d1);
    }
    // Constructor to convert from type __m128d used in intrinsics:
    Vec2d(__m128d const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128d used in intrinsics:
    Vec2d & operator = (__m128d const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128d used in intrinsics
    operator __m128d() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec2d & load(double const * p) {
        xmm = _mm_loadu_pd(p);
        return *this;
    }
    // Member function to load from array, aligned by 16
    // "load_a" is faster than "load" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 16.
    Vec2d const load_a(double const * p) {
        xmm = _mm_load_pd(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm_storeu_pd(p, xmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(double * p) const {
        _mm_stream_pd(p, xmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 16
    // "store_a" is faster than "store" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale) and Atom, but not on other processors from Intel, AMD or VIA.
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 16.
    void store_a(double * p) const {
        _mm_store_pd(p, xmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec2d & load_partial(int n, double const * p) {
#if INSTRSET >= 10   // AVX512VL
        xmm = _mm_maskz_loadu_pd(__mmask8((1u << n) - 1), p);
#else 
        if (n == 1) {
            xmm = _mm_load_sd(p);
        }
        else if (n == 2) {
            load(p);
        }
        else {
            xmm = _mm_setzero_pd();
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm_mask_storeu_pd(p, __mmask8((1u << n) - 1), xmm);
#else 
        if (n == 1) {
            _mm_store_sd(p, xmm);
        }
        else if (n == 2) {
            store(p);
        }
#endif
    }
    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec2d & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_pd(__mmask8((1u << n) - 1), xmm);
#else 
        xmm = _mm_castps_pd(Vec4f(_mm_castpd_ps(xmm)).cutoff(n * 2));
#endif
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec2d const insert(int index, double value) {
#if INSTRSET >= 10   // AVX512VL         
        xmm = _mm_mask_movedup_pd(xmm, __mmask8(1u << index), _mm_set_sd(value));
#else
        __m128d v2 = _mm_set_sd(value);
        if (index == 0) {
            xmm = _mm_shuffle_pd(v2, xmm, 2);
        }
        else {
            xmm = _mm_shuffle_pd(xmm, v2, 0);
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    double extract(int index) const {
#if INSTRSET >= 10   // AVX512VL 
        __m128d x = _mm_mask_unpackhi_pd(xmm, __mmask8(index), xmm, xmm);
        return _mm_cvtsd_f64(x);
#else
        double x[2];
        store(x);
        return x[index & 1];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 2;
    }
    static constexpr int elementtype() {
        return 17;
    }
    typedef __m128d registertype;
};


/*****************************************************************************
*
*          Operators for Vec2d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec2d operator + (Vec2d const a, Vec2d const b) {
    return _mm_add_pd(a, b);
}

// vector operator + : add vector and scalar
static inline Vec2d operator + (Vec2d const a, double b) {
    return a + Vec2d(b);
}
static inline Vec2d operator + (double a, Vec2d const b) {
    return Vec2d(a) + b;
}

// vector operator += : add
static inline Vec2d & operator += (Vec2d & a, Vec2d const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec2d operator ++ (Vec2d & a, int) {
    Vec2d a0 = a;
    a = a + 1.0;
    return a0;
}

// prefix operator ++
static inline Vec2d & operator ++ (Vec2d & a) {
    a = a + 1.0;
    return a;
}

// vector operator - : subtract element by element
static inline Vec2d operator - (Vec2d const a, Vec2d const b) {
    return _mm_sub_pd(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec2d operator - (Vec2d const a, double b) {
    return a - Vec2d(b);
}
static inline Vec2d operator - (double a, Vec2d const b) {
    return Vec2d(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec2d operator - (Vec2d const a) {
    return _mm_xor_pd(a, _mm_castsi128_pd(_mm_setr_epi32(0, 0x80000000, 0, 0x80000000)));
}

// vector operator -= : subtract
static inline Vec2d & operator -= (Vec2d & a, Vec2d const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec2d operator -- (Vec2d & a, int) {
    Vec2d a0 = a;
    a = a - 1.0;
    return a0;
}

// prefix operator --
static inline Vec2d & operator -- (Vec2d & a) {
    a = a - 1.0;
    return a;
}

// vector operator * : multiply element by element
static inline Vec2d operator * (Vec2d const a, Vec2d const b) {
    return _mm_mul_pd(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec2d operator * (Vec2d const a, double b) {
    return a * Vec2d(b);
}
static inline Vec2d operator * (double a, Vec2d const b) {
    return Vec2d(a) * b;
}

// vector operator *= : multiply
static inline Vec2d & operator *= (Vec2d & a, Vec2d const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec2d operator / (Vec2d const a, Vec2d const b) {
    return _mm_div_pd(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec2d operator / (Vec2d const a, double b) {
    return a / Vec2d(b);
}
static inline Vec2d operator / (double a, Vec2d const b) {
    return Vec2d(a) / b;
}

// vector operator /= : divide
static inline Vec2d & operator /= (Vec2d & a, Vec2d const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec2db operator == (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 0);
#else
    return _mm_cmpeq_pd(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec2db operator != (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 4);
#else
    return _mm_cmpneq_pd(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec2db operator < (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 1);
#else
    return _mm_cmplt_pd(a, b);
#endif
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec2db operator <= (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 2);
#else
    return _mm_cmple_pd(a, b);
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec2db operator > (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 6);
#else
    return b < a;
#endif
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec2db operator >= (Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_pd_mask(a, b, 5);
#else
    return b <= a;
#endif
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec2d operator & (Vec2d const a, Vec2d const b) {
    return _mm_and_pd(a, b);
}

// vector operator &= : bitwise and
static inline Vec2d & operator &= (Vec2d & a, Vec2d const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec2d and Vec2db
static inline Vec2d operator & (Vec2d const a, Vec2db const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_maskz_mov_pd(b, a);
#else
    return _mm_and_pd(a, b);
#endif
}
static inline Vec2d operator & (Vec2db const a, Vec2d const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec2d operator | (Vec2d const a, Vec2d const b) {
    return _mm_or_pd(a, b);
}

// vector operator |= : bitwise or
static inline Vec2d & operator |= (Vec2d & a, Vec2d const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec2d operator ^ (Vec2d const a, Vec2d const b) {
    return _mm_xor_pd(a, b);
}

// vector operator ^= : bitwise xor
static inline Vec2d & operator ^= (Vec2d & a, Vec2d const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec2db operator ! (Vec2d const a) {
    return a == Vec2d(0.0);
}


/*****************************************************************************
*
*          Functions for Vec2d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). 
// No other values are allowed.
static inline Vec2d select(Vec2db const s, Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_pd(b, s, a);
#else
    return selectd(s, a, b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec2d if_add(Vec2db const f, Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10
    return _mm_mask_add_pd (a, f, a, b);
#else
    return a + (Vec2d(f) & b);
#endif
}

// Conditional subtract
static inline Vec2d if_sub(Vec2db const f, Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10
    return _mm_mask_sub_pd (a, f, a, b);
#else
    return a - (Vec2d(f) & b);
#endif
}

// Conditional multiply
static inline Vec2d if_mul(Vec2db const f, Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10
    return _mm_mask_mul_pd (a, f, a, b);
#else
    return a * select(f, b, 1.);
#endif
}

// Conditional divide
static inline Vec2d if_div(Vec2db const f, Vec2d const a, Vec2d const b) {
#if INSTRSET >= 10
    return _mm_mask_div_pd (a, f, a, b);
#else 
    return a / select(f, b, 1.);
#endif
}

// Sign functions

// change signs on vectors Vec2d
// Each index i0 - i1 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1>
static inline Vec2d change_sign(Vec2d const a) {
    if ((i0 | i1) == 0) return a;
    __m128i mask = constant4ui<0, i0 ? 0x80000000 : 0, 0, i1 ? 0x80000000 : 0>();
    return  _mm_xor_pd(a, _mm_castsi128_pd(mask));  // flip sign bits
}

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec2d(-0.0)) gives true, while Vec2d(-0.0) < Vec2d(0.0) gives false
static inline Vec2db sign_bit(Vec2d const a) {
    Vec2q t1 = _mm_castpd_si128(a);    // reinterpret as 64-bit integer
    Vec2q t2 = t1 >> 63;               // extend sign bit
#if INSTRSET >= 10
    return t2 != 0;
#else
    return _mm_castsi128_pd(t2);       // reinterpret as 64-bit Boolean
#endif
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec2d sign_combine(Vec2d const a, Vec2d const b) {
#if INSTRSET < 10
    return a ^ (b & Vec2d(-0.0));
#else
    return _mm_castsi128_pd (_mm_ternarylogic_epi64(
        _mm_castpd_si128(a), _mm_castpd_si128(b), Vec2q(0x8000000000000000), 0x78));
#endif
}

// Categorization functions

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec2db is_finite(Vec2d const a) {
#if INSTRSET >= 10
    return __mmask8(_mm_fpclass_pd_mask(a, 0x99) ^ 0x03);
#else
    Vec2q t1 = _mm_castpd_si128(a);    // reinterpret as integer
    Vec2q t2 = t1 << 1;                // shift out sign bit
    Vec2q t3 = 0xFFE0000000000000ll;   // exponent mask
    Vec2qb t4 = Vec2q(t2 & t3) != t3;  // exponent field is not all 1s
    return t4;
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec2db is_inf(Vec2d const a) {
#if INSTRSET >= 10
    return _mm_fpclass_pd_mask(a, 0x18);
#else
    Vec2q t1 = _mm_castpd_si128(a);    // reinterpret as integer
    Vec2q t2 = t1 << 1;                // shift out sign bit
    return t2 == 0xFFE0000000000000ll; // exponent is all 1s, fraction is 0
#endif
}


// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec2db is_nan(Vec2d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return Vec2db(_mm_fpclass_pd_mask(a, 0x81));
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__)
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec2db is_nan(Vec2d const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
//#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
//static inline Vec2db is_nan(Vec2d const a) {
//    __m128d aa = a;
//    __m128i unordered;
//    __asm volatile("vcmppd $3,  %1, %1, %0" : "=x" (unordered) :  "x" (aa) );
//    return Vec2db(unordered);
//}
#else
static inline Vec2db is_nan(Vec2d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
//    return _mm_cmp_pd(a, a, 3); // compare unordered
     return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif


// Function is_subnormal: gives true for elements that are subnormal (denormal)
// false for finite numbers, zero, NAN and INF
static inline Vec2db is_subnormal(Vec2d const a) {
#if INSTRSET >= 10
    return _mm_fpclass_pd_mask(a, 0x20);
#else
    Vec2q t1 = _mm_castpd_si128(a);    // reinterpret as 32-bit integer
    Vec2q t2 = t1 << 1;                // shift out sign bit
    Vec2q t3 = 0xFFE0000000000000ll;   // exponent mask
    Vec2q t4 = t2 & t3;                // exponent
    Vec2q t5 = _mm_andnot_si128(t3, t2);// fraction
    return Vec2qb((t4 == 0) & (t5 != 0));  // exponent = 0 and fraction != 0
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec2db is_zero_or_subnormal(Vec2d const a) {
#if INSTRSET >= 10
    return _mm_fpclass_pd_mask(a, 0x26);
#else
    Vec2q t = _mm_castpd_si128(a);     // reinterpret as 32-bit integer
    t &= 0x7FF0000000000000ll;   // isolate exponent
    return t == 0;                     // exponent = 0
#endif
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add(Vec2d const a) {

#if false &&  INSTRSET >= 3  // SSE3     
    // This version causes errors in Clang version 9.0 (https://bugs.llvm.org/show_bug.cgi?id=44111)
    // It is also inefficient on most processors, so we drop it
    __m128d t1 = _mm_hadd_pd(a, a);
    return _mm_cvtsd_f64(t1);

#elif true
    // This version is OK
    __m128d t1 = _mm_unpackhi_pd(a, a);
    __m128d t2 = _mm_add_pd(a, t1);
    return _mm_cvtsd_f64(t2);

#else
    // This version is also OK
    __m128  t0 = _mm_castpd_ps(a);
    __m128d t1 = _mm_castps_pd(_mm_movehl_ps(t0, t0));
    __m128d t2 = _mm_add_sd(a, t1);
    return _mm_cvtsd_f64(t2);

#endif
}

// function max: a > b ? a : b
static inline Vec2d max(Vec2d const a, Vec2d const b) {
    return _mm_max_pd(a, b);
}

// function min: a < b ? a : b
static inline Vec2d min(Vec2d const a, Vec2d const b) {
    return _mm_min_pd(a, b);
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec2d abs(Vec2d const a) {
#if INSTRSET >= 10  // AVX512VL
    return _mm_range_pd(a, a, 8);
#else
    __m128d mask = _mm_castsi128_pd(_mm_setr_epi32(-1, 0x7FFFFFFF, -1, 0x7FFFFFFF));
    return _mm_and_pd(a, mask);
#endif
}

// function sqrt: square root
static inline Vec2d sqrt(Vec2d const a) {
    return _mm_sqrt_pd(a);
}

// function square: a * a
static inline Vec2d square(Vec2d const a) {
    return a * a;
}

// pow(Vec2d, int):
// The purpose of this template is to prevent implicit conversion of a float
// exponent to int when calling pow(vector, float) and vectormath_exp.h is not included
template <typename TT> static Vec2d pow(Vec2d const a, TT const n);

// Raise floating point numbers to integer power n
template <>
inline Vec2d pow<int>(Vec2d const x0, int const n) {
    return pow_template_i<Vec2d>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec2d pow<uint32_t>(Vec2d const x0, uint32_t const n) {
    return pow_template_i<Vec2d>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec2d pow(Vec2d const a, Const_int_t<n>) {
    return pow_n<Vec2d, n>(a);
}

// function round: round to nearest integer (even). (result as double vector)
#if INSTRSET >= 5   // SSE4.1 supported
static inline Vec2d round(Vec2d const a) {
    return _mm_round_pd(a, 0 + 8);
}
#else

// avoid unsafe optimization in function round
#if defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) && INSTRSET < 5
static inline Vec2d round(Vec2d const a) __attribute__((optimize("-fno-unsafe-math-optimizations")));
#elif defined(__clang__) && INSTRSET < 5
static inline Vec2d round(Vec2d const a) __attribute__((optnone));
#elif defined (FLOAT_CONTROL_PRECISE_FOR_ROUND)
#pragma float_control(push) 
#pragma float_control(precise,on)
#endif
// function round: round to nearest integer (even). (result as double vector)
static inline Vec2d round(Vec2d const a) {
    // Note: assume MXCSR control register is set to rounding
    // (don't use conversion to int, it will limit the value to +/- 2^31)
    Vec2d signmask = _mm_castsi128_pd(constant4ui<0, 0x80000000, 0, 0x80000000>()); // -0.0
    Vec2d magic = _mm_castsi128_pd(constant4ui<0, 0x43300000, 0, 0x43300000>());    // magic number = 2^52
    Vec2d sign = _mm_and_pd(a, signmask);        // signbit of a
    Vec2d signedmagic = _mm_or_pd(magic, sign);  // magic number with sign of a
    Vec2d y = a + signedmagic - signedmagic;     // round by adding magic number
#ifdef SIGNED_ZERO
    y |= (a & Vec2d(-0.0));                      // sign of zero
#endif
    return y;
}
#if defined (FLOAT_CONTROL_PRECISE_FOR_ROUND)
#pragma float_control(pop)
#endif
#endif

// function truncate: round towards zero. (result as double vector)
static inline Vec2d truncate(Vec2d const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_pd(a, 3 + 8);
#else  // SSE2
    Vec2d a1 = abs(a);                        // abs
    Vec2d y1 = round(a1);                     // round
    Vec2d y2 = y1 - (Vec2d(1.0) & (y1 > a1)); // subtract 1 if bigger
    Vec2d y3 = y2 | (a & Vec2d(-0.));         // put the sign back in
    return y3;
#endif
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec2d floor(Vec2d const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_pd(a, 1 + 8);
#else  // SSE2
    Vec2d y = round(a);                      // round
    y -= Vec2d(1.0) & (y > a);               // subtract 1 if bigger
#ifdef SIGNED_ZERO
    y |= (a & Vec2d(-0.0));                  // sign of zero
#endif
    return y;
#endif
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec2d ceil(Vec2d const a) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_round_pd(a, 2 + 8);
#else  // SSE2
    Vec2d y = round(a);                      // round
    y += Vec2d(1.0) & (y < a);               // add 1 if smaller
#ifdef SIGNED_ZERO
    y |= (a & Vec2d(-0.0));                  // sign of zero
#endif
    return y;
#endif
}

// function truncate_to_int32: round towards zero.
static inline Vec4i truncate_to_int32(Vec2d const a, Vec2d const b) {
    Vec4i t1 = _mm_cvttpd_epi32(a);
    Vec4i t2 = _mm_cvttpd_epi32(b);
    return _mm_unpacklo_epi64(t1,t2);
}
//static inline Vec4i truncate_to_int(Vec2d const a, Vec2d const b) { // deprecated
//    return truncate_to_int32(a, b);}

// function truncate_to_int32: round towards zero.
static inline Vec4i truncate_to_int32(Vec2d const a) {
    return _mm_cvttpd_epi32(a);
}
//static inline Vec4i truncate_to_int(Vec2d const a) { // deprecated
//    return truncate_to_int32(a);}

// function truncatei: round towards zero. (inefficient for lower instruction sets)
static inline Vec2q truncatei(Vec2d const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    //return _mm_maskz_cvttpd_epi64( __mmask8(0xFF), a);
    return _mm_cvttpd_epi64(a);
#else
    double aa[2];
    a.store(aa);
    return Vec2q(int64_t(aa[0]), int64_t(aa[1]));
#endif
}
//static inline Vec2q truncate_to_int64(Vec2d const a) { return truncatei(a); } // deprecated

// function round_to_int: round to nearest integer (even).
// result as 32-bit integer vector
static inline Vec4i round_to_int32(Vec2d const a, Vec2d const b) {
    // Note: assume MXCSR control register is set to rounding
    Vec4i t1 = _mm_cvtpd_epi32(a);
    Vec4i t2 = _mm_cvtpd_epi32(b);
    return _mm_unpacklo_epi64(t1,t2);
}
//static inline Vec4i round_to_int(Vec2d const a, Vec2d const b) {  // deprecated
//    return round_to_int32(a, b);}

// function round_to_int: round to nearest integer (even).
// result as 32-bit integer vector. Upper two values of result are 0
static inline Vec4i round_to_int32(Vec2d const a) {
    Vec4i t1 = _mm_cvtpd_epi32(a);
    return t1;
}
//static inline Vec4i round_to_int(Vec2d const a) { return round_to_int32(a); }  // deprecated

// function round_to_int64: round to nearest or even. (inefficient for lower instruction sets)
static inline Vec2q roundi(Vec2d const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm_cvtpd_epi64(a);
#else
    return truncatei(round(a));
#endif
}
//static inline Vec2q round_to_int64(Vec2d const a) { return roundi(a); } // deprecated

// function to_double: convert integer vector elements to double vector (inefficient for lower instruction sets)
static inline Vec2d to_double(Vec2q const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm_maskz_cvtepi64_pd(__mmask8(0xFF), a);
#else
    int64_t aa[2];
    a.store(aa);
    return Vec2d(double(aa[0]), double(aa[1]));
#endif
}

static inline Vec2d to_double(Vec2uq const a) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm_cvtepu64_pd(a);
#else
    uint64_t aa[2];      // inefficient
    a.store(aa);
    return Vec2d(double(aa[0]), double(aa[1]));
#endif
}

// function to_double_low: convert integer vector elements [0] and [1] to double vector
static inline Vec2d to_double_low(Vec4i const a) {
    return _mm_cvtepi32_pd(a);
}

// function to_double_high: convert integer vector elements [2] and [3] to double vector
static inline Vec2d to_double_high(Vec4i const a) {
    return to_double_low(_mm_srli_si128(a, 8));
}

// function compress: convert two Vec2d to one Vec4f
static inline Vec4f compress(Vec2d const low, Vec2d const high) {
    Vec4f t1 = _mm_cvtpd_ps(low);
    Vec4f t2 = _mm_cvtpd_ps(high);
    return _mm_shuffle_ps(t1, t2, 0x44);
}

// Function extend_low : convert Vec4f vector elements [0] and [1] to Vec2d
static inline Vec2d extend_low(Vec4f const a) {
    return _mm_cvtps_pd(a);
}

// Function extend_high : convert Vec4f vector elements [2] and [3] to Vec2d
static inline Vec2d extend_high(Vec4f const a) {
    return _mm_cvtps_pd(_mm_movehl_ps(a, a));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec2d mul_add(Vec2d const a, Vec2d const b, Vec2d const c) {
#ifdef __FMA__
    return _mm_fmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm_macc_pd(a, b, c);
#else
    return a * b + c;
#endif
}

// Multiply and subtract
static inline Vec2d mul_sub(Vec2d const a, Vec2d const b, Vec2d const c) {
#ifdef __FMA__
    return _mm_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm_msub_pd(a, b, c);
#else
    return a * b - c;
#endif
}

// Multiply and inverse subtract
static inline Vec2d nmul_add(Vec2d const a, Vec2d const b, Vec2d const c) {
#ifdef __FMA__
    return _mm_fnmadd_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm_nmacc_pd(a, b, c);
#else
    return c - a * b;
#endif
}


// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split.
// This is used in mathematical functions. Do not use it in general code 
// because it is inaccurate in certain cases
static inline Vec2d mul_sub_x(Vec2d const a, Vec2d const b, Vec2d const c) {
#ifdef __FMA__
    return _mm_fmsub_pd(a, b, c);
#elif defined (__FMA4__)
    return _mm_msub_pd(a, b, c);
#else
    // calculate a * b - c with extra precision
    Vec2q upper_mask = -(1LL << 27);                       // mask to remove lower 27 bits
    Vec2d a_high = a & Vec2d(_mm_castsi128_pd(upper_mask));// split into high and low parts
    Vec2d b_high = b & Vec2d(_mm_castsi128_pd(upper_mask));
    Vec2d a_low = a - a_high;
    Vec2d b_low = b - b_high;
    Vec2d r1 = a_high * b_high;                            // this product is exact
    Vec2d r2 = r1 - c;                                     // subtract c from high product
    Vec2d r3 = r2 + (a_high * b_low + b_high * a_low) + a_low * b_low; // add rest of product
    return r3; // + ((r2 - r1) + c);
#endif
}

// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec2q exponent(Vec2d const a) {
    Vec2uq t1 = _mm_castpd_si128(a);   // reinterpret as 64-bit integer
    Vec2uq t2 = t1 << 1;               // shift out sign bit
    Vec2uq t3 = t2 >> 53;              // shift down logical to position 0
    Vec2q  t4 = Vec2q(t3) - 0x3FF;     // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25
// NOTE: The name fraction clashes with an ENUM in MAC XCode CarbonCore script.h !
static inline Vec2d fraction(Vec2d const a) {
#if INSTRSET >= 10
    return _mm_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
#else
    Vec2uq t1 = _mm_castpd_si128(a);   // reinterpret as 64-bit integer
    Vec2uq t2 = Vec2uq((t1 & 0x000FFFFFFFFFFFFFll) | 0x3FF0000000000000ll); // set exponent to 0 + bias
    return _mm_castsi128_pd(t2);
#endif
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec2d exp2(Vec2q const n) {
    Vec2q t1 = max(n, -0x3FF);        // limit to allowed range
    Vec2q t2 = min(t1, 0x400);
    Vec2q t3 = t2 + 0x3FF;             // add bias
    Vec2q t4 = t3 << 52;               // put exponent into position 52
    return _mm_castsi128_pd(t4);       // reinterpret as double
}
//static Vec2d exp2(Vec2d const x); // defined in vectormath_exp.h


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

static inline __m128i reinterpret_i(__m128i const x) {
    return x;
}

static inline __m128i reinterpret_i(__m128  const x) {
    return _mm_castps_si128(x);
}

static inline __m128i reinterpret_i(__m128d const x) {
    return _mm_castpd_si128(x);
}

static inline __m128  reinterpret_f(__m128i const x) {
    return _mm_castsi128_ps(x);
}

static inline __m128  reinterpret_f(__m128  const x) {
    return x;
}

static inline __m128  reinterpret_f(__m128d const x) {
    return _mm_castpd_ps(x);
}

static inline __m128d reinterpret_d(__m128i const x) {
    return _mm_castsi128_pd(x);
}

static inline __m128d reinterpret_d(__m128  const x) {
    return _mm_castps_pd(x);
}

static inline __m128d reinterpret_d(__m128d const x) {
    return x;
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec2d infinite2d() {
    return reinterpret_d(Vec2q(0x7FF0000000000000));
}

// Function nan2d: returns a vector where all elements are +NAN (quiet)
static inline Vec2d nan2d(int n = 0x10) {
    return nan_vec<Vec2d>(n);
}


/*****************************************************************************
*
*          Vector permute and blend functions
*
******************************************************************************
*
* The permute function can reorder the elements of a vector and optionally
* set some elements to zero.
*
* See vectori128.h for details
*
*****************************************************************************/

// permute vector Vec2d
template <int i0, int i1>
static inline Vec2d permute2(Vec2d const a) {
    int constexpr indexs[2] = { i0, i1 };                  // indexes as array
    __m128d y = a;                                         // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec2q>(indexs);
    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");
    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_pd();  // just return zero

    constexpr bool fit_shleft  = (flags & perm_shleft)  != 0;
    constexpr bool fit_shright = (flags & perm_shright) != 0;
    constexpr bool fit_punpckh = (flags & perm_punpckh) != 0;
    constexpr bool fit_punpckl = (flags & perm_punpckl) != 0;
    constexpr bool fit_zeroing = (flags & perm_zeroing) != 0;
    if constexpr ((flags & perm_perm) != 0) {              // permutation needed
        // try to fit various instructions
        if constexpr (fit_shleft && fit_zeroing) {
            // pslldq does both permutation and zeroing. if zeroing not needed use punpckl instead
            return _mm_castsi128_pd(_mm_bslli_si128(_mm_castpd_si128(a), 8));
        }
        if constexpr (fit_shright && fit_zeroing) {       
            // psrldq does both permutation and zeroing. if zeroing not needed use punpckh instead
            return _mm_castsi128_pd(_mm_bsrli_si128(_mm_castpd_si128(a), 8));
        }
        if constexpr (fit_punpckh) {       // fits punpckhi
            y = _mm_unpackhi_pd(a, a);
        }
        else if constexpr (fit_punpckl) {  // fits punpcklo
            y = _mm_unpacklo_pd(a, a);
        }
        else {  // needs general permute
            y = _mm_shuffle_pd(a, a, (i0 & 1) | (i1 & 1) * 2);
        }
    }
    if constexpr (fit_zeroing) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_pd(zero_mask<2>(indexs), y);
#else  // use unpack to avoid using data cache
        if constexpr (i0 == -1) {
            y = _mm_unpackhi_pd(_mm_setzero_pd(), y);
        }
        else if constexpr (i1 == -1) {
            y = _mm_unpacklo_pd(y, _mm_setzero_pd());
        }
#endif
    }
    return y;
}


// permute vector Vec4f
template <int i0, int i1, int i2, int i3>
static inline Vec4f permute4(Vec4f const a) {
    constexpr int indexs[4] = {i0, i1, i2, i3};            // indexes as array
    __m128 y = a;                                          // result

    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec4f>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_ps();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {
            // use larger permutation
            constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // permutation pattern
            y = reinterpret_f(permute2 <L.a[0], L.a[1]> (Vec2d(reinterpret_d(a))));
            if (!(flags & perm_addz)) return y;                 // no remaining zeroing
        }
#if  INSTRSET >= 4 && INSTRSET < 10 // SSSE3, but no compact mask
        else if constexpr ((flags & perm_zeroing) != 0) {  
            // Do both permutation and zeroing with PSHUFB instruction
            const EList <int8_t, 16> bm = pshufb_mask<Vec4i>(indexs);
            return _mm_castsi128_ps(_mm_shuffle_epi8(_mm_castps_si128(a), Vec4i().load(bm.a)));
        }
#endif 
        else if constexpr ((flags & perm_punpckh) != 0) {  // fits punpckhi
            y = _mm_unpackhi_ps(a, a);
        }
        else if constexpr ((flags & perm_punpckl) != 0) {  // fits punpcklo
            y = _mm_unpacklo_ps(a, a);
        }
        else if constexpr ((flags & perm_shleft) != 0) {   // fits pslldq
            y = _mm_castsi128_ps(_mm_bslli_si128(_mm_castps_si128(a), (16-(flags >> perm_rot_count)) & 0xF)); 
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_shright) != 0) {  // fits psrldq 
            y = _mm_castsi128_ps(_mm_bsrli_si128(_mm_castps_si128(a), (flags >> perm_rot_count) & 0xF)); 
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
#if INSTRSET >= 3  // SSE3
        else if constexpr (i0 == 0 && i1 == 0 && i2 == 2 && i3 == 2) {
            return _mm_moveldup_ps(a);
        }
        else if constexpr (i0 == 1 && i1 == 1 && i2 == 3 && i3 == 3) {
            return _mm_movehdup_ps(a);
        }
#endif
        else {  // needs general permute
            y = _mm_shuffle_ps(a, a, (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6);
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        // The mask-zero operation can be merged into the preceding instruction, whatever that is.
        // A good optimizing compiler will do this automatically.
        // I don't want to clutter all the branches above with this
        y = _mm_maskz_mov_ps (zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 4> bm = zero_mask_broad<Vec4i>(indexs);
        y = _mm_and_ps(_mm_castsi128_ps(Vec4i().load(bm.a)), y);
#endif
    }  
    return y;
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/
// permute and blend Vec2d
template <int i0, int i1>
static inline Vec2d blend2(Vec2d const a, Vec2d const b) {
    int constexpr indexs[2] = { i0, i1 };                  // indexes as array
    __m128d y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec2d>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_pd ();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute2 <i0, i1> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute2 <i0<0 ? i0 : i0&1, i1<0 ? i1 : i1&1> (b);
    }

    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_pd (a, (uint8_t)make_bit_mask<2, 0x301>(indexs), b);
#elif INSTRSET >= 5  // SSE4.1
        y = _mm_blend_pd (a, b, ((i0 & 2) ? 0x01 : 0) | ((i1 & 2) ? 0x02 : 0));
#else  // SSE2
        const EList <int64_t, 2> bm = make_broad_mask<Vec2d>(make_bit_mask<2, 0x301>(indexs));
        y = selectd(_mm_castsi128_pd(Vec2q().load(bm.a)), b, a);
#endif        
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_pd (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_pd (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_pd (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_pd (b, a);
    }
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm_shuffle_pd(a, b, (flags >> blend_shufpattern) & 3);
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm_shuffle_pd(b, a, (flags >> blend_shufpattern) & 3);
    }
    else { // No special cases. permute a and b separately, then blend.
           // This will not occur if ALLOW_FP_PERMUTE is true
#if INSTRSET >= 5  // SSE4.1
        constexpr bool dozero = false;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        constexpr EList<int, 4> L = blend_perm_indexes<2, (int)dozero>(indexs); // get permutation indexes
        __m128d ya = permute2<L.a[0], L.a[1]>(a);
        __m128d yb = permute2<L.a[2], L.a[3]>(b);
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_pd (ya, (uint8_t)make_bit_mask<2, 0x301>(indexs), yb);
#elif INSTRSET >= 5  // SSE4.1
        y = _mm_blend_pd (ya, yb, ((i0 & 2) ? 0x01 : 0) | ((i1 & 2) ? 0x02 : 0));
#else  // SSE2
        return _mm_or_pd(ya, yb);
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_pd(zero_mask<2>(indexs), y);
#else  // use broad mask
        const EList <int64_t, 2> bm = zero_mask_broad<Vec2q>(indexs);
        y = _mm_and_pd(_mm_castsi128_pd(Vec2q().load(bm.a)), y);
#endif
    }
    return y;
}


// permute and blend Vec4f
template <int i0, int i1, int i2, int i3>
static inline Vec4f blend4(Vec4f const a, Vec4f const b) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m128 y = a;                                          // result
    constexpr uint64_t flags = blend_flags<Vec4f>(indexs); // get flags for possibilities that fit the index pattern

    constexpr bool blendonly = (flags & (blend_perma | blend_permb)) == 0; // no permutation, only blending

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_ps();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute4 <i0, i1, i2, i3> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute4 < i0<0?i0:i0&3, i1<0?i1:i1&3, i2<0?i2:i2&3, i3<0?i3:i3&3> (b);
    }
    if constexpr ((flags & blend_largeblock) != 0) {       // fits blending with larger block size
        constexpr EList<int, 2> L = largeblock_indexes<4>(indexs);
        y = _mm_castpd_ps(blend2 <L.a[0], L.a[1]> (Vec2d(_mm_castps_pd(a)), Vec2d(_mm_castps_pd(b))));
        if constexpr ((flags & blend_addz) == 0) {
            return y;                                      // any zeroing has been done by larger blend
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_ps (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_ps (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_ps (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_ps (b, a);
    }
    else if constexpr ((flags & blend_shufab) != 0 && !blendonly) { // use floating point instruction shufps
        y = _mm_shuffle_ps(a, b, uint8_t(flags >> blend_shufpattern));
    }
    else if constexpr ((flags & blend_shufba) != 0 && !blendonly) { // use floating point instruction shufps
        y = _mm_shuffle_ps(b, a, uint8_t(flags >> blend_shufpattern));
    }
#if INSTRSET >= 4 // SSSE3
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm_castsi128_ps(_mm_alignr_epi8(_mm_castps_si128(a), _mm_castps_si128(b), flags >> blend_rotpattern));
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm_castsi128_ps(_mm_alignr_epi8(_mm_castps_si128(b), _mm_castps_si128(a), flags >> blend_rotpattern));
    }
#endif
    else { // No special cases. permute a and b separately, then blend.
#if INSTRSET >= 5  // SSE4.1
        constexpr bool dozero = false;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        Vec4f ya = a, yb = b;   // a and b permuted
        constexpr EList<int, 8> L = blend_perm_indexes<4, (int)dozero>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0 || dozero) {
            ya = permute4 <L.a[0], L.a[1], L.a[2], L.a[3]>(a);
        }
        if constexpr ((flags & blend_permb) != 0 || dozero) {
            yb = permute4 <L.a[4], L.a[5], L.a[6], L.a[7]>(b);
        }
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_ps (ya, (uint8_t)make_bit_mask<4, 0x302>(indexs), yb);
#elif INSTRSET >= 5  // SSE4.1
        constexpr uint8_t mm = ((i0 & 4) ? 0x01 : 0) | ((i1 & 4) ? 0x02 : 0) | ((i2 & 4) ? 0x04 : 0) | ((i3 & 4) ? 0x08 : 0);
        if constexpr (mm == 0x01) y = _mm_move_ss(ya, yb);
        else if constexpr (mm == 0x0E) y = _mm_move_ss(yb, ya);
        else {
            y = _mm_blend_ps (ya, yb, mm);
        }
#else  // SSE2. dozero = true
        return _mm_or_ps(ya, yb);
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_ps(zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 4> bm = zero_mask_broad<Vec4i>(indexs);
        y = _mm_and_ps(_mm_castsi128_ps(Vec4i().load(bm.a)), y);
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

static inline Vec4f lookup4(Vec4i const index, Vec4f const table) {
#if INSTRSET >= 7  // AVX
    return _mm_permutevar_ps(table, index);
#else
    int32_t ii[4];
    float   tt[6];
    table.store(tt);  (index & 3).store(ii);
    __m128 r01 = _mm_loadh_pi(_mm_load_ss(&tt[ii[0]]), (const __m64 *) & tt[ii[1]]);
    __m128 r23 = _mm_loadh_pi(_mm_load_ss(&tt[ii[2]]), (const __m64 *) & tt[ii[3]]);
    return _mm_shuffle_ps(r01, r23, 0x88);
#endif
}

static inline Vec4f lookup8(Vec4i const index, Vec4f const table0, Vec4f const table1) {
#if INSTRSET >= 8  // AVX2
    __m256 tt = _mm256_insertf128_ps(_mm256_castps128_ps256(table0), table1, 1); // combine tables
    __m128 r  = _mm256_castps256_ps128(_mm256_permutevar8x32_ps(tt, _mm256_castsi128_si256(index)));
    return r;

#elif INSTRSET >= 7  // AVX 
    __m128  r0 = _mm_permutevar_ps(table0, index);
    __m128  r1 = _mm_permutevar_ps(table1, index);
    __m128i i4 = _mm_slli_epi32(index, 29);
    return _mm_blendv_ps(r0, r1, _mm_castsi128_ps(i4));

#elif INSTRSET >= 5  // SSE4.1
    Vec4f   r0 = lookup4(index, table0);
    Vec4f   r1 = lookup4(index, table1);
    __m128i i4 = _mm_slli_epi32(index, 29);
    return _mm_blendv_ps(r0, r1, _mm_castsi128_ps(i4));

#else               // SSE2
    Vec4f   r0 = lookup4(index, table0);
    Vec4f   r1 = lookup4(index, table1);
    __m128i i4 = _mm_srai_epi32(_mm_slli_epi32(index, 29), 31);
    return selectf(_mm_castsi128_ps(i4), r1, r0);
#endif
}

template <int n>
static inline Vec4f lookup(Vec4i const index, float const * table) {
    if (n <= 0) return 0.0f;
    if (n <= 4) return lookup4(index, Vec4f().load(table));
    if (n <= 8) {
#if INSTRSET >= 8  // AVX2
        __m256 tt = _mm256_loadu_ps(table);
        __m128 r  = _mm256_castps256_ps128(_mm256_permutevar8x32_ps(tt, _mm256_castsi128_si256(index)));
        return r;
#else   // not AVX2
        return lookup8(index, Vec4f().load(table), Vec4f().load(table + 4));
#endif
    }
    // n > 8. Limit index
    Vec4ui index1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec4ui(index) & (n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec4ui(index), n - 1);
    }
#if INSTRSET >= 8  // AVX2
    return _mm_i32gather_ps(table, index1, 4);
#else
    uint32_t ii[4];  index1.store(ii);
    return Vec4f(table[ii[0]], table[ii[1]], table[ii[2]], table[ii[3]]);
#endif
}

static inline Vec2d lookup2(Vec2q const index, Vec2d const table) {
#if INSTRSET >= 7  // AVX
    return _mm_permutevar_pd(table, index + index);
#else
    int32_t ii[4];
    double  tt[2];
    table.store(tt);  (index & 1).store(ii);
    return Vec2d(tt[ii[0]], tt[ii[2]]);
#endif
}

static inline Vec2d lookup4(Vec2q const index, Vec2d const table0, Vec2d const table1) {
#if INSTRSET >= 7  // AVX
    Vec2q index2 = index + index;          // index << 1
    __m128d r0 = _mm_permutevar_pd(table0, index2);
    __m128d r1 = _mm_permutevar_pd(table1, index2);
    __m128i i4 = _mm_slli_epi64(index, 62);
    return _mm_blendv_pd(r0, r1, _mm_castsi128_pd(i4));
#else
    int32_t ii[4];
    double  tt[4];
    table0.store(tt);  table1.store(tt + 2);
    (index & 3).store(ii);
    return Vec2d(tt[ii[0]], tt[ii[2]]);
#endif
}

template <int n>
static inline Vec2d lookup(Vec2q const index, double const * table) {
    if (n <= 0) return 0.0;
    if (n <= 2) return lookup2(index, Vec2d().load(table));
#if INSTRSET < 8  // not AVX2
    if (n <= 4) return lookup4(index, Vec2d().load(table), Vec2d().load(table + 2));
#endif
    // Limit index
    Vec2uq index1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec2uq(index) & (n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec2uq(index), n - 1);
    }
#if INSTRSET >= 8  // AVX2
    return _mm_i64gather_pd(table, index1, 8);
#else
    uint32_t ii[4];  index1.store(ii);
    return Vec2d(table[ii[0]], table[ii[2]]);
#endif
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3
template <int i0, int i1, int i2, int i3>
static inline Vec4f gather4f(void const * a) {
    return reinterpret_f(gather4i<i0, i1, i2, i3>(a));
}

// Load elements from array a with indices i0, i1
template <int i0, int i1>
static inline Vec2d gather2d(void const * a) {
    return reinterpret_d(gather2q<i0, i1>(a));
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

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4f const data, float * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __m128i indx = constant4ui<i0, i1, i2, i3>();
    __mmask8 mask = uint8_t((i0 >= 0) | ((i1 >= 0) << 1) | ((i2 >= 0) << 2) | ((i3 >= 0) << 3));
    _mm_mask_i32scatter_ps(destination, mask, indx, data, 4);

#elif INSTRSET >= 9  //  __AVX512F__
    __m512i indx = _mm512_castsi128_si512(constant4ui<i0, i1, i2, i3>());
    __mmask16 mask = uint16_t((i0 >= 0) | ((i1 >= 0) << 1) | ((i2 >= 0) << 2) | ((i3 >= 0) << 3));
    _mm512_mask_i32scatter_ps(destination, mask, indx, _mm512_castps128_ps512(data), 4);

#else
    const int index[4] = { i0,i1,i2,i3 };
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) destination[index[i]] = data[i];
    }
#endif
}

template <int i0, int i1>
static inline void scatter(Vec2d const data, double * destination) {
    if (i0 >= 0) destination[i0] = data[0];
    if (i1 >= 0) destination[i1] = data[1];
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec4i const index, uint32_t limit, Vec4f const data, float * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm_cmplt_epu32_mask(index, Vec4ui(limit));
    _mm_mask_i32scatter_ps(destination, mask, index, data, 4);
#else
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) destination[index[i]] = data[i];
    }
#endif
}

static inline void scatter(Vec2q const index, uint32_t limit, Vec2d const data, double * destination) {
    if (uint64_t(index[0]) < uint64_t(limit)) destination[index[0]] = data[0];
    if (uint64_t(index[1]) < uint64_t(limit)) destination[index[1]] = data[1];
}


#if INSTRSET < 10  // these are defined in vectori128.h for compact boolean vectors

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4fb const x) {
    return to_bits(Vec4ib(x));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2db const x) {
    return to_bits(Vec2qb(x));
}

#endif  // INSTRSET < 10


#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORF128_H
