/****************************  vectorf512.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 512-bit floating point vector classes
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec16f    Vector of  16  single precision floating point numbers
* Vec16fb   Vector of  16  Booleans for use with Vec16f
* Vec8d     Vector of   8  double precision floating point numbers
* Vec8db    Vector of   8  Booleans for use with Vec8d
*
* Each vector object is represented internally in the CPU a 512-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2014-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORF512_H
#define VECTORF512_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

#ifdef VECTORF512E_H
#error Two different versions of vectorf512.h included
#endif

#include "vectori512.h"

#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Vec16fb: Vector of 16 Booleans for use with Vec16f
*          Vec8db:  Vector of 8  Booleans for use with Vec8d
*
*****************************************************************************/

typedef Vec16b Vec16fb;
typedef Vec8b  Vec8db;

#if INSTRSET == 9  // special cases of mixed compact and broad vectors
inline Vec16b::Vec16b(Vec8ib const x0, Vec8ib const x1) {
    mm = to_bits(x0) | uint16_t(to_bits(x1) << 8);
}
inline Vec16b::Vec16b(Vec8fb const x0, Vec8fb const x1) {
    mm = to_bits(x0) | uint16_t(to_bits(x1) << 8);
}
inline Vec8b::Vec8b(Vec4qb const x0, Vec4qb const x1) {
    mm = to_bits(x0) | (to_bits(x1) << 4);
}
inline Vec8b::Vec8b(Vec4db const x0, Vec4db const x1) {
    mm = to_bits(x0) | (to_bits(x1) << 4);
}

inline Vec8ib Vec16b::get_low() const {
    return Vec8ib().load_bits(uint8_t(mm));
}
inline Vec8ib Vec16b::get_high() const {
    return Vec8ib().load_bits(uint8_t((uint16_t)mm >> 8u));
}
inline Vec4qb Vec8b::get_low() const {
    return Vec4qb().load_bits(mm & 0xF);
}
inline Vec4qb Vec8b::get_high() const {
    return Vec4qb().load_bits(mm >> 4u);
}

#endif


/*****************************************************************************
*
*          Vec16f: Vector of 16 single precision floating point values
*
*****************************************************************************/

class Vec16f {
protected:
    __m512 zmm; // Float vector
public:
    // Default constructor:
    Vec16f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16f(float f) {
        zmm = _mm512_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec16f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7,
    float f8, float f9, float f10, float f11, float f12, float f13, float f14, float f15) {
        zmm = _mm512_setr_ps(f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15); 
    }
    // Constructor to build from two Vec8f:
    Vec16f(Vec8f const a0, Vec8f const a1) {
        zmm = _mm512_castpd_ps(_mm512_insertf64x4(_mm512_castps_pd(_mm512_castps256_ps512(a0)), _mm256_castps_pd(a1), 1));
    }
    // Constructor to convert from type __m512 used in intrinsics:
    Vec16f(__m512 const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512 used in intrinsics:
    Vec16f & operator = (__m512 const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512 used in intrinsics
    operator __m512() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec16f & load(float const * p) {
        zmm = _mm512_loadu_ps(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address divisible by 64
    Vec16f & load_a(float const * p) {
        zmm = _mm512_load_ps(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm512_storeu_ps(p, zmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(float * p) const {
        _mm512_stream_ps(p, zmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 64;
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address divisible by 64
    void store_a(float * p) const {
        _mm512_store_ps(p, zmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16f & load_partial(int n, float const * p) {
        zmm = _mm512_maskz_loadu_ps(__mmask16((1 << n) - 1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        _mm512_mask_storeu_ps(p, __mmask16((1 << n) - 1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16f & cutoff(int n) {
        zmm = _mm512_maskz_mov_ps(__mmask16((1 << n) - 1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16f const insert(int index, float value) {
        zmm = _mm512_mask_broadcastss_ps(zmm, __mmask16(1u << index), _mm_set_ss(value));
        return *this;
    }
    // Member function extract a single element from vector
    float extract(int index) const {
        __m512 x = _mm512_maskz_compress_ps(__mmask16(1u << index), zmm);
        return _mm512_cvtss_f32(x);        
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec8f get_low() const {
        return _mm512_castps512_ps256(zmm);
    }
    Vec8f get_high() const {
        return _mm256_castpd_ps(_mm512_extractf64x4_pd(_mm512_castps_pd(zmm),1));
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 16;
    }
    typedef __m512 registertype;
};


/*****************************************************************************
*
*          Operators for Vec16f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16f operator + (Vec16f const a, Vec16f const b) {
    return _mm512_add_ps(a, b);
}

// vector operator + : add vector and scalar
static inline Vec16f operator + (Vec16f const a, float b) {
    return a + Vec16f(b);
}
static inline Vec16f operator + (float a, Vec16f const b) {
    return Vec16f(a) + b;
}

// vector operator += : add
static inline Vec16f & operator += (Vec16f & a, Vec16f const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16f operator ++ (Vec16f & a, int) {
    Vec16f a0 = a;
    a = a + 1.0f;
    return a0;
}

// prefix operator ++
static inline Vec16f & operator ++ (Vec16f & a) {
    a = a + 1.0f;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16f operator - (Vec16f const a, Vec16f const b) {
    return _mm512_sub_ps(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec16f operator - (Vec16f const a, float b) {
    return a - Vec16f(b);
}
static inline Vec16f operator - (float a, Vec16f const b) {
    return Vec16f(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec16f operator - (Vec16f const a) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) ^ 0x80000000);
}

// vector operator -= : subtract
static inline Vec16f & operator -= (Vec16f & a, Vec16f const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16f operator -- (Vec16f & a, int) {
    Vec16f a0 = a;
    a = a - 1.0f;
    return a0;
}

// prefix operator --
static inline Vec16f & operator -- (Vec16f & a) {
    a = a - 1.0f;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16f operator * (Vec16f const a, Vec16f const b) {
    return _mm512_mul_ps(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec16f operator * (Vec16f const a, float b) {
    return a * Vec16f(b);
}
static inline Vec16f operator * (float a, Vec16f const b) {
    return Vec16f(a) * b;
}

// vector operator *= : multiply
static inline Vec16f & operator *= (Vec16f & a, Vec16f const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16f operator / (Vec16f const a, Vec16f const b) {
    return _mm512_div_ps(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec16f operator / (Vec16f const a, float b) {
    return a / Vec16f(b);
}
static inline Vec16f operator / (float a, Vec16f const b) {
    return Vec16f(a) / b;
}

// vector operator /= : divide
static inline Vec16f & operator /= (Vec16f & a, Vec16f const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16fb operator == (Vec16f const a, Vec16f const b) {
//    return _mm512_cmpeq_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec16fb operator != (Vec16f const a, Vec16f const b) {
//    return _mm512_cmpneq_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec16fb operator < (Vec16f const a, Vec16f const b) {
//    return _mm512_cmplt_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 1);
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec16fb operator <= (Vec16f const a, Vec16f const b) {
//    return _mm512_cmple_ps_mask(a, b);
    return _mm512_cmp_ps_mask(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec16fb operator > (Vec16f const a, Vec16f const b) {
    return _mm512_cmp_ps_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec16fb operator >= (Vec16f const a, Vec16f const b) {
    return _mm512_cmp_ps_mask(a, b, 5);
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec16f operator & (Vec16f const a, Vec16f const b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) & Vec16i(_mm512_castps_si512(b)));
}

// vector operator &= : bitwise and
static inline Vec16f & operator &= (Vec16f & a, Vec16f const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec16f and Vec16fb
static inline Vec16f operator & (Vec16f const a, Vec16fb const b) {
    return _mm512_maskz_mov_ps(b, a);
}
static inline Vec16f operator & (Vec16fb const a, Vec16f const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec16f operator | (Vec16f const a, Vec16f const b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) | Vec16i(_mm512_castps_si512(b)));
}

// vector operator |= : bitwise or
static inline Vec16f & operator |= (Vec16f & a, Vec16f const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16f operator ^ (Vec16f const a, Vec16f const b) {
    return _mm512_castsi512_ps(Vec16i(_mm512_castps_si512(a)) ^ Vec16i(_mm512_castps_si512(b)));
}

// vector operator ^= : bitwise xor
static inline Vec16f & operator ^= (Vec16f & a, Vec16f const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec16fb operator ! (Vec16f const a) {
    return a == Vec16f(0.0f);
}


/*****************************************************************************
*
*          Functions for Vec16f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16f select (Vec16fb const s, Vec16f const a, Vec16f const b) {
    return _mm512_mask_mov_ps(b, s, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16f if_add (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return _mm512_mask_add_ps(a, f, a, b);
}

// Conditional subtract
static inline Vec16f if_sub (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return _mm512_mask_sub_ps(a, f, a, b);
}

// Conditional multiply
static inline Vec16f if_mul (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return _mm512_mask_mul_ps(a, f, a, b);
}

// Conditional divide
static inline Vec16f if_div (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return _mm512_mask_div_ps(a, f, a, b);
}


// sign functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec16f(-0.0f)) gives true, while Vec16f(-0.0f) < Vec16f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb sign_bit(Vec16f const a) {
    Vec16i t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    return Vec16fb(t1 < 0);
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec16f sign_combine(Vec16f const a, Vec16f const b) {
    // return a ^ (b & Vec16f(-0.0f));
    return _mm512_castsi512_ps (_mm512_ternarylogic_epi32(
        _mm512_castps_si512(a), _mm512_castps_si512(b), Vec16i(0x80000000), 0x78));
}

// Categorization functions

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_finite(Vec16f const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    __mmask16 f = _mm512_fpclass_ps_mask(a, 0x99);
    return _mm512_knot(f);
#else
    Vec16i  t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    Vec16i  t2 = t1 << 1;                   // shift out sign bit
    Vec16ib t3 = Vec16i(t2 & 0xFF000000) != 0xFF000000; // exponent field is not all 1s
    return Vec16fb(t3);
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_inf(Vec16f const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_ps_mask(a, 0x18);
#else
    Vec16i t1 = _mm512_castps_si512(a); // reinterpret as 32-bit integer
    Vec16i t2 = t1 << 1;                // shift out sign bit
    return Vec16fb(t2 == 0xFF000000);   // exponent is all 1s, fraction is 0
#endif
} 

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec16fb is_nan(Vec16f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm512_fpclass_ps_mask(a, 0x81);
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) 
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec16fb is_nan(Vec16f const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
static inline Vec16fb is_nan(Vec16f const a) {
    __m512 aa = a;
    __mmask16 unordered;
    __asm volatile("vcmpps $3, %1, %1, %0" : "=Yk" (unordered) :  "v" (aa) );
    return Vec16fb(unordered);
}
#else
static inline Vec16fb is_nan(Vec16f const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return Vec16fb().load_bits(_mm512_cmp_ps_mask(a, a, 3)); // compare unordered
    // return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif


// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec16fb is_subnormal(Vec16f const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_ps_mask(a, 0x20);
#else
    Vec16i t1 = _mm512_castps_si512(a);    // reinterpret as 32-bit integer
    Vec16i t2 = t1 << 1;                   // shift out sign bit
    Vec16i t3 = 0xFF000000;                // exponent mask
    Vec16i t4 = t2 & t3;                   // exponent
    Vec16i t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec16fb(t4 == 0 && t5 != 0);     // exponent = 0 and fraction != 0
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec16fb is_zero_or_subnormal(Vec16f const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_ps_mask(a, 0x26);
#else
    Vec16i t = _mm512_castps_si512(a);            // reinterpret as 32-bit integer
    t &= 0x7F800000;                       // isolate exponent
    return Vec16fb(t == 0);                       // exponent = 0
#endif
}

// change signs on vectors Vec16f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f change_sign(Vec16f const a) {
    constexpr __mmask16 m = __mmask16((i0&1) | (i1&1)<<1 | (i2&1)<< 2 | (i3&1)<<3 | (i4&1)<<4 | (i5&1)<<5 | (i6&1)<<6 | (i7&1)<<7
        | (i8&1)<<8 | (i9&1)<<9 | (i10&1)<<10 | (i11&1)<<11 | (i12&1)<<12 | (i13&1)<<13 | (i14&1)<<14 | (i15&1)<<15);
    if constexpr ((uint16_t)m == 0) return a;
    __m512 s = _mm512_castsi512_ps(_mm512_maskz_set1_epi32(m, 0x80000000));
    return a ^ s;
}

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec16f const a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_ps(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function max: a > b ? a : b
static inline Vec16f max(Vec16f const a, Vec16f const b) {
    return _mm512_max_ps(a,b);
}

// function min: a < b ? a : b
static inline Vec16f min(Vec16f const a, Vec16f const b) {
    return _mm512_min_ps(a,b);
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec16f abs(Vec16f const a) {
#if INSTRSET >= 10  // AVX512DQ
    return _mm512_range_ps(a, a, 8);
#else
    return a & Vec16f(_mm512_castsi512_ps(Vec16i(0x7FFFFFFF)));
#endif
}

// function sqrt: square root
static inline Vec16f sqrt(Vec16f const a) {
    return _mm512_sqrt_ps(a);
}

// function square: a * a
static inline Vec16f square(Vec16f const a) {
    return a * a;
}

// pow(Vec16f, int):
template <typename TT> static Vec16f pow(Vec16f const a, TT const n);

// Raise floating point numbers to integer power n
template <>
inline Vec16f pow<int>(Vec16f const x0, int const n) {
    return pow_template_i<Vec16f>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec16f pow<uint32_t>(Vec16f const x0, uint32_t const n) {
    return pow_template_i<Vec16f>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec16f pow(Vec16f const a, Const_int_t<n>) {
    return pow_n<Vec16f, n>(a);
}
 
// function round: round to nearest integer (even). (result as float vector)
static inline Vec16f round(Vec16f const a) {
    return _mm512_roundscale_ps(a, 0+8);
}

// function truncate: round towards zero. (result as float vector)
static inline Vec16f truncate(Vec16f const a) {
    return _mm512_roundscale_ps(a, 3+8);
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec16f floor(Vec16f const a) {
    return _mm512_roundscale_ps(a, 1+8);
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec16f ceil(Vec16f const a) {
    return _mm512_roundscale_ps(a, 2+8);
}

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec16i roundi(Vec16f const a) {
    return _mm512_cvt_roundps_epi32(a, 0+8 /*_MM_FROUND_NO_EXC*/);
}
//static inline Vec16i round_to_int(Vec16f const a) {return roundi(a);} // deprecated

// function truncatei: round towards zero. (result as integer vector)
static inline Vec16i truncatei(Vec16f const a) {
    return _mm512_cvtt_roundps_epi32(a, 0+8 /*_MM_FROUND_NO_EXC*/);
}
//static inline Vec16i truncate_to_int(Vec16f const a) {return truncatei(a);} // deprecated

// function to_float: convert integer vector to float vector
static inline Vec16f to_float(Vec16i const a) {
    return _mm512_cvtepi32_ps(a);
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec16f to_float(Vec16ui const a) {
    return _mm512_cvtepu32_ps(a);
}

// Approximate math functions

// approximate reciprocal (Faster than 1.f / a.
// relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512F, full precision with AVX512ER)
static inline Vec16f approx_recipr(Vec16f const a) {
#ifdef __AVX512ER__  // AVX512ER instruction set includes fast reciprocal with better precision
    return _mm512_rcp28_round_ps(a, _MM_FROUND_NO_EXC);
#else
    return _mm512_rcp14_ps(a);
#endif
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a).
// Relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512F, full precision with AVX512ER)
static inline Vec16f approx_rsqrt(Vec16f const a) {
#ifdef __AVX512ER__  // AVX512ER instruction set includes fast reciprocal squareroot with better precision
    return _mm512_rsqrt28_round_ps(a, _MM_FROUND_NO_EXC);
#else
    return _mm512_rsqrt14_ps(a);
#endif
}


// Fused multiply and add functions

// Multiply and add
static inline Vec16f mul_add(Vec16f const a, Vec16f const b, Vec16f const c) {
    return _mm512_fmadd_ps(a, b, c);
}

// Multiply and subtract
static inline Vec16f mul_sub(Vec16f const a, Vec16f const b, Vec16f const c) {
    return _mm512_fmsub_ps(a, b, c);
}

// Multiply and inverse subtract
static inline Vec16f nmul_add(Vec16f const a, Vec16f const b, Vec16f const c) {
    return _mm512_fnmadd_ps(a, b, c);
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// Do not use mul_sub_x in general code because it is inaccurate in certain cases when FMA is not supported
static inline Vec16f mul_sub_x(Vec16f const a, Vec16f const b, Vec16f const c) {
    return _mm512_fmsub_ps(a, b, c);
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec16i exponent(Vec16f const a) {
    // return roundi(Vec16i(_mm512_getexp_ps(a)));
    Vec16ui t1 = _mm512_castps_si512(a);// reinterpret as 32-bit integers
    Vec16ui t2 = t1 << 1;               // shift out sign bit
    Vec16ui t3 = t2 >> 24;              // shift down logical to position 0
    Vec16i  t4 = Vec16i(t3) - 0x7F;     // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec16f fraction(Vec16f const a) {
    return _mm512_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec16f exp2(Vec16i const n) {
    Vec16i t1 = max(n,  -0x7F);         // limit to allowed range
    Vec16i t2 = min(t1,  0x80);
    Vec16i t3 = t2 + 0x7F;              // add bias
    Vec16i t4 = t3 << 23;               // put exponent into position 23
    return _mm512_castsi512_ps(t4);     // reinterpret as float
}
//static Vec16f exp2(Vec16f const x); // defined in vectormath_exp.h


/*****************************************************************************
*
*          Vec8d: Vector of 8 double precision floating point values
*
*****************************************************************************/

class Vec8d {
protected:
    __m512d zmm; // double vector
public:
    // Default constructor:
    Vec8d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8d(double d) {
        zmm = _mm512_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec8d(double d0, double d1, double d2, double d3, double d4, double d5, double d6, double d7) {
        zmm = _mm512_setr_pd(d0, d1, d2, d3, d4, d5, d6, d7); 
    }
    // Constructor to build from two Vec4d:
    Vec8d(Vec4d const a0, Vec4d const a1) {
        zmm = _mm512_insertf64x4(_mm512_castpd256_pd512(a0), a1, 1);
    }
    // Constructor to convert from type __m512d used in intrinsics:
    Vec8d(__m512d const x) {
        zmm = x;
    }
    // Assignment operator to convert from type __m512d used in intrinsics:
    Vec8d & operator = (__m512d const x) {
        zmm = x;
        return *this;
    }
    // Type cast operator to convert to __m512d used in intrinsics
    operator __m512d() const {
        return zmm;
    }
    // Member function to load from array (unaligned)
    Vec8d & load(double const * p) {
        zmm = _mm512_loadu_pd(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 64
    Vec8d & load_a(double const * p) {
        zmm = _mm512_load_pd(p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm512_storeu_pd(p, zmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(double * p) const {
        _mm512_stream_pd(p, zmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 64;
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 64
    void store_a(double * p) const {
        _mm512_store_pd(p, zmm);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8d & load_partial(int n, double const * p) {
        zmm = _mm512_maskz_loadu_pd(__mmask16((1<<n)-1), p);
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        _mm512_mask_storeu_pd(p, __mmask16((1<<n)-1), zmm);
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8d & cutoff(int n) {
        zmm = _mm512_maskz_mov_pd(__mmask16((1<<n)-1), zmm);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8d const insert(int index, double value) {
        zmm = _mm512_mask_broadcastsd_pd(zmm, __mmask8(1u << index), _mm_set_sd(value));
        return *this;
    }
    // Member function extract a single element from vector
    double extract(int index) const {
#if INSTRSET >= 10
        __m512d x = _mm512_maskz_compress_pd(__mmask8(1u << index), zmm);
        return _mm512_cvtsd_f64(x);        
#else 
        double a[8];
        store(a);
        return a[index & 7];        
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4d:
    Vec4d get_low() const {
        return _mm512_castpd512_pd256(zmm);
    }
    Vec4d get_high() const {
        return _mm512_extractf64x4_pd(zmm,1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 17;
    }
    typedef __m512d registertype;
};


/*****************************************************************************
*
*          Operators for Vec8d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8d operator + (Vec8d const a, Vec8d const b) {
    return _mm512_add_pd(a, b);
}

// vector operator + : add vector and scalar
static inline Vec8d operator + (Vec8d const a, double b) {
    return a + Vec8d(b);
}
static inline Vec8d operator + (double a, Vec8d const b) {
    return Vec8d(a) + b;
}

// vector operator += : add
static inline Vec8d & operator += (Vec8d & a, Vec8d const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8d operator ++ (Vec8d & a, int) {
    Vec8d a0 = a;
    a = a + 1.0;
    return a0;
}

// prefix operator ++
static inline Vec8d & operator ++ (Vec8d & a) {
    a = a + 1.0;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8d operator - (Vec8d const a, Vec8d const b) {
    return _mm512_sub_pd(a, b);
}

// vector operator - : subtract vector and scalar
static inline Vec8d operator - (Vec8d const a, double b) {
    return a - Vec8d(b);
}
static inline Vec8d operator - (double a, Vec8d const b) {
    return Vec8d(a) - b;
}

// vector operator - : unary minus
// Change sign bit, even for 0, INF and NAN
static inline Vec8d operator - (Vec8d const a) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) ^ Vec8q(0x8000000000000000));
}

// vector operator -= : subtract
static inline Vec8d & operator -= (Vec8d & a, Vec8d const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8d operator -- (Vec8d & a, int) {
    Vec8d a0 = a;
    a = a - 1.0;
    return a0;
}

// prefix operator --
static inline Vec8d & operator -- (Vec8d & a) {
    a = a - 1.0;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8d operator * (Vec8d const a, Vec8d const b) {
    return _mm512_mul_pd(a, b);
}

// vector operator * : multiply vector and scalar
static inline Vec8d operator * (Vec8d const a, double b) {
    return a * Vec8d(b);
}
static inline Vec8d operator * (double a, Vec8d const b) {
    return Vec8d(a) * b;
}

// vector operator *= : multiply
static inline Vec8d & operator *= (Vec8d & a, Vec8d const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8d operator / (Vec8d const a, Vec8d const b) {
    return _mm512_div_pd(a, b);
}

// vector operator / : divide vector and scalar
static inline Vec8d operator / (Vec8d const a, double b) {
    return a / Vec8d(b);
}
static inline Vec8d operator / (double a, Vec8d const b) {
    return Vec8d(a) / b;
}

// vector operator /= : divide
static inline Vec8d & operator /= (Vec8d & a, Vec8d const b) {
    a = a / b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8db operator == (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 0);
}

// vector operator != : returns true for elements for which a != b
static inline Vec8db operator != (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 4);
}

// vector operator < : returns true for elements for which a < b
static inline Vec8db operator < (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 1);
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8db operator <= (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 2);
}

// vector operator > : returns true for elements for which a > b
static inline Vec8db operator > (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 6);
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8db operator >= (Vec8d const a, Vec8d const b) {
    return _mm512_cmp_pd_mask(a, b, 5);
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8d operator & (Vec8d const a, Vec8d const b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) & Vec8q(_mm512_castpd_si512(b)));
}

// vector operator &= : bitwise and
static inline Vec8d & operator &= (Vec8d & a, Vec8d const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8d and Vec8db
static inline Vec8d operator & (Vec8d const a, Vec8db const b) {
    return _mm512_maskz_mov_pd((uint8_t)b, a);
}

static inline Vec8d operator & (Vec8db const a, Vec8d const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec8d operator | (Vec8d const a, Vec8d const b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) | Vec8q(_mm512_castpd_si512(b)));
}

// vector operator |= : bitwise or
static inline Vec8d & operator |= (Vec8d & a, Vec8d const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8d operator ^ (Vec8d const a, Vec8d const b) {
    return _mm512_castsi512_pd(Vec8q(_mm512_castpd_si512(a)) ^ Vec8q(_mm512_castpd_si512(b)));
}

// vector operator ^= : bitwise xor
static inline Vec8d & operator ^= (Vec8d & a, Vec8d const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8db operator ! (Vec8d const a) {
    return a == Vec8d(0.0);
}


/*****************************************************************************
*
*          Functions for Vec8d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8d select (Vec8db const s, Vec8d const a, Vec8d const b) {
    return _mm512_mask_mov_pd (b, (uint8_t)s, a);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8d if_add (Vec8db const f, Vec8d const a, Vec8d const b) {
    return _mm512_mask_add_pd(a, (uint8_t)f, a, b);
}

// Conditional subtract
static inline Vec8d if_sub (Vec8db const f, Vec8d const a, Vec8d const b) {
    return _mm512_mask_sub_pd(a, (uint8_t)f, a, b);
}

// Conditional multiply
static inline Vec8d if_mul (Vec8db const f, Vec8d const a, Vec8d const b) {
    return _mm512_mask_mul_pd(a, (uint8_t)f, a, b);
}

// Conditional divide
static inline Vec8d if_div (Vec8db const f, Vec8d const a, Vec8d const b) {
    return _mm512_mask_div_pd(a, (uint8_t)f, a, b);
}

// Sign functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
static inline Vec8db sign_bit(Vec8d const a) {
    Vec8q t1 = _mm512_castpd_si512(a);    // reinterpret as 64-bit integer
    return Vec8db(t1 < 0);
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8d sign_combine(Vec8d const a, Vec8d const b) {
    // return a ^ (b & Vec8d(-0.0));
    return _mm512_castsi512_pd (_mm512_ternarylogic_epi64(
        _mm512_castpd_si512(a), _mm512_castpd_si512(b), Vec8q(0x8000000000000000), 0x78));
}

// Categorization functions

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec8db is_finite(Vec8d const a) {
#if INSTRSET >= 10 // __AVX512DQ__
    __mmask8 f = _mm512_fpclass_pd_mask(a, 0x99);
    return __mmask8(_mm512_knot(f));
#else
    Vec8q  t1 = _mm512_castpd_si512(a); // reinterpret as 64-bit integer
    Vec8q  t2 = t1 << 1;                // shift out sign bit
    Vec8q  t3 = 0xFFE0000000000000ll;   // exponent mask
    Vec8qb t4 = Vec8q(t2 & t3) != t3;   // exponent field is not all 1s
    return Vec8db(t4);
#endif
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec8db is_inf(Vec8d const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_pd_mask(a, 0x18);
#else
    Vec8q t1 = _mm512_castpd_si512(a);           // reinterpret as 64-bit integer
    Vec8q t2 = t1 << 1;                          // shift out sign bit
    return Vec8db(t2 == 0xFFE0000000000000ll);   // exponent is all 1s, fraction is 0
#endif
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
#if INSTRSET >= 10
static inline Vec8db is_nan(Vec8d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return _mm512_fpclass_pd_mask(a, 0x81);
}
//#elif defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__) 
//__attribute__((optimize("-fno-unsafe-math-optimizations")))
//static inline Vec8db is_nan(Vec8d const a) {
//    return a != a; // not safe with -ffinite-math-only compiler option
//}
#elif (defined(__GNUC__) || defined(__clang__)) && !defined(__INTEL_COMPILER)
static inline Vec8db is_nan(Vec8d const a) {
    __m512d aa = a;
    __mmask16 unordered;
    __asm volatile("vcmppd $3, %1, %1, %0" : "=Yk" (unordered) :  "v" (aa) );
    return Vec8db(unordered);
}
#else
static inline Vec8db is_nan(Vec8d const a) {
    // assume that compiler does not optimize this away with -ffinite-math-only:
    return Vec8db().load_bits(_mm512_cmp_pd_mask(a, a, 3)); // compare unordered
    // return a != a; // This is not safe with -ffinite-math-only, -ffast-math, or /fp:fast compiler option
}
#endif


// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8db is_subnormal(Vec8d const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_pd_mask(a, 0x20);
#else
    Vec8q t1 = _mm512_castpd_si512(a); // reinterpret as 64-bit integer
    Vec8q t2 = t1 << 1;                // shift out sign bit
    Vec8q t3 = 0xFFE0000000000000ll;   // exponent mask
    Vec8q t4 = t2 & t3;                // exponent
    Vec8q t5 = _mm512_andnot_si512(t3,t2);// fraction
    return Vec8db(t4 == 0 && t5 != 0); // exponent = 0 and fraction != 0
#endif
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8db is_zero_or_subnormal(Vec8d const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_fpclass_pd_mask(a, 0x26);
#else
    Vec8q t = _mm512_castpd_si512(a);            // reinterpret as 32-bit integer
    t &= 0x7FF0000000000000ll;             // isolate exponent
    return Vec8db(t == 0);                       // exponent = 0
#endif
}

// change signs on vectors Vec8d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d change_sign(Vec8d const a) {
    const __mmask16 m = __mmask16((i0&1) | (i1&1)<<1 | (i2&1)<< 2 | (i3&1)<<3 | (i4&1)<<4 | (i5&1)<<5 | (i6&1)<<6 | (i7&1)<<7);
    if ((uint8_t)m == 0) return a;
#ifdef __x86_64__
    __m512d s = _mm512_castsi512_pd(_mm512_maskz_set1_epi64(m, 0x8000000000000000));
#else  // 32 bit mode
    __m512i v = Vec8q(0x8000000000000000);
    __m512d s = _mm512_castsi512_pd(_mm512_maskz_mov_epi64(m, v));
#endif
    return a ^ s;
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec8d const a) {
#if defined(__INTEL_COMPILER)
    return _mm512_reduce_add_pd(a);
#else
    return horizontal_add(a.get_low() + a.get_high());
#endif
}

// function max: a > b ? a : b
static inline Vec8d max(Vec8d const a, Vec8d const b) {
    return _mm512_max_pd(a,b);
}

// function min: a < b ? a : b
static inline Vec8d min(Vec8d const a, Vec8d const b) {
    return _mm512_min_pd(a,b);
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
static inline Vec8d abs(Vec8d const a) {
#if INSTRSET >= 10  // AVX512DQ
    return _mm512_range_pd(a, a, 8);
#else
    return a & Vec8d(_mm512_castsi512_pd(Vec8q(0x7FFFFFFFFFFFFFFF)));
#endif
}

// function sqrt: square root
static inline Vec8d sqrt(Vec8d const a) {
    return _mm512_sqrt_pd(a);
}

// function square: a * a
static inline Vec8d square(Vec8d const a) {
    return a * a;
}

// The purpose of this template is to prevent implicit conversion of a float
// exponent to int when calling pow(vector, float) and vectormath_exp.h is not included 
template <typename TT> static Vec8d pow(Vec8d const a, TT const n); // = delete;

// pow(Vec8d, int):
// Raise floating point numbers to integer power n
template <>
inline Vec8d pow<int>(Vec8d const x0, int const n) {
    return pow_template_i<Vec8d>(x0, n);
}

// allow conversion from unsigned int
template <>
inline Vec8d pow<uint32_t>(Vec8d const x0, uint32_t const n) {
    return pow_template_i<Vec8d>(x0, (int)n);
}

// Raise floating point numbers to integer power n, where n is a compile-time constant
template <int n>
static inline Vec8d pow(Vec8d const a, Const_int_t<n>) {
    return pow_n<Vec8d, n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec8d round(Vec8d const a) {
    return _mm512_roundscale_pd(a, 0);
}

// function truncate: round towards zero. (result as double vector)
static inline Vec8d truncate(Vec8d const a) {
    return _mm512_roundscale_pd(a, 3);
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec8d floor(Vec8d const a) {
    return _mm512_roundscale_pd(a, 1);
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec8d ceil(Vec8d const a) {
    return _mm512_roundscale_pd(a, 2);
}

// function round_to_int32: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int32(Vec8d const a) {
    //return _mm512_cvtpd_epi32(a);
    return _mm512_cvt_roundpd_epi32(a, 0+8);
}
//static inline Vec8i round_to_int(Vec8d const a) {return round_to_int32(a);} // deprecated


// function truncate_to_int32: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int32(Vec8d const a) {
    return _mm512_cvttpd_epi32(a);
}
//static inline Vec8i truncate_to_int(Vec8d const a) {return truncate_to_int32(a);} // deprecated


// function truncatei: round towards zero
static inline Vec8q truncatei(Vec8d const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_cvttpd_epi64(a);
#else
    double aa[8];            // inefficient
    a.store(aa);
    return Vec8q(int64_t(aa[0]), int64_t(aa[1]), int64_t(aa[2]), int64_t(aa[3]), int64_t(aa[4]), int64_t(aa[5]), int64_t(aa[6]), int64_t(aa[7]));
#endif
}
//static inline Vec8q truncate_to_int64(Vec8d const a) {return truncatei(a);} // deprecated

// function roundi: round to nearest or even
static inline Vec8q roundi(Vec8d const a) {
#if INSTRSET >= 10  // __AVX512DQ__
    return _mm512_cvtpd_epi64(a);
#else
    return truncatei(round(a));
#endif
}
//static inline Vec8q round_to_int64(Vec8d const a) {return roundi(a);} // deprecated

// function to_double: convert integer vector elements to double vector
static inline Vec8d to_double(Vec8q const a) {
#if INSTRSET >= 10 // __AVX512DQ__ 
    return _mm512_cvtepi64_pd(a);
#else
    int64_t aa[8];           // inefficient
    a.store(aa);
    return Vec8d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]), double(aa[4]), double(aa[5]), double(aa[6]), double(aa[7]));
#endif
}

static inline Vec8d to_double(Vec8uq const a) {
#if INSTRSET >= 10 // __AVX512DQ__ 
    return _mm512_cvtepu64_pd(a);
#else
    uint64_t aa[8];          // inefficient
    a.store(aa);
    return Vec8d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]), double(aa[4]), double(aa[5]), double(aa[6]), double(aa[7]));
#endif
}

// function to_double: convert integer vector to double vector
static inline Vec8d to_double(Vec8i const a) {
    return _mm512_cvtepi32_pd(a);
}

// function compress: convert two Vec8d to one Vec16f
static inline Vec16f compress (Vec8d const low, Vec8d const high) {
    __m256 t1 = _mm512_cvtpd_ps(low);
    __m256 t2 = _mm512_cvtpd_ps(high);
    return Vec16f(t1, t2);
}

// Function extend_low : convert Vec16f vector elements 0 - 3 to Vec8d
static inline Vec8d extend_low(Vec16f const a) {
    return _mm512_cvtps_pd(_mm512_castps512_ps256(a));
}

// Function extend_high : convert Vec16f vector elements 4 - 7 to Vec8d
static inline Vec8d extend_high (Vec16f const a) {
    return _mm512_cvtps_pd(a.get_high());
}


// Fused multiply and add functions

// Multiply and add
static inline Vec8d mul_add(Vec8d const a, Vec8d const b, Vec8d const c) {
    return _mm512_fmadd_pd(a, b, c);
}

// Multiply and subtract
static inline Vec8d mul_sub(Vec8d const a, Vec8d const b, Vec8d const c) {
    return _mm512_fmsub_pd(a, b, c);
}

// Multiply and inverse subtract
static inline Vec8d nmul_add(Vec8d const a, Vec8d const b, Vec8d const c) {
    return _mm512_fnmadd_pd(a, b, c);
}

// Multiply and subtract with extra precision on the intermediate calculations. used internally in math functions
static inline Vec8d mul_sub_x(Vec8d const a, Vec8d const b, Vec8d const c) {
    return _mm512_fmsub_pd(a, b, c);
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec8q exponent(Vec8d const a) {
    Vec8uq t1 = _mm512_castpd_si512(a);// reinterpret as 64-bit integer
    Vec8uq t2 = t1 << 1;               // shift out sign bit
    Vec8uq t3 = t2 >> 53;              // shift down logical to position 0
    Vec8q  t4 = Vec8q(t3) - 0x3FF;     // subtract bias from exponent
    return t4;
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec8d fraction(Vec8d const a) {
    return _mm512_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero);
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec8d exp2(Vec8q const n) {
    Vec8q t1 = max(n,  -0x3FF);        // limit to allowed range
    Vec8q t2 = min(t1,  0x400);
    Vec8q t3 = t2 + 0x3FF;             // add bias
    Vec8q t4 = t3 << 52;               // put exponent into position 52
    return _mm512_castsi512_pd(t4);    // reinterpret as double
}
//static Vec8d exp2(Vec8d const x);    // defined in vectormath_exp.h



/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

// AVX512 requires gcc version 4.9 or higher. Apparently the problem with mangling intrinsic vector types no longer exists in gcc 4.x

static inline __m512i reinterpret_i (__m512i const x) {
    return x;
}

static inline __m512i reinterpret_i (__m512  const x) {
    return _mm512_castps_si512(x);
}

static inline __m512i reinterpret_i (__m512d const x) {
    return _mm512_castpd_si512(x);
}

static inline __m512  reinterpret_f (__m512i const x) {
    return _mm512_castsi512_ps(x);
}

static inline __m512  reinterpret_f (__m512  const x) {
    return x;
}

static inline __m512  reinterpret_f (__m512d const x) {
    return _mm512_castpd_ps(x);
}

static inline __m512d reinterpret_d (__m512i const x) {
    return _mm512_castsi512_pd(x);
}

static inline __m512d reinterpret_d (__m512  const x) {
    return _mm512_castps_pd(x);
}

static inline __m512d reinterpret_d (__m512d const x) {
    return x;
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec16f infinite16f() {
    return reinterpret_f(Vec16i(0x7F800000));
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec16f nan16f(int n = 0x100) {
    return nan_vec<Vec16f>(n);
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec8d infinite8d() {
    return reinterpret_d(Vec8q(0x7FF0000000000000));
}

// Function nan8d: returns a vector where all elements are +NAN (quiet NAN)
static inline Vec8d nan8d(int n = 0x10) {
    return nan_vec<Vec8d>(n);
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
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d permute8(Vec8d const a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m512d y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec8d>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm512_setzero_pd();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // permutation pattern
            constexpr uint8_t  ppat = (L.a[0] & 3) | (L.a[1]<<2 & 0xC) | (L.a[2]<<4 & 0x30) | (L.a[3]<<6 & 0xC0);
            y = _mm512_shuffle_f64x2(a, a, ppat);
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in all lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm512_unpackhi_pd(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm512_unpacklo_pd(y, y);
            }
            else { // general permute within lanes
                constexpr uint8_t mm0 = (i0&1) | (i1&1)<<1 | (i2&1)<<2 | (i3&1)<<3 | (i4&1)<<4 | (i5&1)<<5 | (i6&1)<<6 | (i7&1)<<7;
                y = _mm512_permute_pd(a, mm0);             // select within same lane
            }
        }
        else {  // different patterns in all lanes
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                y = _mm512_castsi512_pd(_mm512_alignr_epi64 (_mm512_castpd_si512(y), _mm512_castpd_si512(y), rot));
            } 
            else if constexpr ((flags & perm_broadcast) != 0) {  // broadcast one element
                constexpr int e = flags >> perm_rot_count;
                if constexpr(e != 0) {
                    y = _mm512_castsi512_pd(_mm512_alignr_epi64(_mm512_castpd_si512(y), _mm512_castpd_si512(y), e));
                }
                y = _mm512_broadcastsd_pd(_mm512_castpd512_pd128(y));
            }
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_pd(__mmask8(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_pd(__mmask8(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_cross_lane) == 0) {  // no lane crossing
                if constexpr ((flags & perm_zeroing) == 0) {      // no zeroing. use vpermilps
                    const __m512i pmask = constant16ui <i0<<1, 0, i1<<1, 0, i2<<1, 0, i3<<1, 0, i4<<1, 0, i5<<1, 0, i6<<1, 0, i7<<1, 0>();
                    return _mm512_permutevar_pd(a, pmask);
                }
                else { // with zeroing. pshufb may be marginally better because it needs no extra zero mask
                    const EList <int8_t, 64> bm = pshufb_mask<Vec8q>(indexs);
                    return _mm512_castsi512_pd(_mm512_shuffle_epi8(_mm512_castpd_si512(y), Vec8q().load(bm.a)));
                }
            } 
            else {
                // full permute needed
                const __m512i pmask = constant16ui <
                    i0 & 7, 0, i1 & 7, 0, i2 & 7, 0, i3 & 7, 0, i4 & 7, 0, i5 & 7, 0, i6 & 7, 0, i7 & 7, 0>();
                y = _mm512_permutexvar_pd(pmask, y);
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) { // additional zeroing needed
        y = _mm512_maskz_mov_pd(zero_mask<8>(indexs), y);
    }
    return y;
}


// Permute vector of 16 32-bit integers.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f permute16(Vec16f const a) {
    int constexpr indexs[16] = {  // indexes as array
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };    
    __m512 y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec16f>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm512_setzero_ps();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // permutation pattern
            y = _mm512_castpd_ps( 
                permute8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> 
                (Vec8d(_mm512_castps_pd(a))));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in all lanes
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm512_unpackhi_ps(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm512_unpacklo_ps(y, y);
            }
            else { // general permute within lanes
                y = _mm512_permute_ps(a, uint8_t(flags >> perm_ipattern));
            }
        }
        else {  // different patterns in all lanes
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                y = _mm512_castsi512_ps(_mm512_alignr_epi32(_mm512_castps_si512(y), _mm512_castps_si512(y), rot));
            }
            else if constexpr ((flags & perm_broadcast) != 0) {  // broadcast one element
                constexpr int e = flags >> perm_rot_count;       // element index
                if constexpr(e != 0) {
                    y = _mm512_castsi512_ps(_mm512_alignr_epi32(_mm512_castps_si512(y), _mm512_castps_si512(y), e));
                }
                y = _mm512_broadcastss_ps(_mm512_castps512_ps128(y));
            }
            else if constexpr ((flags & perm_zext) != 0) {       // zero extension
                y = _mm512_castsi512_ps(_mm512_cvtepu32_epi64(_mm512_castsi512_si256(_mm512_castps_si512(y))));
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm512_maskz_compress_ps(__mmask16(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm512_maskz_expand_ps(__mmask16(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_cross_lane) == 0) {  // no lane crossing
                if constexpr ((flags & perm_zeroing) == 0) {      // no zeroing. use vpermilps
                    const __m512i pmask = constant16ui <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15>();
                    return _mm512_permutevar_ps(a, pmask);
                }
                else { // with zeroing. pshufb may be marginally better because it needs no extra zero mask
                    const EList <int8_t, 64> bm = pshufb_mask<Vec16i>(indexs);
                    return _mm512_castsi512_ps(_mm512_shuffle_epi8(_mm512_castps_si512(a), Vec16i().load(bm.a)));
                }
            }
            else {
                // full permute needed
                const __m512i pmaskf = constant16ui <
                    i0 & 15, i1 & 15, i2 & 15, i3 & 15, i4 & 15, i5 & 15, i6 & 15, i7 & 15,
                    i8 & 15, i9 & 15, i10 & 15, i11 & 15, i12 & 15, i13 & 15, i14 & 15, i15 & 15>();
                y = _mm512_permutexvar_ps(pmaskf, a);
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) { // additional zeroing needed
        y = _mm512_maskz_mov_ps(zero_mask<16>(indexs), y);
    }
    return y;
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8d blend8(Vec8d const a, Vec8d const b) { 
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m512d y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec8d>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_pd();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute8 <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        constexpr EList<int, 16> L = blend_perm_indexes<8, 2>(indexs); // get permutation indexes
        return permute8 < L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] > (b);
    } 
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint8_t mb = (uint8_t)make_bit_mask<8, 0x303>(indexs);  // blend mask
        y = _mm512_mask_mov_pd (a, mb, b);
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 128-bit blocks
        constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // get 128-bit blend pattern
        constexpr uint8_t shuf = (L.a[0] & 3) | (L.a[1] & 3) << 2 | (L.a[2] & 3) << 4 | (L.a[3] & 3) << 6;
        if constexpr (make_bit_mask<8, 0x103>(indexs) == 0) { // fits vshufi64x2 (a,b)
            y = _mm512_shuffle_f64x2(a, b, shuf);
        }
        else if constexpr (make_bit_mask<8, 0x203>(indexs) == 0) { // fits vshufi64x2 (b,a)
            y = _mm512_shuffle_f64x2(b, a, shuf);
        }
        else {
            const EList <int64_t, 8> bm = perm_mask_broad<Vec8q>(indexs);  
            y = _mm512_permutex2var_pd(a, Vec8q().load(bm.a), b);
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm512_unpacklo_pd (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm512_unpacklo_pd (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm512_unpackhi_pd (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm512_unpackhi_pd (b, a);
    }
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm512_shuffle_pd(a, b, uint8_t(flags >> blend_shufpattern));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm512_shuffle_pd(b, a, uint8_t(flags >> blend_shufpattern));
    }
    else { // No special cases
        const EList <int64_t, 8> bm = perm_mask_broad<Vec8q>(indexs);  
        y = _mm512_permutex2var_pd(a, Vec8q().load(bm.a), b);
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_pd(zero_mask<8>(indexs), y);
    }
    return y;
}


template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16f blend16(Vec16f const a, Vec16f const b) {
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15}; // indexes as array
    __m512 y = a;                                          // result
    constexpr uint64_t flags = blend_flags<Vec16f>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm512_setzero_ps();  // just return zero

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
        y = _mm512_mask_mov_ps(a, mb, b);
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 64-bit blocks
        constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // get 64-bit blend pattern
        y = _mm512_castpd_ps(blend8 <
            L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7] >
            (Vec8d(_mm512_castps_pd(a)), Vec8d(_mm512_castps_pd(b))));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing
    }
    else if constexpr ((flags & blend_same_pattern) != 0) { 
        // same pattern in all 128-bit lanes. check if pattern fits special cases
        if constexpr ((flags & blend_punpcklab) != 0) {
            y = _mm512_unpacklo_ps(a, b);
        }
        else if constexpr ((flags & blend_punpcklba) != 0) {
            y = _mm512_unpacklo_ps(b, a);
        }
        else if constexpr ((flags & blend_punpckhab) != 0) {
            y = _mm512_unpackhi_ps(a, b);
        }
        else if constexpr ((flags & blend_punpckhba) != 0) {
            y = _mm512_unpackhi_ps(b, a);
        }
        else if constexpr ((flags & blend_shufab) != 0) {  // use floating point instruction shufpd
            y = _mm512_shuffle_ps(a, b, uint8_t(flags >> blend_shufpattern));
        }
        else if constexpr ((flags & blend_shufba) != 0) {  // use floating point instruction shufpd
            y = _mm512_shuffle_ps(b, a, uint8_t(flags >> blend_shufpattern));
        }
        else {
            // Use vshufps twice. This generates two instructions in the dependency chain, 
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
            constexpr uint8_t  pattern = uint8_t(shuf(indexs));                     // permute pattern
            constexpr uint16_t froma = (uint16_t)make_bit_mask<16, 0x004>(indexs);  // elements from a
            constexpr uint16_t fromb = (uint16_t)make_bit_mask<16, 0x304>(indexs);  // elements from b
            y = _mm512_maskz_shuffle_ps(   froma, a, a, pattern);
            y = _mm512_mask_shuffle_ps (y, fromb, b, b, pattern);
            return y;  // we have already zeroed any unused elements
        }
    }
    else { // No special cases
        const EList <int32_t, 16> bm = perm_mask_broad<Vec16i>(indexs);  
        y = _mm512_permutex2var_ps(a, Vec16i().load(bm.a), b);
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
        y = _mm512_maskz_mov_ps(zero_mask<16>(indexs), y);
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

static inline Vec16f lookup16(Vec16i const index, Vec16f const table) {
    return _mm512_permutexvar_ps(index, table);
}

template <int n>
static inline Vec16f lookup(Vec16i const index, float const * table) {
    if (n <= 0) return 0;
    if (n <= 16) {
        Vec16f table1 = Vec16f().load((float*)table);
        return lookup16(index, table1);
    }
    if (n <= 32) {
        Vec16f table1 = Vec16f().load((float*)table);
        Vec16f table2 = Vec16f().load((float*)table + 16);
        return _mm512_permutex2var_ps(table1, index, table2);
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
    return _mm512_i32gather_ps(index1, (const float*)table, 4);
}


static inline Vec8d lookup8(Vec8q const index, Vec8d const table) {
    return _mm512_permutexvar_pd(index, table);
}

template <int n>
static inline Vec8d lookup(Vec8q const index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8d table1 = Vec8d().load((double*)table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8d table1 = Vec8d().load((double*)table);
        Vec8d table2 = Vec8d().load((double*)table + 8);
        return _mm512_permutex2var_pd(table1, index, table2);
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
    return _mm512_i64gather_pd(index1, (const double*)table, 8);
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f gather16f(void const * a) {
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 15) {
        // load one contiguous block and permute
        if constexpr (imax > 15) {
            // make sure we don't read past the end of the array
            Vec16f b = Vec16f().load((float const *)a + imax-15);
            return permute16<i0-imax+15, i1-imax+15, i2-imax+15, i3-imax+15, i4-imax+15, i5-imax+15, i6-imax+15, i7-imax+15,
                i8-imax+15, i9-imax+15, i10-imax+15, i11-imax+15, i12-imax+15, i13-imax+15, i14-imax+15, i15-imax+15> (b);
        }
        else {
            Vec16f b = Vec16f().load((float const *)a + imin);
            return permute16<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin,
                i8-imin, i9-imin, i10-imin, i11-imin, i12-imin, i13-imin, i14-imin, i15-imin> (b);
        }
    }
    if constexpr ((i0<imin+16  || i0>imax-16)  && (i1<imin+16  || i1>imax-16)  && (i2<imin+16  || i2>imax-16)  && (i3<imin+16  || i3>imax-16)
    &&  (i4<imin+16  || i4>imax-16)  && (i5<imin+16  || i5>imax-16)  && (i6<imin+16  || i6>imax-16)  && (i7<imin+16  || i7>imax-16)    
    &&  (i8<imin+16  || i8>imax-16)  && (i9<imin+16  || i9>imax-16)  && (i10<imin+16 || i10>imax-16) && (i11<imin+16 || i11>imax-16)
    &&  (i12<imin+16 || i12>imax-16) && (i13<imin+16 || i13>imax-16) && (i14<imin+16 || i14>imax-16) && (i15<imin+16 || i15>imax-16) ) {
        // load two contiguous blocks and blend
        Vec16f b = Vec16f().load((float const *)a + imin);
        Vec16f c = Vec16f().load((float const *)a + imax-15);
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
    return _mm512_i32gather_ps(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const float *)a, 4);
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d gather8d(void const * a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 7) {
        // load one contiguous block and permute
        if constexpr (imax > 7) {
            // make sure we don't read past the end of the array
            Vec8d b = Vec8d().load((double const *)a + imax-7);
            return permute8<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7> (b);
        }
        else {
            Vec8d b = Vec8d().load((double const *)a + imin);
            return permute8<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin> (b);
        }
    }
    if constexpr ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8d b = Vec8d().load((double const *)a + imin);
        Vec8d c = Vec8d().load((double const *)a + imax-7);
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
    return _mm512_i64gather_pd(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const double *)a, 8);
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
    static inline void scatter(Vec16f const data, float * array) {
    __m512i indx = constant16ui<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15>();
    Vec16fb mask(i0>=0, i1>=0, i2>=0, i3>=0, i4>=0, i5>=0, i6>=0, i7>=0,
        i8>=0, i9>=0, i10>=0, i11>=0, i12>=0, i13>=0, i14>=0, i15>=0);
    _mm512_mask_i32scatter_ps(array, mask, indx, data, 4);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline void scatter(Vec8d const data, double * array) {
    __m256i indx = constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>();
    Vec8db mask(i0>=0, i1>=0, i2>=0, i3>=0, i4>=0, i5>=0, i6>=0, i7>=0);
    _mm512_mask_i32scatter_pd(array, mask, indx, data, 8);
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec16i const index, uint32_t limit, Vec16f const data, float * destination) {
    Vec16fb mask = Vec16ui(index) < limit;
    _mm512_mask_i32scatter_ps(destination, mask, index, data, 4);
}

static inline void scatter(Vec8q const index, uint32_t limit, Vec8d const data, double * destination) {
    Vec8db mask = Vec8uq(index) < uint64_t(limit);
    _mm512_mask_i64scatter_pd(destination, (uint8_t)mask, index, data, 8);
}

static inline void scatter(Vec8i const index, uint32_t limit, Vec8d const data, double * destination) {
#if INSTRSET >= 10 // __AVX512VL__, __AVX512DQ__
    __mmask8 mask = _mm256_cmplt_epu32_mask(index, Vec8ui(limit));
#else
    __mmask16 mask = _mm512_cmplt_epu32_mask(_mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec8ui(limit)));
#endif
    _mm512_mask_i32scatter_pd(destination, (__mmask8)mask, index, data, 8);
}


#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORF512_H
