/****************************  vectormath_exp.h   ******************************
* Author:        Agner Fog
* Date created:  2014-04-18
* Last modified: 2019-08-30
* Version:       2.00.01
* Project:       vector class library
* Description:
* Header file containing inline vector functions of logarithms, exponential 
* and power functions:
* exp         exponential function
* exp2        exponential function base 2
* exp10       exponential function base 10
* exmp1       exponential function minus 1
* log         natural logarithm
* log2        logarithm base 2
* log10       logarithm base 10
* log1p       natural logarithm of 1+x
* cbrt        cube root
* pow         raise vector elements to power
* pow_ratio   raise vector elements to rational power
*
* Theory, methods and inspiration based partially on these sources:
* > Moshier, Stephen Lloyd Baluk: Methods and programs for mathematical functions.
*   Ellis Horwood, 1989.
* > VDT library developed on CERN by Danilo Piparo, Thomas Hauth and Vincenzo Innocente,
*   2012, https://root.cern.ch/doc/v606_/md_math_vdt_ReadMe.html
* > Cephes math library by Stephen L. Moshier 1992,
*   http://www.netlib.org/cephes/
*
* For detailed instructions see vcl_manual.pdf
*
* (c) Copyright 2014-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef VECTORMATH_EXP_H
#define VECTORMATH_EXP_H  1 

#include "vectormath_common.h"  

#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

/******************************************************************************
*                 Exponential functions
******************************************************************************/

// Helper functions, used internally:

// This function calculates pow(2,n) where n must be an integer. Does not check for overflow or underflow
static inline Vec2d vm_pow2n (Vec2d const n) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec2d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec2q b = reinterpret_i(a);                  // bit-cast to integer
    Vec2q c = b << 52;                           // shift left 52 places to get into exponent field
    Vec2d d = reinterpret_d(c);                  // bit-cast back to double
    return d;
}

static inline Vec4f vm_pow2n (Vec4f const n) {
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec4f a = n + (bias + pow2_23);              // put n + bias in least significant bits
    Vec4i b = reinterpret_i(a);                  // bit-cast to integer
    Vec4i c = b << 23;                           // shift left 23 places to get into exponent field
    Vec4f d = reinterpret_f(c);                  // bit-cast back to float
    return d;
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d vm_pow2n (Vec4d const n) {
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec4d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec4q b = reinterpret_i(a);                  // bit-cast to integer
    Vec4q c = b << 52;                           // shift left 52 places to get value into exponent field
    Vec4d d = reinterpret_d(c);                  // bit-cast back to double
    return d;
}

static inline Vec8f vm_pow2n (Vec8f const n) {
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec8f a = n + (bias + pow2_23);              // put n + bias in least significant bits
    Vec8i b = reinterpret_i(a);                  // bit-cast to integer
    Vec8i c = b << 23;                           // shift left 23 places to get into exponent field
    Vec8f d = reinterpret_f(c);                  // bit-cast back to float
    return d;
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d vm_pow2n (Vec8d const n) {
#ifdef __AVX512ER__
    return _mm512_exp2a23_round_pd(n, _MM_FROUND_NO_EXC); // this is exact only for integral n
#else
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    Vec8d a = n + (bias + pow2_52);              // put n + bias in least significant bits
    Vec8q b = Vec8q(reinterpret_i(a));           // bit-cast to integer
    Vec8q c = b << 52;                           // shift left 52 places to get value into exponent field
    Vec8d d = Vec8d(reinterpret_d(c));           // bit-cast back to double
    return d;
#endif
}

static inline Vec16f vm_pow2n (Vec16f const n) {
#ifdef __AVX512ER__
    return _mm512_exp2a23_round_ps(n, _MM_FROUND_NO_EXC);
#else
    const float pow2_23 =  8388608.0;            // 2^23
    const float bias = 127.0;                    // bias in exponent
    Vec16f a = n + (bias + pow2_23);             // put n + bias in least significant bits
    Vec16i b = Vec16i(reinterpret_i(a));         // bit-cast to integer
    Vec16i c = b << 23;                          // shift left 23 places to get into exponent field
    Vec16f d = Vec16f(reinterpret_f(c));         // bit-cast back to float
    return d;
#endif
}

#endif // MAX_VECTOR_SIZE >= 512


// Template for exp function, double precision
// The limit of abs(x) is defined by max_x below
// This function does not produce denormals
// Template parameters:
// VTYPE:  double vector type
// M1: 0 for exp, 1 for expm1
// BA: 0 for exp, 1 for 0.5*exp, 2 for pow(2,x), 10 for pow(10,x)

#if true  // choose method

// Taylor expansion
template<typename VTYPE, int M1, int BA> 
static inline VTYPE exp_d(VTYPE const initial_x) {    

    // Taylor coefficients, 1/n!
    // Not using minimax approximation because we prioritize precision close to x = 0
    const double p2  = 1./2.;
    const double p3  = 1./6.;
    const double p4  = 1./24.;
    const double p5  = 1./120.; 
    const double p6  = 1./720.; 
    const double p7  = 1./5040.; 
    const double p8  = 1./40320.; 
    const double p9  = 1./362880.; 
    const double p10 = 1./3628800.; 
    const double p11 = 1./39916800.; 
    const double p12 = 1./479001600.; 
    const double p13 = 1./6227020800.; 

    // maximum abs(x), value depends on BA, defined below
    // The lower limit of x is slightly more restrictive than the upper limit.
    // We are specifying the lower limit, except for BA = 1 because it is not used for negative x
    double max_x;

    // data vectors
    VTYPE  x, r, z, n2;

    if constexpr (BA <= 1) { // exp(x)
        max_x = BA == 0 ? 708.39 : 709.7;        // lower limit for 0.5*exp(x) is -707.6, but we are using 0.5*exp(x) only for positive x in hyperbolic functions
        const double ln2d_hi = 0.693145751953125;
        const double ln2d_lo = 1.42860682030941723212E-6;
        x  = initial_x;
        r  = round(initial_x*VM_LOG2E);
        // subtraction in two steps for higher precision
        x = nmul_add(r, ln2d_hi, x);             //  x -= r * ln2d_hi;
        x = nmul_add(r, ln2d_lo, x);             //  x -= r * ln2d_lo;
    }
    else if constexpr (BA == 2) { // pow(2,x)
        max_x = 1022.0;
        r  = round(initial_x);
        x  = initial_x - r;
        x *= VM_LN2;
    }
    else if constexpr (BA == 10) { // pow(10,x)
        max_x = 307.65;
        const double log10_2_hi = 0.30102999554947019; // log10(2) in two parts
        const double log10_2_lo = 1.1451100899212592E-10;
        x  = initial_x;
        r  = round(initial_x*(VM_LOG2E*VM_LN10));
        // subtraction in two steps for higher precision
        x  = nmul_add(r, log10_2_hi, x);         //  x -= r * log10_2_hi;
        x  = nmul_add(r, log10_2_lo, x);         //  x -= r * log10_2_lo;
        x *= VM_LN10;
    }
    else  {  // undefined value of BA
        return 0.;
    }

    z = polynomial_13m(x, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);

    if constexpr (BA == 1) r--;  // 0.5 * exp(x)

    // multiply by power of 2 
    n2 = vm_pow2n(r);

    if constexpr (M1 == 0) {
        // exp
        z = (z + 1.0) * n2;
    }
    else {
        // expm1
        z = mul_add(z, n2, n2 - 1.0);            // z = z * n2 + (n2 - 1.0);
#ifdef SIGNED_ZERO                               // pedantic preservation of signed zero         
        z = select(initial_x == 0., initial_x, z);
#endif
    }

    // check for overflow
    auto inrange  = abs(initial_x) < max_x;      // boolean vector
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return z;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.-(M1&1), infinite_vec<VTYPE>()); // value in case of +/- overflow or INF
        z = select(inrange, z, r);                         // +/- underflow
        z = select(is_nan(initial_x), initial_x, z);       // NAN goes through
        return z;
    }
}

#else

// Pade expansion uses less code and fewer registers, but is slower
template<typename VTYPE, int M1, int BA> 
static inline VTYPE exp_d(VTYPE const initial_x) {

    // define constants
    const double ln2p1   = 0.693145751953125;
    const double ln2p2   = 1.42860682030941723212E-6;
    const double log2e   = VM_LOG2E;
    const double max_exp = 708.39;
    // coefficients of pade polynomials
    const double P0exp = 9.99999999999999999910E-1;
    const double P1exp = 3.02994407707441961300E-2;
    const double P2exp = 1.26177193074810590878E-4;
    const double Q0exp = 2.00000000000000000009E0;
    const double Q1exp = 2.27265548208155028766E-1;
    const double Q2exp = 2.52448340349684104192E-3;
    const double Q3exp = 3.00198505138664455042E-6;

    VTYPE  x, r, xx, px, qx, y, n2;              // data vectors

    x = initial_x;
    r = round(initial_x*log2e);

    // subtraction in one step would gives loss of precision
    x -= r * ln2p1;
    x -= r * ln2p2;

    xx = x * x;

    // px = x * P(x^2).
    px = polynomial_2(xx, P0exp, P1exp, P2exp) * x;

    // Evaluate Q(x^2).
    qx = polynomial_3(xx, Q0exp, Q1exp, Q2exp, Q3exp);

    // e^x = 1 + 2*P(x^2)/( Q(x^2) - P(x^2) )
    y = (2.0 * px) / (qx - px);

    // Get 2^n in double.
    // n  = round_to_int64_limited(r);
    // n2 = exp2(n);
    n2 = vm_pow2n(r);  // this is faster

    if constexpr (M1 == 0) {
        // exp
        y = (y + 1.0) * n2;
    }
    else {
        // expm1
        y = y * n2 + (n2 - 1.0);
    }

    // overflow
    auto inrange  = abs(initial_x) < max_exp;
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return y;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.-M1, infinite_vec<VTYPE>()); // value in case of overflow or INF
        y = select(inrange, y, r);                                     // +/- overflow
        y = select(is_nan(initial_x), initial_x, y);                   // NAN goes through
        return y;
    }
}
#endif

// instances of exp_d template
static inline Vec2d exp(Vec2d const x) {
    return exp_d<Vec2d, 0, 0>(x);
}

static inline Vec2d expm1(Vec2d const x) {
    return exp_d<Vec2d, 3, 0>(x);
}

static inline Vec2d exp2(Vec2d const x) {
    return exp_d<Vec2d, 0, 2>(x);
}

static inline Vec2d exp10(Vec2d const x) {
    return exp_d<Vec2d, 0, 10>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d exp(Vec4d const x) {
    return exp_d<Vec4d, 0, 0>(x);
}

static inline Vec4d expm1(Vec4d const x) {
    return exp_d<Vec4d, 3, 0>(x);
}

static inline Vec4d exp2(Vec4d const x) {
    return exp_d<Vec4d, 0, 2>(x);
}

static inline Vec4d exp10(Vec4d const x) {
    return exp_d<Vec4d, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d exp(Vec8d const x) {
    return exp_d<Vec8d, 0, 0>(x);
}

static inline Vec8d expm1(Vec8d const x) {
    return exp_d<Vec8d, 3, 0>(x);
}

static inline Vec8d exp2(Vec8d const x) {
    return exp_d<Vec8d, 0, 2>(x);
}

static inline Vec8d exp10(Vec8d const x) {
    return exp_d<Vec8d, 0, 10>(x);
}

#endif // MAX_VECTOR_SIZE >= 512


// Template for exp function, single precision
// The limit of abs(x) is defined by max_x below
// This function does not produce denormals
// Template parameters:
// VTYPE:  float vector type
// M1: 0 for exp, 1 for expm1
// BA: 0 for exp, 1 for 0.5*exp, 2 for pow(2,x), 10 for pow(10,x)

template<typename VTYPE, int M1, int BA> 
static inline VTYPE exp_f(VTYPE const initial_x) {

    // Taylor coefficients
    const float P0expf   =  1.f/2.f;
    const float P1expf   =  1.f/6.f;
    const float P2expf   =  1.f/24.f;
    const float P3expf   =  1.f/120.f; 
    const float P4expf   =  1.f/720.f; 
    const float P5expf   =  1.f/5040.f; 

    VTYPE  x, r, x2, z, n2;                      // data vectors        

    // maximum abs(x), value depends on BA, defined below
    // The lower limit of x is slightly more restrictive than the upper limit.
    // We are specifying the lower limit, except for BA = 1 because it is not used for negative x
    float max_x;

    if constexpr (BA <= 1) { // exp(x)
        const float ln2f_hi  =  0.693359375f;
        const float ln2f_lo  = -2.12194440e-4f;
        max_x = (BA == 0) ? 87.3f : 89.0f;

        x = initial_x;
        r = round(initial_x*float(VM_LOG2E));
        x = nmul_add(r, VTYPE(ln2f_hi), x);      //  x -= r * ln2f_hi;
        x = nmul_add(r, VTYPE(ln2f_lo), x);      //  x -= r * ln2f_lo;
    }
    else if constexpr (BA == 2) {                // pow(2,x)
        max_x = 126.f;
        r = round(initial_x);
        x = initial_x - r;
        x = x * (float)VM_LN2;
    }
    else if constexpr (BA == 10) {               // pow(10,x)
        max_x = 37.9f;
        const float log10_2_hi = 0.301025391f;   // log10(2) in two parts
        const float log10_2_lo = 4.60503907E-6f;
        x = initial_x;
        r = round(initial_x*float(VM_LOG2E*VM_LN10));
        x = nmul_add(r, VTYPE(log10_2_hi), x);   //  x -= r * log10_2_hi;
        x = nmul_add(r, VTYPE(log10_2_lo), x);   //  x -= r * log10_2_lo;
        x = x * (float)VM_LN10;
    }
    else  {  // undefined value of BA
        return 0.;
    }

    x2 = x * x;
    z = polynomial_5(x,P0expf,P1expf,P2expf,P3expf,P4expf,P5expf);    
    z = mul_add(z, x2, x);                       // z *= x2;  z += x;

    if constexpr (BA == 1) r--;                  // 0.5 * exp(x)

    // multiply by power of 2 
    n2 = vm_pow2n(r);

    if constexpr (M1 == 0) {
        // exp
        z = (z + 1.0f) * n2;
    }
    else {
        // expm1
        z = mul_add(z, n2, n2 - 1.0f);           //  z = z * n2 + (n2 - 1.0f);
#ifdef SIGNED_ZERO                               // pedantic preservation of signed zero         
        z = select(initial_x == 0.f, initial_x, z);
#endif
    }

    // check for overflow
    auto inrange  = abs(initial_x) < max_x;      // boolean vector
    // check for INF and NAN
    inrange &= is_finite(initial_x);

    if (horizontal_and(inrange)) {
        // fast normal path
        return z;
    }
    else {
        // overflow, underflow and NAN
        r = select(sign_bit(initial_x), 0.f-(M1&1), infinite_vec<VTYPE>()); // value in case of +/- overflow or INF
        z = select(inrange, z, r);                         // +/- underflow
        z = select(is_nan(initial_x), initial_x, z);       // NAN goes through
        return z;
    }
}
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512
// forward declarations of fast 512 bit versions
static Vec16f exp(Vec16f const x);
static Vec16f exp2(Vec16f const x);
static Vec16f exp10(Vec16f const x);
#endif

// instances of exp_f template
static inline Vec4f exp(Vec4f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps128(exp(Vec16f(_mm512_castps128_ps512(x))));
#else
    return exp_f<Vec4f, 0, 0>(x);
#endif
}

static inline Vec4f expm1(Vec4f const x) {
    return exp_f<Vec4f, 3, 0>(x);
}

static inline Vec4f exp2(Vec4f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps128(exp2(Vec16f(_mm512_castps128_ps512(x))));
#else
    return exp_f<Vec4f, 0, 2>(x);
#endif
}

static inline Vec4f exp10(Vec4f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps128(exp10(Vec16f(_mm512_castps128_ps512(x))));
#else
    return exp_f<Vec4f, 0, 10>(x);
#endif
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f exp(Vec8f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps256(exp(Vec16f(_mm512_castps256_ps512(x))));
#else
    return exp_f<Vec8f, 0, 0>(x);
#endif
}

static inline Vec8f expm1(Vec8f const x) {
    return exp_f<Vec8f, 3, 0>(x);
}

static inline Vec8f exp2(Vec8f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps256(exp2(Vec16f(_mm512_castps256_ps512(x))));
#else
    return exp_f<Vec8f, 0, 2>(x);
#endif
}

static inline Vec8f exp10(Vec8f const x) {
#if defined(__AVX512ER__) && MAX_VECTOR_SIZE >= 512        // use faster 512 bit version
    return _mm512_castps512_ps256(exp10(Vec16f(_mm512_castps256_ps512(x))));
#else
    return exp_f<Vec8f, 0, 10>(x);
#endif
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f exp(Vec16f const x) {
#ifdef __AVX512ER__  // AVX512ER instruction set includes fast exponential function
#ifdef VCL_FASTEXP
    // very fast, but less precise for large x:
    return _mm512_exp2a23_round_ps(x*float(VM_LOG2E), _MM_FROUND_NO_EXC);
#else
    // best precision, also for large x:
    const Vec16f log2e = float(VM_LOG2E);
    const float ln2f_hi = 0.693359375f;
    const float ln2f_lo = -2.12194440e-4f;
    Vec16f x1 = x, r, y;
    r = round(x1*log2e);
    x1 = nmul_add(r, Vec16f(ln2f_hi), x1);       //  x -= r * ln2f_hi;
    x1 = nmul_add(r, Vec16f(ln2f_lo), x1);       //  x -= r * ln2f_lo;
    x1 = x1 * log2e;
    y = _mm512_exp2a23_round_ps(r, _MM_FROUND_NO_EXC);
    // y = vm_pow2n(r);
    return y * _mm512_exp2a23_round_ps(x1, _MM_FROUND_NO_EXC);
#endif // VCL_FASTEXP
#else  // no AVX512ER, use above template
    return exp_f<Vec16f, 0, 0>(x);
#endif
} 

static inline Vec16f expm1(Vec16f const x) {
    return exp_f<Vec16f, 3, 0>(x);
}

static inline Vec16f exp2(Vec16f const x) {
#ifdef __AVX512ER__
    return Vec16f(_mm512_exp2a23_round_ps(x, _MM_FROUND_NO_EXC));
#else
    return exp_f<Vec16f, 0, 2>(x);
#endif
}

static inline Vec16f exp10(Vec16f const x) {
#ifdef __AVX512ER__  // AVX512ER instruction set includes fast exponential function
#ifdef VCL_FASTEXP
    // very fast, but less precise for large x:
    return _mm512_exp2a23_round_ps(x*float(VM_LOG210), _MM_FROUND_NO_EXC);
#else
    // best precision, also for large x:
    const float log10_2_hi = 0.301025391f;       // log10(2) in two parts
    const float log10_2_lo = 4.60503907E-6f;
    Vec16f x1 = x, r, y;
    Vec16f log210 = float(VM_LOG210);
    r = round(x1*log210);
    x1 = nmul_add(r, Vec16f(log10_2_hi), x1);    //  x -= r * log10_2_hi
    x1 = nmul_add(r, Vec16f(log10_2_lo), x1);    //  x -= r * log10_2_lo
    x1 = x1 * log210;
    // y = vm_pow2n(r);
    y = _mm512_exp2a23_round_ps(r, _MM_FROUND_NO_EXC);
    return y * _mm512_exp2a23_round_ps(x1, _MM_FROUND_NO_EXC);
#endif // VCL_FASTEXP
#else  // no AVX512ER, use above template
    return exp_f<Vec16f, 0, 10>(x);
#endif
}

#endif // MAX_VECTOR_SIZE >= 512



/******************************************************************************
*                 Logarithm functions
******************************************************************************/

// Helper function: fraction_2(x) = fraction(x)*0.5

// Modified fraction function:
// Extract the fraction part of a floating point number, and divide by 2
// The fraction function is defined in vectorf128.h etc.
// fraction_2(x) = fraction(x)*0.5
// This version gives half the fraction without extra delay
// Does not work for x = 0
static inline Vec4f fraction_2(Vec4f const a) {
    Vec4ui t1 = _mm_castps_si128(a);                       // reinterpret as 32-bit integer
    Vec4ui t2 = Vec4ui((t1 & 0x007FFFFF) | 0x3F000000);    // set exponent to 0 + bias
    return _mm_castsi128_ps(t2);
}

static inline Vec2d fraction_2(Vec2d const a) {
    Vec2uq t1 = _mm_castpd_si128(a);                       // reinterpret as 64-bit integer
    Vec2uq t2 = Vec2uq((t1 & 0x000FFFFFFFFFFFFFll) | 0x3FE0000000000000ll); // set exponent to 0 + bias
    return _mm_castsi128_pd(t2);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f fraction_2(Vec8f const a) {
#if defined (VECTORI256_H) && VECTORI256_H > 2             // 256 bit integer vectors are available, AVX2
    Vec8ui t1 = _mm256_castps_si256(a);                    // reinterpret as 32-bit integer
    Vec8ui t2 = (t1 & 0x007FFFFF) | 0x3F000000;            // set exponent to 0 + bias
    return _mm256_castsi256_ps(t2);
#else
    return Vec8f(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

static inline Vec4d fraction_2(Vec4d const a) {
#if VECTORI256_H > 1  // AVX2
    Vec4uq t1 = _mm256_castpd_si256(a);                    // reinterpret as 64-bit integer
    Vec4uq t2 = Vec4uq((t1 & 0x000FFFFFFFFFFFFFll) | 0x3FE0000000000000ll); // set exponent to 0 + bias
    return _mm256_castsi256_pd(t2);
#else
    return Vec4d(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f fraction_2(Vec16f const a) {
#if INSTRSET >= 9                                          // 512 bit integer vectors are available, AVX512
    return _mm512_getmant_ps(a, _MM_MANT_NORM_p5_1, _MM_MANT_SIGN_zero);
    //return Vec16f(_mm512_getmant_ps(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero)) * 0.5f;
#else
    return Vec16f(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

static inline Vec8d fraction_2(Vec8d const a) {
#if INSTRSET >= 9                                          // 512 bit integer vectors are available, AVX512
    return _mm512_getmant_pd(a, _MM_MANT_NORM_p5_1, _MM_MANT_SIGN_zero);
    //return Vec8d(_mm512_getmant_pd(a, _MM_MANT_NORM_1_2, _MM_MANT_SIGN_zero)) * 0.5;
#else
    return Vec8d(fraction_2(a.get_low()), fraction_2(a.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 512


// Helper function: exponent_f(x) = exponent(x) as floating point number

union vm_ufi {
    float f;
    uint32_t i;
};

union vm_udi {
    double d;
    uint64_t i;
};

// extract exponent of a positive number x as a floating point number
static inline Vec4f exponent_f(Vec4f const x) {
#ifdef __AVX512VL__                              // AVX512VL
    // prevent returning -inf for x=0
    return _mm_maskz_getexp_ps(_mm_cmp_ps_mask(x,Vec4f(0.f),4), x);
#else
    const float pow2_23 =  8388608.0f;           // 2^23
    const float bias = 127.f;                    // bias in exponent
    const vm_ufi upow2_23 = {pow2_23};
    Vec4ui a = reinterpret_i(x);                 // bit-cast x to integer
    Vec4ui b = a >> 23;                          // shift down exponent to low bits
    Vec4ui c = b | Vec4ui(upow2_23.i);           // insert new exponent
    Vec4f  d = reinterpret_f(c);                 // bit-cast back to double
    Vec4f  e = d - (pow2_23 + bias);             // subtract magic number and bias
    return e;
#endif
}

static inline Vec2d exponent_f(Vec2d const x) {
#ifdef __AVX512VL__                              // AVX512VL
    // prevent returning -inf for x=0
    //return _mm_maskz_getexp_pd(x != 0., x);
    return _mm_maskz_getexp_pd(_mm_cmp_pd_mask(x,Vec2d(0.),4), x);

#else
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    const vm_udi upow2_52 = {pow2_52};

    Vec2uq a = reinterpret_i(x);                 // bit-cast x to integer
    Vec2uq b = a >> 52;                          // shift down exponent to low bits
    Vec2uq c = b | Vec2uq(upow2_52.i);           // insert new exponent
    Vec2d  d = reinterpret_d(c);                 // bit-cast back to double
    Vec2d  e = d - (pow2_52 + bias);             // subtract magic number and bias
    return e;
#endif
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f exponent_f(Vec8f const x) {
#ifdef __AVX512VL__                              // AVX512VL
    // prevent returning -inf for x=0
    //return _mm256_maskz_getexp_ps(x != 0.f, x);
    return _mm256_maskz_getexp_ps(_mm256_cmp_ps_mask(x,Vec8f(0.f),4), x);
#else
    const float pow2_23 =  8388608.0f;           // 2^23
    const float bias = 127.f;                    // bias in exponent
    const vm_ufi upow2_23 = {pow2_23};
    Vec8ui a = reinterpret_i(x);                 // bit-cast x to integer
    Vec8ui b = a >> 23;                          // shift down exponent to low bits
    Vec8ui c = b | Vec8ui(upow2_23.i);           // insert new exponent
    Vec8f  d = reinterpret_f(c);                 // bit-cast back to double
    Vec8f  e = d - (pow2_23 + bias);             // subtract magic number and bias
    return e;
#endif
} 

// extract exponent of a positive number x as a floating point number
static inline Vec4d exponent_f(Vec4d const x) {
#ifdef __AVX512VL__                              // AVX512VL
    // prevent returning -inf for x=0
    //return _mm256_maskz_getexp_pd(x != 0., x);
    return _mm256_maskz_getexp_pd(_mm256_cmp_pd_mask(x,Vec4d(0.),4), x);
#else
    const double pow2_52 = 4503599627370496.0;   // 2^52
    const double bias = 1023.0;                  // bias in exponent
    const vm_udi upow2_52 = {pow2_52};
    Vec4uq a = reinterpret_i(x);                 // bit-cast x to integer
    Vec4uq b = a >> 52;                          // shift down exponent to low bits
    Vec4uq c = b | Vec4uq(upow2_52.i);           // insert new exponent
    Vec4d  d = reinterpret_d(c);                 // bit-cast back to double
    Vec4d  e = d - (pow2_52 + bias);             // subtract magic number and bias
    return e;
#endif
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f exponent_f(Vec16f const x) {
#if INSTRSET >= 9                                // AVX512
    // prevent returning -inf for x=0
    return _mm512_maskz_getexp_ps(x != 0.f, x);
#else
    return Vec16f(exponent_f(x.get_low()), exponent_f(x.get_high()));
#endif
} 

// extract exponent of a positive number x as a floating point number
static inline Vec8d exponent_f(Vec8d const x) {
#if INSTRSET >= 9                                // AVX512
    // prevent returning -inf for x=0
    return _mm512_maskz_getexp_pd(uint8_t(x != 0.), x);
#else
    return Vec8d(exponent_f(x.get_low()), exponent_f(x.get_high()));
#endif
}

#endif // MAX_VECTOR_SIZE >= 512

// Helper function: log_special_cases(x,r). Handle special cases for log function
#if MAX_VECTOR_SIZE >= 512
static inline Vec8d log_special_cases(Vec8d const x1, Vec8d const r) {
    Vec8d res = r;
#if INSTRSET >= 10  // AVX512DQ
    Vec8db specialcases = _mm512_fpclass_pd_mask(x1, 0x7E);// zero, subnormal, negative, +-inf
    if (!horizontal_or(specialcases)) {
        return res;            // normal path
    }
    res = _mm512_fixupimm_pd(res, x1, Vec8q(0x03530411),0);// handle most cases
    res = select(Vec8db(_mm512_fpclass_pd_mask(x1, 0x26)),-infinite_vec<Vec8d>(),res);  // subnormal -> -INF
    res = select(Vec8db(_mm512_fpclass_pd_mask(x1, 0x50)),nan_vec<Vec8d>(NAN_LOG),res); // negative -> specific NAN
    return res;
#else
    Vec8db overflow = !is_finite(x1);
    Vec8db underflow = x1 < VM_SMALLEST_NORMAL;  // denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec8d>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec8d>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                      // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec8d>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
}

static inline Vec16f log_special_cases(Vec16f const x1, Vec16f const r) {
    Vec16f res = r;
#if INSTRSET >= 10  // AVX512DQ
    Vec16fb specialcases = _mm512_fpclass_ps_mask(x1, 0x7E);  // zero, subnormal, negative, +-inf
    if (!horizontal_or(specialcases)) {
        return res;          // normal path
    }
    res = _mm512_fixupimm_ps(res, x1, Vec16i(0x03530411), 0); // handle most cases
    res = select(Vec16fb(_mm512_fpclass_ps_mask(x1, 0x26)),-infinite_vec<Vec16f>(),res);  // subnormal -> -INF
    res = select(Vec16fb(_mm512_fpclass_ps_mask(x1, 0x50)),nan_vec<Vec16f>(NAN_LOG),res); // negative -> specific NAN
    return res;
#else
    Vec16fb overflow = !is_finite(x1);
    Vec16fb underflow = x1 < VM_SMALLEST_NORMALF;// denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec16f>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec16f>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                       // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec16f>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
}

#endif // MAX_VECTOR_SIZE >= 512

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d log_special_cases(Vec4d const x1, Vec4d const r) {
    Vec4d res = r;
#if INSTRSET >= 10  // AVX512DQ AVX512VL
    __mmask8 specialcases = _mm256_fpclass_pd_mask(x1, 0x7E);  // zero, subnormal, negative, +-inf
    if (specialcases == 0) {
        return res;          // normal path
    }
    res = _mm256_fixupimm_pd(res, x1, Vec4q(0x03530411), 0);   // handle most cases
    res = _mm256_mask_mov_pd(res, _mm256_fpclass_pd_mask(x1, 0x26), -infinite_vec<Vec4d>());  // subnormal -> -INF
    res = _mm256_mask_mov_pd(res, _mm256_fpclass_pd_mask(x1, 0x50), nan_vec<Vec4d>(NAN_LOG)); // negative -> specific NAN
    return res;
#else
    Vec4db overflow = !is_finite(x1);
    Vec4db underflow = x1 < VM_SMALLEST_NORMAL;  // denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec4d>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec4d>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                      // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec4d>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
}

static inline Vec8f log_special_cases(Vec8f const x1, Vec8f const r) {
    Vec8f res = r;
#if INSTRSET >= 10  // AVX512DQ AVX512VL
    __mmask8 specialcases = _mm256_fpclass_ps_mask(x1, 0x7E); // zero, subnormal, negative, +-inf
    if (specialcases == 0) {
        return res;          // normal path
    }
    res = _mm256_fixupimm_ps(res, x1, Vec8i(0x03530411), 0);  // handle most cases
    res = _mm256_mask_mov_ps(res, _mm256_fpclass_ps_mask(x1, 0x26), -infinite_vec<Vec8f>());  // subnormal -> -INF
    res = _mm256_mask_mov_ps(res, _mm256_fpclass_ps_mask(x1, 0x50), nan_vec<Vec8f>(NAN_LOG)); // negative -> specific NAN
    return res;
#else
    Vec8fb overflow = !is_finite(x1);
    Vec8fb underflow = x1 < VM_SMALLEST_NORMALF; // denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec8f>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec8f>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                      // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec8f>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
} 

#endif // MAX_VECTOR_SIZE >= 256

static inline Vec2d log_special_cases(Vec2d const x1, Vec2d const r) {
    Vec2d res = r;
#if INSTRSET >= 10  // AVX512DQ AVX512VL
    __mmask8 specialcases = _mm_fpclass_pd_mask(x1, 0x7E); // zero, subnormal, negative, +-inf
    if (specialcases == 0) {
        return res;            // normal path
    }
    res = _mm_fixupimm_pd(res, x1, Vec2q(0x03530411), 0);  // handle most cases
    res = _mm_mask_mov_pd(res, _mm_fpclass_pd_mask(x1, 0x26), -infinite_vec<Vec2d>());  // subnormal -> -INF
    res = _mm_mask_mov_pd(res, _mm_fpclass_pd_mask(x1, 0x50), nan_vec<Vec2d>(NAN_LOG)); // negative -> specific NAN
    return res;
#else
    Vec2db overflow = !is_finite(x1);
    Vec2db underflow = x1 < VM_SMALLEST_NORMAL;  // denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec2d>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec2d>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                      // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec2d>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
}

static inline Vec4f log_special_cases(Vec4f const x1, Vec4f const r) {
    Vec4f res = r;
#if INSTRSET >= 10  // AVX512DQ AVX512VL
    __mmask8 specialcases = _mm_fpclass_ps_mask(x1, 0x7E); // zero, subnormal, negative, +-inf
    if (specialcases == 0) {
        return res;          // normal path
    }
    res = _mm_fixupimm_ps(res, x1, Vec4i(0x03530411), 0);  // handle most cases
    res = _mm_mask_mov_ps(res, _mm_fpclass_ps_mask(x1, 0x26), -infinite_vec<Vec4f>());  // subnormal -> -INF
    res = _mm_mask_mov_ps(res, _mm_fpclass_ps_mask(x1, 0x50), nan_vec<Vec4f>(NAN_LOG)); // negative -> specific NAN
    return res;
#else
    Vec4fb overflow = !is_finite(x1);
    Vec4fb underflow = x1 < VM_SMALLEST_NORMALF; // denormals not supported by this functions
    if (!horizontal_or(overflow | underflow)) {
        return res;                              // normal path
    }
    // overflow and underflow
    res = select(underflow, nan_vec<Vec4f>(NAN_LOG), res);                // x1  < 0 gives NAN
    res = select(is_zero_or_subnormal(x1), -infinite_vec<Vec4f>(), res);  // x1 == 0 gives -INF
    res = select(overflow, x1, res);                                      // INF or NAN goes through
    res = select(is_inf(x1) & sign_bit(x1), nan_vec<Vec4f>(NAN_LOG), res);// -INF gives NAN
    return res;
#endif // INSTRSET
}


// log function, double precision
// template parameters:
// VTYPE:  f.p. vector type
// M1: 0 for log, 1 for log1p
template<typename VTYPE, int M1> 
static inline VTYPE log_d(VTYPE const initial_x) {

    // define constants
    const double ln2_hi =  0.693359375;
    const double ln2_lo = -2.121944400546905827679E-4;
    const double P0log  =  7.70838733755885391666E0;
    const double P1log  =  1.79368678507819816313E1;
    const double P2log  =  1.44989225341610930846E1;
    const double P3log  =  4.70579119878881725854E0;
    const double P4log  =  4.97494994976747001425E-1;
    const double P5log  =  1.01875663804580931796E-4;
    const double Q0log  =  2.31251620126765340583E1;
    const double Q1log  =  7.11544750618563894466E1;
    const double Q2log  =  8.29875266912776603211E1;
    const double Q3log  =  4.52279145837532221105E1;
    const double Q4log  =  1.12873587189167450590E1;

    VTYPE  x1, x, x2, px, qx, res, fe;           // data vectors

    if constexpr (M1 == 0) {
        x1 = initial_x;                          // log(x)
    }
    else {
        x1 = initial_x + 1.0;                    // log(x+1)
    }
    // separate mantissa from exponent 
    // VTYPE x  = fraction(x1) * 0.5;
    x  = fraction_2(x1);
    fe = exponent_f(x1);

    auto blend = x > VM_SQRT2*0.5;               // boolean vector
    x  = if_add(!blend, x, x);                   // conditional add
    fe = if_add(blend, fe, 1.);                  // conditional add

    if constexpr (M1 == 0) {
        // log(x). Expand around 1.0
        x -= 1.0;
    }
    else {
        // log(x+1). Avoid loss of precision when adding 1 and later subtracting 1 if exponent = 0
        x = select(fe==0., initial_x, x - 1.0);
    }

    // rational form 
    px  = polynomial_5 (x, P0log, P1log, P2log, P3log, P4log, P5log);
    x2  = x * x;
    px *= x * x2;
    qx  = polynomial_5n(x, Q0log, Q1log, Q2log, Q3log, Q4log);
    res = px / qx ;

    // add exponent
    res  = mul_add(fe, ln2_lo, res);             // res += fe * ln2_lo;
    res += nmul_add(x2, 0.5, x);                 // res += x  - 0.5 * x2;
    res  = mul_add(fe, ln2_hi, res);             // res += fe * ln2_hi;
#ifdef SIGNED_ZERO                               // pedantic preservation of signed zero                 
    res = select(initial_x == 0., initial_x, res);
#endif
    // handle special cases, or return res
    return log_special_cases(x1, res);
}


static inline Vec2d log(Vec2d const x) {
    return log_d<Vec2d, 0>(x);
}

static inline Vec2d log1p(Vec2d const x) {
    return log_d<Vec2d, 3>(x);
}

static inline Vec2d log2(Vec2d const x) {
    return VM_LOG2E * log_d<Vec2d, 0>(x);
}

static inline Vec2d log10(Vec2d const x) {
    return VM_LOG10E * log_d<Vec2d, 0>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d log(Vec4d const x) {
    return log_d<Vec4d, 0>(x);
}

static inline Vec4d log1p(Vec4d const x) {
    return log_d<Vec4d, 3>(x);
}

static inline Vec4d log2(Vec4d const x) {
    return VM_LOG2E * log_d<Vec4d, 0>(x);
}

static inline Vec4d log10(Vec4d const x) {
    return VM_LOG10E * log_d<Vec4d, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d log(Vec8d const x) {
    return log_d<Vec8d, 0>(x);
}

static inline Vec8d log1p(Vec8d const x) {
    return log_d<Vec8d, 3>(x);
}

static inline Vec8d log2(Vec8d const x) {
    return VM_LOG2E * log_d<Vec8d, 0>(x);
}

static inline Vec8d log10(Vec8d const x) {
    return VM_LOG10E * log_d<Vec8d, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 512


// log function, single precision
// template parameters:
// VTYPE:  f.p. vector type
// M1: 0 for log, 1 for log1p
template<typename VTYPE, int M1> 
static inline VTYPE log_f(VTYPE const initial_x) {

    // define constants
    const float ln2f_hi =  0.693359375f;
    const float ln2f_lo = -2.12194440E-4f;
    const float P0logf  =  3.3333331174E-1f;
    const float P1logf  = -2.4999993993E-1f;
    const float P2logf  =  2.0000714765E-1f;
    const float P3logf  = -1.6668057665E-1f;
    const float P4logf  =  1.4249322787E-1f;
    const float P5logf  = -1.2420140846E-1f;
    const float P6logf  =  1.1676998740E-1f;
    const float P7logf  = -1.1514610310E-1f;
    const float P8logf  =  7.0376836292E-2f;

    VTYPE  x1, x, res, x2, fe;                   // data vectors

    if constexpr (M1 == 0) {
        x1 = initial_x;                          // log(x)
    }
    else {
        x1 = initial_x + 1.0f;                   // log(x+1)
    }

    // separate mantissa from exponent 
    x = fraction_2(x1);
    auto e = exponent(x1);                       // integer vector

    auto blend = x > float(VM_SQRT2*0.5);        // boolean vector
    x  = if_add(!blend, x, x);                   // conditional add
    e  = if_add(decltype(e>e)(blend),  e, decltype(e)(1));  // conditional add
    fe = to_float(e);

    if constexpr (M1 == 0) {
        // log(x). Expand around 1.0
        x -= 1.0f;
    }
    else {
        // log(x+1). Avoid loss of precision when adding 1 and later subtracting 1 if exponent = 0
        x = select(decltype(x>x)(e==0), initial_x, x - 1.0f);
    }

    // Taylor expansion
    res = polynomial_8(x, P0logf, P1logf, P2logf, P3logf, P4logf, P5logf, P6logf, P7logf, P8logf);
    x2  = x*x;
    res *= x2*x;

    // add exponent
    res  = mul_add(fe, ln2f_lo, res);            // res += ln2f_lo  * fe;
    res += nmul_add(x2, 0.5f, x);                // res += x - 0.5f * x2;
    res  = mul_add(fe, ln2f_hi, res);            // res += ln2f_hi  * fe;
#ifdef SIGNED_ZERO                               // pedantic preservation of signed zero         
    res = select(initial_x == 0.f, initial_x, res);
#endif
    // handle special cases, or return res
    return log_special_cases(x1, res);
}

static inline Vec4f log(Vec4f const x) {
    return log_f<Vec4f, 0>(x);
}

static inline Vec4f log1p(Vec4f const x) {
    return log_f<Vec4f, 3>(x);
}

static inline Vec4f log2(Vec4f const x) {
    return float(VM_LOG2E) * log_f<Vec4f, 0>(x);
}

static inline Vec4f log10(Vec4f const x) {
    return float(VM_LOG10E) * log_f<Vec4f, 0>(x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f log(Vec8f const x) {
    return log_f<Vec8f, 0>(x);
}

static inline Vec8f log1p(Vec8f const x) {
    return log_f<Vec8f, 3>(x);
}

static inline Vec8f log2(Vec8f const x) {
    return float(VM_LOG2E) * log_f<Vec8f, 0>(x);
}

static inline Vec8f log10(Vec8f const x) {
    return float(VM_LOG10E) * log_f<Vec8f, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f log(Vec16f const x) {
    return log_f<Vec16f, 0>(x);
}

static inline Vec16f log1p(Vec16f const x) {
    return log_f<Vec16f, 3>(x);
}

static inline Vec16f log2(Vec16f const x) {
    return float(VM_LOG2E) * log_f<Vec16f, 0>(x);
}

static inline Vec16f log10(Vec16f const x) {
    return float(VM_LOG10E) * log_f<Vec16f, 0>(x);
}

#endif // MAX_VECTOR_SIZE >= 512


/******************************************************************************
*           Cube root and reciprocal cube root
******************************************************************************/

// cube root template, double precision
// template parameters:
// VTYPE:  f.p. vector type
// CR:     -1 for reciprocal cube root, 1 for cube root, 2 for cube root squared
template<typename VTYPE, int CR> 
static inline VTYPE cbrt_d(VTYPE const x) {
    const int iter = 7;     // iteration count of x^(-1/3) loop
    int i;
    typedef decltype(x < x) BVTYPE;              // boolean vector type
    typedef decltype(roundi(x)) ITYPE64;         // 64 bit integer vector type
    typedef decltype(roundi(compress(x,x))) ITYPE32; // 32 bit integer vector type

    ITYPE32 m1, m2;
    BVTYPE underflow;
    ITYPE64 q1(0x5540000000000000ULL);           // exponent bias
    ITYPE64 q2(0x0005555500000000ULL);           // exponent multiplier for 1/3
    ITYPE64 q3(0x0010000000000000ULL);           // denormal limit

    VTYPE  xa, xa3, a, a2;
    const double one_third  = 1./3.;
    const double four_third = 4./3.;

    xa  = abs(x);
    xa3 = one_third*xa;

    // multiply exponent by -1/3
    m1 = reinterpret_i(xa);
    m2 = ITYPE32(q1) - (m1 >> 20) * ITYPE32(q2);
    a  = reinterpret_d(m2);
    underflow = BVTYPE(ITYPE64(m1) <= q3);       // true if denormal or zero

    // Newton Raphson iteration. Warning: may overflow!
    for (i = 0; i < iter-1; i++) {
        a2 = a * a;
        a = nmul_add(xa3, a2*a2, four_third*a);  // a = four_third*a - xa3*a2*a2;
    }
    // last iteration with better precision
    a2 = a * a;    
    a = mul_add(one_third, nmul_add(xa, a2*a2, a), a); // a = a + one_third*(a - xa*a2*a2);

    if constexpr (CR == -1) {                    // reciprocal cube root
        a = select(underflow, infinite_vec<VTYPE>(), a); // generate INF if underflow
        a = select(is_inf(x), VTYPE(0), a);      // special case for INF                                                 // get sign
        a = sign_combine(a, x);                  // get sign
    }
    else if constexpr (CR == 1) {                // cube root
        a = a * a * x;        
        a = select(underflow, 0., a);            // generate 0 if underflow
        a = select(is_inf(x), x, a);             // special case for INF
#ifdef SIGNED_ZERO
        a = a | (x & VTYPE(-0.0));                      // get sign of x
#endif
    }
    else if constexpr (CR == 2) {                // cube root squared
        a = a * xa;        
        a = select(underflow, 0., a);            // generate 0 if underflow
        a = select(is_inf(x), xa, a);            // special case for INF
    }
    return a;
}

// template instances for cbrt and reciprocal_cbrt

// cube root
static inline Vec2d cbrt(Vec2d const x) {
    return cbrt_d<Vec2d, 1> (x);
}

// reciprocal cube root
static inline Vec2d reciprocal_cbrt(Vec2d const x) {
    return cbrt_d<Vec2d, -1> (x);
}

// square cube root
static inline Vec2d square_cbrt(Vec2d const x) {
    return cbrt_d<Vec2d, 2> (x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d cbrt(Vec4d const x) {
    return cbrt_d<Vec4d, 1> (x);
}

static inline Vec4d reciprocal_cbrt(Vec4d const x) {
    return cbrt_d<Vec4d, -1> (x);
}

static inline Vec4d square_cbrt(Vec4d const x) {
    return cbrt_d<Vec4d, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec8d cbrt(Vec8d const x) {
    return cbrt_d<Vec8d, 1> (x);
}

static inline Vec8d reciprocal_cbrt(Vec8d const x) {
    return cbrt_d<Vec8d, -1> (x);
}

static inline Vec8d square_cbrt(Vec8d const x) {
    return cbrt_d<Vec8d, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 512


// cube root template, single precision
// template parameters:
// VTYPE:  f.p. vector type
// CR:     -1 for reciprocal cube root, 1 for cube root, 2 for cube root squared
template<typename VTYPE, int CR> 
static inline VTYPE cbrt_f(VTYPE const x) {

    const int iter = 4;                          // iteration count of x^(-1/3) loop
    int i;

    typedef decltype(roundi(x)) ITYPE;           // integer vector type
    typedef decltype(x < x) BVTYPE;              // boolean vector type

    VTYPE  xa, xa3, a, a2;
    ITYPE  m1, m2;
    BVTYPE underflow;
    ITYPE  q1(0x54800000U);                      // exponent bias
    ITYPE  q2(0x002AAAAAU);                      // exponent multiplier for 1/3
    ITYPE  q3(0x00800000U);                      // denormal limit
    const  float one_third  = float(1./3.);
    const  float four_third = float(4./3.);

    xa  = abs(x);
    xa3 = one_third*xa;

    // multiply exponent by -1/3
    m1 = reinterpret_i(xa);
    m2 = q1 - (m1 >> 23) * q2;
    a  = reinterpret_f(m2);

    underflow = BVTYPE(m1 <= q3);                // true if denormal or zero

    // Newton Raphson iteration
    for (i = 0; i < iter-1; i++) {
        a2 = a*a;        
        a = nmul_add(xa3, a2*a2, four_third*a);  // a = four_third*a - xa3*a2*a2;
    }
    // last iteration with better precision
    a2 = a*a;    
    a = mul_add(one_third, nmul_add(xa, a2*a2, a), a); //a = a + one_third*(a - xa*a2*a2);

    if constexpr (CR == -1) {                    // reciprocal cube root
        // generate INF if underflow
        a = select(underflow, infinite_vec<VTYPE>(), a);
        a = select(is_inf(x), VTYPE(0), a);      // special case for INF                                                 // get sign
        a = sign_combine(a, x);
    }
    else if constexpr (CR == 1) {                // cube root
        a = a * a * x;
        a = select(underflow, 0.f, a);           // generate 0 if underflow
        a = select(is_inf(x), x, a);             // special case for INF
#ifdef SIGNED_ZERO
        a = a | (x & VTYPE(-0.0f));                     // get sign of x
#endif
    }
    else if constexpr (CR == 2) {                // cube root squared
        a = a * xa;                              // abs only to fix -INF
        a = select(underflow, 0., a);            // generate 0 if underflow
        a = select(is_inf(x), xa, a);            // special case for INF
    }
    return a;
}

// template instances for cbrt and reciprocal_cbrt

// cube root
static inline Vec4f cbrt(Vec4f const x) {
    return cbrt_f<Vec4f, 1> (x);
}

// reciprocal cube root
static inline Vec4f reciprocal_cbrt(Vec4f const x) {
    return cbrt_f<Vec4f, -1> (x);
}

// square cube root
static inline Vec4f square_cbrt(Vec4f const x) {
    return cbrt_f<Vec4f, 2> (x);
}

#if MAX_VECTOR_SIZE >= 256

static inline Vec8f cbrt(Vec8f const x) {
    return cbrt_f<Vec8f, 1> (x);
}

static inline Vec8f reciprocal_cbrt(Vec8f const x) {
    return cbrt_f<Vec8f, -1> (x);
}

static inline Vec8f square_cbrt(Vec8f const x) {
    return cbrt_f<Vec8f, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

static inline Vec16f cbrt(Vec16f const x) {
    return cbrt_f<Vec16f, 1> (x);
}

static inline Vec16f reciprocal_cbrt(Vec16f const x) {
    return cbrt_f<Vec16f, -1> (x);
}

static inline Vec16f square_cbrt(Vec16f const x) {
    return cbrt_f<Vec16f, 2> (x);
}

#endif // MAX_VECTOR_SIZE >= 512



/* ****************************************************************************
                    pow functions
*******************************************************************************
Note about standard conformance:
This implementation of a pow function differs from the IEEE 754-2008 floating 
point standard regarding nan propagation.
The standard has pow(nan,0) = 1, and pow(1,nan) = 1, probably for historic reasons.
The present implementation is guaranteed to always propagate nan's for reasons
explained in this report:
Agner Fog: "NAN propagation versus fault trapping in floating point code", 2019,
https://www.agner.org/optimize/nan_propagation.pdf

The standard defines another function, powr, which propagates NAN's, but powr
will be less useful to programmers because it does not allow integer powers of
negative x. 

******************************************************************************/

// Helper functions:

#if MAX_VECTOR_SIZE >= 512

// Helper function for power function: insert special values of pow(x,y) when x=0:
// y<0 -> inf, y=0 -> 1, y>0 -> 0, y=nan -> nan
static inline Vec8d wm_pow_case_x0(Vec8db const xiszero, Vec8d const y, Vec8d const z) {
#if INSTRSET >= 9
    const __m512i table = Vec8q(0x85858A00);
    return _mm512_mask_fixupimm_pd(z, uint8_t(xiszero), y, table, 0);
#else
    return select(xiszero, select(y < 0., infinite_vec<Vec8d>(), select(y == 0., Vec8d(1.), Vec8d(0.))), z);
#endif
}

// Helper function for power function: insert special values of pow(x,y) when x=0:
// y<0 -> inf, y=0 -> 1, y>0 -> 0, y=nan -> nan
static inline Vec16f wm_pow_case_x0(Vec16fb const xiszero, Vec16f const y, Vec16f const z) {
#if INSTRSET >= 9
    const __m512i table = Vec16ui(0x85858A00);
    return _mm512_mask_fixupimm_ps(z, xiszero, y, table, 0);
#else
    return select(xiszero, select(y < 0.f, infinite_vec<Vec16f>(), select(y == 0.f, Vec16f(1.f), Vec16f(0.f))), z);
#endif
}
  
#endif

#if MAX_VECTOR_SIZE >= 256

static inline Vec4d wm_pow_case_x0(Vec4db const xiszero, Vec4d const y, Vec4d const z) {
//#if INSTRSET >= 10
    //const __m256i table = Vec4q(0x85858A00);
    //return _mm256_mask_fixupimm_pd(z, xiszero, y, table, 0);
//#else
    return select(xiszero, select(y < 0., infinite_vec<Vec4d>(), select(y == 0., Vec4d(1.), Vec4d(0.))), z);
//#endif
}

static inline Vec8f wm_pow_case_x0(Vec8fb const xiszero, Vec8f const y, Vec8f const z) {
    return select(xiszero, select(y < 0.f, infinite_vec<Vec8f>(), select(y == 0.f, Vec8f(1.f), Vec8f(0.f))), z);
}

#endif

static inline Vec2d wm_pow_case_x0(Vec2db const xiszero, Vec2d const y, Vec2d const z) {
//#if INSTRSET >= 10
//    const __m128i table = Vec2q(0x85858A00);
//    return _mm_mask_fixupimm_pd(z, xiszero, y, table, 0);
//#else
    return select(xiszero, select(y < 0., infinite_vec<Vec2d>(), select(y == 0., Vec2d(1.), Vec2d(0.))), z);
//#endif
}

static inline Vec4f wm_pow_case_x0(Vec4fb const xiszero, Vec4f const y, Vec4f const z) {
    return select(xiszero, select(y < 0.f, infinite_vec<Vec4f>(), select(y == 0.f, Vec4f(1.f), Vec4f(0.f))), z);
}


// ****************************************************************************
//                pow template, double precision
// ****************************************************************************
// Calculate x to the power of y.

// Precision is important here because rounding errors get multiplied by y.
// The logarithm is calculated with extra precision, and the exponent is 
// calculated separately.
// The logarithm is calculated by Pade approximation with 6'th degree 
// polynomials. A 7'th degree would be preferred for best precision by high y.
// The alternative method: log(x) = z + z^3*R(z)/S(z), where z = 2(x-1)/(x+1)
// did not give better precision.

// Template parameters:
// VTYPE:  data vector type
template <typename VTYPE>
static inline VTYPE pow_template_d(VTYPE const x0, VTYPE const y) {

    // define constants
    const double ln2d_hi = 0.693145751953125;           // log(2) in extra precision, high bits
    const double ln2d_lo = 1.42860682030941723212E-6;   // low bits of log(2)
    const double log2e   = VM_LOG2E;                    // 1/log(2)

    // coefficients for Pade polynomials
    const double P0logl =  2.0039553499201281259648E1;
    const double P1logl =  5.7112963590585538103336E1;
    const double P2logl =  6.0949667980987787057556E1;
    const double P3logl =  2.9911919328553073277375E1;
    const double P4logl =  6.5787325942061044846969E0;
    const double P5logl =  4.9854102823193375972212E-1;
    const double P6logl =  4.5270000862445199635215E-5;
    const double Q0logl =  6.0118660497603843919306E1;
    const double Q1logl =  2.1642788614495947685003E2;
    const double Q2logl =  3.0909872225312059774938E2;
    const double Q3logl =  2.2176239823732856465394E2;
    const double Q4logl =  8.3047565967967209469434E1;
    const double Q5logl =  1.5062909083469192043167E1;

    // Taylor coefficients for exp function, 1/n!
    const double p2  = 1./2.;
    const double p3  = 1./6.;
    const double p4  = 1./24.;
    const double p5  = 1./120.; 
    const double p6  = 1./720.; 
    const double p7  = 1./5040.; 
    const double p8  = 1./40320.; 
    const double p9  = 1./362880.; 
    const double p10 = 1./3628800.; 
    const double p11 = 1./39916800.; 
    const double p12 = 1./479001600.; 
    const double p13 = 1./6227020800.;

    typedef decltype(roundi(x0)) ITYPE;          // integer vector type
    typedef decltype(x0 < x0) BVTYPE;            // boolean vector type

    // data vectors
    VTYPE x, x1, x2;                             // x variable
    VTYPE px, qx, ef, yr, v;                     // calculation of logarithm
    VTYPE lg, lg1, lg2;
    VTYPE lgerr, x2err;
    VTYPE e1, e2, ee; 
    VTYPE e3, z, z1;                             // calculation of exp and pow
    VTYPE yodd(0);                               // has sign bit set if y is an odd integer
    // integer vectors
    ITYPE ei, ej;
    // boolean vectors
    BVTYPE blend, xzero, xsign;                  // x conditions
    BVTYPE overflow, underflow, xfinite, yfinite, efinite; // error conditions

    // remove sign
    x1 = abs(x0);

    // Separate mantissa from exponent 
    // This gives the mantissa * 0.5
    x  = fraction_2(x1);

    // reduce range of x = +/- sqrt(2)/2
    blend = x > VM_SQRT2*0.5;
    x  = if_add(!blend, x, x);                   // conditional add

    // Pade approximation
    // Higher precision than in log function. Still higher precision wanted
    x -= 1.0;
    x2 = x*x;
    px = polynomial_6  (x, P0logl, P1logl, P2logl, P3logl, P4logl, P5logl, P6logl);
    px *= x * x2;
    qx = polynomial_6n (x, Q0logl, Q1logl, Q2logl, Q3logl, Q4logl, Q5logl);
    lg1 = px / qx;
 
    // extract exponent
    ef = exponent_f(x1);
    ef = if_add(blend, ef, 1.);                  // conditional add

    // multiply exponent by y
    // nearest integer e1 goes into exponent of result, remainder yr is added to log
    e1 = round(ef * y);
    yr = mul_sub_x(ef, y, e1);                   // calculate remainder yr. precision very important here

    // add initial terms to Pade expansion
    lg = nmul_add(0.5, x2, x) + lg1;             // lg = (x - 0.5 * x2) + lg1;
    // calculate rounding errors in lg
    // rounding error in multiplication 0.5*x*x
    x2err = mul_sub_x(0.5*x, x, 0.5*x2);
    // rounding error in additions and subtractions
    lgerr = mul_add(0.5, x2, lg - x) - lg1;      // lgerr = ((lg - x) + 0.5 * x2) - lg1;

    // extract something for the exponent
    e2 = round(lg * y * VM_LOG2E);
    // subtract this from lg, with extra precision
    v = mul_sub_x(lg, y, e2 * ln2d_hi);
    v = nmul_add(e2, ln2d_lo, v);                // v -= e2 * ln2d_lo;

    // add remainder from ef * y
    v = mul_add(yr, VM_LN2, v);                  // v += yr * VM_LN2;

    // correct for previous rounding errors
    v = nmul_add(lgerr + x2err, y, v);           // v -= (lgerr + x2err) * y;

    // exp function

    // extract something for the exponent if possible
    x = v;
    e3 = round(x*log2e);
    // high precision multiplication not needed here because abs(e3) <= 1
    x = nmul_add(e3, VM_LN2, x);                 // x -= e3 * VM_LN2;

    z = polynomial_13m(x, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13);
    z = z + 1.0;

    // contributions to exponent
    ee = e1 + e2 + e3;
    //ei = round_to_int64_limited(ee);
    ei = roundi(ee);
    // biased exponent of result:
    ej = ei + (ITYPE(reinterpret_i(z)) >> 52);
    // check exponent for overflow and underflow
    overflow  = BVTYPE(ej >= 0x07FF) | (ee >  3000.);
    underflow = BVTYPE(ej <= 0x0000) | (ee < -3000.);

    // add exponent by integer addition
    z = reinterpret_d(ITYPE(reinterpret_i(z)) + (ei << 52));

    // check for special cases
    xfinite   = is_finite(x0);
    yfinite   = is_finite(y);
    efinite   = is_finite(ee);
    xzero     = is_zero_or_subnormal(x0);
    xsign     = sign_bit(x0);  // sign of x0. include -0.

    // check for overflow and underflow
    if (horizontal_or(overflow | underflow)) {
        // handle errors
        z = select(underflow, VTYPE(0.), z);
        z = select(overflow, infinite_vec<VTYPE>(), z);
    }

    // check for x == 0
    z = wm_pow_case_x0(xzero, y, z);
    //z = select(xzero, select(y < 0., infinite_vec<VTYPE>(), select(y == 0., VTYPE(1.), VTYPE(0.))), z);

    // check for sign of x (include -0.). y must be integer
    if (horizontal_or(xsign)) {
        // test if y is an integer
        BVTYPE yinteger = y == round(y);
        // test if y is odd: convert to int and shift bit 0 into position of sign bit.
        // this will be 0 if overflow
        yodd = reinterpret_d(roundi(y) << 63);
        z1 = select(yinteger, z | yodd,                    // y is integer. get sign if y is odd
            select(x0 == 0., z, nan_vec<VTYPE>(NAN_POW))); // NAN unless x0 == -0.
        yodd = select(yinteger, yodd, 0.);                 // yodd used below. only if y is integer
        z = select(xsign, z1, z);
    }

    // check for range errors
    if (horizontal_and(xfinite & yfinite & (efinite | xzero))) {
        // fast return if no special cases
        return z;
    }

    // handle special error cases: y infinite
    z1 = select(yfinite & efinite, z, 
        select(x1 == 1., VTYPE(1.), 
            select((x1 > 1.) ^ sign_bit(y), infinite_vec<VTYPE>(), 0.)));

    // handle x infinite
    z1 = select(xfinite, z1, 
        select(y == 0., VTYPE(1.), 
            select(y < 0., yodd & z,      // 0.0 with the sign of z from above
                abs(x0) | (x0 & yodd)))); // get sign of x0 only if y is odd integer

    // Always propagate nan:
    // Deliberately differing from the IEEE-754 standard which has pow(0,nan)=1, and pow(1,nan)=1
    z1 = select(is_nan(x0)|is_nan(y), x0+y, z1);

    return z1;
}


//This template is in vectorf128.h to prevent implicit conversion of float y to int when float version is not defined:
//template <typename TT> static Vec2d pow(Vec2d const a, TT n);

// instantiations of pow_template_d:
template <>
inline Vec2d pow<Vec2d>(Vec2d const x, Vec2d const y) {
    return pow_template_d(x, y);
}

template <>
inline Vec2d pow<double>(Vec2d const x, double const y) {
    return pow_template_d<Vec2d>(x, y);
}
template <>
inline Vec2d pow<float>(Vec2d const x, float const y) {
    return pow_template_d<Vec2d>(x, (double)y);
}

#if MAX_VECTOR_SIZE >= 256

template <>
inline Vec4d pow<Vec4d>(Vec4d const x, Vec4d const y) {
    return pow_template_d(x, y);
}

template <>
inline Vec4d pow<double>(Vec4d const x, double const y) {
    return pow_template_d<Vec4d>(x, y);
}

template <>
inline Vec4d pow<float>(Vec4d const x, float const y) {
    return pow_template_d<Vec4d>(x, (double)y);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

template <>
inline Vec8d pow<Vec8d>(Vec8d const x, Vec8d const y) {
    return pow_template_d(x, y);
}

template <>
inline Vec8d pow<double>(Vec8d const x, double const y) {
    return pow_template_d<Vec8d>(x, y);
}

template <>
inline Vec8d pow<float>(Vec8d const x, float const y) {
    return pow_template_d<Vec8d>(x, (double)y);
}

#endif // MAX_VECTOR_SIZE >= 512


// ****************************************************************************
//                pow template, single precision
// ****************************************************************************

// Template parameters:
// VTYPE:  data vector type
// Calculate x to the power of y
template <typename VTYPE>
static inline VTYPE pow_template_f(VTYPE const x0, VTYPE const y) {

    // define constants
    const float ln2f_hi  =  0.693359375f;        // log(2), split in two for extended precision
    const float ln2f_lo  = -2.12194440e-4f;
    const float log2e    =  float(VM_LOG2E);     // 1/log(2)

    const float P0logf  =  3.3333331174E-1f;     // coefficients for logarithm expansion
    const float P1logf  = -2.4999993993E-1f;
    const float P2logf  =  2.0000714765E-1f;
    const float P3logf  = -1.6668057665E-1f;
    const float P4logf  =  1.4249322787E-1f;
    const float P5logf  = -1.2420140846E-1f;
    const float P6logf  =  1.1676998740E-1f;
    const float P7logf  = -1.1514610310E-1f;
    const float P8logf  =  7.0376836292E-2f;

    const float p2expf   =  1.f/2.f;             // coefficients for Taylor expansion of exp
    const float p3expf   =  1.f/6.f;
    const float p4expf   =  1.f/24.f;
    const float p5expf   =  1.f/120.f; 
    const float p6expf   =  1.f/720.f; 
    const float p7expf   =  1.f/5040.f; 

    typedef decltype(roundi(x0)) ITYPE;          // integer vector type
    typedef decltype(x0 < x0) BVTYPE;            // boolean vector type

    // data vectors
    VTYPE x, x1, x2;                             // x variable
    VTYPE ef, e1, e2, e3, ee;                    // exponent
    VTYPE yr;                                    // remainder
    VTYPE lg, lg1, lgerr, x2err, v;              // logarithm
    VTYPE z, z1;                                 // pow(x,y)
    VTYPE yodd(0);                               // has sign bit set if y is an odd integer
    // integer vectors
    ITYPE ei, ej;                                // exponent
    // boolean vectors
    BVTYPE blend, xzero, xsign;                  // x conditions
    BVTYPE overflow, underflow, xfinite, yfinite, efinite; // error conditions

    // remove sign
    x1 = abs(x0);

    // Separate mantissa from exponent 
    // This gives the mantissa * 0.5
    x  = fraction_2(x1);

    // reduce range of x = +/- sqrt(2)/2
    blend = x > float(VM_SQRT2 * 0.5);
    x  = if_add(!blend, x, x);                   // conditional add

    // Taylor expansion, high precision
    x   -= 1.0f;
    x2   = x * x;
    lg1  = polynomial_8(x, P0logf, P1logf, P2logf, P3logf, P4logf, P5logf, P6logf, P7logf, P8logf);
    lg1 *= x2 * x; 
 
    // extract exponent
    ef = exponent_f(x1);
    ef = if_add(blend, ef, 1.0f);                // conditional add

    // multiply exponent by y
    // nearest integer e1 goes into exponent of result, remainder yr is added to log
    e1 = round(ef * y);
    yr = mul_sub_x(ef, y, e1);                   // calculate remainder yr. precision very important here

    // add initial terms to expansion
    lg = nmul_add(0.5f, x2, x) + lg1;            // lg = (x - 0.5f * x2) + lg1;

    // calculate rounding errors in lg
    // rounding error in multiplication 0.5*x*x
    x2err = mul_sub_x(0.5f*x, x, 0.5f * x2);
    // rounding error in additions and subtractions
    lgerr = mul_add(0.5f, x2, lg - x) - lg1;     // lgerr = ((lg - x) + 0.5f * x2) - lg1;

    // extract something for the exponent
    e2 = round(lg * y * float(VM_LOG2E));
    // subtract this from lg, with extra precision
    v = mul_sub_x(lg, y, e2 * ln2f_hi);
    v = nmul_add(e2, ln2f_lo, v);                // v -= e2 * ln2f_lo;

    // correct for previous rounding errors
    v -= mul_sub(lgerr + x2err, y, yr * float(VM_LN2)); // v -= (lgerr + x2err) * y - yr * float(VM_LN2) ;

    // exp function

    // extract something for the exponent if possible
    x = v;
    e3 = round(x*log2e);
    // high precision multiplication not needed here because abs(e3) <= 1
    x = nmul_add(e3, float(VM_LN2), x);          // x -= e3 * float(VM_LN2);

    // Taylor polynomial
    x2  = x  * x;
    z = polynomial_5(x, p2expf, p3expf, p4expf, p5expf, p6expf, p7expf)*x2 + x + 1.0f;

    // contributions to exponent
    ee = e1 + e2 + e3;
    ei = roundi(ee);
    // biased exponent of result:
    ej = ei + (ITYPE(reinterpret_i(z)) >> 23);
    // check exponent for overflow and underflow
    overflow  = BVTYPE(ej >= 0x0FF) | (ee >  300.f);
    underflow = BVTYPE(ej <= 0x000) | (ee < -300.f);

    // add exponent by integer addition
    z = reinterpret_f(ITYPE(reinterpret_i(z)) + (ei << 23)); // the extra 0x10000 is shifted out here

    // check for special cases
    xfinite   = is_finite(x0);
    yfinite   = is_finite(y);
    efinite   = is_finite(ee);

    xzero     = is_zero_or_subnormal(x0);
    xsign     = sign_bit(x0);  // x is negative or -0.

    // check for overflow and underflow
    if (horizontal_or(overflow | underflow)) {
        // handle errors
        z = select(underflow, VTYPE(0.f), z);
        z = select(overflow, infinite_vec<VTYPE>(), z);
    }

    // check for x == 0
    z = wm_pow_case_x0(xzero, y, z);
    //z = select(xzero, select(y < 0.f, infinite_vec<VTYPE>(), select(y == 0.f, VTYPE(1.f), VTYPE(0.f))), z);

    // check for sign of x (include -0.). y must be integer
    if (horizontal_or(xsign)) {
        // test if y is an integer
        BVTYPE yinteger = y == round(y);
        // test if y is odd: convert to int and shift bit 0 into position of sign bit.
        // this will be 0 if overflow
        yodd = reinterpret_f(roundi(y) << 31);
        z1 = select(yinteger, z | yodd,                    // y is integer. get sign if y is odd
            select(x0 == 0.f, z, nan_vec<VTYPE>(NAN_POW)));// NAN unless x0 == -0.
        yodd = select(yinteger, yodd, 0);                  // yodd used below. only if y is integer
        z = select(xsign, z1, z);
    }

    // check for range errors
    if (horizontal_and(xfinite & yfinite & (efinite | xzero))) {
        return z;            // fast return if no special cases
    }

    // handle special error cases: y infinite
    z1 = select(yfinite & efinite, z, 
        select(x1 == 1.f, VTYPE(1.f), 
            select((x1 > 1.f) ^ sign_bit(y), infinite_vec<VTYPE>(), 0.f)));

    // handle x infinite
    z1 = select(xfinite, z1, 
        select(y == 0.f, VTYPE(1.f), 
            select(y < 0.f, yodd & z,     // 0.0 with the sign of z from above
                abs(x0) | (x0 & yodd)))); // get sign of x0 only if y is odd integer

    // Always propagate nan:
    // Deliberately differing from the IEEE-754 standard which has pow(0,nan)=1, and pow(1,nan)=1
    z1 = select(is_nan(x0)|is_nan(y), x0+y, z1);
    return z1;
}

//This template is in vectorf128.h to prevent implicit conversion of float y to int when float version is not defined:
//template <typename TT> static Vec4f pow(Vec4f const a, TT n);

template <>
inline Vec4f pow<Vec4f>(Vec4f const x, Vec4f const y) {
    return pow_template_f(x, y);
}

template <>
inline Vec4f pow<float>(Vec4f const x, float const y) {
    return pow_template_f<Vec4f>(x, y);
}

template <>
inline Vec4f pow<double>(Vec4f const x, double const y) {
    return pow_template_f<Vec4f>(x, (float)y);
}

#if MAX_VECTOR_SIZE >= 256

template <>
inline Vec8f pow<Vec8f>(Vec8f const x, Vec8f const y) {
    return pow_template_f(x, y);
}

template <>
inline Vec8f pow<float>(Vec8f const x, float const y) {
    return pow_template_f<Vec8f>(x, y);
}
template <>
inline Vec8f pow<double>(Vec8f const x, double const y) {
    return pow_template_f<Vec8f>(x, (float)y);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

template <>
inline Vec16f pow<Vec16f>(Vec16f const x, Vec16f const y) {
    return pow_template_f(x, y);
}

template <>
inline Vec16f pow<float>(Vec16f const x, float const y) {
    return pow_template_f<Vec16f>(x, y);
}

template <>
inline Vec16f pow<double>(Vec16f const x, double const y) {
    return pow_template_f<Vec16f>(x, (float)y);
}

#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             power function with rational exponent
// *************************************************************

// macro to call template power_rational
#define pow_ratio(x, a, b) (power_rational<decltype(x+x), a, b> (x))

// Power function with rational exponent: pow(x,a/b)
template <typename V, int a0, int b0>
V power_rational (V const x) {

    // constexpr lambda to reduce rational number a/b
    auto reduce_rational = [](int const aa, int const bb) constexpr {
        int a = aa, b = bb;
        if (b < 0) {
            a = -a; b = -b;                           // make b positive
        }
        while ((((a | b) & 1) == 0) && b > 0) {       // prime factor 2
            a /= 2;  b /= 2;
        }
        while (a % 3 == 0 && b % 3 == 0 && b > 0) {   // prime factor 3
            a /= 3;  b /= 3;
        }
        while (a % 5 == 0 && b % 5 == 0 && b > 0) {   // prime factor 5
            a /= 5;  b /= 5;
        }
        return bb / b;                                // return common denominator
    };
    constexpr int d = reduce_rational(a0, b0);
    constexpr int a = a0 / d;
    constexpr int b = b0 / d;

    // special cases
    if constexpr (a == 0) return V(1.f);

    else if constexpr (b == 1) return pow_n<V,a>(x);

    else if constexpr (b == 2) {
        V y, t = sqrt(x);
        if constexpr (a == 1) y = t;
        else if constexpr (a == -1) y = V(1.f) / t;
        else {
            constexpr int a2 = a > 0 ? a / 2 : (a - 1) / 2;
            y = pow_n<V, a2>(x) * t;
        }
#ifdef SIGNED_ZERO
        y = abs(y);    // pow(-0., a/2.) must be +0.
#endif
        return y;
    }

    else if constexpr (b == 3) {
        V y;
        constexpr int a3 = a % 3;
        if constexpr (a3 == -2) {
            V t = reciprocal_cbrt(x);
            t *= t;
            if constexpr (a == -2) y = t;
            else y = t / pow_n<V, (-a-2)/3>(x);
        }
        else if constexpr (a3 == -1) {
            V t = reciprocal_cbrt(x);
            if constexpr (a == -1) y = t;
            else y = t / pow_n<V, (-a-1)/3>(x);       // fail if INF          
        }
        else if constexpr (a3 == 1) {
            V t = cbrt(x);
            if constexpr (a == 1) y = t;
            else y = t * pow_n<V, a/3>(x);
        }
        else if constexpr (a3 == 2) {
            V t = square_cbrt(x);
            if constexpr (a == 2) y = t;
            else y = t * pow_n<V, a/3>(x);
        }
        return y;
    }

    else if constexpr (b == 4) {
        constexpr int a4 = a % 4;
        V s1, s2, y;
        s1 = sqrt(x);
        if ((a & 1) == 1) s2 = sqrt(s1);

        if constexpr (a4 == -3) {
            y = s2 / pow_n<V, 1+(-a)/4>(x);
        }
        else if constexpr (a4 == -1) {
            if constexpr (a != -1) s2 *= pow_n<V, (-a)/4>(x);
            y = V(1.f) / s2;
        }
        else if constexpr (a4 == 1) {
            if constexpr (a == 1) y = s2;
            else y = s2 * pow_n<V, a/4>(x);
        }
        else if constexpr (a4 == 3) {
            V t = s1 * s2;
            if constexpr (a != 3) t *= pow_n<V, a/4>(x);
            y = t;
        }
#ifdef SIGNED_ZERO
        y = abs(y);
#endif
        return y;
    }

    else if constexpr (b == 6) {
        constexpr int a6 = a % 6;
        V y;
        if constexpr (a6 == -5) {
            V t = cbrt(sqrt(x)) / x;
            if constexpr (a != -5) t /= pow_n<V, (-a)/6>(x);
            y = t;            
        }
        else if constexpr (a6 == -1) {
            V t = reciprocal_cbrt(sqrt(x));
            if constexpr (a != -1) t /= pow_n<V, (-a)/6>(x);
            y = t;
        }
        else if constexpr (a6 == 1) {
            V t = cbrt(sqrt(x));
            if constexpr (a != 1) t *= pow_n<V, a/6>(x);
            y = t;
        }
        else if constexpr (a6 == 5) {
            V s1 = sqrt(x);
            V t = cbrt(s1);
            t = t*t*s1;
            if constexpr (a != 5) t *= pow_n<V, a/6>(x);
            y = t;
        }
#ifdef SIGNED_ZERO
        y = abs(y);
#endif
        return y;
    }

    else if constexpr (b == 8) {
        V s1 = sqrt(x);                // x^(1/2)
        V s2 = sqrt(s1);               // x^(1/4)
        V s3 = sqrt(s2);               // x^(1/8)
        V y;
        constexpr int a8 = a % 8;
        if constexpr (a8 == -7) {
            y = s3 / pow_n<V, 1+(-a)/8>(x);
        }
        else if constexpr (a8 == -5) {
            y = s3 * (s2 / pow_n<V, 1+(-a)/8>(x));
        }
        else if constexpr (a8 == -3) {
            y = s3 * (s1 / pow_n<V, 1+(-a)/8>(x));
        }
        else if constexpr (a8 == -1) {
            if constexpr (a != -1) s3 *= pow_n<V, (-a)/8>(x);
            y = V(1.f) / s3;
        }
        else if constexpr (a8 == 1) {
            if constexpr (a == 1) y = s3;
            else y = s3 * pow_n<V, a/8>(x);
        }
        else if constexpr (a8 == 3) {
            V t = s2 * s3;
            if constexpr (a != 3) t *= pow_n<V, a/8>(x);
            y = t;
        }
        else if constexpr (a8 == 5) {
            V t = s1 * s3;
            if constexpr (a != 5) t *= pow_n<V, a/8>(x);
            y = t;
        }
        else if constexpr (a8 == 7) {
            V t = s2 * s3;
            if constexpr (a != 7) s1 *= pow_n<V, a/8>(x);
            t *= s1;
            y = t;
        }
#ifdef SIGNED_ZERO
        y = abs(y);
#endif
        return y;
    }

    else {
        // general case
        V y = x;
        // negative x allowed when b odd or a even
        // (if a is even then either b is odd or a/b can be reduced, 
        // but we can check a even anyway at no cost to be sure)
        if constexpr (((b | ~a) & 1) == 1) y = abs(y);
        y = pow(y, (double(a) / double(b)));
        if constexpr ((a & b & 1) == 1) y = sign_combine(y, x); // apply sign if a and b both odd
        return y;
    }
}



/******************************************************************************
*                 Detect NAN codes
*
* These functions return the code hidden in a NAN. The sign bit is ignored
******************************************************************************/

static inline Vec4ui nan_code(Vec4f const x) {
    Vec4ui a = Vec4ui(reinterpret_i(x));
    Vec4ui const n = 0x007FFFFF;
    return select(Vec4ib(is_nan(x)), a & n, 0);
}

// This function returns the code hidden in a NAN. The sign bit is ignored
static inline Vec2uq nan_code(Vec2d const x) {
    Vec2uq a = Vec2uq(reinterpret_i(x));
    return select(Vec2qb(is_nan(x)), a << 12 >> (12+29), 0);
}

#if MAX_VECTOR_SIZE >= 256

// This function returns the code hidden in a NAN. The sign bit is ignored
static inline Vec8ui nan_code(Vec8f const x) {
    Vec8ui a = Vec8ui(reinterpret_i(x));
    Vec8ui const n = 0x007FFFFF;
    return select(Vec8ib(is_nan(x)), a & n, 0);
}

// This function returns the code hidden in a NAN. The sign bit is ignored
static inline Vec4uq nan_code(Vec4d const x) {
    Vec4uq a = Vec4uq(reinterpret_i(x));
    return select(Vec4qb(is_nan(x)), a << 12 >> (12+29), 0);
}

#endif // MAX_VECTOR_SIZE >= 256 
#if MAX_VECTOR_SIZE >= 512

// This function returns the code hidden in a NAN. The sign bit is ignored
static inline Vec16ui nan_code(Vec16f const x) {
    Vec16ui a = Vec16ui(reinterpret_i(x));
    Vec16ui const n = 0x007FFFFF;
    return select(Vec16ib(is_nan(x)), a & n, 0);
}

// This function returns the code hidden in a NAN. The sign bit is ignored
static inline Vec8uq nan_code(Vec8d const x) {
    Vec8uq a = Vec8uq(reinterpret_i(x));
    return select(Vec8qb(is_nan(x)), a << 12 >> (12+29), 0);
}

#endif // MAX_VECTOR_SIZE >= 512

#ifdef VCL_NAMESPACE
}
#endif

#endif  // VECTORMATH_EXP_H
