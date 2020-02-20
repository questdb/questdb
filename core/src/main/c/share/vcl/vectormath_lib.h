/****************************  vectormath_lib.h   *****************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-08-01
* Version:       2.00.00
* Project:       vector class library
* Description:
* Header file defining mathematical functions on floating point vectors
* using Intel SVML library
*
* Instructions to use SVML library:
* Include this file and link with svml
*
* Alternatively, use the inline math functions by including 
* vectormath_exp.h for power and exponential functions
* vectormath_trig.h for trigonometric functions
* vectormath_hyp.h for hyperbolic functions
* 
* For detailed instructions, see vcl_manual.pdf
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
\*****************************************************************************/

// check combination of header files
#ifndef VECTORMATH_LIB_H
#define VECTORMATH_LIB_H  1

#ifdef VECTORMATH_COMMON_H
#error conflicting header files. More than one implementation of mathematical functions included
#else

#include "vectorclass.h"     // make sure vector classes are defined first

#ifdef   VCL_NAMESPACE
namespace VCL_NAMESPACE {    // optional name space
#endif


#ifdef __INTEL_COMPILER
/*****************************************************************************
*
*      128-bit vector functions using Intel compiler
*
*****************************************************************************/

// exponential and power functions
static inline Vec4f exp(Vec4f const x) {    // exponential function
    return _mm_exp_ps(x);
}
static inline Vec2d exp(Vec2d const x) {    // exponential function
    return _mm_exp_pd(x);
}
static inline Vec4f expm1(Vec4f const x) {  // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm_expm1_ps(x);
}
static inline Vec2d expm1(Vec2d const x) {  // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm_expm1_pd(x);
}
static inline Vec4f exp2(Vec4f const x) {   // pow(2,x)
    return _mm_exp2_ps(x);
}
static inline Vec2d exp2(Vec2d const x) {   // pow(2,x)
    return _mm_exp2_pd(x);
}
static inline Vec4f exp10(Vec4f const x) {  // pow(10,x)
    return _mm_exp10_ps(x);
}
static inline Vec2d exp10(Vec2d const x) {  // pow(10,x)
    return _mm_exp10_pd(x);
}
static inline Vec4f pow(Vec4f const a, Vec4f const b) {    // pow(a,b) = a to the power of b
    return _mm_pow_ps(a, b);
}
static inline Vec4f pow(Vec4f const a, float const b) {    // pow(a,b) = a to the power of b
    return _mm_pow_ps(a, Vec4f(b));
}
static inline Vec2d pow(Vec2d const a, Vec2d const b) {    // pow(a,b) = a to the power of b
    return _mm_pow_pd(a, b);
}
static inline Vec2d pow(Vec2d const a, double const b) {   // pow(a,b) = a to the power of b
    return _mm_pow_pd(a, Vec2d(b));
}
static inline Vec4f cbrt(Vec4f const x) {   // pow(x,1/3)
    return _mm_cbrt_ps(x);
}
static inline Vec2d cbrt(Vec2d const x) {   // pow(x,1/3)
    return _mm_cbrt_pd(x);
}
// logarithms
static inline Vec4f log(Vec4f const x) {    // natural logarithm
    return _mm_log_ps(x);
}
static inline Vec2d log(Vec2d const x) {    // natural logarithm
    return _mm_log_pd(x);
}
static inline Vec4f log1p(Vec4f const x) {  // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm_log1p_ps(x);
}
static inline Vec2d log1p(Vec2d const x) {  // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm_log1p_pd(x);
}
static inline Vec4f log2(Vec4f const x) {   // logarithm base 2
    return _mm_log2_ps(x);
}
static inline Vec2d log2(Vec2d const x) {   // logarithm base 2
    return _mm_log2_pd(x);
}
static inline Vec4f log10(Vec4f const x) {  // logarithm base 10
    return _mm_log10_ps(x);
}
static inline Vec2d log10(Vec2d const x) {  // logarithm base 10
    return _mm_log10_pd(x);
}

// trigonometric functions
static inline Vec4f sin(Vec4f const x) {    // sine
    return _mm_sin_ps(x);
}
static inline Vec2d sin(Vec2d const x) {    // sine
    return _mm_sin_pd(x);
}
static inline Vec4f cos(Vec4f const x) {    // cosine
    return _mm_cos_ps(x);
}
static inline Vec2d cos(Vec2d const x) {    // cosine
    return _mm_cos_pd(x);
} 
static inline Vec4f sincos(Vec4f * pcos, Vec4f const x) {  // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128 r_sin, r_cos;
    r_sin = _mm_sincos_ps(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec2d sincos(Vec2d * pcos, Vec2d const x) {  // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128d r_sin, r_cos;
    r_sin = _mm_sincos_pd(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec4f tan(Vec4f const x) {    // tangent
    return _mm_tan_ps(x);
}
static inline Vec2d tan(Vec2d const x) {    // tangent
    return _mm_tan_pd(x);
}

// inverse trigonometric functions
static inline Vec4f asin(Vec4f const x) {   // inverse sine
    return _mm_asin_ps(x);
}
static inline Vec2d asin(Vec2d const x) {   // inverse sine
    return _mm_asin_pd(x);
}

static inline Vec4f acos(Vec4f const x) {   // inverse cosine
    return _mm_acos_ps(x);
}
static inline Vec2d acos(Vec2d const x) {   // inverse cosine
    return _mm_acos_pd(x);
}

static inline Vec4f atan(Vec4f const x) {   // inverse tangent
    return _mm_atan_ps(x);
}
static inline Vec2d atan(Vec2d const x) {   // inverse tangent
    return _mm_atan_pd(x);
}
static inline Vec4f atan2(Vec4f const a, Vec4f const b) {  // inverse tangent of a/b
    return _mm_atan2_ps(a, b);
}
static inline Vec2d atan2(Vec2d const a, Vec2d const b) {  // inverse tangent of a/b
    return _mm_atan2_pd(a, b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec4f sinh(Vec4f const x) {   // hyperbolic sine
    return _mm_sinh_ps(x);
}
static inline Vec2d sinh(Vec2d const x) {   // hyperbolic sine
    return _mm_sinh_pd(x);
}
static inline Vec4f cosh(Vec4f const x) {   // hyperbolic cosine
    return _mm_cosh_ps(x);
}
static inline Vec2d cosh(Vec2d const x) {   // hyperbolic cosine
    return _mm_cosh_pd(x);
}
static inline Vec4f tanh(Vec4f const x) {   // hyperbolic tangent
    return _mm_tanh_ps(x);
}
static inline Vec2d tanh(Vec2d const x) {   // hyperbolic tangent
    return _mm_tanh_pd(x);
}
static inline Vec4f asinh(Vec4f const x) {  // inverse hyperbolic sine
    return _mm_asinh_ps(x);
}
static inline Vec2d asinh(Vec2d const x) {  // inverse hyperbolic sine
    return _mm_asinh_pd(x);
}
static inline Vec4f acosh(Vec4f const x) {  // inverse hyperbolic cosine
    return _mm_acosh_ps(x);
}
static inline Vec2d acosh(Vec2d const x) {  // inverse hyperbolic cosine
    return _mm_acosh_pd(x);
}
static inline Vec4f atanh(Vec4f const x) {  // inverse hyperbolic tangent
    return _mm_atanh_ps(x);
}
static inline Vec2d atanh(Vec2d const x) {  // inverse hyperbolic tangent
    return _mm_atanh_pd(x);
}

// error function
static inline Vec4f erf(Vec4f const x) {    // error function
    return _mm_erf_ps(x);
}
static inline Vec2d erf(Vec2d const x) {    // error function
    return _mm_erf_pd(x);
} 
static inline Vec4f erfc(Vec4f const x) {   // error function complement
    return _mm_erfc_ps(x);
}
static inline Vec2d erfc(Vec2d const x) {   // error function complement
    return _mm_erfc_pd(x);
}
static inline Vec4f erfinv(Vec4f const x) { // inverse error function
    return _mm_erfinv_ps(x);
}
static inline Vec2d erfinv(Vec2d const x) { // inverse error function
    return _mm_erfinv_pd(x);
}

static inline Vec4f cdfnorm(Vec4f const x) {     // cumulative normal distribution function
    return _mm_cdfnorm_ps(x);
}
static inline Vec2d cdfnorm(Vec2d const x) {     // cumulative normal distribution function
    return _mm_cdfnorm_pd(x);
}
static inline Vec4f cdfnorminv(Vec4f const x) {  // inverse cumulative normal distribution function
    return _mm_cdfnorminv_ps(x);
}
static inline Vec2d cdfnorminv(Vec2d const x) {  // inverse cumulative normal distribution function
    return _mm_cdfnorminv_pd(x);
}

#else
/*****************************************************************************
*
*      128-bit vector functions using other compiler than Intel
*
*****************************************************************************/

#if (defined(_WIN64) || defined(__CYGWIN__)) && defined(__x86_64__)
// fix incompatible calling convention in Win64
#if defined(_MSC_VER) || defined(__clang__)
#define V_VECTORCALL __vectorcall
#else
// gcc. Change this if future gcc version supports __vectorcall
#define V_VECTORCALL __attribute__((sysv_abi))  // this is inefficient but it works
#endif
#else  // not Win64. Vectors are transferred in registers by default
#define V_VECTORCALL
#endif

// External function prototypes, 128-bit vectors
extern "C" {
    extern __m128  V_VECTORCALL __svml_expf4       (__m128);
    extern __m128d V_VECTORCALL __svml_exp2        (__m128d);
    extern __m128  V_VECTORCALL __svml_expm1f4     (__m128);
    extern __m128d V_VECTORCALL __svml_expm12      (__m128d);
    extern __m128  V_VECTORCALL __svml_exp2f4      (__m128);
    extern __m128d V_VECTORCALL __svml_exp22       (__m128d);
    extern __m128  V_VECTORCALL __svml_exp10f4     (__m128);
    extern __m128d V_VECTORCALL __svml_exp102      (__m128d);
    extern __m128  V_VECTORCALL __svml_powf4       (__m128,  __m128);
    extern __m128d V_VECTORCALL __svml_pow2        (__m128d, __m128d);
    extern __m128  V_VECTORCALL __svml_cbrtf4      (__m128);
    extern __m128d V_VECTORCALL __svml_cbrt2       (__m128d);
    extern __m128  V_VECTORCALL __svml_invsqrtf4   (__m128);
    extern __m128d V_VECTORCALL __svml_invsqrt2    (__m128d);
    extern __m128  V_VECTORCALL __svml_logf4       (__m128);
    extern __m128d V_VECTORCALL __svml_log2        (__m128d);
    extern __m128  V_VECTORCALL __svml_log1pf4     (__m128);
    extern __m128d V_VECTORCALL __svml_log1p2      (__m128d);
    extern __m128  V_VECTORCALL __svml_log2f4      (__m128);
    extern __m128d V_VECTORCALL __svml_log22       (__m128d);
    extern __m128  V_VECTORCALL __svml_log10f4     (__m128);
    extern __m128d V_VECTORCALL __svml_log102      (__m128d);
    extern __m128  V_VECTORCALL __svml_sinf4       (__m128);
    extern __m128d V_VECTORCALL __svml_sin2        (__m128d);
    extern __m128  V_VECTORCALL __svml_cosf4       (__m128);
    extern __m128d V_VECTORCALL __svml_cos2        (__m128d);
    extern __m128  V_VECTORCALL __svml_sincosf4    (__m128);  // cos returned in xmm1
    extern __m128d V_VECTORCALL __svml_sincos2     (__m128d); // cos returned in xmm1
    extern __m128  V_VECTORCALL __svml_tanf4       (__m128);
    extern __m128d V_VECTORCALL __svml_tan2        (__m128d);
    extern __m128  V_VECTORCALL __svml_asinf4      (__m128);
    extern __m128d V_VECTORCALL __svml_asin2       (__m128d);
    extern __m128  V_VECTORCALL __svml_acosf4      (__m128);
    extern __m128d V_VECTORCALL __svml_acos2       (__m128d);
    extern __m128  V_VECTORCALL __svml_atanf4      (__m128);
    extern __m128d V_VECTORCALL __svml_atan2       (__m128d);
    extern __m128  V_VECTORCALL __svml_atan2f4     (__m128,  __m128);
    extern __m128d V_VECTORCALL __svml_atan22      (__m128d, __m128d);
    extern __m128  V_VECTORCALL __svml_sinhf4      (__m128);
    extern __m128d V_VECTORCALL __svml_sinh2       (__m128d);
    extern __m128  V_VECTORCALL __svml_coshf4      (__m128);
    extern __m128d V_VECTORCALL __svml_cosh2       (__m128d);
    extern __m128  V_VECTORCALL __svml_tanhf4      (__m128);
    extern __m128d V_VECTORCALL __svml_tanh2       (__m128d);
    extern __m128  V_VECTORCALL __svml_asinhf4     (__m128);
    extern __m128d V_VECTORCALL __svml_asinh2      (__m128d);
    extern __m128  V_VECTORCALL __svml_acoshf4     (__m128);
    extern __m128d V_VECTORCALL __svml_acosh2      (__m128d);
    extern __m128  V_VECTORCALL __svml_atanhf4     (__m128);
    extern __m128d V_VECTORCALL __svml_atanh2      (__m128d);
    extern __m128  V_VECTORCALL __svml_erff4       (__m128);
    extern __m128d V_VECTORCALL __svml_erf2        (__m128d);
    extern __m128  V_VECTORCALL __svml_erfcf4      (__m128);
    extern __m128d V_VECTORCALL __svml_erfc2       (__m128d);
    extern __m128  V_VECTORCALL __svml_erfinvf4    (__m128);
    extern __m128d V_VECTORCALL __svml_erfinv2     (__m128d);
    extern __m128  V_VECTORCALL __svml_cdfnormf4   (__m128);
    extern __m128d V_VECTORCALL __svml_cdfnorm2    (__m128d);
    extern __m128  V_VECTORCALL __svml_cdfnorminvf4(__m128);
    extern __m128d V_VECTORCALL __svml_cdfnorminv2 (__m128d);
    extern __m128  V_VECTORCALL __svml_cexpf4      (__m128);
    extern __m128d V_VECTORCALL __svml_cexp2       (__m128d);
}


/*****************************************************************************
*
*      Function definitions
*
*****************************************************************************/

// exponential and power functions
static inline Vec4f exp (Vec4f const x) {   // exponential function
    return  __svml_expf4(x);
}
static inline Vec2d exp (Vec2d const x) {   // exponential function
    return  __svml_exp2(x);
}

static inline Vec4f expm1 (Vec4f const x) { // exp(x)-1. Avoids loss of precision if x is close to 1
    return  __svml_expm1f4(x);
}
static inline Vec2d expm1 (Vec2d const x) { // exp(x)-1. Avoids loss of precision if x is close to 1
    return  __svml_expm12(x);
}

static inline Vec4f exp2 (Vec4f const x) {  // pow(2,x)
    return  __svml_exp2f4(x);
}
static inline Vec2d exp2 (Vec2d const x) {  // pow(2,x)
    return  __svml_exp22(x);
}

static inline Vec4f exp10 (Vec4f const x) { // pow(10,x)
    return  __svml_exp10f4(x);
}
static inline Vec2d exp10 (Vec2d const x) { // pow(10,x)
    return  __svml_exp102(x);
}

static inline Vec4f pow (Vec4f const a, Vec4f const b) {   // pow(a,b) = a to the power of b
    return  __svml_powf4(a,b);
}

static inline Vec4f pow (Vec4f const a, float const b) {   // pow(a,b) = a to the power of b
    return  __svml_powf4(a,Vec4f(b));
} 
static inline Vec2d pow (Vec2d const a, Vec2d const b) {   // pow(a,b) = a to the power of b
    return  __svml_pow2(a,b);
}
static inline Vec2d pow (Vec2d const a, double const b) {  // pow(a,b) = a to the power of b
    return  __svml_pow2(a,Vec2d(b));
}

static inline Vec4f cbrt (Vec4f const x) {  // pow(x,1/3)
    return  __svml_cbrtf4(x);
}
static inline Vec2d cbrt (Vec2d const x) {  // pow(x,1/3)
    return  __svml_cbrt2(x);
}

// logarithms
static inline Vec4f log (Vec4f const x) {   // natural logarithm
    return  __svml_logf4(x);
}
static inline Vec2d log (Vec2d const x) {   // natural logarithm
    return  __svml_log2(x);
}

static inline Vec4f log1p (Vec4f const x) { // log(1+x)
    return  __svml_log1pf4(x);
}
static inline Vec2d log1p (Vec2d const x) { // log(1+x)
    return  __svml_log1p2(x);
}

static inline Vec4f log2 (Vec4f const x) {  // logarithm base 2
    return  __svml_log2f4(x);
}
static inline Vec2d log2 (Vec2d const x) {  // logarithm base 2
    return  __svml_log22(x);
}

static inline Vec4f log10 (Vec4f const x) { // logarithm base 10
    return  __svml_log10f4(x);
}
static inline Vec2d log10 (Vec2d const x) { // logarithm base 10
    return  __svml_log102(x);
}

// trigonometric functions (angles in radians)
static inline Vec4f sin (Vec4f const x) {   // sine
    return  __svml_sinf4(x);
}
static inline Vec2d sin (Vec2d const x) {   // sine
    return  __svml_sin2(x);
}

static inline Vec4f cos (Vec4f const x) {   // cosine
    return  __svml_cosf4(x);
}
static inline Vec2d cos (Vec2d const x) {   // cosine
    return  __svml_cos2(x);
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec4f sincos (Vec4f * pcos, Vec4f const x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128 r_sin, r_cos;
    r_sin = __svml_sincosf4(x);
#if defined(__unix__) || defined(__GNUC__)
    //   __asm__ ( "call V_VECTORCALL __svml_sincosf4 \n movaps %%xmm0, %0 \n movaps %%xmm1, %1" : "=m"(r_sin), "=m"(r_cos) : "xmm0"(x) );
    __asm__ __volatile__ ( "movaps %%xmm1, %0":"=m"(r_cos));
#else // Windows
    _asm movaps r_cos, xmm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
static inline Vec2d sincos (Vec2d * pcos, Vec2d const x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m128d r_sin, r_cos;
    r_sin = __svml_sincos2(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "movaps %%xmm1, %0":"=m"(r_cos));
#else // Windows
    _asm movapd r_cos, xmm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
#endif // inline assembly available

static inline Vec4f tan (Vec4f const x) {   // tangent
    return  __svml_tanf4(x);
}
static inline Vec2d tan (Vec2d const x) {   // tangent
    return  __svml_tan2(x);
}

// inverse trigonometric functions
static inline Vec4f asin (Vec4f const x) {  // inverse sine
    return  __svml_asinf4(x);
}
static inline Vec2d asin (Vec2d const x) {  // inverse sine
    return  __svml_asin2(x);
}

static inline Vec4f acos (Vec4f const x) {  // inverse cosine
    return  __svml_acosf4(x);
}
static inline Vec2d acos (Vec2d const x) {  // inverse cosine
    return  __svml_acos2(x);
}

static inline Vec4f atan (Vec4f const x) {  // inverse tangent
    return  __svml_atanf4(x);
}
static inline Vec2d atan (Vec2d const x) {  // inverse tangent
    return  __svml_atan2(x);
}

static inline Vec4f atan2 (Vec4f const a, Vec4f const b) { // inverse tangent of a/b
    return  __svml_atan2f4(a,b);
}
static inline Vec2d atan2 (Vec2d const a, Vec2d const b) { // inverse tangent of a/b
    return  __svml_atan22(a,b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec4f sinh (Vec4f const x) {  // hyperbolic sine
    return  __svml_sinhf4(x);
}
static inline Vec2d sinh (Vec2d const x) {  // hyperbolic sine
    return  __svml_sinh2(x);
}

static inline Vec4f cosh (Vec4f const x) {  // hyperbolic cosine
    return  __svml_coshf4(x);
}
static inline Vec2d cosh (Vec2d const x) {  // hyperbolic cosine
    return  __svml_cosh2(x);
}

static inline Vec4f tanh (Vec4f const x) {  // hyperbolic tangent
    return  __svml_tanhf4(x);
}
static inline Vec2d tanh (Vec2d const x) {  // hyperbolic tangent
    return  __svml_tanh2(x);
}

static inline Vec4f asinh (Vec4f const x) { // inverse hyperbolic sine
    return  __svml_asinhf4(x);
}
static inline Vec2d asinh (Vec2d const x) { // inverse hyperbolic sine
    return  __svml_asinh2(x);
}

static inline Vec4f acosh (Vec4f const x) { // inverse hyperbolic cosine
    return  __svml_acoshf4(x);
}
static inline Vec2d acosh (Vec2d const x) { // inverse hyperbolic cosine
    return  __svml_acosh2(x);
}

static inline Vec4f atanh (Vec4f const x) { // inverse hyperbolic tangent
    return  __svml_atanhf4(x);
}
static inline Vec2d atanh (Vec2d const x) { // inverse hyperbolic tangent
    return  __svml_atanh2(x);
}

// error function
static inline Vec4f erf (Vec4f const x) {   // error function
    return  __svml_erff4(x);
}
static inline Vec2d erf (Vec2d const x) {   // error function
    return  __svml_erf2(x);
}

static inline Vec4f erfc (Vec4f const x) {  // error function complement
    return  __svml_erfcf4(x);
}
static inline Vec2d erfc (Vec2d const x) {  // error function complement
    return  __svml_erfc2(x);
}

static inline Vec4f erfinv (Vec4f const x) {     // inverse error function
    return  __svml_erfinvf4(x);
}
static inline Vec2d erfinv (Vec2d const x) {     // inverse error function
    return  __svml_erfinv2(x);
}

static inline Vec4f cdfnorm (Vec4f const x) {    // cumulative normal distribution function
    return  __svml_cdfnormf4(x);
}
static inline Vec2d cdfnorm (Vec2d const x) {    // cumulative normal distribution function
    return  __svml_cdfnorm2(x);
}

static inline Vec4f cdfnorminv (Vec4f const x) { // inverse cumulative normal distribution function
    return  __svml_cdfnorminvf4(x);
}
static inline Vec2d cdfnorminv (Vec2d const x) { // inverse cumulative normal distribution function
    return  __svml_cdfnorminv2(x);
}

#endif   // __INTEL_COMPILER

#if defined (MAX_VECTOR_SIZE) && MAX_VECTOR_SIZE >= 256  // 256 bit vectors

#if defined (VECTORF256_H)   // 256-bit vector registers supported

#ifdef __INTEL_COMPILER
/*****************************************************************************
*
*      256-bit vector functions using Intel compiler
*
*****************************************************************************/
// exponential and power functions
static inline Vec8f exp(Vec8f const x) {    // exponential function
    return _mm256_exp_ps(x);
}
static inline Vec4d exp(Vec4d const x) {    // exponential function
    return _mm256_exp_pd(x);
}
static inline Vec8f expm1(Vec8f const x) {  // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm256_expm1_ps(x);
}
static inline Vec4d expm1(Vec4d const x) {  // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm256_expm1_pd(x);
}
static inline Vec8f exp2(Vec8f const x) {   // pow(2,x)
    return _mm256_exp2_ps(x);
}
static inline Vec4d exp2(Vec4d const x) {   // pow(2,x)
    return _mm256_exp2_pd(x);
}
static inline Vec8f exp10(Vec8f const x) {  // pow(10,x)
    return _mm256_exp10_ps(x);
}
static inline Vec4d exp10(Vec4d const x) {  // pow(10,x)
    return _mm256_exp10_pd(x);
}
static inline Vec8f pow(Vec8f const a, Vec8f const b) {    // pow(a,b) = a to the power of b
    return _mm256_pow_ps(a, b);
}
static inline Vec8f pow(Vec8f const a, float const b) {    // pow(a,b) = a to the power of b
    return _mm256_pow_ps(a, Vec8f(b));
}
static inline Vec4d pow(Vec4d const a, Vec4d const b) {    // pow(a,b) = a to the power of b
    return _mm256_pow_pd(a, b);
}
static inline Vec4d pow(Vec4d const a, double const b) {   // pow(a,b) = a to the power of b
    return _mm256_pow_pd(a, Vec4d(b));
}
static inline Vec8f cbrt(Vec8f const x) {   // pow(x,1/3)
    return _mm256_cbrt_ps(x);
}
static inline Vec4d cbrt(Vec4d const x) {   // pow(x,1/3)
    return _mm256_cbrt_pd(x);
}
// logarithms
static inline Vec8f log(Vec8f const x) {    // natural logarithm
    return _mm256_log_ps(x);
}
static inline Vec4d log(Vec4d const x) {    // natural logarithm
    return _mm256_log_pd(x);
}
static inline Vec8f log1p(Vec8f const x) {  // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm256_log1p_ps(x);
}
static inline Vec4d log1p(Vec4d const x) {  // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm256_log1p_pd(x);
}
static inline Vec8f log2(Vec8f const x) {   // logarithm base 2
    return _mm256_log2_ps(x);
}
static inline Vec4d log2(Vec4d const x) {   // logarithm base 2
    return _mm256_log2_pd(x);
}
static inline Vec8f log10(Vec8f const x) {  // logarithm base 10
    return _mm256_log10_ps(x);
}
static inline Vec4d log10(Vec4d const x) {  // logarithm base 10
    return _mm256_log10_pd(x);
}

// trigonometric functions
static inline Vec8f sin(Vec8f const x) {    // sine
    return _mm256_sin_ps(x);
}
static inline Vec4d sin(Vec4d const x) {    // sine
    return _mm256_sin_pd(x);
}
static inline Vec8f cos(Vec8f const x) {    // cosine
    return _mm256_cos_ps(x);
}
static inline Vec4d cos(Vec4d const x) {    // cosine
    return _mm256_cos_pd(x);
} 
static inline Vec8f sincos(Vec8f * pcos, Vec8f const x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256 r_sin, r_cos;
    r_sin = _mm256_sincos_ps(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec4d sincos(Vec4d * pcos, Vec4d const x) {  // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256d r_sin, r_cos;
    r_sin = _mm256_sincos_pd(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec8f tan(Vec8f const x) {    // tangent
    return _mm256_tan_ps(x);
}
static inline Vec4d tan(Vec4d const x) {    // tangent
    return _mm256_tan_pd(x);
}

// inverse trigonometric functions
static inline Vec8f asin(Vec8f const x) {   // inverse sine
    return _mm256_asin_ps(x);
}
static inline Vec4d asin(Vec4d const x) {   // inverse sine
    return _mm256_asin_pd(x);
}

static inline Vec8f acos(Vec8f const x) {   // inverse cosine
    return _mm256_acos_ps(x);
}
static inline Vec4d acos(Vec4d const x) {   // inverse cosine
    return _mm256_acos_pd(x);
}

static inline Vec8f atan(Vec8f const x) {   // inverse tangent
    return _mm256_atan_ps(x);
}
static inline Vec4d atan(Vec4d const x) {   // inverse tangent
    return _mm256_atan_pd(x);
}
static inline Vec8f atan2(Vec8f const a, Vec8f const b) {  // inverse tangent of a/b
    return _mm256_atan2_ps(a, b);
}
static inline Vec4d atan2(Vec4d const a, Vec4d const b) {  // inverse tangent of a/b
    return _mm256_atan2_pd(a, b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh(Vec8f const x) {   // hyperbolic sine
    return _mm256_sinh_ps(x);
}
static inline Vec4d sinh(Vec4d const x) {   // hyperbolic sine
    return _mm256_sinh_pd(x);
}
static inline Vec8f cosh(Vec8f const x) {   // hyperbolic cosine
    return _mm256_cosh_ps(x);
}
static inline Vec4d cosh(Vec4d const x) {   // hyperbolic cosine
    return _mm256_cosh_pd(x);
}
static inline Vec8f tanh(Vec8f const x) {   // hyperbolic tangent
    return _mm256_tanh_ps(x);
}
static inline Vec4d tanh(Vec4d const x) {   // hyperbolic tangent
    return _mm256_tanh_pd(x);
}
static inline Vec8f asinh(Vec8f const x) {  // inverse hyperbolic sine
    return _mm256_asinh_ps(x);
}
static inline Vec4d asinh(Vec4d const x) {  // inverse hyperbolic sine
    return _mm256_asinh_pd(x);
}
static inline Vec8f acosh(Vec8f const x) {  // inverse hyperbolic cosine
    return _mm256_acosh_ps(x);
}
static inline Vec4d acosh(Vec4d const x) {  // inverse hyperbolic cosine
    return _mm256_acosh_pd(x);
}
static inline Vec8f atanh(Vec8f const x) {  // inverse hyperbolic tangent
    return _mm256_atanh_ps(x);
}
static inline Vec4d atanh(Vec4d const x) {  // inverse hyperbolic tangent
    return _mm256_atanh_pd(x);
}

// error function
static inline Vec8f erf(Vec8f const x) {    // error function
    return _mm256_erf_ps(x);
}
static inline Vec4d erf(Vec4d const x) {    // error function
    return _mm256_erf_pd(x);
} 
static inline Vec8f erfc(Vec8f const x) {   // error function complement
    return _mm256_erfc_ps(x);
}
static inline Vec4d erfc(Vec4d const x) {   // error function complement
    return _mm256_erfc_pd(x);
}
static inline Vec8f erfinv(Vec8f const x) { // inverse error function
    return _mm256_erfinv_ps(x);
}
static inline Vec4d erfinv(Vec4d const x) { // inverse error function
    return _mm256_erfinv_pd(x);
}

static inline Vec8f cdfnorm(Vec8f const x) {     // cumulative normal distribution function
    return _mm256_cdfnorm_ps(x);
}
static inline Vec4d cdfnorm(Vec4d const x) {     // cumulative normal distribution function
    return _mm256_cdfnorm_pd(x);
}
static inline Vec8f cdfnorminv(Vec8f const x) {  // inverse cumulative normal distribution function
    return _mm256_cdfnorminv_ps(x);
}
static inline Vec4d cdfnorminv(Vec4d const x) {  // inverse cumulative normal distribution function
    return _mm256_cdfnorminv_pd(x);
}


#else    // __INTEL_COMPILER
/*****************************************************************************
*
*      256-bit vector functions using other compiler than Intel
*
*****************************************************************************/
// External function prototypes, 256-bit vectors
extern "C" {
    extern __m256  V_VECTORCALL __svml_expf8       (__m256);
    extern __m256d V_VECTORCALL __svml_exp4        (__m256d);
    extern __m256  V_VECTORCALL __svml_expm1f8     (__m256);
    extern __m256d V_VECTORCALL __svml_expm14      (__m256d);
    extern __m256  V_VECTORCALL __svml_exp2f8      (__m256);
    extern __m256d V_VECTORCALL __svml_exp24       (__m256d);
    extern __m256  V_VECTORCALL __svml_exp10f8     (__m256);
    extern __m256d V_VECTORCALL __svml_exp104      (__m256d);
    extern __m256  V_VECTORCALL __svml_powf8       (__m256,  __m256);
    extern __m256d V_VECTORCALL __svml_pow4        (__m256d, __m256d);
    extern __m256  V_VECTORCALL __svml_cbrtf8      (__m256);
    extern __m256d V_VECTORCALL __svml_cbrt4       (__m256d);
    extern __m256  V_VECTORCALL __svml_invsqrtf8   (__m256);
    extern __m256d V_VECTORCALL __svml_invsqrt4    (__m256d);
    extern __m256  V_VECTORCALL __svml_logf8       (__m256);
    extern __m256d V_VECTORCALL __svml_log4        (__m256d);
    extern __m256  V_VECTORCALL __svml_log1pf8     (__m256);
    extern __m256d V_VECTORCALL __svml_log1p4      (__m256d);
    extern __m256  V_VECTORCALL __svml_log2f8      (__m256);
    extern __m256d V_VECTORCALL __svml_log24       (__m256d);
    extern __m256  V_VECTORCALL __svml_log10f8     (__m256);
    extern __m256d V_VECTORCALL __svml_log104      (__m256d);
    extern __m256  V_VECTORCALL __svml_sinf8       (__m256);
    extern __m256d V_VECTORCALL __svml_sin4        (__m256d);
    extern __m256  V_VECTORCALL __svml_cosf8       (__m256);
    extern __m256d V_VECTORCALL __svml_cos4        (__m256d);
    extern __m256  V_VECTORCALL __svml_sincosf8    (__m256);  // cos returned in ymm1
    extern __m256d V_VECTORCALL __svml_sincos4     (__m256d); // cos returned in ymm1
    extern __m256  V_VECTORCALL __svml_tanf8       (__m256);
    extern __m256d V_VECTORCALL __svml_tan4        (__m256d);
    extern __m256  V_VECTORCALL __svml_asinf8      (__m256);
    extern __m256d V_VECTORCALL __svml_asin4       (__m256d);
    extern __m256  V_VECTORCALL __svml_acosf8      (__m256);
    extern __m256d V_VECTORCALL __svml_acos4       (__m256d);
    extern __m256  V_VECTORCALL __svml_atanf8      (__m256);
    extern __m256d V_VECTORCALL __svml_atan4       (__m256d);
    extern __m256  V_VECTORCALL __svml_atan2f8     (__m256, __m256);
    extern __m256d V_VECTORCALL __svml_atan24      (__m256d, __m256d);
    extern __m256  V_VECTORCALL __svml_sinhf8      (__m256);
    extern __m256d V_VECTORCALL __svml_sinh4       (__m256d);
    extern __m256  V_VECTORCALL __svml_coshf8      (__m256);
    extern __m256d V_VECTORCALL __svml_cosh4       (__m256d);
    extern __m256  V_VECTORCALL __svml_tanhf8      (__m256);
    extern __m256d V_VECTORCALL __svml_tanh4       (__m256d);
    extern __m256  V_VECTORCALL __svml_asinhf8     (__m256);
    extern __m256d V_VECTORCALL __svml_asinh4      (__m256d);
    extern __m256  V_VECTORCALL __svml_acoshf8     (__m256);
    extern __m256d V_VECTORCALL __svml_acosh4      (__m256d);
    extern __m256  V_VECTORCALL __svml_atanhf8     (__m256);
    extern __m256d V_VECTORCALL __svml_atanh4      (__m256d);
    extern __m256  V_VECTORCALL __svml_erff8       (__m256);
    extern __m256d V_VECTORCALL __svml_erf4        (__m256d);
    extern __m256  V_VECTORCALL __svml_erfcf8      (__m256);
    extern __m256d V_VECTORCALL __svml_erfc4       (__m256d);
    extern __m256  V_VECTORCALL __svml_erfinvf8    (__m256);
    extern __m256d V_VECTORCALL __svml_erfinv4     (__m256d);
    extern __m256  V_VECTORCALL __svml_cdfnorminvf8(__m256);
    extern __m256d V_VECTORCALL __svml_cdfnorminv4 (__m256d);
    extern __m256  V_VECTORCALL __svml_cdfnormf8   (__m256);
    extern __m256d V_VECTORCALL __svml_cdfnorm4    (__m256d);
    //extern __m256  V_VECTORCALL __svml_cexpf8      (__m256);
    //extern __m256d V_VECTORCALL __svml_cexp4       (__m256d);
}


// exponential and power functions
static inline Vec8f exp (Vec8f const x) {   // exponential function
    return  __svml_expf8(x);
}
static inline Vec4d exp (Vec4d const x) {   // exponential function
    return  __svml_exp4(x);
} 
static inline Vec8f expm1 (Vec8f const x) { // exp(x)-1
    return  __svml_expm1f8(x);
}
static inline Vec4d expm1 (Vec4d const x) { // exp(x)-1
    return  __svml_expm14(x);
}
static inline Vec8f exp2 (Vec8f const x) {  // pow(2,x)
    return  __svml_exp2f8(x);
}
static inline Vec4d exp2 (Vec4d const x) {  // pow(2,x)
    return  __svml_exp24(x);
} 
static inline Vec8f exp10 (Vec8f const x) { // pow(10,x)
    return  __svml_exp10f8(x);
}
static inline Vec4d exp10 (Vec4d const x) { // pow(10,x)
    return  __svml_exp104(x);
}
static inline Vec8f pow (Vec8f const a, Vec8f const b) {   // pow(a,b) = a to the power of b
    return  __svml_powf8(a,b);
}
static inline Vec8f pow (Vec8f const a, float const b) {   // pow(a,b) = a to the power of b
    return  __svml_powf8(a,Vec8f(b));
}
static inline Vec4d pow (Vec4d const a, Vec4d const b) {   // pow(a,b) = a to the power of b
    return  __svml_pow4(a,b);
}
static inline Vec4d pow (Vec4d const a, double const b) {  // pow(a,b) = a to the power of b
    return  __svml_pow4(a,Vec4d(b));
}
static inline Vec8f cbrt (Vec8f const x) {  // pow(x,1/3)
    return  __svml_cbrtf8(x);
}
static inline Vec4d cbrt (Vec4d const x) {  // pow(x,1/3)
    return  __svml_cbrt4(x);
}

// logarithms
static inline Vec8f log (Vec8f const x) {   // natural logarithm
    return  __svml_logf8(x);
}
static inline Vec4d log (Vec4d const x) {   // natural logarithm
    return  __svml_log4(x);
}
static inline Vec8f log1p (Vec8f const x) { // log(1+x). Avoids loss of precision if 1+x is close to 1
    return  __svml_log1pf8(x);
}
static inline Vec4d log1p (Vec4d const x) { // log(1+x). Avoids loss of precision if 1+x is close to 1
    return  __svml_log1p4(x);
} 
static inline Vec8f log2 (Vec8f const x) {  // logarithm base 2
    return  __svml_log2f8(x);
}
static inline Vec4d log2 (Vec4d const x) {  // logarithm base 2
    return  __svml_log24(x);
} 
static inline Vec8f log10 (Vec8f const x) { // logarithm base 10
    return  __svml_log10f8(x);
}
static inline Vec4d log10 (Vec4d const x) { // logarithm base 10
    return  __svml_log104(x);
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const x) {   // sine
    return  __svml_sinf8(x);
}
static inline Vec4d sin (Vec4d const x) {   // sine
    return  __svml_sin4(x);
}
static inline Vec8f cos (Vec8f const x) {   // cosine
    return  __svml_cosf8(x);
}
static inline Vec4d cos (Vec4d const x) {   // cosine
    return  __svml_cos4(x);
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec8f sincos (Vec8f * pcos, Vec8f const x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256 r_sin, r_cos;
    r_sin = __svml_sincosf8(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "vmovaps %%ymm1, %0":"=m"(r_cos));
#else // Windows
    _asm vmovaps r_cos, ymm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const x) {   // sine and cosine. sin(x) returned, cos(x) in pcos
    __m256d r_sin, r_cos;
    r_sin = __svml_sincos4(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "vmovaps %%ymm1, %0":"=m"(r_cos));
#else // Windows
    _asm vmovapd r_cos, ymm1;
#endif
    *pcos = r_cos;
    return r_sin;
}
#endif // inline assembly available

static inline Vec8f tan (Vec8f const x) {   // tangent
    return  __svml_tanf8(x);
}
static inline Vec4d tan (Vec4d const x) {   // tangent
    return  __svml_tan4(x);
} 

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const x) {  // inverse sine
    return  __svml_asinf8(x);
}
static inline Vec4d asin (Vec4d const x) {  // inverse sine
    return  __svml_asin4(x);
}
static inline Vec8f acos (Vec8f const x) {  // inverse cosine
    return  __svml_acosf8(x);
}
static inline Vec4d acos (Vec4d const x) {  // inverse cosine
    return  __svml_acos4(x);
}
static inline Vec8f atan (Vec8f const x) {  // inverse tangent
    return  __svml_atanf8(x);
}
static inline Vec4d atan (Vec4d const x) {  // inverse tangent
    return  __svml_atan4(x);
} 
static inline Vec8f atan2 (Vec8f const a, Vec8f const b) { // inverse tangent of a/b
    return  __svml_atan2f8(a,b);
}
static inline Vec4d atan2 (Vec4d const a, Vec4d const b) { // inverse tangent of a/b
    return  __svml_atan24(a,b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec8f sinh (Vec8f const x) {  // hyperbolic sine
    return  __svml_sinhf8(x);
}
static inline Vec4d sinh (Vec4d const x) {  // hyperbolic sine
    return  __svml_sinh4(x);
} 
static inline Vec8f cosh (Vec8f const x) {  // hyperbolic cosine
    return  __svml_coshf8(x);
}
static inline Vec4d cosh (Vec4d const x) {  // hyperbolic cosine
    return  __svml_cosh4(x);
} 
static inline Vec8f tanh (Vec8f const x) {  // hyperbolic tangent
    return  __svml_tanhf8(x);
}
static inline Vec4d tanh (Vec4d const x) {  // hyperbolic tangent
    return  __svml_tanh4(x);
}
static inline Vec8f asinh (Vec8f const x) { // inverse hyperbolic sine
    return  __svml_asinhf8(x);
}
static inline Vec4d asinh (Vec4d const x) { // inverse hyperbolic sine
    return  __svml_asinh4(x);
} 
static inline Vec8f acosh (Vec8f const x) { // inverse hyperbolic cosine
    return  __svml_acoshf8(x);
}
static inline Vec4d acosh (Vec4d const x) { // inverse hyperbolic cosine
    return  __svml_acosh4(x);
}

static inline Vec8f atanh (Vec8f const x) { // inverse hyperbolic tangent
    return  __svml_atanhf8(x);
}
static inline Vec4d atanh (Vec4d const x) { // inverse hyperbolic tangent
    return  __svml_atanh4(x);
}

// error function
static inline Vec8f erf (Vec8f const x) {   // error function
    return  __svml_erff8(x);
}
static inline Vec4d erf (Vec4d const x) {   // error function
    return  __svml_erf4(x);
}
static inline Vec8f erfc (Vec8f const x) {  // error function complement
    return  __svml_erfcf8(x);
}
static inline Vec4d erfc (Vec4d const x) {  // error function complement
    return  __svml_erfc4(x);
} 
static inline Vec8f erfinv (Vec8f const x) {     // inverse error function
    return  __svml_erfinvf8(x);
}
static inline Vec4d erfinv (Vec4d const x) {     // inverse error function
    return  __svml_erfinv4(x);
} 

static inline Vec8f cdfnorm (Vec8f const x) {    // cumulative normal distribution function
    return  __svml_cdfnormf8(x);
}
static inline Vec4d cdfnorm (Vec4d const x) {    // cumulative normal distribution function
    return  __svml_cdfnorm4(x);
} 
static inline Vec8f cdfnorminv (Vec8f const x) { // inverse cumulative normal distribution function
    return  __svml_cdfnorminvf8(x);
}
static inline Vec4d cdfnorminv (Vec4d const x) { // inverse cumulative normal distribution function
    return  __svml_cdfnorminv4(x);
}

#endif   // __INTEL_COMPILER

#else    // VECTORF256_H

/*****************************************************************************
*
*      256-bit vector functions emulated with 128-bit vectors
*
*****************************************************************************/
// exponential and power functions
static inline Vec8f exp (Vec8f const x) {        // exponential function
    return Vec8f(exp(x.get_low()), exp(x.get_high()));
}
static inline Vec4d exp (Vec4d const x) {        // exponential function
    return Vec4d(exp(x.get_low()), exp(x.get_high()));
} 
static inline Vec8f expm1 (Vec8f const x) {      // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec8f(expm1(x.get_low()), expm1(x.get_high()));
}
static inline Vec4d expm1 (Vec4d const x) {      // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec4d(expm1(x.get_low()), expm1(x.get_high()));
} 
static inline Vec8f exp2 (Vec8f const x) {       // pow(2,x)
    return Vec8f(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec4d exp2 (Vec4d const x) {       // pow(2,x)
    return Vec4d(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec8f exp10 (Vec8f const x) {      // pow(10,x)
    return Vec8f(exp10(x.get_low()), exp10(x.get_high()));
}                                                
static inline Vec4d exp10 (Vec4d const x) {      // pow(10,x)
    return Vec4d(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec8f pow (Vec8f const a, Vec8f const b) {   // pow(a,b) = a to the power of b
    return Vec8f(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}
static inline Vec8f pow (Vec8f const a, float const b) {   // pow(a,b) = a to the power of b
    return Vec8f(pow(a.get_low(),b), pow(a.get_high(),b));
}
static inline Vec4d pow (Vec4d const a, Vec4d const b) {   // pow(a,b) = a to the power of b
    return Vec4d(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
} 
static inline Vec4d pow (Vec4d const a, double const b) {  // pow(a,b) = a to the power of b
    return Vec4d(pow(a.get_low(),b), pow(a.get_high(),b));
} 
static inline Vec8f cbrt (Vec8f const x) {   // pow(x,1/3)
    return Vec8f(cbrt(x.get_low()), cbrt(x.get_high()));
}
static inline Vec4d cbrt (Vec4d const x) {   // pow(x,1/3)
    return Vec4d(cbrt(x.get_low()), cbrt(x.get_high()));
}

// logarithms
static inline Vec8f log (Vec8f const x) {   // natural logarithm
    return Vec8f(log(x.get_low()), log(x.get_high()));
}
static inline Vec4d log (Vec4d const x) {   // natural logarithm
    return Vec4d(log(x.get_low()), log(x.get_high()));
} 
static inline Vec8f log1p (Vec8f const x) { // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec8f(log1p(x.get_low()), log1p(x.get_high()));
}
static inline Vec4d log1p (Vec4d const x) { // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec4d(log1p(x.get_low()), log1p(x.get_high()));
} 
static inline Vec8f log2 (Vec8f const x) {  // logarithm base 2
    return Vec8f(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec4d log2 (Vec4d const x) {  // logarithm base 2
    return Vec4d(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec8f log10 (Vec8f const x) { // logarithm base 10
    return Vec8f(log10(x.get_low()), log10(x.get_high()));
}
static inline Vec4d log10 (Vec4d const x) { // logarithm base 10
    return Vec4d(log10(x.get_low()), log10(x.get_high()));
}

// trigonometric functions (angles in radians)
static inline Vec8f sin (Vec8f const x) {   // sine
    return Vec8f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec4d sin (Vec4d const x) {   // sine
    return Vec4d(sin(x.get_low()), sin(x.get_high()));
} 
static inline Vec8f cos (Vec8f const x) {   // cosine
    return Vec8f(cos(x.get_low()), cos(x.get_high()));
}
static inline Vec4d cos (Vec4d const x) {   // cosine
    return Vec4d(cos(x.get_low()), cos(x.get_high()));
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec8f sincos (Vec8f * pcos, Vec8f const x) { // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec4f r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec8f(r_cos0, r_cos1);
    return Vec8f(r_sin0, r_sin1); 
}
static inline Vec4d sincos (Vec4d * pcos, Vec4d const x) { // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec2d r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec4d(r_cos0, r_cos1);
    return Vec4d(r_sin0, r_sin1); 
}
#endif // inline assembly available

static inline Vec8f tan (Vec8f const x) {   // tangent
    return Vec8f(tan(x.get_low()), tan(x.get_high()));
}
static inline Vec4d tan (Vec4d const x) {   // tangent
    return Vec4d(tan(x.get_low()), tan(x.get_high()));
} 

// inverse trigonometric functions
static inline Vec8f asin (Vec8f const x) {  // inverse sine
    return Vec8f(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec4d asin (Vec4d const x) {  // inverse sine
    return Vec4d(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec8f acos (Vec8f const x) {  // inverse cosine
    return Vec8f(acos(x.get_low()), acos(x.get_high()));
}
static inline Vec4d acos (Vec4d const x) {  // inverse cosine
    return Vec4d(acos(x.get_low()), acos(x.get_high()));
} 
static inline Vec8f atan (Vec8f const x) {  // inverse tangent
    return Vec8f(atan(x.get_low()), atan(x.get_high()));
}
static inline Vec4d atan (Vec4d const x) {  // inverse tangent
    return Vec4d(atan(x.get_low()), atan(x.get_high()));
} 
static inline Vec8f atan2 (Vec8f const a, Vec8f const b) { // inverse tangent of a/b
    return Vec8f(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}
static inline Vec4d atan2 (Vec4d const a, Vec4d const b) { // inverse tangent of a/b
    return Vec4d(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}

// hyperbolic functions
static inline Vec8f sinh (Vec8f const x) {  // hyperbolic sine
    return Vec8f(sinh(x.get_low()), sinh(x.get_high()));
}
static inline Vec4d sinh (Vec4d const x) {  // hyperbolic sine
    return Vec4d(sinh(x.get_low()), sinh(x.get_high()));
} 
static inline Vec8f cosh (Vec8f const x) {  // hyperbolic cosine
    return Vec8f(cosh(x.get_low()), cosh(x.get_high()));
}
static inline Vec4d cosh (Vec4d const x) {  // hyperbolic cosine
    return Vec4d(cosh(x.get_low()), cosh(x.get_high()));
} 
static inline Vec8f tanh (Vec8f const x) {  // hyperbolic tangent
    return Vec8f(tanh(x.get_low()), tanh(x.get_high()));
}
static inline Vec4d tanh (Vec4d const x) {  // hyperbolic tangent
    return Vec4d(tanh(x.get_low()), tanh(x.get_high()));
}

// inverse hyperbolic functions
static inline Vec8f asinh (Vec8f const x) { // inverse hyperbolic sine
    return Vec8f(asinh(x.get_low()), asinh(x.get_high()));
}
static inline Vec4d asinh (Vec4d const x) { // inverse hyperbolic sine
    return Vec4d(asinh(x.get_low()), asinh(x.get_high()));
}
static inline Vec8f acosh (Vec8f const x) { // inverse hyperbolic cosine
    return Vec8f(acosh(x.get_low()), acosh(x.get_high()));
}
static inline Vec4d acosh (Vec4d const x) { // inverse hyperbolic cosine
    return Vec4d(acosh(x.get_low()), acosh(x.get_high()));
}
static inline Vec8f atanh (Vec8f const x) { // inverse hyperbolic tangent
    return Vec8f(atanh(x.get_low()), atanh(x.get_high()));
}
static inline Vec4d atanh (Vec4d const x) { // inverse hyperbolic tangent
    return Vec4d(atanh(x.get_low()), atanh(x.get_high()));
}

// error function
static inline Vec8f erf (Vec8f const x) {   // error function
    return Vec8f(erf(x.get_low()), erf(x.get_high()));
}
static inline Vec4d erf (Vec4d const x) {   // error function
    return Vec4d(erf(x.get_low()), erf(x.get_high()));
} 
static inline Vec8f erfc (Vec8f const x) {  // error function complement
    return Vec8f(erfc(x.get_low()), erfc(x.get_high()));
}
static inline Vec4d erfc (Vec4d const x) {  // error function complement
    return Vec4d(erfc(x.get_low()), erfc(x.get_high()));
} 
static inline Vec8f erfinv (Vec8f const x) {     // inverse error function
    return Vec8f(erfinv(x.get_low()), erfinv(x.get_high()));
}
static inline Vec4d erfinv (Vec4d const x) {     // inverse error function
    return Vec4d(erfinv(x.get_low()), erfinv(x.get_high()));
}

static inline Vec8f cdfnorm (Vec8f const x) {    // cumulative normal distribution function
    return Vec8f(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
}
static inline Vec4d cdfnorm (Vec4d const x) {    // cumulative normal distribution function
    return Vec4d(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
} 
static inline Vec8f cdfnorminv (Vec8f const x) { // inverse cumulative normal distribution function
    return Vec8f(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}
static inline Vec4d cdfnorminv (Vec4d const x) { // inverse cumulative normal distribution function
    return Vec4d(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}

#endif   // VECTORF256_H

#endif   // MAX_VECTOR_SIZE >= 256

#if defined (MAX_VECTOR_SIZE) && MAX_VECTOR_SIZE >= 512    // 512 bit vectors

#if defined (VECTORF512_H)  // 512-bit vector registers supported

#ifdef __INTEL_COMPILER
/*****************************************************************************
*
*      512-bit vector functions using Intel compiler
*
*****************************************************************************/

// exponential and power functions
static inline Vec16f exp(Vec16f const x) {       // exponential function
    return _mm512_exp_ps(x);
}
static inline Vec8d exp(Vec8d const x) {         // exponential function
    return _mm512_exp_pd(x);
}
static inline Vec16f expm1(Vec16f const x) {     // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm512_expm1_ps(x);
}
static inline Vec8d expm1(Vec8d const x) {       // exp(x)-1. Avoids loss of precision if x is close to 1
    return _mm512_expm1_pd(x);
}
static inline Vec16f exp2(Vec16f const x) {      // pow(2,x)
    return _mm512_exp2_ps(x);
}
static inline Vec8d exp2(Vec8d const x) {        // pow(2,x)
    return _mm512_exp2_pd(x);
}
static inline Vec16f exp10(Vec16f const x) {     // pow(10,x)
    return _mm512_exp10_ps(x);
}
static inline Vec8d exp10(Vec8d const x) {       // pow(10,x)
    return _mm512_exp10_pd(x);
}
static inline Vec16f pow(Vec16f const a, Vec16f const b) { // pow(a,b) = a to the power of b
    return _mm512_pow_ps(a, b);
}
static inline Vec16f pow(Vec16f const a, float const b) {  // pow(a,b) = a to the power of b
    return _mm512_pow_ps(a, Vec16f(b));
}
static inline Vec8d pow(Vec8d const a, Vec8d const b) {    // pow(a,b) = a to the power of b
    return _mm512_pow_pd(a, b);
}
static inline Vec8d pow(Vec8d const a, double const b) {   // pow(a,b) = a to the power of b
    return _mm512_pow_pd(a, Vec8d(b));
}
static inline Vec16f cbrt(Vec16f const x) {      // pow(x,1/3)
    return _mm512_cbrt_ps(x);
}
static inline Vec8d cbrt(Vec8d const x) {        // pow(x,1/3)
    return _mm512_cbrt_pd(x);
}
// logarithms
static inline Vec16f log(Vec16f const x) {       // natural logarithm
    return _mm512_log_ps(x);
}
static inline Vec8d log(Vec8d const x) {         // natural logarithm
    return _mm512_log_pd(x);
}
static inline Vec16f log1p(Vec16f const x) {     // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm512_log1p_ps(x);
}
static inline Vec8d log1p(Vec8d const x) {       // log(1+x). Avoids loss of precision if 1+x is close to 1
    return _mm512_log1p_pd(x);
}
static inline Vec16f log2(Vec16f const x) {      // logarithm base 2
    return _mm512_log2_ps(x);
}
static inline Vec8d log2(Vec8d const x) {        // logarithm base 2
    return _mm512_log2_pd(x);
}
static inline Vec16f log10(Vec16f const x) {     // logarithm base 10
    return _mm512_log10_ps(x);
}
static inline Vec8d log10(Vec8d const x) {       // logarithm base 10
    return _mm512_log10_pd(x);
}

// trigonometric functions
static inline Vec16f sin(Vec16f const x) {       // sine
    return _mm512_sin_ps(x);
}
static inline Vec8d sin(Vec8d const x) {         // sine
    return _mm512_sin_pd(x);
}
static inline Vec16f cos(Vec16f const x) {       // cosine
    return _mm512_cos_ps(x);
}
static inline Vec8d cos(Vec8d const x) {         // cosine
    return _mm512_cos_pd(x);
} 
static inline Vec16f sincos(Vec16f * pcos, Vec16f const x) { // sine and cosine. sin(x) returned, cos(x) in pcos
    __m512 r_sin, r_cos;
    r_sin = _mm512_sincos_ps(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec8d sincos(Vec8d * pcos, Vec8d const x) {    // sine and cosine. sin(x) returned, cos(x) in pcos
    __m512d r_sin, r_cos;
    r_sin = _mm512_sincos_pd(&r_cos, x);
    *pcos = r_cos;
    return r_sin;
}
static inline Vec16f tan(Vec16f const x) {       // tangent
    return _mm512_tan_ps(x);
}
static inline Vec8d tan(Vec8d const x) {         // tangent
    return _mm512_tan_pd(x);
}

// inverse trigonometric functions
static inline Vec16f asin(Vec16f const x) {      // inverse sine
    return _mm512_asin_ps(x);
}
static inline Vec8d asin(Vec8d const x) {        // inverse sine
    return _mm512_asin_pd(x);
}

static inline Vec16f acos(Vec16f const x) {      // inverse cosine
    return _mm512_acos_ps(x);
}
static inline Vec8d acos(Vec8d const x) {        // inverse cosine
    return _mm512_acos_pd(x);
}

static inline Vec16f atan(Vec16f const x) {      // inverse tangent
    return _mm512_atan_ps(x);
}
static inline Vec8d atan(Vec8d const x) {        // inverse tangent
    return _mm512_atan_pd(x);
}
static inline Vec16f atan2(Vec16f const a, Vec16f const b) { // inverse tangent of a/b
    return _mm512_atan2_ps(a, b);
}
static inline Vec8d atan2(Vec8d const a, Vec8d const b) {    // inverse tangent of a/b
    return _mm512_atan2_pd(a, b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec16f sinh(Vec16f const x) {      // hyperbolic sine
    return _mm512_sinh_ps(x);
}
static inline Vec8d sinh(Vec8d const x) {        // hyperbolic sine
    return _mm512_sinh_pd(x);
}
static inline Vec16f cosh(Vec16f const x) {      // hyperbolic cosine
    return _mm512_cosh_ps(x);
}
static inline Vec8d cosh(Vec8d const x) {        // hyperbolic cosine
    return _mm512_cosh_pd(x);
}
static inline Vec16f tanh(Vec16f const x) {      // hyperbolic tangent
    return _mm512_tanh_ps(x);
}
static inline Vec8d tanh(Vec8d const x) {        // hyperbolic tangent
    return _mm512_tanh_pd(x);
}
static inline Vec16f asinh(Vec16f const x) {     // inverse hyperbolic sine
    return _mm512_asinh_ps(x);
}
static inline Vec8d asinh(Vec8d const x) {       // inverse hyperbolic sine
    return _mm512_asinh_pd(x);
}
static inline Vec16f acosh(Vec16f const x) {     // inverse hyperbolic cosine
    return _mm512_acosh_ps(x);
}
static inline Vec8d acosh(Vec8d const x) {       // inverse hyperbolic cosine
    return _mm512_acosh_pd(x);
}
static inline Vec16f atanh(Vec16f const x) {     // inverse hyperbolic tangent
    return _mm512_atanh_ps(x);
}
static inline Vec8d atanh(Vec8d const x) {       // inverse hyperbolic tangent
    return _mm512_atanh_pd(x);
}

// error function
static inline Vec16f erf(Vec16f const x) {       // error function
    return _mm512_erf_ps(x);
}
static inline Vec8d erf(Vec8d const x) {         // error function
    return _mm512_erf_pd(x);
} 
static inline Vec16f erfc(Vec16f const x) {      // error function complement
    return _mm512_erfc_ps(x);
}
static inline Vec8d erfc(Vec8d const x) {        // error function complement
    return _mm512_erfc_pd(x);
}
static inline Vec16f erfinv(Vec16f const x) {    // inverse error function
    return _mm512_erfinv_ps(x);
}
static inline Vec8d erfinv(Vec8d const x) {      // inverse error function
    return _mm512_erfinv_pd(x);
}

static inline Vec16f cdfnorm(Vec16f const x) {   // cumulative normal distribution function
    return _mm512_cdfnorm_ps(x);
}
static inline Vec8d cdfnorm(Vec8d const x) {     // cumulative normal distribution function
    return _mm512_cdfnorm_pd(x);
}
static inline Vec16f cdfnorminv(Vec16f const x) {// inverse cumulative normal distribution function
    return _mm512_cdfnorminv_ps(x);
}
static inline Vec8d cdfnorminv(Vec8d const x) {  // inverse cumulative normal distribution function
    return _mm512_cdfnorminv_pd(x);
}

#else    // __INTEL_COMPILER
/*****************************************************************************
*
*      512-bit vector functions using other compiler than Intel
*
*****************************************************************************/
                             
// External function prototypes, 512-bit vectors
extern "C" {
    extern __m512  V_VECTORCALL __svml_expf16       (__m512);
    extern __m512d V_VECTORCALL __svml_exp8        (__m512d);
    extern __m512  V_VECTORCALL __svml_expm1f16     (__m512);
    extern __m512d V_VECTORCALL __svml_expm18      (__m512d);
    extern __m512  V_VECTORCALL __svml_exp2f16      (__m512);
    extern __m512d V_VECTORCALL __svml_exp28       (__m512d);
    extern __m512  V_VECTORCALL __svml_exp10f16     (__m512);
    extern __m512d V_VECTORCALL __svml_exp108      (__m512d);
    extern __m512  V_VECTORCALL __svml_powf16       (__m512,  __m512);
    extern __m512d V_VECTORCALL __svml_pow8        (__m512d, __m512d);
    extern __m512  V_VECTORCALL __svml_cbrtf16      (__m512);
    extern __m512d V_VECTORCALL __svml_cbrt8       (__m512d);
    extern __m512  V_VECTORCALL __svml_invsqrtf16   (__m512);
    extern __m512d V_VECTORCALL __svml_invsqrt8    (__m512d);
    extern __m512  V_VECTORCALL __svml_logf16       (__m512);
    extern __m512d V_VECTORCALL __svml_log8        (__m512d);
    extern __m512  V_VECTORCALL __svml_log1pf16     (__m512);
    extern __m512d V_VECTORCALL __svml_log1p8      (__m512d);
    extern __m512  V_VECTORCALL __svml_log2f16      (__m512);
    extern __m512d V_VECTORCALL __svml_log28       (__m512d);
    extern __m512  V_VECTORCALL __svml_log10f16     (__m512);
    extern __m512d V_VECTORCALL __svml_log108      (__m512d);
    extern __m512  V_VECTORCALL __svml_sinf16       (__m512);
    extern __m512d V_VECTORCALL __svml_sin8        (__m512d);
    extern __m512  V_VECTORCALL __svml_cosf16       (__m512);
    extern __m512d V_VECTORCALL __svml_cos8        (__m512d);
    extern __m512  V_VECTORCALL __svml_sincosf16    (__m512); // cos returned in ymm1
    extern __m512d V_VECTORCALL __svml_sincos8     (__m512d); // cos returned in ymm1
    extern __m512  V_VECTORCALL __svml_tanf16       (__m512);
    extern __m512d V_VECTORCALL __svml_tan8        (__m512d);
    extern __m512  V_VECTORCALL __svml_asinf16      (__m512);
    extern __m512d V_VECTORCALL __svml_asin8       (__m512d);
    extern __m512  V_VECTORCALL __svml_acosf16      (__m512);
    extern __m512d V_VECTORCALL __svml_acos8       (__m512d);
    extern __m512  V_VECTORCALL __svml_atanf16      (__m512);
    extern __m512d V_VECTORCALL __svml_atan8       (__m512d);
    extern __m512  V_VECTORCALL __svml_atan2f16     (__m512, __m512);
    extern __m512d V_VECTORCALL __svml_atan28      (__m512d, __m512d);
    extern __m512  V_VECTORCALL __svml_sinhf16      (__m512);
    extern __m512d V_VECTORCALL __svml_sinh8       (__m512d);
    extern __m512  V_VECTORCALL __svml_coshf16      (__m512);
    extern __m512d V_VECTORCALL __svml_cosh8       (__m512d);
    extern __m512  V_VECTORCALL __svml_tanhf16      (__m512);
    extern __m512d V_VECTORCALL __svml_tanh8       (__m512d);
    extern __m512  V_VECTORCALL __svml_asinhf16     (__m512);
    extern __m512d V_VECTORCALL __svml_asinh8      (__m512d);
    extern __m512  V_VECTORCALL __svml_acoshf16     (__m512);
    extern __m512d V_VECTORCALL __svml_acosh8      (__m512d);
    extern __m512  V_VECTORCALL __svml_atanhf16     (__m512);
    extern __m512d V_VECTORCALL __svml_atanh8      (__m512d);
    extern __m512  V_VECTORCALL __svml_erff16       (__m512);
    extern __m512d V_VECTORCALL __svml_erf8        (__m512d);
    extern __m512  V_VECTORCALL __svml_erfcf16      (__m512);
    extern __m512d V_VECTORCALL __svml_erfc8       (__m512d);
    extern __m512  V_VECTORCALL __svml_erfinvf16    (__m512);
    extern __m512d V_VECTORCALL __svml_erfinv8     (__m512d);
    extern __m512  V_VECTORCALL __svml_cdfnorminvf16(__m512);
    extern __m512d V_VECTORCALL __svml_cdfnorminv8 (__m512d);
    extern __m512  V_VECTORCALL __svml_cdfnormf16   (__m512);
    extern __m512d V_VECTORCALL __svml_cdfnorm8    (__m512d);
    //extern __m512  V_VECTORCALL __svml_cexpf16    (__m512);
    //extern __m512d V_VECTORCALL __svml_cexp8     (__m512d);
}


// exponential and power functions
static inline Vec16f exp (Vec16f const x) {      // exponential function
    return  __svml_expf16(x);
}
static inline Vec8d exp (Vec8d const x) {        // exponential function
    return  __svml_exp8(x);
} 
static inline Vec16f expm1 (Vec16f const x) {    // exp(x)-1
    return  __svml_expm1f16(x);
}
static inline Vec8d expm1 (Vec8d const x) {      // exp(x)-1
    return  __svml_expm18(x);
}
static inline Vec16f exp2 (Vec16f const x) {     // pow(2,x)
    return  __svml_exp2f16(x);
}
static inline Vec8d exp2 (Vec8d const x) {       // pow(2,x)
    return  __svml_exp28(x);
} 
static inline Vec16f exp10 (Vec16f const x) {    // pow(10,x)
    return  __svml_exp10f16(x);
}
static inline Vec8d exp10 (Vec8d const x) {      // pow(10,x)
    return  __svml_exp108(x);
}
static inline Vec16f pow (Vec16f const a, Vec16f const b) {  // pow(a,b) = a to the power of b
    return  __svml_powf16(a,b);
}
static inline Vec16f pow (Vec16f const a, float const b) {   // pow(a,b) = a to the power of b
    return  __svml_powf16(a,Vec16f(b));
}
static inline Vec8d pow (Vec8d const a, Vec8d const b) {     // pow(a,b) = a to the power of b
    return  __svml_pow8(a,b);
}
static inline Vec8d pow (Vec8d const a, double const b) {    // pow(a,b) = a to the power of b
    return  __svml_pow8(a,Vec8d(b));
}
static inline Vec16f cbrt (Vec16f const x) {     // pow(x,1/3)
    return  __svml_cbrtf16(x);
}
static inline Vec8d cbrt (Vec8d const x) {       // pow(x,1/3)
    return  __svml_cbrt8(x);
}

// logarithms
static inline Vec16f log (Vec16f const x) {      // natural logarithm
    return  __svml_logf16(x);
}
static inline Vec8d log (Vec8d const x) {        // natural logarithm
    return  __svml_log8(x);
}
static inline Vec16f log1p (Vec16f const x) {    // log(1+x). Avoids loss of precision if 1+x is close to 1
    return  __svml_log1pf16(x);
}
static inline Vec8d log1p (Vec8d const x) {      // log(1+x). Avoids loss of precision if 1+x is close to 1
    return  __svml_log1p8(x);
} 
static inline Vec16f log2 (Vec16f const x) {     // logarithm base 2
    return  __svml_log2f16(x);
}
static inline Vec8d log2 (Vec8d const x) {       // logarithm base 2
    return  __svml_log28(x);
} 
static inline Vec16f log10 (Vec16f const x) {    // logarithm base 10
    return  __svml_log10f16(x);
}
static inline Vec8d log10 (Vec8d const x) {      // logarithm base 10
    return  __svml_log108(x);
}

// trigonometric functions (angles in radians)
static inline Vec16f sin (Vec16f const x) {      // sine
    return  __svml_sinf16(x);
}
static inline Vec8d sin (Vec8d const x) {        // sine
    return  __svml_sin8(x);
}
static inline Vec16f cos (Vec16f const x) {      // cosine
    return  __svml_cosf16(x);
}
static inline Vec8d cos (Vec8d const x) {        // cosine
    return  __svml_cos8(x);
}

#if defined(__unix__) || defined(__INTEL_COMPILER) //|| !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
// sine and cosine. sin(x) returned, cos(x) in pcos
static inline Vec16f sincos (Vec16f * pcos, Vec16f const x) { 
    __m512 r_sin, r_cos;
    r_sin = __svml_sincosf16(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "vmovaps %%zmm1, %0":"=m"(r_cos));
#else // Windows
    // _asm vmovaps r_cos, zmm1; // does not work in VS 2019
#endif
    *pcos = r_cos;
    return r_sin;
}
// sine and cosine. sin(x) returned, cos(x) in pcos
static inline Vec8d sincos (Vec8d * pcos, Vec8d const x) {   
    __m512d r_sin, r_cos;
    r_sin = __svml_sincos8(x);
#if defined(__unix__) || defined(__GNUC__)
    __asm__ __volatile__ ( "vmovaps %%zmm1, %0":"=m"(r_cos));
#else // Windows
    // _asm vmovapd r_cos, zmm1;  // does not work in VS 2019
#endif
    *pcos = r_cos;
    return r_sin;
}
#endif // inline assembly available

static inline Vec16f tan (Vec16f const x) {      // tangent
    return  __svml_tanf16(x);
}
static inline Vec8d tan (Vec8d const x) {        // tangent
    return  __svml_tan8(x);
} 

// inverse trigonometric functions
static inline Vec16f asin (Vec16f const x) {     // inverse sine
    return  __svml_asinf16(x);
}
static inline Vec8d asin (Vec8d const x) {       // inverse sine
    return  __svml_asin8(x);
}
static inline Vec16f acos (Vec16f const x) {     // inverse cosine
    return  __svml_acosf16(x);
}
static inline Vec8d acos (Vec8d const x) {       // inverse cosine
    return  __svml_acos8(x);
}
static inline Vec16f atan (Vec16f const x) {     // inverse tangent
    return  __svml_atanf16(x);
}
static inline Vec8d atan (Vec8d const x) {       // inverse tangent
    return  __svml_atan8(x);
} 
static inline Vec16f atan2 (Vec16f const a, Vec16f const b) {// inverse tangent of a/b
    return  __svml_atan2f16(a,b);
}
static inline Vec8d atan2 (Vec8d const a, Vec8d const b) {   // inverse tangent of a/b
    return  __svml_atan28(a,b);
}

// hyperbolic functions and inverse hyperbolic functions
static inline Vec16f sinh (Vec16f const x) {     // hyperbolic sine
    return  __svml_sinhf16(x);
}
static inline Vec8d sinh (Vec8d const x) {       // hyperbolic sine
    return  __svml_sinh8(x);
} 
static inline Vec16f cosh (Vec16f const x) {     // hyperbolic cosine
    return  __svml_coshf16(x);
}
static inline Vec8d cosh (Vec8d const x) {       // hyperbolic cosine
    return  __svml_cosh8(x);
} 
static inline Vec16f tanh (Vec16f const x) {     // hyperbolic tangent
    return  __svml_tanhf16(x);
}
static inline Vec8d tanh (Vec8d const x) {       // hyperbolic tangent
    return  __svml_tanh8(x);
}
static inline Vec16f asinh (Vec16f const x) {    // inverse hyperbolic sine
    return  __svml_asinhf16(x);
}
static inline Vec8d asinh (Vec8d const x) {      // inverse hyperbolic sine
    return  __svml_asinh8(x);
} 
static inline Vec16f acosh (Vec16f const x) {    // inverse hyperbolic cosine
    return  __svml_acoshf16(x);
}
static inline Vec8d acosh (Vec8d const x) {      // inverse hyperbolic cosine
    return  __svml_acosh8(x);
} 
static inline Vec16f atanh (Vec16f const x) {    // inverse hyperbolic tangent
    return  __svml_atanhf16(x);
}
static inline Vec8d atanh (Vec8d const x) {      // inverse hyperbolic tangent
    return  __svml_atanh8(x);
}

// error function
static inline Vec16f erf (Vec16f const x) {      // error function
    return  __svml_erff16(x);
}
static inline Vec8d erf (Vec8d const x) {        // error function
    return  __svml_erf8(x);
}
static inline Vec16f erfc (Vec16f const x) {     // error function complement
    return  __svml_erfcf16(x);
}
static inline Vec8d erfc (Vec8d const x) {       // error function complement
    return  __svml_erfc8(x);
} 
static inline Vec16f erfinv (Vec16f const x) {   // inverse error function
    return  __svml_erfinvf16(x);
}
static inline Vec8d erfinv (Vec8d const x) {     // inverse error function
    return  __svml_erfinv8(x);
} 

static inline Vec16f cdfnorm (Vec16f const x) {  // cumulative normal distribution function
    return  __svml_cdfnormf16(x);
}
static inline Vec8d cdfnorm (Vec8d const x) {    // cumulative normal distribution function
    return  __svml_cdfnorm8(x);
} 
static inline Vec16f cdfnorminv (Vec16f const x) {  // inverse cumulative normal distribution function
    return  __svml_cdfnorminvf16(x);
}
static inline Vec8d cdfnorminv (Vec8d const x) {    // inverse cumulative normal distribution function
    return  __svml_cdfnorminv8(x);
}

#endif   // __INTEL_COMPILER

#else    // VECTORF512_H
/*****************************************************************************
*
*      512-bit vector functions emulated with 256-bit vectors
*
*****************************************************************************/

// exponential and power functions
static inline Vec16f exp (Vec16f const x) {      // exponential function
    return Vec16f(exp(x.get_low()), exp(x.get_high()));
}
static inline Vec8d exp (Vec8d const x) {        // exponential function
    return Vec8d(exp(x.get_low()), exp(x.get_high()));
} 
static inline Vec16f expm1 (Vec16f const x) {    // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec16f(expm1(x.get_low()), expm1(x.get_high()));
}
static inline Vec8d expm1 (Vec8d const x) {      // exp(x)-1. Avoids loss of precision if x is close to 1
    return Vec8d(expm1(x.get_low()), expm1(x.get_high()));
} 
static inline Vec16f exp2 (Vec16f const x) {     // pow(2,x)
    return Vec16f(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec8d exp2 (Vec8d const x) {       // pow(2,x)
    return Vec8d(exp2(x.get_low()), exp2(x.get_high()));
}
static inline Vec16f exp10 (Vec16f const x) {    // pow(10,x)
    return Vec16f(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec8d exp10 (Vec8d const x) {      // pow(10,x)
    return Vec8d(exp10(x.get_low()), exp10(x.get_high()));
}
static inline Vec16f pow (Vec16f const a, Vec16f const b) {  // pow(a,b) = a to the power of b
    return Vec16f(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
}
static inline Vec16f pow (Vec16f const a, float const b) {   // pow(a,b) = a to the power of b
    return Vec16f(pow(a.get_low(),b), pow(a.get_high(),b));
}
static inline Vec8d pow (Vec8d const a, Vec8d const b) {     // pow(a,b) = a to the power of b
    return Vec8d(pow(a.get_low(),b.get_low()), pow(a.get_high(),b.get_high()));
} 
static inline Vec8d pow (Vec8d const a, double const b) {    // pow(a,b) = a to the power of b
    return Vec8d(pow(a.get_low(),b), pow(a.get_high(),b));
} 
static inline Vec16f cbrt (Vec16f const x) {     // pow(x,1/3)
    return Vec16f(cbrt(x.get_low()), cbrt(x.get_high()));
}
static inline Vec8d cbrt (Vec8d const x) {       // pow(x,1/3)
    return Vec8d(cbrt(x.get_low()), cbrt(x.get_high()));
}

// logarithms
static inline Vec16f log (Vec16f const x) {      // natural logarithm
    return Vec16f(log(x.get_low()), log(x.get_high()));
}
static inline Vec8d log (Vec8d const x) {        // natural logarithm
    return Vec8d(log(x.get_low()), log(x.get_high()));
} 
static inline Vec16f log1p (Vec16f const x) {    // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec16f(log1p(x.get_low()), log1p(x.get_high()));
}
static inline Vec8d log1p (Vec8d const x) {      // log(1+x). Avoids loss of precision if 1+x is close to 1
    return Vec8d(log1p(x.get_low()), log1p(x.get_high()));
} 
static inline Vec16f log2 (Vec16f const x) {     // logarithm base 2
    return Vec16f(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec8d log2 (Vec8d const x) {       // logarithm base 2
    return Vec8d(log2(x.get_low()), log2(x.get_high()));
}
static inline Vec16f log10 (Vec16f const x) {    // logarithm base 10
    return Vec16f(log10(x.get_low()), log10(x.get_high()));
}
static inline Vec8d log10 (Vec8d const x) {      // logarithm base 10
    return Vec8d(log10(x.get_low()), log10(x.get_high()));
}

// trigonometric functions (angles in radians)
static inline Vec16f sin (Vec16f const x) {      // sine
    return Vec16f(sin(x.get_low()), sin(x.get_high()));
}
static inline Vec8d sin (Vec8d const x) {        // sine
    return Vec8d(sin(x.get_low()), sin(x.get_high()));
} 
static inline Vec16f cos (Vec16f const x) {      // cosine
    return Vec16f(cos(x.get_low()), cos(x.get_high()));
}
static inline Vec8d cos (Vec8d const x) {        // cosine
    return Vec8d(cos(x.get_low()), cos(x.get_high()));
}

#if defined(__unix__) || defined(__INTEL_COMPILER) || !defined(__x86_64__) || !defined(_MSC_VER)
// no inline assembly in 64 bit MS compiler
static inline Vec16f sincos (Vec16f * pcos, Vec16f const x) {  // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec8f r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec16f(r_cos0, r_cos1);
    return Vec16f(r_sin0, r_sin1); 
}
static inline Vec8d sincos (Vec8d * pcos, Vec8d const x) {     // sine and cosine. sin(x) returned, cos(x) in pcos
    Vec4d r_sin0, r_sin1, r_cos0, r_cos1;
    r_sin0 = sincos(&r_cos0, x.get_low()); 
    r_sin1 = sincos(&r_cos1, x.get_high());
    *pcos = Vec8d(r_cos0, r_cos1);
    return Vec8d(r_sin0, r_sin1); 
}
#endif // inline assembly available


static inline Vec16f tan (Vec16f const x) {      // tangent
    return Vec16f(tan(x.get_low()), tan(x.get_high()));
}
static inline Vec8d tan (Vec8d const x) {        // tangent
    return Vec8d(tan(x.get_low()), tan(x.get_high()));
} 

// inverse trigonometric functions
static inline Vec16f asin (Vec16f const x) {     // inverse sine
    return Vec16f(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec8d asin (Vec8d const x) {       // inverse sine
    return Vec8d(asin(x.get_low()), asin(x.get_high()));
}
static inline Vec16f acos (Vec16f const x) {     // inverse cosine
    return Vec16f(acos(x.get_low()), acos(x.get_high()));
}
static inline Vec8d acos (Vec8d const x) {       // inverse cosine
    return Vec8d(acos(x.get_low()), acos(x.get_high()));
} 
static inline Vec16f atan (Vec16f const x) {     // inverse tangent
    return Vec16f(atan(x.get_low()), atan(x.get_high()));
}
static inline Vec8d atan (Vec8d const x) {       // inverse tangent
    return Vec8d(atan(x.get_low()), atan(x.get_high()));
} 
static inline Vec16f atan2 (Vec16f const a, Vec16f const b) {  // inverse tangent of a/b
    return Vec16f(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}
static inline Vec8d atan2 (Vec8d const a, Vec8d const b) {     // inverse tangent of a/b
    return Vec8d(atan2(a.get_low(),b.get_low()), atan2(a.get_high(),b.get_high()));
}

// hyperbolic functions
static inline Vec16f sinh (Vec16f const x) {     // hyperbolic sine
    return Vec16f(sinh(x.get_low()), sinh(x.get_high()));
}
static inline Vec8d sinh (Vec8d const x) {       // hyperbolic sine
    return Vec8d(sinh(x.get_low()), sinh(x.get_high()));
} 
static inline Vec16f cosh (Vec16f const x) {     // hyperbolic cosine
    return Vec16f(cosh(x.get_low()), cosh(x.get_high()));
}
static inline Vec8d cosh (Vec8d const x) {       // hyperbolic cosine
    return Vec8d(cosh(x.get_low()), cosh(x.get_high()));
} 
static inline Vec16f tanh (Vec16f const x) {     // hyperbolic tangent
    return Vec16f(tanh(x.get_low()), tanh(x.get_high()));
}
static inline Vec8d tanh (Vec8d const x) {       // hyperbolic tangent
    return Vec8d(tanh(x.get_low()), tanh(x.get_high()));
}

// inverse hyperbolic functions
static inline Vec16f asinh (Vec16f const x) {    // inverse hyperbolic sine
    return Vec16f(asinh(x.get_low()), asinh(x.get_high()));
}
static inline Vec8d asinh (Vec8d const x) {      // inverse hyperbolic sine
    return Vec8d(asinh(x.get_low()), asinh(x.get_high()));
}
static inline Vec16f acosh (Vec16f const x) {    // inverse hyperbolic cosine
    return Vec16f(acosh(x.get_low()), acosh(x.get_high()));
}
static inline Vec8d acosh (Vec8d const x) {      // inverse hyperbolic cosine
    return Vec8d(acosh(x.get_low()), acosh(x.get_high()));
}
static inline Vec16f atanh (Vec16f const x) {    // inverse hyperbolic tangent
    return Vec16f(atanh(x.get_low()), atanh(x.get_high()));
}
static inline Vec8d atanh (Vec8d const x) {      // inverse hyperbolic tangent
    return Vec8d(atanh(x.get_low()), atanh(x.get_high()));
}

// error function
static inline Vec16f erf (Vec16f const x) {      // error function
    return Vec16f(erf(x.get_low()), erf(x.get_high()));
}
static inline Vec8d erf (Vec8d const x) {        // error function
    return Vec8d(erf(x.get_low()), erf(x.get_high()));
} 
static inline Vec16f erfc (Vec16f const x) {     // error function complement
    return Vec16f(erfc(x.get_low()), erfc(x.get_high()));
}
static inline Vec8d erfc (Vec8d const x) {       // error function complement
    return Vec8d(erfc(x.get_low()), erfc(x.get_high()));
} 
static inline Vec16f erfinv (Vec16f const x) {   // inverse error function
    return Vec16f(erfinv(x.get_low()), erfinv(x.get_high()));
}
static inline Vec8d erfinv (Vec8d const x) {     // inverse error function
    return Vec8d(erfinv(x.get_low()), erfinv(x.get_high()));
}

static inline Vec16f cdfnorm (Vec16f const x) {  // cumulative normal distribution function
    return Vec16f(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
}
static inline Vec8d cdfnorm (Vec8d const x) {    // cumulative normal distribution function
    return Vec8d(cdfnorm(x.get_low()), cdfnorm(x.get_high()));
} 
static inline Vec16f cdfnorminv (Vec16f const x) { // inverse cumulative normal distribution function
    return Vec16f(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}
static inline Vec8d cdfnorminv (Vec8d const x) {   // inverse cumulative normal distribution function
    return Vec8d(cdfnorminv(x.get_low()), cdfnorminv(x.get_high()));
}

#endif   // VECTORF512_H

#endif   // MAX_VECTOR_SIZE >= 512

#ifdef   VCL_NAMESPACE
}
#endif   // VCL_NAMESPACE

#endif   // VECTORMATH_COMMON_H

#endif   // VECTORMATH_LIB_H
