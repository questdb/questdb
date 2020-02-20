/***************************  vectormath_common.h   ****************************
* Author:        Agner Fog
* Date created:  2014-04-18
* Last modified: 2019-08-01
* Version:       2.00.00
* Project:       vector classes
* Description:
* Header file containing common code for inline version of mathematical functions.
*
* For detailed instructions, see VectorClass.pdf
*
* (c) Copyright 2014-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef VECTORMATH_COMMON_H
#define VECTORMATH_COMMON_H  2

#ifdef VECTORMATH_LIB_H
#error conflicting header files. More than one implementation of mathematical functions included
#endif

#if VECTORCLASS_H < 20000
#error Incompatible versions of vector class library mixed
#endif

#include <cmath>
#include "vectorclass.h"

/******************************************************************************
                    Define NAN payload values
******************************************************************************/
#define NAN_LOG 0x101  // logarithm for x<0
#define NAN_POW 0x102  // negative number raised to non-integer power
#define NAN_HYP 0x104  // acosh for x<1 and atanh for abs(x)>1


/******************************************************************************
                    Define mathematical constants
******************************************************************************/
#define VM_PI       3.14159265358979323846           // pi
#define VM_PI_2     1.57079632679489661923           // pi / 2
#define VM_PI_4     0.785398163397448309616          // pi / 4
#define VM_SQRT2    1.41421356237309504880           // sqrt(2)
#define VM_LOG2E    1.44269504088896340736           // 1/log(2)
#define VM_LOG10E   0.434294481903251827651          // 1/log(10)
#define VM_LOG210   3.321928094887362347808          // log2(10)
#define VM_LN2      0.693147180559945309417          // log(2)
#define VM_LN10     2.30258509299404568402           // log(10)
#define VM_SMALLEST_NORMAL  2.2250738585072014E-308  // smallest normal number, double
#define VM_SMALLEST_NORMALF 1.17549435E-38f          // smallest normal number, float


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

/******************************************************************************
      templates for producing infinite and nan in desired vector type
******************************************************************************/
template <class VTYPE>
static inline VTYPE infinite_vec();

template <>
inline Vec2d infinite_vec<Vec2d>() {
    return infinite2d();
}

template <>
inline Vec4f infinite_vec<Vec4f>() {
    return infinite4f();
}

#if MAX_VECTOR_SIZE >= 256

template <>
inline Vec4d infinite_vec<Vec4d>() {
    return infinite4d();
}

template <>
inline Vec8f infinite_vec<Vec8f>() {
    return infinite8f();
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512

template <>
inline Vec8d infinite_vec<Vec8d>() {
    return infinite8d();
}

template <>
inline Vec16f infinite_vec<Vec16f>() {
    return infinite16f();
}

#endif // MAX_VECTOR_SIZE >= 512


/******************************************************************************
                  templates for polynomials
Using Estrin's scheme to make shorter dependency chains and use FMA, starting
longest dependency chains first.
******************************************************************************/

// template <typedef VECTYPE, typedef CTYPE> 
template <class VTYPE, class CTYPE>
static inline VTYPE polynomial_2(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2) {
    // calculates polynomial c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    //return = x2 * c2 + (x * c1 + c0);
    return mul_add(x2, c2, mul_add(x, c1, c0));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_3(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3) {
    // calculates polynomial c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    //return (c2 + c3*x)*x2 + (c1*x + c0);
    return mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_4(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4) {
    // calculates polynomial c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return (c2+c3*x)*x2 + ((c0+c1*x) + c4*x4);
    return mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0) + c4*x4);
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_4n(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3) {
    // calculates polynomial 1*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return (c2+c3*x)*x2 + ((c0+c1*x) + x4);
    return mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0) + x4);
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_5(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5) {
    // calculates polynomial c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return (c2+c3*x)*x2 + ((c4+c5*x)*x4 + (c0+c1*x));
    return mul_add(mul_add(c3, x, c2), x2, mul_add(mul_add(c5, x, c4), x4, mul_add(c1, x, c0)));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_5n(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4) {
    // calculates polynomial 1*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return (c2+c3*x)*x2 + ((c4+x)*x4 + (c0+c1*x));
    return mul_add(mul_add(c3, x, c2), x2, mul_add(c4 + x, x4, mul_add(c1, x, c0)));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_6(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6) {
    // calculates polynomial c6*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return  (c4+c5*x+c6*x2)*x4 + ((c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(c6, x2, mul_add(c5, x, c4)), x4, mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0)));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_6n(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5) {
    // calculates polynomial 1*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return  (c4+c5*x+x2)*x4 + ((c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(c5, x, c4 + x2), x4, mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0)));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_7(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7) {
    // calculates polynomial c7*x^7 + c6*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x * x;
    VTYPE x4 = x2 * x2;
    //return  ((c6+c7*x)*x2 + (c4+c5*x))*x4 + ((c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4, mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0)));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_8(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7, CTYPE c8) {
    // calculates polynomial c8*x^8 + c7*x^7 + c6*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x  * x;
    VTYPE x4 = x2 * x2;
    VTYPE x8 = x4 * x4;
    //return  ((c6+c7*x)*x2 + (c4+c5*x))*x4 + (c8*x8 + (c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4,
        mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0) + c8*x8));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_9(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7, CTYPE c8, CTYPE c9) {
    // calculates polynomial c9*x^9 + c8*x^8 + c7*x^7 + c6*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x  * x;
    VTYPE x4 = x2 * x2;
    VTYPE x8 = x4 * x4;
    //return  (((c6+c7*x)*x2 + (c4+c5*x))*x4 + (c8+c9*x)*x8) + ((c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(c9, x, c8), x8, mul_add(
        mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4,
        mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0))));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_10(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7, CTYPE c8, CTYPE c9, CTYPE c10) {
    // calculates polynomial c10*x^10 + c9*x^9 + c8*x^8 + c7*x^7 + c6*x^6 + c5*x^5 + c4*x^4 + c3*x^3 + c2*x^2 + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x  * x;
    VTYPE x4 = x2 * x2;
    VTYPE x8 = x4 * x4;
    //return  (((c6+c7*x)*x2 + (c4+c5*x))*x4 + (c8+c9*x+c10*x2)*x8) + ((c2+c3*x)*x2 + (c0+c1*x));
    return mul_add(mul_add(x2, c10, mul_add(c9, x, c8)), x8,
        mul_add(mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4,
            mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0))));
}

template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_13(VTYPE const x, CTYPE c0, CTYPE c1, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7, CTYPE c8, CTYPE c9, CTYPE c10, CTYPE c11, CTYPE c12, CTYPE c13) {
    // calculates polynomial c13*x^13 + c12*x^12 + ... + c1*x + c0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x  * x;
    VTYPE x4 = x2 * x2;
    VTYPE x8 = x4 * x4;
    return mul_add(
        mul_add(
            mul_add(c13, x, c12), x4,
            mul_add(mul_add(c11, x, c10), x2, mul_add(c9, x, c8))), x8,
        mul_add(
            mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4,
            mul_add(mul_add(c3, x, c2), x2, mul_add(c1, x, c0))));
}


template<class VTYPE, class CTYPE>
static inline VTYPE polynomial_13m(VTYPE const x, CTYPE c2, CTYPE c3, CTYPE c4, CTYPE c5, CTYPE c6, CTYPE c7, CTYPE c8, CTYPE c9, CTYPE c10, CTYPE c11, CTYPE c12, CTYPE c13) {
    // calculates polynomial c13*x^13 + c12*x^12 + ... + x + 0
    // VTYPE may be a vector type, CTYPE is a scalar type
    VTYPE x2 = x  * x;
    VTYPE x4 = x2 * x2;
    VTYPE x8 = x4 * x4;
    // return  ((c8+c9*x) + (c10+c11*x)*x2 + (c12+c13*x)*x4)*x8 + (((c6+c7*x)*x2 + (c4+c5*x))*x4 + ((c2+c3*x)*x2 + x));
    return mul_add(
        mul_add(mul_add(c13, x, c12), x4, mul_add(mul_add(c11, x, c10), x2, mul_add(c9, x, c8))), x8,
        mul_add(mul_add(mul_add(c7, x, c6), x2, mul_add(c5, x, c4)), x4, mul_add(mul_add(c3, x, c2), x2, x)));
}

#ifdef VCL_NAMESPACE
}
#endif

#endif
