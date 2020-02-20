/****************************  vectormath_trig.h   ******************************
* Author:        Agner Fog
* Date created:  2014-04-18
* Last modified: 2019-08-30
* Version:       2.00.01
* Project:       vector class library
* Description:
* Header file containing inline version of trigonometric functions 
* and inverse trigonometric functions
* sin, cos, sincos, tan
* asin, acos, atan, atan2
*
* Theory, methods and inspiration based partially on these sources:
* > Moshier, Stephen Lloyd Baluk: Methods and programs for mathematical functions.
*   Ellis Horwood, 1989.
* > VDT library developed on CERN by Danilo Piparo, Thomas Hauth and
*   Vincenzo Innocente, 2012, https://svnweb.cern.ch/trac/vdt
* > Cephes math library by Stephen L. Moshier 1992,
*   http://www.netlib.org/cephes/
*
* For detailed instructions, see vectormath_common.h and vcl_manual.pdf
*
* (c) Copyright 2014-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef VECTORMATH_TRIG_H
#define VECTORMATH_TRIG_H  1

#include "vectormath_common.h"

#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


// *************************************************************
//             sin/cos template, double precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// SC:     1 = sin, 2 = cos, 3 = sincos
// Paramterers:
// xx = input x (radians)
// cosret = return pointer (only if SC = 3)
template<typename VTYPE, int SC>
static inline VTYPE sincos_d(VTYPE * cosret, VTYPE const xx) {

    // define constants
    const double P0sin = -1.66666666666666307295E-1;
    const double P1sin = 8.33333333332211858878E-3;
    const double P2sin = -1.98412698295895385996E-4;
    const double P3sin = 2.75573136213857245213E-6;
    const double P4sin = -2.50507477628578072866E-8;
    const double P5sin = 1.58962301576546568060E-10;

    const double P0cos = 4.16666666666665929218E-2;
    const double P1cos = -1.38888888888730564116E-3;
    const double P2cos = 2.48015872888517045348E-5;
    const double P3cos = -2.75573141792967388112E-7;
    const double P4cos = 2.08757008419747316778E-9;
    const double P5cos = -1.13585365213876817300E-11;

    const double DP1 = 7.853981554508209228515625E-1 * 2.;
    const double DP2 = 7.94662735614792836714E-9 * 2.;
    const double DP3 = 3.06161699786838294307E-17 * 2.;
    /*
    const double DP1sc = 7.85398125648498535156E-1;
    const double DP2sc = 3.77489470793079817668E-8;
    const double DP3sc = 2.69515142907905952645E-15;
    */
    typedef decltype(roundi(xx)) ITYPE;          // integer vector type
    typedef decltype(xx < xx) BVTYPE;            // boolean vector type

    VTYPE  xa, x, y, x2, s, c, sin1, cos1;       // data vectors
    ITYPE  q, qq, signsin, signcos;              // integer vectors, 64 bit
    BVTYPE swap, overflow;                       // boolean vectors

    xa = abs(xx);

    // Find quadrant
    y = round(xa * (double)(2. / VM_PI));        // quadrant, as float
    q = roundi(y);                               // quadrant, as integer
    // Find quadrant
    //      0 -   pi/4 => 0
    //   pi/4 - 3*pi/4 => 1
    // 3*pi/4 - 5*pi/4 => 2
    // 5*pi/4 - 7*pi/4 => 3
    // 7*pi/4 - 8*pi/4 => 4

    // Reduce by extended precision modular arithmetic
    x = nmul_add(y, DP3, nmul_add(y, DP2, nmul_add(y, DP1, xa)));    // x = ((xa - y * DP1) - y * DP2) - y * DP3;

    // Expansion of sin and cos, valid for -pi/4 <= x <= pi/4
    x2 = x * x;
    s = polynomial_5(x2, P0sin, P1sin, P2sin, P3sin, P4sin, P5sin);
    c = polynomial_5(x2, P0cos, P1cos, P2cos, P3cos, P4cos, P5cos);
    s = mul_add(x * x2, s, x);                                       // s = x + (x * x2) * s;
    c = mul_add(x2 * x2, c, nmul_add(x2, 0.5, 1.0));                 // c = 1.0 - x2 * 0.5 + (x2 * x2) * c;

    // swap sin and cos if odd quadrant
    swap = BVTYPE((q & 1) != 0);

    // check for overflow
    overflow = BVTYPE(q > 0x80000000000000);  // q big if overflow
    overflow &= is_finite(xa);
    s = select(overflow, 0.0, s);
    c = select(overflow, 1.0, c);

    if constexpr ((SC & 1) != 0) {  // calculate sin
        sin1 = select(swap, c, s);
        signsin = ((q << 62) ^ ITYPE(reinterpret_i(xx)));
        sin1 = sign_combine(sin1, reinterpret_d(signsin));
    }
    if constexpr ((SC & 2) != 0) {  // calculate cos
        cos1 = select(swap, s, c);
        signcos = ((q + 1) & 2) << 62;
        cos1 ^= reinterpret_d(signcos);
    }
    if constexpr (SC == 3) {  // calculate both. cos returned through pointer
        *cosret = cos1;
    }
    if constexpr ((SC & 1) != 0) return sin1; else return cos1;
}

// instantiations of sincos_d template:

static inline Vec2d sin(Vec2d const x) {
    return sincos_d<Vec2d, 1>(0, x);
}

static inline Vec2d cos(Vec2d const x) {
    return sincos_d<Vec2d, 2>(0, x);
}

static inline Vec2d sincos(Vec2d * cosret, Vec2d const x) {
    return sincos_d<Vec2d, 3>(cosret, x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d sin(Vec4d const x) {
    return sincos_d<Vec4d, 1>(0, x);
}

static inline Vec4d cos(Vec4d const x) {
    return sincos_d<Vec4d, 2>(0, x);
}

static inline Vec4d sincos(Vec4d * cosret, Vec4d const x) {
    return sincos_d<Vec4d, 3>(cosret, x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d sin(Vec8d const x) {
    return sincos_d<Vec8d, 1>(0, x);
}

static inline Vec8d cos(Vec8d const x) {
    return sincos_d<Vec8d, 2>(0, x);
}

static inline Vec8d sincos(Vec8d * cosret, Vec8d const x) {
    return sincos_d<Vec8d, 3>(cosret, x);
}
#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             sincos template, single precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// SC:     1 = sin, 2 = cos, 3 = sincos, 4 = tan
// Paramterers:
// xx = input x (radians)
// cosret = return pointer (only if SC = 3)
template<typename VTYPE, int SC>
static inline VTYPE sincos_f(VTYPE * cosret, VTYPE const xx) {

    // define constants
    const float DP1F = 0.78515625f * 2.f;
    const float DP2F = 2.4187564849853515625E-4f * 2.f;
    const float DP3F = 3.77489497744594108E-8f * 2.f;

    const float P0sinf = -1.6666654611E-1f;
    const float P1sinf = 8.3321608736E-3f;
    const float P2sinf = -1.9515295891E-4f;

    const float P0cosf = 4.166664568298827E-2f;
    const float P1cosf = -1.388731625493765E-3f;
    const float P2cosf = 2.443315711809948E-5f;

    typedef decltype(roundi(xx)) ITYPE;          // integer vector type
    typedef decltype(xx < xx) BVTYPE;            // boolean vector type

    VTYPE  xa, x, y, x2, s, c, sin1, cos1;       // data vectors
    ITYPE  q, signsin, signcos;                  // integer vectors
    BVTYPE swap, overflow;                       // boolean vectors

    xa = abs(xx);

    // Find quadrant
    y = round(xa * (float)(2. / VM_PI));         // quadrant, as float
    q = roundi(y);                               // quadrant, as integer
    //      0 -   pi/4 => 0
    //   pi/4 - 3*pi/4 => 1
    // 3*pi/4 - 5*pi/4 => 2
    // 5*pi/4 - 7*pi/4 => 3
    // 7*pi/4 - 8*pi/4 => 4

    // Reduce by extended precision modular arithmetic
    // x = ((xa - y * DP1F) - y * DP2F) - y * DP3F;
    x = nmul_add(y, DP3F, nmul_add(y, DP2F, nmul_add(y, DP1F, xa)));
    
    // A two-step reduction saves time at the cost of precision for very big x:
    //x = (xa - y * DP1F) - y * (DP2F+DP3F);

    // Taylor expansion of sin and cos, valid for -pi/4 <= x <= pi/4
    x2 = x * x;
    s = polynomial_2(x2, P0sinf, P1sinf, P2sinf) * (x*x2) + x;
    c = polynomial_2(x2, P0cosf, P1cosf, P2cosf) * (x2*x2) + nmul_add(0.5f, x2, 1.0f);

    // swap sin and cos if odd quadrant
    swap = BVTYPE((q & 1) != 0);

    // check for overflow
    overflow = BVTYPE(q > 0x2000000);  // q big if overflow
    overflow &= is_finite(xa);
    s = select(overflow, 0.0f, s);
    c = select(overflow, 1.0f, c);

    if constexpr ((SC & 5) != 0) {  // calculate sin
        sin1 = select(swap, c, s);
        signsin = ((q << 30) ^ ITYPE(reinterpret_i(xx)));
        sin1 = sign_combine(sin1, reinterpret_f(signsin));
    }
    if constexpr ((SC & 6) != 0) {  // calculate cos
        cos1 = select(swap, s, c);
        signcos = ((q + 1) & 2) << 30;
        cos1 ^= reinterpret_f(signcos);
    }
    if constexpr (SC == 1) return sin1;
    else if constexpr (SC == 2) return cos1;
    else if constexpr (SC == 3) {  // calculate both. cos returned through pointer
        *cosret = cos1;
        return sin1;
    }
    else {  // SC == 4. tan
        return sin1 / cos1;
    }
}

// instantiations of sincos_f template:

static inline Vec4f sin(Vec4f const x) {
    return sincos_f<Vec4f, 1>(0, x);
}

static inline Vec4f cos(Vec4f const x) {
    return sincos_f<Vec4f, 2>(0, x);
}

static inline Vec4f sincos(Vec4f * cosret, Vec4f const x) {
    return sincos_f<Vec4f, 3>(cosret, x);
}

static inline Vec4f tan(Vec4f const x) {
    return sincos_f<Vec4f, 4>(0, x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f sin(Vec8f const x) {
    return sincos_f<Vec8f, 1>(0, x);
}

static inline Vec8f cos(Vec8f const x) {
    return sincos_f<Vec8f, 2>(0, x);
}

static inline Vec8f sincos(Vec8f * cosret, Vec8f const x) {
    return sincos_f<Vec8f, 3>(cosret, x);
}

static inline Vec8f tan(Vec8f const x) {
    return sincos_f<Vec8f, 4>(0, x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f sin(Vec16f const x) {
    return sincos_f<Vec16f, 1>(0, x);
}

static inline Vec16f cos(Vec16f const x) {
    return sincos_f<Vec16f, 2>(0, x);
}

static inline Vec16f sincos(Vec16f * cosret, Vec16f const x) {
    return sincos_f<Vec16f, 3>(cosret, x);
}

static inline Vec16f tan(Vec16f const x) {
    return sincos_f<Vec16f, 4>(0, x);
}
#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             tan template, double precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// Paramterers:
// x = input x (radians)
template<typename VTYPE>
static inline VTYPE tan_d(VTYPE const x) {

    // define constants
    const double DP1 = 7.853981554508209228515625E-1 * 2.;;
    const double DP2 = 7.94662735614792836714E-9 * 2.;;
    const double DP3 = 3.06161699786838294307E-17 * 2.;;

    const double P2tan = -1.30936939181383777646E4;
    const double P1tan = 1.15351664838587416140E6;
    const double P0tan = -1.79565251976484877988E7;

    const double Q3tan = 1.36812963470692954678E4;
    const double Q2tan = -1.32089234440210967447E6;
    const double Q1tan = 2.50083801823357915839E7;
    const double Q0tan = -5.38695755929454629881E7;

    typedef decltype(x > x) BVTYPE;         // boolean vector type 
    VTYPE  xa, y, z, zz, px, qx, tn, recip; // data vectors
    BVTYPE doinvert, xzero, overflow;       // boolean vectors

    xa = abs(x);

    // Find quadrant
    y = round(xa * (double)(2. / VM_PI));   // quadrant, as float
    auto q = roundi(y);                     // quadrant, as integer
    // Find quadrant
    //      0 -   pi/4 => 0
    //   pi/4 - 3*pi/4 => 1
    // 3*pi/4 - 5*pi/4 => 2
    // 5*pi/4 - 7*pi/4 => 3
    // 7*pi/4 - 8*pi/4 => 4

    // Reduce by extended precision modular arithmetic    
    // z = ((xa - y * DP1) - y * DP2) - y * DP3;
    z = nmul_add(y, DP3, nmul_add(y, DP2, nmul_add(y, DP1, xa)));

    // Pade expansion of tan, valid for -pi/4 <= x <= pi/4
    zz = z * z;
    px = polynomial_2(zz, P0tan, P1tan, P2tan);
    qx = polynomial_4n(zz, Q0tan, Q1tan, Q2tan, Q3tan);

    // qx cannot be 0 for x <= pi/4
    tn = mul_add(px / qx, z * zz, z);            // tn = z + z * zz * px / qx;

    // if (q&2) tn = -1/tn
    doinvert = BVTYPE((q & 1) != 0);
    xzero = (xa == 0.);
    // avoid division by 0. We will not be using recip anyway if xa == 0.
    // tn never becomes exactly 0 when x = pi/2 so we only have to make 
    // a special case for x == 0.
    recip = (-1.) / select(xzero, VTYPE(-1.), tn);
    tn = select(doinvert, recip, tn);
    tn = sign_combine(tn, x);       // get original sign

    overflow = BVTYPE(q > 0x80000000000000) & is_finite(xa);
    tn = select(overflow, 0., tn);

    return tn;
}

// instantiations of tan_d template:

static inline Vec2d tan(Vec2d const x) {
    return tan_d(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d tan(Vec4d const x) {
    return tan_d(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d tan(Vec8d const x) {
    return tan_d(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             tan template, single precision
// *************************************************************
// This is removed for the single precision version.
// It is faster to use tan(x) = sin(x)/cos(x)



// *************************************************************
//             asin/acos template, double precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// AC: 0 = asin, 1 = acos
// Paramterers:
// x = input x
template<typename VTYPE, int AC>
static inline VTYPE asin_d(VTYPE const x) {

    // define constants
    const double R4asin = 2.967721961301243206100E-3;
    const double R3asin = -5.634242780008963776856E-1;
    const double R2asin = 6.968710824104713396794E0;
    const double R1asin = -2.556901049652824852289E1;
    const double R0asin = 2.853665548261061424989E1;

    const double S3asin = -2.194779531642920639778E1;
    const double S2asin = 1.470656354026814941758E2;
    const double S1asin = -3.838770957603691357202E2;
    const double S0asin = 3.424398657913078477438E2;

    const double P5asin = 4.253011369004428248960E-3;
    const double P4asin = -6.019598008014123785661E-1;
    const double P3asin = 5.444622390564711410273E0;
    const double P2asin = -1.626247967210700244449E1;
    const double P1asin = 1.956261983317594739197E1;
    const double P0asin = -8.198089802484824371615E0;

    const double Q4asin = -1.474091372988853791896E1;
    const double Q3asin = 7.049610280856842141659E1;
    const double Q2asin = -1.471791292232726029859E2;
    const double Q1asin = 1.395105614657485689735E2;
    const double Q0asin = -4.918853881490881290097E1;

    VTYPE  xa, xb, x1, x2, x3, x4, x5, px, qx, rx, sx, vx, wx, y1, yb, z, z1, z2;
    bool   dobig, dosmall;

    xa = abs(x);
    auto big = xa >= 0.625;  // boolean vector

    /*
    Small: xa < 0.625
    ------------------
    x = xa * xa;
    px = PX(x);
    qx = QX(x);
    y1 = x*px/qx;
    y1 = xa * y1 + xa;

    Big: xa >= 0.625
    ------------------
    x = 1.0 - xa;
    rx = RX(x);
    sx = SX(x);
    y1 = x * rx/sx;
    x3 = sqrt(x+x);
    y3 = x3 * y1 - MOREBITS;
    z = pi/2 - x3 - y3
    */

    // select a common x for all polynomials
    // This allows sharing of powers of x through common subexpression elimination
    x1 = select(big, 1.0 - xa, xa * xa);

    // calculate powers of x1 outside branches to make sure they are only calculated once
    x2 = x1 * x1;
    x4 = x2 * x2;
    x5 = x4 * x1;
    x3 = x2 * x1;

    dosmall = !horizontal_and(big);    // at least one element is small
    dobig = horizontal_or(big);        // at least one element is big

    // calculate polynomials (reuse powers of x)
    if (dosmall) {
        // px = polynomial_5 (x1, P0asin, P1asin, P2asin, P3asin, P4asin, P5asin);
        // qx = polynomial_5n(x1, Q0asin, Q1asin, Q2asin, Q3asin, Q4asin);
        px = mul_add(x3, P3asin, P0asin) + mul_add(x4, P4asin, x1*P1asin) + mul_add(x5, P5asin, x2*P2asin);
        qx = mul_add(x4, Q4asin, x5) + mul_add(x3, Q3asin, x1*Q1asin) + mul_add(x2, Q2asin, Q0asin);
    }
    if (dobig) {
        // rx = polynomial_4 (x1, R0asin, R1asin, R2asin, R3asin, R4asin);
        // sx = polynomial_4n(x1, S0asin, S1asin, S2asin, S3asin);
        rx = mul_add(x3, R3asin, x2*R2asin) + mul_add(x4, R4asin, mul_add(x1, R1asin, R0asin));
        sx = mul_add(x3, S3asin, x4) + mul_add(x2, S2asin, mul_add(x1, S1asin, S0asin));
    }

    // select and divide outside branches to avoid dividing twice
    vx = select(big, rx, px);
    wx = select(big, sx, qx);
    y1 = vx / wx * x1;

    // results for big
    if (dobig) {                                 // avoid square root if all are small
        xb = sqrt(x1 + x1);                      // this produces NAN if xa > 1 so we don't need a special case for xa > 1
        z1 = mul_add(xb, y1, xb);                // yb = xb * y1; z1 = xb + yb;
    }

    // results for small        
    z2 = mul_add(xa, y1, xa);                    // z2 = xa * y1 + xa;

    // correct for sign
    if constexpr (AC == 1) {                     // acos
        z1 = select(x < 0., VM_PI - z1, z1);
        z2 = VM_PI_2 - sign_combine(z2, x);
        z = select(big, z1, z2);
    }
    else {     // asin
        z1 = VM_PI_2 - z1;
        z = select(big, z1, z2);
        z = sign_combine(z, x);
    }
    return z;
}

// instantiations of asin_d template:

static inline Vec2d asin(Vec2d const x) {
    return asin_d<Vec2d, 0>(x);
}

static inline Vec2d acos(Vec2d const x) {
    return asin_d<Vec2d, 1>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d asin(Vec4d const x) {
    return asin_d<Vec4d, 0>(x);
}

static inline Vec4d acos(Vec4d const x) {
    return asin_d<Vec4d, 1>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d asin(Vec8d const x) {
    return asin_d<Vec8d, 0>(x);
}

static inline Vec8d acos(Vec8d const x) {
    return asin_d<Vec8d, 1>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             asin/acos template, single precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// AC: 0 = asin, 1 = acos
// Paramterers:
// x = input x
template<typename VTYPE, int AC>
static inline VTYPE asin_f(VTYPE const x) {

    // define constants
    const float P4asinf = 4.2163199048E-2f;
    const float P3asinf = 2.4181311049E-2f;
    const float P2asinf = 4.5470025998E-2f;
    const float P1asinf = 7.4953002686E-2f;
    const float P0asinf = 1.6666752422E-1f;

    VTYPE  xa, x1, x2, x3, x4, xb, z, z1, z2;

    xa = abs(x);
    auto big = xa > 0.5f;                        // boolean vector

    x1 = 0.5f * (1.0f - xa);
    x2 = xa * xa;
    x3 = select(big, x1, x2);

    //if (horizontal_or(big)) 
    {
        xb = sqrt(x1);
    }
    x4 = select(big, xb, xa);

    z = polynomial_4(x3, P0asinf, P1asinf, P2asinf, P3asinf, P4asinf);
    z = mul_add(z, x3*x4, x4);                   // z = z * (x3*x4) + x4;
    z1 = z + z;

    // correct for sign
    if constexpr (AC == 1) {                     // acos
        z1 = select(x < 0., float(VM_PI) - z1, z1);
        z2 = float(VM_PI_2) - sign_combine(z, x);
        z = select(big, z1, z2);
    }
    else {     // asin
        z1 = float(VM_PI_2) - z1;
        z = select(big, z1, z);
        z = sign_combine(z, x);
    }

    return z;
}

// instantiations of asin_f template:

static inline Vec4f asin(Vec4f const x) {
    return asin_f<Vec4f, 0>(x);
}

static inline Vec4f acos(Vec4f const x) {
    return asin_f<Vec4f, 1>(x);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f asin(Vec8f const x) {
    return asin_f<Vec8f, 0>(x);
}
static inline Vec8f acos(Vec8f const x) {
    return asin_f<Vec8f, 1>(x);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f asin(Vec16f const x) {
    return asin_f<Vec16f, 0>(x);
}
static inline Vec16f acos(Vec16f const x) {
    return asin_f<Vec16f, 1>(x);
}
#endif // MAX_VECTOR_SIZE >= 512


// *************************************************************
//             atan template, double precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// T2:     0 = atan, 1 = atan2
// Paramterers:
// y, x. calculate tan(y/x)
// result is between -pi/2 and +pi/2 when x > 0
// result is between -pi and -pi/2 or between pi/2 and pi when x < 0 for atan2
template<typename VTYPE, int T2>
static inline VTYPE atan_d(VTYPE const y, VTYPE const x) {

    // define constants
    //const double ONEOPIO4 = 4./VM_PI;
    const double MOREBITS = 6.123233995736765886130E-17;
    const double MOREBITSO2 = MOREBITS * 0.5;
    const double T3PO8 = VM_SQRT2 + 1.; // 2.41421356237309504880;

    const double P4atan = -8.750608600031904122785E-1;
    const double P3atan = -1.615753718733365076637E1;
    const double P2atan = -7.500855792314704667340E1;
    const double P1atan = -1.228866684490136173410E2;
    const double P0atan = -6.485021904942025371773E1;

    const double Q4atan = 2.485846490142306297962E1;
    const double Q3atan = 1.650270098316988542046E2;
    const double Q2atan = 4.328810604912902668951E2;
    const double Q1atan = 4.853903996359136964868E2;
    const double Q0atan = 1.945506571482613964425E2;

    typedef decltype (x > x) BVTYPE;                            // boolean vector type
    VTYPE  t, x1, x2, y1, y2, s, fac, a, b, z, zz, px, qx, re;  // data vectors
    BVTYPE swapxy, notbig, notsmal;                             // boolean vectors

    if constexpr (T2 == 1) {  // atan2(y,x)
        // move in first octant
        x1 = abs(x);
        y1 = abs(y);
        swapxy = (y1 > x1);
        // swap x and y if y1 > x1
        x2 = select(swapxy, y1, x1);
        y2 = select(swapxy, x1, y1);

        // check for special case: x and y are both +/- INF
        BVTYPE both_infinite = is_inf(x) & is_inf(y);   // x and Y are both infinite
        if (horizontal_or(both_infinite)) {             // at least one element has both infinite
            VTYPE mone = VTYPE(-1.0);
            x2 = select(both_infinite, x2 & mone, x2);  // get 1.0 with the sign of x
            y2 = select(both_infinite, y2 & mone, y2);  // get 1.0 with the sign of y
        }

        t = y2 / x2;                  // x = y = 0 gives NAN here
    }
    else {    // atan(y)
        t = abs(y);
    }

    // small:  t < 0.66
    // medium: 0.66 <= t <= 2.4142 (1+sqrt(2))
    // big:    t > 2.4142
    notbig  = t <= T3PO8;  // t <= 2.4142
    notsmal = t >= 0.66;   // t >= 0.66

    s   = select(notbig, VTYPE(VM_PI_4), VTYPE(VM_PI_2));
    s   = notsmal & s;                   // select(notsmal, s, 0.);
    fac = select(notbig, VTYPE(MOREBITSO2), VTYPE(MOREBITS));
    fac = notsmal & fac;  //select(notsmal, fac, 0.);

    // small:  z = t / 1.0;
    // medium: z = (t-1.0) / (t+1.0);
    // big:    z = -1.0 / t;
    a = notbig & t;                    // select(notbig, t, 0.);
    a = if_add(notsmal, a, -1.);
    b = notbig & VTYPE(1.);            //  select(notbig, 1., 0.);
    b = if_add(notsmal, b, t);
    z = a / b;                         // division by 0 will not occur unless x and y are both 0

    zz = z * z;

    px = polynomial_4(zz, P0atan, P1atan, P2atan, P3atan, P4atan);
    qx = polynomial_5n(zz, Q0atan, Q1atan, Q2atan, Q3atan, Q4atan);

    re = mul_add(px / qx, z * zz, z);  // re = (px / qx) * (z * zz) + z;
    re += s + fac;

    if constexpr (T2 == 1) {           // atan2(y,x)
        // move back in place
        re = select(swapxy, VM_PI_2 - re, re);
        re = select((x | y) == 0., 0., re);      // atan2(0,0) = 0 by convention
        re = select(sign_bit(x), VM_PI - re, re);// also for x = -0.
    }
    // get sign bit
    re = sign_combine(re, y);

    return re;
}

// instantiations of atan_d template:

static inline Vec2d atan2(Vec2d const y, Vec2d const x) {
    return atan_d<Vec2d, 1>(y, x);
}

static inline Vec2d atan(Vec2d const y) {
    return atan_d<Vec2d, 0>(y, 0.);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec4d atan2(Vec4d const y, Vec4d const x) {
    return atan_d<Vec4d, 1>(y, x);
}

static inline Vec4d atan(Vec4d const y) {
    return atan_d<Vec4d, 0>(y, 0.);
}
#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec8d atan2(Vec8d const y, Vec8d const x) {
    return atan_d<Vec8d, 1>(y, x);
}

static inline Vec8d atan(Vec8d const y) {
    return atan_d<Vec8d, 0>(y, 0.);
}
#endif // MAX_VECTOR_SIZE >= 512



// *************************************************************
//             atan template, single precision
// *************************************************************
// Template parameters:
// VTYPE:  f.p. vector type
// T2:     0 = atan, 1 = atan2
// Paramterers:
// y, x. calculate tan(y/x)
// result is between -pi/2 and +pi/2 when x > 0
// result is between -pi and -pi/2 or between pi/2 and pi when x < 0 for atan2
template<typename VTYPE, int T2>
static inline VTYPE atan_f(VTYPE const y, VTYPE const x) {

    // define constants
    const float P3atanf = 8.05374449538E-2f;
    const float P2atanf = -1.38776856032E-1f;
    const float P1atanf = 1.99777106478E-1f;
    const float P0atanf = -3.33329491539E-1f;

    typedef decltype (x > x) BVTYPE;             // boolean vector type
    VTYPE  t, x1, x2, y1, y2, s, a, b, z, zz, re;// data vectors
    BVTYPE swapxy, notbig, notsmal;              // boolean vectors

    if constexpr (T2 == 1) {  // atan2(y,x)
        // move in first octant
        x1 = abs(x);
        y1 = abs(y);
        swapxy = (y1 > x1);
        // swap x and y if y1 > x1
        x2 = select(swapxy, y1, x1);
        y2 = select(swapxy, x1, y1);

        // check for special case: x and y are both +/- INF
        BVTYPE both_infinite = is_inf(x) & is_inf(y);   // x and Y are both infinite
        if (horizontal_or(both_infinite)) {             // at least one element has both infinite
            VTYPE mone = VTYPE(-1.0f);
            x2 = select(both_infinite, x2 & mone, x2);  // get 1.0 with the sign of x
            y2 = select(both_infinite, y2 & mone, y2);  // get 1.0 with the sign of y
        }

        // x = y = 0 will produce NAN. No problem, fixed below
        t = y2 / x2;
    }
    else {    // atan(y)
        t = abs(y);
    }

    // small:  t < 0.4142
    // medium: 0.4142 <= t <= 2.4142
    // big:    t > 2.4142  (not for atan2)
    if constexpr (T2 == 0) {  // atan(y)
        notsmal = t >= float(VM_SQRT2 - 1.);     // t >= tan  pi/8
        notbig = t <= float(VM_SQRT2 + 1.);      // t <= tan 3pi/8

        s = select(notbig, VTYPE(float(VM_PI_4)), VTYPE(float(VM_PI_2)));
        s = notsmal & s;                         // select(notsmal, s, 0.);

        // small:  z = t / 1.0;
        // medium: z = (t-1.0) / (t+1.0);
        // big:    z = -1.0 / t;
        a = notbig & t;                // select(notbig, t, 0.);
        a = if_add(notsmal, a, -1.f);
        b = notbig & VTYPE(1.f);       //  select(notbig, 1., 0.);
        b = if_add(notsmal, b, t);
        z = a / b;                     // division by 0 will not occur unless x and y are both 0
    }
    else {  // atan2(y,x)
        // small:  z = t / 1.0;
        // medium: z = (t-1.0) / (t+1.0);
        notsmal = t >= float(VM_SQRT2 - 1.);
        a = if_add(notsmal, t, -1.f);
        b = if_add(notsmal, 1.f, t);
        s = notsmal & VTYPE(float(VM_PI_4));
        z = a / b;
    }

    zz = z * z;

    // Taylor expansion
    re = polynomial_3(zz, P0atanf, P1atanf, P2atanf, P3atanf);
    re = mul_add(re, zz * z, z) + s;

    if constexpr (T2 == 1) {                               // atan2(y,x)
        // move back in place
        re = select(swapxy, float(VM_PI_2) - re, re);
        re = select((x | y) == 0.f, 0.f, re);              // atan2(0,+0) = 0 by convention
        re = select(sign_bit(x), float(VM_PI) - re, re);   // also for x = -0.
    }
    // get sign bit
    re = sign_combine(re, y);

    return re;
}

// instantiations of atan_f template:

static inline Vec4f atan2(Vec4f const y, Vec4f const x) {
    return atan_f<Vec4f, 1>(y, x);
}

static inline Vec4f atan(Vec4f const y) {
    return atan_f<Vec4f, 0>(y, 0.);
}

#if MAX_VECTOR_SIZE >= 256
static inline Vec8f atan2(Vec8f const y, Vec8f const x) {
    return atan_f<Vec8f, 1>(y, x);
}

static inline Vec8f atan(Vec8f const y) {
    return atan_f<Vec8f, 0>(y, 0.);
}

#endif // MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
static inline Vec16f atan2(Vec16f const y, Vec16f const x) {
    return atan_f<Vec16f, 1>(y, x);
}

static inline Vec16f atan(Vec16f const y) {
    return atan_f<Vec16f, 0>(y, 0.);
}

#endif // MAX_VECTOR_SIZE >= 512

#ifdef VCL_NAMESPACE
}
#endif

#endif
