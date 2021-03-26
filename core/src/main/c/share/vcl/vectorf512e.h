/****************************  vectorf512.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 512-bit floating point vector classes
* Emulated for processors without AVX512 instruction set
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec16f    Vector of  16  single precision floating point numbers
* Vec16fb   Vector of  16  Booleans for use with Vec16f
* Vec8d     Vector of   8  double precision floating point numbers
* Vec8db    Vector of   8  Booleans for use with Vec8d
*
* Each vector object is represented internally in the CPU as two 256-bit registers.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2014-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORF512E_H
#define VECTORF512E_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

#if defined (VECTORF512_H)
#error Two different versions of vectorf512.h included
#endif

#include "vectori512e.h"

#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

/*****************************************************************************
*
*          Vec16fb: Vector of 16 broad booleans for use with Vec16f
*
*****************************************************************************/
class Vec16fb : public Vec16b {
public:
    // Default constructor:
    Vec16fb () {
    }
    // Constructor to build from all elements:
    Vec16fb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16b(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15) {
    }
    // Constructor from Vec16b
    Vec16fb (Vec16b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor from two Vec8fb
    Vec16fb (Vec8fb const x0, Vec8fb const x1) {
#ifdef VECTORF256E_H
        z0 = reinterpret_i(x0);
        z1 = reinterpret_i(x1);
#else
        z0 = x0;
        z1 = x1;
#endif
    }
    // Constructor to broadcast scalar value:
    Vec16fb(bool b) : Vec16b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16fb & operator = (bool b) {
        *this = Vec16b(b);
        return *this;
    }
    // Get low and high half
    Vec8fb get_low() const {
        return reinterpret_f(Vec8i(z0));
    }
    Vec8fb get_high() const {
        return reinterpret_f(Vec8i(z1));
    }
    // Member function to change a bitfield to a boolean vector
    Vec16fb & load_bits(uint16_t a) {
        z0 = Vec8ib().load_bits(uint8_t(a));
        z1 = Vec8ib().load_bits(uint8_t(a>>8));
        return *this;
    }
    // Prevent constructing from int, etc.
    Vec16fb(int b) = delete;
    Vec16fb & operator = (int x) = delete;
};

// Define operators for Vec16fb

// vector operator & : bitwise and
static inline Vec16fb operator & (Vec16fb const a, Vec16fb const b) {
    return Vec16fb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16fb operator && (Vec16fb const a, Vec16fb const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16fb operator | (Vec16fb const a, Vec16fb const b) {
    return Vec16fb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16fb operator || (Vec16fb const a, Vec16fb const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16fb operator ^ (Vec16fb const a, Vec16fb const b) {
    return Vec16fb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator == : xnor
static inline Vec16fb operator == (Vec16fb const a, Vec16fb const b) {
    return Vec16fb(Vec16fb(a) ^ Vec16fb(~b));
}

// vector operator != : xor
static inline Vec16fb operator != (Vec16fb const a, Vec16fb const b) {
    return Vec16fb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec16fb operator ~ (Vec16fb const a) {
    return Vec16fb(~a.get_low(), ~a.get_high());
}

// vector operator ! : element not
static inline Vec16fb operator ! (Vec16fb const a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16fb & operator &= (Vec16fb & a, Vec16fb const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16fb & operator |= (Vec16fb & a, Vec16fb const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16fb & operator ^= (Vec16fb & a, Vec16fb const b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Vec8db: Vector of 8 broad booleans for use with Vec8d
*
*****************************************************************************/

class Vec8db : public Vec512b {
public:
    // Default constructor:
    Vec8db () {
    }
    // Constructor to build from all elements:
    Vec8db(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        z0 = Vec4qb(x0, x1, x2, x3);
        z1 = Vec4qb(x4, x5, x6, x7);
    }
    // Construct from Vec512b
    Vec8db (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor from two Vec4db
    Vec8db (Vec4db const x0, Vec4db const x1) {
#ifdef VECTORF256E_H
        z0 = reinterpret_i(x0);
        z1 = reinterpret_i(x1);
#else
        z0 = x0;
        z1 = x1;
#endif
    }
    // Constructor to broadcast single value:
    Vec8db(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec8db & operator = (bool b) {
        *this = Vec8db(b);
        return *this;
    }
    Vec8db & insert(int index, bool a) {
        if (index < 4) {
            z0 = Vec4q(z0).insert(index, -(int64_t)a);
        }
        else {
            z1 = Vec4q(z1).insert(index-4, -(int64_t)a);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4q(z0).extract(index) != 0;
        }
        else {
            return Vec4q(z1).extract(index-4) != 0;
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Get low and high half
    Vec4db get_low() const {
        return reinterpret_d(Vec4q(z0));
    }
    Vec4db get_high() const {
        return reinterpret_d(Vec4q(z1));
    }
    // Member function to change a bitfield to a boolean vector
    Vec8db & load_bits(uint8_t a) {
        z0 = Vec4qb().load_bits(a);
        z1 = Vec4qb().load_bits(uint8_t(a>>4u));
        return *this;
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc. because of ambiguity
    Vec8db(int b) = delete;
    // Prevent assigning int because of ambiguity
    Vec8db & operator = (int x) = delete;
};

// Define operators for Vec8db

// vector operator & : bitwise and
static inline Vec8db operator & (Vec8db const a, Vec8db const b) {
    return Vec8db(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8db operator && (Vec8db const a, Vec8db const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8db operator | (Vec8db const a, Vec8db const b) {
    return Vec8db(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec8db operator || (Vec8db const a, Vec8db const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8db operator ^ (Vec8db const a, Vec8db const b) {
    return Vec8db(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator == : xnor
static inline Vec8db operator == (Vec8db const a, Vec8db const b) {
    return Vec8db(Vec8db(a) ^ Vec8db(~b));
}

// vector operator != : xor
static inline Vec8db operator != (Vec8db const a, Vec8db const b) {
    return Vec8db(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec8db operator ~ (Vec8db const a) {
    return Vec8db(~a.get_low(), ~a.get_high());
}

// vector operator ! : element not
static inline Vec8db operator ! (Vec8db const a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8db & operator &= (Vec8db & a, Vec8db const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8db & operator |= (Vec8db & a, Vec8db const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8db & operator ^= (Vec8db & a, Vec8db const b) {
    a = a ^ b;
    return a;
}


/*****************************************************************************
*
*          Vec16f: Vector of 16 single precision floating point values
*
*****************************************************************************/

class Vec16f {
protected:
    Vec8f z0;
    Vec8f z1;
public:
    // Default constructor:
    Vec16f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16f(float f) {
        z0 = z1 = Vec8f(f);
    }
    // Constructor to build from all elements:
    Vec16f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7,
    float f8, float f9, float f10, float f11, float f12, float f13, float f14, float f15) {
        z0 = Vec8f(f0, f1, f2, f3, f4, f5, f6, f7);
        z1 = Vec8f(f8, f9, f10, f11, f12, f13, f14, f15);
    }
    // Constructor to build from two Vec8f:
    Vec16f(Vec8f const a0, Vec8f const a1) {
        z0 = a0;
        z1 = a1;
    }
    // split into two halves
    Vec8f get_low() const {
        return z0;
    }
    Vec8f get_high() const {
        return z1;
    }
    // Member function to load from array (unaligned)
    Vec16f & load(float const * p) {
        z0 = Vec8f().load(p);
        z1 = Vec8f().load(p+8);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address divisible by 64
    Vec16f & load_a(float const * p) {
        z0 = Vec8f().load_a(p);
        z1 = Vec8f().load_a(p+8);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        Vec8f(z0).store(p);
        Vec8f(z1).store(p+8);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(float * p) const {
        Vec8f(z0).store_nt(p);
        Vec8f(z1).store_nt(p+8);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return Vec8f::store_nt_alignment();
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address divisible by 64
    void store_a(float * p) const {
        Vec8f(z0).store_a(p);
        Vec8f(z1).store_a(p+8);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16f & load_partial(int n, float const * p) {
        if (n < 8) {
            z0 = Vec8f().load_partial(n, p);
            z1 = Vec8f(0.f);
        }
        else {
            z0 = Vec8f().load(p);
            z1 = Vec8f().load_partial(n-8, p + 8);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        if (n < 8) {
            Vec8f(z0).store_partial(n, p);
        }
        else {
            Vec8f(z0).store(p);
            Vec8f(z1).store_partial(n-8, p+8);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16f & cutoff(int n) {
        if (n < 8) {
            z0 = Vec8f(z0).cutoff(n);
            z1 = Vec8f(0.f);
        }
        else {
            z1 = Vec8f(z1).cutoff(n-8);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec16f const insert(int index, float value) {
        if ((uint32_t)index < 8) {
            z0 = Vec8f(z0).insert(index, value);
        }
        else {
            z1 = Vec8f(z1).insert(index-8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    float extract(int index) const {
        float a[16];
        store(a);
        return a[index & 15];
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 16;
    }
};


/*****************************************************************************
*
*          Operators for Vec16f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16f operator + (Vec16f const a, Vec16f const b) {
    return Vec16f(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec16f(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec16f(-a.get_low(), -a.get_high());
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
    return Vec16f(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec16f(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec16fb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16fb operator != (Vec16f const a, Vec16f const b) {
    return Vec16fb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16fb operator < (Vec16f const a, Vec16f const b) {
    return Vec16fb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec16fb operator <= (Vec16f const a, Vec16f const b) {
    return Vec16fb(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec16fb operator > (Vec16f const a, Vec16f const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec16fb operator >= (Vec16f const a, Vec16f const b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec16f operator & (Vec16f const a, Vec16f const b) {
    return Vec16f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec16f & operator &= (Vec16f & a, Vec16f const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec16f and Vec16fb
static inline Vec16f operator & (Vec16f const a, Vec16fb const b) {
    return Vec16f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16f operator & (Vec16fb const a, Vec16f const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec16f operator | (Vec16f const a, Vec16f const b) {
    return Vec16f(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec16f & operator |= (Vec16f & a, Vec16f const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16f operator ^ (Vec16f const a, Vec16f const b) {
    return Vec16f(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec16f & operator ^= (Vec16f & a, Vec16f const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec16fb operator ! (Vec16f const a) {
    return Vec16fb(!a.get_low(), !a.get_high());
}


/*****************************************************************************
*
*          Functions for Vec16f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFF (true). No other values are allowed.
static inline Vec16f select (Vec16fb const s, Vec16f const a, Vec16f const b) {
    return Vec16f(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16f if_add (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return Vec16f(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec16f if_sub (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return Vec16f(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec16f if_mul (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return Vec16f(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional divide
static inline Vec16f if_div (Vec16fb const f, Vec16f const a, Vec16f const b) {
    return Vec16f(if_div(f.get_low(), a.get_low(), b.get_low()), if_div(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec16f const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec16f max(Vec16f const a, Vec16f const b) {
    return Vec16f(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16f min(Vec16f const a, Vec16f const b) {
    return Vec16f(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec16f abs(Vec16f const a) {
    return Vec16f(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec16f sqrt(Vec16f const a) {
    return Vec16f(sqrt(a.get_low()), sqrt(a.get_high()));
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
static inline Vec16f pow_n(Vec16f const a) {
    if (n < 0)    return Vec16f(1.0f) / pow_n<-n>(a);
    if (n == 0)   return Vec16f(1.0f);
    if (n >= 256) return pow(a, n);
    Vec16f x = a;                      // a^(2^i)
    Vec16f y;                          // accumulator
    const int lowest = n - (n & (n-1));// lowest set bit in n
    if (n & 1) y = x;
    if (n < 2) return y;
    x = x*x;                           // x^2
    if (n & 2) {
        if (lowest == 2) y = x; else y *= x;
    }
    if (n < 4) return y;
    x = x*x;                           // x^4
    if (n & 4) {
        if (lowest == 4) y = x; else y *= x;
    }
    if (n < 8) return y;
    x = x*x;                           // x^8
    if (n & 8) {
        if (lowest == 8) y = x; else y *= x;
    }
    if (n < 16) return y;
    x = x*x;                           // x^16
    if (n & 16) {
        if (lowest == 16) y = x; else y *= x;
    }
    if (n < 32) return y;
    x = x*x;                           // x^32
    if (n & 32) {
        if (lowest == 32) y = x; else y *= x;
    }
    if (n < 64) return y;
    x = x*x;                           // x^64
    if (n & 64) {
        if (lowest == 64) y = x; else y *= x;
    }
    if (n < 128) return y;
    x = x*x;                           // x^128
    if (n & 128) {
        if (lowest == 128) y = x; else y *= x;
    }
    return y;
}

template <int n>
static inline Vec16f pow(Vec16f const a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as float vector)
static inline Vec16f round(Vec16f const a) {
    return Vec16f(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as float vector)
static inline Vec16f truncate(Vec16f const a) {
    return Vec16f(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec16f floor(Vec16f const a) {
    return Vec16f(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec16f ceil(Vec16f const a) {
    return Vec16f(ceil(a.get_low()), ceil(a.get_high()));
}

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec16i roundi(Vec16f const a) {
    return Vec16i(roundi(a.get_low()), roundi(a.get_high()));
}
//static inline Vec16i round_to_int(Vec16f const a) {return roundi(a);} // deprecated

// function truncatei: round towards zero. (result as integer vector)
static inline Vec16i truncatei(Vec16f const a) {
    return Vec16i(truncatei(a.get_low()), truncatei(a.get_high()));
}
//static inline Vec16i truncate_to_int(Vec16f const a) {return truncatei(a);} // deprecated

// function to_float: convert integer vector to float vector
static inline Vec16f to_float(Vec16i const a) {
    return Vec16f(to_float(a.get_low()), to_float(a.get_high()));
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec16f to_float(Vec16ui const a) {
    return Vec16f(to_float(a.get_low()), to_float(a.get_high()));
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a.
// relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_recipr(Vec16f const a) {
    return Vec16f(approx_recipr(a.get_low()), approx_recipr(a.get_high()));
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a).
// Relative accuracy better than 2^-11 without AVX512, 2^-14 with AVX512)
static inline Vec16f approx_rsqrt(Vec16f const a) {
    return Vec16f(approx_rsqrt(a.get_low()), approx_rsqrt(a.get_high()));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec16f mul_add(Vec16f const a, Vec16f const b, Vec16f const c) {
    return Vec16f(mul_add(a.get_low(), b.get_low(), c.get_low()), mul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract
static inline Vec16f mul_sub(Vec16f const a, Vec16f const b, Vec16f const c) {
    return Vec16f(mul_sub(a.get_low(), b.get_low(), c.get_low()), mul_sub(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and inverse subtract
static inline Vec16f nmul_add(Vec16f const a, Vec16f const b, Vec16f const c) {
    return Vec16f(nmul_add(a.get_low(), b.get_low(), c.get_low()), nmul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec16f mul_sub_x(Vec16f const a, Vec16f const b, Vec16f const c) {
    return Vec16f(mul_sub_x(a.get_low(), b.get_low(), c.get_low()), mul_sub_x(a.get_high(), b.get_high(), c.get_high()));
}


// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec16i exponent(Vec16f const a) {
    return Vec16i(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec16f fraction(Vec16f const a) {
    return Vec16f(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec16f exp2(Vec16i const n) {
    return Vec16f(exp2(n.get_low()), exp2(n.get_high()));
}
//static Vec16f exp2(Vec16f const x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec16f(-0.0f)) gives true, while Vec16f(-0.0f) < Vec16f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb sign_bit(Vec16f const a) {
    return Vec16fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec16f sign_combine(Vec16f const a, Vec16f const b) {
    return Vec16f(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_finite(Vec16f const a) {
    return Vec16fb(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_inf(Vec16f const a) {
    return Vec16fb(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec16fb is_nan(Vec16f const a) {
    return Vec16fb(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec16fb is_subnormal(Vec16f const a) {
    return Vec16fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec16fb is_zero_or_subnormal(Vec16f const a) {
    return Vec16fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec16f infinite16f() {
    Vec8f inf = infinite8f();
    return Vec16f(inf, inf);
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec16f nan16f(int n = 0x10) {
    Vec8f nan = nan8f(n);
    return Vec16f(nan, nan);
}

// change signs on vectors Vec16f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
// ("static" is removed from change_sign templates because it seems to generate problems for 
// the Clang compiler with nested template calls. "static" is probably superfluous anyway.)
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
inline Vec16f change_sign(Vec16f const a) {
    return Vec16f(change_sign<i0,i1,i2,i3,i4,i5,i6,i7>(a.get_low()), change_sign<i8,i9,i10,i11,i12,i13,i14,i15>(a.get_high()));
}


/*****************************************************************************
*
*          Vec8d: Vector of 8 double precision floating point values
*
*****************************************************************************/

class Vec8d {
protected:
    Vec4d z0;
    Vec4d z1;
public:
    // Default constructor:
    Vec8d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8d(double d) {
        z0 = z1 = Vec4d(d);
    }
    // Constructor to build from all elements:
    Vec8d(double d0, double d1, double d2, double d3, double d4, double d5, double d6, double d7) {
        z0 = Vec4d(d0, d1, d2, d3);
        z1 = Vec4d(d4, d5, d6, d7);
    }
    // Constructor to build from two Vec4d:
    Vec8d(Vec4d const a0, Vec4d const a1) {
        z0 = a0;
        z1 = a1;
    }
    // Member function to load from array (unaligned)
    Vec8d & load(double const * p) {
        z0.load(p);
        z1.load(p+4);
        return *this;
    }
    // Member function to load from array, aligned by 64
    // You may use load_a instead of load if you are certain that p points to an address divisible by 64
    Vec8d & load_a(double const * p) {
        z0.load_a(p);
        z1.load_a(p+4);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        z0.store(p);
        z1.store(p+4);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(double * p) const {
        z0.store_nt(p);
        z1.store_nt(p+4);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return Vec4d::store_nt_alignment();
    }
    // Member function to store into array, aligned by 64
    // You may use store_a instead of store if you are certain that p points to an address divisible by 64
    void store_a(double * p) const {
        z0.store_a(p);
        z1.store_a(p+4);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8d & load_partial(int n, double const * p) {
        if (n < 4) {
            z0.load_partial(n, p);
            z1 = Vec4d(0.);
        }
        else {
            z0.load(p);
            z1.load_partial(n-4, p+4);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        if (n < 4) {
            z0.store_partial(n, p);
        }
        else {
            z0.store(p);
            z1.store_partial(n-4, p+4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8d & cutoff(int n) {
        if (n < 4) {
            z0.cutoff(n);
            z1 = Vec4d(0.);
        }
        else {
            z1.cutoff(n-4);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec8d const insert(int index, double value) {
        if ((uint32_t)index < 4) {
            z0.insert(index, value);
        }
        else {
            z1.insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    double extract(int index) const {
        double a[8];
        store(a);
        return a[index & 7];        
    }

    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4d:
    Vec4d get_low() const {
        return z0;
    }
    Vec4d get_high() const {
        return z1;
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 17;
    }
}; 


/*****************************************************************************
*
*          Operators for Vec8d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8d operator + (Vec8d const a, Vec8d const b) {
    return Vec8d(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8d(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec8d(-a.get_low(), -a.get_high());
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
    return Vec8d(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec8d(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec8db(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8db operator != (Vec8d const a, Vec8d const b) {
    return Vec8db(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8db operator < (Vec8d const a, Vec8d const b) {
    return Vec8db(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8db operator <= (Vec8d const a, Vec8d const b) {
    return Vec8db(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec8db operator > (Vec8d const a, Vec8d const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8db operator >= (Vec8d const a, Vec8d const b) {
    return b <= a;
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8d operator & (Vec8d const a, Vec8d const b) {
    return Vec8d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8d & operator &= (Vec8d & a, Vec8d const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8d and Vec8db
static inline Vec8d operator & (Vec8d const a, Vec8db const b) {
    return Vec8d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

static inline Vec8d operator & (Vec8db const a, Vec8d const b) {
    return b & a;
}

// vector operator | : bitwise or
static inline Vec8d operator | (Vec8d const a, Vec8d const b) {
    return Vec8d(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8d & operator |= (Vec8d & a, Vec8d const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8d operator ^ (Vec8d const a, Vec8d const b) {
    return Vec8d(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8d & operator ^= (Vec8d & a, Vec8d const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8db operator ! (Vec8d const a) {
    return Vec8db(!a.get_low(), !a.get_high());
}

/*****************************************************************************
*
*          Functions for Vec8d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8d select (Vec8db const s, Vec8d const a, Vec8d const b) {
    return Vec8d(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8d if_add (Vec8db const f, Vec8d const a, Vec8d const b) {
    return Vec8d(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec8d if_sub (Vec8db const f, Vec8d const a, Vec8d const b) {
    return Vec8d(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec8d if_mul (Vec8db const f, Vec8d const a, Vec8d const b) {
    return Vec8d(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional divide
static inline Vec8d if_div (Vec8db const f, Vec8d const a, Vec8d const b) {
    return Vec8d(if_div(f.get_low(), a.get_low(), b.get_low()), if_div(f.get_high(), a.get_high(), b.get_high()));
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec8d const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec8d max(Vec8d const a, Vec8d const b) {
    return Vec8d(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8d min(Vec8d const a, Vec8d const b) {
    return Vec8d(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8d abs(Vec8d const a) {
    return Vec8d(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec8d sqrt(Vec8d const a) {
    return Vec8d(sqrt(a.get_low()), sqrt(a.get_high()));
}

// function square: a * a
static inline Vec8d square(Vec8d const a) {
    return a * a;
}

// pow(Vec8d, int):
template <typename TT> static Vec8d pow(Vec8d const a, TT const n);

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
static inline Vec8d pow_n(Vec8d const a) {
    if (n < 0)    return Vec8d(1.0) / pow_n<-n>(a);
    if (n == 0)   return Vec8d(1.0);
    if (n >= 256) return pow(a, n);
    Vec8d x = a;                       // a^(2^i)
    Vec8d y;                           // accumulator
    const int lowest = n - (n & (n-1));// lowest set bit in n
    if (n & 1) y = x;
    if (n < 2) return y;
    x = x*x;                           // x^2
    if (n & 2) {
        if (lowest == 2) y = x; else y *= x;
    }
    if (n < 4) return y;
    x = x*x;                           // x^4
    if (n & 4) {
        if (lowest == 4) y = x; else y *= x;
    }
    if (n < 8) return y;
    x = x*x;                           // x^8
    if (n & 8) {
        if (lowest == 8) y = x; else y *= x;
    }
    if (n < 16) return y;
    x = x*x;                           // x^16
    if (n & 16) {
        if (lowest == 16) y = x; else y *= x;
    }
    if (n < 32) return y;
    x = x*x;                           // x^32
    if (n & 32) {
        if (lowest == 32) y = x; else y *= x;
    }
    if (n < 64) return y;
    x = x*x;                           // x^64
    if (n & 64) {
        if (lowest == 64) y = x; else y *= x;
    }
    if (n < 128) return y;
    x = x*x;                           // x^128
    if (n & 128) {
        if (lowest == 128) y = x; else y *= x;
    }
    return y;
}

template <int n>
static inline Vec8d pow(Vec8d const a, Const_int_t<n>) {
    return pow_n<n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec8d round(Vec8d const a) {
    return Vec8d(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as double vector)
static inline Vec8d truncate(Vec8d const a) {
    return Vec8d(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec8d floor(Vec8d const a) {
    return Vec8d(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec8d ceil(Vec8d const a) {
    return Vec8d(ceil(a.get_low()), ceil(a.get_high()));
}

// function round_to_int32: round to nearest integer (even). (result as integer vector)
static inline Vec8i round_to_int32(Vec8d const a) {
    // Note: assume MXCSR control register is set to rounding
    return Vec8i(round_to_int32(a.get_low()), round_to_int32(a.get_high()));
}
//static inline Vec8i round_to_int(Vec8d const a) {return round_to_int32(a);} // deprecated

// function truncate_to_int32: round towards zero. (result as integer vector)
static inline Vec8i truncate_to_int32(Vec8d const a) {
    return Vec8i(truncate_to_int32(a.get_low()), truncate_to_int32(a.get_high()));
}
//static inline Vec8i truncate_to_int(Vec8d const a) {return truncate_to_int32(a);} // deprecated

// function truncatei: round towards zero. (inefficient)
static inline Vec8q truncatei(Vec8d const a) {
    return Vec8q(truncatei(a.get_low()), truncatei(a.get_high()));
}
//static inline Vec8q truncate_to_int64(Vec8d const a) {return truncatei(a);} // deprecated

// function roundi: round to nearest or even. (inefficient)
static inline Vec8q roundi(Vec8d const a) {
    return Vec8q(roundi(a.get_low()), roundi(a.get_high()));
}
//static inline Vec8q round_to_int64(Vec8d const a) {return roundi(a);} // deprecated

// function to_double: convert integer vector elements to double vector (inefficient)
static inline Vec8d to_double(Vec8q const a) {
    return Vec8d(to_double(a.get_low()), to_double(a.get_high()));
}

// function to_double: convert unsigned integer vector elements to double vector (inefficient)
static inline Vec8d to_double(Vec8uq const a) {
    return Vec8d(to_double(a.get_low()), to_double(a.get_high()));
}

// function to_double: convert integer vector to double vector
static inline Vec8d to_double(Vec8i const a) {
    return Vec8d(to_double(a.get_low()), to_double(a.get_high()));
}

// function compress: convert two Vec8d to one Vec16f
static inline Vec16f compress (Vec8d const low, Vec8d const high) {
    return Vec16f(compress(low.get_low(), low.get_high()), compress(high.get_low(), high.get_high()));
}

// Function extend_low : convert Vec16f vector elements 0 - 3 to Vec8d
static inline Vec8d extend_low(Vec16f const a) {
    return Vec8d(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : convert Vec16f vector elements 4 - 7 to Vec8d
static inline Vec8d extend_high (Vec16f const a) {
    return Vec8d(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec8d mul_add(Vec8d const a, Vec8d const b, Vec8d const c) {
    return Vec8d(mul_add(a.get_low(), b.get_low(), c.get_low()), mul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract
static inline Vec8d mul_sub(Vec8d const a, Vec8d const b, Vec8d const c) {
    return Vec8d(mul_sub(a.get_low(), b.get_low(), c.get_low()), mul_sub(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and inverse subtract
static inline Vec8d nmul_add(Vec8d const a, Vec8d const b, Vec8d const c) {
    return Vec8d(nmul_add(a.get_low(), b.get_low(), c.get_low()), nmul_add(a.get_high(), b.get_high(), c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec8d mul_sub_x(Vec8d const a, Vec8d const b, Vec8d const c) {
    return Vec8d(mul_sub_x(a.get_low(), b.get_low(), c.get_low()), mul_sub_x(a.get_high(), b.get_high(), c.get_high()));
}

// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec8q exponent(Vec8d const a) {
    return Vec8q(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec8d fraction(Vec8d const a) {
    return Vec8d(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec8d exp2(Vec8q const n) {
    return Vec8d(exp2(n.get_low()), exp2(n.get_high()));
}
//static Vec8d exp2(Vec8d const x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec8d(-0.0)) gives true, while Vec8d(-0.0) < Vec8d(0.0) gives false
static inline Vec8db sign_bit(Vec8d const a) {
    return Vec8db(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8d sign_combine(Vec8d const a, Vec8d const b) {
    return Vec8d(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec8db is_finite(Vec8d const a) {
    return Vec8db(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec8db is_inf(Vec8d const a) {
    return Vec8db(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec8db is_nan(Vec8d const a) {
    return Vec8db(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8db is_subnormal(Vec8d const a) {
    return Vec8db(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8db is_zero_or_subnormal(Vec8d const a) {
    return Vec8db(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec8d infinite8d() {
    Vec4d inf = infinite4d();
    return Vec8d(inf, inf);
}

// Function nan8d: returns a vector where all elements are +NAN (quiet NAN)
static inline Vec8d nan8d(int n = 0x10) {
    Vec4d nan = nan4d(n);
    return Vec8d(nan, nan);
}

// change signs on vectors Vec8d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
inline Vec8d change_sign(Vec8d const a) {
    return Vec8d(change_sign<i0,i1,i2,i3>(a.get_low()), change_sign<i4,i5,i6,i7>(a.get_high()));
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

static inline Vec512b reinterpret_i (Vec512b const x) {
    return x;
}

static inline Vec512b reinterpret_i (Vec16f  const x) {
    return Vec512b(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec512b reinterpret_i (Vec8d const x) {
    return Vec512b(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec16f  reinterpret_f (Vec512b const x) {
    return Vec16f(Vec8f(reinterpret_f(x.get_low())), Vec8f(reinterpret_f(x.get_high())));
}

static inline Vec16f  reinterpret_f (Vec16f  const x) {
    return x;
}

static inline Vec16f  reinterpret_f (Vec8d const x) {
    return Vec16f(Vec8f(reinterpret_f(x.get_low())), Vec8f(reinterpret_f(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec512b const x) {
    return Vec8d(Vec4d(reinterpret_d(x.get_low())), Vec4d(reinterpret_d(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec16f  const x) {
    return Vec8d(Vec4d(reinterpret_d(x.get_low())), Vec4d(reinterpret_d(x.get_high())));
}

static inline Vec8d reinterpret_d (Vec8d const x) {
    return x;
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

// Permute vector of 8 double
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8d permute8(Vec8d const a) {
    return Vec8d(blend4<i0,i1,i2,i3> (a.get_low(), a.get_high()),
                 blend4<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

// Permute vector of 16 float
// Index -1 gives 0, index -256 means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16f permute16(Vec16f const a) {
    return Vec16f(blend8<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()),
                  blend8<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

// blend vectors Vec8d
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8d blend8(Vec8d const a, Vec8d const b) {  
    Vec4d x0 = blend_half<Vec8d, i0, i1, i2, i3>(a, b);
    Vec4d x1 = blend_half<Vec8d, i4, i5, i6, i7>(a, b);
    return Vec8d(x0, x1);
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16f blend16(Vec16f const a, Vec16f const b) {
    Vec8f x0 = blend_half<Vec16f, i0, i1, i2, i3, i4, i5, i6, i7>(a, b);
    Vec8f x1 = blend_half<Vec16f, i8, i9, i10, i11, i12, i13, i14, i15>(a, b);
    return Vec16f(x0, x1);
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
    float tab[16];
    table.store(tab);
    Vec8f t0 = reinterpret_f(lookup<16>(index.get_low(), tab));
    Vec8f t1 = reinterpret_f(lookup<16>(index.get_high(), tab));
    return Vec16f(t0, t1);
}

template <int n>
static inline Vec16f lookup(Vec16i const index, float const * table) {
    if (n <=  0) return 0;
    if (n <= 16) return lookup16(index, Vec16f().load(table));
    // n > 16. Limit index
    Vec16ui i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec16ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec16ui(index), n-1);
    }
    float const * t = table;
    return Vec16f(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]],
        t[i1[8]],t[i1[9]],t[i1[10]],t[i1[11]],t[i1[12]],t[i1[13]],t[i1[14]],t[i1[15]]);
}

static inline Vec8d lookup8(Vec8q const index, Vec8d const table) {
    double tab[8];
    table.store(tab);
    Vec4d t0 = reinterpret_d(lookup<8>(index.get_low(), tab));
    Vec4d t1 = reinterpret_d(lookup<8>(index.get_high(), tab));
    return Vec8d(t0, t1);
} 

template <int n>
static inline Vec8d lookup(Vec8q const index, double const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        return lookup8(index, Vec8d().load(table));
    }
    // n > 8. Limit index
    Vec8uq i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec8uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec8uq(index), n-1);
    }
    double const * t = table;
    return Vec8d(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
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
    // use lookup function
    return lookup<imax+1>(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), (const float *)a);
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
    // use lookup function
    return lookup<imax+1>(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), (const double *)a);
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
    const int index[16] = {i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15};
    for (int i = 0; i < 16; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline void scatter(Vec8d const data, double * array) {
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
}

// Scatter functions with variable indexes:

static inline void scatter(Vec16i const index, uint32_t limit, Vec16f const data, float * destination) {
    uint32_t ix[16];  index.store(ix);
    for (int i = 0; i < 16; i++) {
        if (ix[i] < limit) destination[ix[i]] = data[i];
    }
}

static inline void scatter(Vec8q const index, uint32_t limit, Vec8d const data, double * destination) {
    uint64_t ix[8];  index.store(ix);
    for (int i = 0; i < 8; i++) {
        if (ix[i] < limit) destination[ix[i]] = data[i];
    }
}

static inline void scatter(Vec8i const index, uint32_t limit, Vec8d const data, double * destination) {
    uint32_t ix[8];  index.store(ix);
    for (int i = 0; i < 8; i++) {
        if (ix[i] < limit) destination[ix[i]] = data[i];
    }
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16fb const x) {
    return to_bits(Vec16ib(x));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8db const x) {
    return to_bits(Vec8qb(x));
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORF512E_H
