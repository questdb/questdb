/****************************  vectorf256e.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 256-bit floating point vector classes
* Emulated for processors without AVX instruction set.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec8f     Vector of 8 single precision floating point numbers
* Vec8fb    Vector of 8 Booleans for use with Vec8f
* Vec4d     Vector of 4 double precision floating point numbers
* Vec4db    Vector of 4 Booleans for use with Vec4d
*
* Each vector object is represented internally in the CPU as two 128-bit registers.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORF256E_H
#define VECTORF256E_H  1

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

#ifdef VECTORF256_H
#error Two different versions of vectorf256.h included
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

/*****************************************************************************
*
*          base class Vec256fe and Vec256de
*
*****************************************************************************/

// base class to replace __m256 when AVX is not supported
class Vec256fe {
protected:
    __m128 y0;                         // low half
    __m128 y1;                         // high half
public:
    Vec256fe(void) {};                 // default constructor
    Vec256fe(__m128 x0, __m128 x1) {   // constructor to build from two __m128
        y0 = x0;  y1 = x1;
    }
    __m128 get_low() const {           // get low half
        return y0;
    }
    __m128 get_high() const {          // get high half
        return y1;
    }
};

// base class to replace __m256d when AVX is not supported
class Vec256de {
public:
    Vec256de() {};                     // default constructor
    Vec256de(__m128d x0, __m128d x1) { // constructor to build from two __m128d
        y0 = x0;  y1 = x1;
    }
    __m128d get_low() const {          // get low half
        return y0;
    }
    __m128d get_high() const {         // get high half
        return y1;
    }
protected:
    __m128d y0;                        // low half
    __m128d y1;                        // high half
};


/*****************************************************************************
*
*          select functions
*
*****************************************************************************/
// Select between two Vec256fe sources, element by element using broad boolean vector.
// Used in various functions and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFF (true).
static inline Vec256fe selectf (Vec256fe const s, Vec256fe const a, Vec256fe const b) {
    return Vec256fe(selectf(b.get_low(), a.get_low(), s.get_low()), selectf(b.get_high(), a.get_high(), s.get_high()));
}

// Same, with two Vec256de sources.
// and operators. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each element in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). No other 
// values are allowed.
static inline Vec256de selectd (Vec256de const s, Vec256de const a, Vec256de const b) {
    return Vec256de(selectd(b.get_low(), a.get_low(), s.get_low()), selectd(b.get_high(), a.get_high(), s.get_high()));
} 


/*****************************************************************************
*
*          Vec8fb: Vector of 8 Booleans for use with Vec8f
*
*****************************************************************************/

class Vec8fb : public Vec256fe {
public:
    // Default constructor:
    Vec8fb() {
    }
    // Constructor to build from all elements:
    Vec8fb(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
        y0 = Vec4fb(b0, b1, b2, b3);
        y1 = Vec4fb(b4, b5, b6, b7);
    }
    // Constructor to build from two Vec4fb:
    Vec8fb(Vec4fb const a0, Vec4fb const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256fe
    Vec8fb(Vec256fe const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Constructor to broadcast scalar value:
    Vec8fb(bool b) {
        y0 = y1 = Vec4fb(b);
    }
    // Assignment operator to convert from type Vec256fe
    Vec8fb & operator = (Vec256fe const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Constructor to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb(Vec8ib const x) {
        y0 = _mm_castsi128_ps(Vec8i(x).get_low());
        y1 = _mm_castsi128_ps(Vec8i(x).get_high());
    }
    // Assignment operator to convert from type Vec8ib used as Boolean for integer vectors
    Vec8fb & operator = (Vec8ib const x) {
        y0 = _mm_castsi128_ps(Vec8i(x).get_low());
        y1 = _mm_castsi128_ps(Vec8i(x).get_high());
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec8fb & operator = (bool b) {
        y0 = y1 = Vec4fb(b);
        return *this;
    }
    // Type cast operator to convert to type Vec8ib used as Boolean for integer vectors
    operator Vec8ib() const {
        return Vec8i(_mm_castps_si128(y0), _mm_castps_si128(y1));
    }

    // Member function to change a single element in vector
    Vec8fb const insert(int index, bool value) {
        if ((uint32_t)index < 4) {
            y0 = Vec4fb(y0).insert(index, value);
        }
        else {
            y1 = Vec4fb(y1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4fb(y0).extract(index);
        }
        else {
            return Vec4fb(y1).extract(index-4);
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec4fb get_low() const {
        return y0;
    }
    Vec4fb get_high() const {
        return y1;
    }
    // Member function to change a bitfield to a boolean vector
    Vec8fb & load_bits(uint8_t a) {
        y0 = Vec4fb().load_bits(a);
        y1 = Vec4fb().load_bits(uint8_t(a>>4u));
        return *this;
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


/*****************************************************************************
*
*          Operators for Vec8fb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec8fb operator & (Vec8fb const a, Vec8fb const b) {
    return Vec8fb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec8fb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec8fb operator || (Vec8fb const a, Vec8fb const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec8fb & operator |= (Vec8fb & a, Vec8fb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8fb operator ^ (Vec8fb const a, Vec8fb const b) {
    return Vec8fb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8fb & operator ^= (Vec8fb & a, Vec8fb const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8fb operator ~ (Vec8fb const a) {
    return Vec8fb(~a.get_low(), ~a.get_high());
}

// vector operator == : xnor
static inline Vec8fb operator == (Vec8fb const a, Vec8fb const b) {
    return Vec8fb(Vec8fb(a) ^ Vec8fb(~b));
}

// vector operator != : xor
static inline Vec8fb operator != (Vec8fb const a, Vec8fb const b) {
    return Vec8fb(a ^ b);
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec8fb operator ! (Vec8fb const a) {
    return Vec8fb(!a.get_low(), !a.get_high());
}

// Functions for Vec8fb

// andnot: a & ~ b
static inline Vec8fb andnot(Vec8fb const a, Vec8fb const b) {
    return Vec8fb(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec8fb const a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec8fb const a) {
    return horizontal_or(a.get_low() | a.get_high());
}


/*****************************************************************************
*
*          Vec4db: Vector of 4 Booleans for use with Vec4d
*
*****************************************************************************/

class Vec4db : public Vec256de {
public:
    // Default constructor:
    Vec4db() {
    }
    // Constructor to build from all elements:
    Vec4db(bool b0, bool b1, bool b2, bool b3) {
        y0 = Vec2db(b0, b1);
        y1 = Vec2db(b2, b3);
    }
    // Constructor to build from two Vec2db:
    Vec4db(Vec2db const a0, Vec2db const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256de
    Vec4db(Vec256de const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Constructor to broadcast scalar value:
    Vec4db(bool b) {
        y0 = y1 = Vec2db(b);
    }
    // Assignment operator to convert from type Vec256de
    Vec4db & operator = (Vec256de const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }

    // Constructor to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db(Vec4qb const x) {
        y0 = _mm_castsi128_pd(Vec4q(x).get_low());
        y1 = _mm_castsi128_pd(Vec4q(x).get_high());
    }
    // Assignment operator to convert from type Vec4qb used as Boolean for integer vectors
    Vec4db & operator = (Vec4qb const x) {
        y0 = _mm_castsi128_pd(Vec4q(x).get_low());
        y1 = _mm_castsi128_pd(Vec4q(x).get_high());
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec4db & operator = (bool b) {
        y0 = y1 = Vec2db(b);
        return *this;
    }
    // Type cast operator to convert to type Vec4qb used as Boolean for integer vectors
    operator Vec4qb() const {
        return Vec4q(_mm_castpd_si128(y0), _mm_castpd_si128(y1));
    }

    // Member function to change a single element in vector
    Vec4db const insert(int index, bool value) {
        if ((uint32_t)index < 2) {
            y0 = Vec2db(y0).insert(index, value);
        }
        else {
            y1 = Vec2db(y1).insert(index - 2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        if ((uint32_t)index < 2) {
            return Vec2db(y0).extract(index);
        }
        else {
            return Vec2db(y1).extract(index - 2);
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4fb:
    Vec2db get_low() const {
        return y0;
    }
    Vec2db get_high() const {
        return y1;
    }
    // Member function to change a bitfield to a boolean vector
    Vec4db & load_bits(uint8_t a) {
        y0 = Vec2db().load_bits(a);
        y1 = Vec2db().load_bits(uint8_t(a>>2u));
        return *this;
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


/*****************************************************************************
*
*          Operators for Vec4db
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec4db operator & (Vec4db const a, Vec4db const b) {
    return Vec4db(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec4db(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec4db operator || (Vec4db const a, Vec4db const b) {
    return a | b;
}

// vector operator |= : bitwise or
static inline Vec4db & operator |= (Vec4db & a, Vec4db const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4db operator ^ (Vec4db const a, Vec4db const b) {
    return Vec4db(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec4db & operator ^= (Vec4db & a, Vec4db const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4db operator ~ (Vec4db const a) {
    return Vec4db(~a.get_low(), ~a.get_high());
}

// vector operator == : xnor
static inline Vec4db operator == (Vec4db const a, Vec4db const b) {
    return Vec4db(Vec4db(a) ^ Vec4db(~b));
}

// vector operator != : xor
static inline Vec4db operator != (Vec4db const a, Vec4db const b) {
    return Vec4db(a ^ b);
}

// vector operator ! : logical not
// (operator ! is less efficient than operator ~. Use only where not
// all bits in an element are the same)
static inline Vec4db operator ! (Vec4db const a) {
    return Vec4db(!a.get_low(), !a.get_high());
}

// Functions for Vec4db

// andnot: a & ~ b
static inline Vec4db andnot(Vec4db const a, Vec4db const b) {
    return Vec4db(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec4db const a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec4db const a) {
    return horizontal_or(a.get_low() | a.get_high());
}


/*****************************************************************************
*
*          Vec8f: Vector of 8 single precision floating point values
*
*****************************************************************************/

class Vec8f : public Vec256fe {
public:
    // Default constructor:
    Vec8f() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8f(float f) {
        y1 = y0 = _mm_set1_ps(f);
    }
    // Constructor to build from all elements:
    Vec8f(float f0, float f1, float f2, float f3, float f4, float f5, float f6, float f7) {
        y0 = _mm_setr_ps(f0, f1, f2, f3);
        y1 = _mm_setr_ps(f4, f5, f6, f7); 
    }
    // Constructor to build from two Vec4f:
    Vec8f(Vec4f const a0, Vec4f const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256fe
    Vec8f(Vec256fe const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256fe
    Vec8f & operator = (Vec256fe const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8f & load(float const * p) {
        y0 = _mm_loadu_ps(p);
        y1 = _mm_loadu_ps(p+4);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address divisible by 32.
    Vec8f & load_a(float const * p) {
        y0 = _mm_load_ps(p);
        y1 = _mm_load_ps(p+4);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(float * p) const {
        _mm_storeu_ps(p,   y0);
        _mm_storeu_ps(p+4, y1);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(float * p) const {
        _mm_stream_ps(p,   y0);
        _mm_stream_ps(p+4, y1);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address divisible by 32.
    void store_a(float * p) const {
        _mm_store_ps(p,   y0);
        _mm_store_ps(p+4, y1);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8f & load_partial(int n, float const * p) {
        if (n > 0 && n <= 4) {
            *this = Vec8f(Vec4f().load_partial(n, p),_mm_setzero_ps());
        }
        else if (n > 4 && n <= 8) {
            *this = Vec8f(Vec4f().load(p), Vec4f().load_partial(n - 4, p + 4));
        }
        else {
            y1 = y0 = _mm_setzero_ps();
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, float * p) const {
        if (n <= 4) {
            get_low().store_partial(n, p);
        }
        else if (n <= 8) {
            get_low().store(p);
            get_high().store_partial(n - 4, p + 4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8f & cutoff(int n) {
        if (uint32_t(n) >= 8) return *this;
        else if (n >= 4) {
            y1 = Vec4f(y1).cutoff(n - 4);
        }
        else {
            y0 = Vec4f(y0).cutoff(n);
            y1 = Vec4f(0.0f);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec8f const insert(int index, float value) {
        if ((uint32_t)index < 4) {
            y0 = Vec4f(y0).insert(index, value);
        }
        else {
            y1 = Vec4f(y1).insert(index - 4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    float extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4f(y0).extract(index);
        }
        else {
            return Vec4f(y1).extract(index - 4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    float operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4f:
    Vec4f get_low() const {
        return y0;
    }
    Vec4f get_high() const {
        return y1;
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 16;
    } 
};


/*****************************************************************************
*
*          Operators for Vec8f
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8f operator + (Vec8f const a, Vec8f const b) {
    return Vec8f(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8f(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec8f(-a.get_low(), -a.get_high());
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
    return Vec8f(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec8f(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec8fb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8fb operator != (Vec8f const a, Vec8f const b) {
    return Vec8fb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8fb operator < (Vec8f const a, Vec8f const b) {
    return Vec8fb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec8fb operator <= (Vec8f const a, Vec8f const b) {
    return Vec8fb(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec8fb operator > (Vec8f const a, Vec8f const b) {
    return Vec8fb(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec8fb operator >= (Vec8f const a, Vec8f const b) {
    return Vec8fb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec8f operator & (Vec8f const a, Vec8f const b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8f & operator &= (Vec8f & a, Vec8f const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec8f and Vec8fb
static inline Vec8f operator & (Vec8f const a, Vec8fb const b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8f operator & (Vec8fb const a, Vec8f const b) {
    return Vec8f(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator | : bitwise or
static inline Vec8f operator | (Vec8f const a, Vec8f const b) {
    return Vec8f(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8f & operator |= (Vec8f & a, Vec8f const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8f operator ^ (Vec8f const a, Vec8f const b) {
    return Vec8f(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec8f & operator ^= (Vec8f & a, Vec8f const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec8fb operator ! (Vec8f const a) {
    return Vec8fb(!a.get_low(), !a.get_high());
}


/*****************************************************************************
*
*          Functions for Vec8f
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFF (true). No other values are allowed.
static inline Vec8f select (Vec8fb const s, Vec8f const a, Vec8f const b) {
    return Vec8f(select(s.get_low(),a.get_low(),b.get_low()), select(s.get_high(),a.get_high(),b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8f if_add (Vec8fb const f, Vec8f const a, Vec8f const b) {
    return a + (Vec8f(f) & b);
}

// Conditional subtract
static inline Vec8f if_sub (Vec8fb const f, Vec8f const a, Vec8f const b) {
    return a - (Vec8f(f) & b);
}

// Conditional multiply
static inline Vec8f if_mul (Vec8fb const f, Vec8f const a, Vec8f const b) {
    return a * select(f, b, 1.f);
}

// Conditional divide
static inline Vec8f if_div (Vec8fb const f, Vec8f const a, Vec8f const b) {
    return a / select(f, b, 1.f);
}

// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline float horizontal_add (Vec8f const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec8f max(Vec8f const a, Vec8f const b) {
    return Vec8f(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8f min(Vec8f const a, Vec8f const b) {
    return Vec8f(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec8f abs(Vec8f const a) {
    return Vec8f(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec8f sqrt(Vec8f const a) {
    return Vec8f(sqrt(a.get_low()), sqrt(a.get_high()));
}

// function square: a * a
static inline Vec8f square(Vec8f const a) {
    return Vec8f(square(a.get_low()), square(a.get_high()));
}

// pow(Vec8f, int):
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
// implement as function pow(vector, const_int)
template <int n>
static inline Vec8f pow(Vec8f const a, Const_int_t<n>) {
    return pow_n<Vec8f, n>(a);
}


// function round: round to nearest integer (even). (result as float vector)
static inline Vec8f round(Vec8f const a) {
    return Vec8f(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as float vector)
static inline Vec8f truncate(Vec8f const a) {
    return Vec8f(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as float vector)
static inline Vec8f floor(Vec8f const a) {
    return Vec8f(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as float vector)
static inline Vec8f ceil(Vec8f const a) {
    return Vec8f(ceil(a.get_low()), ceil(a.get_high()));
}

// function roundi: round to nearest integer (even). (result as integer vector)
static inline Vec8i roundi(Vec8f const a) {
    return Vec8i(roundi(a.get_low()), roundi(a.get_high()));
}

// function truncatei: round towards zero. (result as integer vector)
static inline Vec8i truncatei(Vec8f const a) {
    return Vec8i(truncatei(a.get_low()), truncatei(a.get_high()));
}

// function to_float: convert integer vector to float vector
static inline Vec8f to_float(Vec8i const a) {
    return Vec8f(to_float(a.get_low()), to_float(a.get_high()));
}

// function to_float: convert unsigned integer vector to float vector
static inline Vec8f to_float(Vec8ui const a) {
    return Vec8f(to_float(a.get_low()), to_float(a.get_high()));
}


// Approximate math functions

// approximate reciprocal (Faster than 1.f / a. relative accuracy better than 2^-11)
static inline Vec8f approx_recipr(Vec8f const a) {
    return Vec8f(approx_recipr(a.get_low()), approx_recipr(a.get_high()));
}

// approximate reciprocal squareroot (Faster than 1.f / sqrt(a). Relative accuracy better than 2^-11)
static inline Vec8f approx_rsqrt(Vec8f const a) {
    return Vec8f(approx_rsqrt(a.get_low()), approx_rsqrt(a.get_high()));
}

// Fused multiply and add functions

// Multiply and add
static inline Vec8f mul_add(Vec8f const a, Vec8f const b, Vec8f const c) {
    return Vec8f(mul_add(a.get_low(),b.get_low(),c.get_low()), mul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract
static inline Vec8f mul_sub(Vec8f const a, Vec8f const b, Vec8f const c) {
    return Vec8f(mul_sub(a.get_low(),b.get_low(),c.get_low()), mul_sub(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and inverse subtract
static inline Vec8f nmul_add(Vec8f const a, Vec8f const b, Vec8f const c) {
    return Vec8f(nmul_add(a.get_low(),b.get_low(),c.get_low()), nmul_add(a.get_high(),b.get_high(),c.get_high()));
}


// Multiply and subtract with extra precision on the intermediate calculations, used internally
static inline Vec8f mul_sub_x(Vec8f const a, Vec8f const b, Vec8f const c) {
    return Vec8f(mul_sub_x(a.get_low(),b.get_low(),c.get_low()), mul_sub_x(a.get_high(),b.get_high(),c.get_high()));
}

// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0f) = 0, exponent(0.0f) = -127, exponent(INF) = +128, exponent(NAN) = +128
static inline Vec8i exponent(Vec8f const a) {
    return Vec8i(exponent(a.get_low()), exponent(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =    0 gives 1.0f
// n >=  128 gives +INF
// n <= -127 gives 0.0f
// This function will never produce denormals, and never raise exceptions
static inline Vec8f exp2(Vec8i const a) {
    return Vec8f(exp2(a.get_low()), exp2(a.get_high()));
}
//static Vec8f exp2(Vec8f const x); // defined in vectormath_exp.h

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0f) = 1.0f, fraction(5.0f) = 1.25f 
static inline Vec8f fraction(Vec8f const a) {
    return Vec8f(fraction(a.get_low()), fraction(a.get_high()));
}


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0f, -INF and -NAN
// Note that sign_bit(Vec8f(-0.0f)) gives true, while Vec8f(-0.0f) < Vec8f(0.0f) gives false
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb sign_bit(Vec8f const a) {
    return Vec8fb(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec8f sign_combine(Vec8f const a, Vec8f const b) {
    return Vec8f(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_finite(Vec8f const a) {
    return Vec8fb(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_inf(Vec8f const a) {
    return Vec8fb(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
// (the underscore in the name avoids a conflict with a macro in Intel's mathimf.h)
static inline Vec8fb is_nan(Vec8f const a) {
    return Vec8fb(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec8fb is_subnormal(Vec8f const a) {
    return Vec8fb(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec8fb is_zero_or_subnormal(Vec8f const a) {
    return Vec8fb(is_zero_or_subnormal(a.get_low()), is_zero_or_subnormal(a.get_high()));
}

// Function infinite4f: returns a vector where all elements are +INF
static inline Vec8f infinite8f() {
    return Vec8f(infinite4f(),infinite4f());
}

// Function nan4f: returns a vector where all elements are +NAN (quiet)
static inline Vec8f nan8f(int n = 0x10) {
    return Vec8f(nan4f(n), nan4f(n));
}

// change signs on vectors Vec8f
// Each index i0 - i7 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
inline Vec8f change_sign(Vec8f const a) {
    if ((i0 | i1 | i2 | i3 | i4 | i5 | i6 | i7) == 0) return a;
    Vec4f lo = change_sign<i0,i1,i2,i3>(a.get_low());
    Vec4f hi = change_sign<i4,i5,i6,i7>(a.get_high());
    return Vec8f(lo, hi);
}


/*****************************************************************************
*
*          Vec2d: Vector of 2 double precision floating point values
*
*****************************************************************************/

class Vec4d : public Vec256de {
public:
    // Default constructor:
    Vec4d() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4d(double d) {
        y1 = y0 = _mm_set1_pd(d);
    }
    // Constructor to build from all elements:
    Vec4d(double d0, double d1, double d2, double d3) {
        y0 = _mm_setr_pd(d0, d1); 
        y1 = _mm_setr_pd(d2, d3); 
    }
    // Constructor to build from two Vec4f:
    Vec4d(Vec2d const a0, Vec2d const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256de
    Vec4d(Vec256de const x) {
        y0 = x.get_low();
        y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256de
    Vec4d & operator = (Vec256de const x) {
        y0 = x.get_low();
        y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4d & load(double const * p) {
        y0 = _mm_loadu_pd(p);
        y1 = _mm_loadu_pd(p+2);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32
    Vec4d & load_a(double const * p) {
        y0 = _mm_load_pd(p);
        y1 = _mm_load_pd(p+2);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(double * p) const {
        _mm_storeu_pd(p,   y0);
        _mm_storeu_pd(p+2, y1);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(double * p) const {
        _mm_stream_pd(p,   y0);
        _mm_stream_pd(p+2, y1);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32
    void store_a(double * p) const {
        _mm_store_pd(p,   y0);
        _mm_store_pd(p+2, y1);
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4d & load_partial(int n, double const * p) {
        if (n > 0 && n <= 2) {
            *this = Vec4d(Vec2d().load_partial(n, p), _mm_setzero_pd());
        }
        else if (n > 2 && n <= 4) {
            *this = Vec4d(Vec2d().load(p), Vec2d().load_partial(n - 2, p + 2));
        }
        else {
            y1 = y0 = _mm_setzero_pd();
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, double * p) const {
        if (n <= 2) {
            get_low().store_partial(n, p);
        }
        else if (n <= 4) {
            get_low().store(p);
            get_high().store_partial(n - 2, p + 2);
        }
    }
    Vec4d & cutoff(int n) {
        if (uint32_t(n) >= 4) return *this;
        else if (n >= 2) {
            y1 = Vec2d(y1).cutoff(n - 2);
        }
        else {
            y0 = Vec2d(y0).cutoff(n);
            y1 = Vec2d(0.0);
        }
        return *this;
    }    
    // Member function to change a single element in vector
    Vec4d const insert(int index, double value) {
        if ((uint32_t)index < 2) {
            y0 = Vec2d(y0).insert(index, value);
        }
        else {
            y1 = Vec2d(y1).insert(index-2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    double extract(int index) const {
        if ((uint32_t)index < 2) {
            return Vec2d(y0).extract(index);
        }
        else {
            return Vec2d(y1).extract(index-2);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    double operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2d:
    Vec2d get_low() const {
        return y0;
    }
    Vec2d get_high() const {
        return y1;
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 17;
    } 
};


/*****************************************************************************
*
*          Operators for Vec4d
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4d operator + (Vec4d const a, Vec4d const b) {
    return Vec4d(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec4d(a.get_low() - b.get_low(), a.get_high() - b.get_high());
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
    return Vec4d(-a.get_low(), -a.get_high());
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
    return Vec4d(a.get_low() * b.get_low(), a.get_high() * b.get_high());
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
    return Vec4d(a.get_low() / b.get_low(), a.get_high() / b.get_high());
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
    return Vec4db(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec4db operator != (Vec4d const a, Vec4d const b) {
    return Vec4db(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec4db operator < (Vec4d const a, Vec4d const b) {
    return Vec4db(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator <= : returns true for elements for which a <= b
static inline Vec4db operator <= (Vec4d const a, Vec4d const b) {
    return Vec4db(a.get_low() <= b.get_low(), a.get_high() <= b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec4db operator > (Vec4d const a, Vec4d const b) {
    return Vec4db(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator >= : returns true for elements for which a >= b
static inline Vec4db operator >= (Vec4d const a, Vec4d const b) {
    return Vec4db(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// Bitwise logical operators

// vector operator & : bitwise and
static inline Vec4d operator & (Vec4d const a, Vec4d const b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec4d & operator &= (Vec4d & a, Vec4d const b) {
    a = a & b;
    return a;
}

// vector operator & : bitwise and of Vec4d and Vec4db
static inline Vec4d operator & (Vec4d const a, Vec4db const b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec4d operator & (Vec4db const a, Vec4d const b) {
    return Vec4d(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator | : bitwise or
static inline Vec4d operator | (Vec4d const a, Vec4d const b) {
    return Vec4d(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec4d & operator |= (Vec4d & a, Vec4d const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4d operator ^ (Vec4d const a, Vec4d const b) {
    return Vec4d(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec4d & operator ^= (Vec4d & a, Vec4d const b) {
    a = a ^ b;
    return a;
}

// vector operator ! : logical not. Returns Boolean vector
static inline Vec4db operator ! (Vec4d const a) {
    return Vec4db(!a.get_low(), !a.get_high());
}


/*****************************************************************************
*
*          Functions for Vec4d
*
*****************************************************************************/

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFFFFFFFFFFFFFFFF (true). 
// No other values are allowed.
static inline Vec4d select (Vec4db const s, Vec4d const a, Vec4d const b) {
    return Vec4d(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4d if_add (Vec4db const f, Vec4d const a, Vec4d const b) {
    return a + (Vec4d(f) & b);
}

// Conditional subtract
static inline Vec4d if_sub (Vec4db const f, Vec4d const a, Vec4d const b) {
    return a - (Vec4d(f) & b);
}

// Conditional multiply
static inline Vec4d if_mul (Vec4db const f, Vec4d const a, Vec4d const b) {
    return a * select(f, b, 1.f);
}

// Conditional divide
static inline Vec4d if_div (Vec4db const f, Vec4d const a, Vec4d const b) {
    return a / select(f, b, 1.);
}


// General arithmetic functions, etc.

// Horizontal add: Calculates the sum of all vector elements.
static inline double horizontal_add (Vec4d const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec4d max(Vec4d const a, Vec4d const b) {
    return Vec4d(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4d min(Vec4d const a, Vec4d const b) {
    return Vec4d(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}
// NAN-safe versions of maximum and minimum are in vector_convert.h

// function abs: absolute value
// Removes sign bit, even for -0.0f, -INF and -NAN
static inline Vec4d abs(Vec4d const a) {
    return Vec4d(abs(a.get_low()), abs(a.get_high()));
}

// function sqrt: square root
static inline Vec4d sqrt(Vec4d const a) {
    return Vec4d(sqrt(a.get_low()), sqrt(a.get_high()));
}

// function square: a * a
static inline Vec4d square(Vec4d const a) {
    return Vec4d(square(a.get_low()), square(a.get_high()));
}

// pow(Vec4d, int):
// Raise floating point numbers to integer power n
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
// implement as function pow(vector, const_int)
template <int n>
static inline Vec4d pow(Vec4d const a, Const_int_t<n>) {
    return pow_n<Vec4d, n>(a);
}


// function round: round to nearest integer (even). (result as double vector)
static inline Vec4d round(Vec4d const a) {
    return Vec4d(round(a.get_low()), round(a.get_high()));
}

// function truncate: round towards zero. (result as double vector)
static inline Vec4d truncate(Vec4d const a) {
    return Vec4d(truncate(a.get_low()), truncate(a.get_high()));
}

// function floor: round towards minus infinity. (result as double vector)
static inline Vec4d floor(Vec4d const a) {
    return Vec4d(floor(a.get_low()), floor(a.get_high()));
}

// function ceil: round towards plus infinity. (result as double vector)
static inline Vec4d ceil(Vec4d const a) {
    return Vec4d(ceil(a.get_low()), ceil(a.get_high()));
}

// function round_to_int32: round to nearest integer (even). (result as integer vector)
static inline Vec4i round_to_int32(Vec4d const a) {
    return round_to_int32(a.get_low(), a.get_high());
}

// function truncate_to_int32: round towards zero. (result as integer vector)
static inline Vec4i truncate_to_int32(Vec4d const a) {
    return truncate_to_int32(a.get_low(), a.get_high());
}

// function truncatei: round towards zero. (inefficient)
static inline Vec4q truncatei(Vec4d const a) {
    double aa[4];
    a.store(aa);
    return Vec4q(int64_t(aa[0]), int64_t(aa[1]), int64_t(aa[2]), int64_t(aa[3]));
}

// function roundi: round to nearest or even. (inefficient)
static inline Vec4q roundi(Vec4d const a) {
    return truncatei(round(a));
}

// function to_double: convert integer vector elements to double vector (inefficient)
static inline Vec4d to_double(Vec4q const a) {
    int64_t aa[4];
    a.store(aa);
    return Vec4d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]));
}

// function to_double: convert unsigned integer vector elements to double vector (inefficient)
static inline Vec4d to_double(Vec4uq const a) {
    uint64_t aa[4];
    a.store(aa);
    return Vec4d(double(aa[0]), double(aa[1]), double(aa[2]), double(aa[3]));
}

// function to_double: convert integer vector to double vector
static inline Vec4d to_double(Vec4i const a) {
    return Vec4d(to_double_low(a), to_double_high(a));
}

// function compress: convert two Vec4d to one Vec8f
static inline Vec8f compress (Vec4d const low, Vec4d const high) {
    return Vec8f(compress(low.get_low(), low.get_high()), compress(high.get_low(), high.get_high()));
}

// Function extend_low : convert Vec8f vector elements 0 - 3 to Vec4d
static inline Vec4d extend_low (Vec8f const a) {
    return Vec4d(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : convert Vec8f vector elements 4 - 7 to Vec4d
static inline Vec4d extend_high (Vec8f const a) {
    return Vec4d(extend_low(a.get_high()), extend_high(a.get_high()));
}


// Fused multiply and add functions

// Multiply and add
static inline Vec4d mul_add(Vec4d const a, Vec4d const b, Vec4d const c) {
    return Vec4d(mul_add(a.get_low(),b.get_low(),c.get_low()), mul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract
static inline Vec4d mul_sub(Vec4d const a, Vec4d const b, Vec4d const c) {
    return Vec4d(mul_sub(a.get_low(),b.get_low(),c.get_low()), mul_sub(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and inverse subtract
static inline Vec4d nmul_add(Vec4d const a, Vec4d const b, Vec4d const c) {
    return Vec4d(nmul_add(a.get_low(),b.get_low(),c.get_low()), nmul_add(a.get_high(),b.get_high(),c.get_high()));
}

// Multiply and subtract with extra precision on the intermediate calculations, 
// even if FMA instructions not supported, using Veltkamp-Dekker split
static inline Vec4d mul_sub_x(Vec4d const a, Vec4d const b, Vec4d const c) {
    return Vec4d(mul_sub_x(a.get_low(),b.get_low(),c.get_low()), mul_sub_x(a.get_high(),b.get_high(),c.get_high()));
}

// Math functions using fast bit manipulation

// Extract the exponent as an integer
// exponent(a) = floor(log2(abs(a)));
// exponent(1.0) = 0, exponent(0.0) = -1023, exponent(INF) = +1024, exponent(NAN) = +1024
static inline Vec4q exponent(Vec4d const a) {
    return Vec4q(exponent(a.get_low()), exponent(a.get_high()));
}

// Extract the fraction part of a floating point number
// a = 2^exponent(a) * fraction(a), except for a = 0
// fraction(1.0) = 1.0, fraction(5.0) = 1.25 
static inline Vec4d fraction(Vec4d const a) {
    return Vec4d(fraction(a.get_low()), fraction(a.get_high()));
}

// Fast calculation of pow(2,n) with n integer
// n  =     0 gives 1.0
// n >=  1024 gives +INF
// n <= -1023 gives 0.0
// This function will never produce denormals, and never raise exceptions
static inline Vec4d exp2(Vec4q const a) {
    return Vec4d(exp2(a.get_low()), exp2(a.get_high()));
}
//static Vec4d exp2(Vec4d const x); // defined in vectormath_exp.h


// Categorization functions

// Function sign_bit: gives true for elements that have the sign bit set
// even for -0.0, -INF and -NAN
// Note that sign_bit(Vec4d(-0.0)) gives true, while Vec4d(-0.0) < Vec4d(0.0) gives false
static inline Vec4db sign_bit(Vec4d const a) {
    return Vec4db(sign_bit(a.get_low()), sign_bit(a.get_high()));
}

// Function sign_combine: changes the sign of a when b has the sign bit set
// same as select(sign_bit(b), -a, a)
static inline Vec4d sign_combine(Vec4d const a, Vec4d const b) {
    return Vec4d(sign_combine(a.get_low(), b.get_low()), sign_combine(a.get_high(), b.get_high()));
}

// Function is_finite: gives true for elements that are normal, denormal or zero, 
// false for INF and NAN
static inline Vec4db is_finite(Vec4d const a) {
    return Vec4db(is_finite(a.get_low()), is_finite(a.get_high()));
}

// Function is_inf: gives true for elements that are +INF or -INF
// false for finite numbers and NAN
static inline Vec4db is_inf(Vec4d const a) {
    return Vec4db(is_inf(a.get_low()), is_inf(a.get_high()));
}

// Function is_nan: gives true for elements that are +NAN or -NAN
// false for finite numbers and +/-INF
static inline Vec4db is_nan(Vec4d const a) {
    return Vec4db(is_nan(a.get_low()), is_nan(a.get_high()));
}

// Function is_subnormal: gives true for elements that are denormal (subnormal)
// false for finite numbers, zero, NAN and INF
static inline Vec4db is_subnormal(Vec4d const a) {
    return Vec4db(is_subnormal(a.get_low()), is_subnormal(a.get_high()));
}

// Function is_zero_or_subnormal: gives true for elements that are zero or subnormal (denormal)
// false for finite numbers, NAN and INF
static inline Vec4db is_zero_or_subnormal(Vec4d const a) {
    return Vec4db(is_zero_or_subnormal(a.get_low()),is_zero_or_subnormal(a.get_high()));
}

// Function infinite2d: returns a vector where all elements are +INF
static inline Vec4d infinite4d() {
    return Vec4d(infinite2d(), infinite2d());
}

// Function nan2d: returns a vector where all elements are +NAN (quiet)
static inline Vec4d nan4d(int n = 0x10) {
    return Vec4d(nan2d(n), nan2d(n));
}

// change signs on vectors Vec4d
// Each index i0 - i3 is 1 for changing sign on the corresponding element, 0 for no change
template <int i0, int i1, int i2, int i3>
inline Vec4d change_sign(Vec4d const a) {
    if ((i0 | i1 | i2 | i3) == 0) return a;
    Vec2d lo = change_sign<i0,i1>(a.get_low());
    Vec2d hi = change_sign<i2,i3>(a.get_high());
    return Vec4d(lo, hi);
}


/*****************************************************************************
*
*          Functions for reinterpretation between vector types
*
*****************************************************************************/

static inline Vec256b reinterpret_i (Vec256b const x) {
    return x;
}

static inline Vec256b reinterpret_i (Vec256fe  const x) {
    return Vec256b(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec256b reinterpret_i (Vec256de const x) {
    return Vec256b(reinterpret_i(x.get_low()), reinterpret_i(x.get_high()));
}

static inline Vec256fe  reinterpret_f (Vec256b const x) {
    return Vec256fe(reinterpret_f(x.get_low()), reinterpret_f(x.get_high()));
}

static inline Vec256fe  reinterpret_f (Vec256fe  const x) {
    return x;
}

static inline Vec256fe  reinterpret_f (Vec256de const x) {
    return Vec256fe(reinterpret_f(x.get_low()), reinterpret_f(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256b const x) {
    return Vec256de(reinterpret_d(x.get_low()), reinterpret_d(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256fe  const x) {
    return Vec256de(reinterpret_d(x.get_low()), reinterpret_d(x.get_high()));
}

static inline Vec256de reinterpret_d (Vec256de const x) {
    return x;
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
    return Vec4d(blend2<i0,i1> (a.get_low(), a.get_high()), 
           blend2<i2,i3> (a.get_low(), a.get_high()));
}

// permute vector Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f permute8(Vec8f const a) {
    return Vec8f(blend4<i0,i1,i2,i3> (a.get_low(), a.get_high()), 
        blend4<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}


// blend vectors Vec4d
template <int i0, int i1, int i2, int i3>
static inline Vec4d blend4(Vec4d const a, Vec4d const b) {
    Vec2d x0 = blend_half<Vec4d, i0, i1>(a, b);
    Vec2d x1 = blend_half<Vec4d, i2, i3>(a, b);
    return Vec4d(x0, x1);
}

// blend vectors Vec8f
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8f blend8(Vec8f const a, Vec8f const b) {
    Vec4f x0 = blend_half<Vec8f, i0, i1, i2, i3>(a, b);
    Vec4f x1 = blend_half<Vec8f, i4, i5, i6, i7>(a, b);
    return Vec8f(x0, x1);
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
    Vec4f  r0 = lookup8(index.get_low() , table.get_low(), table.get_high());
    Vec4f  r1 = lookup8(index.get_high(), table.get_low(), table.get_high());
    return Vec8f(r0, r1);
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
    if (n <= 8) {
        return lookup8(index, Vec8f().load(table));
    }
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
    return Vec8f(table[index1[0]],table[index1[1]],table[index1[2]],table[index1[3]],
    table[index1[4]],table[index1[5]],table[index1[6]],table[index1[7]]);
}

static inline Vec4d lookup4(Vec4q const index, Vec4d const table) {
    Vec2d  r0 = lookup4(index.get_low() , table.get_low(), table.get_high());
    Vec2d  r1 = lookup4(index.get_high(), table.get_low(), table.get_high());
    return Vec4d(r0, r1);
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
    // Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & Vec8ui(n-1, 0, n-1, 0, n-1, 0, n-1, 0);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), Vec8ui(n-1, 0, n-1, 0, n-1, 0, n-1, 0));
    }
    Vec4q index2 = Vec4q(index1);
    return Vec4d(table[index2[0]],table[index2[1]],table[index2[2]],table[index2[3]]);
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
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
}

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4d const data, double * array) {
    const int index[4] = {i0,i1,i2,i3};
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) array[index[i]] = data[i];
    }
}

// scatter functions with variable indexes

static inline void scatter(Vec8i const index, uint32_t limit, Vec8f const data, float * destination) {
    for (int i = 0; i < 8; i++) {
        if (uint32_t(index[i]) < limit) destination[index[i]] = data[i];
    }
}

static inline void scatter(Vec4q const index, uint32_t limit, Vec4d const data, double * destination) {
    for (int i = 0; i < 4; i++) {
        if (uint64_t(index[i]) < uint64_t(limit)) destination[index[i]] = data[i];
    }
} 

static inline void scatter(Vec4i const index, uint32_t limit, Vec4d const data, double * destination) {
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) destination[index[i]] = data[i];
    }
} 


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8fb const x) {
    return to_bits(Vec8ib(reinterpret_i(x)));
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4db const x) {
    return to_bits(Vec4qb(reinterpret_i(x)));
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORF256E_H
