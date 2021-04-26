/****************************  vectori128.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 128-bit integer vector classes
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec128b   Vector of 128  bits. Used internally as base class
* Vec16c    Vector of  16  8-bit signed    integers
* Vec16uc   Vector of  16  8-bit unsigned  integers
* Vec16cb   Vector of  16  Booleans for use with Vec16c and Vec16uc
* Vec8s     Vector of   8  16-bit signed   integers
* Vec8us    Vector of   8  16-bit unsigned integers
* Vec8sb    Vector of   8  Booleans for use with Vec8s and Vec8us
* Vec4i     Vector of   4  32-bit signed   integers
* Vec4ui    Vector of   4  32-bit unsigned integers
* Vec4ib    Vector of   4  Booleans for use with Vec4i and Vec4ui
* Vec2q     Vector of   2  64-bit signed   integers
* Vec2uq    Vector of   2  64-bit unsigned integers
* Vec2qb    Vector of   2  Booleans for use with Vec2q and Vec2uq
*
* Each vector object is represented internally in the CPU as a 128-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORI128_H
#define VECTORI128_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

#ifdef VCL_NAMESPACE         // optional namespace
namespace VCL_NAMESPACE {
#endif


// Generate a constant vector of 4 integers stored in memory.
template <uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3>
static inline constexpr __m128i constant4ui() {
    /*
    const union {
        uint32_t i[4];
        __m128i  xmm;
    } u = { {i0,i1,i2,i3} };
    return u.xmm;
    */
    return _mm_setr_epi32(i0, i1, i2, i3);
}


/*****************************************************************************
*
*          Compact boolean vectors
*
*****************************************************************************/
#if INSTRSET >= 9
class Vec8b;  // allow forward reference to Vec8b

#if INSTRSET == 9 && MAX_VECTOR_SIZE >= 512  // special case of mixed compact and broad vectors
class Vec8ib;
class Vec8fb;
class Vec4qb;
class Vec4db;
#endif

// Compact vector of 16 booleans
class Vec16b {
protected:
    __mmask16  mm; // Boolean mask register
public:
    // Default constructor:
    Vec16b() {
    }
    // Constructor to convert from type __mmask16 used in intrinsics
    Vec16b(__mmask16 x) {
        mm = x;
    }
    // Constructor to build from all elements:
    Vec16b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7,
        bool b8, bool b9, bool b10, bool b11, bool b12, bool b13, bool b14, bool b15) {
        mm = uint16_t(
            (uint16_t)b0 | (uint16_t)b1 << 1 | (uint16_t)b2 << 2 | (uint16_t)b3 << 3 |
            (uint16_t)b4 << 4 | (uint16_t)b5 << 5 | (uint16_t)b6 << 6 | (uint16_t)b7 << 7 |
            (uint16_t)b8 << 8 | (uint16_t)b9 << 9 | (uint16_t)b10 << 10 | (uint16_t)b11 << 11 |
            (uint16_t)b12 << 12 | (uint16_t)b13 << 13 | (uint16_t)b14 << 14 | (uint16_t)b15 << 15);
    }
    // Constructor to broadcast single value:
    Vec16b(bool b) {
        mm = __mmask16(-int16_t(b));
    }
    // Constructor to make from two halves. Implemented below after declaration of Vec8b
    inline Vec16b(Vec8b const x0, Vec8b const x1);
#if INSTRSET == 9 && MAX_VECTOR_SIZE >= 512  // special case of mixed compact and broad vectors
    inline Vec16b(Vec8ib const x0, Vec8ib const x1);  // in vectorf512.h
    inline Vec16b(Vec8fb const x0, Vec8fb const x1);  // in vectorf512.h
#endif

    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec16b & operator = (__mmask16 x) {
        mm = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec16b & operator = (bool b) {
        mm = Vec16b(b);
        return *this;
    }
    // Type cast operator to convert to __mmask16 used in intrinsics
    operator __mmask16() const {
        return mm;
    }
    // split into two halves
#if INSTRSET >= 10
    Vec8b get_low() const;
    Vec8b get_high() const;
#elif INSTRSET == 9 && MAX_VECTOR_SIZE >= 512  // special case of mixed compact and broad vectors
    Vec8ib get_low()  const;    // in vectorf512.h
    Vec8ib get_high() const;    // in vectorf512.h
#endif
    // Member function to change a single element in vector
    Vec16b const insert(int index, bool value) {
        mm = __mmask16(((uint16_t)mm & ~(1 << index)) | (int)value << index);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return ((uint32_t)mm >> index) & 1;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec16b & load_bits(uint16_t a) {
        mm = __mmask16(a);
        return *this;
    }
    // Number of elements
    static constexpr int size() {
        return 16;
    }
    // Type of elements
    static constexpr int elementtype() {
        return 2;
    }
    // I would like to prevent implicit conversion from int, but this is 
    // not possible because __mmask16 and int16_t are treated as the same type:
    // Vec16b(int b) = delete;
    // Vec16b & operator = (int x) = delete;
};

#if INSTRSET >= 10
class Vec2b;
class Vec4b;
#endif 

// Compact vector of 8 booleans
class Vec8b {
#if INSTRSET < 10
    // There is a problem in the case where we have AVX512F but not AVX512DQ:
    // We have 8-bit masks, but 8-bit mask operations (KMOVB, KANDB, etc.) require AVX512DQ.
    // We have to use 16-bit mask operations on 8-bit masks (KMOVW, KANDW, etc.).
    // I don't know if this is necessary, but I am using __mmask16 rather than __mmask8
    // in this case to avoid that the compiler generates 8-bit mask instructions.
    // We may get warnings in MS compiler when using __mmask16 on intrinsic functions
    // that require __mmask8, but I would rather have warnings than code that crashes.
    #define Vec8b_masktype __mmask16 
#else
    #define Vec8b_masktype __mmask8 
#endif
protected:
    Vec8b_masktype mm;  // Boolean mask register
public:
    // Default constructor:
    Vec8b() {
    }
    // Constructor to convert from type  __mmask8 used in intrinsics
    Vec8b(__mmask8 x) {
        mm = __mmask8(x);
    }
    // Constructor to convert from type  __mmask16 used in intrinsics
    Vec8b(__mmask16 x) {
        mm = Vec8b_masktype(x);
    }
    // Constructor to make from two halves
#if INSTRSET >= 10
    inline Vec8b(Vec4b const x0, Vec4b const x1);     //  Implemented below after declaration of Vec4b
#elif INSTRSET == 9 && MAX_VECTOR_SIZE >= 512         // special case of mixed compact and broad vectors
    inline Vec8b(Vec4qb const x0, Vec4qb const x1);   // in vectorf512.h
    inline Vec8b(Vec4db const x0, Vec4db const x1);   // in vectorf512.h
#endif

    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec8b & operator = (Vec8b_masktype x) {
        mm = Vec8b_masktype(x);
        return *this;
    }
    // Constructor to build from all elements:
    Vec8b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7) {
        mm = uint8_t(
            (uint8_t)b0 | (uint8_t)b1 << 1 | (uint8_t)b2 << 2 | (uint8_t)b3 << 3 |
            (uint8_t)b4 << 4 | (uint8_t)b5 << 5 | (uint8_t)b6 << 6 | (uint8_t)b7 << 7);
    }
    // Constructor to broadcast single value:
    Vec8b(bool b) {
        mm = Vec8b_masktype(-int16_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec8b & operator = (bool b) {
        mm = Vec8b_masktype(Vec8b(b));
        return *this;
    }
    // Type cast operator to convert to __mmask16 used in intrinsics
    operator Vec8b_masktype() const {
        return mm;
    }
    // split into two halves
#if INSTRSET >= 10
    Vec4b get_low()  const;
    Vec4b get_high() const;
#elif INSTRSET == 9 && MAX_VECTOR_SIZE >= 512    // special case of mixed compact and broad vectors
    Vec4qb get_low()  const;                     // in vectorf512.h
    Vec4qb get_high() const;                     // in vectorf512.h
#endif 
    // Member function to change a single element in vector
    Vec8b const insert(int index, bool value) {
        mm = Vec8b_masktype(((uint8_t)mm & ~(1 << index)) | (int)value << index);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return ((uint32_t)mm >> index) & 1;
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec8b & load_bits(uint8_t a) {
        mm = Vec8b_masktype(a);
        return *this;
    }
    // Number of elements
    static constexpr int size() {
        return 8;
    }
    // Type of elements
    static constexpr int elementtype() {
        return 2;
    }
};

// Members of Vec16b that refer to Vec8b:
inline Vec16b::Vec16b(Vec8b const x0, Vec8b const x1) {
    mm = uint8_t(x0) | uint16_t(x1) << 8;
}
#if INSTRSET >= 10
inline Vec8b Vec16b::get_low() const {
    return Vec8b().load_bits(uint8_t(mm));
}
inline Vec8b Vec16b::get_high() const {
    return Vec8b().load_bits(uint8_t((uint16_t)mm >> 8u));
}
#endif

#endif   // INSTRSET >= 9

#if INSTRSET >= 10
class Vec4b : public Vec8b {
public:
    // Default constructor:
    Vec4b() {
    }
    // Constructor to make from two halves
    inline Vec4b(Vec2b const x0, Vec2b const x1); // Implemented below after declaration of Vec4b

    // Constructor to convert from type __mmask8 used in intrinsics
    Vec4b(__mmask8 x) {
        mm = x;
    }
    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec4b & operator = (__mmask8 x) {
        mm = x;
        return *this;
    }
    // Constructor to build from all elements:
    Vec4b(bool b0, bool b1, bool b2, bool b3) {
        mm = (uint8_t)b0 | (uint8_t)b1 << 1 | (uint8_t)b2 << 2 | (uint8_t)b3 << 3;
    }
    // Constructor to broadcast single value:
    Vec4b(bool b) {
        mm = -int8_t(b) & 0x0F;
    }
    // Assignment operator to broadcast scalar value:
    Vec4b & operator = (bool b) {
        mm = Vec4b(b);
        return *this;
    }
    // split into two halves
    Vec2b get_low()  const;  // Implemented below after declaration of Vec4b
    Vec2b get_high() const;  // Implemented below after declaration of Vec4b

    // Member function to change a bitfield to a boolean vector
    Vec4b & load_bits(uint8_t a) {
        mm = a & 0x0F;
        return *this;
    }
    // Number of elements
    static constexpr int size() {
        return 4;
    }
};

class Vec2b : public Vec8b {
public:
    // Default constructor:
    Vec2b() {
    }
    // Constructor to convert from type  __mmask8 used in intrinsics
    Vec2b(__mmask8 x) {
        mm = x;
    }
    // Assignment operator to convert from type __mmask16 used in intrinsics:
    Vec2b & operator = (__mmask8 x) {
        mm = x;
        return *this;
    }
    // Constructor to build from all elements:
    Vec2b(bool b0, bool b1) {
        mm = (uint8_t)b0 | (uint8_t)b1 << 1;
    }
    // Constructor to broadcast single value:
    Vec2b(bool b) {
        mm = -int8_t(b) & 0x03;
    }
    // Assignment operator to broadcast scalar value:
    Vec2b & operator = (bool b) {
        mm = Vec2b(b);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec2b & load_bits(uint8_t a) {
        mm = a & 0x03;
        return *this;
    }
    // Number of elements
    static constexpr int size() {
        return 2;
    }
};

// Members of Vec8b that refer to Vec4b:
inline Vec8b::Vec8b(Vec4b const x0, Vec4b const x1) {
    mm = (uint8_t(x0) & 0x0F) | (uint8_t(x1) << 4);
}
inline Vec4b Vec8b::get_low() const {
    return Vec4b().load_bits(mm & 0xF);
}
inline Vec4b Vec8b::get_high() const {
    return Vec4b().load_bits(mm >> 4u);
} 
//  Members of Vec4b that refer to Vec2b:
inline Vec4b::Vec4b(Vec2b const x0, Vec2b const x1) {
    mm = (uint8_t(x0) & 0x03) | (uint8_t(x1) << 2);
}
inline Vec2b Vec4b::get_low() const {
    return Vec2b().load_bits(mm & 3);
}
inline Vec2b Vec4b::get_high() const {
    return Vec2b().load_bits(mm >> 2u);
} 

#endif

/*****************************************************************************
*
*          Define operators and functions for Vec16b
*
*****************************************************************************/

#if INSTRSET >= 9

// vector operator & : and
static inline Vec16b operator & (Vec16b a, Vec16b b) {
    return _mm512_kand(__mmask16(a), __mmask16(b));
}
static inline Vec16b operator && (Vec16b a, Vec16b b) {
    return a & b;
}

// vector operator | : or
static inline Vec16b operator | (Vec16b a, Vec16b b) {
    return _mm512_kor(__mmask16(a), __mmask16(b));
}
static inline Vec16b operator || (Vec16b a, Vec16b b) {
    return a | b;
}

// vector operator ^ : xor
static inline Vec16b operator ^ (Vec16b a, Vec16b b) {
    return _mm512_kxor(__mmask16(a), __mmask16(b));
}

// vector operator == : xnor
static inline Vec16b operator == (Vec16b a, Vec16b b) {
    return _mm512_kxnor(__mmask16(a), __mmask16(b));
}

// vector operator != : xor
static inline Vec16b operator != (Vec16b a, Vec16b b) {
    return a ^ b;
}

// vector operator ~ : not
static inline Vec16b operator ~ (Vec16b a) {
    return _mm512_knot(__mmask16(a));
}

// vector operator ! : element not
static inline Vec16b operator ! (Vec16b a) {
    return ~a;
}

// vector operator &= : and
static inline Vec16b & operator &= (Vec16b & a, Vec16b b) {
    a = a & b;
    return a;
}

// vector operator |= : or
static inline Vec16b & operator |= (Vec16b & a, Vec16b b) {
    a = a | b;
    return a;
}

// vector operator ^= : xor
static inline Vec16b & operator ^= (Vec16b & a, Vec16b b) {
    a = a ^ b;
    return a;
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec16b const a) {
    return __mmask16(a) == 0xFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec16b const a) {
    return __mmask16(a) != 0;
}

// function andnot: a & ~ b
static inline Vec16b andnot(Vec16b const a, Vec16b const b) {
    return _mm512_kandn(b, a);
}

#endif


/*****************************************************************************
*
*          Define operators and functions for Vec8b
*
*****************************************************************************/

#if INSTRSET >= 9   // compact boolean vectors

// vector operator & : and
static inline Vec8b operator & (Vec8b a, Vec8b b) {
#if INSTRSET >= 10  // 8-bit mask operations require AVX512DQ
    // _kand_mask8(__mmask8(a), __mmask8(b)) // not defined
    // must convert result to 8 bit, because bitwise operators promote everything to 32 bit results
    return __mmask8(__mmask8(a) & __mmask8(b));
#else
    return _mm512_kand(__mmask16(a), __mmask16(b));
#endif
}
static inline Vec8b operator && (Vec8b a, Vec8b b) {
    return a & b;
}

// vector operator | : or
static inline Vec8b operator | (Vec8b a, Vec8b b) {
#if INSTRSET >= 10  // 8-bit mask operations require AVX512DQ
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kor_mask8(__mmask8(a), __mmask8(b));
#else
    return _mm512_kor(__mmask16(a), __mmask16(b));
#endif
}
static inline Vec8b operator || (Vec8b a, Vec8b b) {
    return a | b;
}

// vector operator ^ : xor
static inline Vec8b operator ^ (Vec8b a, Vec8b b) {
#if INSTRSET >= 10  // 8-bit mask operations require AVX512DQ
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kxor_mask8(__mmask8(a), __mmask8(b));
#else
    return _mm512_kxor(__mmask16(a), __mmask16(b));
#endif
}

// vector operator == : xnor
static inline Vec8b operator == (Vec8b a, Vec8b b) {
#if INSTRSET >= 10  // 8-bit mask operations require AVX512DQ
    return __mmask8(~(__mmask8(a) ^ __mmask8(b))); // _kxnor_mask8(__mmask8(a), __mmask8(b));
#else
    return __mmask16(uint8_t(__mmask8(a) ^ __mmask8(b)));
#endif
}

// vector operator != : xor
static inline Vec8b operator != (Vec8b a, Vec8b b) {
    return a ^ b;
}

// vector operator ~ : not
static inline Vec8b operator ~ (Vec8b a) {
#if INSTRSET >= 10  // 8-bit mask operations require AVX512DQ
    return __mmask8(~__mmask8(a)); //_knot_mask8(__mmask8(a));
#else
    return _mm512_knot(__mmask16(a));
#endif
}

// vector operator ! : element not
static inline Vec8b operator ! (Vec8b a) {
    return ~a;
}

// vector operator &= : and
static inline Vec8b & operator &= (Vec8b & a, Vec8b b) {
    a = a & b;
    return a;
}

// vector operator |= : or
static inline Vec8b & operator |= (Vec8b & a, Vec8b b) {
    a = a | b;
    return a;
}

// vector operator ^= : xor
static inline Vec8b & operator ^= (Vec8b & a, Vec8b b) {
    a = a ^ b;
    return a;
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec8b const a) {
    return uint8_t(Vec8b_masktype(a)) == 0xFFu;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec8b const a) {
    return uint8_t(Vec8b_masktype(a)) != 0;
}

// function andnot: a & ~ b
static inline Vec8b andnot(Vec8b const a, Vec8b const b) {
    return Vec8b_masktype(_mm512_kandn(b, a));
}
#endif


/*****************************************************************************
*
*          Define operators for Vec4b
*
*****************************************************************************/

#if INSTRSET >= 10  // compact boolean vectors

// vector operator & : and
static inline Vec4b operator & (Vec4b a, Vec4b b) {
    return __mmask8(__mmask8(a) & __mmask8(b)); // _kand_mask8(__mmask8(a), __mmask8(b)) // not defined
}
static inline Vec4b operator && (Vec4b a, Vec4b b) {
    return a & b;
}

// vector operator | : or
static inline Vec4b operator | (Vec4b a, Vec4b b) {
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kor_mask8(__mmask8(a), __mmask8(b));
}
static inline Vec4b operator || (Vec4b a, Vec4b b) {
    return a | b;
}

// vector operator ^ : xor
static inline Vec4b operator ^ (Vec4b a, Vec4b b) {
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kxor_mask8(__mmask8(a), __mmask8(b));
}

// vector operator ~ : not
static inline Vec4b operator ~ (Vec4b a) {
    return __mmask8(__mmask8(a) ^ 0x0F);
}

// vector operator == : xnor
static inline Vec4b operator == (Vec4b a, Vec4b b) {
    return ~(a ^ b);
}

// vector operator != : xor
static inline Vec4b operator != (Vec4b a, Vec4b b) {
    return a ^ b;
}

// vector operator ! : element not
static inline Vec4b operator ! (Vec4b a) {
    return ~a;
}

// vector operator &= : and
static inline Vec4b & operator &= (Vec4b & a, Vec4b b) {
    a = a & b;
    return a;
}

// vector operator |= : or
static inline Vec4b & operator |= (Vec4b & a, Vec4b b) {
    a = a | b;
    return a;
}

// vector operator ^= : xor
static inline Vec4b & operator ^= (Vec4b & a, Vec4b b) {
    a = a ^ b;
    return a;
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec4b const a) {
    return (__mmask8(a) & 0x0F) == 0x0F;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec4b const a) {
    return (__mmask8(a) & 0x0F) != 0;
}

// function andnot: a & ~ b
static inline Vec4b andnot(Vec4b const a, Vec4b const b) {
    return __mmask8(andnot(Vec8b(a), Vec8b(b)));
} 


/*****************************************************************************
*
*          Define operators for Vec2b
*
*****************************************************************************/

// vector operator & : and
static inline Vec2b operator & (Vec2b a, Vec2b b) {
    return __mmask8(__mmask8(a) & __mmask8(b)); // _kand_mask8(__mmask8(a), __mmask8(b)) // not defined
}
static inline Vec2b operator && (Vec2b a, Vec2b b) {
    return a & b;
}

// vector operator | : or
static inline Vec2b operator | (Vec2b a, Vec2b b) {
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kor_mask8(__mmask8(a), __mmask8(b));
}
static inline Vec2b operator || (Vec2b a, Vec2b b) {
    return a | b;
}

// vector operator ^ : xor
static inline Vec2b operator ^ (Vec2b a, Vec2b b) {
    return __mmask8(__mmask8(a) | __mmask8(b)); // _kxor_mask8(__mmask8(a), __mmask8(b));
}

// vector operator ~ : not
static inline Vec2b operator ~ (Vec2b a) {
    return __mmask8(__mmask8(a) ^ 0x03);
}

// vector operator == : xnor
static inline Vec2b operator == (Vec2b a, Vec2b b) {
    return ~(a ^ b);
}

// vector operator != : xor
static inline Vec2b operator != (Vec2b a, Vec2b b) {
    return a ^ b;
}

// vector operator ! : element not
static inline Vec2b operator ! (Vec2b a) {
    return ~a;
}

// vector operator &= : and
static inline Vec2b & operator &= (Vec2b & a, Vec2b b) {
    a = a & b;
    return a;
}

// vector operator |= : or
static inline Vec2b & operator |= (Vec2b & a, Vec2b b) {
    a = a | b;
    return a;
}

// vector operator ^= : xor
static inline Vec2b & operator ^= (Vec2b & a, Vec2b b) {
    a = a ^ b;
    return a;
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec2b const a) {
    return (__mmask8(a) & 0x03) == 0x03;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec2b const a) {
    return (__mmask8(a) & 0x03) != 0;
}

// function andnot: a & ~ b
static inline Vec2b andnot(Vec2b const a, Vec2b const b) {
    return __mmask8(andnot(Vec8b(a), Vec8b(b)));
}
#endif

/*****************************************************************************
*
*     Vector of 128 bits. Used internally as base class
*
*****************************************************************************/
class Vec128b {
protected:
    __m128i xmm; // Integer vector
public:
    // Default constructor:
    Vec128b() {
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec128b(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec128b & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec128b & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 16
    // "load_a" is faster than "load" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale, and Atom), but not on other processors from Intel, AMD or VIA.
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 16.
    void load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm_storeu_si128((__m128i*)p, xmm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(void * p) const {
        _mm_stream_si128((__m128i*)p, xmm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 16
    // "store_a" is faster than "store" on older Intel processors (Pentium 4, Pentium M, Core 1,
    // Merom, Wolfdale, and Atom), but not on other processors from Intel, AMD or VIA.
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 16.
    void store_a(void * p) const {
        _mm_store_si128((__m128i*)p, xmm);
    }
    static constexpr int size() {
        return 128;
    }
    static constexpr int elementtype() {
        return 1;
    }
    typedef __m128i registertype;
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec128b operator & (Vec128b const a, Vec128b const b) {
    return _mm_and_si128(a, b);
}
static inline Vec128b operator && (Vec128b const a, Vec128b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec128b operator | (Vec128b const a, Vec128b const b) {
    return _mm_or_si128(a, b);
}
static inline Vec128b operator || (Vec128b const a, Vec128b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec128b operator ^ (Vec128b const a, Vec128b const b) {
    return _mm_xor_si128(a, b);
}

// vector operator ~ : bitwise not
static inline Vec128b operator ~ (Vec128b const a) {
    return _mm_xor_si128(a, _mm_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec128b & operator &= (Vec128b & a, Vec128b const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec128b & operator |= (Vec128b & a, Vec128b const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec128b & operator ^= (Vec128b & a, Vec128b const b) {
    a = a ^ b;
    return a;
}

// Define functions for this class

// function andnot: a & ~ b
static inline Vec128b andnot(Vec128b const a, Vec128b const b) {
    return _mm_andnot_si128(b, a);
}


/*****************************************************************************
*
*          selectb function
*
*****************************************************************************/
// Select between two sources, byte by byte, using broad boolean vector s.
// Used in various functions and operators
// Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFF (true). No other values are allowed.
// The implementation depends on the instruction set: 
// If SSE4.1 is supported then only bit 7 in each byte of s is checked, 
// otherwise all bits in s are used.
static inline __m128i selectb(__m128i const s, __m128i const a, __m128i const b) {
#if INSTRSET >= 5    // SSE4.1
    return _mm_blendv_epi8(b, a, s);
#else
    return _mm_or_si128(_mm_and_si128(s, a), _mm_andnot_si128(s, b));
#endif
}


/*****************************************************************************
*
*          Horizontal Boolean functions
*
*****************************************************************************/

static inline bool horizontal_and(Vec128b const a) {
#if INSTRSET >= 5   // SSE4.1. Use PTEST
    return _mm_testc_si128(a, _mm_set1_epi32(-1)) != 0;
#else
    __m128i t1 = _mm_unpackhi_epi64(a, a);                 // get 64 bits down
    __m128i t2 = _mm_and_si128(a, t1);                     // and 64 bits
#ifdef __x86_64__
    int64_t t5 = _mm_cvtsi128_si64(t2);                    // transfer 64 bits to integer
    return  t5 == int64_t(-1);
#else
    __m128i t3 = _mm_srli_epi64(t2, 32);                   // get 32 bits down
    __m128i t4 = _mm_and_si128(t2, t3);                    // and 32 bits
    int     t5 = _mm_cvtsi128_si32(t4);                    // transfer 32 bits to integer
    return  t5 == -1;
#endif  // __x86_64__
#endif  // INSTRSET
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or(Vec128b const a) {
#if INSTRSET >= 5   // SSE4.1. Use PTEST
    return !_mm_testz_si128(a, a);
#else
    __m128i t1 = _mm_unpackhi_epi64(a, a);                 // get 64 bits down
    __m128i t2 = _mm_or_si128(a, t1);                      // and 64 bits
#ifdef __x86_64__
    int64_t t5 = _mm_cvtsi128_si64(t2);                    // transfer 64 bits to integer
    return  t5 != int64_t(0);
#else
    __m128i t3 = _mm_srli_epi64(t2, 32);                   // get 32 bits down
    __m128i t4 = _mm_or_si128(t2, t3);                     // and 32 bits
    int     t5 = _mm_cvtsi128_si32(t4);                    // transfer to integer
    return  t5 != 0;
#endif  // __x86_64__
#endif  // INSTRSET
}


/*****************************************************************************
*
*          Vector of 16 8-bit signed integers
*
*****************************************************************************/

class Vec16c : public Vec128b {
public:
    // Default constructor:
    Vec16c() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16c(int i) {
        xmm = _mm_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec16c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15) {
        xmm = _mm_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16c(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16c & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec16c & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec16c & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16c & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        xmm = _mm_maskz_loadu_epi8(__mmask16((1u << n) - 1), p);
#else
        if (n >= 16) load(p);
        else if (n <= 0) * this = 0;
        else if (((int)(intptr_t)p & 0xFFF) < 0xFF0) {
            // p is at least 16 bytes from a page boundary. OK to read 16 bytes
            load(p);
        }
        else {
            // worst case. read 1 byte at a time and suffer store forwarding penalty
            char x[16];
            for (int i = 0; i < n; i++) x[i] = ((char const *)p)[i];
            load(x);
        }
        cutoff(n);
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm_mask_storeu_epi8(p, __mmask16((1u << n) - 1), xmm);
#else
        if (n >= 16) {
            store(p);
            return;
        }
        if (n <= 0) return;
        // we are not using _mm_maskmoveu_si128 because it is too slow on many processors
        union {
            int8_t  c[16];
            int16_t s[8];
            int32_t i[4];
            int64_t q[2];
        } u;
        store(u.c);
        int j = 0;
        if (n & 8) {
            *(int64_t*)p = u.q[0];
            j += 8;
        }
        if (n & 4) {
            ((int32_t*)p)[j / 4] = u.i[j / 4];
            j += 4;
        }
        if (n & 2) {
            ((int16_t*)p)[j / 2] = u.s[j / 2];
            j += 2;
        }
        if (n & 1) {
            ((int8_t*)p)[j] = u.c[j];
        }
#endif
    }

    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16c & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_epi8(__mmask16((1u << n) - 1), xmm);
#else 
        if (uint32_t(n) >= 16) return *this;
        const char mask[32] = { -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };
        *this &= Vec16c().load(mask + 16 - n);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec16c const insert(int index, int8_t value) {
#if INSTRSET >= 10
        xmm = _mm_mask_set1_epi8(xmm, __mmask16(1u << index), value);
#else
        const int8_t maskl[32] = { 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
            -1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0 };
        __m128i broad = _mm_set1_epi8(value);  // broadcast value into all elements
        __m128i mask  = _mm_loadu_si128((__m128i const*)(maskl + 16 - (index & 0x0F))); // mask with FF at index position
        xmm = selectb(mask, broad, xmm);
#endif
        return *this;
    }
    /* Note: The extract(), insert(), size(), [], etc. all use int index for consistency.
    An unsigned type for index might cause problems in case of underflow, for example:
    for (i = 0; i < a.size() - 4; i++) a[i] = ...
    This would go nuts if a.size() is 2.
    */

    // Member function extract a single element from vector
    int8_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m128i x = _mm_maskz_compress_epi8(__mmask16(1u << index), xmm);
        return (int8_t)_mm_cvtsi128_si32(x);
#else   
        int8_t x[16];
        store(x);
        return x[index & 0x0F];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 4;
    }
};


/*****************************************************************************
*
*          Vec16cb: Vector of 16 Booleans for use with Vec16c and Vec16uc
*
*****************************************************************************/
#if INSTRSET < 10   // broad boolean vectors
class Vec16cb : public Vec16c {
public:
    // Default constructor
    Vec16cb() {}
    // Constructor to build from all elements:
    Vec16cb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) {
        xmm = Vec16c(-int8_t(x0), -int8_t(x1), -int8_t(x2), -int8_t(x3), -int8_t(x4), -int8_t(x5), -int8_t(x6), -int8_t(x7),
            -int8_t(x8), -int8_t(x9), -int8_t(x10), -int8_t(x11), -int8_t(x12), -int8_t(x13), -int8_t(x14), -int8_t(x15));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16cb(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16cb & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16cb(bool b) : Vec16c(-int8_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16cb & operator = (bool b) {
        *this = Vec16cb(b);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16cb & insert(int index, bool a) {
        Vec16c::insert(index, -(int)a);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec16cb & load_bits(uint16_t a) {
        uint16_t an = uint16_t(~a);              // invert because we have no compare-not-equal
#if  INSTRSET >= 4  // SSSE3 (PSHUFB available under SSSE3)
        __m128i a1 = _mm_cvtsi32_si128(an);      // load into xmm register
        __m128i dist = constant4ui<0, 0, 0x01010101, 0x01010101>();
        __m128i a2 = _mm_shuffle_epi8(a1, dist); // one byte of a in each element
        __m128i mask = constant4ui<0x08040201, 0x80402010, 0x08040201, 0x80402010>();
        __m128i a3 = _mm_and_si128(a2, mask);    // isolate one bit in each byte       
#else
        __m128i b1 = _mm_set1_epi8((int8_t)an);  // broadcast low byte
        __m128i b2 = _mm_set1_epi8((int8_t)(an >> 8));  // broadcast high byte
        __m128i m1 = constant4ui<0x08040201, 0x80402010, 0, 0>();
        __m128i m2 = constant4ui<0, 0, 0x08040201, 0x80402010>();
        __m128i c1 = _mm_and_si128(b1, m1); // isolate one bit in each byte of lower half
        __m128i c2 = _mm_and_si128(b2, m2); // isolate one bit in each byte of upper half
        __m128i a3 = _mm_or_si128(c1, c2);
#endif
        xmm = _mm_cmpeq_epi8(a3, _mm_setzero_si128());  // compare with 0
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec16c::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec16cb(int b) = delete;
    Vec16cb & operator = (int x) = delete;
};

#else
typedef Vec16b Vec16cb;  // compact boolean vector
#endif    // broad boolean vectors


/*****************************************************************************
*
*          Define operators for Vec16cb
*
*****************************************************************************/

#if INSTRSET < 10   // broad boolean vectors

// vector operator & : bitwise and
static inline Vec16cb operator & (Vec16cb const a, Vec16cb const b) {
    return Vec16cb(Vec128b(a) & Vec128b(b));
}
static inline Vec16cb operator && (Vec16cb const a, Vec16cb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16cb & operator &= (Vec16cb & a, Vec16cb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16cb operator | (Vec16cb const a, Vec16cb const b) {
    return Vec16cb(Vec128b(a) | Vec128b(b));
}
static inline Vec16cb operator || (Vec16cb const a, Vec16cb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16cb & operator |= (Vec16cb & a, Vec16cb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16cb operator ^ (Vec16cb const a, Vec16cb const b) {
    return Vec16cb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16cb & operator ^= (Vec16cb & a, Vec16cb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec16cb operator == (Vec16cb const a, Vec16cb const b) {
    return Vec16cb(a ^ (~b));
}

// vector operator != : xor
static inline Vec16cb operator != (Vec16cb const a, Vec16cb const b) {
    return Vec16cb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec16cb operator ~ (Vec16cb const a) {
    return Vec16cb(~Vec128b(a));
}

// vector operator ! : element not
static inline Vec16cb operator ! (Vec16cb const a) {
    return ~a;
}

// vector function andnot
static inline Vec16cb andnot(Vec16cb const a, Vec16cb const b) {
    return Vec16cb(andnot(Vec128b(a), Vec128b(b)));
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec16cb const a) {
    return _mm_movemask_epi8(a) == 0xFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec16cb const a) {
#if INSTRSET >= 5   // SSE4.1. Use PTEST
    return !_mm_testz_si128(a, a);
#else
    return _mm_movemask_epi8(a) != 0;
#endif
}
#endif    // broad boolean vectors


/*****************************************************************************
*
*          Define operators for Vec16c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16c operator + (Vec16c const a, Vec16c const b) {
    return _mm_add_epi8(a, b);
}
// vector operator += : add
static inline Vec16c & operator += (Vec16c & a, Vec16c const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16c operator ++ (Vec16c & a, int) {
    Vec16c a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec16c & operator ++ (Vec16c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16c operator - (Vec16c const a, Vec16c const b) {
    return _mm_sub_epi8(a, b);
}
// vector operator - : unary minus
static inline Vec16c operator - (Vec16c const a) {
    return _mm_sub_epi8(_mm_setzero_si128(), a);
}
// vector operator -= : add
static inline Vec16c & operator -= (Vec16c & a, Vec16c const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16c operator -- (Vec16c & a, int) {
    Vec16c a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec16c & operator -- (Vec16c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16c operator * (Vec16c const a, Vec16c const b) {
    // There is no 8-bit multiply in SSE2. Split into two 16-bit multiplies
    __m128i aodd = _mm_srli_epi16(a, 8);         // odd numbered elements of a
    __m128i bodd = _mm_srli_epi16(b, 8);         // odd numbered elements of b
    __m128i muleven = _mm_mullo_epi16(a, b);     // product of even numbered elements
    __m128i mulodd = _mm_mullo_epi16(aodd, bodd);// product of odd  numbered elements
    mulodd = _mm_slli_epi16(mulodd, 8);          // put odd numbered elements back in place
#if INSTRSET >= 10   // AVX512VL + AVX512BW
    return _mm_mask_mov_epi8(mulodd, 0x5555, muleven);
#else
    __m128i mask = _mm_set1_epi32(0x00FF00FF);   // mask for even positions
    return selectb(mask, muleven, mulodd);       // interleave even and odd
#endif
}

// vector operator *= : multiply
static inline Vec16c & operator *= (Vec16c & a, Vec16c const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16c operator << (Vec16c const a, int b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;         // mask to remove bits that are shifted out
    __m128i am = _mm_and_si128(a, _mm_set1_epi8((char)mask));// remove bits that will overflow
    __m128i res = _mm_sll_epi16(am, _mm_cvtsi32_si128(b));// 16-bit shifts
    return res;
}
// vector operator <<= : shift left
static inline Vec16c & operator <<= (Vec16c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec16c operator >> (Vec16c const a, int b) {
    __m128i aeven = _mm_slli_epi16(a, 8);                  // even numbered elements of a. get sign bit in position
    aeven = _mm_sra_epi16(aeven, _mm_cvtsi32_si128(b + 8));// shift arithmetic, back to position
    __m128i aodd = _mm_sra_epi16(a, _mm_cvtsi32_si128(b)); // shift odd numbered elements arithmetic
#if INSTRSET >= 10   // AVX512VL + AVX512BW
    return _mm_mask_mov_epi8(aodd, 0x5555, aeven);
#else
    __m128i mask = _mm_set1_epi32(0x00FF00FF);             // mask for even positions
    __m128i res = selectb(mask, aeven, aodd);              // interleave even and odd
    return res;
#endif
}
// vector operator >>= : shift right arithmetic
static inline Vec16c & operator >>= (Vec16c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16cb operator == (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 0);
#else
    return _mm_cmpeq_epi8(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec16cb operator != (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 4);
#else
    return Vec16cb(Vec16c(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec16cb operator > (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 6);
#else
    return _mm_cmpgt_epi8(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec16cb operator < (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16cb operator >= (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 5);
#else
    return Vec16cb(Vec16c(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16cb operator <= (Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec16c operator & (Vec16c const a, Vec16c const b) {
    return Vec16c(Vec128b(a) & Vec128b(b));
}
static inline Vec16c operator && (Vec16c const a, Vec16c const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16c & operator &= (Vec16c & a, Vec16c const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16c operator | (Vec16c const a, Vec16c const b) {
    return Vec16c(Vec128b(a) | Vec128b(b));
}
static inline Vec16c operator || (Vec16c const a, Vec16c const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16c & operator |= (Vec16c & a, Vec16c const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16c operator ^ (Vec16c const a, Vec16c const b) {
    return Vec16c(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16c & operator ^= (Vec16c & a, Vec16c const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16c operator ~ (Vec16c const a) {
    return Vec16c(~Vec128b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec16cb operator ! (Vec16c const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi8_mask(a, _mm_setzero_si128(), 0);
#else
    return _mm_cmpeq_epi8(a, _mm_setzero_si128());
#endif    
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
static inline Vec16c select(Vec16cb const s, Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi8(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16c if_add(Vec16cb const f, Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi8(a, f, a, b);
#else
    return a + (Vec16c(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec16c if_sub(Vec16cb const f, Vec16c const a, Vec16c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi8(a, f, a, b);
#else
    return a - (Vec16c(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec16c if_mul(Vec16cb const f, Vec16c const a, Vec16c const b) {
    return select(f, a * b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add(Vec16c const a) {
    __m128i sum1 = _mm_sad_epu8(a, _mm_setzero_si128());
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);
    __m128i sum3 = _mm_add_epi16(sum1, sum2);
    int8_t  sum4 = (int8_t)_mm_cvtsi128_si32(sum3);        // truncate to 8 bits
    return  sum4;                                          // sign extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x(Vec16c const a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epi8(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    return         _mm_cvtsi128_si32(sum3);
#else  
    __m128i aeven = _mm_slli_epi16(a, 8);                  // even numbered elements of a. get sign bit in position
    aeven = _mm_srai_epi16(aeven, 8);                      // sign extend even numbered elements
    __m128i aodd = _mm_srai_epi16(a, 8);                   // sign extend odd  numbered elements
    __m128i sum1 = _mm_add_epi16(aeven, aodd);             // add even and odd elements
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
#if INSTRSET >= 4 && false // SSSE3
    __m128i sum2 = _mm_hadd_epi16(sum1, sum1);
    __m128i sum3 = _mm_hadd_epi16(sum2, sum2);
    __m128i sum4 = _mm_hadd_epi16(sum3, sum3);
#else
    __m128i sum2 = _mm_add_epi16(sum1, _mm_unpackhi_epi64(sum1, sum1));
    __m128i sum3 = _mm_add_epi16(sum2, _mm_shuffle_epi32(sum2, 1));
    __m128i sum4 = _mm_add_epi16(sum3, _mm_shufflelo_epi16(sum3, 1));
#endif
    int16_t sum5 = (int16_t)_mm_cvtsi128_si32(sum4);       // 16 bit sum
    return  sum5;                                          // sign extend to 32 bits
#endif
}


// function add_saturated: add element by element, signed with saturation
static inline Vec16c add_saturated(Vec16c const a, Vec16c const b) {
    return _mm_adds_epi8(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16c sub_saturated(Vec16c const a, Vec16c const b) {
    return _mm_subs_epi8(a, b);
}

// function max: a > b ? a : b
static inline Vec16c max(Vec16c const a, Vec16c const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epi8(a, b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80808080);
    __m128i a1 = _mm_xor_si128(a, signbit);                // add 0x80
    __m128i b1 = _mm_xor_si128(b, signbit);                // add 0x80
    __m128i m1 = _mm_max_epu8(a1, b1);                     // unsigned max
    return  _mm_xor_si128(m1, signbit);                    // sub 0x80
#endif
}

// function min: a < b ? a : b
static inline Vec16c min(Vec16c const a, Vec16c const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epi8(a, b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80808080);
    __m128i a1 = _mm_xor_si128(a, signbit);                // add 0x80
    __m128i b1 = _mm_xor_si128(b, signbit);                // add 0x80
    __m128i m1 = _mm_min_epu8(a1, b1);                     // unsigned min
    return  _mm_xor_si128(m1, signbit);                    // sub 0x80
#endif
}

// function abs: a >= 0 ? a : -a
static inline Vec16c abs(Vec16c const a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_abs_epi8(a);
#else                 // SSE2
    __m128i nega = _mm_sub_epi8(_mm_setzero_si128(), a);
    return _mm_min_epu8(a, nega);   // unsigned min (the negative value is bigger when compared as unsigned)
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16c abs_saturated(Vec16c const a) {
    __m128i absa = abs(a);                                 // abs(a)
#if INSTRSET >= 10
    return _mm_min_epu8(absa, Vec16c(0x7F));
#else
    __m128i overfl = _mm_cmpgt_epi8(_mm_setzero_si128(), absa);// 0 > a
    return           _mm_add_epi8(absa, overfl);           // subtract 1 if 0x80
#endif
}

// function rotate_left: rotate each element left by b bits 
// Use negative count to rotate right
static inline Vec16c rotate_left(Vec16c const a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return (Vec16c)_mm_rot_epi8(a, _mm_set1_epi8(b));
#else  // SSE2 instruction set
    uint8_t mask = 0xFFu << b;                             // mask off overflow bits
    __m128i m = _mm_set1_epi8(mask);
    __m128i bb = _mm_cvtsi32_si128(b & 7);                 // b modulo 8
    __m128i mbb = _mm_cvtsi32_si128((-b) & 7);             // 8-b modulo 8
    __m128i left = _mm_sll_epi16(a, bb);                   // a << b
    __m128i right = _mm_srl_epi16(a, mbb);                 // a >> 8-b
    left  = _mm_and_si128(m, left);                        // mask off overflow bits
    right = _mm_andnot_si128(m, right);
    return  _mm_or_si128(left, right);                     // combine left and right shifted bits
#endif
}


/*****************************************************************************
*
*          Vector of 16 8-bit unsigned integers
*
*****************************************************************************/

class Vec16uc : public Vec16c {
public:
    // Default constructor:
    Vec16uc() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16uc(uint32_t i) {
        xmm = _mm_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec16uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15) {
        xmm = _mm_setr_epi8((int8_t)i0, (int8_t)i1, (int8_t)i2, (int8_t)i3, (int8_t)i4, (int8_t)i5, (int8_t)i6,
            (int8_t)i7, (int8_t)i8, (int8_t)i9, (int8_t)i10, (int8_t)i11, (int8_t)i12, (int8_t)i13, (int8_t)i14, (int8_t)i15);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec16uc(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec16uc & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16uc & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec16uc & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16uc const insert(int index, uint8_t value) {
        Vec16c::insert(index, (int8_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(int index) const {
        return uint8_t(Vec16c::extract(index));
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int elementtype() {
        return 5;
    }
};

// Define operators for this class

// vector operator << : shift left all elements
static inline Vec16uc operator << (Vec16uc const a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;               // mask to remove bits that are shifted out
    __m128i am = _mm_and_si128(a, _mm_set1_epi8((char)mask));  // remove bits that will overflow
    __m128i res = _mm_sll_epi16(am, _mm_cvtsi32_si128((int)b)); // 16-bit shifts
    return res;
}

// vector operator << : shift left all elements
static inline Vec16uc operator << (Vec16uc const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec16uc operator >> (Vec16uc const a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF << (uint32_t)b;               // mask to remove bits that are shifted out
    __m128i am = _mm_and_si128(a, _mm_set1_epi8((char)mask));  // remove bits that will overflow
    __m128i res = _mm_srl_epi16(am, _mm_cvtsi32_si128((int)b)); // 16-bit shifts
    return res;
}

// vector operator >> : shift right logical all elements
static inline Vec16uc operator >> (Vec16uc const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec16uc & operator >>= (Vec16uc & a, int b) {
    a = a >> b;
    return a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16cb operator >= (Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu8_mask(a, b, 5);
#else
    return (Vec16cb)_mm_cmpeq_epi8(_mm_max_epu8(a, b), a); // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16cb operator <= (Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu8_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16cb operator > (Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu8_mask(a, b, 6);
#else
    return Vec16cb(Vec16c(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16cb operator < (Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu8_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator + : add
static inline Vec16uc operator + (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec16c(a) + Vec16c(b));
}

// vector operator - : subtract
static inline Vec16uc operator - (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec16c(a) - Vec16c(b));
}

// vector operator * : multiply
static inline Vec16uc operator * (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec16c(a) * Vec16c(b));
}

// vector operator & : bitwise and
static inline Vec16uc operator & (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec128b(a) & Vec128b(b));
}
static inline Vec16uc operator && (Vec16uc const a, Vec16uc const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16uc operator | (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec128b(a) | Vec128b(b));
}
static inline Vec16uc operator || (Vec16uc const a, Vec16uc const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16uc operator ^ (Vec16uc const a, Vec16uc const b) {
    return Vec16uc(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec16uc operator ~ (Vec16uc const a) {
    return Vec16uc(~Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16uc select(Vec16cb const s, Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi8(b, s, a);
#else
    return selectb(s, a, b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16uc if_add(Vec16cb const f, Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi8(a, f, a, b);
#else
    return a + (Vec16uc(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec16uc if_sub(Vec16cb const f, Vec16uc const a, Vec16uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi8(a, f, a, b);
#else
    return a - (Vec16uc(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec16uc if_mul(Vec16cb const f, Vec16uc const a, Vec16uc const b) {
    return select(f, a * b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
// (Note: horizontal_add_x(Vec16uc) is slightly faster)
static inline uint32_t horizontal_add(Vec16uc const a) {
    __m128i sum1 = _mm_sad_epu8(a, _mm_setzero_si128());
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);
    __m128i sum3 = _mm_add_epi16(sum1, sum2);
    uint16_t sum4 = (uint16_t)_mm_cvtsi128_si32(sum3);     // truncate to 16 bits
    return  sum4;
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x(Vec16uc const a) {
    __m128i sum1 = _mm_sad_epu8(a, _mm_setzero_si128());
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);
    __m128i sum3 = _mm_add_epi16(sum1, sum2);
    return (uint32_t)_mm_cvtsi128_si32(sum3);
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16uc add_saturated(Vec16uc const a, Vec16uc const b) {
    return _mm_adds_epu8(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16uc sub_saturated(Vec16uc const a, Vec16uc const b) {
    return _mm_subs_epu8(a, b);
}

// function max: a > b ? a : b
static inline Vec16uc max(Vec16uc const a, Vec16uc const b) {
    return _mm_max_epu8(a, b);
}

// function min: a < b ? a : b
static inline Vec16uc min(Vec16uc const a, Vec16uc const b) {
    return _mm_min_epu8(a, b);
}



/*****************************************************************************
*
*          Vector of 8 16-bit signed integers
*
*****************************************************************************/

class Vec8s : public Vec128b {
public:
    // Default constructor:
    Vec8s() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8s(int i) {
        xmm = _mm_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec8s(int16_t i0, int16_t i1, int16_t i2, int16_t i3, int16_t i4, int16_t i5, int16_t i6, int16_t i7) {
        xmm = _mm_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8s(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8s & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec8s & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec8s & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8s & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        xmm = _mm_maskz_loadu_epi16(__mmask8((1u << n) - 1), p);
#else
        if (n >= 8) load(p);
        else if (n <= 0) * this = 0;
        else if (((int)(intptr_t)p & 0xFFF) < 0xFF0) {
            // p is at least 16 bytes from a page boundary. OK to read 16 bytes
            load(p);
        }
        else {
            // worst case. read 1 byte at a time and suffer store forwarding penalty
            int16_t x[8];
            for (int i = 0; i < n; i++) x[i] = ((int16_t const *)p)[i];
            load(x);
        }
        cutoff(n);
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm_mask_storeu_epi16(p, __mmask8((1u << n) - 1), xmm);
#else
        if (n >= 8) {
            store(p);
            return;
        }
        if (n <= 0) return;
        // we are not using _mm_maskmoveu_si128 because it is too slow on many processors
        union {
            int8_t  c[16];
            int16_t s[8];
            int32_t i[4];
            int64_t q[2];
        } u;
        store(u.c);
        int j = 0;
        if (n & 4) {
            *(int64_t*)p = u.q[0];
            j += 8;
        }
        if (n & 2) {
            ((int32_t*)p)[j / 4] = u.i[j / 4];
            j += 4;
        }
        if (n & 1) {
            ((int16_t*)p)[j / 2] = u.s[j / 2];
        }
#endif
    }

    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8s & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_epi16(__mmask8((1u << n) - 1), xmm);
#else 
        *this = Vec16c(xmm).cutoff(n * 2);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec8s const insert(int index, int16_t value) {
#if INSTRSET >= 10
        xmm = _mm_mask_set1_epi16(xmm, __mmask8(1u << index), value);
#else
        switch (index) {
        case 0:
            xmm = _mm_insert_epi16(xmm, value, 0);  break;
        case 1:
            xmm = _mm_insert_epi16(xmm, value, 1);  break;
        case 2:
            xmm = _mm_insert_epi16(xmm, value, 2);  break;
        case 3:
            xmm = _mm_insert_epi16(xmm, value, 3);  break;
        case 4:
            xmm = _mm_insert_epi16(xmm, value, 4);  break;
        case 5:
            xmm = _mm_insert_epi16(xmm, value, 5);  break;
        case 6:
            xmm = _mm_insert_epi16(xmm, value, 6);  break;
        case 7:
            xmm = _mm_insert_epi16(xmm, value, 7);  break;
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int16_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m128i x = _mm_maskz_compress_epi16(__mmask8(1u << index), xmm);
        return (int16_t)_mm_cvtsi128_si32(x);
#else 
        switch (index) {
        case 0:
            return (int16_t)_mm_extract_epi16(xmm, 0);
        case 1:
            return (int16_t)_mm_extract_epi16(xmm, 1);
        case 2:
            return (int16_t)_mm_extract_epi16(xmm, 2);
        case 3:
            return (int16_t)_mm_extract_epi16(xmm, 3);
        case 4:
            return (int16_t)_mm_extract_epi16(xmm, 4);
        case 5:
            return (int16_t)_mm_extract_epi16(xmm, 5);
        case 6:
            return (int16_t)_mm_extract_epi16(xmm, 6);
        case 7:
            return (int16_t)_mm_extract_epi16(xmm, 7);
        }
        return 0;
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 6;
    }
};

/*****************************************************************************
*
*          Vec8sb: Vector of 8 Booleans for use with Vec8s and Vec8us
*
*****************************************************************************/
#if INSTRSET < 10   // broad boolean vectors

class Vec8sb : public Vec8s {
public:
    // Constructor to build from all elements:
    Vec8sb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        xmm = Vec8s(-int16_t(x0), -int16_t(x1), -int16_t(x2), -int16_t(x3), -int16_t(x4), -int16_t(x5), -int16_t(x6), -int16_t(x7));
    }
    // Default constructor:
    Vec8sb() {
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8sb(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8sb & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec8sb(bool b) : Vec8s(-int16_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8sb & operator = (bool b) {
        *this = Vec8sb(b);
        return *this;
    }
    Vec8sb & insert(int index, bool a) {
        Vec8s::insert(index, -(int16_t)a);
        return *this;
    }
    // Member function extract a single element from vector
    // Note: This function is inefficient. Use store function if extracting more than one element
    bool extract(int index) const {
        return Vec8s::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec8sb & load_bits(uint8_t a) {
        __m128i b1 = _mm_set1_epi8((int8_t)a);  // broadcast byte. Invert because we have no compare-not-equal
        __m128i m1 = constant4ui<0x00020001, 0x00080004, 0x00200010, 0x00800040>();
        __m128i c1 = _mm_and_si128(b1, m1); // isolate one bit in each byte
        xmm = _mm_cmpgt_epi16(c1, _mm_setzero_si128());  // compare with 0
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec8sb(int b) = delete;
    Vec8sb & operator = (int x) = delete;
};
#else
typedef Vec8b Vec8sb;
#endif


/*****************************************************************************
*
*          Define operators for Vec8sb
*
*****************************************************************************/
#if INSTRSET < 10   // broad boolean vectors

// vector operator & : bitwise and
static inline Vec8sb operator & (Vec8sb const a, Vec8sb const b) {
    return Vec8sb(Vec128b(a) & Vec128b(b));
}
static inline Vec8sb operator && (Vec8sb const a, Vec8sb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8sb & operator &= (Vec8sb & a, Vec8sb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8sb operator | (Vec8sb const a, Vec8sb const b) {
    return Vec8sb(Vec128b(a) | Vec128b(b));
}
static inline Vec8sb operator || (Vec8sb const a, Vec8sb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8sb & operator |= (Vec8sb & a, Vec8sb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8sb operator ^ (Vec8sb const a, Vec8sb const b) {
    return Vec8sb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8sb & operator ^= (Vec8sb & a, Vec8sb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec8sb operator == (Vec8sb const a, Vec8sb const b) {
    return Vec8sb(a ^ (~b));
}

// vector operator != : xor
static inline Vec8sb operator != (Vec8sb const a, Vec8sb const b) {
    return Vec8sb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec8sb operator ~ (Vec8sb const a) {
    return Vec8sb(~Vec128b(a));
}

// vector operator ! : element not
static inline Vec8sb operator ! (Vec8sb const a) {
    return ~a;
}

// vector function andnot
static inline Vec8sb andnot(Vec8sb const a, Vec8sb const b) {
    return Vec8sb(andnot(Vec128b(a), Vec128b(b)));
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec8sb const a) {
    return _mm_movemask_epi8(a) == 0xFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec8sb const a) {
#if INSTRSET >= 5   // SSE4.1. Use PTEST
    return !_mm_testz_si128(a, a);
#else
    return _mm_movemask_epi8(a) != 0;
#endif
}
#endif   // broad boolean vectors


/*****************************************************************************
*
*         operators for Vec8s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8s operator + (Vec8s const a, Vec8s const b) {
    return _mm_add_epi16(a, b);
}
// vector operator += : add
static inline Vec8s & operator += (Vec8s & a, Vec8s const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8s operator ++ (Vec8s & a, int) {
    Vec8s a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec8s & operator ++ (Vec8s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8s operator - (Vec8s const a, Vec8s const b) {
    return _mm_sub_epi16(a, b);
}
// vector operator - : unary minus
static inline Vec8s operator - (Vec8s const a) {
    return _mm_sub_epi16(_mm_setzero_si128(), a);
}
// vector operator -= : subtract
static inline Vec8s & operator -= (Vec8s & a, Vec8s const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8s operator -- (Vec8s & a, int) {
    Vec8s a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec8s & operator -- (Vec8s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8s operator * (Vec8s const a, Vec8s const b) {
    return _mm_mullo_epi16(a, b);
}

// vector operator *= : multiply
static inline Vec8s & operator *= (Vec8s & a, Vec8s const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec8s operator << (Vec8s const a, int b) {
    return _mm_sll_epi16(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec8s & operator <<= (Vec8s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8s operator >> (Vec8s const a, int b) {
    return _mm_sra_epi16(a, _mm_cvtsi32_si128(b));
}

// vector operator >>= : shift right arithmetic
static inline Vec8s & operator >>= (Vec8s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8sb operator == (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10   // compact boolean vectors
    return _mm_cmpeq_epi16_mask(a, b);
#else
    return _mm_cmpeq_epi16(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec8sb operator != (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10   // compact boolean vectors
    return _mm_cmpneq_epi16_mask(a, b);
#else
    return Vec8sb(~(a == b));
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec8sb operator > (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10   // compact boolean vectors
    return  _mm_cmp_epi16_mask(a, b, 6);
#else
    return _mm_cmpgt_epi16(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec8sb operator < (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi16_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8sb operator >= (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10   // compact boolean vectors
    return  _mm_cmp_epi16_mask(a, b, 5);
#else
    return Vec8sb(~(b > a));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8sb operator <= (Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi16_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec8s operator & (Vec8s const a, Vec8s const b) {
    return Vec8s(Vec128b(a) & Vec128b(b));
}
static inline Vec8s operator && (Vec8s const a, Vec8s const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8s & operator &= (Vec8s & a, Vec8s const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8s operator | (Vec8s const a, Vec8s const b) {
    return Vec8s(Vec128b(a) | Vec128b(b));
}
static inline Vec8s operator || (Vec8s const a, Vec8s const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8s & operator |= (Vec8s & a, Vec8s const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8s operator ^ (Vec8s const a, Vec8s const b) {
    return Vec8s(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8s & operator ^= (Vec8s & a, Vec8s const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8s operator ~ (Vec8s const a) {
    return Vec8s(~Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8s select(Vec8sb const s, Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi16(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8s if_add(Vec8sb const f, Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi16(a, f, a, b);
#else
    return a + (Vec8s(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec8s if_sub(Vec8sb const f, Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi16(a, f, a, b);
#else
    return a - (Vec8s(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec8s if_mul(Vec8sb const f, Vec8s const a, Vec8s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi16(a, f, a, b);
#else
    return select(f, a * b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int16_t horizontal_add(Vec8s const a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epi16(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    int16_t sum4 = _mm_cvtsi128_si32(sum3);                // truncate to 16 bits
    return  sum4;                                          // sign extend to 32 bits
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
#elif  INSTRSET >= 4 && false // SSSE3
    __m128i sum1 = _mm_hadd_epi16(a, a);                   // horizontally add 8 elements in 3 steps
    __m128i sum2 = _mm_hadd_epi16(sum1, sum1);
    __m128i sum3 = _mm_hadd_epi16(sum2, sum2);
    int16_t sum4 = (int16_t)_mm_cvtsi128_si32(sum3);       // 16 bit sum
    return  sum4;                                          // sign extend to 32 bits
#else                 // SSE2
    __m128i sum1 = _mm_unpackhi_epi64(a, a);               // 4 high elements
    __m128i sum2 = _mm_add_epi16(a, sum1);                 // 4 sums
    __m128i sum3 = _mm_shuffle_epi32(sum2, 0x01);          // 2 high elements
    __m128i sum4 = _mm_add_epi16(sum2, sum3);              // 2 sums
    __m128i sum5 = _mm_shufflelo_epi16(sum4, 0x01);        // 1 high element
    __m128i sum6 = _mm_add_epi16(sum4, sum5);              // 1 sum
    int16_t sum7 = (int16_t)_mm_cvtsi128_si32(sum6);       // 16 bit sum
    return  sum7;                                          // sign extend to 32 bits
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x(Vec8s const a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epi16(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);
#else
    __m128i aeven = _mm_slli_epi32(a, 16);                 // even numbered elements of a. get sign bit in position
    aeven = _mm_srai_epi32(aeven, 16);                     // sign extend even numbered elements
    __m128i aodd = _mm_srai_epi32(a, 16);                  // sign extend odd  numbered elements
    __m128i sum1 = _mm_add_epi32(aeven, aodd);             // add even and odd elements
#if  INSTRSET >= 4 && false // SSSE3
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i sum2 = _mm_hadd_epi32(sum1, sum1);             // horizontally add 4 elements in 2 steps
    __m128i sum3 = _mm_hadd_epi32(sum2, sum2);
    return  _mm_cvtsi128_si32(sum3);
#else                 // SSE2
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);         // 2 high elements
    __m128i sum3 = _mm_add_epi32(sum1, sum2);
    __m128i sum4 = _mm_shuffle_epi32(sum3, 1);             // 1 high elements
    __m128i sum5 = _mm_add_epi32(sum3, sum4);
    return  _mm_cvtsi128_si32(sum5);                       // 32 bit sum
#endif
#endif
}

// function add_saturated: add element by element, signed with saturation
static inline Vec8s add_saturated(Vec8s const a, Vec8s const b) {
    return _mm_adds_epi16(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8s sub_saturated(Vec8s const a, Vec8s const b) {
    return _mm_subs_epi16(a, b);
}

// function max: a > b ? a : b
static inline Vec8s max(Vec8s const a, Vec8s const b) {
    return _mm_max_epi16(a, b);
}

// function min: a < b ? a : b
static inline Vec8s min(Vec8s const a, Vec8s const b) {
    return _mm_min_epi16(a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8s abs(Vec8s const a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_abs_epi16(a);
#else                 // SSE2
    __m128i nega = _mm_sub_epi16(_mm_setzero_si128(), a);
    return _mm_max_epi16(a, nega);
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8s abs_saturated(Vec8s const a) {
#if INSTRSET >= 10
    return _mm_min_epu16(abs(a), Vec8s(0x7FFF));
#else
    __m128i absa = abs(a);                                 // abs(a)
    __m128i overfl = _mm_srai_epi16(absa, 15);             // sign
    return           _mm_add_epi16(absa, overfl);          // subtract 1 if 0x8000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8s rotate_left(Vec8s const a, int b) {
#ifdef __XOP__  // AMD XOP instruction set
    return (Vec8s)_mm_rot_epi16(a, _mm_set1_epi16(b));
#else  // SSE2 instruction set
    __m128i left  = _mm_sll_epi16(a, _mm_cvtsi32_si128(b & 0x0F));     // a << b 
    __m128i right = _mm_srl_epi16(a, _mm_cvtsi32_si128((-b) & 0x0F));  // a >> (16 - b)
    __m128i rot   = _mm_or_si128(left, right);                         // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 8 16-bit unsigned integers
*
*****************************************************************************/

class Vec8us : public Vec8s {
public:
    // Default constructor:
    Vec8us() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8us(uint32_t i) {
        xmm = _mm_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec8us(uint16_t i0, uint16_t i1, uint16_t i2, uint16_t i3, uint16_t i4, uint16_t i5, uint16_t i6, uint16_t i7) {
        xmm = _mm_setr_epi16((int16_t)i0, (int16_t)i1, (int16_t)i2, (int16_t)i3, (int16_t)i4, (int16_t)i5, (int16_t)i6, (int16_t)i7);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec8us(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec8us & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8us & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec8us & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec8us const insert(int index, uint16_t value) {
        Vec8s::insert(index, (int16_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint16_t extract(int index) const {
        return (uint16_t)Vec8s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int elementtype() {
        return 7;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8us operator + (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec8s(a) + Vec8s(b));
}

// vector operator - : subtract
static inline Vec8us operator - (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec8s(a) - Vec8s(b));
}

// vector operator * : multiply
static inline Vec8us operator * (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec8s(a) * Vec8s(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec8us operator >> (Vec8us const a, uint32_t b) {
    return _mm_srl_epi16(a, _mm_cvtsi32_si128((int)b));
}

// vector operator >> : shift right logical all elements
static inline Vec8us operator >> (Vec8us const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec8us & operator >>= (Vec8us & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec8us operator << (Vec8us const a, uint32_t b) {
    return _mm_sll_epi16(a, _mm_cvtsi32_si128((int)b));
}

// vector operator << : shift left all elements
static inline Vec8us operator << (Vec8us const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8sb operator >= (Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu16_mask(a, b, 5);
#elif defined (__XOP__)  // AMD XOP instruction set
    return (Vec8sb)_mm_comge_epu16(a, b);
#elif INSTRSET >= 5   // SSE4.1
    __m128i max_ab = _mm_max_epu16(a, b);                   // max(a,b), unsigned
    return _mm_cmpeq_epi16(a, max_ab);                      // a == max(a,b)
#else  // SSE2 instruction set
    __m128i s = _mm_subs_epu16(b, a);                       // b-a, saturated
    return  _mm_cmpeq_epi16(s, _mm_setzero_si128());       // s == 0 
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8sb operator <= (Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu16_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8sb operator > (Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu16_mask(a, b, 6);
#elif defined (__XOP__)  // AMD XOP instruction set
    return (Vec8sb)_mm_comgt_epu16(a, b);
#else  // SSE2 instruction set
    return Vec8sb(~(b >= a));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8sb operator < (Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu16_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator & : bitwise and
static inline Vec8us operator & (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec128b(a) & Vec128b(b));
}
static inline Vec8us operator && (Vec8us const a, Vec8us const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8us operator | (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec128b(a) | Vec128b(b));
}
static inline Vec8us operator || (Vec8us const a, Vec8us const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8us operator ^ (Vec8us const a, Vec8us const b) {
    return Vec8us(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec8us operator ~ (Vec8us const a) {
    return Vec8us(~Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8us select(Vec8sb const s, Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi16(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8us if_add(Vec8sb const f, Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi16(a, f, a, b);
#else
    return a + (Vec8us(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec8us if_sub(Vec8sb const f, Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi16(a, f, a, b);
#else
    return a - (Vec8us(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec8us if_mul(Vec8sb const f, Vec8us const a, Vec8us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi16(a, f, a, b);
#else
    return select(f, a * b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements.
// Overflow will wrap around
static inline uint32_t horizontal_add(Vec8us const a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epu16(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    uint16_t sum4 = _mm_cvtsi128_si32(sum3);               // truncate to 16 bits
    return  sum4;                                          // zero extend to 32 bits
#elif  INSTRSET >= 4 && false // SSSE3
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i sum1 = _mm_hadd_epi16(a, a);                   // horizontally add 8 elements in 3 steps
    __m128i sum2 = _mm_hadd_epi16(sum1, sum1);
    __m128i sum3 = _mm_hadd_epi16(sum2, sum2);
    uint16_t sum4 = (uint16_t)_mm_cvtsi128_si32(sum3);     // 16 bit sum
    return  sum4;                                          // zero extend to 32 bits
#else                 // SSE2
    __m128i sum1 = _mm_unpackhi_epi64(a, a);               // 4 high elements
    __m128i sum2 = _mm_add_epi16(a, sum1);                 // 4 sums
    __m128i sum3 = _mm_shuffle_epi32(sum2, 0x01);          // 2 high elements
    __m128i sum4 = _mm_add_epi16(sum2, sum3);              // 2 sums
    __m128i sum5 = _mm_shufflelo_epi16(sum4, 0x01);        // 1 high element
    __m128i sum6 = _mm_add_epi16(sum4, sum5);              // 1 sum
    uint16_t sum7 = (uint16_t)_mm_cvtsi128_si32(sum6);     // 16 bit sum
    return  sum7;                                          // zero extend to 32 bits
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x(Vec8us const a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epu16(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    return  (uint32_t)_mm_cvtsi128_si32(sum3);
    /*
#elif INSTRSET >= 4  // SSSE3
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i mask  = _mm_set1_epi32(0x0000FFFF);            // mask for even positions
    __m128i aeven = _mm_and_si128(a,mask);                 // even numbered elements of a
    __m128i aodd  = _mm_srli_epi32(a,16);                  // zero extend odd numbered elements
    __m128i sum1  = _mm_add_epi32(aeven,aodd);             // add even and odd elements
    __m128i sum2  = _mm_hadd_epi32(sum1,sum1);             // horizontally add 4 elements in 2 steps
    __m128i sum3  = _mm_hadd_epi32(sum2,sum2);
    return  (uint32_t)_mm_cvtsi128_si32(sum3);
    */
#else                 // SSE2
#if INSTRSET >= 10  // AVX512VL + AVX512BW
    __m128i aeven = _mm_maskz_mov_epi16(0x55, a);
#else
    __m128i mask  = _mm_set1_epi32(0x0000FFFF);            // mask for even positions
    __m128i aeven = _mm_and_si128(a, mask);                // even numbered elements of a
#endif    
    __m128i aodd = _mm_srli_epi32(a, 16);                  // zero extend odd numbered elements
    __m128i sum1 = _mm_add_epi32(aeven, aodd);             // add even and odd elements
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);         // 2 high elements
    __m128i sum3 = _mm_add_epi32(sum1, sum2);
    __m128i sum4 = _mm_shuffle_epi32(sum3, 0x01);          // 1 high elements
    __m128i sum5 = _mm_add_epi32(sum3, sum4);
    return  (uint32_t)_mm_cvtsi128_si32(sum5);             // 16 bit sum
#endif
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8us add_saturated(Vec8us const a, Vec8us const b) {
    return _mm_adds_epu16(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8us sub_saturated(Vec8us const a, Vec8us const b) {
    return _mm_subs_epu16(a, b);
}

// function max: a > b ? a : b
static inline Vec8us max(Vec8us const a, Vec8us const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epu16(a, b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80008000);
    __m128i a1 = _mm_xor_si128(a, signbit);                // add 0x8000
    __m128i b1 = _mm_xor_si128(b, signbit);                // add 0x8000
    __m128i m1 = _mm_max_epi16(a1, b1);                    // signed max
    return  _mm_xor_si128(m1, signbit);                    // sub 0x8000
#endif
}

// function min: a < b ? a : b
static inline Vec8us min(Vec8us const a, Vec8us const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epu16(a, b);
#else  // SSE2
    __m128i signbit = _mm_set1_epi32(0x80008000);
    __m128i a1 = _mm_xor_si128(a, signbit);                // add 0x8000
    __m128i b1 = _mm_xor_si128(b, signbit);                // add 0x8000
    __m128i m1 = _mm_min_epi16(a1, b1);                    // signed min
    return  _mm_xor_si128(m1, signbit);                    // sub 0x8000
#endif
}


/*****************************************************************************
*
*          Vector of 4 32-bit signed integers
*
*****************************************************************************/

class Vec4i : public Vec128b {
public:
    // Default constructor:
    Vec4i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4i(int i) {
        xmm = _mm_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec4i(int32_t i0, int32_t i1, int32_t i2, int32_t i3) {
        xmm = _mm_setr_epi32(i0, i1, i2, i3);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4i(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4i & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec4i & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec4i & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4i & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        xmm = _mm_maskz_loadu_epi32(__mmask8((1u << n) - 1), p);
#else
        switch (n) {
        case 0:
            *this = 0;  break;
        case 1:
            xmm = _mm_cvtsi32_si128(*(int32_t const*)p);  break;
        case 2:
            // intrinsic for movq is missing!
            xmm = _mm_setr_epi32(((int32_t const*)p)[0], ((int32_t const*)p)[1], 0, 0);  break;
        case 3:
            xmm = _mm_setr_epi32(((int32_t const*)p)[0], ((int32_t const*)p)[1], ((int32_t const*)p)[2], 0);  break;
        case 4:
            load(p);  break;
        default:
            break;
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm_mask_storeu_epi32(p, __mmask8((1u << n) - 1), xmm);
#else 
        union {
            int32_t i[4];
            int64_t q[2];
        } u;
        switch (n) {
        case 1:
            *(int32_t*)p = _mm_cvtsi128_si32(xmm);  break;
        case 2:
            // intrinsic for movq is missing!
            store(u.i);
            *(int64_t*)p = u.q[0];  break;
        case 3:
            store(u.i);
            *(int64_t*)p = u.q[0];
            ((int32_t*)p)[2] = u.i[2];  break;
        case 4:
            store(p);  break;
        default:
            break;
        }
#endif
    }

    // cut off vector to n elements. The last 4-n elements are set to zero
    Vec4i & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_epi32(__mmask8((1u << n) - 1), xmm);
#else 
        * this = Vec16c(xmm).cutoff(n * 4);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec4i const insert(int index, int32_t value) {
#if INSTRSET >= 10
        xmm = _mm_mask_set1_epi32(xmm, __mmask8(1u << index), value);
#else
        __m128i broad = _mm_set1_epi32(value);  // broadcast value into all elements
        const int32_t maskl[8] = { 0,0,0,0,-1,0,0,0 };
        __m128i mask = _mm_loadu_si128((__m128i const*)(maskl + 4 - (index & 3))); // mask with FFFFFFFF at index position
        xmm = selectb(mask, broad, xmm);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(int index) const {
#if INSTRSET >= 10
        __m128i x = _mm_maskz_compress_epi32(__mmask8(1u << index), xmm);
        return _mm_cvtsi128_si32(x);
#else
        int32_t x[4];
        store(x);
        return x[index & 3];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 8;
    }
};


/*****************************************************************************
*
*          Vec4ib: Vector of 4 Booleans for use with Vec4i and Vec4ui
*
*****************************************************************************/
#if INSTRSET < 10   // broad boolean vectors

class Vec4ib : public Vec4i {
public:
    // Default constructor:
    Vec4ib() {
    }
    // Constructor to build from all elements:
    Vec4ib(bool x0, bool x1, bool x2, bool x3) {
        xmm = Vec4i(-int32_t(x0), -int32_t(x1), -int32_t(x2), -int32_t(x3));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4ib(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4ib & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec4ib(bool b) : Vec4i(-int32_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec4ib & operator = (bool b) {
        *this = Vec4ib(b);
        return *this;
    }
    Vec4ib & insert(int index, bool a) {
        Vec4i::insert(index, -(int)a);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec4i::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec4ib & load_bits(uint8_t a) {
        __m128i b1 = _mm_set1_epi8((int8_t)a);  // broadcast byte
        __m128i m1 = constant4ui<1, 2, 4, 8>();
        __m128i c1 = _mm_and_si128(b1, m1); // isolate one bit in each byte
        xmm = _mm_cmpgt_epi32(c1, _mm_setzero_si128());  // compare signed because no numbers are negative
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec4ib(int b) = delete;
    Vec4ib & operator = (int x) = delete;
};

#else

typedef Vec4b Vec4ib;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Define operators for Vec4ib
*
*****************************************************************************/

#if INSTRSET < 10   // broad boolean vectors

// vector operator & : bitwise and
static inline Vec4ib operator & (Vec4ib const a, Vec4ib const b) {
    return Vec4ib(Vec128b(a) & Vec128b(b));
}
static inline Vec4ib operator && (Vec4ib const a, Vec4ib const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4ib & operator &= (Vec4ib & a, Vec4ib const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4ib operator | (Vec4ib const a, Vec4ib const b) {
    return Vec4ib(Vec128b(a) | Vec128b(b));
}
static inline Vec4ib operator || (Vec4ib const a, Vec4ib const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4ib & operator |= (Vec4ib & a, Vec4ib const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4ib operator ^ (Vec4ib const a, Vec4ib const b) {
    return Vec4ib(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4ib & operator ^= (Vec4ib & a, Vec4ib const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec4ib operator == (Vec4ib const a, Vec4ib const b) {
    return Vec4ib(a ^ (~b));
}

// vector operator != : xor
static inline Vec4ib operator != (Vec4ib const a, Vec4ib const b) {
    return Vec4ib(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec4ib operator ~ (Vec4ib const a) {
    return Vec4ib(~Vec128b(a));
}

// vector operator ! : element not
static inline Vec4ib operator ! (Vec4ib const a) {
    return ~a;
}

// vector function andnot
static inline Vec4ib andnot(Vec4ib const a, Vec4ib const b) {
    return Vec4ib(andnot(Vec128b(a), Vec128b(b)));
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec4ib const a) {
    return _mm_movemask_epi8(a) == 0xFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec4ib const a) {
#if INSTRSET >= 5   // SSE4.1 supported. Use PTEST
    return !_mm_testz_si128(a, a);
#else
    return _mm_movemask_epi8(a) != 0;
#endif
}
#endif  // broad boolean vectors


/*****************************************************************************
*
*          Operators for Vec4i
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4i operator + (Vec4i const a, Vec4i const b) {
    return _mm_add_epi32(a, b);
}
// vector operator += : add
static inline Vec4i & operator += (Vec4i & a, Vec4i const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4i operator ++ (Vec4i & a, int) {
    Vec4i a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec4i & operator ++ (Vec4i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4i operator - (Vec4i const a, Vec4i const b) {
    return _mm_sub_epi32(a, b);
}
// vector operator - : unary minus
static inline Vec4i operator - (Vec4i const a) {
    return _mm_sub_epi32(_mm_setzero_si128(), a);
}
// vector operator -= : subtract
static inline Vec4i & operator -= (Vec4i & a, Vec4i const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4i operator -- (Vec4i & a, int) {
    Vec4i a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec4i & operator -- (Vec4i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4i operator * (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 5  // SSE4.1 instruction set
    return _mm_mullo_epi32(a, b);
#else
    __m128i a13 = _mm_shuffle_epi32(a, 0xF5);              // (-,a3,-,a1)
    __m128i b13 = _mm_shuffle_epi32(b, 0xF5);              // (-,b3,-,b1)
    __m128i prod02 = _mm_mul_epu32(a, b);                  // (-,a2*b2,-,a0*b0)
    __m128i prod13 = _mm_mul_epu32(a13, b13);              // (-,a3*b3,-,a1*b1)
    __m128i prod01 = _mm_unpacklo_epi32(prod02, prod13);   // (-,-,a1*b1,a0*b0) 
    __m128i prod23 = _mm_unpackhi_epi32(prod02, prod13);   // (-,-,a3*b3,a2*b2) 
    return           _mm_unpacklo_epi64(prod01, prod23);   // (ab3,ab2,ab1,ab0)
#endif
}

// vector operator *= : multiply
static inline Vec4i & operator *= (Vec4i & a, Vec4i const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec4i operator << (Vec4i const a, int32_t b) {
    return _mm_sll_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec4i & operator <<= (Vec4i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4i operator >> (Vec4i const a, int32_t b) {
    return _mm_sra_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec4i & operator >>= (Vec4i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4ib operator == (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 0);
#else
    return _mm_cmpeq_epi32(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec4ib operator != (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 4);
#else
    return Vec4ib(Vec4i(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec4ib operator > (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 6);
#else
    return _mm_cmpgt_epi32(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec4ib operator < (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4ib operator >= (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 5);
#else
    return Vec4ib(Vec4i(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4ib operator <= (Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec4i operator & (Vec4i const a, Vec4i const b) {
    return Vec4i(Vec128b(a) & Vec128b(b));
}
static inline Vec4i operator && (Vec4i const a, Vec4i const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4i & operator &= (Vec4i & a, Vec4i const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4i operator | (Vec4i const a, Vec4i const b) {
    return Vec4i(Vec128b(a) | Vec128b(b));
}
static inline Vec4i operator || (Vec4i const a, Vec4i const b) {
    return a | b;
}
// vector operator |= : bitwise and
static inline Vec4i & operator |= (Vec4i & a, Vec4i const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4i operator ^ (Vec4i const a, Vec4i const b) {
    return Vec4i(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise and
static inline Vec4i & operator ^= (Vec4i & a, Vec4i const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4i operator ~ (Vec4i const a) {
    return Vec4i(~Vec128b(a));
}

// vector operator ! : returns true for elements == 0
static inline Vec4ib operator ! (Vec4i const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi32_mask(a, _mm_setzero_si128(), 0);
#else
    return _mm_cmpeq_epi32(a, _mm_setzero_si128());
#endif
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4i select(Vec4ib const s, Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi32(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4i if_add(Vec4ib const f, Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi32(a, f, a, b);
#else
    return a + (Vec4i(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec4i if_sub(Vec4ib const f, Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi32(a, f, a, b);
#else
    return a - (Vec4i(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec4i if_mul(Vec4ib const f, Vec4i const a, Vec4i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi32(a, f, a, b);
#else
    return select(f, a * b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add(Vec4i const a) {
#ifdef __XOP__       // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epi32(a);
    __m128i sum2 = _mm_shuffle_epi32(sum1, 0x0E);          // high element
    __m128i sum3 = _mm_add_epi32(sum1, sum2);              // sum
    return          _mm_cvtsi128_si32(sum3);               // truncate to 32 bits
#elif  INSTRSET >= 4 & false // SSSE3
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i sum1 = _mm_hadd_epi32(a, a);                   // horizontally add 4 elements in 2 steps
    __m128i sum2 = _mm_hadd_epi32(sum1, sum1);
    return          _mm_cvtsi128_si32(sum2);               // 32 bit sum
#else                 // SSE2
    __m128i sum1 = _mm_unpackhi_epi64(a, a);               // 2 high elements
    __m128i sum2 = _mm_add_epi32(a, sum1);                 // 2 sums
    __m128i sum3 = _mm_shuffle_epi32(sum2, 0x01);          // 1 high element
    __m128i sum4 = _mm_add_epi32(sum2, sum3);              // 2 sums
    return          _mm_cvtsi128_si32(sum4);               // 32 bit sum
#endif
}

// function used below
static inline int64_t _emulate_movq(__m128i const x) {
#ifdef __x86_64__
    return _mm_cvtsi128_si64(x);
#else
    // 64 bit registers not available
    union {
        __m128i m;
        int64_t y;
    } u;
    _mm_storel_epi64(&u.m, x);
    return u.y;
#endif
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x(Vec4i const a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epi32(a);
#else              // SSE2
    __m128i signs = _mm_srai_epi32(a, 31);                 // sign of all elements
    __m128i a01 = _mm_unpacklo_epi32(a, signs);            // sign-extended a0, a1
    __m128i a23 = _mm_unpackhi_epi32(a, signs);            // sign-extended a2, a3
    __m128i sum1 = _mm_add_epi64(a01, a23);                // add
#endif
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);         // high qword
    __m128i sum3 = _mm_add_epi64(sum1, sum2);              // add
    return _emulate_movq(sum3);
}

// function add_saturated: add element by element, signed with saturation
static inline Vec4i add_saturated(Vec4i const a, Vec4i const b) {
    // is there a faster method?
    __m128i sum = _mm_add_epi32(a, b);                     // a + b
    __m128i axb = _mm_xor_si128(a, b);                     // check if a and b have different sign
    __m128i axs = _mm_xor_si128(a, sum);                   // check if a and sum have different sign
    __m128i overf1 = _mm_andnot_si128(axb, axs);           // check if sum has wrong sign
    __m128i overf2 = _mm_srai_epi32(overf1, 31);           // -1 if overflow
    __m128i asign = _mm_srli_epi32(a, 31);                 // 1  if a < 0
    __m128i sat1 = _mm_srli_epi32(overf2, 1);              // 7FFFFFFF if overflow
    __m128i sat2 = _mm_add_epi32(sat1, asign);             // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2, sat2, sum);                    // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec4i sub_saturated(Vec4i const a, Vec4i const b) {
    __m128i diff = _mm_sub_epi32(a, b);                    // a + b
    __m128i axb = _mm_xor_si128(a, b);                     // check if a and b have different sign
    __m128i axs = _mm_xor_si128(a, diff);                  // check if a and sum have different sign
    __m128i overf1 = _mm_and_si128(axb, axs);              // check if sum has wrong sign
    __m128i overf2 = _mm_srai_epi32(overf1, 31);           // -1 if overflow
    __m128i asign = _mm_srli_epi32(a, 31);                 // 1  if a < 0
    __m128i sat1 = _mm_srli_epi32(overf2, 1);              // 7FFFFFFF if overflow
    __m128i sat2 = _mm_add_epi32(sat1, asign);             // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2, sat2, diff);                   // diff if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec4i max(Vec4i const a, Vec4i const b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_max_epi32(a, b);
#else
    __m128i greater = _mm_cmpgt_epi32(a, b);
    return selectb(greater, a, b);
#endif
}

// function min: a < b ? a : b
static inline Vec4i min(Vec4i const a, Vec4i const b) {
#if INSTRSET >= 5   // SSE4.1 supported
    return _mm_min_epi32(a, b);
#else
    __m128i greater = _mm_cmpgt_epi32(a, b);
    return selectb(greater, b, a);
#endif
}

// function abs: a >= 0 ? a : -a
static inline Vec4i abs(Vec4i const a) {
#if INSTRSET >= 4     // SSSE3 supported
    return _mm_abs_epi32(a);
#else                 // SSE2
    __m128i sign = _mm_srai_epi32(a, 31);                  // sign of a
    __m128i inv = _mm_xor_si128(a, sign);                  // invert bits if negative
    return         _mm_sub_epi32(inv, sign);               // add 1
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4i abs_saturated(Vec4i const a) {
#if INSTRSET >= 10
    return _mm_min_epu32(abs(a), Vec4i(0x7FFFFFFF));
#else
    __m128i absa = abs(a);                                 // abs(a)
    __m128i overfl = _mm_srai_epi32(absa, 31);             // sign
    return           _mm_add_epi32(absa, overfl);          // subtract 1 if 0x80000000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4i rotate_left(Vec4i const a, int b) {
#if INSTRSET >= 10  // __AVX512VL__
    return _mm_rolv_epi32(a, _mm_set1_epi32(b));
#elif defined __XOP__  // AMD XOP instruction set
    return _mm_rot_epi32(a, _mm_set1_epi32(b));
#else  // SSE2 instruction set
    __m128i left = _mm_sll_epi32(a, _mm_cvtsi32_si128(b & 0x1F));    // a << b 
    __m128i right = _mm_srl_epi32(a, _mm_cvtsi32_si128((-b) & 0x1F));// a >> (32 - b)
    __m128i rot = _mm_or_si128(left, right);                         // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 4 32-bit unsigned integers
*
*****************************************************************************/

class Vec4ui : public Vec4i {
public:
    // Default constructor:
    Vec4ui() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4ui(uint32_t i) {
        xmm = _mm_set1_epi32((int32_t)i);
    }
    // Constructor to build from all elements:
    Vec4ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3) {
        xmm = _mm_setr_epi32((int32_t)i0, (int32_t)i1, (int32_t)i2, (int32_t)i3);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec4ui(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec4ui & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4ui & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec4ui & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec4ui const insert(int index, uint32_t value) {
        Vec4i::insert(index, (int32_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(int index) const {
        return (uint32_t)Vec4i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int elementtype() {
        return 9;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec4ui operator + (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec4i(a) + Vec4i(b));
}

// vector operator - : subtract
static inline Vec4ui operator - (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec4i(a) - Vec4i(b));
}

// vector operator * : multiply
static inline Vec4ui operator * (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec4i(a) * Vec4i(b));
}

// vector operator / : divide. See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec4ui operator >> (Vec4ui const a, uint32_t b) {
    return _mm_srl_epi32(a, _mm_cvtsi32_si128((int)b));
}
// vector operator >> : shift right logical all elements
static inline Vec4ui operator >> (Vec4ui const a, int32_t b) {
    return a >> (uint32_t)b;
}
// vector operator >>= : shift right logical
static inline Vec4ui & operator >>= (Vec4ui & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec4ui operator << (Vec4ui const a, uint32_t b) {
    return Vec4ui((Vec4i)a << (int32_t)b);
} 
// vector operator << : shift left all elements
static inline Vec4ui operator << (Vec4ui const a, int32_t b) {
    return Vec4ui((Vec4i)a << (int32_t)b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec4ib operator > (Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu32_mask(a, b, 6);
#elif defined (__XOP__)  // AMD XOP instruction set
    return (Vec4ib)_mm_comgt_epu32(a, b);
#else  // SSE2 instruction set
    __m128i signbit = _mm_set1_epi32(0x80000000);
    __m128i a1 = _mm_xor_si128(a, signbit);
    __m128i b1 = _mm_xor_si128(b, signbit);
    return (Vec4ib)_mm_cmpgt_epi32(a1, b1);                // signed compare
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4ib operator < (Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu32_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4ib operator >= (Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu32_mask(a, b, 5);
#else
#ifdef __XOP__  // AMD XOP instruction set
    return (Vec4ib)_mm_comge_epu32(a, b);
#elif INSTRSET >= 5   // SSE4.1
    __m128i max_ab = _mm_max_epu32(a, b);                  // max(a,b), unsigned
    return (Vec4ib)_mm_cmpeq_epi32(a, max_ab);             // a == max(a,b)
#else  // SSE2 instruction set
    return Vec4ib(Vec4i(~(b > a)));
#endif
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4ib operator <= (Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu32_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec4ui operator & (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec128b(a) & Vec128b(b));
}
static inline Vec4ui operator && (Vec4ui const a, Vec4ui const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4ui operator | (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec128b(a) | Vec128b(b));
}
static inline Vec4ui operator || (Vec4ui const a, Vec4ui const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4ui operator ^ (Vec4ui const a, Vec4ui const b) {
    return Vec4ui(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec4ui operator ~ (Vec4ui const a) {
    return Vec4ui(~Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec4ui select(Vec4ib const s, Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi32(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4ui if_add(Vec4ib const f, Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi32(a, f, a, b);
#else
    return a + (Vec4ui(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec4ui if_sub(Vec4ib const f, Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi32(a, f, a, b);
#else
    return a - (Vec4ui(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec4ui if_mul(Vec4ib const f, Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi32(a, f, a, b);
#else
    return select(f, a * b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add(Vec4ui const a) {
    return (uint32_t)horizontal_add((Vec4i)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x(Vec4ui const a) {
#ifdef __XOP__     // AMD XOP instruction set
    __m128i sum1 = _mm_haddq_epu32(a);
#else              // SSE2
    __m128i zero = _mm_setzero_si128();                    // 0
    __m128i a01  = _mm_unpacklo_epi32(a, zero);            // zero-extended a0, a1
    __m128i a23  = _mm_unpackhi_epi32(a, zero);            // zero-extended a2, a3
    __m128i sum1 = _mm_add_epi64(a01, a23);                // add
#endif
    __m128i sum2 = _mm_unpackhi_epi64(sum1, sum1);         // high qword
    __m128i sum3 = _mm_add_epi64(sum1, sum2);              // add
    return (uint64_t)_emulate_movq(sum3);
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec4ui add_saturated(Vec4ui const a, Vec4ui const b) {
    Vec4ui sum = a + b;
    Vec4ui aorb = Vec4ui(a | b);
#if INSTRSET >= 10
    Vec4b  overflow = _mm_cmp_epu32_mask(sum, aorb, 1);
    return _mm_mask_set1_epi32(sum, overflow, -1);
#else
    Vec4ui overflow = Vec4ui(sum < aorb);                  // overflow if a + b < (a | b)
    return Vec4ui(sum | overflow);                         // return 0xFFFFFFFF if overflow
#endif
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec4ui sub_saturated(Vec4ui const a, Vec4ui const b) {
    Vec4ui diff = a - b;
#if INSTRSET >= 10
    Vec4b  nunderflow = _mm_cmp_epu32_mask(diff, a, 2);    // not underflow if a - b <= a
    return _mm_maskz_mov_epi32(nunderflow, diff);          // zero if underflow
#else
    Vec4ui underflow = Vec4ui(diff > a);                   // underflow if a - b > a
    return _mm_andnot_si128(underflow, diff);              // return 0 if underflow
#endif
}

// function max: a > b ? a : b
static inline Vec4ui max(Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_max_epu32(a, b);
#else  // SSE2
    return select(a > b, a, b);
#endif
}

// function min: a < b ? a : b
static inline Vec4ui min(Vec4ui const a, Vec4ui const b) {
#if INSTRSET >= 5   // SSE4.1
    return _mm_min_epu32(a, b);
#else  // SSE2
    return select(a > b, b, a);
#endif
}


/*****************************************************************************
*
*          Vector of 2 64-bit signed integers
*
*****************************************************************************/

class Vec2q : public Vec128b {
public:
    // Default constructor:
    Vec2q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec2q(int64_t i) {
        xmm = _mm_set1_epi64x(i);
    }
    // Constructor to build from all elements:
    Vec2q(int64_t i0, int64_t i1) {
        xmm = _mm_set_epi64x(i1, i0);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2q(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2q & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Type cast operator to convert to __m128i used in intrinsics
    operator __m128i() const {
        return xmm;
    }
    // Member function to load from array (unaligned)
    Vec2q & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec2q & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec2q & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        xmm = _mm_maskz_loadu_epi64(__mmask8((1u << n) - 1), p);
#else
        switch (n) {
        case 0:
            *this = 0;  break;
        case 1:
            // intrinsic for movq is missing!
            *this = Vec2q(*(int64_t const*)p, 0);  break;
        case 2:
            load(p);  break;
        default:
            break;
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm_mask_storeu_epi64(p, __mmask8((1u << n) - 1), xmm);
#else 
        switch (n) {
        case 1:
            int64_t q[2];
            store(q);
            *(int64_t*)p = q[0];  break;
        case 2:
            store(p);  break;
        default:
            break;
        }
#endif
    }
    // cut off vector to n elements. The last 2-n elements are set to zero
    Vec2q & cutoff(int n) {
#if INSTRSET >= 10 
        xmm = _mm_maskz_mov_epi64(__mmask8((1u << n) - 1), xmm);
#else 
        *this = Vec16c(xmm).cutoff(n * 8);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec2q const insert(int index, int64_t value) {
#if INSTRSET >= 10
        xmm = _mm_mask_set1_epi64(xmm, __mmask8(1u << index), value);
#elif INSTRSET >= 5 && defined(__x86_64__)  // SSE4.1 supported, 64 bit mode
        if (index == 0) {
            xmm = _mm_insert_epi64(xmm, value, 0);
        }
        else {
            xmm = _mm_insert_epi64(xmm, value, 1);
        }
#else               // SSE2
#if defined(__x86_64__)                                    // 64 bit mode
        __m128i v = _mm_cvtsi64_si128(value);              // 64 bit load
#else
        union {
            __m128i m;
            int64_t ii;
        } u;
        u.ii = value;
        __m128i v = _mm_loadl_epi64(&u.m);
#endif
        if (index == 0) {
            v = _mm_unpacklo_epi64(v, v);
            xmm = _mm_unpackhi_epi64(v, xmm);
        }
        else {  // index = 1
            xmm = _mm_unpacklo_epi64(xmm, v);
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(int index) const {
#if INSTRSET >= 10
        __m128i x = _mm_mask_unpackhi_epi64(xmm, __mmask8(index), xmm, xmm);
        return _emulate_movq(x);
#else
        int64_t x[2];
        store(x);
        return x[index & 1];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 2;
    }
    static constexpr int elementtype() {
        return 10;
    }
};


/*****************************************************************************
*
*          Vec2qb: Vector of 2 Booleans for use with Vec2q and Vec2uq
*
*****************************************************************************/

#if INSTRSET < 10   // broad boolean vectors

// Definition will be different for the AVX512 instruction set
class Vec2qb : public Vec2q {
public:
    // Default constructor:
    Vec2qb() {
    }
    // Constructor to build from all elements:
    Vec2qb(bool x0, bool x1) {
        xmm = Vec2q(-int64_t(x0), -int64_t(x1));
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2qb(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2qb & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec2qb(bool b) : Vec2q(-int64_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec2qb & operator = (bool b) {
        *this = Vec2qb(b);
        return *this;
    }
    Vec2qb & insert(int index, bool a) {
        Vec2q::insert(index, -(int64_t)a);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec2q::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec2qb & load_bits(uint8_t a) {
        __m128i b1 = _mm_set1_epi8((int8_t)a);  // broadcast byte
        __m128i m1 = constant4ui<1, 1, 2, 2>();
        __m128i c1 = _mm_and_si128(b1, m1); // isolate one bit in each byte
        xmm = _mm_cmpgt_epi32(c1, _mm_setzero_si128());  // compare with 0 (64 bit compare requires SSE4.1)
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec2qb(int b) = delete;
    Vec2qb & operator = (int x) = delete;
};

#else

typedef Vec2b Vec2qb;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Define operators for Vec2qb
*
*****************************************************************************/

#if INSTRSET < 10   // broad boolean vectors

// vector operator & : bitwise and
static inline Vec2qb operator & (Vec2qb const a, Vec2qb const b) {
    return Vec2qb(Vec128b(a) & Vec128b(b));
}
static inline Vec2qb operator && (Vec2qb const a, Vec2qb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec2qb & operator &= (Vec2qb & a, Vec2qb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec2qb operator | (Vec2qb const a, Vec2qb const b) {
    return Vec2qb(Vec128b(a) | Vec128b(b));
}
static inline Vec2qb operator || (Vec2qb const a, Vec2qb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec2qb & operator |= (Vec2qb & a, Vec2qb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec2qb operator ^ (Vec2qb const a, Vec2qb const b) {
    return Vec2qb(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec2qb & operator ^= (Vec2qb & a, Vec2qb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec2qb operator == (Vec2qb const a, Vec2qb const b) {
    return Vec2qb(a ^ (~b));
}

// vector operator != : xor
static inline Vec2qb operator != (Vec2qb const a, Vec2qb const b) {
    return Vec2qb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec2qb operator ~ (Vec2qb const a) {
    return Vec2qb(~Vec128b(a));
}

// vector operator ! : element not
static inline Vec2qb operator ! (Vec2qb const a) {
    return ~a;
}

// vector function andnot
static inline Vec2qb andnot(Vec2qb const a, Vec2qb const b) {
    return Vec2qb(andnot(Vec128b(a), Vec128b(b)));
}

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec2qb const a) {
    return _mm_movemask_epi8(a) == 0xFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec2qb const a) {
#if INSTRSET >= 5   // SSE4.1 supported. Use PTEST
    return !_mm_testz_si128(a, a);
#else
    return _mm_movemask_epi8(a) != 0;
#endif
}

#endif     // broad boolean vectors



/*****************************************************************************
*
*          Operators for Vec2q
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec2q operator + (Vec2q const a, Vec2q const b) {
    return _mm_add_epi64(a, b);
}
// vector operator += : add
static inline Vec2q & operator += (Vec2q & a, Vec2q const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec2q operator ++ (Vec2q & a, int) {
    Vec2q a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec2q & operator ++ (Vec2q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec2q operator - (Vec2q const a, Vec2q const b) {
    return _mm_sub_epi64(a, b);
}
// vector operator - : unary minus
static inline Vec2q operator - (Vec2q const a) {
    return _mm_sub_epi64(_mm_setzero_si128(), a);
}
// vector operator -= : subtract
static inline Vec2q & operator -= (Vec2q & a, Vec2q const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec2q operator -- (Vec2q & a, int) {
    Vec2q a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec2q & operator -- (Vec2q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec2q operator * (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm_mullo_epi64(a, b);
#elif INSTRSET >= 5   // SSE4.1 supported
    // Split into 32-bit multiplies
    __m128i bswap = _mm_shuffle_epi32(b, 0xB1);            // b0H,b0L,b1H,b1L (swap H<->L)
    __m128i prodlh = _mm_mullo_epi32(a, bswap);            // a0Lb0H,a0Hb0L,a1Lb1H,a1Hb1L, 32 bit L*H products
    __m128i zero = _mm_setzero_si128();                    // 0
    __m128i prodlh2 = _mm_hadd_epi32(prodlh, zero);        // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
    __m128i prodlh3 = _mm_shuffle_epi32(prodlh2, 0x73);    // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
    __m128i prodll = _mm_mul_epu32(a, b);                  // a0Lb0L,a1Lb1L, 64 bit unsigned products
    __m128i prod = _mm_add_epi64(prodll, prodlh3);         // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
    return  prod;
#else               // SSE2
    int64_t aa[2], bb[2];
    a.store(aa);                                           // split into elements
    b.store(bb);
    return Vec2q(aa[0] * bb[0], aa[1] * bb[1]);            // multiply elements separetely
#endif
}

// vector operator *= : multiply
static inline Vec2q & operator *= (Vec2q & a, Vec2q const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec2q operator << (Vec2q const a, int32_t b) {
    return _mm_sll_epi64(a, _mm_cvtsi32_si128(b));
}

// vector operator <<= : shift left
static inline Vec2q & operator <<= (Vec2q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec2q operator >> (Vec2q const a, int32_t b) {
#if INSTRSET >= 10   // AVX512VL
    return _mm_sra_epi64(a, _mm_cvtsi32_si128(b));
#else
    __m128i bb, shi, slo, sra2;
    if (b <= 32) {
        bb = _mm_cvtsi32_si128(b);               // b
        shi = _mm_sra_epi32(a, bb);              // a >> b signed dwords
        slo = _mm_srl_epi64(a, bb);              // a >> b unsigned qwords
    }
    else {  // b > 32
        bb = _mm_cvtsi32_si128(b - 32);          // b - 32
        shi = _mm_srai_epi32(a, 31);             // sign of a
        sra2 = _mm_sra_epi32(a, bb);             // a >> (b-32) signed dwords
        slo = _mm_srli_epi64(sra2, 32);          // a >> (b-32) >> 32 (second shift unsigned qword)
    }
#if INSTRSET >= 5  // SSE4.1
    return _mm_blend_epi16(slo, shi, 0xCC);
#else
    __m128i mask = _mm_setr_epi32(0, -1, 0, -1); // mask for high part containing only sign
    return  selectb(mask, shi, slo);
#endif
#endif
}

// vector operator >>= : shift right arithmetic
static inline Vec2q & operator >>= (Vec2q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec2qb operator == (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epi64_mask(a, b, 0);
#elif INSTRSET >= 5   // SSE4.1 supported
    return _mm_cmpeq_epi64(a, b);
#else               // SSE2
    // no 64 compare instruction. Do two 32 bit compares
    __m128i com32 = _mm_cmpeq_epi32(a, b);                 // 32 bit compares
    __m128i com32s = _mm_shuffle_epi32(com32, 0xB1);       // swap low and high dwords
    __m128i test = _mm_and_si128(com32, com32s);           // low & high
    __m128i teste = _mm_srai_epi32(test, 31);              // extend sign bit to 32 bits
    __m128i testee = _mm_shuffle_epi32(teste, 0xF5);       // extend sign bit to 64 bits
    return  Vec2qb(Vec2q(testee));
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec2qb operator != (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epi64_mask(a, b, 4);
#elif defined (__XOP__)  // AMD XOP instruction set
    return Vec2qb(_mm_comneq_epi64(a, b));
#else  // SSE2 instruction set
    return Vec2qb(Vec2q(~(a == b)));
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec2qb operator < (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epi64_mask(a, b, 1);
#elif INSTRSET >= 6   // SSE4.2 supported
    return Vec2qb(Vec2q(_mm_cmpgt_epi64(b, a)));
#else               // SSE2
    // no 64 compare instruction. Subtract
    __m128i s = _mm_sub_epi64(a, b);                       // a-b
    // a < b if a and b have same sign and s < 0 or (a < 0 and b >= 0)
    // The latter () corrects for overflow
    __m128i axb = _mm_xor_si128(a, b);                     // a ^ b
    __m128i anb = _mm_andnot_si128(b, a);                  // a & ~b
    __m128i snaxb = _mm_andnot_si128(axb, s);              // s & ~(a ^ b)
    __m128i or1 = _mm_or_si128(anb, snaxb);                // (a & ~b) | (s & ~(a ^ b))
    __m128i teste = _mm_srai_epi32(or1, 31);               // extend sign bit to 32 bits
    __m128i testee = _mm_shuffle_epi32(teste, 0xF5);       // extend sign bit to 64 bits
    return  testee;
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec2qb operator > (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi64_mask(a, b, 6);
#else
    return b < a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec2qb operator >= (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epi64_mask(a, b, 5);
#elif defined (__XOP__)  // AMD XOP instruction set
    return Vec2qb(_mm_comge_epi64(a, b));
#else  // SSE2 instruction set
    return Vec2qb(Vec2q(~(a < b)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec2qb operator <= (Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epi64_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec2q operator & (Vec2q const a, Vec2q const b) {
    return Vec2q(Vec128b(a) & Vec128b(b));
}
static inline Vec2q operator && (Vec2q const a, Vec2q const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec2q & operator &= (Vec2q & a, Vec2q const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec2q operator | (Vec2q const a, Vec2q const b) {
    return Vec2q(Vec128b(a) | Vec128b(b));
}
static inline Vec2q operator || (Vec2q const a, Vec2q const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec2q & operator |= (Vec2q & a, Vec2q const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec2q operator ^ (Vec2q const a, Vec2q const b) {
    return Vec2q(Vec128b(a) ^ Vec128b(b));
}
// vector operator ^= : bitwise xor
static inline Vec2q & operator ^= (Vec2q & a, Vec2q const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec2q operator ~ (Vec2q const a) {
    return Vec2q(~Vec128b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec2qb operator ! (Vec2q const a) {
    return a == Vec2q(_mm_setzero_si128());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec2q select(Vec2qb const s, Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi64(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec2q if_add(Vec2qb const f, Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi64(a, f, a, b);
#else
    return a + (Vec2q(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec2q if_sub(Vec2qb const f, Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi64(a, f, a, b);
#else
    return a - (Vec2q(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec2q if_mul(Vec2qb const f, Vec2q const a, Vec2q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi64(a, f, a, b);
#else
    return select(f, a * b, a);
#endif    
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int64_t horizontal_add(Vec2q const a) {
    __m128i sum1 = _mm_unpackhi_epi64(a, a);               // high element
    __m128i sum2 = _mm_add_epi64(a, sum1);                 // sum
    return _emulate_movq(sum2);
}

// function max: a > b ? a : b
static inline Vec2q max(Vec2q const a, Vec2q const b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec2q min(Vec2q const a, Vec2q const b) {
    return select(a < b, a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec2q abs(Vec2q const a) {
#if INSTRSET >= 10     // AVX512VL
    return _mm_abs_epi64(a);
#elif INSTRSET >= 6     // SSE4.2 supported
    __m128i sign = _mm_cmpgt_epi64(_mm_setzero_si128(), a);// 0 > a
    __m128i inv = _mm_xor_si128(a, sign);                  // invert bits if negative
    return          _mm_sub_epi64(inv, sign);              // add 1
#else                 // SSE2
    __m128i signh = _mm_srai_epi32(a, 31);                 // sign in high dword
    __m128i sign = _mm_shuffle_epi32(signh, 0xF5);         // copy sign to low dword
    __m128i inv = _mm_xor_si128(a, sign);                  // invert bits if negative
    return          _mm_sub_epi64(inv, sign);              // add 1
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec2q abs_saturated(Vec2q const a) {
#if INSTRSET >= 10
    return _mm_min_epu64(abs(a), Vec2q(0x7FFFFFFFFFFFFFFF));    
#elif INSTRSET >= 6     // SSE4.2 supported
    __m128i absa = abs(a);                                 // abs(a)
    __m128i overfl = _mm_cmpgt_epi64(_mm_setzero_si128(), absa);// 0 > a
    return           _mm_add_epi64(absa, overfl);          // subtract 1 if 0x8000000000000000
#else                 // SSE2
    __m128i absa = abs(a);                                 // abs(a)
    __m128i signh = _mm_srai_epi32(absa, 31);              // sign in high dword
    __m128i overfl = _mm_shuffle_epi32(signh, 0xF5);       // copy sign to low dword
    return           _mm_add_epi64(absa, overfl);          // subtract 1 if 0x8000000000000000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec2q rotate_left(Vec2q const a, int b) {
#if INSTRSET >= 10  // __AVX512VL__
    return _mm_rolv_epi64(a, _mm_set1_epi64x(int64_t(b)));
#elif defined __XOP__  // AMD XOP instruction set
    return (Vec2q)_mm_rot_epi64(a, Vec2q(b));
#else  // SSE2 instruction set
    __m128i left = _mm_sll_epi64(a, _mm_cvtsi32_si128(b & 0x3F));    // a << b 
    __m128i right = _mm_srl_epi64(a, _mm_cvtsi32_si128((-b) & 0x3F));// a >> (64 - b)
    __m128i rot = _mm_or_si128(left, right);                         // or
    return  (Vec2q)rot;
#endif
}


/*****************************************************************************
*
*          Vector of 2 64-bit unsigned integers
*
*****************************************************************************/

class Vec2uq : public Vec2q {
public:
    // Default constructor:
    Vec2uq() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec2uq(uint64_t i) {
        xmm = Vec2q((int64_t)i);
    }
    // Constructor to build from all elements:
    Vec2uq(uint64_t i0, uint64_t i1) {
        xmm = Vec2q((int64_t)i0, (int64_t)i1);
    }
    // Constructor to convert from type __m128i used in intrinsics:
    Vec2uq(__m128i const x) {
        xmm = x;
    }
    // Assignment operator to convert from type __m128i used in intrinsics:
    Vec2uq & operator = (__m128i const x) {
        xmm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec2uq & load(void const * p) {
        xmm = _mm_loadu_si128((__m128i const*)p);
        return *this;
    }
    // Member function to load from array (aligned)
    Vec2uq & load_a(void const * p) {
        xmm = _mm_load_si128((__m128i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec2uq const insert(int index, uint64_t value) {
        Vec2q::insert(index, (int64_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(int index) const {
        return (uint64_t)Vec2q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (int index) const {
        return extract(index);
    }
    static constexpr int elementtype() {
        return 11;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec2uq operator + (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec2q(a) + Vec2q(b));
}

// vector operator - : subtract
static inline Vec2uq operator - (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec2q(a) - Vec2q(b));
}

// vector operator * : multiply element by element
static inline Vec2uq operator * (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec2q(a) * Vec2q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec2uq operator >> (Vec2uq const a, uint32_t b) {
    return _mm_srl_epi64(a, _mm_cvtsi32_si128((int)b));
}

// vector operator >> : shift right logical all elements
static inline Vec2uq operator >> (Vec2uq const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec2uq & operator >>= (Vec2uq & a, int b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec2uq operator << (Vec2uq const a, uint32_t b) {
    return Vec2uq((Vec2q)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec2uq operator << (Vec2uq const a, int32_t b) {
    return Vec2uq((Vec2q)a << b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec2qb operator > (Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu64_mask(a, b, 6);
#elif defined ( __XOP__ ) // AMD XOP instruction set
    return Vec2qb(_mm_comgt_epu64(a, b));
#elif INSTRSET >= 6 // SSE4.2
    __m128i sign64 = constant4ui<0, 0x80000000, 0, 0x80000000>();
    __m128i aflip = _mm_xor_si128(a, sign64);              // flip sign bits to use signed compare
    __m128i bflip = _mm_xor_si128(b, sign64);
    Vec2q   cmp = _mm_cmpgt_epi64(aflip, bflip);
    return Vec2qb(cmp);
#else  // SSE2 instruction set
    __m128i sign32 = _mm_set1_epi32(0x80000000);           // sign bit of each dword
    __m128i aflip = _mm_xor_si128(a, sign32);              // a with sign bits flipped to use signed compare
    __m128i bflip = _mm_xor_si128(b, sign32);              // b with sign bits flipped to use signed compare
    __m128i equal = _mm_cmpeq_epi32(a, b);                 // a == b, dwords
    __m128i bigger = _mm_cmpgt_epi32(aflip, bflip);        // a > b, dwords
    __m128i biggerl = _mm_shuffle_epi32(bigger, 0xA0);     // a > b, low dwords copied to high dwords
    __m128i eqbig = _mm_and_si128(equal, biggerl);         // high part equal and low part bigger
    __m128i hibig = _mm_or_si128(bigger, eqbig);           // high part bigger or high part equal and low part bigger
    __m128i big = _mm_shuffle_epi32(hibig, 0xF5);          // result copied to low part
    return  Vec2qb(Vec2q(big));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec2qb operator < (Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu64_mask(a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec2qb operator >= (Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // broad boolean vectors
    return _mm_cmp_epu64_mask(a, b, 5);
#elif defined (__XOP__)  // AMD XOP instruction set
    return Vec2qb(_mm_comge_epu64(a, b));
#else  // SSE2 instruction set
    return  Vec2qb(Vec2q(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec2qb operator <= (Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_cmp_epu64_mask(a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec2uq operator & (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec128b(a) & Vec128b(b));
}
static inline Vec2uq operator && (Vec2uq const a, Vec2uq const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec2uq operator | (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec128b(a) | Vec128b(b));
}
static inline Vec2uq operator || (Vec2uq const a, Vec2uq const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec2uq operator ^ (Vec2uq const a, Vec2uq const b) {
    return Vec2uq(Vec128b(a) ^ Vec128b(b));
}

// vector operator ~ : bitwise not
static inline Vec2uq operator ~ (Vec2uq const a) {
    return Vec2uq(~Vec128b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 2; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec2uq select(Vec2qb const s, Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mov_epi64(b, s, a);
#else
    return selectb(s, a, b);
#endif    
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec2uq if_add(Vec2qb const f, Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_add_epi64(a, f, a, b);
#else
    return a + (Vec2uq(f) & b);
#endif
}

// Conditional sub: For all vector elements i: result[i] = f[i] ? (a[i] - b[i]) : a[i]
static inline Vec2uq if_sub(Vec2qb const f, Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_sub_epi64(a, f, a, b);
#else
    return a - (Vec2uq(f) & b);
#endif
}

// Conditional mul: For all vector elements i: result[i] = f[i] ? (a[i] * b[i]) : a[i]
static inline Vec2uq if_mul(Vec2qb const f, Vec2uq const a, Vec2uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm_mask_mullo_epi64(a, f, a, b);
#else
    return select(f, a * b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint64_t horizontal_add(Vec2uq const a) {
    return (uint64_t)horizontal_add((Vec2q)a);
}

// function max: a > b ? a : b
static inline Vec2uq max(Vec2uq const a, Vec2uq const b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec2uq min(Vec2uq const a, Vec2uq const b) {
    return select(a > b, b, a);
}


/*****************************************************************************
*
*          Vector permute functions
*
******************************************************************************
*
* These permute functions can reorder the elements of a vector and optionally
* set some elements to zero.
*
* The indexes are inserted as template parameters in <>. 
* These indexes must be constants. 
* Each template parameter is an index to the element you want to select. 
* An index of -1 will generate zero. 
* An index of V_DC means don't care. This gives the best instruction that 
* fits the remaining indexes
*
* Example:
* Vec4i a(10,11,12,13);        // a is (10,11,12,13)
* Vec4i b, c;
* b = permute4<0,0,2,2>(a);    // b is (10,10,12,12)
* c = permute4<3,2,-1,-1>(a);  // c is (13,12, 0, 0)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fits the template parameters and instruction set. 
* The final optimized code will contain only one or a few instructions.
* Higher instruction sets may give you more efficient code.
*
*****************************************************************************/

// permute Vec2q
template <int i0, int i1>
static inline Vec2q permute2(Vec2q const a) {
    constexpr int indexs[2] = { i0, i1 };                  // indexes as array
    __m128i y = a;                                         // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec2q>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_si128();  // just return zero

    constexpr bool fit_shleft  = (flags & perm_shleft)  != 0;
    constexpr bool fit_shright = (flags & perm_shright) != 0;
    constexpr bool fit_punpckh = (flags & perm_punpckh) != 0;
    constexpr bool fit_punpckl = (flags & perm_punpckl) != 0;
    constexpr bool fit_zeroing = (flags & perm_zeroing) != 0;    

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed
        // try to fit various instructions

        if constexpr (fit_shleft && fit_zeroing) {
            // pslldq does both permutation and zeroing. if zeroing not needed use punpckl instead
            return _mm_bslli_si128(a, 8);
        }
        if constexpr (fit_shright && fit_zeroing) {       
            // psrldq does both permutation and zeroing. if zeroing not needed use punpckh instead
            return _mm_bsrli_si128(a, 8);
        }
        if constexpr (fit_punpckh) {       // fits punpckhi
            y = _mm_unpackhi_epi64(a, a);
        }
        else if constexpr (fit_punpckl) {  // fits punpcklo
            y = _mm_unpacklo_epi64(a, a);
        }
        else {  // needs general permute
            y = _mm_shuffle_epi32(a, i0 * 0x0A + i1 * 0xA0 + 0x44);
        }
    }
    if constexpr (fit_zeroing) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi64(zero_mask<2>(indexs), y);
#else  // use unpack to avoid using data cache
        if constexpr (i0 == -1) {
            y = _mm_unpackhi_epi64(_mm_setzero_si128(), y);
        }
        else if constexpr (i1 == -1) {
            y = _mm_unpacklo_epi64(y, _mm_setzero_si128());
        }
#endif
    }
    return y;
} 

template <int i0, int i1>
static inline Vec2uq permute2(Vec2uq const a) {
    return Vec2uq(permute2 <i0, i1>((Vec2q)a));
}

// permute Vec4i
template <int i0, int i1, int i2, int i3>
static inline Vec4i permute4(Vec4i const a) {
    constexpr int indexs[4] = {i0, i1, i2, i3};            // indexes as array
    __m128i y = a;                                         // result

    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec4i>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {
            // use larger permutation
            constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // permutation pattern
            y = permute2 <L.a[0], L.a[1]> (Vec2q(a));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_shleft) != 0) {   // fits pslldq
            y = _mm_bslli_si128(a, (16-(flags >> perm_rot_count)) & 0xF); 
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_shright) != 0) {  // fits psrldq 
            y = _mm_bsrli_si128(a, (flags >> perm_rot_count) & 0xF); 
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
#if  INSTRSET >= 4 && INSTRSET < 10 // SSSE3, but no compact mask
        else if constexpr ((flags & perm_zeroing) != 0) {  
            // Do both permutation and zeroing with PSHUFB instruction
            const EList <int8_t, 16> bm = pshufb_mask<Vec4i>(indexs);
            return _mm_shuffle_epi8(a, Vec4i().load(bm.a));
        }
#endif 
        else if constexpr ((flags & perm_punpckh) != 0) {  // fits punpckhi
            y = _mm_unpackhi_epi32(a, a);
        }
        else if constexpr ((flags & perm_punpckl) != 0) {  // fits punpcklo
            y = _mm_unpacklo_epi32(a, a);
        }
#if INSTRSET >= 4  // SSSE3
        else if constexpr ((flags & perm_rotate) != 0) {   // fits palignr 
            y = _mm_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF); 
        }
#endif
        else {  // needs general permute
            y = _mm_shuffle_epi32(a, (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6);
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        // The mask-zero operation can be merged into the preceding instruction, whatever that is.
        // A good optimizing compiler will do this automatically.
        // I don't want to clutter all the branches above with this
        y = _mm_maskz_mov_epi32 (zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 4> bm = zero_mask_broad<Vec4i>(indexs);
        y = _mm_and_si128(Vec4i().load(bm.a), y);
#endif
    }  
    return y;
} 

template <int i0, int i1, int i2, int i3>
static inline Vec4ui permute4(Vec4ui const a) {
    return Vec4ui(permute4 <i0, i1, i2, i3>(Vec4i(a)));
}


// permute Vec8s
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8s permute8(Vec8s const a) {
    // indexes as array
    constexpr int indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 };
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags   = perm_flags<Vec8s>(indexs);
    constexpr uint64_t flags16 = perm16_flags<Vec8s>(indexs);

    constexpr bool fit_zeroing = (flags & perm_zeroing) != 0;// needs additional zeroing
    constexpr bool L2L = (flags16 & 1) != 0;               // from low  to low  64-bit part
    constexpr bool H2H = (flags16 & 2) != 0;               // from high to high 64-bit part
    constexpr bool H2L = (flags16 & 4) != 0;               // from high to low  64-bit part
    constexpr bool L2H = (flags16 & 8) != 0;               // from low  to high 64-bit part
    constexpr uint8_t pL2L = uint8_t(flags16 >> 32);       // low  to low  permute pattern
    constexpr uint8_t pH2H = uint8_t(flags16 >> 40);       // high to high permute pattern
    constexpr uint8_t noperm = 0xE4;                       // pattern for no permute

    __m128i y = a;                                         // result

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & perm_perm) != 0) {
        // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {
            // use larger permutation
            constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // permutation pattern
            y = permute4 <L.a[0], L.a[1], L.a[2], L.a[3]> (Vec4i(a));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_shleft) != 0 && (flags & perm_addz) == 0) {// fits pslldq
            return _mm_bslli_si128(a, (16-(flags >> perm_rot_count)) & 0xF); 
        }
        else if constexpr ((flags & perm_shright) != 0 && (flags & perm_addz) == 0) {// fits psrldq 
            return _mm_bsrli_si128(a, (flags >> perm_rot_count) & 0xF); 
        }
        else if constexpr ((flags & perm_broadcast) != 0 && (flags & perm_zeroing) == 0 && (flags >> perm_rot_count & 0xF) == 0) {
#if INSTRSET >= 8   // AVX2
            return _mm_broadcastw_epi16(y);
#else
            y = _mm_shufflelo_epi16(a, 0);                 // broadcast of first element
            return _mm_unpacklo_epi64(y, y);
#endif 
        }
#if  INSTRSET >= 4 && INSTRSET < 10                        // SSSE3, but no compact mask
        else if constexpr (fit_zeroing) {  
            // Do both permutation and zeroing with PSHUFB instruction
            const EList <int8_t, 16> bm = pshufb_mask<Vec8s>(indexs);
            return _mm_shuffle_epi8(a, Vec8s().load(bm.a));
        }
#endif 
        else if constexpr ((flags & perm_punpckh) != 0) {  // fits punpckhi
            y = _mm_unpackhi_epi16(a, a);
        }
        else if constexpr ((flags & perm_punpckl) != 0) {  // fits punpcklo
            y = _mm_unpacklo_epi16(a, a);
        }
#if INSTRSET >= 4  // SSSE3
        else if constexpr ((flags & perm_rotate) != 0) {   // fits palignr 
            y = _mm_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF); 
        }
#endif
        else if constexpr (!H2L && !L2H) {                 // no crossing of 64-bit boundary
            if constexpr (L2L && pL2L != noperm) {
                y = _mm_shufflelo_epi16(y, pL2L);          // permute low 64-bits
            }
            if constexpr (H2H && pH2H != noperm) {
                y = _mm_shufflehi_epi16(y, pH2H);          // permute high 64-bits
            }
        }
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        else if constexpr ((flags & perm_compress) != 0) {
            y = _mm_maskz_compress_epi16(__mmask8(compress_mask(indexs)), y); // compress
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
        else if constexpr ((flags & perm_expand) != 0) {
            y = _mm_maskz_expand_epi16(__mmask8(expand_mask(indexs)), y); // expand
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#endif  // AVX512VBMI2
#if INSTRSET >= 4  // SSSE3
        else {  // needs general permute
            const EList <int8_t, 16> bm = pshufb_mask<Vec8s>(indexs);
            y = _mm_shuffle_epi8(a, Vec8s().load(bm.a));
            return y;  // _mm_shuffle_epi8 also does zeroing
        }
    }
    if constexpr (fit_zeroing) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi16(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int16_t, 8> bm = zero_mask_broad<Vec8s>(indexs);
        y = _mm_and_si128(Vec8s().load(bm.a), y);
#endif
    }
    return y;
#else // INSTRSET < 4
        else {
        // Difficult case. Use permutations of low and high half separately
            constexpr uint8_t pH2L = uint8_t(flags16 >> 48);       // high to low  permute pattern
            constexpr uint8_t pL2H = uint8_t(flags16 >> 56);       // low  to high permute pattern
            __m128i yswap = _mm_shuffle_epi32(y, 0x4E);    // swap low and high 64-bits
            if constexpr (H2L && pH2L != noperm) {
                yswap = _mm_shufflelo_epi16(yswap, pH2L);  // permute low 64-bits
            }
            if constexpr (L2H && pL2H != noperm) {
                yswap = _mm_shufflehi_epi16(yswap, pL2H);  // permute high 64-bits
            }
            if constexpr (L2L && pL2L != noperm) {
                y =     _mm_shufflelo_epi16(y, pL2L);      // permute low 64-bits
            }
            if constexpr (H2H && pH2H != noperm) {
                y =     _mm_shufflehi_epi16(y, pH2H);      // permute high 64-bits
            }
            if constexpr (H2H || L2L) {                    // merge data from y and yswap
                auto selb = make_bit_mask<8,0x102>(indexs);// blend by bit 2. invert upper half
                const EList <int16_t, 8> bm = make_broad_mask<Vec8s>(selb);// convert to broad mask
                y = selectb(Vec8s().load(bm.a), yswap, y);
            }
            else {
                y = yswap;
            }
        }
    }
    if constexpr (fit_zeroing) {
        // additional zeroing needed
        const EList <int16_t, 8> bm = zero_mask_broad<Vec8s>(indexs);
        y = _mm_and_si128(Vec8s().load(bm.a), y);
    }
    return y;
#endif
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8us permute8(Vec8us const a) {
    return Vec8us(permute8 <i0, i1, i2, i3, i4, i5, i6, i7>(Vec8s(a)));
}

// permute Vec16c
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
    static inline Vec16c permute16(Vec16c const a) {
    
    // indexes as array
    constexpr int indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec16c>(indexs);

    constexpr bool fit_zeroing = (flags & perm_zeroing) != 0;// needs additional zeroing

    __m128i y = a;                                         // result

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & perm_perm) != 0) {
        // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {
            // use larger permutation
            constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // permutation pattern
            y = permute8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7] > (Vec8s(a));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_shleft) != 0) {   // fits pslldq
            y = _mm_bslli_si128(a, (16-(flags >> perm_rot_count)) & 0xF); 
            if ((flags & perm_addz) == 0) return y;
        }
        else if constexpr ((flags & perm_shright) != 0) {  // fits psrldq 
            y = _mm_bsrli_si128(a, (flags >> perm_rot_count) & 0xF); 
            if ((flags & perm_addz) == 0) return y;
        }
#if  INSTRSET >= 4 && INSTRSET < 10 // SSSE3, but no compact mask
        else if constexpr (fit_zeroing) {  
            // Do both permutation and zeroing with PSHUFB instruction
            const EList <int8_t, 16> bm = pshufb_mask<Vec16c>(indexs);
            return _mm_shuffle_epi8(a, Vec16c().load(bm.a));
        }
#endif 
        else if constexpr ((flags & perm_punpckh) != 0) {  // fits punpckhi
            y = _mm_unpackhi_epi8(a, a);
        }
        else if constexpr ((flags & perm_punpckl) != 0) {  // fits punpcklo
            y = _mm_unpacklo_epi8(a, a);
        }
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        else if constexpr ((flags & perm_compress) != 0) {
            y = _mm_maskz_compress_epi8(__mmask16(compress_mask(indexs)), y); // compress
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
        else if constexpr ((flags & perm_expand) != 0) {
            y = _mm_maskz_expand_epi8(__mmask16(expand_mask(indexs)), y); // expand
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#endif  // AVX512VBMI2
#if INSTRSET >= 8  // AVX2
        else if constexpr ((flags & perm_broadcast) != 0 && (flags & fit_zeroing) == 0 && (flags >> perm_rot_count & 0xF) == 0) {
            return _mm_broadcastb_epi8(y);
        }
#endif
#if INSTRSET >= 4  // SSSE3
        else if constexpr ((flags & perm_rotate) != 0) {   // fits palignr 
            y = _mm_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF); 
        }
        else {  // needs general permute
            const EList <int8_t, 16> bm = pshufb_mask<Vec16c>(indexs);
            y = _mm_shuffle_epi8(a, Vec16c().load(bm.a));
            return y;  // _mm_shuffle_epi8 also does zeroing
        }
    }
#else
        else {
            // Difficult case. Use permutations of low and high half separately
            Vec16c swapped, te2e, te2o, to2e, to2o, combeven, combodd;

            // get permutation indexes for four 16-bit permutes:
            // k = 0: e2e: even bytes of source to even bytes of destination
            // k = 1: o2e: odd  bytes of source to even bytes of destination
            // k = 2: e2o: even bytes of source to odd  bytes of destination
            // k = 3: o2o: odd  bytes of source to odd  bytes of destination
            auto eoperm = [](uint8_t const k, int const (&indexs)[16]) constexpr {
                uint8_t  ix = 0;            // index element
                uint64_t r = 0;             // return value
                uint8_t  i = (k >> 1) & 1;  // look at odd indexes if destination is odd
                for (; i < 16; i += 2) {
                    ix = (indexs[i] >= 0 && ((indexs[i] ^ k) & 1) == 0) ? (uint8_t)indexs[i]/2u : 0xFFu;
                    r |= uint64_t(ix) << (i / 2u * 8u);
                }
                return r;
            };
            constexpr uint64_t ixe2e = eoperm(0, indexs);
            constexpr uint64_t ixo2e = eoperm(1, indexs);
            constexpr uint64_t ixe2o = eoperm(2, indexs);
            constexpr uint64_t ixo2o = eoperm(3, indexs);

            constexpr bool e2e = ixe2e != -1ll;  // even bytes of source to odd  bytes of destination
            constexpr bool e2o = ixe2o != -1ll;  // even bytes of source to odd  bytes of destination
            constexpr bool o2e = ixo2e != -1ll;  // odd  bytes of source to even bytes of destination
            constexpr bool o2o = ixo2o != -1ll;  // odd  bytes of source to odd  bytes of destination

            if constexpr (e2o || o2e) swapped = rotate_left(Vec8s(a), 8); // swap odd and even bytes

            if constexpr (e2e) te2e = permute8 < int8_t(ixe2e), int8_t(ixe2e>>8), int8_t(ixe2e>>16), int8_t(ixe2e>>24), 
                int8_t(ixe2e>>32), int8_t(ixe2e>>40), int8_t(ixe2e>>48), int8_t(ixe2e>>56)> (Vec8s(a));

            if constexpr (e2o) te2o = permute8 < int8_t(ixe2o), int8_t(ixe2o>>8), int8_t(ixe2o>>16), int8_t(ixe2o>>24), 
                int8_t(ixe2o>>32), int8_t(ixe2o>>40), int8_t(ixe2o>>48), int8_t(ixe2o>>56)> (Vec8s(swapped));

            if constexpr (o2e) to2e = permute8 < int8_t(ixo2e), int8_t(ixo2e>>8), int8_t(ixo2e>>16), int8_t(ixo2e>>24), 
                int8_t(ixo2e>>32), int8_t(ixo2e>>40), int8_t(ixo2e>>48), int8_t(ixo2e>>56)> (Vec8s(swapped));

            if constexpr (o2o) to2o = permute8 < int8_t(ixo2o), int8_t(ixo2o>>8), int8_t(ixo2o>>16), int8_t(ixo2o>>24), 
                int8_t(ixo2o>>32), int8_t(ixo2o>>40), int8_t(ixo2o>>48), int8_t(ixo2o>>56)> (Vec8s(a));

            if constexpr (e2e && o2e) combeven = te2e | to2e;
            else if constexpr (e2e)   combeven = te2e;
            else if constexpr (o2e)   combeven = to2e;
            else                      combeven = _mm_setzero_si128();

            if constexpr (e2o && o2o) combodd = te2o | to2o;
            else if constexpr (e2o)   combodd = te2o;
            else if constexpr (o2o)   combodd = to2o;
            else                      combodd = _mm_setzero_si128();

            __m128i maske = constant4ui <        // mask used even bytes
                (i0  < 0 ? 0 : 0xFF)   | (i2  < 0 ? 0 : 0xFF0000u),
                (i4  < 0 ? 0 : 0xFF)   | (i6  < 0 ? 0 : 0xFF0000u),
                (i8  < 0 ? 0 : 0xFF)   | (i10 < 0 ? 0 : 0xFF0000u),
                (i12 < 0 ? 0 : 0xFF)   | (i14 < 0 ? 0 : 0xFF0000u) >();
            __m128i masko = constant4ui <        // mask used odd bytes
                (i1  < 0 ? 0 : 0xFF00) | (i3  < 0 ? 0 : 0xFF000000u),
                (i5  < 0 ? 0 : 0xFF00) | (i7  < 0 ? 0 : 0xFF000000u),
                (i9  < 0 ? 0 : 0xFF00) | (i11 < 0 ? 0 : 0xFF000000u),
                (i13 < 0 ? 0 : 0xFF00) | (i15 < 0 ? 0 : 0xFF000000u) >();

            return  _mm_or_si128(                // combine even and odd bytes
                    _mm_and_si128(combeven, maske),
                    _mm_and_si128(combodd,  masko));
        }
    }
#endif
    if constexpr (fit_zeroing) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi8(zero_mask<16>(indexs), y);
#else  // use broad mask
        const EList <int8_t, 16> bm = zero_mask_broad<Vec16c>(indexs);
        y = _mm_and_si128(Vec16c().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
    static inline Vec16uc permute16(Vec16uc const a) {
    return Vec16uc(permute16 <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15>(Vec16c(a)));
}


/*****************************************************************************
*
*          Vector blend functions
*
******************************************************************************
*
* These blend functions can mix elements from two different vectors of N 
* elements eadh and optionally set some elements to zero.
*
* The N indexes are inserted as template parameters in <>. 
* These indexes must be compile-time constants. Each template parameter 
* selects an element from one of the input vectors a and b.
* An index in the range 0 .. N-1 selects the corresponding element from a.
* An index in the range N .. 2*N-1 selects an element from b.
* An index with the value -1 gives zero in the corresponding element of
* the result. 
* An index with the value V_DC means don't care. The code will select the
* optimal sequence of instructions that fits the remaining indexes.
*
* Example:
* Vec4i a(100,101,102,103);         // a is (100, 101, 102, 103)
* Vec4i b(200,201,202,203);         // b is (200, 201, 202, 203)
* Vec4i c;
* c = blend4<1,4,-1,7> (a,b);       // c is (101, 200,   0, 203)
*
* A lot of the code here is metaprogramming aiming to find the instructions
* that best fit the template parameters and instruction set. The metacode
* will be reduced out to leave only a few vector instructions in release
* mode with optimization on.
*****************************************************************************/

// permute and blend Vec2q
template <int i0, int i1>
static inline Vec2q blend2(Vec2q const a, Vec2q const b) {
    int constexpr indexs[2] = { i0, i1 };                  // indexes as array
    __m128i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec2q>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute2 <i0, i1> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute2 <i0<0 ? i0 : i0&1, i1<0 ? i1 : i1&1> (b);
    }

    if constexpr ((flags & (blend_perma | blend_permb)) == 0) {// no permutation, only blending
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_epi64 (a, (uint8_t)make_bit_mask<2, 0x301>(indexs), b);
#elif INSTRSET >= 5  // SSE4.1
        y = _mm_blend_epi16 (a, b, ((i0 & 2) ? 0x0F : 0) | ((i1 & 2) ? 0xF0 : 0));
#else  // SSE2
        const EList <int64_t, 2> bm = make_broad_mask<Vec2q>(make_bit_mask<2, 0x301>(indexs));
        y = selectb(Vec2q().load(bm.a), b, a);
#endif        
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_epi64 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_epi64 (b, a);
    }
#if INSTRSET >= 4 // SSSE3
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#endif
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm_castpd_si128(_mm_shuffle_pd(_mm_castsi128_pd(a), _mm_castsi128_pd(b), (flags >> blend_shufpattern) & 3));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm_castpd_si128(_mm_shuffle_pd(_mm_castsi128_pd(b), _mm_castsi128_pd(a), (flags >> blend_shufpattern) & 3));
    }
#endif
    else { // No special cases. permute a and b separately, then blend.
           // This will not occur if ALLOW_FP_PERMUTE is true
#if INSTRSET >= 5  // SSE4.1
        constexpr bool dozero = false;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        constexpr EList<int, 4> L = blend_perm_indexes<2, (int)dozero>(indexs); // get permutation indexes
        __m128i ya = permute2<L.a[0], L.a[1]>(a);
        __m128i yb = permute2<L.a[2], L.a[3]>(b);
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_epi64 (ya, (uint8_t)make_bit_mask<2, 0x301>(indexs), yb);
#elif INSTRSET >= 5  // SSE4.1
        y = _mm_blend_epi16 (ya, yb, ((i0 & 2) ? 0x0F : 0) | ((i1 & 2) ? 0xF0 : 0));
#else  // SSE2
        return _mm_or_si128(ya, yb);
#endif
    }

    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi64(zero_mask<2>(indexs), y);
#else  // use broad mask
        const EList <int64_t, 2> bm = zero_mask_broad<Vec2q>(indexs);
        y = _mm_and_si128(Vec2q().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1>
static inline Vec2uq blend2(Vec2uq const a, Vec2uq const b) {
    return Vec2uq(blend2 <i0, i1>(Vec2q(a), Vec2q(b)));
}


// permute and blend Vec4i
template <int i0, int i1, int i2, int i3>
static inline Vec4i blend4(Vec4i const a, Vec4i const b) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m128i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec4i>(indexs); // get flags for possibilities that fit the index pattern

    constexpr bool blendonly = (flags & (blend_perma | blend_permb)) == 0; // no permutation, only blending

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute4 <i0, i1, i2, i3> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute4 < i0<0?i0:i0&3, i1<0?i1:i1&3, i2<0?i2:i2&3, i3<0?i3:i3&3> (b);
    }
    if constexpr ((flags & blend_largeblock) != 0) {       // fits blending with larger block size
        constexpr EList<int, 2> L = largeblock_indexes<4>(indexs);
        y = blend2 <L.a[0], L.a[1]> (Vec2q(a), Vec2q(b));
        if constexpr ((flags & blend_addz) == 0) {
            return y;                                      // any zeroing has been done by larger blend
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_epi32 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_epi32 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_epi32 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_epi32 (b, a);
    }
#if INSTRSET >= 4 // SSSE3
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#endif
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
    else if constexpr ((flags & blend_shufab) != 0 && !blendonly) { // use floating point instruction shufps
        y = _mm_castps_si128(_mm_shuffle_ps(_mm_castsi128_ps(a), _mm_castsi128_ps(b), uint8_t(flags >> blend_shufpattern)));
    }
    else if constexpr ((flags & blend_shufba) != 0 && !blendonly) { // use floating point instruction shufps
        y = _mm_castps_si128(_mm_shuffle_ps(_mm_castsi128_ps(b), _mm_castsi128_ps(a), uint8_t(flags >> blend_shufpattern)));
    }
#endif
    else { // No special cases. permute a and b separately, then blend.
#if INSTRSET >= 5  // SSE4.1
        constexpr bool dozero = false;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        Vec4i ya = a, yb = b;   // a and b permuted
        constexpr EList<int, 8> L = blend_perm_indexes<4, (int)dozero>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0 || dozero) {
            ya = permute4 <L.a[0], L.a[1], L.a[2], L.a[3]>(a);
        }
        if constexpr ((flags & blend_permb) != 0 || dozero) {
            yb = permute4 <L.a[4], L.a[5], L.a[6], L.a[7]>(b);
        }
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_epi32 (ya, (uint8_t)make_bit_mask<4, 0x302>(indexs), yb);
#elif INSTRSET >= 5  // SSE4.1
        constexpr uint8_t mm = ((i0 & 4) ? 0x03 : 0) | ((i1 & 4) ? 0x0C : 0) | ((i2 & 4) ? 0x30 : 0) | ((i3 & 4) ? 0xC0 : 0);
        y = _mm_blend_epi16 (ya, yb, mm);
#else  // SSE2. dozero = true
        return _mm_or_si128(ya, yb);
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi32(zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 4> bm = zero_mask_broad<Vec4i>(indexs);
        y = _mm_and_si128(Vec4i().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3>
static inline Vec4ui blend4(Vec4ui const a, Vec4ui const b) {
    return Vec4ui(blend4<i0, i1, i2, i3>(Vec4i(a), Vec4i(b)));
}


// permute and blend Vec8s
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8s blend8(Vec8s const a, Vec8s const b) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 };  // indexes as array
    __m128i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec8s>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_si128();

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute8 <i0, i1, i2, i3, i4, i5, i6, i7> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute8 < i0<0?i0:i0&7, i1<0?i1:i1&7, i2<0?i2:i2&7, i3<0?i3:i3&7, 
                          i4<0?i4:i4&7, i5<0?i5:i5&7, i6<0?i6:i6&7, i7<0?i7:i7&7 > (b);
    }
    if constexpr ((flags & blend_largeblock) != 0) {       // fits blending with larger block size
        constexpr EList<int, 4> L = largeblock_indexes<8>(indexs);
        y = blend4 <L.a[0], L.a[1], L.a[2], L.a[3]> (Vec4i(a), Vec4i(b));
        if constexpr ((flags & blend_addz) == 0) {
            return y;                                      // any zeroing has been done by larger blend
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_epi16 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_epi16 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_epi16 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_epi16 (b, a);
    }
#if INSTRSET >= 4 // SSSE3
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#endif
    else { // No special cases.
#if INSTRSET >= 10  // AVX512BW
        const EList <int16_t, 8> bm = perm_mask_broad<Vec8s>(indexs);
        return _mm_maskz_permutex2var_epi16(zero_mask<8>(indexs), a, Vec8s().load(bm.a), b);  
#endif 
        // full blend instruction not available,        
        // permute a and b separately, then blend.
#if INSTRSET >= 5  // SSE4.1
        constexpr bool dozero = (flags & blend_zeroing) != 0;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        Vec8s ya = a, yb = b;   // a and b permuted
        constexpr EList<int, 16> L = blend_perm_indexes<8, (int)dozero>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0 || dozero) {
            ya = permute8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> (a);
        }
        if constexpr ((flags & blend_permb) != 0 || dozero) {
            yb = permute8 <L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]> (b);
        }
        if constexpr (dozero) {  // unused elements are zero
            return _mm_or_si128(ya, yb);    
        }
        else { // blend ya and yb

#if  INSTRSET >= 5  // SSE4.1
        constexpr uint8_t mm = ((i0 & 8) ? 0x01 : 0) | ((i1 & 8) ? 0x02 : 0) | ((i2 & 8) ? 0x04 : 0) | ((i3 & 8) ? 0x08 : 0) |
                               ((i4 & 8) ? 0x10 : 0) | ((i5 & 8) ? 0x20 : 0) | ((i6 & 8) ? 0x40 : 0) | ((i7 & 8) ? 0x80 : 0);
        y = _mm_blend_epi16 (ya, yb, mm);
#endif
        }
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed after special cases
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi16(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int16_t, 8> bm = zero_mask_broad<Vec8s>(indexs);
        y = _mm_and_si128(Vec8s().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8us blend8(Vec8us const a, Vec8us const b) {
    return Vec8us(blend8<i0, i1, i2, i3, i4, i5, i6, i7>(Vec8s(a), Vec8s(b)));
}


// permute and blend Vec16c
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
    static inline Vec16c blend16(Vec16c const a, Vec16c const b) {
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };          // indexes as array
    __m128i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec16c>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm_setzero_si128(); 

    else if constexpr ((flags & blend_b) == 0) {           // nothing from b. just permute a
        return permute16 <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15> (a);
    }
    else if constexpr ((flags & blend_a) == 0) {           // nothing from a. just permute b
        constexpr EList<int, 32> L = blend_perm_indexes<16, 2>(indexs); // get permutation indexes
        return permute16 < L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31] > (b);
    }
#if INSTRSET >= 4 // SSSE3
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#endif
    else if constexpr ((flags & blend_largeblock) != 0) {  // fits blending with larger block size
        constexpr EList<int, 8> L = largeblock_indexes<16>(indexs);
        y = blend8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7] > (Vec8s(a), Vec8s(b));
        if constexpr ((flags & blend_addz) == 0) {
            return y;                                      // any zeroing has been done by larger blend
        }
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm_unpacklo_epi8 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm_unpacklo_epi8 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm_unpackhi_epi8 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm_unpackhi_epi8 (b, a);
    }
    else { // No special cases. Full permute needed
#if INSTRSET >= 10 && defined ( __AVX512VBMI__ ) // AVX512VBMI 
        const EList <int8_t, 16> bm = perm_mask_broad<Vec16c>(indexs);
        return _mm_maskz_permutex2var_epi8(zero_mask<16>(indexs), a, Vec16c().load(bm.a), b);
#endif // __AVX512VBMI__
    
        // full blend instruction not available,
        // permute a and b separately, then blend.
#if INSTRSET >= 10  // AVX512VL           
//#elif INSTRSET >= 5  // SSE4.1    // This is optimal only if both permute16<> calls below have simple special cases
        constexpr bool dozero = (flags & blend_zeroing) != 0;
#else  // SSE2
        constexpr bool dozero = true;
#endif
        Vec16c ya = a, yb = b;   // a and b permuted
        constexpr EList<int, 32> L = blend_perm_indexes<16, (int)dozero>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0 || dozero) {
            ya = permute16 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7], 
                L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]> (a);
        }
        if constexpr ((flags & blend_permb) != 0 || dozero) {
            yb = permute16 <L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
                L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31]> (b);
        }
        if constexpr (dozero) {  // unused fields in ya and yb are zero
            return _mm_or_si128(ya, yb);
        }
        else {
#if INSTRSET >= 10 // AVX512VL
        y = _mm_mask_mov_epi8 (ya, (__mmask16)make_bit_mask<16, 0x304>(indexs), yb);
#endif
        }
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm_maskz_mov_epi8(zero_mask<16>(indexs), y);
#else  // use broad mask
        const EList <int8_t, 16> bm = zero_mask_broad<Vec16c>(indexs);
        y = _mm_and_si128(Vec16c().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
    static inline Vec16uc blend16(Vec16uc const a, Vec16uc const b) {
    return Vec16uc(blend16<i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15>(Vec16c(a), Vec16c(b)));
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
* This can be used for several purposes:
*  - table lookup
*  - permute or blend with variable indexes
*  - blend from more than two sources
*  - gather non-contiguous data
*
* An index out of range may produce any value - the actual value produced is
* implementation dependent and may be different for different instruction
* sets. An index out of range does not produce an error message or exception.
*
* Example:
* Vec4i a(2,0,0,3);           // index a is (  2,   0,   0,   3)
* Vec4i b(100,101,102,103);   // table b is (100, 101, 102, 103)
* Vec4i c;
* c = lookup4 (a,b);          // c is (102, 100, 100, 103)
*
*****************************************************************************/

static inline Vec16c lookup16(Vec16c const index, Vec16c const table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index);
#else
    uint8_t ii[16];
    int8_t  tt[16], rr[16];
    table.store(tt);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x0F];
    return Vec16c().load(rr);
#endif
}

static inline Vec16c lookup32(Vec16c const index, Vec16c const table0, Vec16c const table1) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return (Vec16c)_mm_perm_epi8(table0, table1, index);
#elif INSTRSET >= 5  // SSSE3
    Vec16c r0 = _mm_shuffle_epi8(table0, index + 0x70);           // make negative index for values >= 16
    Vec16c r1 = _mm_shuffle_epi8(table1, (index ^ 0x10) + 0x70);  // make negative index for values <  16
    return r0 | r1;
#else
    uint8_t ii[16];
    int8_t  tt[32], rr[16];
    table0.store(tt);  table1.store(tt + 16);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x1F];
    return Vec16c().load(rr);
#endif
}

template <int n>
static inline Vec16c lookup(Vec16c const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 16) return lookup16(index, Vec16c().load(table));
    if (n <= 32) return lookup32(index, Vec16c().load(table), Vec16c().load((int8_t*)table + 16));
    // n > 32. Limit index
    Vec16uc index1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16uc(index) & uint8_t(n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16uc(index), uint8_t(n - 1));
    }
    uint8_t ii[16];  index1.store(ii);
    int8_t  rr[16];
    for (int j = 0; j < 16; j++) {
        rr[j] = ((int8_t*)table)[ii[j]];
    }
    return Vec16c().load(rr);
}

static inline Vec8s lookup8(Vec8s const index, Vec8s const table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x202 + 0x100);
#else
    int16_t ii[8], tt[8], rr[8];
    table.store(tt);  index.store(ii);
    for (int j = 0; j < 8; j++) rr[j] = tt[ii[j] & 0x07];
    return Vec8s().load(rr);
#endif
}

static inline Vec8s lookup16(Vec8s const index, Vec8s const table0, Vec8s const table1) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return (Vec8s)_mm_perm_epi8(table0, table1, index * 0x202 + 0x100);
#elif INSTRSET >= 5  // SSSE3
    Vec8s r0 = _mm_shuffle_epi8(table0, Vec16c(index * 0x202) + Vec16c(Vec8s(0x7170)));
    Vec8s r1 = _mm_shuffle_epi8(table1, Vec16c(index * 0x202 ^ 0x1010) + Vec16c(Vec8s(0x7170)));
    return r0 | r1;
#else
    int16_t ii[16], tt[32], rr[16];
    table0.store(tt);  table1.store(tt + 8);  index.store(ii);
    for (int j = 0; j < 16; j++) rr[j] = tt[ii[j] & 0x1F];
    return Vec8s().load(rr);
#endif
}

template <int n>
static inline Vec8s lookup(Vec8s const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) return lookup8(index, Vec8s().load(table));
    if (n <= 16) return lookup16(index, Vec8s().load(table), Vec8s().load((int16_t*)table + 8));
    // n > 16. Limit index
    Vec8us index1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8us(index) & (n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8us(index), n - 1);
    }
#if INSTRSET >= 8 // AVX2. Use VPERMD
    Vec8s t1 = _mm_i32gather_epi32((const int *)table, __m128i((Vec4i(index1)) & (Vec4i(0x0000FFFF))), 2);  // even positions
    Vec8s t2 = _mm_i32gather_epi32((const int *)table, _mm_srli_epi32(index1, 16), 2);  // odd  positions
    return blend8<0, 8, 2, 10, 4, 12, 6, 14>(t1, t2);
#else
    uint16_t ii[8];  index1.store(ii);
    return Vec8s(((int16_t*)table)[ii[0]], ((int16_t*)table)[ii[1]], ((int16_t*)table)[ii[2]], ((int16_t*)table)[ii[3]],
        ((int16_t*)table)[ii[4]], ((int16_t*)table)[ii[5]], ((int16_t*)table)[ii[6]], ((int16_t*)table)[ii[7]]);
#endif
}


static inline Vec4i lookup4(Vec4i const index, Vec4i const table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x04040404 + 0x03020100);
#else
    return Vec4i(table[index[0]], table[index[1]], table[index[2]], table[index[3]]);
#endif
}

static inline Vec4i lookup8(Vec4i const index, Vec4i const table0, Vec4i const table1) {
    // return Vec4i(lookup16(Vec8s(index * 0x20002 + 0x10000), Vec8s(table0), Vec8s(table1)));
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    return (Vec4i)_mm_perm_epi8(table0, table1, index * 0x04040404 + 0x03020100);
#elif INSTRSET >= 8 // AVX2. Use VPERMD
    __m256i table01 = _mm256_inserti128_si256(_mm256_castsi128_si256(table0), table1, 1); // join tables into 256 bit vector
    return _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table01, _mm256_castsi128_si256(index)));

#elif INSTRSET >= 4  // SSSE3
    Vec4i r0 = _mm_shuffle_epi8(table0, Vec16c(index * 0x04040404) + Vec16c(Vec4i(0x73727170)));
    Vec4i r1 = _mm_shuffle_epi8(table1, Vec16c(index * 0x04040404 ^ 0x10101010) + Vec16c(Vec4i(0x73727170)));
    return r0 | r1;
#else    // SSE2
    int32_t ii[4], tt[8], rr[4];
    table0.store(tt);  table1.store(tt + 4);  index.store(ii);
    for (int j = 0; j < 4; j++) rr[j] = tt[ii[j] & 0x07];
    return Vec4i().load(rr);
#endif
}

static inline Vec4i lookup16(Vec4i const index, Vec4i const table0, Vec4i const table1, Vec4i const table2, Vec4i const table3) {
#if INSTRSET >= 8 // AVX2. Use VPERMD
    __m256i table01 = _mm256_inserti128_si256(_mm256_castsi128_si256(table0), table1, 1); // join tables into 256 bit vector
    __m256i table23 = _mm256_inserti128_si256(_mm256_castsi128_si256(table2), table3, 1); // join tables into 256 bit vector
    __m128i r0 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table01, _mm256_castsi128_si256(index)));
    __m128i r1 = _mm256_castsi256_si128(_mm256_permutevar8x32_epi32(table23, _mm256_castsi128_si256(index ^ 8)));
    return select(index >= 8, Vec4i(r1), Vec4i(r0));
    //return _mm_blendv_epi8(r0, r1, index >= 8);

#elif defined (__XOP__)  // AMD XOP instruction set. Use VPPERM
    Vec4i r0 = _mm_perm_epi8(table0, table1, ((index) * 0x04040404u + 0x63626160u) & 0X9F9F9F9Fu);
    Vec4i r1 = _mm_perm_epi8(table2, table3, ((index ^ 8) * 0x04040404u + 0x63626160u) & 0X9F9F9F9Fu);
    return r0 | r1;

#elif INSTRSET >= 5  // SSSE3
    Vec16c aa = Vec16c(Vec4i(0x73727170));
    Vec4i r0 = _mm_shuffle_epi8(table0, Vec16c((index) * 0x04040404) + aa);
    Vec4i r1 = _mm_shuffle_epi8(table1, Vec16c((index ^ 4) * 0x04040404) + aa);
    Vec4i r2 = _mm_shuffle_epi8(table2, Vec16c((index ^ 8) * 0x04040404) + aa);
    Vec4i r3 = _mm_shuffle_epi8(table3, Vec16c((index ^ 12) * 0x04040404) + aa);
    return (r0 | r1) | (r2 | r3);

#else    // SSE2
    int32_t ii[4], tt[16], rr[4];
    table0.store(tt);  table1.store(tt + 4);  table2.store(tt + 8);  table3.store(tt + 12);
    index.store(ii);
    for (int j = 0; j < 4; j++) rr[j] = tt[ii[j] & 0x0F];
    return Vec4i().load(rr);
#endif
}

template <int n>
static inline Vec4i lookup(Vec4i const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) return lookup4(index, Vec4i().load(table));
    if (n <= 8) return lookup8(index, Vec4i().load(table), Vec4i().load((int32_t*)table + 4));
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
#if INSTRSET >= 8 // AVX2. Use VPERMD
    return _mm_i32gather_epi32((const int *)table, index1, 4);
#else
    uint32_t ii[4];  index1.store(ii);
    return Vec4i(((int32_t*)table)[ii[0]], ((int32_t*)table)[ii[1]], ((int32_t*)table)[ii[2]], ((int32_t*)table)[ii[3]]);
#endif
}


static inline Vec2q lookup2(Vec2q const index, Vec2q const table) {
#if INSTRSET >= 5  // SSSE3
    return _mm_shuffle_epi8(table, index * 0x0808080808080808ll + 0x0706050403020100ll);
#else
    int64_t ii[2], tt[2];
    table.store(tt);  index.store(ii);
    return Vec2q(tt[int(ii[0])], tt[int(ii[1])]);
#endif
}

template <int n>
static inline Vec2q lookup(Vec2q const index, void const * table) {
    if (n <= 0) return 0;
    // n > 0. Limit index
    Vec2uq index1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec2uq(index) & (n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1.
        // There is no 64-bit min instruction, but we can use the 32-bit unsigned min,
        // since n is a 32-bit integer
        index1 = Vec2uq(min(Vec2uq(index), constant4ui<n - 1, 0, n - 1, 0>()));
    }
    uint32_t ii[4];  index1.store(ii);  // use only lower 32 bits of each index
    int64_t const * tt = (int64_t const *)table;
    return Vec2q(tt[ii[0]], tt[ii[2]]);
}


/*****************************************************************************
*
*          Byte shifts
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
template <unsigned int b>
static inline Vec16c shift_bytes_up(Vec16c const a) {
#if INSTRSET >= 4    // SSSE3
    if (b < 16) {
        return _mm_alignr_epi8(a, _mm_setzero_si128(), 16 - b);
    }
    else {
        return _mm_setzero_si128();                       // zero
    }
#else
    int8_t dat[32];
    if (b < 16) {
        Vec16c(0).store(dat);
        a.store(dat + b);
        return Vec16c().load(dat);
    }
    else return 0;
#endif
}

// Function shift_bytes_down: shift whole vector right by b bytes
template <unsigned int b>
static inline Vec16c shift_bytes_down(Vec16c const a) {
#if INSTRSET >= 4    // SSSE3
    if (b < 16) {
        return _mm_alignr_epi8(_mm_setzero_si128(), a, b);
    }
    else {
        return _mm_setzero_si128();
    }
#else
    int8_t dat[32];
    if (b < 16) {
        a.store(dat);
        Vec16c(0).store(dat + 16);
        return Vec16c().load(dat + b);
    }
    else return 0;
#endif
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// find lowest and highest index
template <int N>
constexpr int min_index(const int (&a)[N]) {
    int ix = a[0];
    for (int i = 1; i < N; i++) {
        if (a[i] < ix) ix = a[i];
    }
    return ix;
}

template <int N>
constexpr int max_index(const int (&a)[N]) {
    int ix = a[0];
    for (int i = 1; i < N; i++) {
        if (a[i] > ix) ix = a[i];
    }
    return ix;
}

// Load elements from array a with indices i0, i1, i2, i3
template <int i0, int i1, int i2, int i3>
static inline Vec4i gather4i(void const * a) {
    int constexpr indexs[4] = { i0, i1, i2, i3 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 3) {
        // load one contiguous block and permute
        if constexpr (imax > 3) {
            // make sure we don't read past the end of the array
            Vec4i b = Vec4i().load((int32_t const *)a + imax - 3);
            return permute4<i0 - imax + 3, i1 - imax + 3, i2 - imax + 3, i3 - imax + 3>(b);
        }
        else {
            Vec4i b = Vec4i().load((int32_t const *)a + imin);
            return permute4<i0 - imin, i1 - imin, i2 - imin, i3 - imin>(b);
        }
    }
    if constexpr ((i0<imin + 4 || i0>imax - 4) && (i1<imin + 4 || i1>imax - 4) && (i2<imin + 4 || i2>imax - 4) && (i3<imin + 4 || i3>imax - 4)) {
        // load two contiguous blocks and blend
        Vec4i b = Vec4i().load((int32_t const *)a + imin);
        Vec4i c = Vec4i().load((int32_t const *)a + imax - 3);
        constexpr int j0 = i0 < imin + 4 ? i0 - imin : 7 - imax + i0;
        constexpr int j1 = i1 < imin + 4 ? i1 - imin : 7 - imax + i1;
        constexpr int j2 = i2 < imin + 4 ? i2 - imin : 7 - imax + i2;
        constexpr int j3 = i3 < imin + 4 ? i3 - imin : 7 - imax + i3;
        return blend4<j0, j1, j2, j3>(b, c);
    }
    // use AVX2 gather if available
#if INSTRSET >= 8
    return _mm_i32gather_epi32((const int *)a, Vec4i(i0, i1, i2, i3), 4);
#else
    return lookup<imax + 1>(Vec4i(i0, i1, i2, i3), a);
#endif
}

// Load elements from array a with indices i0, i1
template <int i0, int i1>
static inline Vec2q gather2q(void const * a) {
    constexpr int imin = i0 < i1 ? i0 : i1;
    constexpr int imax = i0 > i1 ? i0 : i1;
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 1) {
        // load one contiguous block and permute
        if constexpr (imax > 1) {
            // make sure we don't read past the end of the array
            Vec2q b = Vec2q().load((int64_t const *)a + imax - 1);
            return permute2<i0 - imax + 1, i1 - imax + 1>(b);
        }
        else {
            Vec2q b = Vec2q().load((int64_t const *)a + imin);
            return permute2<i0 - imin, i1 - imin>(b);
        }
    }
    return Vec2q(((int64_t*)a)[i0], ((int64_t*)a)[i1]);
}


/*****************************************************************************
*
*          Vector scatter functions with fixed indexes
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
* The scatter functions are useful if the data are distributed in a sparce
* manner into the array. If the array is dense then it is more efficient
* to permute the data into the right positions and then write the whole
* permuted vector into the array.
*
* Example:
* Vec8q a(10,11,12,13,14,15,16,17);
* int64_t b[16] = {0};
* scatter<0,2,14,10,1,-1,5,9>(a,b); // b = (10,14,11,0,0,16,0,0,0,17,13,0,0,0,12,0)
*
*****************************************************************************/

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4i const data, void * destination) {
#if INSTRSET >= 10 // AVX512VL
    __m128i indx = constant4ui<i0, i1, i2, i3>();
    __mmask8 mask = uint8_t((i0 >= 0) | ((i1 >= 0) << 1) | ((i2 >= 0) << 2) | ((i3 >= 0) << 3));
    _mm_mask_i32scatter_epi32((int*)destination, mask, indx, data, 4);

#elif INSTRSET >= 9  //  AVX512F
    __m512i indx = _mm512_castsi128_si512(constant4ui<i0, i1, i2, i3>());
    __mmask16 mask = uint16_t((i0 >= 0) | ((i1 >= 0) << 1) | ((i2 >= 0) << 2) | ((i3 >= 0) << 3));
    _mm512_mask_i32scatter_epi32(destination, mask, indx, _mm512_castsi128_si512(data), 4);

#else
    int32_t * arr = (int32_t*)destination;
    const int index[4] = { i0,i1,i2,i3 };
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
#endif
}

template <int i0, int i1>
static inline void scatter(Vec2q const data, void * destination) {
    int64_t* arr = (int64_t*)destination;
    if (i0 >= 0) arr[i0] = data[0];
    if (i1 >= 0) arr[i1] = data[1];
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec4i const index, uint32_t limit, Vec4i const data, void * destination) {
#if INSTRSET >= 10  //  AVX512VL
    __mmask8 mask = _mm_cmplt_epu32_mask(index, Vec4ui(limit));
    _mm_mask_i32scatter_epi32((int*)destination, mask, index, data, 4);
#elif INSTRSET >= 9 //  AVX512F
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xF, _mm512_castsi128_si512(index), _mm512_castsi128_si512(Vec4ui(limit)));
    _mm512_mask_i32scatter_epi32((int*)destination, mask, _mm512_castsi128_si512(index), _mm512_castsi128_si512(data), 4);
#else
    int32_t* arr = (int32_t*)destination;
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
#endif
}

static inline void scatter(Vec2q const index, uint32_t limit, Vec2q const data, void * destination) {
    int64_t* arr = (int64_t*)destination;
    if (uint64_t(index[0]) < uint64_t(limit)) arr[index[0]] = data[0];
    if (uint64_t(index[1]) < uint64_t(limit)) arr[index[1]] = data[1];
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 16 bits with sign extension
static inline Vec8s extend_low(Vec16c const a) {
    __m128i sign = _mm_cmpgt_epi8(_mm_setzero_si128(), a);  // 0 > a
    return         _mm_unpacklo_epi8(a, sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 8 elements to 16 bits with sign extension
static inline Vec8s extend_high(Vec16c const a) {
    __m128i sign = _mm_cmpgt_epi8(_mm_setzero_si128(), a);  // 0 > a
    return         _mm_unpackhi_epi8(a, sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 8 elements to 16 bits with zero extension
static inline Vec8us extend_low(Vec16uc const a) {
    return    _mm_unpacklo_epi8(a, _mm_setzero_si128());    // interleave with zero extensions
}

// Function extend_high : extends the high 8 elements to 16 bits with zero extension
static inline Vec8us extend_high(Vec16uc const a) {
    return    _mm_unpackhi_epi8(a, _mm_setzero_si128());    // interleave with zero extensions
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 32 bits with sign extension
static inline Vec4i extend_low(Vec8s const a) {
    __m128i sign = _mm_srai_epi16(a, 15);                   // sign bit
    return         _mm_unpacklo_epi16(a, sign);             // interleave with sign extensions
}

// Function extend_high : extends the high 4 elements to 32 bits with sign extension
static inline Vec4i extend_high(Vec8s const a) {
    __m128i sign = _mm_srai_epi16(a, 15);                   // sign bit
    return         _mm_unpackhi_epi16(a, sign);             // interleave with sign extensions
}

// Function extend_low : extends the low 4 elements to 32 bits with zero extension
static inline Vec4ui extend_low(Vec8us const a) {
    return    _mm_unpacklo_epi16(a, _mm_setzero_si128());   // interleave with zero extensions
}

// Function extend_high : extends the high 4 elements to 32 bits with zero extension
static inline Vec4ui extend_high(Vec8us const a) {
    return    _mm_unpackhi_epi16(a, _mm_setzero_si128());   // interleave with zero extensions
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 2 elements to 64 bits with sign extension
static inline Vec2q extend_low(Vec4i const a) {
    __m128i sign = _mm_srai_epi32(a, 31);                   // sign bit
    return         _mm_unpacklo_epi32(a, sign);             // interleave with sign extensions
}

// Function extend_high : extends the high 2 elements to 64 bits with sign extension
static inline Vec2q extend_high(Vec4i const a) {
    __m128i sign = _mm_srai_epi32(a, 31);                   // sign bit
    return         _mm_unpackhi_epi32(a, sign);             // interleave with sign extensions
}

// Function extend_low : extends the low 2 elements to 64 bits with zero extension
static inline Vec2uq extend_low(Vec4ui const a) {
    return    _mm_unpacklo_epi32(a, _mm_setzero_si128());   // interleave with zero extensions
}

// Function extend_high : extends the high 2 elements to 64 bits with zero extension
static inline Vec2uq extend_high(Vec4ui const a) {
    return    _mm_unpackhi_epi32(a, _mm_setzero_si128());   // interleave with zero extensions
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec16c compress(Vec8s const low, Vec8s const high) {
    __m128i mask = _mm_set1_epi32(0x00FF00FF);            // mask for low bytes
    __m128i lowm = _mm_and_si128(low, mask);               // bytes of low
    __m128i highm = _mm_and_si128(high, mask);              // bytes of high
    return  _mm_packus_epi16(lowm, highm);                  // unsigned pack
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec16c compress_saturated(Vec8s const low, Vec8s const high) {
    return  _mm_packs_epi16(low, high);
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec16uc compress(Vec8us const low, Vec8us const high) {
    return  Vec16uc(compress((Vec8s)low, (Vec8s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec16uc compress_saturated(Vec8us const low, Vec8us const high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i maxval = _mm_set1_epi32(0x00FF00FF);           // maximum value
    __m128i low1 = _mm_min_epu16(low, maxval);             // upper limit
    __m128i high1 = _mm_min_epu16(high, maxval);           // upper limit
    return            _mm_packus_epi16(low1, high1);       // this instruction saturates from signed 32 bit to unsigned 16 bit
#else
    __m128i zero = _mm_setzero_si128();
    __m128i signlow = _mm_cmpgt_epi16(zero, low);          // sign bit of low
    __m128i signhi = _mm_cmpgt_epi16(zero, high);          // sign bit of high
    __m128i slow2 = _mm_srli_epi16(signlow, 8);            // FF if low negative
    __m128i shigh2 = _mm_srli_epi16(signhi, 8);            // FF if high negative
    __m128i maskns = _mm_set1_epi32(0x7FFF7FFF);           // mask for removing sign bit
    __m128i lowns = _mm_and_si128(low, maskns);            // low,  with sign bit removed
    __m128i highns = _mm_and_si128(high, maskns);          // high, with sign bit removed
    __m128i lowo = _mm_or_si128(lowns, slow2);             // low,  sign bit replaced by 00FF
    __m128i higho = _mm_or_si128(highns, shigh2);          // high, sign bit replaced by 00FF
    return            _mm_packus_epi16(lowo, higho);       // this instruction saturates from signed 16 bit to unsigned 8 bit
#endif
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8s compress(Vec4i const low, Vec4i const high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i mask = _mm_set1_epi32(0x0000FFFF);             // mask for low words
    __m128i lowm = _mm_and_si128(low, mask);               // bytes of low
    __m128i highm = _mm_and_si128(high, mask);             // bytes of high
    return  _mm_packus_epi32(lowm, highm);                 // unsigned pack
#else
    __m128i low1 = _mm_shufflelo_epi16(low, 0xD8);         // low words in place
    __m128i high1 = _mm_shufflelo_epi16(high, 0xD8);       // low words in place
    __m128i low2 = _mm_shufflehi_epi16(low1, 0xD8);        // low words in place
    __m128i high2 = _mm_shufflehi_epi16(high1, 0xD8);      // low words in place
    __m128i low3 = _mm_shuffle_epi32(low2, 0xD8);          // low dwords of low  to pos. 0 and 32
    __m128i high3 = _mm_shuffle_epi32(high2, 0xD8);        // low dwords of high to pos. 0 and 32
    return  _mm_unpacklo_epi64(low3, high3);               // interleave
#endif
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec8s compress_saturated(Vec4i const low, Vec4i const high) {
    return  _mm_packs_epi32(low, high);                    // pack with signed saturation
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8us compress(Vec4ui const low, Vec4ui const high) {
    return Vec8us(compress((Vec4i)low, (Vec4i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec8us compress_saturated(Vec4ui const low, Vec4ui const high) {
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i maxval = _mm_set1_epi32(0x0000FFFF);           // maximum value
    __m128i low1 = _mm_min_epu32(low, maxval);             // upper limit
    __m128i high1 = _mm_min_epu32(high, maxval);           // upper limit
    return            _mm_packus_epi32(low1, high1);       // this instruction saturates from signed 32 bit to unsigned 16 bit
#else
    __m128i zero = _mm_setzero_si128();
    __m128i lowzero = _mm_cmpeq_epi16(low, zero);          // for each word is zero
    __m128i highzero = _mm_cmpeq_epi16(high, zero);        // for each word is zero
    __m128i mone = _mm_set1_epi32(-1);                     // FFFFFFFF
    __m128i lownz = _mm_xor_si128(lowzero, mone);          // for each word is nonzero
    __m128i highnz = _mm_xor_si128(highzero, mone);        // for each word is nonzero
    __m128i lownz2 = _mm_srli_epi32(lownz, 16);            // shift down to low dword
    __m128i highnz2 = _mm_srli_epi32(highnz, 16);          // shift down to low dword
    __m128i lowsatur = _mm_or_si128(low, lownz2);          // low, saturated
    __m128i hisatur = _mm_or_si128(high, highnz2);         // high, saturated
    return  Vec8us(compress(Vec4i(lowsatur), Vec4i(hisatur)));
#endif
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec4i compress(Vec2q const low, Vec2q const high) {
    __m128i low2 = _mm_shuffle_epi32(low, 0xD8);           // low dwords of low  to pos. 0 and 32
    __m128i high2 = _mm_shuffle_epi32(high, 0xD8);         // low dwords of high to pos. 0 and 32
    return  _mm_unpacklo_epi64(low2, high2);               // interleave
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
// This function is very inefficient unless the SSE4.2 instruction set is supported
static inline Vec4i compress_saturated(Vec2q const low, Vec2q const high) {
    Vec2q maxval = _mm_set_epi32(0, 0x7FFFFFFF, 0, 0x7FFFFFFF);
    Vec2q minval = _mm_set_epi32(-1, 0x80000000, -1, 0x80000000);
    Vec2q low1 = min(low, maxval);
    Vec2q high1 = min(high, maxval);
    Vec2q low2 = max(low1, minval);
    Vec2q high2 = max(high1, minval);
    return compress(low2, high2);
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec4ui compress(Vec2uq const low, Vec2uq const high) {
    return Vec4ui(compress((Vec2q)low, (Vec2q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec4ui compress_saturated(Vec2uq const low, Vec2uq const high) {
    __m128i zero = _mm_setzero_si128();
    __m128i lowzero = _mm_cmpeq_epi32(low, zero);          // for each dword is zero
    __m128i highzero = _mm_cmpeq_epi32(high, zero);        // for each dword is zero
    __m128i mone = _mm_set1_epi32(-1);                     // FFFFFFFF
    __m128i lownz = _mm_xor_si128(lowzero, mone);          // for each dword is nonzero
    __m128i highnz = _mm_xor_si128(highzero, mone);        // for each dword is nonzero
    __m128i lownz2 = _mm_srli_epi64(lownz, 32);            // shift down to low dword
    __m128i highnz2 = _mm_srli_epi64(highnz, 32);          // shift down to low dword
    __m128i lowsatur = _mm_or_si128(low, lownz2);          // low, saturated
    __m128i hisatur = _mm_or_si128(high, highnz2);         // high, saturated
    return  Vec4ui(compress(Vec2q(lowsatur), Vec2q(hisatur)));
}



/*****************************************************************************
*
*          Integer division operators
*
******************************************************************************
*
* The instruction set does not support integer vector division. Instead, we
* are using a method for fast integer division based on multiplication and
* shift operations. This method is faster than simple integer division if the
* same divisor is used multiple times.
*
* All elements in a vector are divided by the same divisor. It is not possible
* to divide different elements of the same vector by different divisors.
*
* The parameters used for fast division are stored in an object of a
* Divisor class. This object can be created implicitly, for example in:
*        Vec4i a, b; int c;
*        a = b / c;
* or explicitly as:
*        a = b / Divisor_i(c);
*
* It takes more time to compute the parameters used for fast division than to
* do the division. Therefore, it is advantageous to use the same divisor object
* multiple times. For example, to divide 80 unsigned short integers by 10:
*
*        uint16_t dividends[80], quotients[80];         // numbers to work with
*        Divisor_us div10(10);                          // make divisor object for dividing by 10
*        Vec8us temp;                                   // temporary vector
*        for (int i = 0; i < 80; i += 8) {              // loop for 4 elements per iteration
*            temp.load(dividends+i);                    // load 4 elements
*            temp /= div10;                             // divide each element by 10
*            temp.store(quotients+i);                   // store 4 elements
*        }
*
* The parameters for fast division can also be computed at compile time. This is
* an advantage if the divisor is known at compile time. Use the const_int or const_uint
* macro to do this. For example, for signed integers:
*        Vec8s a, b;
*        a = b / const_int(10);
* Or, for unsigned integers:
*        Vec8us a, b;
*        a = b / const_uint(10);
*
* The division of a vector of 16-bit integers is faster than division of a vector
* of other integer sizes.
*
*
* Mathematical formula, used for signed division with fixed or variable divisor:
* (From T. Granlund and P. L. Montgomery: Division by Invariant Integers Using Multiplication,
* Proceedings of the SIGPLAN 1994 Conference on Programming Language Design and Implementation.
* http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.1.2556 )
* x = dividend
* d = abs(divisor)
* w = integer word size, bits
* L = ceil(log2(d)) = bit_scan_reverse(d-1)+1
* L = max(L,1)
* m = 1 + 2^(w+L-1)/d - 2^w                      [division should overflow to 0 if d = 1]
* sh1 = L-1
* q = x + (m*x >> w)                             [high part of signed multiplication with 2w bits]
* q = (q >> sh1) - (x<0 ? -1 : 0)
* if (divisor < 0) q = -q
* result = trunc(x/d) = q
*
* Mathematical formula, used for unsigned division with variable divisor:
* (Also from T. Granlund and P. L. Montgomery)
* x = dividend
* d = divisor
* w = integer word size, bits
* L = ceil(log2(d)) = bit_scan_reverse(d-1)+1
* m = 1 + 2^w * (2^L-d) / d                      [2^L should overflow to 0 if L = w]
* sh1 = min(L,1)
* sh2 = max(L-1,0)
* t = m*x >> w                                   [high part of unsigned multiplication with 2w bits]
* result = floor(x/d) = (((x-t) >> sh1) + t) >> sh2
*
* Mathematical formula, used for unsigned division with fixed divisor:
* (From Terje Mathisen, unpublished)
* x = dividend
* d = divisor
* w = integer word size, bits
* b = floor(log2(d)) = bit_scan_reverse(d)
* f = 2^(w+b) / d                                [exact division]
* If f is an integer then d is a power of 2 then go to case A
* If the fractional part of f is < 0.5 then go to case B
* If the fractional part of f is > 0.5 then go to case C
* Case A:  [shift only]
* result = x >> b
* Case B:  [round down f and compensate by adding one to x]
* result = ((x+1)*floor(f)) >> (w+b)             [high part of unsigned multiplication with 2w bits]
* Case C:  [round up f, no compensation for rounding error]
* result = (x*ceil(f)) >> (w+b)                  [high part of unsigned multiplication with 2w bits]
*
*
*****************************************************************************/

// encapsulate parameters for fast division on vector of 4 32-bit signed integers
class Divisor_i {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count used in fast division
    __m128i sign;                                          // sign of divisor
public:
    Divisor_i() {};                                        // Default constructor
    Divisor_i(int32_t d) {                                 // Constructor with divisor
        set(d);
    }
    Divisor_i(int m, int s1, int sgn) {                    // Constructor with precalculated multiplier, shift and sign
        multiplier = _mm_set1_epi32(m);
        shift1 = _mm_cvtsi32_si128(s1);
        sign = _mm_set1_epi32(sgn);
    }
    void set(int32_t d) {                                  // Set or change divisor, calculate parameters
        const int32_t d1 = ::abs(d);
        int32_t sh, m;
        if (d1 > 1) {
            sh = (int)bit_scan_reverse(uint32_t(d1 - 1));  // shift count = ceil(log2(d1))-1 = (bit_scan_reverse(d1-1)+1)-1
            m = int32_t((int64_t(1) << (32 + sh)) / d1 - ((int64_t(1) << 32) - 1)); // calculate multiplier
        }
        else {
            m = 1;                                         // for d1 = 1
            sh = 0;
            if (d == 0) m /= d;                            // provoke error here if d = 0
            if (uint32_t(d) == 0x80000000u) {              // fix overflow for this special case
                m = 0x80000001;
                sh = 30;
            }
        }
        multiplier = _mm_set1_epi32(m);                    // broadcast multiplier
        shift1 = _mm_cvtsi32_si128(sh);                    // shift count
        //sign = _mm_set1_epi32(d < 0 ? -1 : 0);           // bug in VS2019, 32 bit release. Replace by this:
        if (d < 0) sign = _mm_set1_epi32(-1); else sign = _mm_set1_epi32(0);  // sign of divisor
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count
        return shift1;
    }
    __m128i getsign() const {                              // get sign of divisor
        return sign;
    }
};

// encapsulate parameters for fast division on vector of 4 32-bit unsigned integers
class Divisor_ui {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count 1 used in fast division
    __m128i shift2;                                        // shift count 2 used in fast division
public:
    Divisor_ui() {};                                       // Default constructor
    Divisor_ui(uint32_t d) {                               // Constructor with divisor
        set(d);
    }
    Divisor_ui(uint32_t m, int s1, int s2) {               // Constructor with precalculated multiplier and shifts
        multiplier = _mm_set1_epi32((int32_t)m);
        shift1 = _mm_setr_epi32(s1, 0, 0, 0);
        shift2 = _mm_setr_epi32(s2, 0, 0, 0);
    }
    void set(uint32_t d) {                                 // Set or change divisor, calculate parameters
        uint32_t L, L2, sh1, sh2, m;
        switch (d) {
        case 0:
            m = sh1 = sh2 = 1 / d;                         // provoke error for d = 0
            break;
        case 1:
            m = 1; sh1 = sh2 = 0;                          // parameters for d = 1
            break;
        case 2:
            m = 1; sh1 = 1; sh2 = 0;                       // parameters for d = 2
            break;
        default:                                           // general case for d > 2
            L = bit_scan_reverse(d - 1) + 1;               // ceil(log2(d))
            L2 = uint32_t(L < 32 ? 1 << L : 0);            // 2^L, overflow to 0 if L = 32
            m = 1 + uint32_t((uint64_t(L2 - d) << 32) / d);// multiplier
            sh1 = 1;  sh2 = L - 1;                         // shift counts
        }
        multiplier = _mm_set1_epi32((int32_t)m);
        shift1 = _mm_setr_epi32((int32_t)sh1, 0, 0, 0);
        shift2 = _mm_setr_epi32((int32_t)sh2, 0, 0, 0);
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count 1
        return shift1;
    }
    __m128i gets2() const {                                // get shift count 2
        return shift2;
    }
};


// encapsulate parameters for fast division on vector of 8 16-bit signed integers
class Divisor_s {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count used in fast division
    __m128i sign;                                          // sign of divisor
public:
    Divisor_s() {};                                        // Default constructor
    Divisor_s(int16_t d) {                                 // Constructor with divisor
        set(d);
    }
    Divisor_s(int16_t m, int s1, int sgn) {                // Constructor with precalculated multiplier, shift and sign
        multiplier = _mm_set1_epi16(m);
        shift1 = _mm_setr_epi32(s1, 0, 0, 0);
        sign = _mm_set1_epi32(sgn);
    }
    void set(int16_t d) {                                  // Set or change divisor, calculate parameters
        const int32_t d1 = ::abs(d);
        int32_t sh, m;
        if (d1 > 1) {
            sh = (int32_t)bit_scan_reverse(uint32_t(d1-1));// shift count = ceil(log2(d1))-1 = (bit_scan_reverse(d1-1)+1)-1
            m = ((int32_t(1) << (16 + sh)) / d1 - ((int32_t(1) << 16) - 1)); // calculate multiplier
        }
        else {
            m = 1;                                         // for d1 = 1
            sh = 0;
            if (d == 0) m /= d;                            // provoke error here if d = 0
            if (uint16_t(d) == 0x8000u) {                  // fix overflow for this special case
                m = 0x8001;
                sh = 14;
            }
        }
        multiplier = _mm_set1_epi16(int16_t(m));           // broadcast multiplier
        shift1 = _mm_setr_epi32(sh, 0, 0, 0);              // shift count
        sign = _mm_set1_epi32(d < 0 ? -1 : 0);             // sign of divisor
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count
        return shift1;
    }
    __m128i getsign() const {                              // get sign of divisor
        return sign;
    }
};


// encapsulate parameters for fast division on vector of 8 16-bit unsigned integers
class Divisor_us {
protected:
    __m128i multiplier;                                    // multiplier used in fast division
    __m128i shift1;                                        // shift count 1 used in fast division
    __m128i shift2;                                        // shift count 2 used in fast division
public:
    Divisor_us() {};                                       // Default constructor
    Divisor_us(uint16_t d) {                               // Constructor with divisor
        set(d);
    }
    Divisor_us(uint16_t m, int s1, int s2) {               // Constructor with precalculated multiplier and shifts
        multiplier = _mm_set1_epi16((int16_t)m);
        shift1 = _mm_setr_epi32(s1, 0, 0, 0);
        shift2 = _mm_setr_epi32(s2, 0, 0, 0);
    }
    void set(uint16_t d) {                                 // Set or change divisor, calculate parameters
        uint16_t L, L2, sh1, sh2, m;
        switch (d) {
        case 0:
            m = sh1 = sh2 = 1u / d;                        // provoke error for d = 0
            break;
        case 1:
            m = 1; sh1 = sh2 = 0;                          // parameters for d = 1
            break;
        case 2:
            m = 1; sh1 = 1; sh2 = 0;                       // parameters for d = 2
            break;
        default:                                           // general case for d > 2
            L = (uint16_t)bit_scan_reverse(d - 1u) + 1u;   // ceil(log2(d))
            L2 = uint16_t(1 << L);                         // 2^L, overflow to 0 if L = 16
            m = 1u + uint16_t((uint32_t(L2 - d) << 16) / d); // multiplier
            sh1 = 1;  sh2 = L - 1u;                        // shift counts
        }
        multiplier = _mm_set1_epi16((int16_t)m);
        shift1 = _mm_setr_epi32((int32_t)sh1, 0, 0, 0);
        shift2 = _mm_setr_epi32((int32_t)sh2, 0, 0, 0);
    }
    __m128i getm() const {                                 // get multiplier
        return multiplier;
    }
    __m128i gets1() const {                                // get shift count 1
        return shift1;
    }
    __m128i gets2() const {                                // get shift count 2
        return shift2;
    }
};


// vector operator / : divide each element by divisor

// vector of 4 32-bit signed integers
static inline Vec4i operator / (Vec4i const a, Divisor_i const d) {
#if INSTRSET >= 5
    __m128i t1 = _mm_mul_epi32(a, d.getm());               // 32x32->64 bit signed multiplication of a[0] and a[2]
    __m128i t2 = _mm_srli_epi64(t1, 32);                   // high dword of result 0 and 2
    __m128i t3 = _mm_srli_epi64(a, 32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4 = _mm_mul_epi32(t3, d.getm());              // 32x32->64 bit signed multiplication of a[1] and a[3]
    __m128i t7 = _mm_blend_epi16(t2, t4, 0xCC);
    __m128i t8 = _mm_add_epi32(t7, a);                     // add
    __m128i t9 = _mm_sra_epi32(t8, d.gets1());             // shift right arithmetic
    __m128i t10 = _mm_srai_epi32(a, 31);                   // sign of a
    __m128i t11 = _mm_sub_epi32(t10, d.getsign());         // sign of a - sign of d
    __m128i t12 = _mm_sub_epi32(t9, t11);                  // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t12, d.getsign());         // change sign if divisor negative
#else  // not SSE4.1
    __m128i t1 = _mm_mul_epu32(a, d.getm());               // 32x32->64 bit unsigned multiplication of a[0] and a[2]
    __m128i t2 = _mm_srli_epi64(t1, 32);                   // high dword of result 0 and 2
    __m128i t3 = _mm_srli_epi64(a, 32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4 = _mm_mul_epu32(t3, d.getm());              // 32x32->64 bit unsigned multiplication of a[1] and a[3]
    __m128i t5 = _mm_set_epi32(-1, 0, -1, 0);              // mask of dword 1 and 3
    __m128i t6 = _mm_and_si128(t4, t5);                    // high dword of result 1 and 3
    __m128i t7 = _mm_or_si128(t2, t6);                     // combine all four results of unsigned high mul into one vector
    // convert unsigned to signed high multiplication (from: H S Warren: Hacker's delight, 2003, p. 132)
    __m128i u1 = _mm_srai_epi32(a, 31);                    // sign of a
    __m128i u2 = _mm_srai_epi32(d.getm(), 31);             // sign of m [ m is always negative, except for abs(d) = 1 ]
    __m128i u3 = _mm_and_si128(d.getm(), u1);              // m * sign of a
    __m128i u4 = _mm_and_si128(a, u2);                     // a * sign of m
    __m128i u5 = _mm_add_epi32(u3, u4);                    // sum of sign corrections
    __m128i u6 = _mm_sub_epi32(t7, u5);                    // high multiplication result converted to signed
    __m128i t8 = _mm_add_epi32(u6, a);                     // add a
    __m128i t9 = _mm_sra_epi32(t8, d.gets1());             // shift right arithmetic
    __m128i t10 = _mm_sub_epi32(u1, d.getsign());          // sign of a - sign of d
    __m128i t11 = _mm_sub_epi32(t9, t10);                  // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t11, d.getsign());         // change sign if divisor negative
#endif
}

// vector of 4 32-bit unsigned integers
static inline Vec4ui operator / (Vec4ui const a, Divisor_ui const d) {
    __m128i t1 = _mm_mul_epu32(a, d.getm());               // 32x32->64 bit unsigned multiplication of a[0] and a[2]
    __m128i t2 = _mm_srli_epi64(t1, 32);                   // high dword of result 0 and 2
    __m128i t3 = _mm_srli_epi64(a, 32);                    // get a[1] and a[3] into position for multiplication
    __m128i t4 = _mm_mul_epu32(t3, d.getm());              // 32x32->64 bit unsigned multiplication of a[1] and a[3]
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i t7 = _mm_blend_epi16(t2, t4, 0xCC);            // blend two results
#else
    __m128i t5 = _mm_set_epi32(-1, 0, -1, 0);              // mask of dword 1 and 3
    __m128i t6 = _mm_and_si128(t4, t5);                    // high dword of result 1 and 3
    __m128i t7 = _mm_or_si128(t2, t6);                     // combine all four results into one vector
#endif
    __m128i t8 = _mm_sub_epi32(a, t7);                     // subtract
    __m128i t9 = _mm_srl_epi32(t8, d.gets1());             // shift right logical
    __m128i t10 = _mm_add_epi32(t7, t9);                   // add
    return        _mm_srl_epi32(t10, d.gets2());           // shift right logical 
}

// vector of 8 16-bit signed integers
static inline Vec8s operator / (Vec8s const a, Divisor_s const d) {
    __m128i t1 = _mm_mulhi_epi16(a, d.getm());             // multiply high signed words
    __m128i t2 = _mm_add_epi16(t1, a);                     // + a
    __m128i t3 = _mm_sra_epi16(t2, d.gets1());             // shift right arithmetic
    __m128i t4 = _mm_srai_epi16(a, 15);                    // sign of a
    __m128i t5 = _mm_sub_epi16(t4, d.getsign());           // sign of a - sign of d
    __m128i t6 = _mm_sub_epi16(t3, t5);                    // + 1 if a < 0, -1 if d < 0
    return        _mm_xor_si128(t6, d.getsign());          // change sign if divisor negative
}

// vector of 8 16-bit unsigned integers
static inline Vec8us operator / (Vec8us const a, Divisor_us const d) {
    __m128i t1 = _mm_mulhi_epu16(a, d.getm());             // multiply high unsigned words
    __m128i t2 = _mm_sub_epi16(a, t1);                     // subtract
    __m128i t3 = _mm_srl_epi16(t2, d.gets1());             // shift right logical
    __m128i t4 = _mm_add_epi16(t1, t3);                    // add
    return        _mm_srl_epi16(t4, d.gets2());            // shift right logical 
}


// vector of 16 8-bit signed integers
static inline Vec16c operator / (Vec16c const a, Divisor_s const d) {
    // expand into two Vec8s
    Vec8s low = extend_low(a) / d;
    Vec8s high = extend_high(a) / d;
    return compress(low, high);
}

// vector of 16 8-bit unsigned integers
static inline Vec16uc operator / (Vec16uc const a, Divisor_us const d) {
    // expand into two Vec8s
    Vec8us low = extend_low(a) / d;
    Vec8us high = extend_high(a) / d;
    return compress(low, high);
}

// vector operator /= : divide
static inline Vec8s & operator /= (Vec8s & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec8us & operator /= (Vec8us & a, Divisor_us const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec4i & operator /= (Vec4i & a, Divisor_i const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec4ui & operator /= (Vec4ui & a, Divisor_ui const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16c & operator /= (Vec16c & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16uc & operator /= (Vec16uc & a, Divisor_us const d) {
    a = a / d;
    return a;
}

/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec4i by compile-time constant
template <int32_t d>
static inline Vec4i divide_by_i(Vec4i const x) {
    static_assert(d != 0, "Integer division by zero");     // Error message if dividing by zero
    if constexpr (d == 1) return  x;
    if constexpr (d == -1) return -x;
    if constexpr (uint32_t(d) == 0x80000000u) return Vec4i(x == Vec4i(0x80000000)) & 1; // prevent overflow when changing sign
    constexpr uint32_t d1 = d > 0 ? uint32_t(d) : uint32_t(-d);// compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if constexpr ((d1 & (d1 - 1)) == 0) {
        // d1 is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(d1);
        __m128i sign;
        if constexpr (k > 1) sign = _mm_srai_epi32(x, k-1); else sign = x;// k copies of sign bit
        __m128i bias = _mm_srli_epi32(sign, 32 - k);       // bias = x >= 0 ? 0 : k-1
        __m128i xpbias = _mm_add_epi32(x, bias);           // x + bias
        __m128i q = _mm_srai_epi32(xpbias, k);             // (x + bias) >> k
        if (d > 0)      return q;                          // d > 0: return  q
        return _mm_sub_epi32(_mm_setzero_si128(), q);      // d < 0: return -q
    }
    // general case
    constexpr int32_t sh = bit_scan_reverse_const(uint32_t(d1) - 1); // ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    constexpr int32_t mult = int(1 + (uint64_t(1) << (32 + sh)) / uint32_t(d1) - (int64_t(1) << 32));   // multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec4i a / const_int(d)
template <int32_t d>
static inline Vec4i operator / (Vec4i const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec4i a / const_uint(d)
template <uint32_t d>
static inline Vec4i operator / (Vec4i const a, Const_uint_t<d>) {
    static_assert(d < 0x80000000u, "Dividing signed by overflowing unsigned");
    return divide_by_i<int32_t(d)>(a);                     // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec4i & operator /= (Vec4i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec4i & operator /= (Vec4i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec4ui by compile-time constant
template <uint32_t d>
static inline Vec4ui divide_by_ui(Vec4ui const x) {
    static_assert(d != 0, "Integer division by zero");     // Error message if dividing by zero
    if constexpr (d == 1) return x;                        // divide by 1
    constexpr int b = bit_scan_reverse_const(d);           // floor(log2(d))
    if constexpr ((uint32_t(d) & (uint32_t(d) - 1)) == 0) {
        // d is a power of 2. use shift
        return    _mm_srli_epi32(x, b);                    // x >> b
    }
    // general case (d > 2)
    constexpr uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d); // multiplier = 2^(32+b) / d
    constexpr uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    constexpr bool round_down = (2 * rem < d);             // check if fraction is less than 0.5
    constexpr uint32_t mult1 = round_down ? mult : mult + 1;
    // do 32*32->64 bit unsigned multiplication and get high part of result
#if INSTRSET >= 10
    const __m128i multv = _mm_maskz_set1_epi32(0x05, mult1);// zero-extend mult and broadcast
#else
    const __m128i multv = Vec2uq(uint64_t(mult1));         // zero-extend mult and broadcast
#endif
    __m128i t1 = _mm_mul_epu32(x, multv);                  // 32x32->64 bit unsigned multiplication of x[0] and x[2]
    if constexpr (round_down) {
        t1 = _mm_add_epi64(t1, multv);                     // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m128i t2 = _mm_srli_epi64(t1, 32);                   // high dword of result 0 and 2
    __m128i t3 = _mm_srli_epi64(x, 32);                    // get x[1] and x[3] into position for multiplication
    __m128i t4 = _mm_mul_epu32(t3, multv);                 // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if constexpr (round_down) {
        t4 = _mm_add_epi64(t4, multv);                     // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
#if INSTRSET >= 5   // SSE4.1 supported
    __m128i t7 = _mm_blend_epi16(t2, t4, 0xCC);            // blend two results
#else
    __m128i t5 = _mm_set_epi32(-1, 0, -1, 0);              // mask of dword 1 and 3
    __m128i t6 = _mm_and_si128(t4, t5);                    // high dword of result 1 and 3
    __m128i t7 = _mm_or_si128(t2, t6);                     // combine all four results into one vector
#endif
    Vec4ui q = _mm_srli_epi32(t7, b);                      // shift right by b
    return q;                                              // no overflow possible
}

// define Vec4ui a / const_uint(d)
template <uint32_t d>
static inline Vec4ui operator / (Vec4ui const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec4ui a / const_int(d)
template <int32_t d>
static inline Vec4ui operator / (Vec4ui const a, Const_int_t<d>) {
    static_assert(d < 0x80000000u, "Dividing unsigned integer by negative value is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec4ui & operator /= (Vec4ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec4ui & operator /= (Vec4ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8s by compile-time constant 
template <int d>
static inline Vec8s divide_by_i(Vec8s const x) {
    constexpr int16_t d0 = int16_t(d);                     // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");    // Error message if dividing by zero
    if constexpr (d0 == 1) return  x;                      // divide by  1
    if constexpr (d0 == -1) return -x;                     // divide by -1
    if constexpr (uint16_t(d0) == 0x8000u) return Vec8s(x == Vec8s(0x8000)) & 1;// prevent overflow when changing sign
    // if (d > 0x7FFF || d < -0x8000) return 0;            // not relevant when d truncated to 16 bits
    const uint32_t d1 = d > 0 ? uint32_t(d) : uint32_t(-d);// compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if constexpr ((d1 & (d1 - 1)) == 0) {
        // d is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(uint32_t(d1));
        __m128i sign;
        if constexpr (k > 1) sign = _mm_srai_epi16(x, k-1); else sign = x;// k copies of sign bit
        __m128i bias = _mm_srli_epi16(sign, 16 - k);       // bias = x >= 0 ? 0 : k-1
        __m128i xpbias = _mm_add_epi16(x, bias);           // x + bias
        __m128i q = _mm_srai_epi16(xpbias, k);             // (x + bias) >> k
        if (d0 > 0)  return q;                             // d0 > 0: return  q
        return _mm_sub_epi16(_mm_setzero_si128(), q);      // d0 < 0: return -q
    }
    // general case
    const int L = bit_scan_reverse_const(uint16_t(d1 - 1)) + 1;// ceil(log2(d)). (d < 2 handled above)
    const int16_t mult = int16_t(1 + (1u << (15 + L)) / uint32_t(d1) - 0x10000);// multiplier
    const int shift1 = L - 1;
    const Divisor_s div(mult, shift1, d0 > 0 ? 0 : -1);
    return x / div;
}

// define Vec8s a / const_int(d)
template <int d>
static inline Vec8s operator / (Vec8s const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec8s a / const_uint(d)
template <uint32_t d>
static inline Vec8s operator / (Vec8s const a, Const_uint_t<d>) {
    static_assert(d < 0x8000u, "Dividing signed by overflowing unsigned");
    return divide_by_i<int(d)>(a);                         // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8s & operator /= (Vec8s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8s & operator /= (Vec8s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8us by compile-time constant
template <uint32_t d>
static inline Vec8us divide_by_ui(Vec8us const x) {
    constexpr uint16_t d0 = uint16_t(d);                   // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");    // Error message if dividing by zero
    if constexpr (d0 == 1) return x;                       // divide by 1
    constexpr int b = bit_scan_reverse_const(d0);          // floor(log2(d))
    if constexpr ((d0 & (d0 - 1u)) == 0) {
        // d is a power of 2. use shift
        return  _mm_srli_epi16(x, b);                      // x >> b
    }
    // general case (d > 2)
    constexpr uint16_t mult = uint16_t((1u << uint32_t(b+16)) / d0); // multiplier = 2^(32+b) / d
    constexpr uint32_t rem = (uint32_t(1) << uint32_t(b + 16)) - uint32_t(d0) * mult;// remainder 2^(32+b) % d
    constexpr bool round_down = (2u * rem < d0);           // check if fraction is less than 0.5
    Vec8us x1 = x;
    if (round_down) {
        x1 = x1 + 1u;                                      // round down mult and compensate by adding 1 to x
    }
    constexpr uint16_t mult1 = round_down ? mult : mult + 1;
    const __m128i multv = _mm_set1_epi16((int16_t)mult1);  // broadcast mult
    __m128i xm = _mm_mulhi_epu16(x1, multv);               // high part of 16x16->32 bit unsigned multiplication
    Vec8us q = _mm_srli_epi16(xm, (int)b);                 // shift right by b
    if constexpr (round_down) {
        Vec8sb overfl = (x1 == (Vec8us)_mm_setzero_si128());// check for overflow of x+1
        return select(overfl, Vec8us(uint16_t(mult1 >> (uint16_t)b)), q); // deal with overflow (rarely needed)
    }
    else {
        return q;                                          // no overflow possible
    }
}

// define Vec8us a / const_uint(d)
template <uint32_t d>
static inline Vec8us operator / (Vec8us const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8us a / const_int(d)
template <int d>
static inline Vec8us operator / (Vec8us const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8us & operator /= (Vec8us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8us & operator /= (Vec8us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// define Vec16c a / const_int(d)
template <int d>
static inline Vec16c operator / (Vec16c const a, Const_int_t<d>) {
    // expand into two Vec8s
    Vec8s low = extend_low(a) / Const_int_t<d>();
    Vec8s high = extend_high(a) / Const_int_t<d>();
    return compress(low, high);
}

// define Vec16c a / const_uint(d)
template <uint32_t d>
static inline Vec16c operator / (Vec16c const a, Const_uint_t<d>) {
    static_assert (uint8_t(d) < 0x80u, "Dividing signed integer by overflowing unsigned");
    return a / Const_int_t<d>();                           // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16c & operator /= (Vec16c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec16c & operator /= (Vec16c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec16uc a / const_uint(d)
template <uint32_t d>
static inline Vec16uc operator / (Vec16uc const a, Const_uint_t<d>) {
    // expand into two Vec8usc
    Vec8us low = extend_low(a) / Const_uint_t<d>();
    Vec8us high = extend_high(a) / Const_uint_t<d>();
    return compress(low, high);
}

// define Vec16uc a / const_int(d)
template <int d>
static inline Vec16uc operator / (Vec16uc const a, Const_int_t<d>) {
    static_assert (int8_t(d) >= 0, "Dividing unsigned integer by negative is ambiguous");
    return a / Const_uint_t<d>();                          // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16uc & operator /= (Vec16uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16uc & operator /= (Vec16uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

#if INSTRSET >= 9  // compact boolean vector Vec16b

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16b const x) {
    return __mmask16(x);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8b const x) {
    return uint8_t(Vec8b_masktype(x));
}

#endif

#if INSTRSET >= 10  // compact boolean vectors, other sizes

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4b const x) {
    return __mmask8(x) & 0x0F;
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2b const x) {
    return __mmask8(x) & 0x03;
}

#else  // broad boolean vectors

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16cb const x) {
    return (uint16_t)_mm_movemask_epi8(x);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8sb const x) {
    __m128i a = _mm_packs_epi16(x, x);  // 16-bit words to bytes
    return (uint8_t)_mm_movemask_epi8(a);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4ib const x) {
    __m128i a = _mm_packs_epi32(x, x);  // 32-bit dwords to 16-bit words
    __m128i b = _mm_packs_epi16(a, a);  // 16-bit words to bytes
    return uint8_t(_mm_movemask_epi8(b) & 0xF);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec2qb const x) {
    uint32_t a = (uint32_t)_mm_movemask_epi8(x);
    return (a & 1) | ((a >> 7) & 2);
}

#endif

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI128_H
