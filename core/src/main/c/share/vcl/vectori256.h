/****************************  vectori256.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-18
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining integer vector classes as interface to intrinsic 
* functions in x86 microprocessors with AVX2 and later instruction sets.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec256b   Vector of 256  bits. Used internally as base class
* Vec32c    Vector of  32  8-bit signed    integers
* Vec32uc   Vector of  32  8-bit unsigned  integers
* Vec32cb   Vector of  32  Booleans for use with Vec32c and Vec32uc
* Vec16s    Vector of  16  16-bit signed   integers
* Vec16us   Vector of  16  16-bit unsigned integers
* Vec16sb   Vector of  16  Booleans for use with Vec16s and Vec16us
* Vec8i     Vector of   8  32-bit signed   integers
* Vec8ui    Vector of   8  32-bit unsigned integers
* Vec8ib    Vector of   8  Booleans for use with Vec8i and Vec8ui
* Vec4q     Vector of   4  64-bit signed   integers
* Vec4uq    Vector of   4  64-bit unsigned integers
* Vec4qb    Vector of   4  Booleans for use with Vec4q and Vec4uq
*
* Each vector object is represented internally in the CPU as a 256-bit register.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORI256_H
#define VECTORI256_H 1

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#if defined (VECTORI256E_H)
#error Two different versions of vectori256.h included
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

// Generate a constant vector of 8 integers stored in memory.
template <uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7 >
    static inline constexpr __m256i constant8ui() {
    /*
    const union {
        uint32_t i[8];
        __m256i ymm;
    } u = { {i0,i1,i2,i3,i4,i5,i6,i7} };
    return u.ymm;
    */
    return _mm256_setr_epi32(i0,i1,i2,i3,i4,i5,i6,i7);
}


// Join two 128-bit vectors
#define set_m128ir(lo,hi) _mm256_inserti128_si256(_mm256_castsi128_si256(lo),(hi),1)

/*****************************************************************************
*
*         Compact boolean vectors
*
*****************************************************************************/

#if INSTRSET >= 10  // 32-bit and 64-bit masks require AVX512BW

// Compact vector of 32 booleans
class Vec32b {
protected:
    __mmask32  mm; // Boolean mask register
public:
    // Default constructor:
    Vec32b() {
    }
    // Constructor to convert from type __mmask32 used in intrinsics
    // Made explicit to prevent implicit conversion from int
    Vec32b(__mmask32 x) {
        mm = x;
    }
    /*
    // Constructor to build from all elements:
    Vec32b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7,
        bool b8, bool b9, bool b10, bool b11, bool b12, bool b13, bool b14, bool b15,
        bool b16, bool b17, bool b18, bool b19, bool b20, bool b21, bool b22, bool b23,
        bool b24, bool b25, bool b26, bool b27, bool b28, bool b29, bool b30, bool b31) {
        mm = uint32_t(
            (uint32_t)b0 | (uint32_t)b1 << 1 | (uint32_t)b2 << 2 | (uint32_t)b3 << 3 |
            (uint32_t)b4 << 4 | (uint32_t)b5 << 5 | (uint32_t)b6 << 6 | (uint32_t)b7 << 7 |
            (uint32_t)b8 << 8 | (uint32_t)b9 << 9 | (uint32_t)b10 << 10 | (uint32_t)b11 << 11 |
            (uint32_t)b12 << 12 | (uint32_t)b13 << 13 | (uint32_t)b14 << 14 | (uint32_t)b15 << 15 |
            (uint32_t)b16 << 16 | (uint32_t)b17 << 17 | (uint32_t)b18 << 18 | (uint32_t)b19 << 19 |
            (uint32_t)b20 << 20 | (uint32_t)b21 << 21 | (uint32_t)b22 << 22 | (uint32_t)b23 << 23 |
            (uint32_t)b24 << 24 | (uint32_t)b25 << 25 | (uint32_t)b26 << 26 | (uint32_t)b27 << 27 |
            (uint32_t)b28 << 28 | (uint32_t)b29 << 29 | (uint32_t)b30 << 30 | (uint32_t)b31 << 31);
    } */
    // Constructor to broadcast single value:
    Vec32b(bool b) {
        mm = __mmask32(-int32_t(b));
    }
    // Constructor to make from two halves
    Vec32b(Vec16b const x0, Vec16b const x1) {
        mm = uint16_t(__mmask16(x0)) | uint32_t(__mmask16(x1)) << 16;
    }
    // Assignment operator to convert from type __mmask32 used in intrinsics:
    Vec32b & operator = (__mmask32 x) {
        mm = x;
        return *this;
    }
    // Assignment operator to broadcast scalar value:
    Vec32b & operator = (bool b) {
        mm = Vec32b(b);
        return *this;
    }
    // Type cast operator to convert to __mmask32 used in intrinsics
    operator __mmask32() const {
        return mm;
    }
    // split into two halves
    Vec16b get_low() const {
        return Vec16b(__mmask16(mm));
    }
    Vec16b get_high() const {
        return Vec16b(__mmask16(mm >> 16));
    }
    // Member function to change a single element in vector
    Vec32b const insert(int index, bool value) {
        mm = __mmask32(((uint32_t)mm & ~(1 << index)) | (uint32_t)value << index);
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
    Vec32b & load_bits(uint32_t a) {
        mm = __mmask32(a);
        return *this;
    }
    // Number of elements
    static constexpr int size() {
        return 32;
    }
    // Type of elements
    static constexpr int elementtype() {
        return 2;
    }
};

#endif


/*****************************************************************************
*
*          Vector of 256 bits. Used as base class
*
*****************************************************************************/

class Vec256b {
protected:
    __m256i ymm; // Integer vector
public:
    // Default constructor:
    Vec256b() {}

    // Constructor to broadcast the same value into all elements
    // Removed because of undesired implicit conversions:
    //Vec256b(int i) {ymm = _mm256_set1_epi32(-(i & 1));}

    // Constructor to build from two Vec128b:
    Vec256b(Vec128b const a0, Vec128b const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec256b(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec256b & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec256b & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    Vec256b & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm256_storeu_si256((__m256i*)p, ymm);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(void * p) const {
        _mm256_stream_si256((__m256i*)p, ymm);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 32;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    void store_a(void * p) const {
        _mm256_store_si256((__m256i*)p, ymm);
    }
    // Member functions to split into two Vec128b:
    Vec128b get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec128b get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int size() {
        return 256;
    }
    static constexpr int elementtype() {
        return 1;
    }
    typedef __m256i registertype;
};


// Define operators and functions for this class

// vector operator & : bitwise and
static inline Vec256b operator & (Vec256b const a, Vec256b const b) {
    return _mm256_and_si256(a, b);
}
static inline Vec256b operator && (Vec256b const a, Vec256b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec256b operator | (Vec256b const a, Vec256b const b) {
    return _mm256_or_si256(a, b);
}
static inline Vec256b operator || (Vec256b const a, Vec256b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec256b operator ^ (Vec256b const a, Vec256b const b) {
    return _mm256_xor_si256(a, b);
}

// vector operator ~ : bitwise not
static inline Vec256b operator ~ (Vec256b const a) {
    return _mm256_xor_si256(a, _mm256_set1_epi32(-1));
}

// vector operator &= : bitwise and
static inline Vec256b & operator &= (Vec256b & a, Vec256b const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec256b & operator |= (Vec256b & a, Vec256b const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec256b & operator ^= (Vec256b & a, Vec256b const b) {
    a = a ^ b;
    return a;
}

// function andnot: a & ~ b
static inline Vec256b andnot (Vec256b const a, Vec256b const b) {
    return _mm256_andnot_si256(b, a);
}


/*****************************************************************************
*
*          selectb function
*
*****************************************************************************/
// Select between two sources, byte by byte. Used in various functions and operators
// Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFF (true). No other values are allowed.
// Only bit 7 in each byte of s is checked, 
static inline __m256i selectb (__m256i const s, __m256i const a, __m256i const b) {
    return _mm256_blendv_epi8 (b, a, s);
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec256b const a) {
    return _mm256_testc_si256(a,_mm256_set1_epi32(-1)) != 0;
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec256b const a) {
    return ! _mm256_testz_si256(a,a);
}


/*****************************************************************************
*
*          Vector of 32 8-bit signed integers
*
*****************************************************************************/

class Vec32c : public Vec256b {
public:
    // Default constructor:
    Vec32c(){
    }
    // Constructor to broadcast the same value into all elements:
    Vec32c(int i) {
        ymm = _mm256_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec32c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31) {
        ymm = _mm256_setr_epi8(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    }
    // Constructor to build from two Vec16c:
    Vec32c(Vec16c const a0, Vec16c const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32c(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32c & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to convert from type Vec256b used in emulation
    Vec32c(Vec256b const & x) {  // gcc requires const &
        ymm = x;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec32c & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32c & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec32c & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_epi8(__mmask32(((uint64_t)1 << n) - 1), p);
#else
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 16) {
            *this = Vec32c(Vec16c().load_partial(n, p), 0);
        }
        else if (n < 32) {
            *this = Vec32c(Vec16c().load(p), Vec16c().load_partial(n-16, (char const*)p+16));
        }
        else {
            load(p);
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm256_mask_storeu_epi8(p, __mmask32(((uint64_t)1 << n) - 1), ymm);
#else 
        if (n <= 0) {
            return;
        }
        else if (n <= 16) {
            get_low().store_partial(n, p);
        }
        else if (n < 32) {
            get_low().store(p);
            get_high().store_partial(n-16, (char*)p+16);
        }
        else {
            store(p);
        }
#endif
    }
    // cut off vector to n elements. The last 32-n elements are set to zero
    Vec32c & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_epi8(__mmask32(((uint64_t)1 << n) - 1), ymm);
#else
        if (uint32_t(n) >= 32) return *this;
        const union {
            int32_t i[16];
            char    c[64];
        } mask = {{-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0}};
        *this &= Vec32c().load(mask.c+32-n);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec32c const insert(int index, int8_t value) {
#if INSTRSET >= 10
        ymm = _mm256_mask_set1_epi8(ymm, __mmask32(1u << index), value);
#else 
        const int8_t maskl[64] = {0,0,0,0, 0,0,0,0, 0,0,0,0 ,0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0,
            -1,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0, 0,0,0,0 ,0,0,0,0, 0,0,0,0, 0,0,0,0};
        __m256i broad = _mm256_set1_epi8(value);  // broadcast value into all elements
        __m256i mask  = _mm256_loadu_si256((__m256i const*)(maskl+32-(index & 0x1F))); // mask with FF at index position
        ymm = selectb(mask,broad,ymm);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m256i x = _mm256_maskz_compress_epi8(__mmask32(1u << index), ymm);
        return (int8_t)_mm_cvtsi128_si32(_mm256_castsi256_si128(x));        
#else 
        int8_t x[32];
        store(x);
        return x[index & 0x1F];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16c:
    Vec16c get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec16c get_high() const {
        return _mm256_extracti128_si256(ymm,1);
    }
    static constexpr int size() {
        return 32;
    }
    static constexpr int elementtype() {
        return 4;
    }
};


/*****************************************************************************
*
*          Vec32cb: Vector of 32 Booleans for use with Vec32c and Vec32uc
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors
class Vec32cb : public Vec32c {
public:
    // Default constructor:
    Vec32cb(){
    }
    // Constructor to build from all elements:
    /*
    Vec32cb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15,
        bool x16, bool x17, bool x18, bool x19, bool x20, bool x21, bool x22, bool x23,
        bool x24, bool x25, bool x26, bool x27, bool x28, bool x29, bool x30, bool x31) :
        Vec32c(-int8_t(x0), -int8_t(x1), -int8_t(x2), -int8_t(x3), -int8_t(x4), -int8_t(x5), -int8_t(x6), -int8_t(x7), 
            -int8_t(x8), -int8_t(x9), -int8_t(x10), -int8_t(x11), -int8_t(x12), -int8_t(x13), -int8_t(x14), -int8_t(x15),
            -int8_t(x16), -int8_t(x17), -int8_t(x18), -int8_t(x19), -int8_t(x20), -int8_t(x21), -int8_t(x22), -int8_t(x23),
            -int8_t(x24), -int8_t(x25), -int8_t(x26), -int8_t(x27), -int8_t(x28), -int8_t(x29), -int8_t(x30), -int8_t(x31))
        {} */
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32cb(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32cb & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec32cb(bool b) : Vec32c(-int8_t(b)) {
    }
    // Constructor to convert from Vec32c
    Vec32cb(Vec32c const a) {
        ymm = a;
    }    
    // Assignment operator to broadcast scalar value:
    Vec32cb & operator = (bool b) {
        *this = Vec32cb(b);
        return *this;
    }
    // Constructor to build from two Vec16cb:
    Vec32cb(Vec16cb const a0, Vec16cb const a1) : Vec32c(Vec16c(a0), Vec16c(a1)) {
    }
    // Member functions to split into two Vec16c:
    Vec16cb get_low() const {
        return Vec16cb(Vec32c::get_low());
    }
    Vec16cb get_high() const {
        return Vec16cb(Vec32c::get_high());
    }
    Vec32cb & insert (int index, bool a) {
        Vec32c::insert(index, -(int8_t)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec32c::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec32cb & load_bits(uint32_t a) {
        __m256i b1 = _mm256_set1_epi32((int32_t)~a);       // broadcast a. Invert because we have no compare-not-equal
        __m256i m1 = constant8ui<0,0,0x01010101,0x01010101,0x02020202,0x02020202,0x03030303,0x03030303>(); 
        __m256i c1 = _mm256_shuffle_epi8(b1, m1);          // get right byte in each position
        __m256i m2 = constant8ui<0x08040201,0x80402010,0x08040201,0x80402010,0x08040201,0x80402010,0x08040201,0x80402010>(); 
        __m256i d1 = _mm256_and_si256(c1, m2);             // isolate one bit in each byte
        ymm = _mm256_cmpeq_epi8(d1,_mm256_setzero_si256());// compare with 0
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec32cb(int b) = delete;
    Vec32cb & operator = (int x) = delete;
};
#else

typedef Vec32b Vec32cb;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Define operators and functions for Vec32b or Vec32cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec32cb operator & (Vec32cb const a, Vec32cb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(__mmask32(a) & __mmask32(b)); // _kand_mask32 not defined in all compilers
#else
    return Vec32c(Vec256b(a) & Vec256b(b));
#endif
}
static inline Vec32cb operator && (Vec32cb const a, Vec32cb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec32cb & operator &= (Vec32cb & a, Vec32cb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32cb operator | (Vec32cb const a, Vec32cb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(__mmask32(a) | __mmask32(b)); // _kor_mask32    
#else
    return Vec32c(Vec256b(a) | Vec256b(b));
#endif
}
static inline Vec32cb operator || (Vec32cb const a, Vec32cb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec32cb & operator |= (Vec32cb & a, Vec32cb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32cb operator ^ (Vec32cb const a, Vec32cb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(__mmask32(a) ^ __mmask32(b)); // _kxor_mask32    
#else
    return Vec32c(Vec256b(a) ^ Vec256b(b));
#endif
}
// vector operator ^= : bitwise xor
static inline Vec32cb & operator ^= (Vec32cb & a, Vec32cb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec32cb operator == (Vec32cb const a, Vec32cb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(__mmask32(a) ^ ~__mmask32(b)); // _kxnor_mask32    
#else
    return Vec32c(a ^ (~b));
#endif
}

// vector operator != : xor
static inline Vec32cb operator != (Vec32cb const a, Vec32cb const b) {
    return Vec32cb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec32cb operator ~ (Vec32cb const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(~ __mmask32(a)); // _knot_mask32    
#else
    return Vec32c( ~ Vec256b(a));
#endif
}

// vector operator ! : element not
static inline Vec32cb operator ! (Vec32cb const a) {
    return ~ a;
}

// vector function andnot
static inline Vec32cb andnot (Vec32cb const a, Vec32cb const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return __mmask32(~__mmask32(b) & __mmask32(a)); // _kandn_mask32    
#else
    return Vec32c(andnot(Vec256b(a), Vec256b(b)));
#endif
}

#if INSTRSET >= 10  // compact boolean vectors

// horizontal_and. Returns true if all elements are true
static inline bool horizontal_and(Vec32b const a) {
    return __mmask32(a) == 0xFFFFFFFF;
}

// horizontal_or. Returns true if at least one element is true
static inline bool horizontal_or(Vec32b const a) {
    return __mmask32(a) != 0;
}

// fix bug in gcc version 70400 header file: _mm256_cmp_epi8_mask returns 16 bit mask, should be 32 bit
template <int i>
static inline __mmask32 _mm256_cmp_epi8_mask_fix(__m256i a, __m256i b) {
#if defined (GCC_VERSION) && GCC_VERSION < 70900 &&  ! defined (__INTEL_COMPILER)
    return (__mmask32) __builtin_ia32_cmpb256_mask ((__v32qi)a, (__v32qi)b, i, (__mmask32)(-1)); 
#else
    return _mm256_cmp_epi8_mask(a, b, i);
#endif
}

template <int i>
static inline __mmask32 _mm256_cmp_epu8_mask_fix(__m256i a, __m256i b) {
#if defined (GCC_VERSION) && GCC_VERSION < 70900 &&  ! defined (__INTEL_COMPILER)
    return (__mmask32) __builtin_ia32_ucmpb256_mask ((__v32qi)a, (__v32qi)b, i, (__mmask32)(-1)); 
#else
    return _mm256_cmp_epu8_mask(a, b, i);
#endif
}

#endif


/*****************************************************************************
*
*          Operators for Vec32c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec32c operator + (Vec32c const a, Vec32c const b) {
    return _mm256_add_epi8(a, b);
}
// vector operator += : add
static inline Vec32c & operator += (Vec32c & a, Vec32c const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec32c operator ++ (Vec32c & a, int) {
    Vec32c a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec32c & operator ++ (Vec32c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec32c operator - (Vec32c const a, Vec32c const b) {
    return _mm256_sub_epi8(a, b);
}
// vector operator - : unary minus
static inline Vec32c operator - (Vec32c const a) {
    return _mm256_sub_epi8(_mm256_setzero_si256(), a);
}
// vector operator -= : add
static inline Vec32c & operator -= (Vec32c & a, Vec32c const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec32c operator -- (Vec32c & a, int) {
    Vec32c a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec32c & operator -- (Vec32c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec32c operator * (Vec32c const a, Vec32c const b) {
    // There is no 8-bit multiply in AVX2. Split into two 16-bit multiplications
    __m256i aodd    = _mm256_srli_epi16(a,8);              // odd numbered elements of a
    __m256i bodd    = _mm256_srli_epi16(b,8);              // odd numbered elements of b
    __m256i muleven = _mm256_mullo_epi16(a,b);             // product of even numbered elements
    __m256i mulodd  = _mm256_mullo_epi16(aodd,bodd);       // product of odd  numbered elements
            mulodd  = _mm256_slli_epi16(mulodd,8);         // put odd numbered elements back in place
#if INSTRSET >= 10   // AVX512VL + AVX512BW
    return _mm256_mask_mov_epi8(mulodd, 0x55555555, muleven);
#else 
    __m256i mask    = _mm256_set1_epi32(0x00FF00FF);       // mask for even positions
    __m256i product = selectb(mask,muleven,mulodd);        // interleave even and odd
    return product;
#endif
}

// vector operator *= : multiply
static inline Vec32c & operator *= (Vec32c & a, Vec32c const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32c operator << (Vec32c const a, int b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;                   // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_sll_epi16(am,_mm_cvtsi32_si128(b));       // 16-bit shifts
    return res;
}

// vector operator <<= : shift left
static inline Vec32c & operator <<= (Vec32c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec32c operator >> (Vec32c const a, int b) {
    __m256i aeven = _mm256_slli_epi16(a,8);                          // even numbered elements of a. get sign bit in position
            aeven = _mm256_sra_epi16(aeven,_mm_cvtsi32_si128(b+8));  // shift arithmetic, back to position
    __m256i aodd  = _mm256_sra_epi16(a,_mm_cvtsi32_si128(b));        // shift odd numbered elements arithmetic
#if INSTRSET >= 10   // AVX512VL + AVX512BW
    return _mm256_mask_mov_epi8(aodd, 0x55555555, aeven);
#else 
    __m256i mask  = _mm256_set1_epi32(0x00FF00FF);                   // mask for even positions
    __m256i res   = selectb(mask,aeven,aodd);                        // interleave even and odd
    return res;
#endif
}

// vector operator >>= : shift right arithmetic
static inline Vec32c & operator >>= (Vec32c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32cb operator == (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    //return _mm256_cmp_epi8_mask (a, b, 0);
    return _mm256_cmp_epi8_mask_fix<0> (a, b);
#else
    return _mm256_cmpeq_epi8(a,b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec32cb operator != (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<4> (a, b);
#else
    return Vec32cb(Vec32c(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec32cb operator > (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<6> (a, b);
#else
    return _mm256_cmpgt_epi8(a,b);
#endif
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec32cb operator < (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<1> (a, b);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32cb operator >= (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<5> (a, b);
#else
    return Vec32cb(Vec32c(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32cb operator <= (Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<2> (a, b);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec32c operator & (Vec32c const a, Vec32c const b) {
    return Vec32c(Vec256b(a) & Vec256b(b));
}
static inline Vec32c operator && (Vec32c const a, Vec32c const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec32c & operator &= (Vec32c & a, Vec32c const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32c operator | (Vec32c const a, Vec32c const b) {
    return Vec32c(Vec256b(a) | Vec256b(b));
}
static inline Vec32c operator || (Vec32c const a, Vec32c const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec32c & operator |= (Vec32c & a, Vec32c const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32c operator ^ (Vec32c const a, Vec32c const b) {
    return Vec32c(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec32c & operator ^= (Vec32c & a, Vec32c const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32c operator ~ (Vec32c const a) {
    return Vec32c( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec32cb operator ! (Vec32c const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi8_mask_fix<0> (a, _mm256_setzero_si256());
#else
    return _mm256_cmpeq_epi8(a,_mm256_setzero_si256());
#endif
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32c select (Vec32cb const s, Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi8(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32c if_add (Vec32cb const f, Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi8 (a, f, a, b);
#else
    return a + (Vec32c(f) & b);
#endif
}

// Conditional subtract
static inline Vec32c if_sub (Vec32cb const f, Vec32c const a, Vec32c const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi8 (a, f, a, b);
#else
    return a - (Vec32c(f) & b);
#endif
}

// Conditional multiply
static inline Vec32c if_mul (Vec32cb const f, Vec32c const a, Vec32c const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int8_t horizontal_add (Vec32c const a) {
    __m256i sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i sum3 = _mm256_add_epi16(sum1,sum2);
    __m128i sum4 = _mm256_extracti128_si256(sum3,1);
    __m128i sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    int8_t  sum6 = (int8_t)_mm_cvtsi128_si32(sum5);                  // truncate to 8 bits
    return  sum6;                                                    // sign extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32c const a) {
    __m256i aeven = _mm256_slli_epi16(a,8);                          // even numbered elements of a. get sign bit in position
            aeven = _mm256_srai_epi16(aeven,8);                      // sign extend even numbered elements
    __m256i aodd  = _mm256_srai_epi16(a,8);                          // sign extend odd  numbered elements
    __m256i sum1  = _mm256_add_epi16(aeven,aodd);                    // add even and odd elements
    __m128i sum2  = _mm_add_epi16(_mm256_extracti128_si256(sum1,1),_mm256_castsi256_si128(sum1));
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
#if false
    __m128i sum3  = _mm_hadd_epi16(sum2,sum2);
    __m128i sum4  = _mm_hadd_epi16(sum3,sum3);
    __m128i sum5  = _mm_hadd_epi16(sum4,sum4);
#else
    __m128i sum3  = _mm_add_epi16(sum2,_mm_unpackhi_epi64(sum2,sum2));
    __m128i sum4  = _mm_add_epi16(sum3,_mm_shuffle_epi32(sum3,1));
    __m128i sum5  = _mm_add_epi16(sum4,_mm_shufflelo_epi16(sum4,1));    
#endif
    int16_t sum6  = (int16_t)_mm_cvtsi128_si32(sum5);                // 16 bit sum
    return  sum6;                                                    // sign extend to 32 bits
}

// function add_saturated: add element by element, signed with saturation
static inline Vec32c add_saturated(Vec32c const a, Vec32c const b) {
    return _mm256_adds_epi8(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32c sub_saturated(Vec32c const a, Vec32c const b) {
    return _mm256_subs_epi8(a, b);
}

// function max: a > b ? a : b
static inline Vec32c max(Vec32c const a, Vec32c const b) {
    return _mm256_max_epi8(a,b);
}

// function min: a < b ? a : b
static inline Vec32c min(Vec32c const a, Vec32c const b) {
    return _mm256_min_epi8(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec32c abs(Vec32c const a) {
    return _mm256_abs_epi8(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32c abs_saturated(Vec32c const a) {
    __m256i absa = abs(a);                                 // abs(a)
#if INSTRSET >= 10
    return _mm256_min_epu8(absa, Vec32c(0x7F));
#else
    __m256i overfl = _mm256_cmpgt_epi8(_mm256_setzero_si256(), absa);// 0 > a
    return           _mm256_add_epi8(absa, overfl);        // subtract 1 if 0x80
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32c rotate_left(Vec32c const a, int b) {
    uint8_t mask = 0xFFu << b;                             // mask off overflow bits
    __m256i m     = _mm256_set1_epi8(mask);
    __m128i bb    = _mm_cvtsi32_si128(b & 7);              // b modulo 8
    __m128i mbb   = _mm_cvtsi32_si128((- b) & 7);          // 8-b modulo 8
    __m256i left  = _mm256_sll_epi16(a, bb);               // a << b
    __m256i right = _mm256_srl_epi16(a, mbb);              // a >> 8-b
            left  = _mm256_and_si256(m, left);             // mask off overflow bits
            right = _mm256_andnot_si256(m, right);
    return  _mm256_or_si256(left, right);                  // combine left and right shifted bits
}


/*****************************************************************************
*
*          Vector of 16 8-bit unsigned integers
*
*****************************************************************************/

class Vec32uc : public Vec32c {
public:
    // Default constructor:
    Vec32uc(){
    }
    // Constructor to broadcast the same value into all elements:
    Vec32uc(uint32_t i) {
        ymm = _mm256_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec32uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31) {
        ymm = _mm256_setr_epi8((int8_t)i0, (int8_t)i1, (int8_t)i2, (int8_t)i3, (int8_t)i4, (int8_t)i5, (int8_t)i6, (int8_t)i7, (int8_t)i8, (int8_t)i9, (int8_t)i10, (int8_t)i11, (int8_t)i12, (int8_t)i13, (int8_t)i14, (int8_t)i15,
            (int8_t)i16, (int8_t)i17, (int8_t)i18, (int8_t)i19, (int8_t)i20, (int8_t)i21, (int8_t)i22, (int8_t)i23, (int8_t)i24, (int8_t)i25, (int8_t)i26, (int8_t)i27, (int8_t)i28, (int8_t)i29, (int8_t)i30, (int8_t)i31);
    }
    // Constructor to build from two Vec16uc:
    Vec32uc(Vec16uc const a0, Vec16uc const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec32uc(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec32uc & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec32uc & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32uc & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec32uc const insert(int index, uint8_t value) {
        Vec32c::insert(index, (int8_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(int index) const {
        return (uint8_t)Vec32c::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16uc:
    Vec16uc get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec16uc get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int elementtype() {
        return 5;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec32uc operator + (Vec32uc const a, Vec32uc const b) {
    return Vec32uc (Vec32c(a) + Vec32c(b));
}

// vector operator - : subtract
static inline Vec32uc operator - (Vec32uc const a, Vec32uc const b) {
    return Vec32uc (Vec32c(a) - Vec32c(b));
}

// vector operator * : multiply
static inline Vec32uc operator * (Vec32uc const a, Vec32uc const b) {
    return Vec32uc (Vec32c(a) * Vec32c(b));
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF >> (uint32_t)b;                // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_sll_epi16(am,_mm_cvtsi32_si128((int)b));    // 16-bit shifts
    return res;
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const a, uint32_t b) {
    uint32_t mask = (uint32_t)0xFF << (uint32_t)b;                // mask to remove bits that are shifted out
    __m256i am    = _mm256_and_si256(a,_mm256_set1_epi8((char)mask));// remove bits that will overflow
    __m256i res   = _mm256_srl_epi16(am,_mm_cvtsi32_si128((int)b));    // 16-bit shifts
    return res;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right arithmetic
static inline Vec32uc & operator >>= (Vec32uc & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec32cb operator >= (Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    //return _mm256_cmp_epu8_mask (a, b, 5);
    return _mm256_cmp_epu8_mask_fix<5> (a, b);
#else
    return _mm256_cmpeq_epi8(_mm256_max_epu8(a,b), a); // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32cb operator <= (Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu8_mask_fix<2> (a, b);
#else
    return b >= a;
#endif
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32cb operator > (Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu8_mask_fix<6> (a, b);
#else
    return Vec32cb(Vec32c(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32cb operator < (Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu8_mask_fix<1> (a, b);
#else
    return b > a;
#endif
}

// vector operator & : bitwise and
static inline Vec32uc operator & (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(Vec256b(a) & Vec256b(b));
}
static inline Vec32uc operator && (Vec32uc const a, Vec32uc const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec32uc operator | (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(Vec256b(a) | Vec256b(b));
}
static inline Vec32uc operator || (Vec32uc const a, Vec32uc const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec32uc operator ^ (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec32uc operator ~ (Vec32uc const a) {
    return Vec32uc( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32uc select (Vec32cb const s, Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi8(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32uc if_add (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi8 (a, f, a, b);
#else
    return a + (Vec32uc(f) & b);
#endif
}

// Conditional subtract
static inline Vec32uc if_sub (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi8 (a, f, a, b);
#else
    return a - (Vec32uc(f) & b);
#endif
}

// Conditional multiply
static inline Vec32uc if_mul (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
// (Note: horizontal_add_x(Vec32uc) is slightly faster)
static inline uint8_t horizontal_add (Vec32uc const a) {
    __m256i  sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i  sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i  sum3 = _mm256_add_epi16(sum1,sum2);
    __m128i  sum4 = _mm256_extracti128_si256(sum3,1);
    __m128i  sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    uint8_t  sum6 = (uint8_t)_mm_cvtsi128_si32(sum5);      // truncate to 8 bits
    return   sum6;                                         // zero extend to 32 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec32uc const a) {
    __m256i sum1 = _mm256_sad_epu8(a,_mm256_setzero_si256());
    __m256i sum2 = _mm256_shuffle_epi32(sum1,2);
    __m256i sum3 = _mm256_add_epi16(sum1,sum2);
    __m128i sum4 = _mm256_extracti128_si256(sum3,1);
    __m128i sum5 = _mm_add_epi16(_mm256_castsi256_si128(sum3),sum4);
    return         (uint32_t)_mm_cvtsi128_si32(sum5);
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32uc add_saturated(Vec32uc const a, Vec32uc const b) {
    return _mm256_adds_epu8(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32uc sub_saturated(Vec32uc const a, Vec32uc const b) {
    return _mm256_subs_epu8(a, b);
}

// function max: a > b ? a : b
static inline Vec32uc max(Vec32uc const a, Vec32uc const b) {
    return _mm256_max_epu8(a,b);
}

// function min: a < b ? a : b
static inline Vec32uc min(Vec32uc const a, Vec32uc const b) {
    return _mm256_min_epu8(a,b);
}

    
/*****************************************************************************
*
*          Vector of 16 16-bit signed integers
*
*****************************************************************************/

class Vec16s : public Vec256b {
public:
    // Default constructor:
    Vec16s() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16s(int i) {
        ymm = _mm256_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec16s(int16_t i0, int16_t i1, int16_t i2,  int16_t i3,  int16_t i4,  int16_t i5,  int16_t i6,  int16_t i7,
           int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15) {
        ymm = _mm256_setr_epi16(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 );
    }
    // Constructor to build from two Vec8s:
    Vec16s(Vec8s const a0, Vec8s const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16s(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16s & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to convert from type Vec256b used in emulation:
    Vec16s(Vec256b const & x) {
        ymm = x;
    }    
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec16s & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16s & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16s & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_epi16(__mmask16((1u << n) - 1), p);
#else
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 8) {
            *this = Vec16s(Vec8s().load_partial(n, p), 0);
        }
        else if (n < 16) {
            *this = Vec16s(Vec8s().load(p), Vec8s().load_partial(n-8, (int16_t const*)p+8));
        }
        else {
            load(p);
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL + AVX512BW
        _mm256_mask_storeu_epi16(p, __mmask16((1u << n) - 1), ymm);
#else 
        if (n <= 0) {
            return;
        }
        else if (n <= 8) {
            get_low().store_partial(n, p);
        }
        else if (n < 16) {
            get_low().store(p);
            get_high().store_partial(n-8, (int16_t*)p+8);
        }
        else {
            store(p);
        }
#endif
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16s & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_epi16(__mmask16((1u << n) - 1), ymm);
#else
        *this = Vec16s(Vec32c(*this).cutoff(n * 2));
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec16s const insert(int index, int16_t value) {
#if INSTRSET >= 10
        ymm = _mm256_mask_set1_epi16(ymm, __mmask16(1u << index), value);
#else 
        const int16_t m[32] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, -1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};
        __m256i mask  = Vec256b().load(m + 16 - (index & 0x0F));
        __m256i broad = _mm256_set1_epi16(value);
        ymm = selectb(mask, broad, ymm);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int16_t extract(int index) const {
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
        __m256i x = _mm256_maskz_compress_epi16(__mmask16(1u << index), ymm);
        return (int16_t)_mm_cvtsi128_si32(_mm256_castsi256_si128(x));        
#else
        int16_t x[16];  // find faster version
        store(x);
        return x[index & 0x0F];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8s:
    Vec8s get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec8s get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 6;
    }
};


/*****************************************************************************
*
*          Vec16sb: Vector of 16 Booleans for use with Vec16s and Vec16us
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

class Vec16sb : public Vec16s {
public:
    // Default constructor:
    Vec16sb() {
    }
    // Constructor to build from all elements:
    /*
    Vec16sb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16s(-int16_t(x0), -int16_t(x1), -int16_t(x2), -int16_t(x3), -int16_t(x4), -int16_t(x5), -int16_t(x6), -int16_t(x7), 
            -int16_t(x8), -int16_t(x9), -int16_t(x10), -int16_t(x11), -int16_t(x12), -int16_t(x13), -int16_t(x14), -int16_t(x15))
        {} */
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16sb(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16sb & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16sb(bool b) : Vec16s(-int16_t(b)) {
    }
    // Constructor to convert from type Vec256b used in emulation:
    Vec16sb(Vec256b const & x) : Vec16s(x) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16sb & operator = (bool b) {
        *this = Vec16sb(b);
        return *this;
    }
    // Constructor to build from two Vec8sb:
    Vec16sb(Vec8sb const a0, Vec8sb const a1) : Vec16s(Vec8s(a0), Vec8s(a1)) {
    }
    Vec8sb get_low() const {
        return Vec8sb(Vec16s::get_low());
    }
    Vec8sb get_high() const {
        return Vec8sb(Vec16s::get_high());
    }
    Vec16sb & insert(int index, bool a) {
        Vec16s::insert(index, -(int)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec16s::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec16sb & load_bits(uint16_t a) {
        __m256i b1 = _mm256_set1_epi16((int16_t)a);  // broadcast a
        __m256i m1 = constant8ui<0,0,0,0,0x00010001,0x00010001,0x00010001,0x00010001>(); 
        __m256i c1 = _mm256_shuffle_epi8(b1, m1);  // get right byte in each position
        __m256i m2 = constant8ui<0x00020001,0x00080004,0x00200010,0x00800040,0x00020001,0x00080004,0x00200010,0x00800040>(); 
        __m256i d1 = _mm256_and_si256(c1, m2); // isolate one bit in each byte
        ymm = _mm256_cmpgt_epi16(d1, _mm256_setzero_si256());  // compare with 0
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec16sb(int b) = delete;
    Vec16sb & operator = (int x) = delete;
};

#else

typedef Vec16b Vec16sb;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Define operators for Vec16sb
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

// vector operator & : bitwise and
static inline Vec16sb operator & (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(Vec256b(a) & Vec256b(b));
}
static inline Vec16sb operator && (Vec16sb const a, Vec16sb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16sb & operator &= (Vec16sb & a, Vec16sb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16sb operator | (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(Vec256b(a) | Vec256b(b));
}
static inline Vec16sb operator || (Vec16sb const a, Vec16sb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16sb & operator |= (Vec16sb & a, Vec16sb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16sb operator ^ (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16sb & operator ^= (Vec16sb & a, Vec16sb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec16sb operator == (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(a ^ Vec16sb(~b));
}

// vector operator != : xor
static inline Vec16sb operator != (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec16sb operator ~ (Vec16sb const a) {
    return Vec16sb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec16sb operator ! (Vec16sb const a) {
    return ~ a;
}

// vector function andnot
static inline Vec16sb andnot (Vec16sb const a, Vec16sb const b) {
    return Vec16sb(andnot(Vec256b(a), Vec256b(b)));
}

#endif

/*****************************************************************************
*
*          Operators for Vec16s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16s operator + (Vec16s const a, Vec16s const b) {
    return _mm256_add_epi16(a, b);
}
// vector operator += : add
static inline Vec16s & operator += (Vec16s & a, Vec16s const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16s operator ++ (Vec16s & a, int) {
    Vec16s a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec16s & operator ++ (Vec16s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16s operator - (Vec16s const a, Vec16s const b) {
    return _mm256_sub_epi16(a, b);
}
// vector operator - : unary minus
static inline Vec16s operator - (Vec16s const a) {
    return _mm256_sub_epi16(_mm256_setzero_si256(), a);
}
// vector operator -= : subtract
static inline Vec16s & operator -= (Vec16s & a, Vec16s const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16s operator -- (Vec16s & a, int) {
    Vec16s a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec16s & operator -- (Vec16s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16s operator * (Vec16s const a, Vec16s const b) {
    return _mm256_mullo_epi16(a, b);
}
// vector operator *= : multiply
static inline Vec16s & operator *= (Vec16s & a, Vec16s const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file


// vector operator << : shift left
static inline Vec16s operator << (Vec16s const a, int b) {
    return _mm256_sll_epi16(a,_mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec16s & operator <<= (Vec16s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16s operator >> (Vec16s const a, int b) {
    return _mm256_sra_epi16(a,_mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec16s & operator >>= (Vec16s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16sb operator == (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 0);
#else
    return _mm256_cmpeq_epi16(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec16sb operator != (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 4);
#else
    return Vec16sb(Vec16s(~(a == b)));
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec16sb operator > (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 6);
#else
    return _mm256_cmpgt_epi16(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec16sb operator < (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16sb operator >= (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 5);
#else
    return Vec16sb(Vec16s(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16sb operator <= (Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec16s operator & (Vec16s const a, Vec16s const b) {
    return Vec16s(Vec256b(a) & Vec256b(b));
}
static inline Vec16s operator && (Vec16s const a, Vec16s const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec16s & operator &= (Vec16s & a, Vec16s const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16s operator | (Vec16s const a, Vec16s const b) {
    return Vec16s(Vec256b(a) | Vec256b(b));
}
static inline Vec16s operator || (Vec16s const a, Vec16s const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec16s & operator |= (Vec16s & a, Vec16s const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16s operator ^ (Vec16s const a, Vec16s const b) {
    return Vec16s(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec16s & operator ^= (Vec16s & a, Vec16s const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16s operator ~ (Vec16s const a) {
    return Vec16s( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec16sb operator ! (Vec16s const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi16_mask (a, _mm256_setzero_si256(), 0);
#else
    return _mm256_cmpeq_epi16(a,_mm256_setzero_si256());
#endif
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16s select (Vec16sb const s, Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi16(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16s if_add (Vec16sb const f, Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi16 (a, f, a, b);
#else
    return a + (Vec16s(f) & b);
#endif
}

// Conditional subtract
static inline Vec16s if_sub (Vec16sb const f, Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi16 (a, f, a, b);
#else
    return a - (Vec16s(f) & b);
#endif
}

// Conditional multiply
static inline Vec16s if_mul (Vec16sb const f, Vec16s const a, Vec16s const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi16 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int16_t horizontal_add (Vec16s const a) {
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i sum1  = _mm_add_epi16(_mm256_extracti128_si256(a,1),_mm256_castsi256_si128(a));
    __m128i sum2  = _mm_add_epi16(sum1,_mm_unpackhi_epi64(sum1,sum1));
    __m128i sum3  = _mm_add_epi16(sum2,_mm_shuffle_epi32(sum2,1));
    __m128i sum4  = _mm_add_epi16(sum3,_mm_shufflelo_epi16(sum3,1));    
    return (int16_t)_mm_cvtsi128_si32(sum4);               // truncate to 16 bits
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x (Vec16s const a) {
    __m256i aeven = _mm256_slli_epi32(a,16);               // even numbered elements of a. get sign bit in position
            aeven = _mm256_srai_epi32(aeven,16);           // sign extend even numbered elements
    __m256i aodd  = _mm256_srai_epi32(a,16);               // sign extend odd  numbered elements
    __m256i sum1  = _mm256_add_epi32(aeven,aodd);          // add even and odd elements
    __m128i sum2  = _mm_add_epi32(_mm256_extracti128_si256(sum1,1),_mm256_castsi256_si128(sum1));
    __m128i sum3  = _mm_add_epi32(sum2,_mm_unpackhi_epi64(sum2,sum2));
    __m128i sum4  = _mm_add_epi32(sum3,_mm_shuffle_epi32(sum3,1));
    return (int16_t)_mm_cvtsi128_si32(sum4);               // truncate to 16 bits
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16s add_saturated(Vec16s const a, Vec16s const b) {
    return _mm256_adds_epi16(a, b);
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16s sub_saturated(Vec16s const a, Vec16s const b) {
    return _mm256_subs_epi16(a, b);
}

// function max: a > b ? a : b
static inline Vec16s max(Vec16s const a, Vec16s const b) {
    return _mm256_max_epi16(a,b);
}

// function min: a < b ? a : b
static inline Vec16s min(Vec16s const a, Vec16s const b) {
    return _mm256_min_epi16(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec16s abs(Vec16s const a) {
    return _mm256_abs_epi16(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16s abs_saturated(Vec16s const a) {
#if INSTRSET >= 10
    return _mm256_min_epu16(abs(a), Vec16s(0x7FFF));
#else
    __m256i absa   = abs(a);                               // abs(a)
    __m256i overfl = _mm256_srai_epi16(absa,15);           // sign
    return           _mm256_add_epi16(absa,overfl);        // subtract 1 if 0x8000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16s rotate_left(Vec16s const a, int b) {
    __m256i left  = _mm256_sll_epi16(a,_mm_cvtsi32_si128(b & 0x0F));    // a << b 
    __m256i right = _mm256_srl_epi16(a,_mm_cvtsi32_si128((-b) & 0x0F)); // a >> (16 - b)
    return          _mm256_or_si256(left,right);                        // or
}


/*****************************************************************************
*
*          Vector of 16 16-bit unsigned integers
*
*****************************************************************************/

class Vec16us : public Vec16s {
public:
    // Default constructor:
    Vec16us(){
    }
    // Constructor to broadcast the same value into all elements:
    Vec16us(uint32_t i) {
        ymm = _mm256_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec16us(uint16_t i0, uint16_t i1, uint16_t i2,  uint16_t i3,  uint16_t i4,  uint16_t i5,  uint16_t i6,  uint16_t i7,
            uint16_t i8, uint16_t i9, uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15) {
        ymm = _mm256_setr_epi16((int16_t)i0, (int16_t)i1, (int16_t)i2, (int16_t)i3, (int16_t)i4, (int16_t)i5, (int16_t)i6, (int16_t)i7, 
            (int16_t)i8, (int16_t)i9, (int16_t)i10, (int16_t)i11, (int16_t)i12, (int16_t)i13, (int16_t)i14, (int16_t)i15);
    }
    // Constructor to build from two Vec8us:
    Vec16us(Vec8us const a0, Vec8us const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec16us(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec16us & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16us & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16us & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16us const insert(int index, uint16_t value) {
        Vec16s::insert(index, (int16_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint16_t extract(int index) const {
        return (uint16_t)Vec16s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8us:
    Vec8us get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec8us get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int elementtype() {
        return 7;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16us operator + (Vec16us const a, Vec16us const b) {
    return Vec16us (Vec16s(a) + Vec16s(b));
}

// vector operator - : subtract
static inline Vec16us operator - (Vec16us const a, Vec16us const b) {
    return Vec16us (Vec16s(a) - Vec16s(b));
}

// vector operator * : multiply
static inline Vec16us operator * (Vec16us const a, Vec16us const b) {
    return Vec16us (Vec16s(a) * Vec16s(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const a, uint32_t b) {
    return _mm256_srl_epi16(a,_mm_cvtsi32_si128((int)b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right arithmetic
static inline Vec16us & operator >>= (Vec16us & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const a, uint32_t b) {
    return _mm256_sll_epi16(a,_mm_cvtsi32_si128((int)b)); 
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16sb operator >= (Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu16_mask (a, b, 5);
#else
    __m256i max_ab = _mm256_max_epu16(a,b);                // max(a,b), unsigned
    return _mm256_cmpeq_epi16(a,max_ab);                   // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16sb operator <= (Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu16_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16sb operator > (Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu16_mask (a, b, 6);
#else
    return Vec16sb(Vec16s(~(b >= a)));
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16sb operator < (Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu16_mask (a, b, 1);
#else
    return b > a;
#endif
}

// vector operator & : bitwise and
static inline Vec16us operator & (Vec16us const a, Vec16us const b) {
    return Vec16us(Vec256b(a) & Vec256b(b));
}
static inline Vec16us operator && (Vec16us const a, Vec16us const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16us operator | (Vec16us const a, Vec16us const b) {
    return Vec16us(Vec256b(a) | Vec256b(b));
}
static inline Vec16us operator || (Vec16us const a, Vec16us const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16us operator ^ (Vec16us const a, Vec16us const b) {
    return Vec16us(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec16us operator ~ (Vec16us const a) {
    return Vec16us( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16us select (Vec16sb const s, Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi16(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16us if_add (Vec16sb const f, Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi16 (a, f, a, b);
#else
    return a + (Vec16us(f) & b);
#endif
}

// Conditional subtract
static inline Vec16us if_sub (Vec16sb const f, Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi16 (a, f, a, b);
#else
    return a - (Vec16us(f) & b);
#endif
}

// Conditional multiply
static inline Vec16us if_mul (Vec16sb const f, Vec16us const a, Vec16us const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi16 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint16_t horizontal_add (Vec16us const a) {
    return (uint16_t)horizontal_add(Vec16s(a));
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec16us const a) {
#if INSTRSET >= 10
    __m256i aeven = _mm256_maskz_mov_epi16 (__mmask16(0x5555), a);
#else
    __m256i mask  = _mm256_set1_epi32(0x0000FFFF);         // mask for even positions
    __m256i aeven = _mm256_and_si256(a,mask);              // even numbered elements of a
#endif
    __m256i aodd  = _mm256_srli_epi32(a,16);               // zero extend odd numbered elements
    __m256i sum1  = _mm256_add_epi32(aeven,aodd);          // add even and odd elements
    __m128i sum2  = _mm_add_epi32(_mm256_extracti128_si256(sum1,1),_mm256_castsi256_si128(sum1));
    __m128i sum3  = _mm_add_epi32(sum2,_mm_unpackhi_epi64(sum2,sum2));
    __m128i sum4  = _mm_add_epi32(sum3,_mm_shuffle_epi32(sum3,1));
    return (int16_t)_mm_cvtsi128_si32(sum4);               // truncate to 16 bits
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16us add_saturated(Vec16us const a, Vec16us const b) {
    return _mm256_adds_epu16(a, b);
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16us sub_saturated(Vec16us const a, Vec16us const b) {
    return _mm256_subs_epu16(a, b);
}

// function max: a > b ? a : b
static inline Vec16us max(Vec16us const a, Vec16us const b) {
    return _mm256_max_epu16(a,b);
}

// function min: a < b ? a : b
static inline Vec16us min(Vec16us const a, Vec16us const b) {
    return _mm256_min_epu16(a,b);
}


/*****************************************************************************
*
*          Vector of 8 32-bit signed integers
*
*****************************************************************************/

class Vec8i : public Vec256b {
public:
    // Default constructor:
    Vec8i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8i(int i) {
        ymm = _mm256_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec8i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7) {
        ymm = _mm256_setr_epi32(i0, i1, i2, i3, i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4i:
    Vec8i(Vec4i const a0, Vec4i const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8i(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8i & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec8i & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8i & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8i & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_epi32(__mmask8((1u << n) - 1), p);
#else 
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 4) {
            *this = Vec8i(Vec4i().load_partial(n, p), 0);
        }
        else if (n < 8) {
            *this = Vec8i(Vec4i().load(p), Vec4i().load_partial(n-4, (int32_t const*)p+4));
        }
        else {
            load(p);
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm256_mask_storeu_epi32(p, __mmask8((1u << n) - 1), ymm);
#else
        if (n <= 0) {
            return;
        }
        else if (n <= 4) {
            get_low().store_partial(n, p);
        }
        else if (n < 8) {
            get_low().store(p);
            get_high().store_partial(n-4, (int32_t*)p+4);
        }
        else {
            store(p);
        }
#endif
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8i & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_epi32(__mmask8((1u << n) - 1), ymm);
#else 
        *this = Vec32c(*this).cutoff(n * 4);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec8i const insert(int index, int32_t value) {
#if INSTRSET >= 10
        ymm = _mm256_mask_set1_epi32(ymm, __mmask8(1u << index), value);
#else
        __m256i broad = _mm256_set1_epi32(value);  // broadcast value into all elements
        const int32_t maskl[16] = {0,0,0,0,0,0,0,0, -1,0,0,0,0,0,0,0};
        __m256i mask  = Vec256b().load(maskl + 8 - (index & 7)); // mask with FFFFFFFF at index position
        ymm = selectb (mask, broad, ymm);
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(int index) const {
#if INSTRSET >= 10
        __m256i x = _mm256_maskz_compress_epi32(__mmask8(1u << index), ymm);
        return _mm_cvtsi128_si32(_mm256_castsi256_si128(x));
#else 
        int32_t x[8];
        store(x);
        return x[index & 7];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4i:
    Vec4i get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec4i get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 8;
    }
};


/*****************************************************************************
*
*          Vec8ib: Vector of 8 Booleans for use with Vec8i and Vec8ui
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

class Vec8ib : public Vec8i {
public:
    // Default constructor:
    Vec8ib() {
    }
    // Constructor to build from all elements:
    Vec8ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) :
        Vec8i(-int32_t(x0), -int32_t(x1), -int32_t(x2), -int32_t(x3), -int32_t(x4), -int32_t(x5), -int32_t(x6), -int32_t(x7))
        {}
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8ib(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8ib & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec8ib(bool b) : Vec8i(-int32_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8ib & operator = (bool b) {
        *this = Vec8ib(b);
        return *this;
    }
    // Constructor to build from two Vec4ib:
    Vec8ib(Vec4ib const a0, Vec4ib const a1) : Vec8i(Vec4i(a0), Vec4i(a1)) {
    }
    Vec4ib get_low() const {
        return Vec4ib(Vec8i::get_low());
    }
    Vec4ib get_high() const {
        return Vec4ib(Vec8i::get_high());
    }
    Vec8ib & insert (int index, bool a) {
        Vec8i::insert(index, -(int)a);
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec8i::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec8ib & load_bits(uint8_t a) {
        __m256i b1 = _mm256_set1_epi32((int32_t)a);  // broadcast a
        __m256i m2 = constant8ui<1,2,4,8,0x10,0x20,0x40,0x80>(); 
        __m256i d1 = _mm256_and_si256(b1, m2); // isolate one bit in each dword
        ymm = _mm256_cmpgt_epi32(d1, _mm256_setzero_si256());  // compare with 0
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec8ib(int b) = delete;
    Vec8ib & operator = (int x) = delete;
};

#else

typedef Vec8b Vec8ib;  // compact boolean vector

#endif


/*****************************************************************************
*
*          Define operators for Vec8ib
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

// vector operator & : bitwise and
static inline Vec8ib operator & (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(Vec256b(a) & Vec256b(b));
}
static inline Vec8ib operator && (Vec8ib const a, Vec8ib const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8ib & operator &= (Vec8ib & a, Vec8ib const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8ib operator | (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(Vec256b(a) | Vec256b(b));
}
static inline Vec8ib operator || (Vec8ib const a, Vec8ib const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8ib & operator |= (Vec8ib & a, Vec8ib const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8ib operator ^ (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8ib & operator ^= (Vec8ib & a, Vec8ib const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec8ib operator == (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(a ^ (~b));
}

// vector operator != : xor
static inline Vec8ib operator != (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec8ib operator ~ (Vec8ib const a) {
    return Vec8ib( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec8ib operator ! (Vec8ib const a) {
    return ~ a;
}

// vector function andnot
static inline Vec8ib andnot (Vec8ib const a, Vec8ib const b) {
    return Vec8ib(andnot(Vec256b(a), Vec256b(b)));
}

#endif

/*****************************************************************************
*
*          Operators for Vec8i
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8i operator + (Vec8i const a, Vec8i const b) {
    return _mm256_add_epi32(a, b);
}
// vector operator += : add
static inline Vec8i & operator += (Vec8i & a, Vec8i const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8i operator ++ (Vec8i & a, int) {
    Vec8i a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec8i & operator ++ (Vec8i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8i operator - (Vec8i const a, Vec8i const b) {
    return _mm256_sub_epi32(a, b);
}
// vector operator - : unary minus
static inline Vec8i operator - (Vec8i const a) {
    return _mm256_sub_epi32(_mm256_setzero_si256(), a);
}
// vector operator -= : subtract
static inline Vec8i & operator -= (Vec8i & a, Vec8i const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8i operator -- (Vec8i & a, int) {
    Vec8i a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec8i & operator -- (Vec8i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8i operator * (Vec8i const a, Vec8i const b) {
    return _mm256_mullo_epi32(a, b);
}
// vector operator *= : multiply
static inline Vec8i & operator *= (Vec8i & a, Vec8i const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file

// vector operator << : shift left
static inline Vec8i operator << (Vec8i const a, int32_t b) {
    return _mm256_sll_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec8i & operator <<= (Vec8i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8i operator >> (Vec8i const a, int32_t b) {
    return _mm256_sra_epi32(a, _mm_cvtsi32_si128(b));
}
// vector operator >>= : shift right arithmetic
static inline Vec8i & operator >>= (Vec8i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8ib operator == (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 0);
#else
    return _mm256_cmpeq_epi32(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec8ib operator != (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 4);
#else
    return Vec8ib(Vec8i(~(a == b)));
#endif
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec8ib operator > (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 6);
#else
    return _mm256_cmpgt_epi32(a, b);
#endif
}

// vector operator < : returns true for elements for which a < b
static inline Vec8ib operator < (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8ib operator >= (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 5);
#else
    return Vec8ib(Vec8i(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8ib operator <= (Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec8i operator & (Vec8i const a, Vec8i const b) {
    return Vec8i(Vec256b(a) & Vec256b(b));
}
static inline Vec8i operator && (Vec8i const a, Vec8i const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec8i & operator &= (Vec8i & a, Vec8i const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8i operator | (Vec8i const a, Vec8i const b) {
    return Vec8i(Vec256b(a) | Vec256b(b));
}
static inline Vec8i operator || (Vec8i const a, Vec8i const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec8i & operator |= (Vec8i & a, Vec8i const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8i operator ^ (Vec8i const a, Vec8i const b) {
    return Vec8i(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec8i & operator ^= (Vec8i & a, Vec8i const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8i operator ~ (Vec8i const a) {
    return Vec8i( ~ Vec256b(a));
}

// vector operator ! : returns true for elements == 0
static inline Vec8ib operator ! (Vec8i const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi32_mask (a, _mm256_setzero_si256(), 0);
#else
    return _mm256_cmpeq_epi32(a, _mm256_setzero_si256());
#endif
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8i select (Vec8ib const s, Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi32(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8i if_add (Vec8ib const f, Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi32 (a, f, a, b);
#else
    return a + (Vec8i(f) & b);
#endif
}

// Conditional subtract
static inline Vec8i if_sub (Vec8ib const f, Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi32 (a, f, a, b);
#else
    return a - (Vec8i(f) & b);
#endif
}

// Conditional multiply
static inline Vec8i if_mul (Vec8ib const f, Vec8i const a, Vec8i const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi32 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add (Vec8i const a) {
    // The hadd instruction is inefficient, and may be split into two instructions for faster decoding
    __m128i sum1  = _mm_add_epi32(_mm256_extracti128_si256(a,1),_mm256_castsi256_si128(a));
    __m128i sum2  = _mm_add_epi32(sum1,_mm_unpackhi_epi64(sum1,sum1));
    __m128i sum3  = _mm_add_epi32(sum2,_mm_shuffle_epi32(sum2,1));
    return (int32_t)_mm_cvtsi128_si32(sum3); 
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
// static inline int64_t horizontal_add_x (Vec8i const a); // defined below

// function add_saturated: add element by element, signed with saturation
static inline Vec8i add_saturated(Vec8i const a, Vec8i const b) {
    __m256i sum    = _mm256_add_epi32(a, b);               // a + b
    __m256i axb    = _mm256_xor_si256(a, b);               // check if a and b have different sign
    __m256i axs    = _mm256_xor_si256(a, sum);             // check if a and sum have different sign
    __m256i overf1 = _mm256_andnot_si256(axb,axs);         // check if sum has wrong sign
    __m256i overf2 = _mm256_srai_epi32(overf1,31);         // -1 if overflow
    __m256i asign  = _mm256_srli_epi32(a,31);              // 1  if a < 0
    __m256i sat1   = _mm256_srli_epi32(overf2,1);          // 7FFFFFFF if overflow
    __m256i sat2   = _mm256_add_epi32(sat1,asign);         // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,sum);                      // sum if not overflow, else sat2
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8i sub_saturated(Vec8i const a, Vec8i const b) {
    __m256i diff   = _mm256_sub_epi32(a, b);               // a + b
    __m256i axb    = _mm256_xor_si256(a, b);               // check if a and b have different sign
    __m256i axs    = _mm256_xor_si256(a, diff);            // check if a and sum have different sign
    __m256i overf1 = _mm256_and_si256(axb,axs);            // check if sum has wrong sign
    __m256i overf2 = _mm256_srai_epi32(overf1,31);         // -1 if overflow
    __m256i asign  = _mm256_srli_epi32(a,31);              // 1  if a < 0
    __m256i sat1   = _mm256_srli_epi32(overf2,1);          // 7FFFFFFF if overflow
    __m256i sat2   = _mm256_add_epi32(sat1,asign);         // 7FFFFFFF if positive overflow 80000000 if negative overflow
    return  selectb(overf2,sat2,diff);                     // diff if not overflow, else sat2
}

// function max: a > b ? a : b
static inline Vec8i max(Vec8i const a, Vec8i const b) {
    return _mm256_max_epi32(a,b);
}

// function min: a < b ? a : b
static inline Vec8i min(Vec8i const a, Vec8i const b) {
    return _mm256_min_epi32(a,b);
}

// function abs: a >= 0 ? a : -a
static inline Vec8i abs(Vec8i const a) {
    return _mm256_abs_epi32(a);
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8i abs_saturated(Vec8i const a) {
#if INSTRSET >= 10
    return _mm256_min_epu32(abs(a), Vec8i(0x7FFFFFFF));
#else
    __m256i absa   = abs(a);                               // abs(a)
    __m256i overfl = _mm256_srai_epi32(absa,31);           // sign
    return           _mm256_add_epi32(absa,overfl);        // subtract 1 if 0x80000000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8i rotate_left(Vec8i const a, int b) {
#if INSTRSET >= 10  // __AVX512VL__
    return _mm256_rolv_epi32(a, _mm256_set1_epi32(b));
#else
    __m256i left  = _mm256_sll_epi32(a,_mm_cvtsi32_si128(b & 0x1F));   // a << b 
    __m256i right = _mm256_srl_epi32(a,_mm_cvtsi32_si128((-b) & 0x1F));// a >> (32 - b)
    __m256i rot   = _mm256_or_si256(left,right);                       // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 8 32-bit unsigned integers
*
*****************************************************************************/

class Vec8ui : public Vec8i {
public:
    // Default constructor:
    Vec8ui() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8ui(uint32_t i) {
        ymm = _mm256_set1_epi32((int32_t)i);
    }
    // Constructor to build from all elements:
    Vec8ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7) {
        ymm = _mm256_setr_epi32((int32_t)i0, (int32_t)i1, (int32_t)i2, (int32_t)i3, (int32_t)i4, (int32_t)i5, (int32_t)i6, (int32_t)i7);
    }
    // Constructor to build from two Vec4ui:
    Vec8ui(Vec4ui const a0, Vec4ui const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec8ui(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec8ui & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8ui & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8ui & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8ui const insert(int index, uint32_t value) {
        Vec8i::insert(index, (int32_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(int index) const {
        return (uint32_t)Vec8i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4ui:
    Vec4ui get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec4ui get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int elementtype() {
        return 9;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8ui operator + (Vec8ui const a, Vec8ui const b) {
    return Vec8ui (Vec8i(a) + Vec8i(b));
}

// vector operator - : subtract
static inline Vec8ui operator - (Vec8ui const a, Vec8ui const b) {
    return Vec8ui (Vec8i(a) - Vec8i(b));
}

// vector operator * : multiply
static inline Vec8ui operator * (Vec8ui const a, Vec8ui const b) {
    return Vec8ui (Vec8i(a) * Vec8i(b));
}

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const a, uint32_t b) {
    return _mm256_srl_epi32(a,_mm_cvtsi32_si128((int)b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const a, int32_t b) {
    return a >> (uint32_t)b;
}
// vector operator >>= : shift right logical
static inline Vec8ui & operator >>= (Vec8ui & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator << : shift left all elements
static inline Vec8ui operator << (Vec8ui const a, uint32_t b) {
    return Vec8ui ((Vec8i)a << (int32_t)b);
}
// vector operator << : shift left all elements
static inline Vec8ui operator << (Vec8ui const a, int32_t b) {
    return Vec8ui ((Vec8i)a << (int32_t)b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8ib operator > (Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu32_mask (a, b, 6);
#else
    __m256i signbit = _mm256_set1_epi32(0x80000000);
    __m256i a1      = _mm256_xor_si256(a,signbit);
    __m256i b1      = _mm256_xor_si256(b,signbit);
    return _mm256_cmpgt_epi32(a1,b1);                      // signed compare
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8ib operator < (Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu32_mask (a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8ib operator >= (Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu32_mask (a, b, 5);
#else
    __m256i max_ab = _mm256_max_epu32(a,b);                // max(a,b), unsigned
    return _mm256_cmpeq_epi32(a,max_ab);                   // a == max(a,b)
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8ib operator <= (Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu32_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec8ui operator & (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(Vec256b(a) & Vec256b(b));
}
static inline Vec8ui operator && (Vec8ui const a, Vec8ui const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8ui operator | (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(Vec256b(a) | Vec256b(b));
}
static inline Vec8ui operator || (Vec8ui const a, Vec8ui const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8ui operator ^ (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(Vec256b(a) ^ Vec256b(b));
}

// vector operator ~ : bitwise not
static inline Vec8ui operator ~ (Vec8ui const a) {
    return Vec8ui( ~ Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8ui select (Vec8ib const s, Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi32(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8ui if_add (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi32 (a, f, a, b);
#else
    return a + (Vec8ui(f) & b);
#endif
}

// Conditional subtract
static inline Vec8ui if_sub (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi32 (a, f, a, b);
#else
    return a - (Vec8ui(f) & b);
#endif
}

// Conditional multiply
static inline Vec8ui if_mul (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi32 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add (Vec8ui const a) {
    return (uint32_t)horizontal_add((Vec8i)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are zero extended before adding to avoid overflow
// static inline uint64_t horizontal_add_x (Vec8ui const a); // defined later

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8ui add_saturated(Vec8ui const a, Vec8ui const b) {
    Vec8ui sum = a + b;
    Vec8ui aorb = Vec8ui(a | b);
#if INSTRSET >= 10
    Vec8b  overflow = _mm256_cmp_epu32_mask(sum, aorb, 1);
    return _mm256_mask_set1_epi32(sum, overflow, -1);
#else
    Vec8ui overflow = Vec8ui(sum < aorb);                  // overflow if a + b < (a | b)
    return Vec8ui(sum | overflow);                         // return 0xFFFFFFFF if overflow
#endif
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8ui sub_saturated(Vec8ui const a, Vec8ui const b) {
    Vec8ui diff = a - b;
#if INSTRSET >= 10
    Vec8b  nunderflow = _mm256_cmp_epu32_mask(diff, a, 2); // not underflow if a - b <= a
    return _mm256_maskz_mov_epi32(nunderflow, diff);       // zero if underflow
#else
    Vec8ui underflow = Vec8ui(diff > a);                   // underflow if a - b > a
    return _mm256_andnot_si256(underflow, diff);           // return 0 if underflow
#endif
}

// function max: a > b ? a : b
static inline Vec8ui max(Vec8ui const a, Vec8ui const b) {
    return _mm256_max_epu32(a,b);
}

// function min: a < b ? a : b
static inline Vec8ui min(Vec8ui const a, Vec8ui const b) {
    return _mm256_min_epu32(a,b);
}


/*****************************************************************************
*
*          Vector of 4 64-bit signed integers
*
*****************************************************************************/

class Vec4q : public Vec256b {
public:
    // Default constructor:
    Vec4q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4q(int64_t i) {
        ymm = _mm256_set1_epi64x(i);
    }
    // Constructor to build from all elements:
    Vec4q(int64_t i0, int64_t i1, int64_t i2, int64_t i3) {
        ymm = _mm256_setr_epi64x(i0, i1, i2, i3);
    }
    // Constructor to build from two Vec2q:
    Vec4q(Vec2q const a0, Vec2q const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4q(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4q & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Type cast operator to convert to __m256i used in intrinsics
    operator __m256i() const {
        return ymm;
    }
    // Member function to load from array (unaligned)
    Vec4q & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4q & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4q & load_partial(int n, void const * p) {
#if INSTRSET >= 10  // AVX512VL
        ymm = _mm256_maskz_loadu_epi64(__mmask8((1u << n) - 1), p);
#else 
        if (n <= 0) {
            *this = 0;
        }
        else if (n <= 2) {
            *this = Vec4q(Vec2q().load_partial(n, p), 0);
        }
        else if (n < 4) {
            *this = Vec4q(Vec2q().load(p), Vec2q().load_partial(n-2, (int64_t const*)p+2));
        }
        else {
            load(p);
        }
#endif
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
#if INSTRSET >= 10  // AVX512VL
        _mm256_mask_storeu_epi64(p, __mmask8((1u << n) - 1), ymm);
#else 
        if (n <= 0) {
            return;
        }
        else if (n <= 2) {
            get_low().store_partial(n, p);
        }
        else if (n < 4) {
            get_low().store(p);
            get_high().store_partial(n-2, (int64_t*)p+2);
        }
        else {
            store(p);
        }
#endif
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec4q & cutoff(int n) {
#if INSTRSET >= 10 
        ymm = _mm256_maskz_mov_epi64(__mmask8((1u << n) - 1), ymm);
#else 
        *this = Vec32c(*this).cutoff(n * 8);
#endif
        return *this;
    }
    // Member function to change a single element in vector
    Vec4q const insert(int index, int64_t value) {
#if INSTRSET >= 10
        ymm = _mm256_mask_set1_epi64(ymm, __mmask8(1u << index), value);
#else 
        Vec4q x(value);
        switch (index) {
        case 0:        
            ymm = _mm256_blend_epi32(ymm,x,0x03);  break;
        case 1:
            ymm = _mm256_blend_epi32(ymm,x,0x0C);  break;
        case 2:
            ymm = _mm256_blend_epi32(ymm,x,0x30);  break;
        case 3:
            ymm = _mm256_blend_epi32(ymm,x,0xC0);  break;
        }
#endif
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(int index) const {
#if INSTRSET >= 10
        __m256i x = _mm256_maskz_compress_epi64(__mmask8(1u << index), ymm);
        return _emulate_movq(_mm256_castsi256_si128(x));
#else 
        int64_t x[4];
        store(x);
        return x[index & 3];
#endif
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec2q get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec2q get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int size() {
        return 4;
    }
    static constexpr int elementtype() {
        return 10;
    }
};

/*****************************************************************************
*
*          Vec4qb: Vector of 4 Booleans for use with Vec4q and Vec4uq
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

class Vec4qb : public Vec4q {
public:
    // Default constructor:
    Vec4qb() {
    }
    // Constructor to build from all elements:
    Vec4qb(bool x0, bool x1, bool x2, bool x3) :
        Vec4q(-int64_t(x0), -int64_t(x1), -int64_t(x2), -int64_t(x3)) {
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4qb(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4qb & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec4qb(bool b) : Vec4q(-int64_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec4qb & operator = (bool b) {
        *this = Vec4qb(b);
        return *this;
    }
    // Constructor to build from two Vec2qb:
    Vec4qb(Vec2qb const a0, Vec2qb const a1) : Vec4q(Vec2q(a0), Vec2q(a1)) {
    }
    // Member functions to split into two Vec2qb:
    Vec2qb get_low() const {
        return Vec2qb(Vec4q::get_low());
    }
    Vec2qb get_high() const {
        return Vec2qb(Vec4q::get_high());
    }
    Vec4qb & insert (int index, bool a) {
        Vec4q::insert(index, -(int64_t)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec4q::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Member function to change a bitfield to a boolean vector
    Vec4qb & load_bits(uint8_t a) {
        __m256i b1 = _mm256_set1_epi32((int32_t)a);  // broadcast a
        __m256i m2 = constant8ui<1,0,2,0,4,0,8,0>(); 
        __m256i d1 = _mm256_and_si256(b1, m2); // isolate one bit in each dword
        ymm = _mm256_cmpgt_epi64(d1, _mm256_setzero_si256());  // we can use signed compare here because no value is negative
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec4qb(int b) = delete;
    Vec4qb & operator = (int x) = delete;
};

#else

typedef Vec4b Vec4qb;  // compact boolean vector

#endif

/*****************************************************************************
*
*          Define operators for Vec4qb
*
*****************************************************************************/

#if INSTRSET < 10  // broad boolean vectors

// vector operator & : bitwise and
static inline Vec4qb operator & (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(Vec256b(a) & Vec256b(b));
}
static inline Vec4qb operator && (Vec4qb const a, Vec4qb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4qb & operator &= (Vec4qb & a, Vec4qb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4qb operator | (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(Vec256b(a) | Vec256b(b));
}
static inline Vec4qb operator || (Vec4qb const a, Vec4qb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4qb & operator |= (Vec4qb & a, Vec4qb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4qb operator ^ (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4qb & operator ^= (Vec4qb & a, Vec4qb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec4qb operator == (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(a ^ (~b));
}

// vector operator != : xor
static inline Vec4qb operator != (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec4qb operator ~ (Vec4qb const a) {
    return Vec4qb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec4qb operator ! (Vec4qb const a) {
    return ~ a;
}

// vector function andnot
static inline Vec4qb andnot (Vec4qb const a, Vec4qb const b) {
    return Vec4qb(andnot(Vec256b(a), Vec256b(b)));
}

#endif


/*****************************************************************************
*
*          Operators for Vec4q
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4q operator + (Vec4q const a, Vec4q const b) {
    return _mm256_add_epi64(a, b);
}
// vector operator += : add
static inline Vec4q & operator += (Vec4q & a, Vec4q const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec4q operator ++ (Vec4q & a, int) {
    Vec4q a0 = a;
    a = a + 1;
    return a0;
}
// prefix operator ++
static inline Vec4q & operator ++ (Vec4q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec4q operator - (Vec4q const a, Vec4q const b) {
    return _mm256_sub_epi64(a, b);
}
// vector operator - : unary minus
static inline Vec4q operator - (Vec4q const a) {
    return _mm256_sub_epi64(_mm256_setzero_si256(), a);
}
// vector operator -= : subtract
static inline Vec4q & operator -= (Vec4q & a, Vec4q const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec4q operator -- (Vec4q & a, int) {
    Vec4q a0 = a;
    a = a - 1;
    return a0;
}
// prefix operator --
static inline Vec4q & operator -- (Vec4q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec4q operator * (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10 // __AVX512DQ__ __AVX512VL__
    return _mm256_mullo_epi64(a, b);
#else
    // Split into 32-bit multiplies
    __m256i bswap   = _mm256_shuffle_epi32(b,0xB1);        // swap H<->L
    __m256i prodlh  = _mm256_mullo_epi32(a,bswap);         // 32 bit L*H products
    __m256i zero    = _mm256_setzero_si256();              // 0
    __m256i prodlh2 = _mm256_hadd_epi32(prodlh,zero);      // a0Lb0H+a0Hb0L,a1Lb1H+a1Hb1L,0,0
    __m256i prodlh3 = _mm256_shuffle_epi32(prodlh2,0x73);  // 0, a0Lb0H+a0Hb0L, 0, a1Lb1H+a1Hb1L
    __m256i prodll  = _mm256_mul_epu32(a,b);               // a0Lb0L,a1Lb1L, 64 bit unsigned products
    __m256i prod    = _mm256_add_epi64(prodll,prodlh3);    // a0Lb0L+(a0Lb0H+a0Hb0L)<<32, a1Lb1L+(a1Lb1H+a1Hb1L)<<32
    return  prod;
#endif
}

// vector operator *= : multiply
static inline Vec4q & operator *= (Vec4q & a, Vec4q const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec4q operator << (Vec4q const a, int32_t b) {
    return _mm256_sll_epi64(a, _mm_cvtsi32_si128(b));
}
// vector operator <<= : shift left
static inline Vec4q & operator <<= (Vec4q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4q operator >> (Vec4q const a, int32_t b) {
#if INSTRSET >= 10   // AVX512VL
    return _mm256_sra_epi64(a, _mm_cvtsi32_si128(b));
#else
    __m128i bb;
    __m256i shi, slo, sra2;
    if (b <= 32) {
        bb   = _mm_cvtsi32_si128(b);             // b
        shi  = _mm256_sra_epi32(a,bb);           // a >> b signed dwords
        slo  = _mm256_srl_epi64(a,bb);           // a >> b unsigned qwords
    }
    else {  // b > 32
        bb   = _mm_cvtsi32_si128(b-32);          // b - 32
        shi  = _mm256_srai_epi32(a,31);          // sign of a
        sra2 = _mm256_sra_epi32(a,bb);           // a >> (b-32) signed dwords
        slo  = _mm256_srli_epi64(sra2,32);       // a >> (b-32) >> 32 (second shift unsigned qword)
    }
    return _mm256_blend_epi32(slo,shi,0xAA);
#endif
}
// vector operator >>= : shift right arithmetic
static inline Vec4q & operator >>= (Vec4q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4qb operator == (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 0);
#else
    return _mm256_cmpeq_epi64(a, b);
#endif
}

// vector operator != : returns true for elements for which a != b
static inline Vec4qb operator != (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 4);
#else
    return Vec4qb(Vec4q(~(a == b)));
#endif
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec4qb operator < (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 1);
#else
    return _mm256_cmpgt_epi64(b, a);
#endif
}

// vector operator > : returns true for elements for which a > b
static inline Vec4qb operator > (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 6);
#else
    return b < a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4qb operator >= (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 5);
#else
    return Vec4qb(Vec4q(~(a < b)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4qb operator <= (Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec4q operator & (Vec4q const a, Vec4q const b) {
    return Vec4q(Vec256b(a) & Vec256b(b));
}
static inline Vec4q operator && (Vec4q const a, Vec4q const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec4q & operator &= (Vec4q & a, Vec4q const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec4q operator | (Vec4q const a, Vec4q const b) {
    return Vec4q(Vec256b(a) | Vec256b(b));
}
static inline Vec4q operator || (Vec4q const a, Vec4q const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec4q & operator |= (Vec4q & a, Vec4q const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec4q operator ^ (Vec4q const a, Vec4q const b) {
    return Vec4q(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec4q & operator ^= (Vec4q & a, Vec4q const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec4q operator ~ (Vec4q const a) {
    return Vec4q( ~ Vec256b(a));
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec4qb operator ! (Vec4q const a) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epi64_mask (a, _mm256_setzero_si256(), 0);
#else
    return a == Vec4q(_mm256_setzero_si256());
#endif
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec4q select (Vec4qb const s, Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi64(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4q if_add (Vec4qb const f, Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi64 (a, f, a, b);
#else
    return a + (Vec4q(f) & b);
#endif
}

// Conditional subtract
static inline Vec4q if_sub (Vec4qb const f, Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi64 (a, f, a, b);
#else
    return a - (Vec4q(f) & b);
#endif
}

// Conditional multiply
static inline Vec4q if_mul (Vec4qb const f, Vec4q const a, Vec4q const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi64 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int64_t horizontal_add (Vec4q const a) {
    __m256i sum1  = _mm256_shuffle_epi32(a,0x0E);                     // high element
    __m256i sum2  = _mm256_add_epi64(a,sum1);                         // sum
    __m128i sum3  = _mm256_extracti128_si256(sum2, 1);                // get high part
    __m128i sum4  = _mm_add_epi64(_mm256_castsi256_si128(sum2),sum3); // add low and high parts
    return _emulate_movq(sum4);
}

// function max: a > b ? a : b
static inline Vec4q max(Vec4q const a, Vec4q const b) {
    return select(a > b, a, b);
}

// function min: a < b ? a : b
static inline Vec4q min(Vec4q const a, Vec4q const b) {
    return select(a < b, a, b);
}

// function abs: a >= 0 ? a : -a
static inline Vec4q abs(Vec4q const a) {
#if INSTRSET >= 10     // AVX512VL
    return _mm256_abs_epi64(a);
#else
    __m256i sign  = _mm256_cmpgt_epi64(_mm256_setzero_si256(), a);// 0 > a
    __m256i inv   = _mm256_xor_si256(a, sign);             // invert bits if negative
    return          _mm256_sub_epi64(inv, sign);           // add 1
#endif
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4q abs_saturated(Vec4q const a) {
#if INSTRSET >= 10
    return _mm256_min_epu64(abs(a), Vec4q(0x7FFFFFFFFFFFFFFF));    
#else
    __m256i absa   = abs(a);                               // abs(a)
    __m256i overfl = _mm256_cmpgt_epi64(_mm256_setzero_si256(), absa); // 0 > a
    return           _mm256_add_epi64(absa, overfl);       // subtract 1 if 0x8000000000000000
#endif
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4q rotate_left(Vec4q const a, int b) {
#if INSTRSET >= 10  // __AVX512VL__
    return _mm256_rolv_epi64(a, _mm256_set1_epi64x(int64_t(b)));
#else
    __m256i left  = _mm256_sll_epi64(a,_mm_cvtsi32_si128(b & 0x3F));    // a << b 
    __m256i right = _mm256_srl_epi64(a,_mm_cvtsi32_si128((-b) & 0x3F)); // a >> (64 - b)
    __m256i rot   = _mm256_or_si256(left, right);                       // or
    return  rot;
#endif
}


/*****************************************************************************
*
*          Vector of 4 64-bit unsigned integers
*
*****************************************************************************/

class Vec4uq : public Vec4q {
public:
    // Default constructor:
    Vec4uq() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec4uq(uint64_t i) {
        ymm = Vec4q((int64_t)i);
    }
    // Constructor to build from all elements:
    Vec4uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3) {
        ymm = Vec4q((int64_t)i0, (int64_t)i1, (int64_t)i2, (int64_t)i3);
    }
    // Constructor to build from two Vec2uq:
    Vec4uq(Vec2uq const a0, Vec2uq const a1) {
        ymm = set_m128ir(a0, a1);
    }
    // Constructor to convert from type __m256i used in intrinsics:
    Vec4uq(__m256i const x) {
        ymm = x;
    }
    // Assignment operator to convert from type __m256i used in intrinsics:
    Vec4uq & operator = (__m256i const x) {
        ymm = x;
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4uq & load(void const * p) {
        ymm = _mm256_loadu_si256((__m256i const*)p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4uq & load_a(void const * p) {
        ymm = _mm256_load_si256((__m256i const*)p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec4uq const insert(int index, uint64_t value) {
        Vec4q::insert(index, (int64_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(int index) const {
        return (uint64_t)Vec4q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2uq:
    Vec2uq get_low() const {
        return _mm256_castsi256_si128(ymm);
    }
    Vec2uq get_high() const {
        return _mm256_extractf128_si256(ymm,1);
    }
    static constexpr int elementtype() {
        return 11;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec4uq operator + (Vec4uq const a, Vec4uq const b) {
    return Vec4uq (Vec4q(a) + Vec4q(b));
}

// vector operator - : subtract
static inline Vec4uq operator - (Vec4uq const a, Vec4uq const b) {
    return Vec4uq (Vec4q(a) - Vec4q(b));
}

// vector operator * : multiply element by element
static inline Vec4uq operator * (Vec4uq const a, Vec4uq const b) {
    return Vec4uq (Vec4q(a) * Vec4q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec4uq operator >> (Vec4uq const a, uint32_t b) {
    return _mm256_srl_epi64(a,_mm_cvtsi32_si128((int)b)); 
}

// vector operator >> : shift right logical all elements
static inline Vec4uq operator >> (Vec4uq const a, int32_t b) {
    return a >> (uint32_t)b;
}
// vector operator >>= : shift right arithmetic
static inline Vec4uq & operator >>= (Vec4uq & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator << : shift left all elements
static inline Vec4uq operator << (Vec4uq const a, uint32_t b) {
    return Vec4uq ((Vec4q)a << (int32_t)b);
}
// vector operator << : shift left all elements
static inline Vec4uq operator << (Vec4uq const a, int32_t b) {
    return Vec4uq ((Vec4q)a << b);
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec4qb operator > (Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu64_mask (a, b, 6);
#else
    __m256i sign64 = Vec4uq(0x8000000000000000);
    __m256i aflip  = _mm256_xor_si256(a, sign64);
    __m256i bflip  = _mm256_xor_si256(b, sign64);
    Vec4q   cmp    = _mm256_cmpgt_epi64(aflip,bflip);
    return Vec4qb(cmp);
#endif
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4qb operator < (Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu64_mask (a, b, 1);
#else
    return b > a;
#endif
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4qb operator >= (Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu64_mask (a, b, 5);
#else
    return  Vec4qb(Vec4q(~(b > a)));
#endif
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4qb operator <= (Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_cmp_epu64_mask (a, b, 2);
#else
    return b >= a;
#endif
}

// vector operator & : bitwise and
static inline Vec4uq operator & (Vec4uq const a, Vec4uq const b) {
    return Vec4uq(Vec256b(a) & Vec256b(b));
}
static inline Vec4uq operator && (Vec4uq const a, Vec4uq const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4uq operator | (Vec4uq const a, Vec4uq const b) {
    return Vec4uq(Vec256b(a) | Vec256b(b));
}
static inline Vec4uq operator || (Vec4uq const a, Vec4uq const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4uq operator ^ (Vec4uq const a, Vec4uq const b) {
    return Vec4uq(Vec256b(a) ^ Vec256b(b));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec4uq select (Vec4qb const s, Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mov_epi64(b, s, a);
#else
    return selectb(s,a,b);
#endif
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4uq if_add (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_add_epi64 (a, f, a, b);
#else
    return a + (Vec4uq(f) & b);
#endif
}

// Conditional subtract
static inline Vec4uq if_sub (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_sub_epi64 (a, f, a, b);
#else
    return a - (Vec4uq(f) & b);
#endif
}

// Conditional multiply
static inline Vec4uq if_mul (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // compact boolean vectors
    return _mm256_mask_mullo_epi64 (a, f, a, b);
#else
    return select(f, a*b, a);
#endif
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint64_t horizontal_add (Vec4uq const a) {
    return (uint64_t)horizontal_add((Vec4q)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign/zero extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec8i const a) {
    __m256i signs = _mm256_srai_epi32(a,31);               // sign of all elements
    Vec4q   a01   = _mm256_unpacklo_epi32(a,signs);        // sign-extended a0, a1, a4, a5
    Vec4q   a23   = _mm256_unpackhi_epi32(a,signs);        // sign-extended a2, a3, a6, a7
    return  horizontal_add(a01 + a23);
}

static inline uint64_t horizontal_add_x (Vec8ui const a) {
    __m256i zero  = _mm256_setzero_si256();                // 0
    __m256i a01   = _mm256_unpacklo_epi32(a,zero);         // zero-extended a0, a1
    __m256i a23   = _mm256_unpackhi_epi32(a,zero);         // zero-extended a2, a3
    return (uint64_t)horizontal_add(Vec4q(a01) + Vec4q(a23));
}

// function max: a > b ? a : b
static inline Vec4uq max(Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // AVX512VL
    return _mm256_max_epu64 (a, b);
#else
    return Vec4uq(select(a > b, a, b));
#endif
}

// function min: a < b ? a : b
static inline Vec4uq min(Vec4uq const a, Vec4uq const b) {
#if INSTRSET >= 10  // AVX512VL
    return _mm256_min_epu64 (a, b);
#else
    return Vec4uq(select(a > b, b, a));
#endif
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

// Permute vector of 4 64-bit integers.
template <int i0, int i1, int i2, int i3 >
static inline Vec4q permute4(Vec4q const a) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m256i y = a;                                         // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec4q>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & perm_largeblock) != 0) {        // permute 128-bit blocks
        constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // get 128-bit permute pattern
        constexpr int j0 = L.a[0];
        constexpr int j1 = L.a[1];
#ifndef ZEXT_MISSING
        if constexpr (j0 == 0 && j1 == -1 && !(flags & perm_addz)) { // zero extend
            return _mm256_zextsi128_si256(_mm256_castsi256_si128(y)); 
        }
        if constexpr (j0 == 1 && j1 < 0 && !(flags & perm_addz)) {   // extract upper part, zero extend
            return _mm256_zextsi128_si256(_mm256_extracti128_si256(y, 1)); 
        }
#endif
        if constexpr ((flags & perm_perm) != 0  && !(flags & perm_zeroing)) {
            return _mm256_permute2x128_si256(y, y, (j0 & 1) | (j1 & 1) << 4);
        }
    } 
    if constexpr ((flags & perm_perm) != 0) {              // permutation needed
        if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            // try to fit various instructions
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm256_unpackhi_epi64(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm256_unpacklo_epi64(y, y);
            }
            else { // general permute
                y = _mm256_shuffle_epi32(a, uint8_t(flags >> perm_ipattern));
            }
        }
        else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
            y = _mm256_broadcastq_epi64(_mm256_castsi256_si128(y)); // broadcast first element
        }
        else {  // different patterns in two lanes
#if INSTRSET >= 10  // AVX512VL 
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                return _mm256_maskz_alignr_epi64 (zero_mask<4>(indexs), y, y, rot);
            } 
            else { // full permute
                constexpr uint8_t mms = (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6;
                constexpr __mmask8 mmz = zero_mask<4>(indexs);//(i0 >= 0) | (i1 >= 0) << 1 | (i2 >= 0) << 2 | (i3 >= 0) << 3;
                return _mm256_maskz_permutex_epi64(mmz, a, mms);
            }
#else
            // full permute
            constexpr int ms = (i0 & 3) | (i1 & 3) << 2 | (i2 & 3) << 4 | (i3 & 3) << 6;        
            y = _mm256_permute4x64_epi64(a, ms);
#endif
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi64(zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int64_t, 4> bm = zero_mask_broad<Vec4q>(indexs);
        y = _mm256_and_si256(Vec4q().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3>
static inline Vec4uq permute4(Vec4uq const a) {
    return Vec4uq (permute4<i0,i1,i2,i3> (Vec4q(a)));
}


// Permute vector of 8 32-bit integers.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8i permute8(Vec8i const a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m256i y = a;                                         // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec8i>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {              // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {    // use larger permutation
            constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // permutation pattern
            y = permute4 <L.a[0], L.a[1], L.a[2], L.a[3]> (Vec4q(a));
            if (!(flags & perm_addz)) return y;            // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            // try to fit various instructions
            if constexpr ((flags & perm_punpckh) != 0) {   // fits punpckhi
                y = _mm256_unpackhi_epi32(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){ // fits punpcklo
                y = _mm256_unpacklo_epi32(y, y);
            }
            else { // general permute
                y = _mm256_shuffle_epi32(a, uint8_t(flags >> perm_ipattern));
            }
        }
#if INSTRSET >= 10
        else if constexpr ((flags & perm_broadcast) != 0 && (flags & perm_zeroing) == 0) {
            constexpr uint8_t e = flags >> perm_rot_count & 0xF; // broadcast one element
            if constexpr (e > 0) {
                y = _mm256_alignr_epi32(y, y, e);
            }
            return _mm256_broadcastd_epi32(_mm256_castsi256_si128(y));
#else
        else if constexpr ((flags & perm_broadcast) != 0 && (flags & perm_zeroing) == 0 && (flags >> perm_rot_count == 0)) {
            return _mm256_broadcastd_epi32(_mm256_castsi256_si128(y)); // broadcast first element
#endif
        }
        else if constexpr ((flags & perm_zext) != 0) {        
            y = _mm256_cvtepu32_epi64(_mm256_castsi256_si128(y));  // zero extension
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#if INSTRSET >= 10  // AVX512VL 
        else if constexpr ((flags & perm_compress) != 0) {
            y = _mm256_maskz_compress_epi32(__mmask8(compress_mask(indexs)), y); // compress
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
        else if constexpr ((flags & perm_expand) != 0) {
            y = _mm256_maskz_expand_epi32(__mmask8(expand_mask(indexs)), y); // expand
            if constexpr ((flags & perm_addz2) == 0) return y;
        }
#endif
        else {  // different patterns in two lanes
#if INSTRSET >= 10  // AVX512VL 
            if constexpr ((flags & perm_rotate_big) != 0) { // fits big rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotation count
                return _mm256_maskz_alignr_epi32(zero_mask<8>(indexs), y, y, rot);
            }
            else
#endif
            if constexpr ((flags & perm_cross_lane) == 0) {  // no lane crossing. Use pshufb
                const EList <int8_t, 32> bm = pshufb_mask<Vec8i>(indexs);
                return _mm256_shuffle_epi8(a, Vec8i().load(bm.a));
            }
            // full permute needed
            __m256i permmask = constant8ui < 
                i0 & 7, i1 & 7, i2 & 7, i3 & 7, i4 & 7, i5 & 7, i6 & 7, i7 & 7 > ();
#if INSTRSET >= 10  // AVX512VL
            return _mm256_maskz_permutexvar_epi32 (zero_mask<8>(indexs), permmask, y);
#else
            y =_mm256_permutevar8x32_epi32(y, permmask);
#endif
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {
        // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi32(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 8> bm = zero_mask_broad<Vec8i>(indexs);
        y = _mm256_and_si256(Vec8i().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8ui permute8(Vec8ui const a) {
    return Vec8ui (permute8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8i(a)));
}


// Permute vector of 16 16-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16s permute16(Vec16s const a) {
    int constexpr indexs[16] = {  // indexes as array
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };    
    __m256i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec16s>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {                   // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {         // use larger permutation
            constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // permutation pattern
            y = permute8 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> (Vec8i(a));
            if (!(flags & perm_addz)) return y;                 // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            // try to fit various instructions
            if constexpr ((flags & perm_punpckh) != 0) {        // fits punpckhi
                y = _mm256_unpackhi_epi16(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){      // fits punpcklo
                y = _mm256_unpacklo_epi16(y, y);
            }
            else if constexpr ((flags & perm_rotate) != 0) {    // fits palignr. rotate within lanes
                y = _mm256_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF); 
            } 
            else {
                // flags for 16 bit permute instructions
                constexpr uint64_t flags16 = perm16_flags<Vec16s>(indexs);
                constexpr bool L2L = (flags16 & 1) != 0;        // from low  to low  64-bit part
                constexpr bool H2H = (flags16 & 2) != 0;        // from high to high 64-bit part
                constexpr bool H2L = (flags16 & 4) != 0;        // from high to low  64-bit part
                constexpr bool L2H = (flags16 & 8) != 0;        // from low  to high 64-bit part
                constexpr uint8_t pL2L = uint8_t(flags16 >> 32);// low  to low  permute pattern
                constexpr uint8_t pH2H = uint8_t(flags16 >> 40);// high to high permute pattern
                constexpr uint8_t noperm = 0xE4;                // pattern for no permute
                if constexpr (!H2L && !L2H) {                   // simple case. no crossing of 64-bit boundary
                    if constexpr (L2L && pL2L != noperm) {
                        y = _mm256_shufflelo_epi16(y, pL2L);    // permute low 64-bits
                    }
                    if constexpr (H2H && pH2H != noperm) {
                        y = _mm256_shufflehi_epi16(y, pH2H);    // permute high 64-bits
                    }
                }
                else {  // use pshufb
                    const EList <int8_t, 32> bm = pshufb_mask<Vec16s>(indexs);
                    return _mm256_shuffle_epi8(a, Vec16s().load(bm.a));
                }
            }
        }
        else {  // different patterns in two lanes
            if constexpr ((flags & perm_zext) != 0) {     // fits zero extension
                y = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(y));  // zero extension
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm256_maskz_compress_epi16(__mmask16(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm256_maskz_expand_epi16(__mmask16(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#endif  // AVX512VBMI2
            else if constexpr ((flags & perm_cross_lane) == 0) {     // no lane crossing. Use pshufb
                const EList <int8_t, 32> bm = pshufb_mask<Vec16s>(indexs);
                return _mm256_shuffle_epi8(a, Vec16s().load(bm.a));
            }
            else if constexpr ((flags & perm_rotate_big) != 0) {// fits full rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count) * 2; // rotate count
                __m256i swap = _mm256_permute4x64_epi64(a,0x4E);// swap 128-bit halves
                if (rot <= 16) {
                    y = _mm256_alignr_epi8(swap, y, rot);
                }
                else {
                    y = _mm256_alignr_epi8(y, swap, rot & 15);
                }
            }
            else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
                y = _mm256_broadcastw_epi16(_mm256_castsi256_si128(y)); // broadcast first element
            }
            else {  // full permute needed
#if INSTRSET >= 10  // AVX512VL
                const EList <int16_t, 16> bm = perm_mask_broad<Vec16s>(indexs);
                y = _mm256_permutexvar_epi16(Vec16s().load(bm.a), y);
#else           // no full permute instruction available
                __m256i swap = _mm256_permute4x64_epi64(y,0x4E);// swap high and low 128-bit lane
                const EList <int8_t, 32> bm1 = pshufb_mask<Vec16s, 1>(indexs);
                const EList <int8_t, 32> bm2 = pshufb_mask<Vec16s, 0>(indexs);
                __m256i r1 = _mm256_shuffle_epi8(swap, Vec16s().load(bm1.a));
                __m256i r2 = _mm256_shuffle_epi8(y,    Vec16s().load(bm2.a));
                return       _mm256_or_si256(r1, r2);
#endif
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) {           // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi16(zero_mask<16>(indexs), y);
#else               // use broad mask
        const EList <int16_t, 16> bm = zero_mask_broad<Vec16s>(indexs);
        y = _mm256_and_si256(Vec16s().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16us permute16(Vec16us const a) {
    return Vec16us (permute16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16s(a)));
}


template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
static inline Vec32c permute32(Vec32c const a) {
    int constexpr indexs[32] = {  // indexes as array
        i0,  i1,  i2,  i3,  i4,  i5,  i6,  i7,  i8,  i9,  i10, i11, i12, i13, i14, i15,
        i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31 };

    __m256i y = a;  // result
    // get flags for possibilities that fit the permutation pattern
    constexpr uint64_t flags = perm_flags<Vec32c>(indexs);

    static_assert((flags & perm_outofrange) == 0, "Index out of range in permute function");

    if constexpr ((flags & perm_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & perm_perm) != 0) {                   // permutation needed

        if constexpr ((flags & perm_largeblock) != 0) {         // use larger permutation
            constexpr EList<int, 16> L = largeblock_perm<32>(indexs); // permutation pattern
            y = permute16 <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7],
                L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]> (Vec16s(a));
            if (!(flags & perm_addz)) return y;                 // no remaining zeroing
        }
        else if constexpr ((flags & perm_same_pattern) != 0) {  // same pattern in both lanes
            if constexpr ((flags & perm_punpckh) != 0) {        // fits punpckhi
                y = _mm256_unpackhi_epi8(y, y);
            }
            else if constexpr ((flags & perm_punpckl)!=0){      // fits punpcklo
                y = _mm256_unpacklo_epi8(y, y);
            }
            else if constexpr ((flags & perm_rotate) != 0) {    // fits palignr. rotate within lanes
                y = _mm256_alignr_epi8(a, a, (flags >> perm_rot_count) & 0xF); 
            } 
            else { // use pshufb
                const EList <int8_t, 32> bm = pshufb_mask<Vec32c>(indexs);
                return _mm256_shuffle_epi8(a, Vec32c().load(bm.a));
            }
        }
        else {  // different patterns in two lanes
            if constexpr ((flags & perm_zext) != 0) {     // fits zero extension
                y = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(y));  // zero extension
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#if INSTRSET >= 10 && defined (__AVX512VBMI2__)
            else if constexpr ((flags & perm_compress) != 0) {
                y = _mm256_maskz_compress_epi8(__mmask32(compress_mask(indexs)), y); // compress
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
            else if constexpr ((flags & perm_expand) != 0) {
                y = _mm256_maskz_expand_epi8(__mmask32(expand_mask(indexs)), y); // expand
                if constexpr ((flags & perm_addz2) == 0) return y;
            }
#endif  // AVX512VBMI2
            else if constexpr ((flags & perm_cross_lane) == 0) {     // no lane crossing. Use pshufb
                const EList <int8_t, 32> bm = pshufb_mask<Vec32c>(indexs);
                return _mm256_shuffle_epi8(a, Vec32c().load(bm.a));
            }
            else if constexpr ((flags & perm_rotate_big) != 0) {// fits full rotate
                constexpr uint8_t rot = uint8_t(flags >> perm_rot_count); // rotate count
                __m256i swap = _mm256_permute4x64_epi64(a,0x4E);// swap 128-bit halves
                if (rot <= 16) {
                    y = _mm256_alignr_epi8(swap, y, rot);
                }
                else {
                    y = _mm256_alignr_epi8(y, swap, rot & 15);
                }
            }
            else if constexpr ((flags & perm_broadcast) != 0 && (flags >> perm_rot_count) == 0) {
                y = _mm256_broadcastb_epi8(_mm256_castsi256_si128(y)); // broadcast first element
            }
            else {  // full permute needed
#if INSTRSET >= 10 && defined ( __AVX512VBMI__ ) // AVX512VBMI
                const EList <int8_t, 32> bm = perm_mask_broad<Vec32c>(indexs);
                y = _mm256_permutexvar_epi8(Vec32c().load(bm.a), y);
#else       
                // no full permute instruction available
                __m256i swap = _mm256_permute4x64_epi64(y, 0x4E);  // swap high and low 128-bit lane
                const EList <int8_t, 32> bm1 = pshufb_mask<Vec32c, 1>(indexs);
                const EList <int8_t, 32> bm2 = pshufb_mask<Vec32c, 0>(indexs);
                __m256i r1 = _mm256_shuffle_epi8(swap, Vec32c().load(bm1.a));
                __m256i r2 = _mm256_shuffle_epi8(y,    Vec32c().load(bm2.a));
                return       _mm256_or_si256(r1, r2);
#endif
            }
        }
    }
    if constexpr ((flags & perm_zeroing) != 0) { // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi8(zero_mask<32>(indexs), y);
#else  // use broad mask
        const EList <int8_t, 32> bm = zero_mask_broad<Vec32c>(indexs);
        y = _mm256_and_si256(Vec32c().load(bm.a), y);
#endif
    }
    return y;
}

template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
    static inline Vec32uc permute32(Vec32uc const a) {
        return Vec32uc (permute32<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,    
            i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (Vec32c(a)));
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

// permute and blend Vec4q
template <int i0, int i1, int i2, int i3>
static inline Vec4q blend4(Vec4q const a, Vec4q const b) {
    int constexpr indexs[4] = { i0, i1, i2, i3 };          // indexes as array
    __m256i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec4q>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & blend_b) == 0) {                // nothing from b. just permute a
        return permute4 <i0, i1, i2, i3> (a);
    }
    if constexpr ((flags & blend_a) == 0) {                // nothing from a. just permute b
        return permute4 <i0<0?i0:i0&3, i1<0?i1:i1&3, i2<0?i2:i2&3, i3<0?i3:i3&3> (b);
    } 
    if constexpr ((flags & (blend_perma | blend_permb)) == 0) { // no permutation, only blending
        constexpr uint8_t mb = (uint8_t)make_bit_mask<4, 0x302>(indexs);  // blend mask
#if INSTRSET >= 10 // AVX512VL
        y = _mm256_mask_mov_epi64 (a, mb, b);
#else  // AVX2
        y = _mm256_blend_epi32(a, b, ((mb & 1) | (mb & 2) << 1 | (mb & 4) << 2 | (mb & 8) << 3) * 3); // duplicate each bit
#endif        
    }
    else if constexpr ((flags & blend_largeblock) != 0) {  // blend and permute 128-bit blocks
        constexpr EList<int, 2> L = largeblock_perm<4>(indexs); // get 128-bit blend pattern
        constexpr uint8_t pp = (L.a[0] & 0xF) | uint8_t(L.a[1] & 0xF) << 4;
        y = _mm256_permute2x128_si256(a, b, pp);
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm256_unpacklo_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm256_unpacklo_epi64 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm256_unpackhi_epi64 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm256_unpackhi_epi64 (b, a);
    }
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm256_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm256_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm256_castpd_si256(_mm256_shuffle_pd(_mm256_castsi256_pd(a), _mm256_castsi256_pd(b), (flags >> blend_shufpattern) & 0xF));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm256_castpd_si256(_mm256_shuffle_pd(_mm256_castsi256_pd(b), _mm256_castsi256_pd(a), (flags >> blend_shufpattern) & 0xF));
    }
#endif
    else { // No special cases
#if INSTRSET >= 10  // AVX512VL. use vpermi2q
        __m256i const maskp = constant8ui<i0 & 15, 0, i1 & 15, 0, i2 & 15, 0, i3 & 15, 0>();
        return _mm256_maskz_permutex2var_epi64 (zero_mask<4>(indexs), a, maskp, b);
#else   // permute a and b separately, then blend.
        constexpr EList<int, 8> L = blend_perm_indexes<4, 0>(indexs); // get permutation indexes
        __m256i ya = permute4<L.a[0], L.a[1], L.a[2], L.a[3]>(a);
        __m256i yb = permute4<L.a[4], L.a[5], L.a[6], L.a[7]>(b);
        constexpr uint8_t mb = (uint8_t)make_bit_mask<4, 0x302>(indexs);  // blend mask
        y = _mm256_blend_epi32(ya, yb, ((mb & 1) | (mb & 2) << 1 | (mb & 4) << 2 | (mb & 8) << 3) * 3); // duplicate each bit
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi64(zero_mask<4>(indexs), y);
#else  // use broad mask
        const EList <int64_t, 4> bm = zero_mask_broad<Vec4q>(indexs);
        y = _mm256_and_si256(Vec4q().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3> 
static inline Vec4uq blend4(Vec4uq const a, Vec4uq const b) {
    return Vec4uq(blend4<i0,i1,i2,i3> (Vec4q(a),Vec4q(b)));
}


// permute and blend Vec8i
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8i blend8(Vec8i const a, Vec8i const b) {  
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    __m256i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec8i>(indexs); // get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & blend_largeblock) != 0) {       // blend and permute 32-bit blocks
        constexpr EList<int, 4> L = largeblock_perm<8>(indexs); // get 32-bit blend pattern
        y = blend4<L.a[0], L.a[1], L.a[2], L.a[3]> (Vec4q(a), Vec4q(b));
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
        y = _mm256_mask_mov_epi32 (a, mb, b);
#else  // AVX2
        y = _mm256_blend_epi32(a, b, mb); 
#endif        
    }
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm256_unpacklo_epi32 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm256_unpacklo_epi32 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm256_unpackhi_epi32 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm256_unpackhi_epi32 (b, a);
    }
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm256_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm256_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
#if ALLOW_FP_PERMUTE  // allow floating point permute instructions on integer vectors
    else if constexpr ((flags & blend_shufab) != 0) {      // use floating point instruction shufpd
        y = _mm256_castps_si256(_mm256_shuffle_ps(_mm256_castsi256_ps(a), _mm256_castsi256_ps(b), uint8_t(flags >> blend_shufpattern)));
    }
    else if constexpr ((flags & blend_shufba) != 0) {      // use floating point instruction shufpd
        y = _mm256_castps_si256(_mm256_shuffle_ps(_mm256_castsi256_ps(b), _mm256_castsi256_ps(a), uint8_t(flags >> blend_shufpattern)));
    }
#endif
    else { // No special cases
#if INSTRSET >= 10  // AVX512VL. use vpermi2d
        __m256i const maskp = constant8ui<i0 & 15, i1 & 15, i2 & 15, i3 & 15, i4 & 15, i5 & 15, i6 & 15, i7 & 15> ();
        return _mm256_maskz_permutex2var_epi32 (zero_mask<8>(indexs), a, maskp, b);
#else   // permute a and b separately, then blend.
        constexpr EList<int, 16> L = blend_perm_indexes<8, 0>(indexs); // get permutation indexes
        __m256i ya = permute8<L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]>(a);
        __m256i yb = permute8<L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15]>(b);
        constexpr uint8_t mb = (uint8_t)make_bit_mask<8, 0x303>(indexs);  // blend mask
        y = _mm256_blend_epi32(ya, yb, mb); 
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi32(zero_mask<8>(indexs), y);
#else  // use broad mask
        const EList <int32_t, 8> bm = zero_mask_broad<Vec8i>(indexs);
        y = _mm256_and_si256(Vec8i().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8ui blend8(Vec8ui const a, Vec8ui const b) {
    return Vec8ui( blend8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8i(a),Vec8i(b)));
}


// permute and blend Vec16s
template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
    static inline Vec16s blend16(Vec16s const a, Vec16s const b) {  
    int constexpr indexs[16] = { 
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };// indexes as array
    __m256i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec16s>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & blend_largeblock) != 0) {       // blend and permute 32-bit blocks
        constexpr EList<int, 8> L = largeblock_perm<16>(indexs); // get 32-bit blend pattern
        y = blend8<L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> (Vec8i(a), Vec8i(b));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing        
    }
    else if constexpr ((flags & blend_b) == 0) {           // nothing from b. just permute a
        return permute16 <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15> (a);
    }
    else if constexpr ((flags & blend_a) == 0) {           // nothing from a. just permute b
        constexpr EList<int, 32> L = blend_perm_indexes<16, 2>(indexs); // get permutation indexes
        return permute16 < 
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31]> (b);
    } 
    // check if pattern fits special cases
    else if constexpr ((flags & blend_punpcklab) != 0) { 
        y = _mm256_unpacklo_epi16 (a, b);
    }
    else if constexpr ((flags & blend_punpcklba) != 0) { 
        y = _mm256_unpacklo_epi16 (b, a);
    }
    else if constexpr ((flags & blend_punpckhab) != 0) { 
        y = _mm256_unpackhi_epi16 (a, b);
    }
    else if constexpr ((flags & blend_punpckhba) != 0) { 
        y = _mm256_unpackhi_epi16 (b, a);
    }
    else if constexpr ((flags & blend_rotateab) != 0) { 
        y = _mm256_alignr_epi8(a, b, flags >> blend_rotpattern);
    }
    else if constexpr ((flags & blend_rotateba) != 0) { 
        y = _mm256_alignr_epi8(b, a, flags >> blend_rotpattern);
    }
    else { // No special cases
#if INSTRSET >= 10  // AVX512VL. use vpermi2w
        if constexpr ((flags & (blend_perma | blend_permb)) != 0) {
            const EList <int16_t, 16> bm = perm_mask_broad<Vec16s>(indexs);
            return _mm256_maskz_permutex2var_epi16(zero_mask<16>(indexs), a, Vec16s().load(bm.a), b);
        }
#endif
        // permute a and b separately, then blend.
        Vec16s ya = a, yb = b;  // a and b permuted
        constexpr EList<int, 32> L = blend_perm_indexes<16, 0>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0) {
            ya = permute16<
                L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7],
                L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] >(ya);
        }
        if constexpr ((flags & blend_permb) != 0) {
            yb = permute16<
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23], 
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31] >(yb);
        }
        constexpr uint16_t mb = (uint16_t)make_bit_mask<16, 0x304>(indexs);  // blend mask
#if INSTRSET >= 10 // AVX512VL
        y = _mm256_mask_mov_epi16 (ya, mb, yb);
#else  // AVX2
        if ((flags & blend_same_pattern) != 0) {           // same blend pattern in both 128-bit lanes    
            y = _mm256_blend_epi16(ya, yb, (uint8_t)mb); 
        }
        else {
            const EList <int16_t, 16> bm = make_broad_mask<Vec16s>(mb);
            y = _mm256_blendv_epi8 (ya, yb, Vec16s().load(bm.a));
        }
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi16(zero_mask<16>(indexs), y);
#else  // use broad mask
        const EList <int16_t, 16> bm = zero_mask_broad<Vec16s>(indexs);
        y = _mm256_and_si256(Vec16s().load(bm.a), y);
#endif
    }
    return y;
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16us blend16(Vec16us const a, Vec16us const b) {
    return Vec16us( blend16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16s(a),Vec16s(b)));
}


// permute and blend Vec32c
template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 > 
static inline Vec32c blend32(Vec32c const a, Vec32c const b) {
    int constexpr indexs[32] = { 
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
        i16, i17, i18, i19, i20, i21, i22, i23,i24, i25, i26, i27, i28, i29, i30, i31 };                  // indexes as array
    __m256i y = a;                                         // result
    constexpr uint64_t flags = blend_flags<Vec32c>(indexs);// get flags for possibilities that fit the index pattern

    static_assert((flags & blend_outofrange) == 0, "Index out of range in blend function");

    if constexpr ((flags & blend_allzero) != 0) return _mm256_setzero_si256();  // just return zero

    if constexpr ((flags & blend_largeblock) != 0) {       // blend and permute 16-bit blocks
        constexpr EList<int, 16> L = largeblock_perm<32>(indexs); // get 16-bit blend pattern
        y = blend16 < L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7],
            L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] > 
            (Vec16s(a), Vec16s(b));
        if (!(flags & blend_addz)) return y;               // no remaining zeroing        
    }
    else if constexpr ((flags & blend_b) == 0) {           // nothing from b. just permute a
        return permute32 <i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
            i16, i17, i18, i19, i20, i21, i22, i23,i24, i25, i26, i27, i28, i29, i30, i31 > (a);
    }
    else if constexpr ((flags & blend_a) == 0) {           // nothing from a. just permute b
        constexpr EList<int, 64> L = blend_perm_indexes<32, 2>(indexs); // get permutation indexes
        return permute32 < 
            L.a[32], L.a[33], L.a[34], L.a[35], L.a[36], L.a[37], L.a[38], L.a[39],
            L.a[40], L.a[41], L.a[42], L.a[43], L.a[44], L.a[45], L.a[46], L.a[47],
            L.a[48], L.a[49], L.a[50], L.a[51], L.a[52], L.a[53], L.a[54], L.a[55],
            L.a[56], L.a[57], L.a[58], L.a[59], L.a[60], L.a[61], L.a[62], L.a[63] > (b);
    } 
    else { // No special cases
#if INSTRSET >= 10 && defined (__AVX512VBMI__) // AVX512VL + AVX512VBMI. use vpermi2b
        if constexpr ((flags & (blend_perma | blend_permb)) != 0) {
            const EList <int8_t, 32> bm = perm_mask_broad<Vec32c>(indexs);
            return _mm256_maskz_permutex2var_epi8(zero_mask<32>(indexs), a, Vec32c().load(bm.a), b);
        }
#endif
        // permute a and b separately, then blend.
        Vec32c ya = a, yb = b;  // a and b permuted
        constexpr EList<int, 64> L = blend_perm_indexes<32, 0>(indexs); // get permutation indexes
        if constexpr ((flags & blend_perma) != 0) {
            ya = permute32 <
                L.a[0],  L.a[1],  L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
                L.a[8],  L.a[9],  L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15],
                L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23], 
                L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31] > (ya);
        }
        if constexpr ((flags & blend_permb) != 0) {
            yb = permute32 <
                L.a[32], L.a[33], L.a[34], L.a[35], L.a[36], L.a[37], L.a[38], L.a[39],
                L.a[40], L.a[41], L.a[42], L.a[43], L.a[44], L.a[45], L.a[46], L.a[47],
                L.a[48], L.a[49], L.a[50], L.a[51], L.a[52], L.a[53], L.a[54], L.a[55],
                L.a[56], L.a[57], L.a[58], L.a[59], L.a[60], L.a[61], L.a[62], L.a[63] > (yb);
        }
        constexpr uint32_t mb = (uint32_t)make_bit_mask<32, 0x305>(indexs);// blend mask
#if INSTRSET >= 10 // AVX512VL
        y = _mm256_mask_mov_epi8 (ya, mb, yb);
#else  // AVX2
        const EList <int8_t, 32> bm = make_broad_mask<Vec32c>(mb);
        y = _mm256_blendv_epi8 (ya, yb, Vec32c().load(bm.a));
#endif
    }
    if constexpr ((flags & blend_zeroing) != 0) {          // additional zeroing needed
#if INSTRSET >= 10  // use compact mask
        y = _mm256_maskz_mov_epi8(zero_mask<32>(indexs), y);
#else  // use broad mask
        const EList <int8_t, 32> bm = zero_mask_broad<Vec32c>(indexs);
        y = _mm256_and_si256(Vec32c().load(bm.a), y);
#endif
    }
    return y;
}

template <int ... i0>    
static inline Vec32uc blend32(Vec32uc const a, Vec32uc const b) {        
    return Vec32uc (blend32 <i0 ...> (Vec32c(a), Vec32c(b)));
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

static inline Vec32c lookup32(Vec32c const index, Vec32c const table) {
#ifdef __XOP__  // AMD XOP instruction set. Use VPPERM
    Vec16c t0 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_low());
    Vec16c t1 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_high());
    return Vec32c(t0, t1);
#else
    Vec32c f0 = constant8ui<0,0,0,0,0x10101010,0x10101010,0x10101010,0x10101010>();
    Vec32c f1 = constant8ui<0x10101010,0x10101010,0x10101010,0x10101010,0,0,0,0>();
    Vec32c tablef = _mm256_permute4x64_epi64(table, 0x4E);   // low and high parts swapped
    Vec32c r0 = _mm256_shuffle_epi8(table,  (index ^ f0) + 0x70);
    Vec32c r1 = _mm256_shuffle_epi8(tablef, (index ^ f1) + 0x70);
    return r0 | r1;
#endif
}

template <int n>
static inline Vec32c lookup(Vec32uc const index, void const * table) {
    if (n <=  0) return 0;
    if (n <= 16) {
        Vec16c tt = Vec16c().load(table);
        Vec16c r0 = lookup16(index.get_low(),  tt);
        Vec16c r1 = lookup16(index.get_high(), tt);
        return Vec32c(r0, r1);
    }
    if (n <= 32) return lookup32(index, Vec32c().load(table));
    // n > 32. Limit index
    Vec32uc index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec32uc(index) & uint8_t(n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec32uc(index), uint8_t(n-1));
    }
    Vec8ui mask0 = Vec8ui(0x000000FF);  // mask 8 bits
    Vec32c t0 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & Vec8ui(index1)),      1); // positions 0, 4, 8,  ...
    Vec32c t1 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & _mm256_srli_epi32(index1, 8)), 1); // positions 1, 5, 9,  ...
    Vec32c t2 = _mm256_i32gather_epi32((const int *)table, __m256i(mask0 & _mm256_srli_epi32(index1,16)), 1); // positions 2, 6, 10, ...
    Vec32c t3 = _mm256_i32gather_epi32((const int *)table, _mm256_srli_epi32(index1,24), 1); // positions 3, 7, 11, ...
    t0 = t0 & Vec32c(mask0);
    t1 = _mm256_slli_epi32(t1 & Vec32c(mask0),  8);
    t2 = _mm256_slli_epi32(t2 & Vec32c(mask0), 16);
    t3 = _mm256_slli_epi32(t3,         24);
    return (t0 | t3) | (t1 | t2);
}

template <int n>
static inline Vec32c lookup(Vec32c const index, void const * table) {
    return lookup<n>(Vec32uc(index), table);
}


static inline Vec16s lookup16(Vec16s const index, Vec16s const table) {
    return Vec16s(lookup32(Vec32c(index * 0x202 + 0x100), Vec32c(table)));
}

template <int n>
static inline Vec16s lookup(Vec16s const index, void const * table) {
    if (n <=  0) return 0;
    if (n <=  8) {
        Vec8s table1 = Vec8s().load(table);        
        return Vec16s(       
            lookup8 (index.get_low(),  table1),
            lookup8 (index.get_high(), table1));
    }
    if (n <= 16) return lookup16(index, Vec16s().load(table));
    // n > 16. Limit index
    Vec16us index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec16us(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec16us(index), n-1);
    }
    Vec16s t1 = _mm256_i32gather_epi32((const int *)table, __m256i(Vec8ui(index1) & 0x0000FFFF), 2);  // even positions
    Vec16s t2 = _mm256_i32gather_epi32((const int *)table, _mm256_srli_epi32(index1, 16) , 2);        // odd  positions
    return blend16<0,16,2,18,4,20,6,22,8,24,10,26,12,28,14,30>(t1, t2);
}

static inline Vec8i lookup8(Vec8i const index, Vec8i const table) {
    return _mm256_permutevar8x32_epi32(table, index);
}

template <int n>
static inline Vec8i lookup(Vec8i const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8i table1 = Vec8i().load(table);
        return lookup8(index, table1);
    }
    if (n <= 16) {
        Vec8i table1 = Vec8i().load(table);
        Vec8i table2 = Vec8i().load((int32_t const*)table + 8);
        Vec8i y1 = lookup8(index, table1);
        Vec8i y2 = lookup8(index, table2);
        Vec8ib s = index > 7;
        return select(s, y2, y1);
    }
    // n > 16. Limit index
    Vec8ui index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        index1 = min(Vec8ui(index), n-1);
    }
    return _mm256_i32gather_epi32((const int *)table, index1, 4);
}

static inline Vec4q lookup4(Vec4q const index, Vec4q const table) {
    return Vec4q(lookup8(Vec8i(index * 0x200000002ll + 0x100000000ll), Vec8i(table)));
}

template <int n>
static inline Vec4q lookup(Vec4q const index, int64_t const * table) {
    if (n <= 0) return 0;
    // n > 0. Limit index
    Vec4uq index1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        index1 = Vec4uq(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1.
        // There is no 64-bit min instruction, but we can use the 32-bit unsigned min,
        // since n is a 32-bit integer
        index1 = Vec4uq(min(Vec8ui(index), constant8ui<n-1, 0, n-1, 0, n-1, 0, n-1, 0>()));
    }
/* old compilers can't agree how to define a 64 bit integer. Intel and MS use __int64, gcc use long long
#if defined (__clang__) && CLANG_VERSION < 30400
// clang 3.3 uses const int * in accordance with official Intel doc., which is wrong. will be fixed
    return _mm256_i64gather_epi64((const int *)table, index1, 8);
#elif defined (_MSC_VER) && _MSC_VER < 1700 && ! defined(__INTEL_COMPILER)
// Old MS and Intel use non-standard type __int64
    return _mm256_i64gather_epi64((const int64_t *)table, index1, 8);
#else
// Gnu, Clang 3.4, MS 11.0
*/
    return _mm256_i64gather_epi64((const long long *)table, index1, 8);
//#endif
}


/*****************************************************************************
*
*          Byte shifts
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
template <unsigned int b>
static inline Vec32c shift_bytes_up(Vec32c const a) {
    __m256i ahi, alo;
    if constexpr (b == 0) return a;
#if INSTRSET >= 10  // AVX512VL
    else if constexpr ((b & 3) == 0) {  // b is divisible by 4
        return _mm256_alignr_epi32(a, _mm256_setzero_si256(), (8 - (b >> 2)) & 7);
    }
#endif
    else if constexpr (b < 16) {    
        alo = a;
        ahi = _mm256_inserti128_si256 (_mm256_setzero_si256(), _mm256_castsi256_si128(a), 1);// shift a 16 bytes up, zero lower part
    }
    else if constexpr (b < 32) {    
        alo = _mm256_inserti128_si256 (_mm256_setzero_si256(), _mm256_castsi256_si128(a), 1);// shift a 16 bytes up, zero lower part
        ahi = _mm256_setzero_si256();  
    }
    else {
        return _mm256_setzero_si256();                     // zero
    }
    if constexpr ((b & 0xF) == 0) return alo;              // modulo 16. no more shift needeed
    return _mm256_alignr_epi8(alo, ahi, 16-(b & 0xF));     // shift within 16-bytes lane
} 

// Function shift_bytes_down: shift whole vector right by b bytes
template <unsigned int b>
static inline Vec32c shift_bytes_down(Vec32c const a) {
#if INSTRSET >= 10  // AVX512VL
    if constexpr ((b & 3) == 0) {  // b is divisible by 4
        return _mm256_alignr_epi32(_mm256_setzero_si256(), a, (b >> 2) & 7);
    }
#endif 
    __m256i ahi, alo;
    if constexpr (b < 16) {
        // shift a 16 bytes down, zero upper part
        alo = _mm256_inserti128_si256(_mm256_setzero_si256(), _mm256_extracti128_si256(a, 1), 0);// make sure the upper part is zero (otherwise, an optimizing compiler can mess it up)
        ahi = a;
    }
    else if constexpr (b < 32) {
        alo = _mm256_setzero_si256();                      // zero
        ahi = _mm256_inserti128_si256(_mm256_setzero_si256(), _mm256_extracti128_si256(a, 1), 0);// shift a 16 bytes down, zero upper part
    }
    else {
        return _mm256_setzero_si256();                     // zero
    }
    if constexpr ((b & 0xF) == 0) return ahi;              // modulo 16. no more shift needeed
    return _mm256_alignr_epi8(alo, ahi, b & 0xF);          // shift within 16-bytes lane
}


/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/
// Load elements from array a with indices i0, i1, i2, i3, i4, i5, i6, i7
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8i gather8i(void const * a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 7) {
        // load one contiguous block and permute
        if constexpr (imax > 7) {
            // make sure we don't read past the end of the array
            Vec8i b = Vec8i().load((int32_t const *)a + imax-7);
            return permute8<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7>(b);
        }
        else {
            Vec8i b = Vec8i().load((int32_t const *)a + imin);
            return permute8<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin>(b);
        }
    }
    if constexpr ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8i b = Vec8i().load((int32_t const *)a + imin);
        Vec8i c = Vec8i().load((int32_t const *)a + imax-7);
        constexpr int j0 = i0<imin+8 ? i0-imin : 15-imax+i0;
        constexpr int j1 = i1<imin+8 ? i1-imin : 15-imax+i1;
        constexpr int j2 = i2<imin+8 ? i2-imin : 15-imax+i2;
        constexpr int j3 = i3<imin+8 ? i3-imin : 15-imax+i3;
        constexpr int j4 = i4<imin+8 ? i4-imin : 15-imax+i4;
        constexpr int j5 = i5<imin+8 ? i5-imin : 15-imax+i5;
        constexpr int j6 = i6<imin+8 ? i6-imin : 15-imax+i6;
        constexpr int j7 = i7<imin+8 ? i7-imin : 15-imax+i7;
        return blend8<j0, j1, j2, j3, j4, j5, j6, j7>(b, c);
    }
    // use AVX2 gather
    return _mm256_i32gather_epi32((const int *)a, Vec8i(i0,i1,i2,i3,i4,i5,i6,i7), 4);
}

template <int i0, int i1, int i2, int i3>
static inline Vec4q gather4q(void const * a) {
    int constexpr indexs[4] = { i0, i1, i2, i3 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 3) {
        // load one contiguous block and permute
        if constexpr (imax > 3) {
            // make sure we don't read past the end of the array
            Vec4q b = Vec4q().load((int64_t const *)a + imax-3);
            return permute4<i0-imax+3, i1-imax+3, i2-imax+3, i3-imax+3>(b);
        }
        else {
            Vec4q b = Vec4q().load((int64_t const *)a + imin);
            return permute4<i0-imin, i1-imin, i2-imin, i3-imin>(b);
        }
    }
    if constexpr ((i0<imin+4 || i0>imax-4) && (i1<imin+4 || i1>imax-4) && (i2<imin+4 || i2>imax-4) && (i3<imin+4 || i3>imax-4)) {
        // load two contiguous blocks and blend
        Vec4q b = Vec4q().load((int64_t const *)a + imin);
        Vec4q c = Vec4q().load((int64_t const *)a + imax-3);
        const int j0 = i0<imin+4 ? i0-imin : 7-imax+i0;
        const int j1 = i1<imin+4 ? i1-imin : 7-imax+i1;
        const int j2 = i2<imin+4 ? i2-imin : 7-imax+i2;
        const int j3 = i3<imin+4 ? i3-imin : 7-imax+i3;
        return blend4<j0, j1, j2, j3>(b, c);
    }
    // use AVX2 gather
    return _mm256_i32gather_epi64((const long long *)a, Vec4i(i0,i1,i2,i3), 8);
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
static inline void scatter(Vec8i const data, void * array) {
#if INSTRSET >= 10 //  __AVX512VL__
    __m256i indx = constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>();
    __mmask8 mask = uint8_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3) |
        ((i4>=0)<<4) | ((i5>=0)<<5) | ((i6>=0)<<6) | ((i7>=0)<<7));
    _mm256_mask_i32scatter_epi32((int*)array, mask, indx, data, 4);
#elif INSTRSET >= 9  //  __AVX512F__
    __m512i indx = _mm512_castsi256_si512(constant8ui<i0,i1,i2,i3,i4,i5,i6,i7>());
    __mmask16 mask = uint16_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3) |
        ((i4>=0)<<4) | ((i5>=0)<<5) | ((i6>=0)<<6) | ((i7>=0)<<7));
    _mm512_mask_i32scatter_epi32((int*)array, mask, indx, _mm512_castsi256_si512(data), 4);
#else
    int32_t* arr = (int32_t*)array;
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
#endif
}

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4q const data, void * array) {
#if INSTRSET >= 10 //  __AVX512VL__
    __m128i indx = constant4ui<i0,i1,i2,i3>();
    __mmask8 mask = uint8_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3));
    _mm256_mask_i32scatter_epi64((long long *)array, mask, indx, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    __m256i indx = _mm256_castsi128_si256(constant4ui<i0,i1,i2,i3>());
    __mmask16 mask = uint16_t((i0>=0) | ((i1>=0)<<1) | ((i2>=0)<<2) | ((i3>=0)<<3));
    _mm512_mask_i32scatter_epi64((long long*)array, (__mmask8)mask, indx, _mm512_castsi256_si512(data), 8);
#else
    int64_t* arr = (int64_t*)array;
    const int index[4] = {i0,i1,i2,i3};
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
#endif
}


/*****************************************************************************
*
*          Scatter functions with variable indexes
*
*****************************************************************************/

static inline void scatter(Vec8i const index, uint32_t limit, Vec8i const data, void * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm256_cmplt_epu32_mask(index, Vec8ui(limit));
    _mm256_mask_i32scatter_epi32((int*)destination, mask, index, data, 4);
#elif INSTRSET >= 9  //  __AVX512F__
    // 16 bit mask, upper 8 bits are 0. Usually, we can rely on the upper bit of an extended vector to be zero, but we will mask then off the be sure
    //__mmask16 mask = _mm512_cmplt_epu32_mask(_mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec8ui(limit)));
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xFF, _mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec8ui(limit)));
    _mm512_mask_i32scatter_epi32((int*)destination, mask, _mm512_castsi256_si512(index), _mm512_castsi256_si512(data), 4);
#else
    int32_t* arr = (int32_t*)destination;
    for (int i = 0; i < 8; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
#endif
} 

static inline void scatter(Vec4q const index, uint32_t limit, Vec4q const data, void * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm256_cmplt_epu64_mask(index, Vec4uq(uint64_t(limit)));
    _mm256_mask_i64scatter_epi64((long long*)destination, mask, index, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    // 16 bit mask. upper 12 bits are 0
    __mmask16 mask = _mm512_mask_cmplt_epu64_mask(0xF, _mm512_castsi256_si512(index), _mm512_castsi256_si512(Vec4uq(uint64_t(limit))));
    _mm512_mask_i64scatter_epi64((long long*)destination, (__mmask8)mask, _mm512_castsi256_si512(index), _mm512_castsi256_si512(data), 8);
#else
    int64_t* arr = (int64_t*)destination;
    for (int i = 0; i < 4; i++) {
        if (uint64_t(index[i]) < uint64_t(limit)) arr[index[i]] = data[i];
    }
#endif
} 

static inline void scatter(Vec4i const index, uint32_t limit, Vec4q const data, void * destination) {
#if INSTRSET >= 10 //  __AVX512VL__
    __mmask8 mask = _mm_cmplt_epu32_mask(index, Vec4ui(limit));
    _mm256_mask_i32scatter_epi64((long long*)destination, mask, index, data, 8);
#elif INSTRSET >= 9  //  __AVX512F__
    // 16 bit mask. upper 12 bits are 0
    __mmask16 mask = _mm512_mask_cmplt_epu32_mask(0xF, _mm512_castsi128_si512(index), _mm512_castsi128_si512(Vec4ui(limit)));
    _mm512_mask_i32scatter_epi64((long long*)destination, (__mmask8)mask, _mm256_castsi128_si256(index), _mm512_castsi256_si512(data), 8);
#else
    int64_t* arr = (int64_t*)destination;
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
#endif
} 

/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 16 elements to 16 bits with sign extension
static inline Vec16s extend_low (Vec32c const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0x10);            // get bits 64-127 to position 128-191
    __m256i sign = _mm256_cmpgt_epi8(_mm256_setzero_si256(),a2); // 0 > a2
    return         _mm256_unpacklo_epi8(a2, sign);               // interleave with sign extensions
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec16s extend_high (Vec32c const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0xC8);            // get bits 128-191 to position 64-127
    __m256i sign = _mm256_cmpgt_epi8(_mm256_setzero_si256(),a2); // 0 > a2
    return         _mm256_unpackhi_epi8(a2, sign);               // interleave with sign extensions
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec16us extend_low (Vec32uc const a) {
    __m256i a2 = _mm256_permute4x64_epi64(a, 0x10);              // get bits 64-127 to position 128-191
    return    _mm256_unpacklo_epi8(a2, _mm256_setzero_si256());  // interleave with zero extensions
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec16us extend_high (Vec32uc const a) {
    __m256i a2 = _mm256_permute4x64_epi64(a, 0xC8);              // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi8(a2, _mm256_setzero_si256());    // interleave with zero extensions
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec8i extend_low (Vec16s const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0x10);            // get bits 64-127 to position 128-191
    __m256i sign = _mm256_srai_epi16(a2, 15);                    // sign bit
    return         _mm256_unpacklo_epi16(a2 ,sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec8i extend_high (Vec16s const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0xC8);            // get bits 128-191 to position 64-127
    __m256i sign = _mm256_srai_epi16(a2, 15);                    // sign bit
    return         _mm256_unpackhi_epi16(a2, sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec8ui extend_low (Vec16us const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0x10);            // get bits 64-127 to position 128-191
    return    _mm256_unpacklo_epi16(a2, _mm256_setzero_si256()); // interleave with zero extensions
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec8ui extend_high (Vec16us const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0xC8);            // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi16(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 64 bits with sign extension
static inline Vec4q extend_low (Vec8i const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0x10);            // get bits 64-127 to position 128-191
    __m256i sign = _mm256_srai_epi32(a2, 31);                    // sign bit
    return         _mm256_unpacklo_epi32(a2, sign);              // interleave with sign extensions
}

// Function extend_high : extends the high 4 elements to 64 bits with sign extension
static inline Vec4q extend_high (Vec8i const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0xC8);            // get bits 128-191 to position 64-127
    __m256i sign = _mm256_srai_epi32(a2, 31);                    // sign bit
    return         _mm256_unpackhi_epi32(a2, sign);              // interleave with sign extensions
}

// Function extend_low : extends the low 4 elements to 64 bits with zero extension
static inline Vec4uq extend_low (Vec8ui const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0x10);            // get bits 64-127 to position 128-191
    return  _mm256_unpacklo_epi32(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Function extend_high : extends the high 4 elements to 64 bits with zero extension
static inline Vec4uq extend_high (Vec8ui const a) {
    __m256i a2   = _mm256_permute4x64_epi64(a, 0xC8);            // get bits 128-191 to position 64-127
    return  _mm256_unpackhi_epi32(a2, _mm256_setzero_si256());   // interleave with zero extensions
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec32c compress (Vec16s const low, Vec16s const high) {
    __m256i mask  = _mm256_set1_epi32(0x00FF00FF);         // mask for low bytes
    __m256i lowm  = _mm256_and_si256(low, mask);           // bytes of low
    __m256i highm = _mm256_and_si256(high, mask);          // bytes of high
    __m256i pk    = _mm256_packus_epi16(lowm, highm);      // unsigned pack
    return          _mm256_permute4x64_epi64(pk, 0xD8);    // put in right place
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec32c compress_saturated (Vec16s const low, Vec16s const high) {
    __m256i pk    = _mm256_packs_epi16(low,high);          // packed with signed saturation
    return          _mm256_permute4x64_epi64(pk, 0xD8);    // put in right place
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec32uc compress (Vec16us const low, Vec16us const high) {
    return  Vec32uc (compress((Vec16s)low, (Vec16s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec32uc compress_saturated (Vec16us const low, Vec16us const high) {
    __m256i maxval  = _mm256_set1_epi32(0x00FF00FF);       // maximum value
    __m256i low1    = _mm256_min_epu16(low,maxval);        // upper limit
    __m256i high1   = _mm256_min_epu16(high,maxval);       // upper limit
    __m256i pk      = _mm256_packus_epi16(low1,high1);     // this instruction saturates from signed 32 bit to unsigned 16 bit
    return            _mm256_permute4x64_epi64(pk, 0xD8);  // put in right place
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16s compress (Vec8i const low, Vec8i const high) {
    __m256i mask  = _mm256_set1_epi32(0x0000FFFF);         // mask for low words
    __m256i lowm  = _mm256_and_si256(low,mask);            // words of low
    __m256i highm = _mm256_and_si256(high,mask);           // words of high
    __m256i pk    = _mm256_packus_epi32(lowm,highm);       // unsigned pack
    return          _mm256_permute4x64_epi64(pk, 0xD8);    // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec16s compress_saturated (Vec8i const low, Vec8i const high) {
    __m256i pk    =  _mm256_packs_epi32(low,high);         // pack with signed saturation
    return           _mm256_permute4x64_epi64(pk, 0xD8);   // put in right place
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16us compress (Vec8ui const low, Vec8ui const high) {
    return Vec16us (compress((Vec8i)low, (Vec8i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec16us compress_saturated (Vec8ui const low, Vec8ui const high) {
    __m256i maxval  = _mm256_set1_epi32(0x0000FFFF);       // maximum value
    __m256i low1    = _mm256_min_epu32(low,maxval);        // upper limit
    __m256i high1   = _mm256_min_epu32(high,maxval);       // upper limit
    __m256i pk      = _mm256_packus_epi32(low1,high1);     // this instruction saturates from signed 32 bit to unsigned 16 bit
    return            _mm256_permute4x64_epi64(pk, 0xD8);  // put in right place
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec8i compress (Vec4q const low, Vec4q const high) {
    __m256i low2  = _mm256_shuffle_epi32(low,0xD8);        // low dwords of low  to pos. 0 and 32
    __m256i high2 = _mm256_shuffle_epi32(high,0xD8);       // low dwords of high to pos. 0 and 32
    __m256i pk    = _mm256_unpacklo_epi64(low2,high2);     // interleave
    return          _mm256_permute4x64_epi64(pk, 0xD8);    // put in right place
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec8i compress_saturated (Vec4q const a, Vec4q const b) {
    Vec4q maxval = constant8ui<0x7FFFFFFF,0,0x7FFFFFFF,0,0x7FFFFFFF,0,0x7FFFFFFF,0>();
    Vec4q minval = constant8ui<0x80000000,0xFFFFFFFF,0x80000000,0xFFFFFFFF,0x80000000,0xFFFFFFFF,0x80000000,0xFFFFFFFF>();
    Vec4q a1  = min(a,maxval);
    Vec4q b1  = min(b,maxval);
    Vec4q a2  = max(a1,minval);
    Vec4q b2  = max(b1,minval);
    return compress(a2,b2);
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8ui compress (Vec4uq const low, Vec4uq const high) {
    return Vec8ui (compress((Vec4q)low, (Vec4q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec8ui compress_saturated (Vec4uq const low, Vec4uq const high) {
    __m256i zero     = _mm256_setzero_si256();             // 0
    __m256i lowzero  = _mm256_cmpeq_epi32(low,zero);       // for each dword is zero
    __m256i highzero = _mm256_cmpeq_epi32(high,zero);      // for each dword is zero
    __m256i mone     = _mm256_set1_epi32(-1);              // FFFFFFFF
    __m256i lownz    = _mm256_xor_si256(lowzero,mone);     // for each dword is nonzero
    __m256i highnz   = _mm256_xor_si256(highzero,mone);    // for each dword is nonzero
    __m256i lownz2   = _mm256_srli_epi64(lownz,32);        // shift down to low dword
    __m256i highnz2  = _mm256_srli_epi64(highnz,32);       // shift down to low dword
    __m256i lowsatur = _mm256_or_si256(low,lownz2);        // low, saturated
    __m256i hisatur  = _mm256_or_si256(high,highnz2);      // high, saturated
    return  Vec8ui (compress(Vec4q(lowsatur), Vec4q(hisatur)));
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 8 32-bit signed integers
static inline Vec8i operator / (Vec8i const a, Divisor_i const d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i sgn = _mm256_broadcastq_epi64(d.getsign());    // broadcast sign of d
    __m256i t1  = _mm256_mul_epi32(a,m);                   // 32x32->64 bit signed multiplication of even elements of a
    __m256i t2  = _mm256_srli_epi64(t1,32);                // high dword of even numbered results
    __m256i t3  = _mm256_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m256i t4  = _mm256_mul_epi32(t3,m);                  // 32x32->64 bit signed multiplication of odd elements
    __m256i t7  = _mm256_blend_epi32(t2,t4,0xAA);
    __m256i t8  = _mm256_add_epi32(t7,a);                  // add
    __m256i t9  = _mm256_sra_epi32(t8,d.gets1());          // shift right arithmetic
    __m256i t10 = _mm256_srai_epi32(a,31);                 // sign of a
    __m256i t11 = _mm256_sub_epi32(t10,sgn);               // sign of a - sign of d
    __m256i t12 = _mm256_sub_epi32(t9,t11);                // + 1 if a < 0, -1 if d < 0
    return        _mm256_xor_si256(t12,sgn);               // change sign if divisor negative
}

// vector of 8 32-bit unsigned integers
static inline Vec8ui operator / (Vec8ui const a, Divisor_ui const d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i t1  = _mm256_mul_epu32(a,m);                   // 32x32->64 bit unsigned multiplication of even elements of a
    __m256i t2  = _mm256_srli_epi64(t1,32);                // high dword of even numbered results
    __m256i t3  = _mm256_srli_epi64(a,32);                 // get odd elements of a into position for multiplication
    __m256i t4  = _mm256_mul_epu32(t3,m);                  // 32x32->64 bit unsigned multiplication of odd elements
    __m256i t7  = _mm256_blend_epi32(t2,t4,0xAA);
    __m256i t8  = _mm256_sub_epi32(a,t7);                  // subtract
    __m256i t9  = _mm256_srl_epi32(t8,d.gets1());          // shift right logical
    __m256i t10 = _mm256_add_epi32(t7,t9);                 // add
    return        _mm256_srl_epi32(t10,d.gets2());         // shift right logical 
}

// vector of 16 16-bit signed integers
static inline Vec16s operator / (Vec16s const a, Divisor_s const d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i sgn = _mm256_broadcastq_epi64(d.getsign());    // broadcast sign of d
    __m256i t1  = _mm256_mulhi_epi16(a, m);                // multiply high signed words
    __m256i t2  = _mm256_add_epi16(t1,a);                  // + a
    __m256i t3  = _mm256_sra_epi16(t2,d.gets1());          // shift right arithmetic
    __m256i t4  = _mm256_srai_epi16(a,15);                 // sign of a
    __m256i t5  = _mm256_sub_epi16(t4,sgn);                // sign of a - sign of d
    __m256i t6  = _mm256_sub_epi16(t3,t5);                 // + 1 if a < 0, -1 if d < 0
    return        _mm256_xor_si256(t6,sgn);                // change sign if divisor negative
}

// vector of 16 16-bit unsigned integers
static inline Vec16us operator / (Vec16us const a, Divisor_us const d) {
    __m256i m   = _mm256_broadcastq_epi64(d.getm());       // broadcast multiplier
    __m256i t1  = _mm256_mulhi_epu16(a, m);                // multiply high signed words
    __m256i t2  = _mm256_sub_epi16(a,t1);                  // subtract
    __m256i t3  = _mm256_srl_epi16(t2,d.gets1());          // shift right logical
    __m256i t4  = _mm256_add_epi16(t1,t3);                 // add
    return        _mm256_srl_epi16(t4,d.gets2());          // shift right logical 
}

// vector of 32 8-bit signed integers
static inline Vec32c operator / (Vec32c const a, Divisor_s const d) {
#if INSTRSET >= 10
    // sign-extend even-numbered and odd-numbered elements to 16 bits
    Vec16s  even = _mm256_srai_epi16(_mm256_slli_epi16(a, 8),8);
    Vec16s  odd  = _mm256_srai_epi16(a, 8);
    Vec16s  evend = even / d;         // divide even-numbered elements
    Vec16s  oddd  = odd  / d;         // divide odd-numbered  elements
            oddd  = _mm256_slli_epi16(oddd, 8); // shift left to put back in place
    __m256i res  = _mm256_mask_mov_epi8(evend, 0xAAAAAAAA, oddd); // interleave even and odd
    return res;
#else
    // expand into two Vec16s
    Vec16s low  = extend_low(a) / d;
    Vec16s high = extend_high(a) / d;
    return compress(low,high);
#endif
}


// vector of 32 8-bit unsigned integers
static inline Vec32uc operator / (Vec32uc const a, Divisor_us const d) {
    // zero-extend even-numbered and odd-numbered elements to 16 bits
#if INSTRSET >= 10
    Vec16us  even = _mm256_maskz_mov_epi8(__mmask32(0x55555555), a);
    Vec16us  odd  = _mm256_srli_epi16(a, 8);
    Vec16us  evend = even / d;          // divide even-numbered elements
    Vec16us  oddd  = odd  / d;          // divide odd-numbered  elements
    oddd  = _mm256_slli_epi16(oddd, 8); // shift left to put back in place
    __m256i res  = _mm256_mask_mov_epi8(evend, 0xAAAAAAAA, oddd); // interleave even and odd
    return res;
#else
    // expand into two Vec16s
    Vec16us low  = extend_low(a) / d;
    Vec16us high = extend_high(a) / d;
    return compress(low,high);
#endif
}

// vector operator /= : divide
static inline Vec8i & operator /= (Vec8i & a, Divisor_i const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec8ui & operator /= (Vec8ui & a, Divisor_ui const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec16s & operator /= (Vec16s & a, Divisor_s const d) {
    a = a / d;
    return a;
}


// vector operator /= : divide
static inline Vec16us & operator /= (Vec16us & a, Divisor_us const d) {
    a = a / d;
    return a;

}

// vector operator /= : divide
static inline Vec32c & operator /= (Vec32c & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec32uc & operator /= (Vec32uc & a, Divisor_us const d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec8i by compile-time constant
template <int32_t d>
static inline Vec8i divide_by_i(Vec8i const x) {
    static_assert(d != 0, "Integer division by zero");
    if constexpr (d ==  1) return  x;
    if constexpr (d == -1) return -x;
    if constexpr (uint32_t(d) == 0x80000000u) return Vec8i(x == Vec8i(0x80000000)) & 1; // prevent overflow when changing sign
    constexpr uint32_t d1 = d > 0 ? uint32_t(d) : uint32_t(-d);// compile-time abs(d). (force GCC compiler to treat d as 32 bits, not 64 bits)
    if constexpr ((d1 & (d1-1)) == 0) {
        // d1 is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(d1);
        __m256i sign;
        if constexpr (k > 1) sign = _mm256_srai_epi32(x, k-1); else sign = x;  // k copies of sign bit
        __m256i bias    = _mm256_srli_epi32(sign, 32-k);   // bias = x >= 0 ? 0 : k-1
        __m256i xpbias  = _mm256_add_epi32 (x, bias);      // x + bias
        __m256i q       = _mm256_srai_epi32(xpbias, k);    // (x + bias) >> k
        if (d > 0)      return q;                          // d > 0: return  q
        return _mm256_sub_epi32(_mm256_setzero_si256(), q);// d < 0: return -q
    }
    // general case
    constexpr int32_t sh = bit_scan_reverse_const(uint32_t(d1)-1);// ceil(log2(d1)) - 1. (d1 < 2 handled by power of 2 case)
    constexpr int32_t mult = int(1 + (uint64_t(1) << (32+sh)) / uint32_t(d1) - (int64_t(1) << 32));// multiplier
    const Divisor_i div(mult, sh, d < 0 ? -1 : 0);
    return x / div;
}

// define Vec8i a / const_int(d)
template <int32_t d>
static inline Vec8i operator / (Vec8i const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec8i a / const_uint(d)
template <uint32_t d>
static inline Vec8i operator / (Vec8i const a, Const_uint_t<d>) {
    static_assert(d < 0x80000000u, "Dividing signed integer by overflowing unsigned");
    return divide_by_i<int32_t(d)>(a);                     // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8i & operator /= (Vec8i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8i & operator /= (Vec8i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec8ui by compile-time constant
template <uint32_t d>
static inline Vec8ui divide_by_ui(Vec8ui const x) {
    static_assert(d != 0, "Integer division by zero");
    if constexpr (d == 1) return x;                        // divide by 1
    constexpr int b = bit_scan_reverse_const(d);           // floor(log2(d))
    if constexpr ((uint32_t(d) & (uint32_t(d)-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm256_srli_epi32(x, b);                   // x >> b
    }
    // general case (d > 2)
    constexpr uint32_t mult = uint32_t((uint64_t(1) << (b+32)) / d); // multiplier = 2^(32+b) / d
    constexpr uint64_t rem = (uint64_t(1) << (b+32)) - uint64_t(d)*mult; // remainder 2^(32+b) % d
    constexpr bool round_down = (2*rem < d);               // check if fraction is less than 0.5
    constexpr uint32_t mult1 = round_down ? mult : mult + 1;
    // do 32*32->64 bit unsigned multiplication and get high part of result
#if INSTRSET >= 10
    const __m256i multv = _mm256_maskz_set1_epi32(0x55, mult1);// zero-extend mult and broadcast
#else
    const __m256i multv = Vec4uq(uint64_t(mult1));         // zero-extend mult and broadcast
#endif
    __m256i t1 = _mm256_mul_epu32(x,multv);                // 32x32->64 bit unsigned multiplication of x[0] and x[2]
    if constexpr (round_down) {
        t1     = _mm256_add_epi64(t1,multv);               // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m256i t2 = _mm256_srli_epi64(t1,32);                 // high dword of result 0 and 2
    __m256i t3 = _mm256_srli_epi64(x,32);                  // get x[1] and x[3] into position for multiplication
    __m256i t4 = _mm256_mul_epu32(t3,multv);               // 32x32->64 bit unsigned multiplication of x[1] and x[3]
    if constexpr (round_down) {
        t4     = _mm256_add_epi64(t4,multv);               // compensate for rounding error. (x+1)*m replaced by x*m+m to avoid overflow
    }
    __m256i t7 = _mm256_blend_epi32(t2,t4,0xAA);
    Vec8ui  q  = _mm256_srli_epi32(t7, b);                 // shift right by b
    return  q;                                             // no overflow possible
}

// define Vec8ui a / const_uint(d)
template <uint32_t d>
static inline Vec8ui operator / (Vec8ui const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec8ui a / const_int(d)
template <int32_t d>
static inline Vec8ui operator / (Vec8ui const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec8ui & operator /= (Vec8ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec8ui & operator /= (Vec8ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16s by compile-time constant 
template <int d>
static inline Vec16s divide_by_i(Vec16s const x) {
    constexpr int16_t d0 = int16_t(d);                     // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");
    if constexpr (d0 ==  1) return  x;                     // divide by  1
    if constexpr (d0 == -1) return -x;                     // divide by -1
    if constexpr (uint16_t(d0) == 0x8000u) return Vec16s(x == Vec16s(0x8000)) & 1;// prevent overflow when changing sign
    constexpr uint16_t d1 = d0 > 0 ? d0 : -d0;             // compile-time abs(d0)
    if constexpr ((d1 & (d1-1)) == 0) {
        // d is a power of 2. use shift
        constexpr int k = bit_scan_reverse_const(uint32_t(d1));
        __m256i sign;
        if constexpr (k > 1) sign = _mm256_srai_epi16(x, k-1); else sign = x;// k copies of sign bit
        __m256i bias    = _mm256_srli_epi16(sign, 16-k);   // bias = x >= 0 ? 0 : k-1
        __m256i xpbias  = _mm256_add_epi16 (x, bias);      // x + bias
        __m256i q       = _mm256_srai_epi16(xpbias, k);    // (x + bias) >> k
        if constexpr (d0 > 0)  return q;                   // d0 > 0: return  q
        return _mm256_sub_epi16(_mm256_setzero_si256(), q);// d0 < 0: return -q
    }
    // general case
    constexpr int L = bit_scan_reverse_const(uint16_t(d1-1)) + 1;// ceil(log2(d)). (d < 2 handled above)
    constexpr int16_t mult = int16_t(1 + (1u << (15+L)) / uint32_t(d1) - 0x10000);// multiplier
    constexpr int shift1 = L - 1;
    const Divisor_s div(mult, shift1, d0 > 0 ? 0 : -1);
    return x / div;
}

// define Vec16s a / const_int(d)
template <int d>
static inline Vec16s operator / (Vec16s const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec16s a / const_uint(d)
template <uint32_t d>
static inline Vec16s operator / (Vec16s const a, Const_uint_t<d>) {
    static_assert(d < 0x8000u, "Dividing signed integer by overflowing unsigned");
    return divide_by_i<int(d)>(a);                         // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16s & operator /= (Vec16s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16s & operator /= (Vec16s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}


// Divide Vec16us by compile-time constant
template <uint32_t d>
static inline Vec16us divide_by_ui(Vec16us const x) {
    constexpr uint16_t d0 = uint16_t(d);                   // truncate d to 16 bits
    static_assert(d0 != 0, "Integer division by zero");
    if constexpr (d0 == 1) return x;                       // divide by 1
    constexpr int b = bit_scan_reverse_const((uint32_t)d0);// floor(log2(d))
    if constexpr ((d0 & (d0-1)) == 0) {
        // d is a power of 2. use shift
        return  _mm256_srli_epi16(x, b);                   // x >> b
    }
    // general case (d > 2)
    constexpr uint16_t mult = uint16_t((uint32_t(1) << (b+16)) / d0);// multiplier = 2^(32+b) / d
    constexpr uint32_t rem = (uint32_t(1) << (b+16)) - uint32_t(d0)*mult;// remainder 2^(32+b) % d
    constexpr bool round_down = (2*rem < d0);              // check if fraction is less than 0.5
    Vec16us x1 = x;
    if constexpr (round_down) {
        x1 = x1 + 1;                                       // round down mult and compensate by adding 1 to x
    }
    constexpr uint16_t mult1 = round_down ? mult : mult + 1;
    const __m256i multv = _mm256_set1_epi16((int16_t)mult1);// broadcast mult
    __m256i xm = _mm256_mulhi_epu16(x1, multv);            // high part of 16x16->32 bit unsigned multiplication
    Vec16us q    = _mm256_srli_epi16(xm, b);               // shift right by b
    if constexpr (round_down) {
        Vec16sb overfl = (x1 == Vec16us(_mm256_setzero_si256())); // check for overflow of x+1
        return select(overfl, Vec16us(uint16_t(mult1 >> (uint16_t)b)), q); // deal with overflow (rarely needed)
    }
    else {
        return q;                                          // no overflow possible
    }
}

// define Vec16us a / const_uint(d)
template <uint32_t d>
static inline Vec16us operator / (Vec16us const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec16us a / const_int(d)
template <int d>
static inline Vec16us operator / (Vec16us const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                             // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16us & operator /= (Vec16us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16us & operator /= (Vec16us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
} 

// define Vec32c a / const_int(d)
template <int d>
static inline Vec32c operator / (Vec32c const a, Const_int_t<d>) {
    // expand into two Vec16s
    Vec16s low  = extend_low(a)  / Const_int_t<d>();
    Vec16s high = extend_high(a) / Const_int_t<d>();
    return compress(low,high);
}

// define Vec32c a / const_uint(d)
template <uint32_t d>
static inline Vec32c operator / (Vec32c const a, Const_uint_t<d>) {
    static_assert(uint8_t(d) < 0x80u, "Dividing signed integer by overflowing unsigned");
    return a / Const_int_t<d>();                           // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32c & operator /= (Vec32c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec32c & operator /= (Vec32c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec32uc a / const_uint(d)
template <uint32_t d>
static inline Vec32uc operator / (Vec32uc const a, Const_uint_t<d>) {
    // expand into two Vec16us
    Vec16us low  = extend_low(a)  / Const_uint_t<d>();
    Vec16us high = extend_high(a) / Const_uint_t<d>();
    return compress(low,high);
}

// define Vec32uc a / const_int(d)
template <int d>
static inline Vec32uc operator / (Vec32uc const a, Const_int_t<d>) {
    static_assert(int8_t(d) >= 0, "Dividing unsigned integer by negative is ambiguous");
    return a / Const_uint_t<d>();                          // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32uc & operator /= (Vec32uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32uc & operator /= (Vec32uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

#if INSTRSET >= 10  // compact boolean vectors, other sizes

// to_bits: convert boolean vector to integer bitfield
static inline uint32_t to_bits(Vec32b const x) {
    return __mmask32(x);
}

#else

// to_bits: convert boolean vector to integer bitfield
static inline uint32_t to_bits(Vec32cb const x) {
    return (uint32_t)_mm256_movemask_epi8(x);
}

static inline uint16_t to_bits(Vec16sb const x) {
    __m128i a = _mm_packs_epi16(x.get_low(), x.get_high());  // 16-bit words to bytes
    return (uint16_t)_mm_movemask_epi8(a);
} 

static inline uint8_t to_bits(Vec8ib const x) {
    __m128i a = _mm_packs_epi32(x.get_low(), x.get_high());  // 32-bit dwords to 16-bit words
    __m128i b = _mm_packs_epi16(a, a);  // 16-bit words to bytes
    return (uint8_t)_mm_movemask_epi8(b);
}

static inline uint8_t to_bits(Vec4qb const x) {
    uint32_t a = (uint32_t)_mm256_movemask_epi8(x);
    return ((a & 1) | ((a >> 7) & 2)) | (((a >> 14) & 4) | ((a >> 21) & 8));
}

#endif  

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI256_H
