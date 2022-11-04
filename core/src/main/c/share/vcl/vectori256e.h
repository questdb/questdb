/****************************  vectori256e.h   *******************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 256-bit integer point vector classes as interface
* to intrinsic functions. Emulated for processors without AVX2 instruction set.
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
* Each vector object is represented internally in the CPU as two 128-bit registers.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORI256E_H
#define VECTORI256E_H 1

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#if defined (VECTORI256_H)
#error Two different versions of vectori256.h included
#endif 


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Vector of 256 bits. used as base class
*
*****************************************************************************/

class Vec256b {
protected:
    __m128i y0;                        // low half
    __m128i y1;                        // high half
public:
    // Default constructor:
    Vec256b() {
    }
    Vec256b(__m128i x0, __m128i x1) {  // constructor to build from two __m128i
        y0 = x0;  y1 = x1;
    }
    // Constructor to build from two Vec128b:
    Vec256b(Vec128b const a0, Vec128b const a1) {
        y0 = a0;  y1 = a1;
    }
    // Member function to load from array (unaligned)
    Vec256b & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    // You may use load_a instead of load if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    Vec256b & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        _mm_storeu_si128((__m128i*)p,     y0);
        _mm_storeu_si128((__m128i*)p + 1, y1);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(void * p) const {
        _mm_stream_si128((__m128i*)p,     y0);
        _mm_stream_si128((__m128i*)p + 1, y1);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return 16;
    }
    // Member function to store into array, aligned by 32
    // You may use store_a instead of store if you are certain that p points to an address
    // divisible by 32, but there is hardly any speed advantage of load_a on modern processors
    void store_a(void * p) const {
        _mm_store_si128((__m128i*)p,     y0);
        _mm_store_si128((__m128i*)p + 1, y1);
    }
    // Member functions to split into two Vec128b:
    Vec128b get_low() const {
        return y0;
    }
    Vec128b get_high() const {
        return y1;
    }
    static constexpr int size() {
        return 256;
    }
    static constexpr int elementtype() {
        return 1;
    }
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec256b operator & (Vec256b const a, Vec256b const b) {
    return Vec256b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec256b operator && (Vec256b const a, Vec256b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec256b operator | (Vec256b const a, Vec256b const b) {
    return Vec256b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec256b operator || (Vec256b const a, Vec256b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec256b operator ^ (Vec256b const a, Vec256b const b) {
    return Vec256b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec256b operator ~ (Vec256b const a) {
    return Vec256b(~a.get_low(), ~a.get_high());
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

/*****************************************************************************
*
*          Functions for this class
*
*****************************************************************************/

// function andnot: a & ~ b
static inline Vec256b andnot (Vec256b const a, Vec256b const b) {
    return Vec256b(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}

// Select between two sources, byte by byte. Used in various functions and operators
// Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or 0xFF (true). No other values are allowed.
// Only bit 7 in each byte of s is checked, 
static inline Vec256b selectb (Vec256b const s, Vec256b const a, Vec256b const b) {
    return Vec256b(selectb(s.get_low(),  a.get_low(),  b.get_low()), 
                   selectb(s.get_high(), a.get_high(), b.get_high()));
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec256b const a) {
    return horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec256b const a) {
    return horizontal_or(a.get_low() | a.get_high());
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
        y1 = y0 = _mm_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec32c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31) {
        y0 = _mm_setr_epi8(i0,  i1,  i2,  i3,  i4,  i5,  i6,  i7,  i8,  i9,  i10, i11, i12, i13, i14, i15);
        y1 = _mm_setr_epi8(i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
    }
    // Constructor to build from two Vec16c:
    Vec32c(Vec16c const a0, Vec16c const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b 
    Vec32c(Vec256b const & x) {
        y0 = x.get_low();
        y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec32c & operator = (Vec256b const x) {
        y0 = x.get_low();
        y1 = x.get_high();
        return *this;
    } 
    // Member function to load from array (unaligned)
    Vec32c & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32c & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec32c & load_partial(int n, void const * p) {
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
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
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
    }
    // cut off vector to n elements. The last 32-n elements are set to zero
    Vec32c & cutoff(int n) {
        if (uint32_t(n) >= 32) return *this;
        static const union {
            int32_t i[16];
            char    c[64];
        } mask = {{-1,-1,-1,-1,-1,-1,-1,-1,0,0,0,0,0,0,0,0}};
        *this &= Vec32c().load(mask.c+32-n);
        return *this;
    }
    // Member function to change a single element in vector
    Vec32c const insert(int index, int8_t value) {
        if ((uint32_t)index < 16) {
            y0 = Vec16c(y0).insert(index, value);
        }
        else {
            y1 = Vec16c(y1).insert(index-16, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(int index) const {
        if ((uint32_t)index < 16) {
            return Vec16c(y0).extract(index);
        }
        else {
            return Vec16c(y1).extract(index-16);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16c:
    Vec16c get_low() const {
        return y0;
    }
    Vec16c get_high() const {
        return y1;
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

class Vec32cb : public Vec32c {
public:
    // Default constructor:
    Vec32cb() {}

    // Constructor to convert from type Vec256b  
    Vec32cb(Vec256b const x) {
        y0 = x.get_low();
        y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec32cb & operator = (Vec256b const x) {
        y0 = x.get_low();
        y1 = x.get_high();
        return *this;
    } 
    // Constructor to broadcast scalar value:
    Vec32cb(bool b) : Vec32c(-int8_t(b)) {
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
        return y0;
    }
    Vec16cb get_high() const {
        return y1;
    }
    Vec32cb & insert (int index, bool a) {
        Vec32c::insert(index, -(int)a);
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
        y0 = Vec16cb().load_bits(uint16_t(a));
        y1 = Vec16cb().load_bits(uint16_t(a>>16));
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec32cb(int b) = delete;
    Vec32cb & operator = (int x) = delete;
};


/*****************************************************************************
*
*          Define operators for Vec32cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec32cb operator & (Vec32cb const a, Vec32cb const b) {
    return Vec32cb(Vec256b(a) & Vec256b(b));
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
    return Vec32cb(Vec256b(a) | Vec256b(b));
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
    return Vec32cb(Vec256b(a) ^ Vec256b(b));
}
// vector operator ^= : bitwise xor
static inline Vec32cb & operator ^= (Vec32cb & a, Vec32cb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec32cb operator == (Vec32cb const a, Vec32cb const b) {
    return Vec32cb(Vec256b(a) ^ Vec256b(~b));
}

// vector operator != : xor
static inline Vec32cb operator != (Vec32cb const a, Vec32cb const b) {
    return Vec32cb(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec32cb operator ~ (Vec32cb const a) {
    return Vec32cb( ~ Vec256b(a));
}

// vector operator ! : element not
static inline Vec32cb operator ! (Vec32cb const a) {
    return ~ a;
}

// vector function andnot
static inline Vec32cb andnot (Vec32cb const a, Vec32cb const b) {
    return Vec32cb(andnot(Vec256b(a), Vec256b(b)));
}


/*****************************************************************************
*
*          Operators for Vec32c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec32c operator + (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec32c(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec32c operator - (Vec32c const a) {
    return Vec32c(-a.get_low(), -a.get_high());
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
    return Vec32c(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec32c & operator *= (Vec32c & a, Vec32c const b) {
    a = a * b;
    return a;
}

// vector of 32 8-bit signed integers
static inline Vec32c operator / (Vec32c const a, Divisor_s const d) {
    return Vec32c(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec32c & operator /= (Vec32c & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32c operator << (Vec32c const a, int b) {
    return Vec32c(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec32c & operator <<= (Vec32c & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic all elements
static inline Vec32c operator >> (Vec32c const a, int b) {
    return Vec32c(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec32c & operator >>= (Vec32c & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32cb operator == (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec32cb operator != (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator > : returns true for elements for which a > b (signed)
static inline Vec32cb operator > (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (signed)
static inline Vec32cb operator < (Vec32c const a, Vec32c const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32cb operator >= (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32cb operator <= (Vec32c const a, Vec32c const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec32c operator & (Vec32c const a, Vec32c const b) {
    return Vec32c(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec32c(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec32c(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec32c & operator ^= (Vec32c & a, Vec32c const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32c operator ~ (Vec32c const a) {
    return Vec32c(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec32cb operator ! (Vec32c const a) {
    return Vec32c(!a.get_low(), !a.get_high());
}

// Functions for this class

// Select between two operands using broad boolean vectors. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
static inline Vec32c select (Vec32cb const s, Vec32c const a, Vec32c const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32c if_add (Vec32cb const f, Vec32c const a, Vec32c const b) {
    return a + (Vec32c(f) & b);
}

// Conditional subtract
static inline Vec32c if_sub (Vec32cb const f, Vec32c const a, Vec32c const b) {
    return a - (Vec32c(f) & b);
}

// Conditional multiply
static inline Vec32c if_mul (Vec32cb const f, Vec32c const a, Vec32c const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint8_t horizontal_add (Vec32c const a) {
    return (uint8_t)horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32c const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec32c add_saturated(Vec32c const a, Vec32c const b) {
    return Vec32c(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32c sub_saturated(Vec32c const a, Vec32c const b) {
    return Vec32c(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec32c max(Vec32c const a, Vec32c const b) {
    return Vec32c(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec32c min(Vec32c const a, Vec32c const b) {
    return Vec32c(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec32c abs(Vec32c const a) {
    return Vec32c(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32c abs_saturated(Vec32c const a) {
    return Vec32c(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32c rotate_left(Vec32c const a, int b) {
    return Vec32c(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = _mm_set1_epi8((char)i);
    }
    // Constructor to build from all elements:
    Vec32uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31) {
        y0 = _mm_setr_epi8((int8_t)i0,  (int8_t)i1,  (int8_t)i2,  (int8_t)i3,  (int8_t)i4,  (int8_t)i5,  (int8_t)i6,  (int8_t)i7,  (int8_t)i8,  (int8_t)i9,  (int8_t)i10, (int8_t)i11, (int8_t)i12, (int8_t)i13, (int8_t)i14, (int8_t)i15);
        y1 = _mm_setr_epi8((int8_t)i16, (int8_t)i17, (int8_t)i18, (int8_t)i19, (int8_t)i20, (int8_t)i21, (int8_t)i22, (int8_t)i23, (int8_t)i24, (int8_t)i25, (int8_t)i26, (int8_t)i27, (int8_t)i28, (int8_t)i29, (int8_t)i30, (int8_t)i31);
    }
    // Constructor to build from two Vec16uc:
    Vec32uc(Vec16uc const a0, Vec16uc const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec32uc(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec32uc & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec32uc & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec32uc & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec16uc get_high() const {
        return y1;
    }
    static constexpr int elementtype() {
        return 5;
    } 
};

// Define operators for this class

// vector operator + : add
static inline Vec32uc operator + (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() + b.get_low(), a.get_high() + b.get_high()); 
}

// vector operator - : subtract
static inline Vec32uc operator - (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() - b.get_low(), a.get_high() - b.get_high()); 
}

// vector operator * : multiply
static inline Vec32uc operator * (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() * b.get_low(), a.get_high() * b.get_high()); 
}

// vector operator / : divide
static inline Vec32uc operator / (Vec32uc const a, Divisor_us const d) {
    return Vec32uc(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec32uc & operator /= (Vec32uc & a, Divisor_us const d) {
    a = a / d;
    return a;
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const a, uint32_t b) {
    return Vec32uc(a.get_low() << b, a.get_high() << b); 
}

// vector operator << : shift left all elements
static inline Vec32uc operator << (Vec32uc const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >> : shift right logical all elements
static inline Vec32uc operator >> (Vec32uc const a, uint32_t b) {
    return Vec32uc(a.get_low() >> b, a.get_high() >> b); 
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
    return Vec32c(a.get_low() >= b.get_low(), a.get_high() >= b.get_high()); 
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32cb operator <= (Vec32uc const a, Vec32uc const b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32cb operator > (Vec32uc const a, Vec32uc const b) {
    return Vec32c(a.get_low() > b.get_low(), a.get_high() > b.get_high()); 
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32cb operator < (Vec32uc const a, Vec32uc const b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec32uc operator & (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec32uc operator && (Vec32uc const a, Vec32uc const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec32uc operator | (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec32uc operator || (Vec32uc const a, Vec32uc const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec32uc operator ^ (Vec32uc const a, Vec32uc const b) {
    return Vec32uc(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec32uc operator ~ (Vec32uc const a) {
    return Vec32uc(~a.get_low(), ~a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 32; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec32uc select (Vec32cb const s, Vec32uc const a, Vec32uc const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32uc if_add (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
    return a + (Vec32uc(f) & b);
}

// Conditional subtract
static inline Vec32uc if_sub (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
    return a - (Vec32uc(f) & b);
}

// Conditional multiply
static inline Vec32uc if_mul (Vec32cb const f, Vec32uc const a, Vec32uc const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
// (Note: horizontal_add_x(Vec32uc) is slightly faster)
static inline uint32_t horizontal_add (Vec32uc const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec32uc const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32uc add_saturated(Vec32uc const a, Vec32uc const b) {
    return Vec32uc(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high())); 
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32uc sub_saturated(Vec32uc const a, Vec32uc const b) {
    return Vec32uc(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high())); 
}

// function max: a > b ? a : b
static inline Vec32uc max(Vec32uc const a, Vec32uc const b) {
    return Vec32uc(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high())); 
}

// function min: a < b ? a : b
static inline Vec32uc min(Vec32uc const a, Vec32uc const b) {
    return Vec32uc(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high())); 
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
        y1 = y0 = _mm_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec16s(int16_t i0, int16_t i1, int16_t i2,  int16_t i3,  int16_t i4,  int16_t i5,  int16_t i6,  int16_t i7,
           int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15) {
        y0 = _mm_setr_epi16(i0, i1, i2,  i3,  i4,  i5,  i6,  i7);
        y1 = _mm_setr_epi16(i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to build from two Vec8s:
    Vec16s(Vec8s const a0, Vec8s const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec16s(Vec256b const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec16s & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16s & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16s & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16s & load_partial(int n, void const * p) {
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
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
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
    }
    // cut off vector to n elements. The last 16-n elements are set to zero
    Vec16s & cutoff(int n) {
        *this = Vec16s(Vec32c(*this).cutoff(n * 2));
        return *this;
    }
    // Member function to change a single element in vector
    Vec16s const insert(int index, int16_t value) {
        if ((uint32_t)index < 8) {
            y0 = Vec8s(y0).insert(index, value);
        }
        else {
            y1 = Vec8s(y1).insert(index-8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int16_t extract(int index) const {
        if ((uint32_t)index < 8) {
            return Vec8s(y0).extract(index);
        }
        else {
            return Vec8s(y1).extract(index-8);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8s:
    Vec8s get_low() const {
        return y0;
    }
    Vec8s get_high() const {
        return y1;
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

class Vec16sb : public Vec16s {
public:
    // Default constructor:
    Vec16sb() {
    }
    // Constructor to build from all elements:
    Vec16sb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) :
        Vec16s(-int16_t(x0), -int16_t(x1), -int16_t(x2), -int16_t(x3), -int16_t(x4), -int16_t(x5), -int16_t(x6), -int16_t(x7), 
            -int16_t(x8), -int16_t(x9), -int16_t(x10), -int16_t(x11), -int16_t(x12), -int16_t(x13), -int16_t(x14), -int16_t(x15))
        {}
    // Constructor to convert from type Vec256b
    Vec16sb(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec16sb & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16sb(bool b) : Vec16s(-int16_t(b)) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16sb & operator = (bool b) {
        *this = Vec16sb(b);
        return *this;
    }
    // Constructor to build from two Vec8sb:
    Vec16sb(Vec8sb const a0, Vec8sb const a1) : Vec16s(Vec8s(a0), Vec8s(a1)) {
    }
    // Member functions to split into two Vec8s:
    Vec8sb get_low() const {
        return y0;
    }
    Vec8sb get_high() const {
        return y1;
    }
    Vec16sb & insert (int index, bool a) {
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
        y0 = Vec8sb().load_bits(uint8_t(a));
        y1 = Vec8sb().load_bits(uint8_t(a>>8));
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec16sb(int b) = delete;
    Vec16sb & operator = (int x) = delete;
};


/*****************************************************************************
*
*          Define operators for Vec16sb
*
*****************************************************************************/

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
    return Vec16sb(Vec256b(a) ^ Vec256b(~b));
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


/*****************************************************************************
*
*          Operators for Vec16s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec16s operator + (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec16s(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec16s operator - (Vec16s const a) {
    return Vec16s(-a.get_low(), -a.get_high());
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
    return Vec16s(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec16s & operator *= (Vec16s & a, Vec16s const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16s operator / (Vec16s const a, Divisor_s const d) {
    return Vec16s(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16s & operator /= (Vec16s & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator << : shift left
static inline Vec16s operator << (Vec16s const a, int b) {
    return Vec16s(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec16s & operator <<= (Vec16s & a, int b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16s operator >> (Vec16s const a, int b) {
    return Vec16s(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec16s & operator >>= (Vec16s & a, int b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16sb operator == (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16sb operator != (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec16sb operator > (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16sb operator < (Vec16s const a, Vec16s const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16sb operator >= (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16sb operator <= (Vec16s const a, Vec16s const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16s operator & (Vec16s const a, Vec16s const b) {
    return Vec16s(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec16s(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec16s(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec16s & operator ^= (Vec16s & a, Vec16s const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16s operator ~ (Vec16s const a) {
    return Vec16s(~Vec256b(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec16s select (Vec16sb const s, Vec16s const a, Vec16s const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16s if_add (Vec16sb const f, Vec16s const a, Vec16s const b) {
    return a + (Vec16s(f) & b);
}

// Conditional subtract
static inline Vec16s if_sub (Vec16sb const f, Vec16s const a, Vec16s const b) {
    return a - (Vec16s(f) & b);
}

// Conditional multiply
static inline Vec16s if_mul (Vec16sb const f, Vec16s const a, Vec16s const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int16_t horizontal_add (Vec16s const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int32_t horizontal_add_x (Vec16s const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16s add_saturated(Vec16s const a, Vec16s const b) {
    return Vec16s(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16s sub_saturated(Vec16s const a, Vec16s const b) {
    return Vec16s(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16s max(Vec16s const a, Vec16s const b) {
    return Vec16s(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16s min(Vec16s const a, Vec16s const b) {
    return Vec16s(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec16s abs(Vec16s const a) {
    return Vec16s(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16s abs_saturated(Vec16s const a) {
    return Vec16s(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16s rotate_left(Vec16s const a, int b) {
    return Vec16s(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = _mm_set1_epi16((int16_t)i);
    }
    // Constructor to build from all elements:
    Vec16us(uint16_t i0, uint16_t i1, uint16_t i2,  uint16_t i3,  uint16_t i4,  uint16_t i5,  uint16_t i6,  uint16_t i7,
            uint16_t i8, uint16_t i9, uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15) {
        y0 = _mm_setr_epi16((int16_t)i0, (int16_t)i1, (int16_t)i2,  (int16_t)i3,  (int16_t)i4,  (int16_t)i5,  (int16_t)i6,  (int16_t)i7);
        y1 = _mm_setr_epi16((int16_t)i8, (int16_t)i9, (int16_t)i10, (int16_t)i11, (int16_t)i12, (int16_t)i13, (int16_t)i14, (int16_t)i15);
    }
    // Constructor to build from two Vec8us:
    Vec16us(Vec8us const a0, Vec8us const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec16us(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec16us & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16us & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec16us & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec8us get_high() const {
        return y1;
    }
    static constexpr int elementtype() {
        return 7;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16us operator + (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator - : subtract
static inline Vec16us operator - (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator * : multiply
static inline Vec16us operator * (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator / : divide
static inline Vec16us operator / (Vec16us const a, Divisor_us const d) {
    return Vec16us(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16us & operator /= (Vec16us & a, Divisor_us const d) {
    a = a / d;
    return a;
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const a, uint32_t b) {
    return Vec16us(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >> : shift right logical all elements
static inline Vec16us operator >> (Vec16us const a, int b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right arithmetic
static inline Vec16us & operator >>= (Vec16us & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const a, uint32_t b) {
    return Vec16us(a.get_low() << b, a.get_high() << b);
}

// vector operator << : shift left all elements
static inline Vec16us operator << (Vec16us const a, int32_t b) {
    return a << (uint32_t)b;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16sb operator >= (Vec16us const a, Vec16us const b) {
    return Vec16s(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16sb operator <= (Vec16us const a, Vec16us const b) {
    return b >= a;
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16sb operator > (Vec16us const a, Vec16us const b) {
    return Vec16s(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16sb operator < (Vec16us const a, Vec16us const b) {
    return b > a;
}

// vector operator & : bitwise and
static inline Vec16us operator & (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16us operator && (Vec16us const a, Vec16us const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16us operator | (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16us operator || (Vec16us const a, Vec16us const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16us operator ^ (Vec16us const a, Vec16us const b) {
    return Vec16us(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec16us operator ~ (Vec16us const a) {
    return Vec16us(~ Vec256b(a));
}


// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec16us select (Vec16sb const s, Vec16us const a, Vec16us const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16us if_add (Vec16sb const f, Vec16us const a, Vec16us const b) {
    return a + (Vec16us(f) & b);
}

// Conditional subtract
static inline Vec16us if_sub (Vec16sb const f, Vec16us const a, Vec16us const b) {
    return a - (Vec16us(f) & b);
}

// Conditional multiply
static inline Vec16us if_mul (Vec16sb const f, Vec16us const a, Vec16us const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add (Vec16us const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is zero-extended before addition to avoid overflow
static inline uint32_t horizontal_add_x (Vec16us const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16us add_saturated(Vec16us const a, Vec16us const b) {
    return Vec16us(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16us sub_saturated(Vec16us const a, Vec16us const b) {
    return Vec16us(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16us max(Vec16us const a, Vec16us const b) {
    return Vec16us(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16us min(Vec16us const a, Vec16us const b) {
    return Vec16us(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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
        y1 = y0 = _mm_set1_epi32(i);
    }
    // Constructor to build from all elements:
    Vec8i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7) {
        y0 = _mm_setr_epi32(i0, i1, i2, i3);
        y1 = _mm_setr_epi32(i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4i:
    Vec8i(Vec4i const a0, Vec4i const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec8i(Vec256b const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec8i & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8i & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8i & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8i & load_partial(int n, void const * p) {
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
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
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
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8i & cutoff(int n) {
        *this = Vec32c(*this).cutoff(n * 4);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8i const insert(int index, int32_t value) {
        if ((uint32_t)index < 4) {
            y0 = Vec4i(y0).insert(index, value);
        }
        else {
            y1 = Vec4i(y1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4i(y0).extract(index);
        }
        else {
            return Vec4i(y1).extract(index-4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4i:
    Vec4i get_low() const {
        return y0;
    }
    Vec4i get_high() const {
        return y1;
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

class Vec8ib : public Vec8i {
public:
    // Default constructor:
    Vec8ib() {
    }
    // Constructor to build from all elements:
    Vec8ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) :
        Vec8i(-int32_t(x0), -int32_t(x1), -int32_t(x2), -int32_t(x3), -int32_t(x4), -int32_t(x5), -int32_t(x6), -int32_t(x7))
        {}
    // Constructor to convert from type Vec256b
    Vec8ib(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec8ib & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
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
    // Member functions to split into two Vec4i:
    Vec4ib get_low() const {
        return y0;
    }
    Vec4ib get_high() const {
        return y1;
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
        y0 = Vec4ib().load_bits(uint16_t(a));
        y1 = Vec4ib().load_bits(uint16_t(a>>4));
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec8ib(int b) = delete;
    Vec8ib & operator = (int x) = delete;
};

/*****************************************************************************
*
*          Define operators for Vec8ib
*
*****************************************************************************/

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
    return Vec8ib(Vec256b(a) ^ Vec256b(~b));
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

/*****************************************************************************
*
*          Operators for Vec8i
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec8i operator + (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec8i(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec8i operator - (Vec8i const a) {
    return Vec8i(-a.get_low(), -a.get_high());
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
    return Vec8i(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec8i & operator *= (Vec8i & a, Vec8i const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec8i operator / (Vec8i const a, Divisor_i const d) {
    return Vec8i(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec8i & operator /= (Vec8i & a, Divisor_i const d) {
    a = a / d;
    return a;
}

// vector operator << : shift left
static inline Vec8i operator << (Vec8i const a, int32_t b) {
    return Vec8i(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec8i & operator <<= (Vec8i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8i operator >> (Vec8i const a, int32_t b) {
    return Vec8i(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec8i & operator >>= (Vec8i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8ib operator == (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8ib operator != (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec8ib operator > (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec8ib operator < (Vec8i const a, Vec8i const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8ib operator >= (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8ib operator <= (Vec8i const a, Vec8i const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8i operator & (Vec8i const a, Vec8i const b) {
    return Vec8i(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec8i(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec8i(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec8i & operator ^= (Vec8i & a, Vec8i const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8i operator ~ (Vec8i const a) {
    return Vec8i(~a.get_low(), ~a.get_high());
}

// vector operator ! : returns true for elements == 0
static inline Vec8ib operator ! (Vec8i const a) {
    return Vec8i(!a.get_low(), !a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 8; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8i select (Vec8ib const s, Vec8i const a, Vec8i const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8i if_add (Vec8ib const f, Vec8i const a, Vec8i const b) {
    return a + (Vec8i(f) & b);
}

// Conditional subtract
static inline Vec8i if_sub (Vec8ib const f, Vec8i const a, Vec8i const b) {
    return a - (Vec8i(f) & b);
}

// Conditional multiply
static inline Vec8i if_mul (Vec8ib const f, Vec8i const a, Vec8i const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add (Vec8i const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec8i const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec8i add_saturated(Vec8i const a, Vec8i const b) {
    return Vec8i(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec8i sub_saturated(Vec8i const a, Vec8i const b) {
    return Vec8i(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec8i max(Vec8i const a, Vec8i const b) {
    return Vec8i(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8i min(Vec8i const a, Vec8i const b) {
    return Vec8i(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec8i abs(Vec8i const a) {
    return Vec8i(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8i abs_saturated(Vec8i const a) {
    return Vec8i(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8i rotate_left(Vec8i const a, int b) {
    return Vec8i(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
}


/*****************************************************************************
*
*          Vector of 4 32-bit unsigned integers
*
*****************************************************************************/

class Vec8ui : public Vec8i {
public:
    // Default constructor:
    Vec8ui() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8ui(uint32_t i) {
        y1 = y0 = _mm_set1_epi32(int32_t(i));
    }
    // Constructor to build from all elements:
    Vec8ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7) {
        y0 = _mm_setr_epi32((int32_t)i0, (int32_t)i1, (int32_t)i2, (int32_t)i3);
        y1 = _mm_setr_epi32((int32_t)i4, (int32_t)i5, (int32_t)i6, (int32_t)i7);
    }
    // Constructor to build from two Vec4ui:
    Vec8ui(Vec4ui const a0, Vec4ui const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec8ui(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec8ui & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8ui & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8ui & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec4ui get_high() const {
        return y1;
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

// vector operator / : divide all elements by same integer
static inline Vec8ui operator / (Vec8ui const a, Divisor_ui const d) {
    return Vec8ui(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec8ui & operator /= (Vec8ui & a, Divisor_ui const d) {
    a = a / d;
    return a;
}

// vector operator >> : shift right logical all elements
static inline Vec8ui operator >> (Vec8ui const a, uint32_t b) {
    return Vec8ui(a.get_low() >> b, a.get_high() >> b);
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

// vector operator >>= : shift right logical
static inline Vec8ui & operator >>= (Vec8ui & a, int32_t b) {
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
    return Vec8i(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8ib operator < (Vec8ui const a, Vec8ui const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8ib operator >= (Vec8ui const a, Vec8ui const b) {
    return Vec8i(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8ib operator <= (Vec8ui const a, Vec8ui const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8ui operator & (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec8ui operator && (Vec8ui const a, Vec8ui const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8ui operator | (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec8ui operator || (Vec8ui const a, Vec8ui const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8ui operator ^ (Vec8ui const a, Vec8ui const b) {
    return Vec8ui(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec8ui operator ~ (Vec8ui const a) {
    return Vec8ui(~a.get_low(), ~a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec8ui select (Vec8ib const s, Vec8ui const a, Vec8ui const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8ui if_add (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
    return a + (Vec8ui(f) & b);
}

// Conditional subtract
static inline Vec8ui if_sub (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
    return a - (Vec8ui(f) & b);
}

// Conditional multiply
static inline Vec8ui if_mul (Vec8ib const f, Vec8ui const a, Vec8ui const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add (Vec8ui const a) {
    return (uint32_t)horizontal_add((Vec8i)a);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec8ui const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec8ui add_saturated(Vec8ui const a, Vec8ui const b) {
    return Vec8ui(add_saturated(a.get_low(),b.get_low()), add_saturated(a.get_high(),b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec8ui sub_saturated(Vec8ui const a, Vec8ui const b) {
    return Vec8ui(sub_saturated(a.get_low(),b.get_low()), sub_saturated(a.get_high(),b.get_high()));
}

// function max: a > b ? a : b
static inline Vec8ui max(Vec8ui const a, Vec8ui const b) {
    return Vec8ui(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8ui min(Vec8ui const a, Vec8ui const b) {
    return Vec8ui(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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
        y0 = y1 = Vec2q(i);
    }
    // Constructor to build from all elements:
    Vec4q(int64_t i0, int64_t i1, int64_t i2, int64_t i3) {
        y0 = Vec2q(i0,i1);
        y1 = Vec2q(i2,i3);
    }
    // Constructor to build from two Vec2q:
    Vec4q(Vec2q const a0, Vec2q const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec4q(Vec256b const & x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec4q & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4q & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4q & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec4q & load_partial(int n, void const * p) {
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
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
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
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec4q & cutoff(int n) {
        *this = Vec32c(*this).cutoff(n * 8);
        return *this;
    }
    // Member function to change a single element in vector
    Vec4q const insert(int index, int64_t value) {
        if ((uint32_t)index < 2) {
            y0 = Vec2q(y0).insert(index, value);
        }
        else {
            y1 = Vec2q(y1).insert(index-2, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(int index) const {
        if ((uint32_t)index < 2) {
            return Vec2q(y0).extract(index);
        }
        else {
            return Vec2q(y1).extract(index-2);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec2q get_low() const {
        return y0;
    }
    Vec2q get_high() const {
        return y1;
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

class Vec4qb : public Vec4q {
public:
    // Default constructor:
    Vec4qb() {
    }
    // Constructor to build from all elements:
    Vec4qb(bool x0, bool x1, bool x2, bool x3) :
        Vec4q(-int64_t(x0), -int64_t(x1), -int64_t(x2), -int64_t(x3)) {
    }
    // Constructor to convert from type Vec256b
    Vec4qb(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec4qb & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
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
        return y0;
    }
    Vec2qb get_high() const {
        return y1;
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
        y0 = Vec2qb().load_bits(a);
        y1 = Vec2qb().load_bits(uint8_t(a>>2u));
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc.
    Vec4qb(int b) = delete;
    Vec4qb & operator = (int x) = delete;
};


/*****************************************************************************
*
*          Define operators for Vec4qb
*
*****************************************************************************/

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
    return Vec4qb(Vec256b(a) ^ Vec256b(~b));
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


/*****************************************************************************
*
*          Operators for Vec4q
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec4q operator + (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() + b.get_low(), a.get_high() + b.get_high());
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
    return Vec4q(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec4q operator - (Vec4q const a) {
    return Vec4q(-a.get_low(), -a.get_high());
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
    return Vec4q(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec4q & operator *= (Vec4q & a, Vec4q const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec4q operator << (Vec4q const a, int32_t b) {
    return Vec4q(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec4q & operator <<= (Vec4q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec4q operator >> (Vec4q const a, int32_t b) {
    return Vec4q(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec4q & operator >>= (Vec4q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec4qb operator == (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec4qb operator != (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec4qb operator < (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec4qb operator > (Vec4q const a, Vec4q const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec4qb operator >= (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec4qb operator <= (Vec4q const a, Vec4q const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4q operator & (Vec4q const a, Vec4q const b) {
    return Vec4q(a.get_low() & b.get_low(), a.get_high() & b.get_high());
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
    return Vec4q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
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
    return Vec4q(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec4q & operator ^= (Vec4q & a, Vec4q const b) {
    a = a ^ b;
    return a;
} 

// vector operator ~ : bitwise not
static inline Vec4q operator ~ (Vec4q const a) {
    return Vec4q(~a.get_low(), ~a.get_high());
}

// vector operator ! : logical not, returns true for elements == 0
static inline Vec4qb operator ! (Vec4q const a) {
    return Vec4q(!a.get_low(), !a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each byte in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4q select (Vec4qb const s, Vec4q const a, Vec4q const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4q if_add (Vec4qb const f, Vec4q const a, Vec4q const b) {
    return a + (Vec4q(f) & b);
}

// Conditional subtract
static inline Vec4q if_sub (Vec4qb const f, Vec4q const a, Vec4q const b) {
    return a - (Vec4q(f) & b);
}

// Conditional multiply
static inline Vec4q if_mul (Vec4qb const f, Vec4q const a, Vec4q const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int64_t horizontal_add (Vec4q const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function max: a > b ? a : b
static inline Vec4q max(Vec4q const a, Vec4q const b) {
    return Vec4q(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4q min(Vec4q const a, Vec4q const b) {
    return Vec4q(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec4q abs(Vec4q const a) {
    return Vec4q(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec4q abs_saturated(Vec4q const a) {
    return Vec4q(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec4q rotate_left(Vec4q const a, int b) {
    return Vec4q(rotate_left(a.get_low(),b), rotate_left(a.get_high(),b));
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
        y1 = y0 = Vec2q((int64_t)i);
    }
    // Constructor to build from all elements:
    Vec4uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3) {
        y0 = Vec2q((int64_t)i0, (int64_t)i1);
        y1 = Vec2q((int64_t)i2, (int64_t)i3);
    }
    // Constructor to build from two Vec2uq:
    Vec4uq(Vec2uq const a0, Vec2uq const a1) {
        y0 = a0;  y1 = a1;
    }
    // Constructor to convert from type Vec256b
    Vec4uq(Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
    }
    // Assignment operator to convert from type Vec256b
    Vec4uq & operator = (Vec256b const x) {
        y0 = x.get_low();  y1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec4uq & load(void const * p) {
        y0 = _mm_loadu_si128((__m128i const*)p);
        y1 = _mm_loadu_si128((__m128i const*)p + 1);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec4uq & load_a(void const * p) {
        y0 = _mm_load_si128((__m128i const*)p);
        y1 = _mm_load_si128((__m128i const*)p + 1);
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
        return y0;
    }
    Vec2uq get_high() const {
        return y1;
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
    return Vec4uq(a.get_low() >> b, a.get_high() >> b);
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
    return Vec4q(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec4qb operator < (Vec4uq const a, Vec4uq const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec4qb operator >= (Vec4uq const a, Vec4uq const b) {
    return Vec4q(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec4qb operator <= (Vec4uq const a, Vec4uq const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec4uq operator & (Vec4uq const a, Vec4uq const b) {
    return Vec4uq(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec4uq operator && (Vec4uq const a, Vec4uq const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec4uq operator | (Vec4uq const a, Vec4uq const b) {
    return Vec4q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec4uq operator || (Vec4uq const a, Vec4uq const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec4uq operator ^ (Vec4uq const a, Vec4uq const b) {
    return Vec4uq(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec4uq operator ~ (Vec4uq const a) {
    return Vec4uq(~a.get_low(), ~a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
// Each word in s must be either 0 (false) or -1 (true). No other values are allowed.
// (s is signed)
static inline Vec4uq select (Vec4qb const s, Vec4uq const a, Vec4uq const b) {
    return selectb(s,a,b);
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec4uq if_add (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
    return a + (Vec4uq(f) & b);
}

// Conditional subtract
static inline Vec4uq if_sub (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
    return a - (Vec4uq(f) & b);
}

// Conditional multiply
static inline Vec4uq if_mul (Vec4qb const f, Vec4uq const a, Vec4uq const b) {
    return select(f, a*b, a);
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint64_t horizontal_add (Vec4uq const a) {
    return (uint64_t)horizontal_add((Vec4q)a);
}

// function max: a > b ? a : b
static inline Vec4uq max(Vec4uq const a, Vec4uq const b) {
    return Vec4uq(max(a.get_low(),b.get_low()), max(a.get_high(),b.get_high()));
}

// function min: a < b ? a : b
static inline Vec4uq min(Vec4uq const a, Vec4uq const b) {
    return Vec4uq(min(a.get_low(),b.get_low()), min(a.get_high(),b.get_high()));
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

// permute vector of 4 64-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3 >
static inline Vec4q permute4(Vec4q const a) {
    return Vec4q(blend2<i0,i1> (a.get_low(), a.get_high()),
                 blend2<i2,i3> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3>
static inline Vec4uq permute4(Vec4uq const a) {
    return Vec4uq (permute4<i0,i1,i2,i3> (Vec4q(a)));
}

// permute vector of 8 32-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8i permute8(Vec8i const a) {
    return Vec8i(blend4<i0,i1,i2,i3> (a.get_low(), a.get_high()), 
                 blend4<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7 >
static inline Vec8ui permute4(Vec8ui const a) {
    return Vec8ui (permute8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8i(a)));
}

// permute vector of 16 16-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16s permute16(Vec16s const a) {
    return Vec16s(blend8<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()), 
                  blend8<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 >
static inline Vec16us permute16(Vec16us const a) {
    return Vec16us (permute16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16s(a)));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
          int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
          int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
static inline Vec32c permute32(Vec32c const a) {
    return Vec32c(blend16<i0, i1, i2 ,i3 ,i4 ,i5 ,i6 ,i7, i8, i9, i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()), 
                  blend16<i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a.get_low(), a.get_high()));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
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

// blend vectors Vec4q
template <int i0, int i1, int i2, int i3>
static inline Vec4q blend4(Vec4q const& a, Vec4q const& b) {
    Vec2q x0 = blend_half<Vec4q, i0, i1>(a, b);
    Vec2q x1 = blend_half<Vec4q, i2, i3>(a, b);
    return Vec4q(x0, x1);
}

// blend vectors Vec8i
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8i blend8(Vec8i const& a, Vec8i const& b) {
    Vec4i x0 = blend_half<Vec8i, i0, i1, i2, i3>(a, b);
    Vec4i x1 = blend_half<Vec8i, i4, i5, i6, i7>(a, b);
    return Vec8i(x0, x1);
}

// blend vectors Vec16s
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16s blend16(Vec16s const& a, Vec16s const& b) {
    Vec8s x0 = blend_half<Vec16s, i0, i1, i2, i3, i4, i5, i6, i7>(a, b);
    Vec8s x1 = blend_half<Vec16s, i8, i9, i10, i11, i12, i13, i14, i15>(a, b);
    return Vec16s(x0, x1);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 > 
    static inline Vec32c blend32(Vec32c const& a, Vec32c const& b) {
    Vec16c x0 = blend_half<Vec32c, i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15>(a, b);
    Vec16c x1 = blend_half<Vec32c, i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31>(a, b);
    return Vec32c(x0, x1);
}

// unsigned types:

template <int i0, int i1, int i2, int i3> 
static inline Vec4uq blend4(Vec4uq const a, Vec4uq const b) {
    return Vec4uq( blend4<i0,i1,i2,i3> (Vec4q(a),Vec4q(b)));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8ui blend8(Vec8ui const a, Vec8ui const b) {
    return Vec8ui( blend8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8i(a),Vec8i(b)));
}

template <int i0, int i1, int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16us blend16(Vec16us const a, Vec16us const b) {
    return Vec16us( blend16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16s(a),Vec16s(b)));
}

template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
    static inline Vec32uc blend32(Vec32uc const a, Vec32uc const b) {
        return Vec32uc (blend32<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15,    
            i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (Vec32c(a), Vec32c(b)));
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
#if defined (__XOP__)   // AMD XOP instruction set. Use VPPERM
    Vec16c t0 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_low());
    Vec16c t1 = _mm_perm_epi8(table.get_low(), table.get_high(), index.get_high());
    return Vec32c(t0, t1);
#else
    Vec16c t0 = lookup32(index.get_low() , table.get_low(), table.get_high());
    Vec16c t1 = lookup32(index.get_high(), table.get_low(), table.get_high());
    return Vec32c(t0, t1);
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
    uint8_t ii[32];  index1.store(ii);
    int8_t  rr[32];
    for (int j = 0; j < 32; j++) {
        rr[j] = ((int8_t*)table)[ii[j]];
    }
    return Vec32c().load(rr);
}

template <int n>
static inline Vec32c lookup(Vec32c const index, void const * table) {
    return lookup<n>(Vec32uc(index), table);
}

static inline Vec16s lookup16(Vec16s const index, Vec16s const table) {
    Vec8s t0 = lookup16(index.get_low() , table.get_low(), table.get_high());
    Vec8s t1 = lookup16(index.get_high(), table.get_low(), table.get_high());
    return Vec16s(t0, t1);
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
    Vec16us i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec16us(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec16us(index), n-1);
    }
    int16_t const * t = (int16_t const *)table;
    return Vec16s(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]],
        t[i1[8]],t[i1[9]],t[i1[10]],t[i1[11]],t[i1[12]],t[i1[13]],t[i1[14]],t[i1[15]]);
}

static inline Vec8i lookup8(Vec8i const index, Vec8i const table) {
    Vec4i t0 = lookup8(index.get_low() , table.get_low(), table.get_high());
    Vec4i t1 = lookup8(index.get_high(), table.get_low(), table.get_high());
    return Vec8i(t0, t1);
}

template <int n>
static inline Vec8i lookup(Vec8i const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4i table1 = Vec4i().load(table);        
        return Vec8i(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
    if (n <= 8) {
        return lookup8(index, Vec8i().load(table));
    }
    // n > 8. Limit index
    Vec8ui i1;
    if ((n & (n-1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec8ui(index) & (n-1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec8ui(index), n-1);
    }
    int32_t const * t = (int32_t const *)table;
    return Vec8i(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
}

static inline Vec4q lookup4(Vec4q const index, Vec4q const table) {
    return lookup8(Vec8i(index * 0x200000002ll + 0x100000000ll), Vec8i(table));
}

template <int n>
static inline Vec4q lookup(Vec4q const index, void const * table) {
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
        index1 = Vec4uq(min(Vec8ui(index), Vec8ui(n-1, 0, n-1, 0, n-1, 0, n-1, 0)));
    }
    uint32_t ii[8];  index1.store(ii);  // use only lower 32 bits of each index
    int64_t const * tt = (int64_t const *)table;
    return Vec4q(tt[ii[0]], tt[ii[2]], tt[ii[4]], tt[ii[6]]);    
}


/*****************************************************************************
*
*          Byte shifts
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
template <unsigned int b>
static inline Vec32c shift_bytes_up(Vec32c const a) {
    int8_t dat[64];
    if (b < 32) {
        Vec32c(0).store(dat);
        a.store(dat+b);
        return Vec32c().load(dat);
    }
    else return 0;
}

// Function shift_bytes_down: shift whole vector right by b bytes
template <unsigned int b>
static inline Vec32c shift_bytes_down(Vec32c const a) {
    int8_t dat[64];
    if (b < 32) {
        a.store(dat);
        Vec32c(0).store(dat+32);
        return Vec32c().load(dat+b);
    }
    else return 0;
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
    return lookup<imax+1>(Vec8i(i0,i1,i2,i3,i4,i5,i6,i7), a);
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
    // use lookup function
    return lookup<imax+1>(Vec4q(i0,i1,i2,i3), a);
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
    int32_t* arr = (int32_t*)array;
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
}

template <int i0, int i1, int i2, int i3>
static inline void scatter(Vec4q const data, void * array) {
    int64_t* arr = (int64_t*)array;
    const int index[4] = {i0,i1,i2,i3};
    for (int i = 0; i < 4; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
}

// scatter functions with variable indexes

static inline void scatter(Vec8i const index, uint32_t limit, Vec8i const data, void * destination) {
    int32_t* arr = (int32_t*)destination;
    for (int i = 0; i < 8; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
}

static inline void scatter(Vec4q const index, uint32_t limit, Vec4q const data, void * destination) {
    int64_t* arr = (int64_t*)destination;
    for (int i = 0; i < 4; i++) {
        if (uint64_t(index[i]) < uint64_t(limit)) arr[index[i]] = data[i];
    }
} 

static inline void scatter(Vec4i const index, uint32_t limit, Vec4q const data, void * destination) {
    int64_t* arr = (int64_t*)destination;
    for (int i = 0; i < 4; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
} 

/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 16 elements to 16 bits with sign extension
static inline Vec16s extend_low (Vec32c const a) {
    return Vec16s(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec16s extend_high (Vec32c const a) {
    return Vec16s(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec16us extend_low (Vec32uc const a) {
    return Vec16us(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec16us extend_high (Vec32uc const a) {
    return Vec16us(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec8i extend_low (Vec16s const a) {
    return Vec8i(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec8i extend_high (Vec16s const a) {
    return Vec8i(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec8ui extend_low (Vec16us const a) {
    return Vec8ui(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec8ui extend_high (Vec16us const a) {
    return Vec8ui(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 4 elements to 64 bits with sign extension
static inline Vec4q extend_low (Vec8i const a) {
    return Vec4q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 4 elements to 64 bits with sign extension
static inline Vec4q extend_high (Vec8i const a) {
    return Vec4q(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 4 elements to 64 bits with zero extension
static inline Vec4uq extend_low (Vec8ui const a) {
    return Vec4uq(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 4 elements to 64 bits with zero extension
static inline Vec4uq extend_high (Vec8ui const a) {
    return Vec4uq(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec32c compress (Vec16s const low, Vec16s const high) {
    return Vec32c(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec32c compress_saturated (Vec16s const low, Vec16s const high) {
    return Vec32c(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec32uc compress (Vec16us const low, Vec16us const high) {
    return Vec32uc(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec32uc compress_saturated (Vec16us const low, Vec16us const high) {
    return Vec32uc(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16s compress (Vec8i const low, Vec8i const high) {
    return Vec16s(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec16s compress_saturated (Vec8i const low, Vec8i const high) {
    return Vec16s(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec16us compress (Vec8ui const low, Vec8ui const high) {
    return Vec16us(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec16us compress_saturated (Vec8ui const low, Vec8ui const high) {
    return Vec16us(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec8i compress (Vec4q const low, Vec4q const high) {
    return Vec8i(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec8i compress_saturated (Vec4q const low, Vec4q const high) {
    return Vec8i(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec8ui compress (Vec4uq const low, Vec4uq const high) {
    return Vec8ui (compress((Vec4q)low, (Vec4q)high));
}

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec8ui compress_saturated (Vec4uq const low, Vec4uq const high) {
    return Vec8ui(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec8i by compile-time constant
template <int32_t d>
static inline Vec8i divide_by_i(Vec8i const a) {
    return Vec8i(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
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
    return divide_by_i<int32_t(d)>(a);                               // signed divide
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
static inline Vec8ui divide_by_ui(Vec8ui const a) {
    return Vec8ui( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
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
    return divide_by_ui<d>(a);                                       // unsigned divide
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
static inline Vec16s divide_by_i(Vec16s const a) {
    return Vec16s( divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
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
    return divide_by_i<int(d)>(a);                                   // signed divide
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
static inline Vec16us divide_by_ui(Vec16us const a) {
    return Vec16us( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
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
    return divide_by_ui<d>(a);                                       // unsigned divide
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
    return a / Const_int_t<d>();                                     // signed divide
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
    return a / Const_uint_t<d>();                                    // unsigned divide
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

// to_bits: convert boolean vector to integer bitfield
static inline uint32_t to_bits(Vec32cb const x) {
    return to_bits(x.get_low()) | (uint32_t)to_bits(x.get_high()) << 16;
}

// to_bits: convert boolean vector to integer bitfield
static inline uint16_t to_bits(Vec16sb const x) {
    return uint16_t(to_bits(x.get_low()) | (uint16_t)to_bits(x.get_high()) << 8);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec8ib const x) {
    return uint8_t(to_bits(x.get_low()) | (uint8_t)to_bits(x.get_high()) << 4);
}

// to_bits: convert boolean vector to integer bitfield
static inline uint8_t to_bits(Vec4qb const x) {
    return uint8_t(to_bits(x.get_low()) | to_bits(x.get_high()) << 2);
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI256E_H
