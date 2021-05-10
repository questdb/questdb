/****************************  vectori512se.h   *******************************
* Author:        Agner Fog
* Date created:  2019-04-20
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file defining 512-bit integer vector classes for 8 and 16 bit integers.
* Emulated for processors without AVX512BW instruction set.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec64c    Vector of  64  8-bit  signed   integers
* Vec64uc   Vector of  64  8-bit  unsigned integers
* Vec64cb   Vector of  64  booleans for use with Vec64c and Vec64uc
* Vec32s    Vector of  32  16-bit signed   integers
* Vec32us   Vector of  32  16-bit unsigned integers
* Vec32sb   Vector of  32  booleans for use with Vec32s and Vec32us
* Other 512-bit integer vectors are defined in Vectori512.h
*
* Each vector object is represented internally in the CPU as two 256-bit registers.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef VECTORI512SE_H
#define VECTORI512SE_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#ifdef VECTORI512S_H
#error Two different versions of vectorf256.h included
#endif 


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Vector of 64 8-bit signed integers
*
*****************************************************************************/

class Vec64c  {
protected:
    Vec256b z0;          // lower 256 bits
    Vec256b z1;          // higher 256 bits
public:
    // Default constructor:
    Vec64c() {
    }
    // Constructor to build from two Vec32c:
    Vec64c(Vec32c const a0, Vec32c const a1) {
        z0 = a0;
        z1 = a1;
    }
    // Constructor to broadcast the same value into all elements:
    Vec64c(int8_t i) {
        z0 = z1 = Vec32c(i);
    } 
    // Constructor to build from all elements:
    Vec64c(int8_t i0, int8_t i1, int8_t i2, int8_t i3, int8_t i4, int8_t i5, int8_t i6, int8_t i7,
        int8_t i8, int8_t i9, int8_t i10, int8_t i11, int8_t i12, int8_t i13, int8_t i14, int8_t i15,        
        int8_t i16, int8_t i17, int8_t i18, int8_t i19, int8_t i20, int8_t i21, int8_t i22, int8_t i23,
        int8_t i24, int8_t i25, int8_t i26, int8_t i27, int8_t i28, int8_t i29, int8_t i30, int8_t i31,
        int8_t i32, int8_t i33, int8_t i34, int8_t i35, int8_t i36, int8_t i37, int8_t i38, int8_t i39,        
        int8_t i40, int8_t i41, int8_t i42, int8_t i43, int8_t i44, int8_t i45, int8_t i46, int8_t i47,        
        int8_t i48, int8_t i49, int8_t i50, int8_t i51, int8_t i52, int8_t i53, int8_t i54, int8_t i55,        
        int8_t i56, int8_t i57, int8_t i58, int8_t i59, int8_t i60, int8_t i61, int8_t i62, int8_t i63) {
        // _mm512_set_epi8 and _mm512_set_epi16 missing in GCC 7.4.0
        int8_t aa[64] = {
            i0, i1, i2, i3, i4, i5, i6, i7,i8, i9, i10, i11, i12, i13, i14, i15,        
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31,
            i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,        
            i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63 };
        load(aa);
    }
#ifdef VECTORI512_H
    // Constructor to convert from type __m512i used in intrinsics:
    Vec64c(__m512i const x) {
        z0 = Vec16i(x).get_low();
        z1 = Vec16i(x).get_high();
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec64c & operator = (__m512i const x) {
        return *this = Vec64c(x);
    }
    // Type cast operator to convert to __m512i used in intrinsics
    operator __m512i() const {
        return Vec16i(Vec8i(z0),Vec8i(z1));
    }
#else
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec64c & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
#endif
    // Constructor to convert from type Vec512b
    Vec64c(Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
     }
    // Type cast operator to convert to Vec512b used in emulation
    operator Vec512b() const {
        return Vec512b(z0,z1);
    }
    // Member function to load from array (unaligned)
    Vec64c & load(void const * p) {
        Vec16i x = Vec16i().load(p);
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec64c & load_a(void const * p) {
        Vec16i x = Vec16i().load_a(p);
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec64c & load_partial(int n, void const * p) {
        Vec32c lo, hi; 
        if ((uint32_t)n < 32) {
            lo = Vec32c().load_partial(n,p);
            hi = Vec32c(0);
        }
        else {
            lo = Vec32c().load(p);
            hi = Vec32c().load_partial(n-32, ((int8_t*)p)+32);
        }
        *this = Vec64c(lo, hi);
        return *this;
    }
    // store
    void store(void * p) const {
        Vec16i x = Vec16i(Vec8i(z0),Vec8i(z1));
        x.store(p);
    }
    // store with non-temporal memory hint
    void store_nt(void * p) const {
        Vec16i x = Vec16i(Vec8i(z0),Vec8i(z1));
        x.store_nt(p);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return Vec16i::store_nt_alignment();
    }
    // store aligned
    void store_a(void * p) const {
        Vec16i x = Vec16i(Vec8i(z0),Vec8i(z1));
        x.store_a(p);
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if ((uint32_t)n < 32) {
            get_low().store_partial(n, p);
        }
        else {
            get_low().store(p);
            get_high().store_partial(n-32, ((int8_t*)p)+32);
        }
    } 
    // cut off vector to n elements. The last 64-n elements are set to zero
    Vec64c & cutoff(int n) {
        Vec32c lo, hi;
        if ((uint32_t)n < 32) {
            lo = Vec32c(get_low()).cutoff(n);
            hi = Vec32c(0);
        }
        else {
            lo = get_low();
            hi = Vec32c(get_high()).cutoff(n-32);
        }
        *this = Vec64c(lo, hi);
        return *this;
    }
    // Member function to change a single element in vector
    Vec64c const insert(int index, int8_t value) {
        Vec32c lo, hi;
        if ((uint32_t)index < 32) {
            lo = Vec32c(get_low()).insert(index, value);
            hi = get_high();
        }
        else {
            lo = get_low();
            hi = Vec32c(get_high()).insert(index-32, value);
        }
        *this = Vec64c(lo, hi);
        return *this;
    }
    // Member function extract a single element from vector
    int8_t extract(int index) const {
        if ((uint32_t)index < 32) {
            return Vec32c(get_low()).extract(index);
        }
        else {
            return Vec32c(get_high()).extract(index-32);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int8_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec32c:
    Vec32c get_low() const {
        return z0;
    }
    Vec32c get_high() const {
        return z1;
    }
    static constexpr int size() {
        return 64;
    }
    static constexpr int elementtype() {
        return 4;
    }
};


/*****************************************************************************
*
*          Vec64cb: Vector of 64 Booleans for use with Vec64c and Vec64uc
*
*****************************************************************************/

class Vec64cb : public Vec64c {
public:
    // Default constructor:
    Vec64cb () {
    }
    Vec64cb (Vec64c const a) : Vec64c(a) {}

    // Constructor to build from all elements: Not implemented

    // Constructor to convert from type __mmask64 used in intrinsics: not possible
    // Vec64cb (__mmask64 x); 

    // Constructor to broadcast single value:
    Vec64cb(bool b) {
        z0 = z1 = Vec32c(-int8_t(b));
    }
    // Constructor to make from two halves (big booleans)
    Vec64cb (Vec32cb const x0, Vec32cb const x1) : Vec64c(x0,x1) {}

    // Assignment operator to convert from type __mmask64 used in intrinsics: not possible
    //Vec64cb & operator = (__mmask64 x);

    // Member functions to split into two Vec32cb:
    Vec32cb get_low() const {
        return Vec32c(z0);
    }
    Vec32cb get_high() const {
        return Vec32c(z1);
    }
    // Assignment operator to broadcast scalar value:
    Vec64cb & operator = (bool b) {
        *this = Vec64cb(b);
        return *this;
    }
    // Member function to change a single element in vector
    Vec64cb & insert (int index, bool a) {
        if ((uint32_t)index < 32) {
            z0 = get_low().insert(index, a);
        }
        else {
            z1 = get_high().insert(index-32, a);
        }
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const { 
        if (index < 32) {
            return get_low().extract(index);
        }
        else {
            return get_high().extract(index-32);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Type cast operator to convert to __mmask64 used in intrinsics. not possible
    //operator __mmask64() const;

    // Member function to change a bitfield to a boolean vector
    Vec64cb & load_bits(uint64_t a) {
        Vec32cb x0 = Vec32cb().load_bits(uint32_t(a));
        Vec32cb x1 = Vec32cb().load_bits(uint32_t(a>>32));
        *this = Vec64cb(x0,x1);
        return *this;
    }
    static constexpr int size() {
        return 64;
    }
    static constexpr int elementtype() {
        return 3;
    }
    Vec64cb(int b) = delete; // Prevent constructing from int, etc.
    Vec64cb & operator = (int x) = delete; // Prevent assigning int because of ambiguity
};


/*****************************************************************************
*
*          Define operators and functions for Vec64cb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec64cb operator & (Vec64cb const a, Vec64cb const b) {
    return Vec64cb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec64cb operator && (Vec64cb const a, Vec64cb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec64cb & operator &= (Vec64cb & a, Vec64cb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec64cb operator | (Vec64cb const a, Vec64cb const b) {
    return Vec64cb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec64cb operator || (Vec64cb const a, Vec64cb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec64cb & operator |= (Vec64cb & a, Vec64cb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec64cb operator ^ (Vec64cb const a, Vec64cb const b) {
    return Vec64cb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec64cb & operator ^= (Vec64cb & a, Vec64cb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec64cb operator == (Vec64cb const a, Vec64cb const b) {
    return Vec64cb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : xor
static inline Vec64cb operator != (Vec64cb const a, Vec64cb const b) {
    return a ^ b;
}

// vector operator ~ : bitwise not
static inline Vec64cb operator ~ (Vec64cb const a) {
    return Vec64cb(~a.get_low(), ~a.get_high());}

// vector operator ! : element not
static inline Vec64cb operator ! (Vec64cb const a) {
    return ~a;
}

// vector function andnot
static inline Vec64cb andnot (Vec64cb const a, Vec64cb const b) {
    return Vec64cb(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec64cb const a) {
    return horizontal_and(a.get_low()) && horizontal_and(a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec64cb const a) {
    return horizontal_or(a.get_low()) || horizontal_or(a.get_high());
}

// to_bits: convert boolean vector to integer bitfield
static inline uint64_t to_bits(Vec64cb x) {
    return (uint64_t(to_bits(x.get_high())) << 32) | to_bits(x.get_low());
}


/*****************************************************************************
*
*          Define operators for Vec64c
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec64c operator + (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator += : add
static inline Vec64c & operator += (Vec64c & a, Vec64c const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec64c operator ++ (Vec64c & a, int) {
    Vec64c a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec64c & operator ++ (Vec64c & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec64c operator - (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec64c operator - (Vec64c const a) {
    return Vec64c(-a.get_low(), -a.get_high());
}

// vector operator -= : subtract
static inline Vec64c & operator -= (Vec64c & a, Vec64c const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec64c operator -- (Vec64c & a, int) {
    Vec64c a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec64c & operator -- (Vec64c & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec64c operator * (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec64c & operator *= (Vec64c & a, Vec64c const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file

// vector operator << : shift left
static inline Vec64c operator << (Vec64c const a, int32_t b) {
    return Vec64c(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec64c & operator <<= (Vec64c & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec64c operator >> (Vec64c const a, int32_t b) {
    return Vec64c(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec64c & operator >>= (Vec64c & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec64cb operator == (Vec64c const a, Vec64c const b) {
    return Vec64cb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec64cb operator != (Vec64c const a, Vec64c const b) {
    return Vec64cb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec64cb operator > (Vec64c const a, Vec64c const b) {
    return Vec64cb(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec64cb operator < (Vec64c const a, Vec64c const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec64cb operator >= (Vec64c const a, Vec64c const b) {
    return Vec64cb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec64cb operator <= (Vec64c const a, Vec64c const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec64c operator & (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec64c & operator &= (Vec64c & a, Vec64c const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec64c operator | (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec64c & operator |= (Vec64c & a, Vec64c const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec64c operator ^ (Vec64c const a, Vec64c const b) {
    return Vec64c(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec64c & operator ^= (Vec64c & a, Vec64c const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec64c operator ~ (Vec64c const a) {
    return Vec64c(~a.get_low(), ~a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec64c select (Vec64cb const s, Vec64c const a, Vec64c const b) {
    return Vec64c(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec64c if_add (Vec64cb const f, Vec64c const a, Vec64c const b) {
    return Vec64c(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec64c if_sub (Vec64cb const f, Vec64c const a, Vec64c const b) {
    return Vec64c(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec64c if_mul (Vec64cb const f, Vec64c const a, Vec64c const b) {
    return Vec64c(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int8_t horizontal_add (Vec64c const a) {
    return (int8_t)horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec64c const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec64c add_saturated(Vec64c const a, Vec64c const b) {
    return Vec64c(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec64c sub_saturated(Vec64c const a, Vec64c const b) {
    return Vec64c(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec64c max(Vec64c const a, Vec64c const b) {
    return Vec64c(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec64c min(Vec64c const a, Vec64c const b) {
    return Vec64c(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec64c abs(Vec64c const a) {
    return Vec64c(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec64c abs_saturated(Vec64c const a) {
    return Vec64c(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec64c rotate_left(Vec64c const a, int b) {
    return Vec64c(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
}


/*****************************************************************************
*
*          Vector of 64 8-bit unsigned integers
*
*****************************************************************************/

class Vec64uc : public Vec64c {
public:
    // Default constructor:
    Vec64uc() {
    }
    // Construct from Vec64c
    Vec64uc(Vec64c const a) : Vec64c(a) {
    }
    // Constructor to broadcast the same value into all elements:
    Vec64uc(uint8_t i) : Vec64c(int8_t(i)) {
    }
    // Constructor to build from two Vec32uc:
    Vec64uc(Vec32uc const a0, Vec32uc const a1) : Vec64c(a0,a1) {
    }
    // Constructor to build from all elements:
    Vec64uc(uint8_t i0, uint8_t i1, uint8_t i2, uint8_t i3, uint8_t i4, uint8_t i5, uint8_t i6, uint8_t i7,
        uint8_t i8, uint8_t i9, uint8_t i10, uint8_t i11, uint8_t i12, uint8_t i13, uint8_t i14, uint8_t i15,        
        uint8_t i16, uint8_t i17, uint8_t i18, uint8_t i19, uint8_t i20, uint8_t i21, uint8_t i22, uint8_t i23,
        uint8_t i24, uint8_t i25, uint8_t i26, uint8_t i27, uint8_t i28, uint8_t i29, uint8_t i30, uint8_t i31,
        uint8_t i32, uint8_t i33, uint8_t i34, uint8_t i35, uint8_t i36, uint8_t i37, uint8_t i38, uint8_t i39,        
        uint8_t i40, uint8_t i41, uint8_t i42, uint8_t i43, uint8_t i44, uint8_t i45, uint8_t i46, uint8_t i47,        
        uint8_t i48, uint8_t i49, uint8_t i50, uint8_t i51, uint8_t i52, uint8_t i53, uint8_t i54, uint8_t i55,        
        uint8_t i56, uint8_t i57, uint8_t i58, uint8_t i59, uint8_t i60, uint8_t i61, uint8_t i62, uint8_t i63) {
        // _mm512_set_epi8 and _mm512_set_epi16 missing in GCC 7.4.0
        uint8_t aa[64] = {
            i0, i1, i2, i3, i4, i5, i6, i7,i8, i9, i10, i11, i12, i13, i14, i15,        
            i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31,
            i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,        
            i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63 };
        load(aa);
    }

#ifdef VECTORI512_H
   // Constructor to convert from type __m512i used in intrinsics:
   Vec64uc(__m512i const x) : Vec64c(x) {};

   // Assignment operator to convert from type __m512i used in intrinsics:
   Vec64uc & operator = (__m512i const x) {
       return *this = Vec64uc(x);
   }
#else
    // Constructor to convert from type Vec512b
    Vec64uc(Vec512b const x) : Vec64c(x) {}

    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec64uc & operator = (Vec512b const x) {
        return *this = Vec64uc(x);
    }
#endif 
    // Member function to load from array (unaligned)
    Vec64uc & load(void const * p) {
        Vec64c::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec64uc & load_a(void const * p) {
        Vec64c::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec64uc const insert(int index, uint8_t value) {
        Vec64c::insert(index, (int8_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint8_t extract(int index) const {
        return (uint8_t)Vec64c::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint8_t operator [] (int index) const {
        return (uint8_t)Vec64c::extract(index);
    }
    // Member functions to split into two Vec32uc:
    Vec32uc get_low() const {
        return Vec32uc(Vec64c::get_low());
    }
    Vec32uc get_high() const {
        return Vec32uc(Vec64c::get_high());
    }
    static constexpr int elementtype() {
        return 5;
    }
};

// Define operators for this class

// vector operator + : add element by element
static inline Vec64uc operator + (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator - : subtract element by element
static inline Vec64uc operator - (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator ' : multiply element by element
static inline Vec64uc operator * (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(a.get_low() * b.get_low(), a.get_high() * b.get_high());
} 

// vector operator / : divide
// See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec64uc operator >> (Vec64uc const a, uint32_t b) {
    return Vec64uc(a.get_low() >> b, a.get_high() >> b);
}
static inline Vec64uc operator >> (Vec64uc const a, int b) {
    return a >> uint32_t(b);
}

// vector operator >>= : shift right logical
static inline Vec64uc & operator >>= (Vec64uc & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical (signed b)
static inline Vec64uc & operator >>= (Vec64uc & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec64uc operator << (Vec64uc const a, uint32_t b) {
    return Vec64uc(a.get_low() << b, a.get_high() << b);
}
static inline Vec64uc operator << (Vec64uc const a, int b) {
    return a << uint32_t(b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec64cb operator < (Vec64uc const a, Vec64uc const b) {
    return Vec64cb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec64cb operator > (Vec64uc const a, Vec64uc const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec64cb operator >= (Vec64uc const a, Vec64uc const b) {
    return Vec64cb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec64cb operator <= (Vec64uc const a, Vec64uc const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec64uc operator & (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) & Vec64c(b));
}

// vector operator | : bitwise or
static inline Vec64uc operator | (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) | Vec64c(b));
}

// vector operator ^ : bitwise xor
static inline Vec64uc operator ^ (Vec64uc const a, Vec64uc const b) {
    return Vec64uc(Vec64c(a) ^ Vec64c(b));
}

// vector operator ~ : bitwise not
static inline Vec64uc operator ~ (Vec64uc const a) {
    return Vec64uc( ~ Vec64c(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec64uc select (Vec64cb const s, Vec64uc const a, Vec64uc const b) {
    return Vec64uc(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec64uc if_add (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    return Vec64uc(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec64uc if_sub (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    return Vec64uc(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec64uc if_mul (Vec64cb const f, Vec64uc const a, Vec64uc const b) {
    return Vec64uc(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
} 

// function add_saturated: add element by element, unsigned with saturation
static inline Vec64uc add_saturated(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec64uc sub_saturated(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec64uc max(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec64uc min(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}


/*****************************************************************************
*
*          Vector of 32 16-bit signed integers
*
*****************************************************************************/

class Vec32s : public Vec64c {
public:
    // Default constructor:
    Vec32s() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec32s(int16_t i) {
        z0 = z1 = Vec16s(i);
    }
    // Constructor to build from all elements:
    Vec32s(int16_t i0, int16_t i1, int16_t i2, int16_t i3, int16_t i4, int16_t i5, int16_t i6, int16_t i7,
        int16_t i8, int16_t i9, int16_t i10, int16_t i11, int16_t i12, int16_t i13, int16_t i14, int16_t i15,        
        int16_t i16, int16_t i17, int16_t i18, int16_t i19, int16_t i20, int16_t i21, int16_t i22, int16_t i23,
        int16_t i24, int16_t i25, int16_t i26, int16_t i27, int16_t i28, int16_t i29, int16_t i30, int16_t i31) {
        Vec16s x0 = Vec16s(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
        Vec16s x1 = Vec16s(i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
        *this = Vec32s(x0,x1);
    }
    // Constructor to build from two Vec16s:
    Vec32s(Vec16s const a0, Vec16s const a1) {
        z0 = a0;  z1 = a1;
    }
#ifdef VECTORI512_H
    // Constructor to convert from type __m512i used in intrinsics:
    Vec32s(__m512i const x) {
        Vec16i zz(x);
        z0 = zz.get_low();
        z1 = zz.get_high();
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec32s & operator = (__m512i const x) {
        Vec16i zz(x);
        z0 = zz.get_low();
        z1 = zz.get_high();
        return *this;
    }
#else
    // Constructor to convert from type Vec512b
    Vec32s(Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Assignment operator to convert from type Vec512b
    Vec32s & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
#endif
    // Member function to load from array (unaligned)
    Vec32s & load(void const * p) {
        z0 = Vec16s().load(p);
        z1 = Vec16s().load((int16_t*)p + 16);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec32s & load_a(void const * p) {
        z0 = Vec16s().load_a(p);
        z1 = Vec16s().load_a((int16_t*)p + 16);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec32s & load_partial(int n, void const * p) {
        if (uint32_t(n) < 16) {
            z0 = Vec16s().load_partial(n, p);
            z1 = Vec16s(0);
        }
        else {
            z0 = Vec16s().load(p);
            z1 = Vec16s().load_partial(n-16, (int16_t*)p + 16);
        }
        return *this;
    }
    // store
    void store(void * p) const {
        Vec16s(z0).store(p);
        Vec16s(z1).store((int16_t*)p + 16);
    }
    // store with non-temporal memory hint
    void store_nt(void * p) const {
        Vec16s(z0).store_nt(p);
        Vec16s(z1).store_nt((int16_t*)p + 16);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return Vec16s::store_nt_alignment();
    }
    // store aligned
    void store_a(void * p) const {
        Vec16s(z0).store_a(p);
        Vec16s(z1).store_a((int16_t*)p + 16);
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (uint32_t(n) < 16) {
            Vec16s(z0).store_partial(n, p);
        }
        else {
            Vec16s(z0).store(p);
            Vec16s(z1).store_partial(n-16, (int16_t*)p + 16);
        }
    } 
    // cut off vector to n elements. The last 32-n elements are set to zero
    Vec32s & cutoff(int n) {
        if (uint32_t(n) < 16) {
            z0 = Vec16s(z0).cutoff(n);
            z1 = Vec16s(0);
        }
        else {
            z1 = Vec16s(z1).cutoff(n-16);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec32s const insert(int index, int16_t value) {
        if ((uint32_t)index < 16) {
            z0 = Vec16s(z0).insert(index, value);
        }
        else {
            z1 = Vec16s(z1).insert(index-16, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int16_t extract(int index) const {
        if (index < 16) {
            return Vec16s(z0).extract(index);
        }
        else {
            return Vec16s(z1).extract(index-16);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int16_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec16s:
    Vec16s get_low() const {
        return z0;
    }
    Vec16s get_high() const {
        return z1;
    }
    static constexpr int size() {
        return 32;
    }
    static constexpr int elementtype() {
        return 6;
    }
};


/*****************************************************************************
*
*          Vec32sb: Vector of 64 Booleans for use with Vec32s and Vec32us
*
*****************************************************************************/

class Vec32sb : public Vec32s {
public:
    // Default constructor:
    Vec32sb () {
    }
    // Constructor to build from all elements: Not implemented

    // Constructor to convert from type __mmask32 used in intrinsics: not possible

    // Constructor to broadcast single value:
    Vec32sb(bool b) {
        z0 = z1 = Vec16s(-int16_t(b));
    }
    // Constructor to make from two halves
    Vec32sb (Vec16sb const x0, Vec16sb const x1) {
        z0 = x0;  z1 = x1;
    }
    // Assignment operator to convert from type __mmask32 used in intrinsics: not possible

    // Assignment operator to broadcast scalar value:
    Vec32sb & operator = (bool b) {
        *this = Vec32sb(b);
        return *this;
    }
    // Member functions to split into two Vec16sb:
    Vec16sb get_low() const {
        return z0;
    }
    Vec16sb get_high() const {
        return z1;
    } 
    // Member function to change a single element in vector
    Vec32sb & insert(int index, bool a) {
        Vec32s::insert(index, -(int16_t)a);
        return *this;
    }    
    // Member function extract a single element from vector
    bool extract(int index) const {
        return Vec32s::extract(index) != 0;
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    // Type cast operator to convert to __mmask64 used in intrinsics. Not possible

    // Member function to change a bitfield to a boolean vector
    Vec32sb & load_bits(uint32_t a) {
        z0 = Vec16sb().load_bits(uint16_t(a));
        z1 = Vec16sb().load_bits(uint16_t(a>>16));
        return *this;
    }
    static constexpr int elementtype() {
        return 3;
    }
    Vec32sb(int b) = delete; // Prevent constructing from int, etc.
    Vec32sb & operator = (int x) = delete; // Prevent assigning int because of ambiguity
};


/*****************************************************************************
*
*          Define operators and functions for Vec32sb
*
*****************************************************************************/

// vector operator & : bitwise and
static inline Vec32sb operator & (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec32sb operator && (Vec32sb const a, Vec32sb const b) {
    return a & b;
}
// vector operator &= : bitwise and
static inline Vec32sb & operator &= (Vec32sb & a, Vec32sb const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32sb operator | (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec32sb operator || (Vec32sb const a, Vec32sb const b) {
    return a | b;
}
// vector operator |= : bitwise or
static inline Vec32sb & operator |= (Vec32sb & a, Vec32sb const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32sb operator ^ (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec32sb & operator ^= (Vec32sb & a, Vec32sb const b) {
    a = a ^ b;
    return a;
}

// vector operator == : xnor
static inline Vec32sb operator == (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(a.get_low() == b.get_low(), a.get_high() == b.get_high());}

// vector operator != : xor
static inline Vec32sb operator != (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());}

// vector operator ~ : bitwise not
static inline Vec32sb operator ~ (Vec32sb const a) {
    return Vec32sb(~a.get_low(), ~a.get_high());}

// vector operator ! : element not
static inline Vec32sb operator ! (Vec32sb const a) {
    return ~a;
}

// vector function andnot
static inline Vec32sb andnot (Vec32sb const a, Vec32sb const b) {
    return Vec32sb(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec32sb const a) {
    return horizontal_and(a.get_low()) && horizontal_and(a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec32sb const a) {
    return horizontal_or(a.get_low()) || horizontal_or(a.get_high());
}

// to_bits: convert boolean vector to integer bitfield
static inline uint32_t to_bits(Vec32sb a) {
    return uint32_t(to_bits(a.get_high())) << 16 | to_bits(a.get_low());
}


/*****************************************************************************
*
*          Define operators for Vec32s
*
*****************************************************************************/

// vector operator + : add element by element
static inline Vec32s operator + (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator += : add
static inline Vec32s & operator += (Vec32s & a, Vec32s const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec32s operator ++ (Vec32s & a, int) {
    Vec32s a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec32s & operator ++ (Vec32s & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec32s operator - (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec32s operator - (Vec32s const a) {
    return Vec32s(-a.get_low(), -a.get_high());
}

// vector operator -= : subtract
static inline Vec32s & operator -= (Vec32s & a, Vec32s const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec32s operator -- (Vec32s & a, int) {
    Vec32s a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec32s & operator -- (Vec32s & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec32s operator * (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec32s & operator *= (Vec32s & a, Vec32s const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer
// See bottom of file 

// vector operator << : shift left
static inline Vec32s operator << (Vec32s const a, int32_t b) {
    return Vec32s(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec32s & operator <<= (Vec32s & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec32s operator >> (Vec32s const a, int32_t b) {
    return Vec32s(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec32s & operator >>= (Vec32s & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec32sb operator == (Vec32s const a, Vec32s const b) {
    return Vec32sb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec32sb operator != (Vec32s const a, Vec32s const b) {
    return Vec32sb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec32sb operator > (Vec32s const a, Vec32s const b) {
    return Vec32sb(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec32sb operator < (Vec32s const a, Vec32s const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec32sb operator >= (Vec32s const a, Vec32s const b) {
    return Vec32sb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec32sb operator <= (Vec32s const a, Vec32s const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec32s operator & (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec32s & operator &= (Vec32s & a, Vec32s const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec32s operator | (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec32s & operator |= (Vec32s & a, Vec32s const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec32s operator ^ (Vec32s const a, Vec32s const b) {
    return Vec32s(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec32s & operator ^= (Vec32s & a, Vec32s const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec32s operator ~ (Vec32s const a) {
    return Vec32s(~a.get_low(), ~a.get_high());
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32s select (Vec32sb const s, Vec32s const a, Vec32s const b) {
    return Vec32s(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32s if_add (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return Vec32s(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec32s if_sub (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return Vec32s(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec32s if_mul (Vec32sb const f, Vec32s const a, Vec32s const b) {
    return Vec32s(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int16_t horizontal_add (Vec32s const a) {
    Vec16s s = a.get_low() + a.get_high();
    return (int16_t)horizontal_add(s);
}

// Horizontal add extended: Calculates the sum of all vector elements.
// Each element is sign-extended before addition to avoid overflow
static inline int32_t horizontal_add_x (Vec32s const a) {
    return horizontal_add_x(a.get_low()) + horizontal_add_x(a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec32s add_saturated(Vec32s const a, Vec32s const b) {
    return Vec32s(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec32s sub_saturated(Vec32s const a, Vec32s const b) {
    return Vec32s(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec32s max(Vec32s const a, Vec32s const b) {
    return Vec32s(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec32s min(Vec32s const a, Vec32s const b) {
    return Vec32s(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec32s abs(Vec32s const a) {
    return Vec32s(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec32s abs_saturated(Vec32s const a) {
    return Vec32s(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec32s rotate_left(Vec32s const a, int b) {
    return Vec32s(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
} 


/*****************************************************************************
*
*          Vector of 32 16-bit unsigned integers
*
*****************************************************************************/

class Vec32us : public Vec32s {
public:
    // Default constructor:
    Vec32us() {
    }
    // Construct from Vec32s
    Vec32us(Vec32s const a) {
        z0 = a.get_low();  z1 = a.get_high(); 
    }
    // Constructor to broadcast the same value into all elements:
    Vec32us(uint16_t i) {
        z0 = z1 = Vec16us(i);
    }
    // Constructor to build from all elements:
    Vec32us(uint16_t i0, uint16_t i1, uint16_t i2, uint16_t i3, uint16_t i4, uint16_t i5, uint16_t i6, uint16_t i7,
        uint16_t i8, uint16_t i9, uint16_t i10, uint16_t i11, uint16_t i12, uint16_t i13, uint16_t i14, uint16_t i15,        
        uint16_t i16, uint16_t i17, uint16_t i18, uint16_t i19, uint16_t i20, uint16_t i21, uint16_t i22, uint16_t i23,
        uint16_t i24, uint16_t i25, uint16_t i26, uint16_t i27, uint16_t i28, uint16_t i29, uint16_t i30, uint16_t i31) {
        Vec16us x0 = Vec16us(i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15);
        Vec16us x1 = Vec16us(i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31);
        *this = Vec32us(x0,x1);
    }
    // Constructor to build from two Vec16us:
    Vec32us(Vec16us const a0, Vec16us const a1) {
        z0 = a0;  z1 = a1;
    }
#ifdef VECTORI512_H
    // Constructor to convert from type __m512i used in intrinsics:
    Vec32us(__m512i const x) : Vec32s(x) {
    }
    // Assignment operator to convert from type __m512i used in intrinsics:
    Vec32us & operator = (__m512i const x) {
        return *this = Vec32us(x);
    }
#else
    // Constructor to convert from type Vec512b
    Vec32us(Vec512b const x) : Vec32s(x) {}
    // Assignment operator to convert from type Vec512b
    Vec32us & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
#endif
    // Member function to load from array (unaligned)
    Vec32us & load(void const * p) {
        Vec32s::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec32us & load_a(void const * p) {
        Vec32s::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec32us const insert(int index, uint16_t value) {
        Vec32s::insert(index, (int16_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint16_t extract(int index) const {
        return (uint16_t)Vec32s::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint16_t operator [] (int index) const {
        return (uint16_t)Vec32s::extract(index);
    }
    // Member functions to split into two Vec16us:
    Vec16us get_low() const {
        return Vec16us(Vec32s::get_low());
    }
    Vec16us get_high() const {
        return Vec16us(Vec32s::get_high());
    }
    static constexpr int elementtype() {
        return 7;
    }
};

// Define operators for this class

// vector operator + : add element by element
static inline Vec32us operator + (Vec32us const a, Vec32us const b) {
    return Vec32us(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator - : subtract element by element
static inline Vec32us operator - (Vec32us const a, Vec32us const b) {
    return Vec32us(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator * : multiply element by element
static inline Vec32us operator * (Vec32us const a, Vec32us const b) {
    return Vec32us(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator / : divide. See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec32us operator >> (Vec32us const a, uint32_t b) {
    return Vec32us(a.get_low() >> b, a.get_high() >> b);
}
static inline Vec32us operator >> (Vec32us const a, int b) {
    return a >> uint32_t(b);
}

// vector operator >>= : shift right logical
static inline Vec32us & operator >>= (Vec32us & a, uint32_t b) {
    a = a >> b;
    return a;
} 

// vector operator >>= : shift right logical (signed b)
static inline Vec32us & operator >>= (Vec32us & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
}

// vector operator << : shift left all elements
static inline Vec32us operator << (Vec32us const a, uint32_t b) {
    return Vec32us(a.get_low() << b, a.get_high() << b);
}
static inline Vec32us operator << (Vec32us const a, int b) {
    return a << uint32_t(b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec32sb operator < (Vec32us const a, Vec32us const b) {
    return Vec32sb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec32sb operator > (Vec32us const a, Vec32us const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec32sb operator >= (Vec32us const a, Vec32us const b) {
    return Vec32sb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec32sb operator <= (Vec32us const a, Vec32us const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec32us operator & (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) & Vec32s(b));
}

// vector operator | : bitwise or
static inline Vec32us operator | (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) | Vec32s(b));
}

// vector operator ^ : bitwise xor
static inline Vec32us operator ^ (Vec32us const a, Vec32us const b) {
    return Vec32us(Vec32s(a) ^ Vec32s(b));
}

// vector operator ~ : bitwise not
static inline Vec32us operator ~ (Vec32us const a) {
    return Vec32us( ~ Vec32s(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec32us select (Vec32sb const s, Vec32us const a, Vec32us const b) {
    return Vec32us(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec32us if_add (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return Vec32us(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec32us if_sub (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return Vec32us(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec32us if_mul (Vec32sb const f, Vec32us const a, Vec32us const b) {
    return Vec32us(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// function add_saturated: add element by element, unsigned with saturation
static inline Vec32us add_saturated(Vec32us const a, Vec32us const b) {
    return Vec32us(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec32us sub_saturated(Vec32us const a, Vec32us const b) {
    return Vec32us(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec32us max(Vec32us const a, Vec32us const b) {
    return Vec32us(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec32us min(Vec32us const a, Vec32us const b) {
    return Vec32us(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}


/*****************************************************************************
*
*          Vector permute and blend functions
*
*****************************************************************************/

// Permute vector of 32 16-bit integers.
template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 >
    static inline Vec32s permute32(Vec32s const a) {
    return Vec32s(
        blend16<i0, i1, i2 ,i3 ,i4 ,i5 ,i6 ,i7, i8, i9, i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()), 
        blend16<i16,i17,i18,i19,i20,i21,i22,i23,i24,i25,i26,i27,i28,i29,i30,i31> (a.get_low(), a.get_high()));
}

template <int... i0 >
    static inline Vec32us permute32(Vec32us const a) {
    return Vec32us (permute32<i0...> (Vec32s(a)));
}

// Permute vector of 64 8-bit integers.
template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31,
    int i32, int i33, int i34, int i35, int i36, int i37, int i38, int i39,
    int i40, int i41, int i42, int i43, int i44, int i45, int i46, int i47,
    int i48, int i49, int i50, int i51, int i52, int i53, int i54, int i55,
    int i56, int i57, int i58, int i59, int i60, int i61, int i62, int i63 >
    static inline Vec64c permute64(Vec64c const a) {
    return Vec64c(
        blend32 <
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
        i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31        
        > (a.get_low(), a.get_high()),
        blend32 <
        i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,
        i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63
        > (a.get_low(), a.get_high()));
}

template <int... i0 >
static inline Vec64uc permute64(Vec64uc const a) {
    return Vec64uc (permute64<i0...> (Vec64c(a)));
} 

// Blend vector of 32 16-bit integers
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31 > 
    static inline Vec32s blend32(Vec32s const& a, Vec32s const& b) {
    Vec16s x0 = blend_half<Vec32s, i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15>(a, b);
    Vec16s x1 = blend_half<Vec32s, i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31>(a, b);
    return Vec32s(x0, x1);
}

template <int ... i0 > 
static inline Vec32us blend32(Vec32us const a, Vec32us const b) {
    return Vec32us(blend32<i0 ...> (Vec32s(a),Vec32s(b)));
}

// Blend vector of 64 8-bit integers
template <
    int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7,
    int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15,
    int i16, int i17, int i18, int i19, int i20, int i21, int i22, int i23,
    int i24, int i25, int i26, int i27, int i28, int i29, int i30, int i31,
    int i32, int i33, int i34, int i35, int i36, int i37, int i38, int i39,
    int i40, int i41, int i42, int i43, int i44, int i45, int i46, int i47,
    int i48, int i49, int i50, int i51, int i52, int i53, int i54, int i55,
    int i56, int i57, int i58, int i59, int i60, int i61, int i62, int i63 >
    static inline Vec64c blend64(Vec64c const a, Vec64c const b) {
    Vec32c x0 = blend_half < Vec64c, 
        i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15,
        i16, i17, i18, i19, i20, i21, i22, i23, i24, i25, i26, i27, i28, i29, i30, i31 > (a, b);
    Vec32c x1 = blend_half < Vec64c, 
        i32, i33, i34, i35, i36, i37, i38, i39, i40, i41, i42, i43, i44, i45, i46, i47,
        i48, i49, i50, i51, i52, i53, i54, i55, i56, i57, i58, i59, i60, i61, i62, i63 > (a, b);
    return Vec64c(x0, x1);
}

template <int ... i0 >
static inline Vec64uc blend64(Vec64uc const a, Vec64uc const b) {
    return Vec64uc(blend64 <i0 ...>(Vec64c(a), Vec64c(b)));
}


/*****************************************************************************
*
*          Vector lookup functions
*
******************************************************************************
*
* These functions use vector elements as indexes into a table.
* The table is given as one or more vectors
*
*****************************************************************************/

// lookup in table of 64 int8_t values
static inline Vec64c lookup64(Vec64c const index, Vec64c const table1) {
    int8_t table[64], result[64];
    table1.store(table);
    for (int i=0; i<64; i++) result[i] = table[index[i] & 63];
    return Vec64c().load(result);
}

// lookup in table of 128 int8_t values
static inline Vec64c lookup128(Vec64c const index, Vec64c const table1, Vec64c const table2) {
    int8_t table[128], result[64];
    table1.store(table);  table2.store(table+64);
    for (int i=0; i<64; i++) result[i] = table[index[i] & 127];
    return Vec64c().load(result);
}

// lookup in table of 256 int8_t values.
// The complete table of all possible 256 byte values is contained in four vectors
// The index is treated as unsigned
static inline Vec64c lookup256(Vec64c const index, Vec64c const table1, Vec64c const table2, Vec64c const table3, Vec64c const table4) {
    int8_t table[256], result[64];
    table1.store(table);  table2.store(table+64);  table3.store(table+128);  table4.store(table+192);
    for (int i=0; i<64; i++) result[i] = table[index[i] & 255];
    return Vec64c().load(result);
}

// lookup in table of 32 values
static inline Vec32s lookup32(Vec32s const index, Vec32s const table1) {
    int16_t table[32], result[32];
    table1.store(table);
    for (int i=0; i<32; i++) result[i] = table[index[i] & 31];
    return Vec32s().load(result);
}

// lookup in table of 64 values
static inline Vec32s lookup64(Vec32s const index, Vec32s const table1, Vec32s const table2) {
    int16_t table[64], result[32];
    table1.store(table);  table2.store(table+32);
    for (int i=0; i<32; i++) result[i] = table[index[i] & 63];
    return Vec32s().load(result);
}

// lookup in table of 128 values
static inline Vec32s lookup128(Vec32s const index, Vec32s const table1, Vec32s const table2, Vec32s const table3, Vec32s const table4) {
    int16_t table[128], result[32];
    table1.store(table);  table2.store(table+32);  table3.store(table+64);  table4.store(table+96);
    for (int i=0; i<32; i++) result[i] = table[index[i] & 127];
    return Vec32s().load(result);
}


/*****************************************************************************
*
*          Byte shifts
*
*****************************************************************************/

// Function shift_bytes_up: shift whole vector left by b bytes.
template <unsigned int b>
static inline Vec64c shift_bytes_up(Vec64c const a) {
    int8_t dat[128];
    if (b < 64) {
        Vec64c(0).store(dat);
        a.store(dat+b);
        return Vec64c().load(dat);
    }
    else return 0;
}

// Function shift_bytes_down: shift whole vector right by b bytes
template <unsigned int b>
static inline Vec64c shift_bytes_down(Vec64c const a) {
    int8_t dat[128];
    if (b < 64) {
        a.store(dat);
        Vec64c(0).store(dat+64);
        return Vec64c().load(dat+b);
    }
    else return 0;
} 


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 8-bit integers to 16-bit integers, signed and unsigned

// Function extend_low : extends the low 32 elements to 16 bits with sign extension
static inline Vec32s extend_low (Vec64c const a) {
    return Vec32s(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 16 elements to 16 bits with sign extension
static inline Vec32s extend_high (Vec64c const a) {
    return Vec32s(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 16 elements to 16 bits with zero extension
static inline Vec32us extend_low (Vec64uc const a) {
    return Vec32us(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 19 elements to 16 bits with zero extension
static inline Vec32us extend_high (Vec64uc const a) {
    return Vec32us(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Extend 16-bit integers to 32-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 32 bits with sign extension
static inline Vec16i extend_low (Vec32s const a) {
    return Vec16i(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with sign extension
static inline Vec16i extend_high (Vec32s const a) {
    return Vec16i(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 8 elements to 32 bits with zero extension
static inline Vec16ui extend_low (Vec32us const a) {
    return Vec16ui(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 32 bits with zero extension
static inline Vec16ui extend_high (Vec32us const a) {
    return Vec16ui(extend_low(a.get_high()), extend_high(a.get_high()));
}


// Compress 16-bit integers to 8-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec64c compress (Vec32s const low, Vec32s const high) {
    return Vec64c(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Signed, with saturation
static inline Vec64c compress_saturated (Vec32s const low, Vec32s const high) {
    return Vec64c(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 16-bit integers to one vector of 8-bit integers
// Unsigned, overflow wraps around
static inline Vec64uc compress (Vec32us const low, Vec32us const high) {
    return  Vec64uc(compress((Vec32s)low, (Vec32s)high));
}

// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Unsigned, with saturation
static inline Vec64uc compress_saturated (Vec32us const low, Vec32us const high) {
    return Vec64uc(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Compress 32-bit integers to 16-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec32s compress (Vec16i const low, Vec16i const high) {
    return Vec32s(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Signed with saturation
static inline Vec32s compress_saturated (Vec16i const low, Vec16i const high) {
    return Vec32s(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Overflow wraps around
static inline Vec32us compress (Vec16ui const low, Vec16ui const high) {
    return Vec32us (compress((Vec16i)low, (Vec16i)high));
}

// Function compress : packs two vectors of 32-bit integers into one vector of 16-bit integers
// Unsigned, with saturation
static inline Vec32us compress_saturated (Vec16ui const low, Vec16ui const high) {
    return Vec32us(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}


/*****************************************************************************
*
*          Integer division operators
*
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector of 32 16-bit signed integers
static inline Vec32s operator / (Vec32s const a, Divisor_s const d) {
    return Vec32s(a.get_low() / d, a.get_high() / d);
}

// vector of 16 16-bit unsigned integers
static inline Vec32us operator / (Vec32us const a, Divisor_us const d) {
    return Vec32us(a.get_low() / d, a.get_high() / d);
}

// vector of 32 8-bit signed integers
static inline Vec64c operator / (Vec64c const a, Divisor_s const d) {
    return Vec64c(a.get_low() / d, a.get_high() / d);
}

// vector of 32 8-bit unsigned integers
static inline Vec64uc operator / (Vec64uc const a, Divisor_us const d) {
    return Vec64uc(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec32s & operator /= (Vec32s & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec32us & operator /= (Vec32us & a, Divisor_us const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec64c & operator /= (Vec64c & a, Divisor_s const d) {
    a = a / d;
    return a;
}

// vector operator /= : divide
static inline Vec64uc & operator /= (Vec64uc & a, Divisor_us const d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/


// Divide Vec32s by compile-time constant 
template <int d>
static inline Vec32s divide_by_i(Vec32s const a) {
    return Vec32s(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
}

// define Vec32s a / const_int(d)
template <int d>
static inline Vec32s operator / (Vec32s const a, Const_int_t<d>) {
    return Vec32s(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
}

// define Vec32s a / const_uint(d)
template <uint32_t d>
static inline Vec32s operator / (Vec32s const a, Const_uint_t<d>) {
    return Vec32s(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32s & operator /= (Vec32s & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32s & operator /= (Vec32s & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// Divide Vec32us by compile-time constant
template <uint32_t d>
static inline Vec32us divide_by_ui(Vec32us const a) {
    return Vec32us( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
}

// define Vec32us a / const_uint(d)
template <uint32_t d>
static inline Vec32us operator / (Vec32us const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec32us a / const_int(d)
template <int d>
static inline Vec32us operator / (Vec32us const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec32us & operator /= (Vec32us & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec32us & operator /= (Vec32us & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// define Vec64c a / const_int(d)
template <int d>
static inline Vec64c operator / (Vec64c const a, Const_int_t<d> b) {
    return Vec64c( a.get_low() / b, a.get_high() / b);
}

// define Vec64c a / const_uint(d)
template <uint32_t d>
static inline Vec64c operator / (Vec64c const a, Const_uint_t<d> b) {
    return Vec64c( a.get_low() / b, a.get_high() / b);
}

// vector operator /= : divide
template <int32_t d>
static inline Vec64c & operator /= (Vec64c & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}
// vector operator /= : divide
template <uint32_t d>
static inline Vec64c & operator /= (Vec64c & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// define Vec64uc a / const_uint(d)
template <uint32_t d>
static inline Vec64uc operator / (Vec64uc const a, Const_uint_t<d> b) {
    return Vec64uc( a.get_low() / b, a.get_high() / b);
}

// define Vec64uc a / const_int(d)
template <int d>
static inline Vec64uc operator / (Vec64uc const a, Const_int_t<d> b) {
    return Vec64uc( a.get_low() / b, a.get_high() / b);
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec64uc & operator /= (Vec64uc & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec64uc & operator /= (Vec64uc & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI512S_H
