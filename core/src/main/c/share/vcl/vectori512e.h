/****************************  vectori512e.h   *******************************
* Author:        Agner Fog
* Date created:  2014-07-23
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector classes
* Description:
* Header file defining 512-bit integer vector classes for 32 and 64 bit integers.
* Emulated for processors without AVX512 instruction set.
*
* Instructions: see vcl_manual.pdf
*
* The following vector classes are defined here:
* Vec16i    Vector of  16  32-bit signed   integers
* Vec16ui   Vector of  16  32-bit unsigned integers
* Vec16ib   Vector of  16  Booleans for use with Vec16i and Vec16ui
* Vec8q     Vector of   8  64-bit signed   integers
* Vec8uq    Vector of   8  64-bit unsigned integers
* Vec8qb    Vector of   8  Booleans for use with Vec8q and Vec8uq
*
* Each vector object is represented internally in the CPU as two 256-bit registers.
* This header file defines operators and functions for these vectors.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
*****************************************************************************/

#ifndef VECTORI512E_H
#define VECTORI512E_H

#ifndef VECTORCLASS_H
#include "vectorclass.h"
#endif

#if VECTORCLASS_H < 20100
#error Incompatible versions of vector class library mixed
#endif

// check combination of header files
#if defined (VECTORI512_H)
#error Two different versions of vectori512.h included
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


/*****************************************************************************
*
*          Vector of 512 bits
*
*****************************************************************************/

class Vec512b {
protected:
    Vec256b z0;                         // low half
    Vec256b z1;                         // high half
public:
    // Default constructor:
    Vec512b() {
    }
    // Constructor to build from two Vec256b:
    Vec512b(Vec256b const a0, Vec256b const a1) {
        z0 = a0;  z1 = a1;
    }
    // Member function to load from array (unaligned)
    Vec512b & load(void const * p) {
        z0 = Vec8i().load(p);
        z1 = Vec8i().load((int32_t const*)p+8);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec512b & load_a(void const * p) {
        z0 = Vec8i().load_a(p);
        z1 = Vec8i().load_a((int32_t const*)p+8);
        return *this;
    }
    // Member function to store into array (unaligned)
    void store(void * p) const {
        Vec8i(z0).store(p);
        Vec8i(z1).store((int32_t*)p+8);
    }
    // Member function to store into array (unaligned) with non-temporal memory hint
    void store_nt(void * p) const {
        Vec8i(z0).store_nt(p);
        Vec8i(z1).store_nt((int32_t*)p+8);
    }
    // Required alignment for store_nt call in bytes
    static constexpr int store_nt_alignment() {
        return Vec8i::store_nt_alignment();
    }
    // Member function to store into array, aligned by 64
    void store_a(void * p) const {
        Vec8i(z0).store_a(p);
        Vec8i(z1).store_a((int32_t*)p+8);
    }
    Vec256b get_low() const {            // get low half
        return z0;
    }
    Vec256b get_high() const {           // get high half
        return z1;
    } 
    static constexpr int size() {
        return 512;
    }
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec512b operator & (Vec512b const a, Vec512b const b) {
    return Vec512b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec512b operator && (Vec512b const a, Vec512b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec512b operator | (Vec512b const a, Vec512b const b) {
    return Vec512b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec512b operator || (Vec512b const a, Vec512b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec512b operator ^ (Vec512b const a, Vec512b const b) {
    return Vec512b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec512b operator ~ (Vec512b const a) {
    return Vec512b(~a.get_low(), ~a.get_high());
}

// vector operator &= : bitwise and
static inline Vec512b & operator &= (Vec512b & a, Vec512b const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec512b & operator |= (Vec512b & a, Vec512b const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec512b & operator ^= (Vec512b & a, Vec512b const b) {
    a = a ^ b;
    return a;
}

// Define functions for this class

// function andnot: a & ~ b
static inline Vec512b andnot (Vec512b const a, Vec512b const b) {
    return Vec512b(andnot(a.get_low(), b.get_low()), andnot(a.get_high(), b.get_high()));
}
 

/*****************************************************************************
*
*          Boolean vector (broad) base classes
*
*****************************************************************************/

class Vec16b : public Vec512b {
public:
    // Default constructor:
    Vec16b () {
    }
    // Constructor to build from all elements:
    Vec16b(bool b0, bool b1, bool b2, bool b3, bool b4, bool b5, bool b6, bool b7, 
    bool b8, bool b9, bool b10, bool b11, bool b12, bool b13, bool b14, bool b15) {
        *this = Vec512b(Vec8i(-(int)b0, -(int)b1, -(int)b2, -(int)b3, -(int)b4, -(int)b5, -(int)b6, -(int)b7), Vec8i(-(int)b8, -(int)b9, -(int)b10, -(int)b11, -(int)b12, -(int)b13, -(int)b14, -(int)b15));
    }
    // Constructor to convert from type Vec512b
    Vec16b (Vec512b const & x) {  // gcc requires const & here
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to make from two halves
    Vec16b (Vec8ib const x0, Vec8ib const x1) {
        z0 = x0;
        z1 = x1;
    }        
    // Constructor to make from two halves
    Vec16b (Vec8i const x0, Vec8i const x1) {
        z0 = x0;
        z1 = x1;
    }        
    // Constructor to broadcast single value:
    Vec16b(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec16b & operator = (bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
        return *this;
    }
    // split into two halves
    Vec8ib get_low() const {
        return Vec8ib(z0);
    }
    Vec8ib get_high() const {
        return Vec8ib(z1);
    }
    /*
    // Assignment operator to convert from type Vec512b
    Vec16b & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    } */
    // Member function to change a single element in vector
    // Note: This function is inefficient. Use load function if changing more than one element
    Vec16b const insert(int index, bool value) {
        if ((uint32_t)index < 8) {
            z0 = Vec8ib(z0).insert(index, value);
        }
        else {
            z1 = Vec8ib(z1).insert(index-8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    bool extract(int index) const {
        if ((uint32_t)index < 8) {
            return Vec8ib(z0).extract(index);
        }
        else {
            return Vec8ib(z1).extract(index-8);
        }
    }
    // Extract a single element. Operator [] can only read an element, not write.
    bool operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 3;
    }
    // Prevent constructing from int, etc. because of ambiguity
    Vec16b(int b) = delete;
    // Prevent assigning int because of ambiguity
    Vec16b & operator = (int x) = delete;
};

// Define operators for this class

// vector operator & : bitwise and
static inline Vec16b operator & (Vec16b const a, Vec16b const b) {
    return Vec16b(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}
static inline Vec16b operator && (Vec16b const a, Vec16b const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16b operator | (Vec16b const a, Vec16b const b) {
    return Vec16b(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}
static inline Vec16b operator || (Vec16b const a, Vec16b const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16b operator ^ (Vec16b const a, Vec16b const b) {
    return Vec16b(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ~ : bitwise not
static inline Vec16b operator ~ (Vec16b const a) {
    return Vec16b(~(a.get_low()), ~(a.get_high()));
}

// vector operator ! : element not
static inline Vec16b operator ! (Vec16b const a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16b & operator &= (Vec16b & a, Vec16b const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16b & operator |= (Vec16b & a, Vec16b const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16b & operator ^= (Vec16b & a, Vec16b const b) {
    a = a ^ b;
    return a;
}

/*****************************************************************************
*
*          Functions for boolean vectors
*
*****************************************************************************/

// function andnot: a & ~ b
static inline Vec16b andnot (Vec16b const a, Vec16b const b) {
    return Vec16b(Vec8ib(andnot(a.get_low(),b.get_low())), Vec8ib(andnot(a.get_high(),b.get_high())));
}

// horizontal_and. Returns true if all bits are 1
static inline bool horizontal_and (Vec16b const a) {
    return  horizontal_and(a.get_low() & a.get_high());
}

// horizontal_or. Returns true if at least one bit is 1
static inline bool horizontal_or (Vec16b const a) {
    return  horizontal_or(a.get_low() | a.get_high());
}


/*****************************************************************************
*
*          Vec16ib: Vector of 16 Booleans for use with Vec16i and Vec16ui
*
*****************************************************************************/

class Vec16ib : public Vec16b {
public:
    // Default constructor:
    Vec16ib () {
    }
    /*
    Vec16ib (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    } */   
    // Constructor to build from all elements:
    Vec16ib(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7,
        bool x8, bool x9, bool x10, bool x11, bool x12, bool x13, bool x14, bool x15) {
        z0 = Vec8ib(x0, x1, x2, x3, x4, x5, x6, x7);
        z1 = Vec8ib(x8, x9, x10, x11, x12, x13, x14, x15);
    }
    // Constructor to convert from type Vec512b
    Vec16ib (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Construct from two halves
    Vec16ib (Vec8ib const x0, Vec8ib const x1) {
        z0 = x0;
        z1 = x1;
    }
    // Assignment operator to convert from type Vec512b
    Vec16ib & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Constructor to broadcast scalar value:
    Vec16ib(bool b) : Vec16b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec16ib & operator = (bool b) {
        *this = Vec16b(b);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec16ib & load_bits(uint16_t a) {
        z0 = Vec8ib().load_bits(uint8_t(a));
        z1 = Vec8ib().load_bits(uint8_t(a>>8));
        return *this;
    } 
    // Prevent constructing from int, etc.
    Vec16ib(int b) = delete;
    Vec16ib & operator = (int x) = delete;
};

// Define operators for Vec16ib

// vector operator & : bitwise and
static inline Vec16ib operator & (Vec16ib const a, Vec16ib const b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec16ib operator && (Vec16ib const a, Vec16ib const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec16ib operator | (Vec16ib const a, Vec16ib const b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec16ib operator || (Vec16ib const a, Vec16ib const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec16ib operator ^ (Vec16ib const a, Vec16ib const b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator == : xnor
static inline Vec16ib operator == (Vec16ib const a, Vec16ib const b) {
    return Vec16ib(Vec16b(a) ^ Vec16b(~b));
}

// vector operator != : xor
static inline Vec16ib operator != (Vec16ib const a, Vec16ib const b) {
    return Vec16ib(a ^ b);
}

// vector operator ~ : bitwise not
static inline Vec16ib operator ~ (Vec16ib const a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec16ib operator ! (Vec16ib const a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec16ib & operator &= (Vec16ib & a, Vec16ib const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec16ib & operator |= (Vec16ib & a, Vec16ib const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec16ib & operator ^= (Vec16ib & a, Vec16ib const b) {
    a = a ^ b;
    return a;
}

// vector function andnot
static inline Vec16ib andnot (Vec16ib const a, Vec16ib const b) {
    return Vec16ib(andnot(Vec16b(a), Vec16b(b)));
}


/*****************************************************************************
*
*          Vec8b: Base class vector of 8 Booleans
*
*****************************************************************************/

class Vec8b : public Vec16b {
public:
    // Default constructor:
    Vec8b () {
    }
    /*
    Vec8b (Vec16b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    } */
    // Constructor to convert from type Vec512b
    Vec8b (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // construct from two halves
    Vec8b (Vec4qb const x0, Vec4qb const x1) {
        z0 = x0;
        z1 = x1;
    }
    // Constructor to broadcast single value:
    Vec8b(bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
    }
    // Assignment operator to broadcast scalar value:
    Vec8b & operator = (bool b) {
        z0 = z1 = Vec8i(-int32_t(b));
        return *this;
    }
    // split into two halves
    Vec4qb get_low() const {
        return Vec4qb(z0);
    }
    Vec4qb get_high() const {
        return Vec4qb(z1);
    }
    /*
    // Assignment operator to convert from type Vec512b
    Vec8b & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    } */
    // Member function to change a single element in vector
    Vec8b const insert(int index, bool value) {
        if ((uint32_t)index < 4) {
            z0 = Vec4qb(z0).insert(index, value);
        }
        else {
            z1 = Vec4qb(z1).insert(index-4, value);
        }
        return *this;
    }
    bool extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4qb(Vec4q(z0)).extract(index);
        }
        else {
            return Vec4qb(Vec4q(z1)).extract(index-4);
        }
    }
    bool operator [] (int index) const {
        return extract(index);
    }
    static constexpr int size() {
        return 8;
    }
    // Prevent constructing from int, etc. because of ambiguity
    Vec8b(int b) = delete;
    // Prevent assigning int because of ambiguity
    Vec8b & operator = (int x) = delete;
};


/*****************************************************************************
*
*          Vec8qb: Vector of 8 Booleans for use with Vec8q and Vec8qu
*
*****************************************************************************/

class Vec8qb : public Vec8b {
public:
    // Default constructor:
    Vec8qb () {
    }
    Vec8qb (Vec16b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to build from all elements:
    Vec8qb(bool x0, bool x1, bool x2, bool x3, bool x4, bool x5, bool x6, bool x7) {
        z0 = Vec4qb(x0, x1, x2, x3);
        z1 = Vec4qb(x4, x5, x6, x7);
    }
    // Constructor to convert from type Vec512b
    Vec8qb (Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // construct from two halves
    Vec8qb (Vec4qb const x0, Vec4qb const x1) {
        z0 = x0;
        z1 = x1;
    }
    // Assignment operator to convert from type Vec512b
    Vec8qb & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Constructor to broadcast single value:
    Vec8qb(bool b) : Vec8b(b) {
    }
    // Assignment operator to broadcast scalar value:
    Vec8qb & operator = (bool b) {
        *this = Vec8b(b);
        return *this;
    }
    // Member function to change a bitfield to a boolean vector
    Vec8qb & load_bits(uint8_t a) {
        z0 = Vec4qb().load_bits(a);
        z1 = Vec4qb().load_bits(uint8_t(a>>4u));
        return *this;
    }
    // Prevent constructing from int, etc. because of ambiguity
    Vec8qb(int b) = delete;
    // Prevent assigning int because of ambiguity
    Vec8qb & operator = (int x) = delete;
};

// Define operators for Vec8qb

// vector operator & : bitwise and
static inline Vec8qb operator & (Vec8qb const a, Vec8qb const b) {
    return Vec16b(a) & Vec16b(b);
}
static inline Vec8qb operator && (Vec8qb const a, Vec8qb const b) {
    return a & b;
}

// vector operator | : bitwise or
static inline Vec8qb operator | (Vec8qb const a, Vec8qb const b) {
    return Vec16b(a) | Vec16b(b);
}
static inline Vec8qb operator || (Vec8qb const a, Vec8qb const b) {
    return a | b;
}

// vector operator ^ : bitwise xor
static inline Vec8qb operator ^ (Vec8qb const a, Vec8qb const b) {
    return Vec16b(a) ^ Vec16b(b);
}

// vector operator == : xnor
static inline Vec8qb operator == (Vec8qb const a, Vec8qb const b) {
    return Vec8qb(Vec16b(a) ^ Vec16b(~b));
}

// vector operator != : xor
static inline Vec8qb operator != (Vec8qb const a, Vec8qb const b) {
    return Vec8qb(a ^ b);
} 

// vector operator ~ : bitwise not
static inline Vec8qb operator ~ (Vec8qb const a) {
    return ~Vec16b(a);
}

// vector operator ! : element not
static inline Vec8qb operator ! (Vec8qb const a) {
    return ~a;
}

// vector operator &= : bitwise and
static inline Vec8qb & operator &= (Vec8qb & a, Vec8qb const b) {
    a = a & b;
    return a;
}

// vector operator |= : bitwise or
static inline Vec8qb & operator |= (Vec8qb & a, Vec8qb const b) {
    a = a | b;
    return a;
}

// vector operator ^= : bitwise xor
static inline Vec8qb & operator ^= (Vec8qb & a, Vec8qb const b) {
    a = a ^ b;
    return a;
}

// vector function andnot
static inline Vec8qb andnot (Vec8qb const a, Vec8qb const b) {
    return Vec8qb(andnot(Vec16b(a), Vec16b(b)));
}


/*****************************************************************************
*
*          Vector of 16 32-bit signed integers
*
*****************************************************************************/

class Vec16i: public Vec512b {
public:
    // Default constructor:
    Vec16i() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec16i(int i) {
        z0 = z1 = Vec8i(i);
    }
    // Constructor to build from all elements:
    Vec16i(int32_t i0, int32_t i1, int32_t i2, int32_t i3, int32_t i4, int32_t i5, int32_t i6, int32_t i7,
    int32_t i8, int32_t i9, int32_t i10, int32_t i11, int32_t i12, int32_t i13, int32_t i14, int32_t i15) {
        z0 = Vec8i(i0, i1, i2, i3, i4, i5, i6, i7);
        z1 = Vec8i(i8, i9, i10, i11, i12, i13, i14, i15);
    }
    // Constructor to build from two Vec8i:
    Vec16i(Vec8i const a0, Vec8i const a1) {
        *this = Vec512b(a0, a1);
    }
    // Constructor to convert from type Vec512b
    Vec16i(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    } 
    // Assignment operator to convert from type Vec512b
    Vec16i & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16i & load(void const * p) {
        Vec512b::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16i & load_a(void const * p) {
        Vec512b::load_a(p);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec16i & load_partial(int n, void const * p) {
        if (n < 8) {
            z0 = Vec8i().load_partial(n, p);
            z1 = Vec8i(0);
        }
        else {
            z0 = Vec8i().load(p);
            z1 = Vec8i().load_partial(n - 8, (int32_t const*)p + 8);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n < 8) {
            Vec8i(get_low()).store_partial(n, p);
        }
        else {
            Vec8i(get_low()).store(p);
            Vec8i(get_high()).store_partial(n - 8, (int32_t *)p + 8);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec16i & cutoff(int n) {
        if (n < 8) {
            z0 = Vec8i(z0).cutoff(n);
            z1 = Vec8i(0);
        }
        else {
            z1 = Vec8i(z1).cutoff(n - 8);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec16i const insert(int index, int32_t value) {
        if ((uint32_t)index < 8) {
            z0 = Vec8i(z0).insert(index, value);
        }
        else {
            z1 = Vec8i(z1).insert(index - 8, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int32_t extract(int index) const {
        if ((uint32_t)index < 8) {
            return Vec8i(z0).extract(index);
        }
        else {
            return Vec8i(z1).extract(index - 8);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec8i:
    Vec8i get_low() const {
        return Vec8i(z0);
    }
    Vec8i get_high() const {
        return Vec8i(z1);
    }
    static constexpr int size() {
        return 16;
    }
    static constexpr int elementtype() {
        return 8;
    }
};


// Define operators for Vec16i

// vector operator + : add element by element
static inline Vec16i operator + (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator += : add
static inline Vec16i & operator += (Vec16i & a, Vec16i const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec16i operator ++ (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec16i & operator ++ (Vec16i & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec16i operator - (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec16i operator - (Vec16i const a) {
    return Vec16i(-a.get_low(), -a.get_high());
}

// vector operator -= : subtract
static inline Vec16i & operator -= (Vec16i & a, Vec16i const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec16i operator -- (Vec16i & a, int) {
    Vec16i a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec16i & operator -- (Vec16i & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec16i operator * (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec16i & operator *= (Vec16i & a, Vec16i const b) {
    a = a * b;
    return a;
}

// vector operator / : divide all elements by same integer. See bottom of file 

// vector operator << : shift left
static inline Vec16i operator << (Vec16i const a, int32_t b) {
    return Vec16i(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec16i & operator <<= (Vec16i & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec16i operator >> (Vec16i const a, int32_t b) {
    return Vec16i(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec16i & operator >>= (Vec16i & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec16ib operator == (Vec16i const a, Vec16i const b) {
    return Vec16ib(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec16ib operator != (Vec16i const a, Vec16i const b) {
    return Vec16ib(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator > : returns true for elements for which a > b
static inline Vec16ib operator > (Vec16i const a, Vec16i const b) {
    return Vec16ib(a.get_low() > b.get_low(), a.get_high() > b.get_high());
}

// vector operator < : returns true for elements for which a < b
static inline Vec16ib operator < (Vec16i const a, Vec16i const b) {
    return b > a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec16ib operator >= (Vec16i const a, Vec16i const b) {
    return Vec16ib(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec16ib operator <= (Vec16i const a, Vec16i const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16i operator & (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec16i & operator &= (Vec16i & a, Vec16i const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec16i operator | (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec16i & operator |= (Vec16i & a, Vec16i const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec16i operator ^ (Vec16i const a, Vec16i const b) {
    return Vec16i(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}

// vector operator ^= : bitwise xor
static inline Vec16i & operator ^= (Vec16i & a, Vec16i const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec16i operator ~ (Vec16i const a) {
    return Vec16i(~(a.get_low()), ~(a.get_high()));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16i select (Vec16ib const s, Vec16i const a, Vec16i const b) {
    return Vec16i(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16i if_add (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return Vec16i(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec16i if_sub (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return Vec16i(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec16i if_mul (Vec16ib const f, Vec16i const a, Vec16i const b) {
    return Vec16i(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int32_t horizontal_add (Vec16i const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// function add_saturated: add element by element, signed with saturation
static inline Vec16i add_saturated(Vec16i const a, Vec16i const b) {
    return Vec16i(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, signed with saturation
static inline Vec16i sub_saturated(Vec16i const a, Vec16i const b) {
    return Vec16i(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16i max(Vec16i const a, Vec16i const b) {
    return Vec16i(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16i min(Vec16i const a, Vec16i const b) {
    return Vec16i(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec16i abs(Vec16i const a) {
    return Vec16i(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec16i abs_saturated(Vec16i const a) {
    return Vec16i(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec16i rotate_left(Vec16i const a, int b) {
    return Vec16i(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
}


/*****************************************************************************
*
*          Vector of 16 32-bit unsigned integers
*
*****************************************************************************/

class Vec16ui : public Vec16i {
public:
    // Default constructor:
    Vec16ui() {
    };
    // Constructor to broadcast the same value into all elements:
    Vec16ui(uint32_t i) {
        z0 = z1 = Vec8ui(i);
    };
    // Constructor to build from all elements:
    Vec16ui(uint32_t i0, uint32_t i1, uint32_t i2, uint32_t i3, uint32_t i4, uint32_t i5, uint32_t i6, uint32_t i7,
    uint32_t i8, uint32_t i9, uint32_t i10, uint32_t i11, uint32_t i12, uint32_t i13, uint32_t i14, uint32_t i15) {
        z0 = Vec8ui(i0, i1, i2, i3, i4, i5, i6, i7);
        z1 = Vec8ui(i8, i9, i10, i11, i12, i13, i14, i15);
    };
    // Constructor to build from two Vec8ui:
    Vec16ui(Vec8ui const a0, Vec8ui const a1) {
        z0 = a0;
        z1 = a1;
    }
    // Constructor to convert from type Vec512b
    Vec16ui(Vec512b const & x) {
        *this = x;
    }
    // Assignment operator to convert from type Vec512b
    Vec16ui & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec16ui & load(void const * p) {
        Vec16i::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec16ui & load_a(void const * p) {
        Vec16i::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec16ui const insert(int index, uint32_t value) {
        Vec16i::insert(index, (int32_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint32_t extract(int index) const {
        return (uint32_t)Vec16i::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint32_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec4ui:
    Vec8ui get_low() const {
        return Vec8ui(Vec16i::get_low());
    }
    Vec8ui get_high() const {
        return Vec8ui(Vec16i::get_high());
    }
    static constexpr int elementtype() {
        return 9;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec16ui operator + (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) + Vec16i(b));
}

// vector operator - : subtract
static inline Vec16ui operator - (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) - Vec16i(b));
}

// vector operator * : multiply
static inline Vec16ui operator * (Vec16ui const a, Vec16ui const b) {
    return Vec16ui (Vec16i(a) * Vec16i(b));
}

// vector operator / : divide. See bottom of file

// vector operator >> : shift right logical all elements
static inline Vec16ui operator >> (Vec16ui const a, uint32_t b) {
    return Vec16ui(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >> : shift right logical all elements
static inline Vec16ui operator >> (Vec16ui const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator >>= : shift right logical
static inline Vec16ui & operator >>= (Vec16ui & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
} 

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const a, uint32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec16ui operator << (Vec16ui const a, int32_t b) {
    return Vec16ui ((Vec16i)a << (int32_t)b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec16ib operator < (Vec16ui const a, Vec16ui const b) {
    return Vec16ib(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec16ib operator > (Vec16ui const a, Vec16ui const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec16ib operator >= (Vec16ui const a, Vec16ui const b) {
    return Vec16ib(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}            

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec16ib operator <= (Vec16ui const a, Vec16ui const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec16ui operator & (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) & Vec16i(b));
}

// vector operator | : bitwise or
static inline Vec16ui operator | (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) | Vec16i(b));
}

// vector operator ^ : bitwise xor
static inline Vec16ui operator ^ (Vec16ui const a, Vec16ui const b) {
    return Vec16ui(Vec16i(a) ^ Vec16i(b));
}

// vector operator ~ : bitwise not
static inline Vec16ui operator ~ (Vec16ui const a) {
    return Vec16ui( ~ Vec16i(a));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 16; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec16ui select (Vec16ib const s, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(select(s, Vec16i(a), Vec16i(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec16ui if_add (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_add(f, Vec16i(a), Vec16i(b)));
}

// Conditional subtract
static inline Vec16ui if_sub (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_sub(f, Vec16i(a), Vec16i(b)));
}

// Conditional multiply
static inline Vec16ui if_mul (Vec16ib const f, Vec16ui const a, Vec16ui const b) {
    return Vec16ui(if_mul(f, Vec16i(a), Vec16i(b)));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint32_t horizontal_add (Vec16ui const a) {
    return (uint32_t)horizontal_add((Vec16i)a);
}

// horizontal_add_x: Horizontal add extended: Calculates the sum of all vector elements. Defined later in this file

// function add_saturated: add element by element, unsigned with saturation
static inline Vec16ui add_saturated(Vec16ui const a, Vec16ui const b) {
    return Vec16ui(add_saturated(a.get_low(), b.get_low()), add_saturated(a.get_high(), b.get_high()));
}

// function sub_saturated: subtract element by element, unsigned with saturation
static inline Vec16ui sub_saturated(Vec16ui const a, Vec16ui const b) {
    return Vec16ui(sub_saturated(a.get_low(), b.get_low()), sub_saturated(a.get_high(), b.get_high()));
}

// function max: a > b ? a : b
static inline Vec16ui max(Vec16ui const a, Vec16ui const b) {
    return Vec16ui(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec16ui min(Vec16ui const a, Vec16ui const b) {
    return Vec16ui(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}


/*****************************************************************************
*
*          Vector of 8 64-bit signed integers
*
*****************************************************************************/

class Vec8q : public Vec512b {
public:
    // Default constructor:
    Vec8q() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8q(int64_t i) {
        z0 = z1 = Vec4q(i);
    }
    // Constructor to build from all elements:
    Vec8q(int64_t i0, int64_t i1, int64_t i2, int64_t i3, int64_t i4, int64_t i5, int64_t i6, int64_t i7) {
        z0 = Vec4q(i0, i1, i2, i3);
        z1 = Vec4q(i4, i5, i6, i7);
    }
    // Constructor to build from two Vec4q:
    Vec8q(Vec4q const a0, Vec4q const a1) {
        z0 = a0;
        z1 = a1;
    }
    // Constructor to convert from type Vec512b
    Vec8q(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Assignment operator to convert from type Vec512b
    Vec8q & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8q & load(void const * p) {
        z0 = Vec4q().load(p);
        z1 = Vec4q().load((int64_t const*)p+4);
        return *this;
    }
    // Member function to load from array, aligned by 64
    Vec8q & load_a(void const * p) {
        z0 = Vec4q().load_a(p);
        z1 = Vec4q().load_a((int64_t const*)p+4);
        return *this;
    }
    // Partial load. Load n elements and set the rest to 0
    Vec8q & load_partial(int n, void const * p) {
        if (n < 4) {
            z0 = Vec4q().load_partial(n, p);
            z1 = Vec4q(0);
        }
        else {
            z0 = Vec4q().load(p);
            z1 = Vec4q().load_partial(n - 4, (int64_t const*)p + 4);
        }
        return *this;
    }
    // Partial store. Store n elements
    void store_partial(int n, void * p) const {
        if (n < 4) {
            Vec4q(get_low()).store_partial(n, p);
        }
        else {
            Vec4q(get_low()).store(p);
            Vec4q(get_high()).store_partial(n - 4, (int64_t *)p + 4);
        }
    }
    // cut off vector to n elements. The last 8-n elements are set to zero
    Vec8q & cutoff(int n) {
        if (n < 4) {
            z0 = Vec4q(z0).cutoff(n);
            z1 = Vec4q(0);
        }
        else {
            z1 = Vec4q(z1).cutoff(n - 4);
        }
        return *this;
    }
    // Member function to change a single element in vector
    Vec8q const insert(int index, int64_t value) {
        if ((uint32_t)index < 4) {
            z0 = Vec4q(z0).insert(index, value);
        }
        else {
            z1 = Vec4q(z1).insert(index-4, value);
        }
        return *this;
    }
    // Member function extract a single element from vector
    int64_t extract(int index) const {
        if ((uint32_t)index < 4) {
            return Vec4q(z0).extract(index);
        }
        else {
            return Vec4q(z1).extract(index - 4);
        }
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    int64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2q:
    Vec4q get_low() const {
        return Vec4q(z0);
    }
    Vec4q get_high() const {
        return Vec4q(z1);
    }
    static constexpr int size() {
        return 8;
    }
    static constexpr int elementtype() {
        return 10;
    }
};

// Define operators for Vec8q

// vector operator + : add element by element
static inline Vec8q operator + (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() + b.get_low(), a.get_high() + b.get_high());
}

// vector operator += : add
static inline Vec8q & operator += (Vec8q & a, Vec8q const b) {
    a = a + b;
    return a;
}

// postfix operator ++
static inline Vec8q operator ++ (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a + 1;
    return a0;
}

// prefix operator ++
static inline Vec8q & operator ++ (Vec8q & a) {
    a = a + 1;
    return a;
}

// vector operator - : subtract element by element
static inline Vec8q operator - (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() - b.get_low(), a.get_high() - b.get_high());
}

// vector operator - : unary minus
static inline Vec8q operator - (Vec8q const a) {
    return Vec8q(- a.get_low(), - a.get_high());
}

// vector operator -= : subtract
static inline Vec8q & operator -= (Vec8q & a, Vec8q const b) {
    a = a - b;
    return a;
}

// postfix operator --
static inline Vec8q operator -- (Vec8q & a, int) {
    Vec8q a0 = a;
    a = a - 1;
    return a0;
}

// prefix operator --
static inline Vec8q & operator -- (Vec8q & a) {
    a = a - 1;
    return a;
}

// vector operator * : multiply element by element
static inline Vec8q operator * (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() * b.get_low(), a.get_high() * b.get_high());
}

// vector operator *= : multiply
static inline Vec8q & operator *= (Vec8q & a, Vec8q const b) {
    a = a * b;
    return a;
}

// vector operator << : shift left
static inline Vec8q operator << (Vec8q const a, int32_t b) {
    return Vec8q(a.get_low() << b, a.get_high() << b);
}

// vector operator <<= : shift left
static inline Vec8q & operator <<= (Vec8q & a, int32_t b) {
    a = a << b;
    return a;
}

// vector operator >> : shift right arithmetic
static inline Vec8q operator >> (Vec8q const a, int32_t b) {
    return Vec8q(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >>= : shift right arithmetic
static inline Vec8q & operator >>= (Vec8q & a, int32_t b) {
    a = a >> b;
    return a;
}

// vector operator == : returns true for elements for which a == b
static inline Vec8qb operator == (Vec8q const a, Vec8q const b) {
    return Vec8qb(a.get_low() == b.get_low(), a.get_high() == b.get_high());
}

// vector operator != : returns true for elements for which a != b
static inline Vec8qb operator != (Vec8q const a, Vec8q const b) {
    return Vec8qb(a.get_low() != b.get_low(), a.get_high() != b.get_high());
}
  
// vector operator < : returns true for elements for which a < b
static inline Vec8qb operator < (Vec8q const a, Vec8q const b) {
    return Vec8qb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b
static inline Vec8qb operator > (Vec8q const a, Vec8q const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (signed)
static inline Vec8qb operator >= (Vec8q const a, Vec8q const b) {
    return Vec8qb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (signed)
static inline Vec8qb operator <= (Vec8q const a, Vec8q const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8q operator & (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() & b.get_low(), a.get_high() & b.get_high());
}

// vector operator &= : bitwise and
static inline Vec8q & operator &= (Vec8q & a, Vec8q const b) {
    a = a & b;
    return a;
}

// vector operator | : bitwise or
static inline Vec8q operator | (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() | b.get_low(), a.get_high() | b.get_high());
}

// vector operator |= : bitwise or
static inline Vec8q & operator |= (Vec8q & a, Vec8q const b) {
    a = a | b;
    return a;
}

// vector operator ^ : bitwise xor
static inline Vec8q operator ^ (Vec8q const a, Vec8q const b) {
    return Vec8q(a.get_low() ^ b.get_low(), a.get_high() ^ b.get_high());
}
// vector operator ^= : bitwise xor
static inline Vec8q & operator ^= (Vec8q & a, Vec8q const b) {
    a = a ^ b;
    return a;
}

// vector operator ~ : bitwise not
static inline Vec8q operator ~ (Vec8q const a) {
    return Vec8q(~(a.get_low()), ~(a.get_high()));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8q select (Vec8qb const s, Vec8q const a, Vec8q const b) {
    return Vec8q(select(s.get_low(), a.get_low(), b.get_low()), select(s.get_high(), a.get_high(), b.get_high()));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8q if_add (Vec8qb const f, Vec8q const a, Vec8q const b) {
    return Vec8q(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec8q if_sub (Vec8qb const f, Vec8q const a, Vec8q const b) {
    return Vec8q(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec8q if_mul (Vec8qb const f, Vec8q const a, Vec8q const b) {
    return Vec8q(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline int64_t horizontal_add (Vec8q const a) {
    return horizontal_add(a.get_low() + a.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are sign extended before adding to avoid overflow
static inline int64_t horizontal_add_x (Vec16i const x) {
    return horizontal_add_x(x.get_low()) + horizontal_add_x(x.get_high());
}

// Horizontal add extended: Calculates the sum of all vector elements
// Elements are zero extended before adding to avoid overflow
static inline uint64_t horizontal_add_x (Vec16ui const x) {
    return horizontal_add_x(x.get_low()) + horizontal_add_x(x.get_high());
}

// function max: a > b ? a : b
static inline Vec8q max(Vec8q const a, Vec8q const b) {
    return Vec8q(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8q min(Vec8q const a, Vec8q const b) {
    return Vec8q(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
}

// function abs: a >= 0 ? a : -a
static inline Vec8q abs(Vec8q const a) {
    return Vec8q(abs(a.get_low()), abs(a.get_high()));
}

// function abs_saturated: same as abs, saturate if overflow
static inline Vec8q abs_saturated(Vec8q const a) {
    return Vec8q(abs_saturated(a.get_low()), abs_saturated(a.get_high()));
}

// function rotate_left all elements
// Use negative count to rotate right
static inline Vec8q rotate_left(Vec8q const a, int b) {
    return Vec8q(rotate_left(a.get_low(), b), rotate_left(a.get_high(), b));
}


/*****************************************************************************
*
*          Vector of 8 64-bit unsigned integers
*
*****************************************************************************/

class Vec8uq : public Vec8q {
public:
    // Default constructor:
    Vec8uq() {
    }
    // Constructor to broadcast the same value into all elements:
    Vec8uq(uint64_t i) {
        z0 = z1 = Vec4uq(i);
    }
    // Constructor to convert from Vec8q:
    Vec8uq(Vec8q const x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to convert from type Vec512b
    Vec8uq(Vec512b const & x) {
        z0 = x.get_low();
        z1 = x.get_high();
    }
    // Constructor to build from all elements:
    Vec8uq(uint64_t i0, uint64_t i1, uint64_t i2, uint64_t i3, uint64_t i4, uint64_t i5, uint64_t i6, uint64_t i7) {
        z0 = Vec4q((int64_t)i0, (int64_t)i1, (int64_t)i2, (int64_t)i3);
        z1 = Vec4q((int64_t)i4, (int64_t)i5, (int64_t)i6, (int64_t)i7);
    }
    // Constructor to build from two Vec4uq:
    Vec8uq(Vec4uq const a0, Vec4uq const a1) {
        z0 = a0;
        z1 = a1;
    }
    // Assignment operator to convert from Vec8q:
    Vec8uq & operator = (Vec8q const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Assignment operator to convert from type Vec512b
    Vec8uq & operator = (Vec512b const x) {
        z0 = x.get_low();
        z1 = x.get_high();
        return *this;
    }
    // Member function to load from array (unaligned)
    Vec8uq & load(void const * p) {
        Vec8q::load(p);
        return *this;
    }
    // Member function to load from array, aligned by 32
    Vec8uq & load_a(void const * p) {
        Vec8q::load_a(p);
        return *this;
    }
    // Member function to change a single element in vector
    Vec8uq const insert(int index, uint64_t value) {
        Vec8q::insert(index, (int64_t)value);
        return *this;
    }
    // Member function extract a single element from vector
    uint64_t extract(int index) const {
        return (uint64_t)Vec8q::extract(index);
    }
    // Extract a single element. Use store function if extracting more than one element.
    // Operator [] can only read an element, not write.
    uint64_t operator [] (int index) const {
        return extract(index);
    }
    // Member functions to split into two Vec2uq:
    Vec4uq get_low() const {
        return Vec4uq(Vec8q::get_low());
    }
    Vec4uq get_high() const {
        return Vec4uq(Vec8q::get_high());
    }
    static constexpr int elementtype() {
        return 11;
    }
};

// Define operators for this class

// vector operator + : add
static inline Vec8uq operator + (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) + Vec8q(b));
}

// vector operator - : subtract
static inline Vec8uq operator - (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) - Vec8q(b));
}

// vector operator * : multiply element by element
static inline Vec8uq operator * (Vec8uq const a, Vec8uq const b) {
    return Vec8uq (Vec8q(a) * Vec8q(b));
}

// vector operator >> : shift right logical all elements
static inline Vec8uq operator >> (Vec8uq const a, uint32_t b) {
    return Vec8uq(a.get_low() >> b, a.get_high() >> b);
}

// vector operator >> : shift right logical all elements
static inline Vec8uq operator >> (Vec8uq const a, int32_t b) {
    return a >> (uint32_t)b;
}

// vector operator >>= : shift right arithmetic
static inline Vec8uq & operator >>= (Vec8uq & a, uint32_t b) {
    a = a >> b;
    return a;
}

// vector operator >>= : shift right logical
static inline Vec8uq & operator >>= (Vec8uq & a, int32_t b) {
    a = a >> uint32_t(b);
    return a;
} 

// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const a, uint32_t b) {
    return Vec8uq ((Vec8q)a << (int32_t)b);
}

// vector operator << : shift left all elements
static inline Vec8uq operator << (Vec8uq const a, int32_t b) {
    return Vec8uq ((Vec8q)a << b);
}

// vector operator < : returns true for elements for which a < b (unsigned)
static inline Vec8qb operator < (Vec8uq const a, Vec8uq const b) {
    return Vec8qb(a.get_low() < b.get_low(), a.get_high() < b.get_high());
}

// vector operator > : returns true for elements for which a > b (unsigned)
static inline Vec8qb operator > (Vec8uq const a, Vec8uq const b) {
    return b < a;
}

// vector operator >= : returns true for elements for which a >= b (unsigned)
static inline Vec8qb operator >= (Vec8uq const a, Vec8uq const b) {
    return Vec8qb(a.get_low() >= b.get_low(), a.get_high() >= b.get_high());
}

// vector operator <= : returns true for elements for which a <= b (unsigned)
static inline Vec8qb operator <= (Vec8uq const a, Vec8uq const b) {
    return b >= a;
}

// vector operator & : bitwise and
static inline Vec8uq operator & (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) & Vec8q(b));
}

// vector operator | : bitwise or
static inline Vec8uq operator | (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) | Vec8q(b));
}

// vector operator ^ : bitwise xor
static inline Vec8uq operator ^ (Vec8uq const a, Vec8uq const b) {
    return Vec8uq(Vec8q(a) ^ Vec8q(b));
}

// Functions for this class

// Select between two operands. Corresponds to this pseudocode:
// for (int i = 0; i < 4; i++) result[i] = s[i] ? a[i] : b[i];
static inline Vec8uq select (Vec8qb const s, Vec8uq const a, Vec8uq const b) {
    return Vec8uq(select(s, Vec8q(a), Vec8q(b)));
}

// Conditional add: For all vector elements i: result[i] = f[i] ? (a[i] + b[i]) : a[i]
static inline Vec8uq if_add (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
    return Vec8uq(if_add(f.get_low(), a.get_low(), b.get_low()), if_add(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional subtract
static inline Vec8uq if_sub (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
    return Vec8uq(if_sub(f.get_low(), a.get_low(), b.get_low()), if_sub(f.get_high(), a.get_high(), b.get_high()));
}

// Conditional multiply
static inline Vec8uq if_mul (Vec8qb const f, Vec8uq const a, Vec8uq const b) {
    return Vec8uq(if_mul(f.get_low(), a.get_low(), b.get_low()), if_mul(f.get_high(), a.get_high(), b.get_high()));
}

// Horizontal add: Calculates the sum of all vector elements. Overflow will wrap around
static inline uint64_t horizontal_add (Vec8uq const a) {
    return (uint64_t)horizontal_add(Vec8q(a));
}

// function max: a > b ? a : b
static inline Vec8uq max(Vec8uq const a, Vec8uq const b) {
    return Vec8uq(max(a.get_low(), b.get_low()), max(a.get_high(), b.get_high()));
}

// function min: a < b ? a : b
static inline Vec8uq min(Vec8uq const a, Vec8uq const b) {
    return Vec8uq(min(a.get_low(), b.get_low()), min(a.get_high(), b.get_high()));
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

// Permute vector of 8 64-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q permute8(Vec8q const a) {
    return Vec8q(blend4<i0,i1,i2,i3> (a.get_low(), a.get_high()),
                 blend4<i4,i5,i6,i7> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8uq permute8(Vec8uq const& a) {
    return Vec8uq(permute8<i0, i1, i2, i3, i4, i5, i6, i7>(Vec8q(a)));
}

// Permute vector of 16 32-bit integers.
// Index -1 gives 0, index V_DC means don't care.
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i permute16(Vec16i const a) {
    return Vec16i(blend8<i0,i1,i2 ,i3 ,i4 ,i5 ,i6 ,i7 > (a.get_low(), a.get_high()),
                  blend8<i8,i9,i10,i11,i12,i13,i14,i15> (a.get_low(), a.get_high()));
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16ui permute16(Vec16ui const a) {
    return Vec16ui (permute16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16i(a)));
}


/*****************************************************************************
*
*          Vector blend functions
*
*****************************************************************************/

// blend vectors Vec8q
template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8q blend8(Vec8q const a, Vec8q const b) {
    Vec4q x0 = blend_half<Vec8q, i0, i1, i2, i3>(a, b);
    Vec4q x1 = blend_half<Vec8q, i4, i5, i6, i7>(a, b);
    return Vec8q(x0, x1);
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7> 
static inline Vec8uq blend8(Vec8uq const a, Vec8uq const b) {
    return Vec8uq( blend8<i0,i1,i2,i3,i4,i5,i6,i7> (Vec8q(a),Vec8q(b)));
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16i blend16(Vec16i const a, Vec16i const b) {
    Vec8i x0 = blend_half<Vec16i, i0, i1, i2, i3, i4, i5, i6, i7>(a, b);
    Vec8i x1 = blend_half<Vec16i, i8, i9, i10, i11, i12, i13, i14, i15>(a, b);
    return Vec16i(x0, x1);
}

template <int i0,  int i1,  int i2,  int i3,  int i4,  int i5,  int i6,  int i7, 
          int i8,  int i9,  int i10, int i11, int i12, int i13, int i14, int i15 > 
static inline Vec16ui blend16(Vec16ui const a, Vec16ui const b) {
    return Vec16ui( blend16<i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15> (Vec16i(a),Vec16i(b)));
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

static inline Vec16i lookup16(Vec16i const i1, Vec16i const table) {
    int32_t t[16];
    table.store(t);
    return Vec16i(t[i1[0]], t[i1[1]], t[i1[2]], t[i1[3]], t[i1[4]], t[i1[5]], t[i1[6]], t[i1[7]],
        t[i1[8]], t[i1[9]], t[i1[10]], t[i1[11]], t[i1[12]], t[i1[13]], t[i1[14]], t[i1[15]]);
} 

template <int n>
static inline Vec16i lookup(Vec16i const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 8) {
        Vec8i table1 = Vec8i().load(table);
        return Vec16i(
            lookup8(index.get_low(), table1),
            lookup8(index.get_high(), table1));
    }
    if (n <= 16) return lookup16(index, Vec16i().load(table));
    // n > 16. Limit index
    Vec16ui i1;
    if ((n & (n - 1)) == 0) {
        // n is a power of 2, make index modulo n
        i1 = Vec16ui(index) & (n - 1);
    }
    else {
        // n is not a power of 2, limit to n-1
        i1 = min(Vec16ui(index), n - 1);
    }
    int32_t const * t = (int32_t const *)table;
    return Vec16i(t[i1[0]], t[i1[1]], t[i1[2]], t[i1[3]], t[i1[4]], t[i1[5]], t[i1[6]], t[i1[7]],
        t[i1[8]], t[i1[9]], t[i1[10]], t[i1[11]], t[i1[12]], t[i1[13]], t[i1[14]], t[i1[15]]);
}

static inline Vec16i lookup32(Vec16i const index, Vec16i const table1, Vec16i const table2) {
    int32_t tab[32];
    table1.store(tab);  table2.store(tab+16);
    Vec8i t0 = lookup<32>(index.get_low(), tab);
    Vec8i t1 = lookup<32>(index.get_high(), tab);
    return Vec16i(t0, t1);
}

static inline Vec16i lookup64(Vec16i const index, Vec16i const table1, Vec16i const table2, Vec16i const table3, Vec16i const table4) {
    int32_t tab[64];
    table1.store(tab);  table2.store(tab + 16);  table3.store(tab + 32);  table4.store(tab + 48);
    Vec8i t0 = lookup<64>(index.get_low(), tab);
    Vec8i t1 = lookup<64>(index.get_high(), tab);
    return Vec16i(t0, t1);
}


static inline Vec8q lookup8(Vec8q const index, Vec8q const table) {
    int64_t tab[8];
    table.store(tab);
    Vec4q t0 = lookup<8>(index.get_low(), tab);
    Vec4q t1 = lookup<8>(index.get_high(), tab);
    return Vec8q(t0, t1);
}

template <int n>
static inline Vec8q lookup(Vec8q const index, void const * table) {
    if (n <= 0) return 0;
    if (n <= 4) {
        Vec4q table1 = Vec4q().load(table);        
        return Vec8q(       
            lookup4 (index.get_low(),  table1),
            lookup4 (index.get_high(), table1));
    }
    if (n <= 8) {
        return lookup8(index, Vec8q().load(table));
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
    int64_t const * t = (int64_t const *)table;
    return Vec8q(t[i1[0]],t[i1[1]],t[i1[2]],t[i1[3]],t[i1[4]],t[i1[5]],t[i1[6]],t[i1[7]]);
}

/*****************************************************************************
*
*          Vector scatter functions
*
*****************************************************************************/

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7,
    int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
    static inline void scatter(Vec16i const data, void * array) {
    int32_t* arr = (int32_t*)array;
    const int index[16] = {i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15};
    for (int i = 0; i < 16; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
}

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline void scatter(Vec8q const data, void * array) {
    int64_t* arr = (int64_t*)array;
    const int index[8] = {i0,i1,i2,i3,i4,i5,i6,i7};
    for (int i = 0; i < 8; i++) {
        if (index[i] >= 0) arr[index[i]] = data[i];
    }
}

static inline void scatter(Vec16i const index, uint32_t limit, Vec16i const data, void * array) {
    int32_t* arr = (int32_t*)array;
    for (int i = 0; i < 16; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
}

static inline void scatter(Vec8q const index, uint32_t limit, Vec8q const data, void * array) {
    int64_t* arr = (int64_t*)array;
    for (int i = 0; i < 8; i++) {
        if (uint64_t(index[i]) < uint64_t(limit)) arr[index[i]] = data[i];
    }
}

static inline void scatter(Vec8i const index, uint32_t limit, Vec8q const data, void * array) {
    int64_t* arr = (int64_t*)array;
    for (int i = 0; i < 8; i++) {
        if (uint32_t(index[i]) < limit) arr[index[i]] = data[i];
    }
}

// Scatter functions with variable indexes:

static inline void scatter16i(Vec16i index, uint32_t limit, Vec16i data, void * destination) {
    uint32_t ix[16];  index.store(ix);
    for (int i = 0; i < 16; i++) {
        if (ix[i] < limit) ((int*)destination)[ix[i]] = data[i];
    }
}

static inline void scatter8q(Vec8q index, uint32_t limit, Vec8q data, void * destination) {
    uint64_t ix[8];  index.store(ix);
    for (int i = 0; i < 8; i++) {
        if (ix[i] < limit) ((int64_t*)destination)[ix[i]] = data[i];
    }
}

static inline void scatter8i(Vec8i index, uint32_t limit, Vec8i data, void * destination) {
    uint32_t ix[8];  index.store(ix);
    for (int i = 0; i < 8; i++) {
        if (ix[i] < limit) ((int*)destination)[ix[i]] = data[i];
    }
}

static inline void scatter4q(Vec4q index, uint32_t limit, Vec4q data, void * destination) {
    uint64_t ix[4];  index.store(ix);
    for (int i = 0; i < 4; i++) {
        if (ix[i] < limit) ((int64_t*)destination)[ix[i]] = data[i];
    }
}

static inline void scatter4i(Vec4i index, uint32_t limit, Vec4i data, void * destination) {
    uint32_t ix[4];  index.store(ix);
    for (int i = 0; i < 4; i++) {
        if (ix[i] < limit) ((int*)destination)[ix[i]] = data[i];
    }
}

/*****************************************************************************
*
*          Gather functions with fixed indexes
*
*****************************************************************************/

template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7, 
int i8, int i9, int i10, int i11, int i12, int i13, int i14, int i15>
static inline Vec16i gather16i(void const * a) {
    int constexpr indexs[16] = { i0, i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, i11, i12, i13, i14, i15 };
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 15) {
        // load one contiguous block and permute
        if constexpr (imax > 15) {
            // make sure we don't read past the end of the array
            Vec16i b = Vec16i().load((int32_t const *)a + imax-15);
            return permute16<i0-imax+15, i1-imax+15, i2-imax+15, i3-imax+15, i4-imax+15, i5-imax+15, i6-imax+15, i7-imax+15,
                i8-imax+15, i9-imax+15, i10-imax+15, i11-imax+15, i12-imax+15, i13-imax+15, i14-imax+15, i15-imax+15> (b);
        }
        else {
            Vec16i b = Vec16i().load((int32_t const *)a + imin);
            return permute16<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin,
                i8-imin, i9-imin, i10-imin, i11-imin, i12-imin, i13-imin, i14-imin, i15-imin> (b);
        }
    }
    if constexpr ((i0<imin+16  || i0>imax-16)  && (i1<imin+16  || i1>imax-16)  && (i2<imin+16  || i2>imax-16)  && (i3<imin+16  || i3>imax-16)
    &&  (i4<imin+16  || i4>imax-16)  && (i5<imin+16  || i5>imax-16)  && (i6<imin+16  || i6>imax-16)  && (i7<imin+16  || i7>imax-16)    
    &&  (i8<imin+16  || i8>imax-16)  && (i9<imin+16  || i9>imax-16)  && (i10<imin+16 || i10>imax-16) && (i11<imin+16 || i11>imax-16)
    &&  (i12<imin+16 || i12>imax-16) && (i13<imin+16 || i13>imax-16) && (i14<imin+16 || i14>imax-16) && (i15<imin+16 || i15>imax-16) ) {
        // load two contiguous blocks and blend
        Vec16i b = Vec16i().load((int32_t const *)a + imin);
        Vec16i c = Vec16i().load((int32_t const *)a + imax-15);
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
    return lookup<imax+1>(Vec16i(i0,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10,i11,i12,i13,i14,i15), a);
}


template <int i0, int i1, int i2, int i3, int i4, int i5, int i6, int i7>
static inline Vec8q gather8q(void const * a) {
    int constexpr indexs[8] = { i0, i1, i2, i3, i4, i5, i6, i7 }; // indexes as array
    constexpr int imin = min_index(indexs);
    constexpr int imax = max_index(indexs);
    static_assert(imin >= 0, "Negative index in gather function");

    if constexpr (imax - imin <= 7) {
        // load one contiguous block and permute
        if constexpr (imax > 7) {
            // make sure we don't read past the end of the array
            Vec8q b = Vec8q().load((int64_t const *)a + imax-7);
            return permute8<i0-imax+7, i1-imax+7, i2-imax+7, i3-imax+7, i4-imax+7, i5-imax+7, i6-imax+7, i7-imax+7> (b);
        }
        else {
            Vec8q b = Vec8q().load((int64_t const *)a + imin);
            return permute8<i0-imin, i1-imin, i2-imin, i3-imin, i4-imin, i5-imin, i6-imin, i7-imin> (b);
        }
    }
    if constexpr ((i0<imin+8 || i0>imax-8) && (i1<imin+8 || i1>imax-8) && (i2<imin+8 || i2>imax-8) && (i3<imin+8 || i3>imax-8)
    &&  (i4<imin+8 || i4>imax-8) && (i5<imin+8 || i5>imax-8) && (i6<imin+8 || i6>imax-8) && (i7<imin+8 || i7>imax-8)) {
        // load two contiguous blocks and blend
        Vec8q b = Vec8q().load((int64_t const *)a + imin);
        Vec8q c = Vec8q().load((int64_t const *)a + imax-7);
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
    return lookup<imax+1>(Vec8q(i0,i1,i2,i3,i4,i5,i6,i7), a);
}


/*****************************************************************************
*
*          Functions for conversion between integer sizes
*
*****************************************************************************/

// Extend 16-bit integers to 32-bit integers, signed and unsigned
/*
// Function extend_to_int : extends Vec16s to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16s const a) {
    return Vec16i(extend_low(a), extend_high(a));
}

// Function extend_to_int : extends Vec16us to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16us const a) {
    return Vec16i(extend_low(a), extend_high(a));
}

// Function extend_to_int : extends Vec16c to Vec16i with sign extension
static inline Vec16i extend_to_int (Vec16c const a) {
    return extend_to_int(Vec16s(extend_low(a), extend_high(a)));
}

// Function extend_to_int : extends Vec16uc to Vec16ui with zero extension
static inline Vec16ui extend_to_int (Vec16uc const a) {
    return extend_to_int(Vec16s(extend_low(a), extend_high(a)));
}*/


// Extend 32-bit integers to 64-bit integers, signed and unsigned

// Function extend_low : extends the low 8 elements to 64 bits with sign extension
static inline Vec8q extend_low (Vec16i const a) {
    return Vec8q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 64 bits with sign extension
static inline Vec8q extend_high (Vec16i const a) {
    return Vec8q(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Function extend_low : extends the low 8 elements to 64 bits with zero extension
static inline Vec8uq extend_low (Vec16ui const a) {
    return Vec8q(extend_low(a.get_low()), extend_high(a.get_low()));
}

// Function extend_high : extends the high 8 elements to 64 bits with zero extension
static inline Vec8uq extend_high (Vec16ui const a) {
    return Vec8q(extend_low(a.get_high()), extend_high(a.get_high()));
}

// Compress 32-bit integers to 8-bit integers, signed and unsigned, with and without saturation
/*
// Function compress : packs two vectors of 16-bit integers into one vector of 8-bit integers
// Overflow wraps around
static inline Vec16c compress_to_int8 (Vec16i const a) {
    Vec16s b = compress(a.get_low(), a.get_high());
    Vec16c c = compress(b.get_low(), b.get_high());
    return c;
}

static inline Vec16s compress_to_int16 (Vec16i const a) {
    return compress(a.get_low(), a.get_high());
}

// with signed saturation
static inline Vec16c compress_to_int8_saturated (Vec16i const a) {
    Vec16s b = compress_saturated(a.get_low(), a.get_high());
    Vec16c c = compress_saturated(b.get_low(), b.get_high());
    return c;
}

static inline Vec16s compress_to_int16_saturated (Vec16i const a) {
    return compress_saturated(a.get_low(), a.get_high());
}

// with unsigned saturation
static inline Vec16uc compress_to_int8_saturated (Vec16ui const a) {
    Vec16us b = compress_saturated(a.get_low(), a.get_high());
    Vec16uc c = compress_saturated(b.get_low(), b.get_high());
    return c;
}

static inline Vec16us compress_to_int16_saturated (Vec16ui const a) {
    return compress_saturated(a.get_low(), a.get_high());
}*/

// Compress 64-bit integers to 32-bit integers, signed and unsigned, with and without saturation

// Function compress : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Overflow wraps around
static inline Vec16i compress (Vec8q const low, Vec8q const high) {
    return Vec16i(compress(low.get_low(),low.get_high()), compress(high.get_low(),high.get_high()));
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Signed, with saturation
static inline Vec16i compress_saturated (Vec8q const low, Vec8q const high) {
    return Vec16i(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}

// Function compress_saturated : packs two vectors of 64-bit integers into one vector of 32-bit integers
// Unsigned, with saturation
static inline Vec16ui compress_saturated (Vec8uq const low, Vec8uq const high) {
    return Vec16ui(compress_saturated(low.get_low(),low.get_high()), compress_saturated(high.get_low(),high.get_high()));
}


/*****************************************************************************
*
*          Integer division operators
*          Please see the file vectori128.h for explanation.
*
*****************************************************************************/

// vector operator / : divide each element by divisor

// vector operator / : divide all elements by same integer
static inline Vec16i operator / (Vec16i const a, Divisor_i const d) {
    return Vec16i(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16i & operator /= (Vec16i & a, Divisor_i const d) {
    a = a / d;
    return a;
}

// vector operator / : divide all elements by same integer
static inline Vec16ui operator / (Vec16ui const a, Divisor_ui const d) {
    return Vec16ui(a.get_low() / d, a.get_high() / d);
}

// vector operator /= : divide
static inline Vec16ui & operator /= (Vec16ui & a, Divisor_ui const d) {
    a = a / d;
    return a;
}


/*****************************************************************************
*
*          Integer division 2: divisor is a compile-time constant
*
*****************************************************************************/

// Divide Vec16i by compile-time constant
template <int32_t d>
static inline Vec16i divide_by_i(Vec16i const a) {
    return Vec16i(divide_by_i<d>(a.get_low()), divide_by_i<d>(a.get_high()));
}

// define Vec16i a / const_int(d)
template <int32_t d>
static inline Vec16i operator / (Vec16i const a, Const_int_t<d>) {
    return divide_by_i<d>(a);
}

// define Vec16i a / const_uint(d)
template <uint32_t d>
static inline Vec16i operator / (Vec16i const a, Const_uint_t<d>) {
    static_assert(d < 0x80000000u, "Dividing signed integer by overflowing unsigned");
    return divide_by_i<int32_t(d)>(a);                               // signed divide
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16i & operator /= (Vec16i & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// Divide Vec16ui by compile-time constant
template <uint32_t d>
static inline Vec16ui divide_by_ui(Vec16ui const a) {
    return Vec16ui( divide_by_ui<d>(a.get_low()), divide_by_ui<d>(a.get_high()));
}

// define Vec16ui a / const_uint(d)
template <uint32_t d>
static inline Vec16ui operator / (Vec16ui const a, Const_uint_t<d>) {
    return divide_by_ui<d>(a);
}

// define Vec16ui a / const_int(d)
template <int32_t d>
static inline Vec16ui operator / (Vec16ui const a, Const_int_t<d>) {
    static_assert(d >= 0, "Dividing unsigned integer by negative is ambiguous");
    return divide_by_ui<d>(a);                                       // unsigned divide
}

// vector operator /= : divide
template <uint32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_uint_t<d> b) {
    a = a / b;
    return a;
}

// vector operator /= : divide
template <int32_t d>
static inline Vec16ui & operator /= (Vec16ui & a, Const_int_t<d> b) {
    a = a / b;
    return a;
}


/*****************************************************************************
*
*          Boolean <-> bitfield conversion functions
*
*****************************************************************************/

// to_bits: convert to integer bitfield
static inline uint16_t to_bits(Vec16b const a) {
    return uint16_t(to_bits(a.get_low()) | ((uint16_t)to_bits(a.get_high()) << 8));
}

// to_bits: convert to integer bitfield
static inline uint16_t to_bits(Vec16ib const a) {
    return uint16_t(to_bits(a.get_low()) | ((uint16_t)to_bits(a.get_high()) << 8));
}

// to_bits: convert to integer bitfield
static inline uint8_t to_bits(Vec8b const a) {
    return uint8_t(to_bits(a.get_low()) | (to_bits(a.get_high()) << 4));
}


#ifdef VCL_NAMESPACE
}
#endif

#endif // VECTORI512E_H
