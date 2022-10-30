/****************************  instrset.h   **********************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-18
* Version:       2.01.00
* Project:       vector class library
* Description:
* Header file for various compiler-specific tasks as well as common
* macros and templates. This file contains:
*
* > Selection of the supported instruction set
* > Defines compiler version macros
* > Undefines certain macros that prevent function overloading
* > Helper functions that depend on instruction set, compiler, or platform
* > Common templates for permute, blend, etc.
*
* For instructions, see vcl_manual.pdf
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#ifndef INSTRSET_H
#define INSTRSET_H 20100


// Allow the use of floating point permute instructions on integer vectors.
// Some CPU's have an extra latency of 1 or 2 clock cycles for this, but
// it may still be faster than alternative implementations:
#define ALLOW_FP_PERMUTE  true


// Macro to indicate 64 bit mode
#if (defined(_M_AMD64) || defined(_M_X64) || defined(__amd64) ) && ! defined(__x86_64__)
#define __x86_64__ 1  // There are many different macros for this, decide on only one
#endif

// The following values of INSTRSET are currently defined:
// 2:  SSE2
// 3:  SSE3
// 4:  SSSE3
// 5:  SSE4.1
// 6:  SSE4.2
// 7:  AVX
// 8:  AVX2
// 9:  AVX512F
// 10: AVX512BW/DQ/VL
// In the future, INSTRSET = 11 may include AVX512VBMI and AVX512VBMI2, but this
// decision cannot be made before the market situation for CPUs with these
// instruction sets is known (these future instruction set extensions are already
// used in some VCL functions and tested with an emulator)

// Find instruction set from compiler macros if INSTRSET is not defined.
// Note: Most of these macros are not defined in Microsoft compilers
#ifndef INSTRSET
#if defined ( __AVX512VL__ ) && defined ( __AVX512BW__ ) && defined ( __AVX512DQ__ ) 
#define INSTRSET 10
#elif defined ( __AVX512F__ ) || defined ( __AVX512__ )
#define INSTRSET 9
#elif defined ( __AVX2__ )
#define INSTRSET 8
#elif defined ( __AVX__ )
#define INSTRSET 7
#elif defined ( __SSE4_2__ )
#define INSTRSET 6
#elif defined ( __SSE4_1__ )
#define INSTRSET 5
#elif defined ( __SSSE3__ )
#define INSTRSET 4
#elif defined ( __SSE3__ )
#define INSTRSET 3
#elif defined ( __SSE2__ ) || defined ( __x86_64__ )
#define INSTRSET 2
#elif defined ( __SSE__ )
#define INSTRSET 1
#elif defined ( _M_IX86_FP )           // Defined in MS compiler. 1: SSE, 2: SSE2
#define INSTRSET _M_IX86_FP
#else
#define INSTRSET 0
#endif // instruction set defines
#endif // INSTRSET

// Include the appropriate header file for intrinsic functions
#if INSTRSET > 7                       // AVX2 and later
#if defined (__GNUC__) && ! defined (__INTEL_COMPILER)
#include <x86intrin.h>                 // x86intrin.h includes header files for whatever instruction 
                                       // sets are specified on the compiler command line, such as:
                                       // xopintrin.h, fma4intrin.h
#else
#include <immintrin.h>                 // MS/Intel version of immintrin.h covers AVX and later
#endif // __GNUC__
#elif INSTRSET == 7
#include <immintrin.h>                 // AVX
#elif INSTRSET == 6
#include <nmmintrin.h>                 // SSE4.2
#elif INSTRSET == 5
#include <smmintrin.h>                 // SSE4.1
#elif INSTRSET == 4
#include <tmmintrin.h>                 // SSSE3
#elif INSTRSET == 3
#include <pmmintrin.h>                 // SSE3
#elif INSTRSET == 2
#include <emmintrin.h>                 // SSE2
#elif INSTRSET == 1
#include <xmmintrin.h>                 // SSE
#endif // INSTRSET

#if INSTRSET >= 8 && !defined(__FMA__)
// Assume that all processors that have AVX2 also have FMA3
#if defined (__GNUC__) && ! defined (__INTEL_COMPILER) 
// Prevent error message in g++ and Clang when using FMA intrinsics with avx2:
#pragma message "It is recommended to specify also option -mfma when using -mavx2 or higher"
#elif ! defined (__clang__)
#define __FMA__  1
#endif
#endif

// AMD  instruction sets
#if defined (__XOP__) || defined (__FMA4__)
#ifdef __GNUC__
#include <x86intrin.h>                 // AMD XOP (Gnu)
#else
#include <ammintrin.h>                 // AMD XOP (Microsoft)
#endif //  __GNUC__
#elif defined (__SSE4A__)              // AMD SSE4A
#include <ammintrin.h>
#endif // __XOP__ 

// FMA3 instruction set
#if defined (__FMA__) && (defined(__GNUC__) || defined(__clang__))  && ! defined (__INTEL_COMPILER)
#include <fmaintrin.h> 
#endif // __FMA__ 

// FMA4 instruction set
#if defined (__FMA4__) && (defined(__GNUC__) || defined(__clang__))
#include <fma4intrin.h> // must have both x86intrin.h and fma4intrin.h, don't know why
#endif // __FMA4__


#include <stdint.h>                    // Define integer types with known size
#include <stdlib.h>                    // define abs(int)

#ifdef _MSC_VER                        // Microsoft compiler or compatible Intel compiler
#include <intrin.h>                    // define _BitScanReverse(int), __cpuid(int[4],int), _xgetbv(int)
#endif // _MSC_VER


// functions in instrset_detect.cpp:
#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif
    int  instrset_detect(void);        // tells which instruction sets are supported
    bool hasFMA3(void);                // true if FMA3 instructions supported
    bool hasFMA4(void);                // true if FMA4 instructions supported
    bool hasXOP(void);                 // true if XOP  instructions supported
    bool hasAVX512ER(void);            // true if AVX512ER instructions supported
    bool hasAVX512VBMI(void);          // true if AVX512VBMI instructions supported
    bool hasAVX512VBMI2(void);         // true if AVX512VBMI2 instructions supported
#ifdef VCL_NAMESPACE
}
#endif

// functions in physical_processors.cpp:
int physicalProcessors(int * logical_processors = 0);


// GCC version
#if defined(__GNUC__) && !defined (GCC_VERSION) && !defined (__clang__)
#define GCC_VERSION  ((__GNUC__) * 10000 + (__GNUC_MINOR__) * 100 + (__GNUC_PATCHLEVEL__))
#endif

// Clang version
#if defined (__clang__)
#define CLANG_VERSION  ((__clang_major__) * 10000 + (__clang_minor__) * 100 + (__clang_patchlevel__))
// Problem: The version number is not consistent across platforms
// http://llvm.org/bugs/show_bug.cgi?id=12643
// Apple bug 18746972
#endif

// Fix problem with non-overloadable macros named min and max in WinDef.h
#ifdef _MSC_VER
#if defined (_WINDEF_) && defined(min) && defined(max)
#undef min
#undef max
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#endif

/* Intel compiler problem:
The Intel compiler currently cannot compile version 2.00 of VCL. It seems to have
a problem with constexpr function returns not being constant enough.
*/
#if defined(__INTEL_COMPILER) && __INTEL_COMPILER < 9999
#error The Intel compiler version 19.00 cannot compile VCL version 2. Use Version 1.xx of VCL instead
#endif

/* Clang problem:
The Clang compiler treats the intrinsic vector types __m128, __m128i, and __m128d as identical.
See the bug report at https://bugs.llvm.org/show_bug.cgi?id=17164
Additional problem: The version number is not consistent across platforms. The Apple build has 
different version numbers. We have to rely on __apple_build_version__ on the Mac platform:
http://llvm.org/bugs/show_bug.cgi?id=12643
We have to make switches here when - hopefully - the error some day has been fixed.
We need different version checks with and without __apple_build_version__
*/
#if (defined (__clang__) || defined(__apple_build_version__)) && !defined(__INTEL_COMPILER) 
#define FIX_CLANG_VECTOR_ALIAS_AMBIGUITY  
#endif

#if defined (GCC_VERSION) && GCC_VERSION < 99999 && !defined(__clang__)
#define ZEXT_MISSING  // Gcc 7.4.0 does not have _mm256_zextsi128_si256 and similar functions
#endif


#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif

// Constant for indicating don't care in permute and blend functions.
// V_DC is -256 in Vector class library version 1.xx
// V_DC can be any value less than -1 in Vector class library version 2.00
constexpr int V_DC = -256;


/*****************************************************************************
*
*    Helper functions that depend on instruction set, compiler, or platform
*
*****************************************************************************/

// Define interface to cpuid instruction.
// input:  functionnumber = leaf (eax), ecxleaf = subleaf(ecx)
// output: output[0] = eax, output[1] = ebx, output[2] = ecx, output[3] = edx
static inline void cpuid(int output[4], int functionnumber, int ecxleaf = 0) {
#if defined(__GNUC__) || defined(__clang__)           // use inline assembly, Gnu/AT&T syntax
    int a, b, c, d;
    __asm("cpuid" : "=a"(a), "=b"(b), "=c"(c), "=d"(d) : "a"(functionnumber), "c"(ecxleaf) : );
    output[0] = a;
    output[1] = b;
    output[2] = c;
    output[3] = d;

#elif defined (_MSC_VER)                              // Microsoft compiler, intrin.h included
    __cpuidex(output, functionnumber, ecxleaf);       // intrinsic function for CPUID

#else                                                 // unknown platform. try inline assembly with masm/intel syntax
    __asm {
        mov eax, functionnumber
        mov ecx, ecxleaf
        cpuid;
        mov esi, output
        mov[esi], eax
        mov[esi + 4], ebx
        mov[esi + 8], ecx
        mov[esi + 12], edx
    }
#endif
}


// Define popcount function. Gives sum of bits
#if INSTRSET >= 6   // SSE4.2
// popcnt instruction is not officially part of the SSE4.2 instruction set,
// but available in all known processors with SSE4.2
static inline uint32_t vml_popcnt(uint32_t a) {
    return (uint32_t)_mm_popcnt_u32(a);  // Intel intrinsic. Supported by gcc and clang
}
#ifdef __x86_64__
static inline int64_t vml_popcnt(uint64_t a) {
    return _mm_popcnt_u64(a);            // Intel intrinsic.
}
#else   // 32 bit mode
static inline int64_t vml_popcnt(uint64_t a) {
    return _mm_popcnt_u32(uint32_t(a >> 32)) + _mm_popcnt_u32(uint32_t(a));
}
#endif
#else  // no SSE4.2
static inline uint32_t vml_popcnt(uint32_t a) {
    // popcnt instruction not available
    uint32_t b = a - ((a >> 1) & 0x55555555);
    uint32_t c = (b & 0x33333333) + ((b >> 2) & 0x33333333);
    uint32_t d = (c + (c >> 4)) & 0x0F0F0F0F;
    uint32_t e = d * 0x01010101;
    return   e >> 24;
}

static inline int32_t vml_popcnt(uint64_t a) {
    return vml_popcnt(uint32_t(a >> 32)) + vml_popcnt(uint32_t(a));
}

#endif

// Define bit-scan-forward function. Gives index to lowest set bit
#if defined (__GNUC__) || defined(__clang__)
    // gcc and Clang have no bit_scan_forward intrinsic
#if defined(__clang__)   // fix clang bug
    // Clang uses a k register as parameter a when inlined from horizontal_find_first
__attribute__((noinline))
#endif
static uint32_t bit_scan_forward(uint32_t a) {
    uint32_t r;
    __asm("bsfl %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
static inline uint32_t bit_scan_forward(uint64_t a) {
    uint32_t lo = uint32_t(a);
    if (lo) return bit_scan_forward(lo);
    uint32_t hi = uint32_t(a >> 32);
    return bit_scan_forward(hi) + 32;
}

#else  // other compilers
static inline uint32_t bit_scan_forward(uint32_t a) {
    unsigned long r;
    _BitScanForward(&r, a);            // defined in intrin.h for MS and Intel compilers
    return r;
}
#ifdef __x86_64__
static inline uint32_t bit_scan_forward(uint64_t a) {
    unsigned long r;
    _BitScanForward64(&r, a);          // defined in intrin.h for MS and Intel compilers
    return (uint32_t)r;
}
#else
static inline uint32_t bit_scan_forward(uint64_t a) {
    uint32_t lo = uint32_t(a);
    if (lo) return bit_scan_forward(lo);
    uint32_t hi = uint32_t(a >> 32);
    return bit_scan_forward(hi) + 32;
}
#endif
#endif


// Define bit-scan-reverse function. Gives index to highest set bit = floor(log2(a))
#if defined (__GNUC__) || defined(__clang__)
static inline uint32_t bit_scan_reverse(uint32_t a) __attribute__((pure));
static inline uint32_t bit_scan_reverse(uint32_t a) {
    uint32_t r;
    __asm("bsrl %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
#ifdef __x86_64__
static inline uint32_t bit_scan_reverse(uint64_t a) {
    uint64_t r;
    __asm("bsrq %1, %0" : "=r"(r) : "r"(a) : );
    return r;
}
#else   // 32 bit mode
static inline uint32_t bit_scan_reverse(uint64_t a) {
    uint64_t ahi = a >> 32;
    if (ahi == 0) return bit_scan_reverse(uint32_t(a));
    else return bit_scan_reverse(uint32_t(ahi)) + 32;
}
#endif
#else
static inline uint32_t bit_scan_reverse(uint32_t a) {
    unsigned long r;
    _BitScanReverse(&r, a);            // defined in intrin.h for MS and Intel compilers
    return r;
}
#ifdef __x86_64__
static inline uint32_t bit_scan_reverse(uint64_t a) {
    unsigned long r;
    _BitScanReverse64(&r, a);          // defined in intrin.h for MS and Intel compilers
    return r;
}
#else   // 32 bit mode
static inline uint32_t bit_scan_reverse(uint64_t a) {
    uint64_t ahi = a >> 32;
    if (ahi == 0) return bit_scan_reverse(uint32_t(a));
    else return bit_scan_reverse(uint32_t(ahi)) + 32;
}
#endif
#endif

// Same function, for compile-time constants
constexpr int bit_scan_reverse_const(uint64_t const n) {
    if (n == 0) return -1;
    uint64_t a = n, b = 0, j = 64, k = 0;
    do {
        j >>= 1;
        k = (uint64_t)1 << j;
        if (a >= k) {
            a >>= j;
            b += j;
        }
    } while (j > 0);
    return int(b);
}


/*****************************************************************************
*
*    Common templates
*
*****************************************************************************/

// Template class to represent compile-time integer constant
template <int32_t  n> class Const_int_t {};      // represent compile-time signed integer constant
template <uint32_t n> class Const_uint_t {};     // represent compile-time unsigned integer constant
#define const_int(n)  (Const_int_t <n>())        // n must be compile-time integer constant
#define const_uint(n) (Const_uint_t<n>())        // n must be compile-time unsigned integer constant


// template for producing quiet NAN
template <class VTYPE>
static inline VTYPE nan_vec(uint32_t payload = 0x100) {
    if constexpr ((VTYPE::elementtype() & 1) != 0) {  // double
        union {
            uint64_t q;
            double f;
        } ud;
        // n is left justified to avoid loss of NAN payload when converting to float
        ud.q = 0x7FF8000000000000 | uint64_t(payload) << 29;
        return VTYPE(ud.f);
    }
    // float will be converted to double if necessary
    union {
        uint32_t i;
        float f;
    } uf;
    uf.i = 0x7FC00000 | (payload & 0x003FFFFF);
    return VTYPE(uf.f);
}


// Test if a parameter is a compile-time constant
/* Unfortunately, this works only for macro parameters, not for inline function parameters.
   I hope that some solution will appear in the future, but for now it appears to be 
   impossible to check if a function parameter is a compile-time constant.
   This would be useful in operator / and in function pow:
   #if defined(__GNUC__) || defined (__clang__)
   #define is_constant(a) __builtin_constant_p(a)
   #else
   #define is_constant(a) false
   #endif
*/


/*****************************************************************************
*
*    Helper functions for permute and blend functions
*
******************************************************************************
Rules for constexpr functions:

> All variable declarations must include initialization

> Do not put variable declarations inside a for-clause, e.g. avoid: for (int i=0; ..
  Instead, you have to declare the loop counter before the for-loop.
  
> Do not make constexpr functions that return vector types. This requires type
  punning with a union, which is not allowed in constexpr functions under C++17.
  It may be possible under C++20

*****************************************************************************/

// Define type for Encapsulated array to use as return type:
template <typename T, int N>
struct EList {
    T a[N];
};


// get_inttype: get an integer of a size that matches the element size 
// of vector class V with the value -1
template <typename V>
constexpr auto get_inttype() {
    constexpr int elementsize = sizeof(V) / V::size();  // size of vector elements

    if constexpr (elementsize >= 8) {
        return -int64_t(1);
    }
    else if constexpr (elementsize >= 4) {
        return int32_t(-1);
    }
    else if constexpr (elementsize >= 2) {
        return int16_t(-1);
    }
    else {
        return int8_t(-1);
    }
}


// zero_mask: return a compact bit mask for zeroing using AVX512 mask.
// Parameter a is a reference to a constexpr int array of permutation indexes
template <int N>
constexpr auto zero_mask(int const (&a)[N]) {
    uint64_t mask = 0;
    int i = 0;

    for (i = 0; i < N; i++) {
        if (a[i] >= 0) mask |= uint64_t(1) << i;
    }
    if constexpr      (N <= 8 ) return uint8_t(mask);
    else if constexpr (N <= 16) return uint16_t(mask);
    else if constexpr (N <= 32) return uint32_t(mask);
    else return mask;
}


// zero_mask_broad: return a broad byte mask for zeroing.
// Parameter a is a reference to a constexpr int array of permutation indexes
template <typename V>
constexpr auto zero_mask_broad(int const (&A)[V::size()]) {
    constexpr int N = V::size();                 // number of vector elements
    typedef decltype(get_inttype<V>()) Etype;    // element type
    EList <Etype, N> u = {{0}};                  // list for return
    int i = 0;
    for (i = 0; i < N; i++) {
        u.a[i] = A[i] >= 0 ? get_inttype<V>() : 0;    
    }
    return u;                                    // return encapsulated array
}


// make_bit_mask: return a compact mask of bits from a list of N indexes:
// B contains options indicating how to gather the mask
// bit 0-7 in B indicates which bit in each index to collect
// bit 8 = 0x100:  set 1 in the lower half of the bit mask if the indicated bit is 1.
// bit 8 = 0    :  set 1 in the lower half of the bit mask if the indicated bit is 0.
// bit 9 = 0x200:  set 1 in the upper half of the bit mask if the indicated bit is 1.
// bit 9 = 0    :  set 1 in the upper half of the bit mask if the indicated bit is 0.
// bit 10 = 0x400: set 1 in the bit mask if the corresponding index is -1 or V_DC
// Parameter a is a reference to a constexpr int array of permutation indexes
template <int N, int B>
constexpr uint64_t make_bit_mask(int const (&a)[N]) {
    uint64_t r = 0;                              // return value
    uint8_t  j = uint8_t(B);                     // index to selected bit
    uint64_t s = 0;                              // bit number i in r
    uint64_t f = 0;                              // 1 if bit not flipped
    int i = 0;
    for (i = 0; i < N; i++) {
        int ix = a[i];
        if (ix < 0) {                            // -1 or V_DC
            s = (B >> 10) & 1;
        }
        else {
            s = ((uint32_t)ix >> j) & 1;         // extract selected bit
            if (i < N/2) {
                f = (B >> 8) & 1;                // lower half
            }
            else {
                f = (B >> 9) & 1;                // upper half
            }
            s ^= f ^ 1;                          // flip bit if needed
        }
        r |= uint64_t(s) << i;                   // set bit in return value
    }
    return r;
}


// make_broad_mask: Convert a bit mask m to a broad mask
// The return value will be a broad boolean mask with elementsize matching vector class V
template <typename V>
constexpr auto make_broad_mask(uint64_t const m) {
    constexpr int N = V::size();                 // number of vector elements
    typedef decltype(get_inttype<V>()) Etype;    // element type
    EList <Etype, N> u = {{0}};                  // list for returning
    int i = 0;
    for (i = 0; i < N; i++) {
        u.a[i] = ((m >> i) & 1) != 0 ? get_inttype<V>() : 0;    
    }
    return u;                                    // return encapsulated array
}


// perm_mask_broad: return a mask for permutation by a vector register index.
// Parameter A is a reference to a constexpr int array of permutation indexes
template <typename V>
constexpr auto perm_mask_broad(int const (&A)[V::size()]) {
    constexpr int N = V::size();                 // number of vector elements
    typedef decltype(get_inttype<V>()) Etype;    // vector element type
    EList <Etype, N> u = {{0}};                  // list for returning
    int i = 0;
    for (i = 0; i < N; i++) {
        u.a[i] = Etype(A[i]);
    }
    return u;                                    // return encapsulated array
}


// perm_flags: returns information about how a permute can be implemented.
// The return value is composed of these flag bits:
const int perm_zeroing             = 1;  // needs zeroing
const int perm_perm                = 2;  // permutation needed
const int perm_allzero             = 4;  // all is zero or don't care
const int perm_largeblock          = 8;  // fits permute with a larger block size (e.g permute Vec2q instead of Vec4i)
const int perm_addz             = 0x10;  // additional zeroing needed after permute with larger block size or shift
const int perm_addz2            = 0x20;  // additional zeroing needed after perm_zext, perm_compress, or perm_expand
const int perm_cross_lane       = 0x40;  // permutation crossing 128-bit lanes
const int perm_same_pattern     = 0x80;  // same permute pattern in all 128-bit lanes
const int perm_punpckh         = 0x100;  // permutation pattern fits punpckh instruction
const int perm_punpckl         = 0x200;  // permutation pattern fits punpckl instruction
const int perm_rotate          = 0x400;  // permutation pattern fits rotation within lanes. 4 bit count returned in bit perm_rot_count
const int perm_shright        = 0x1000;  // permutation pattern fits shift right within lanes. 4 bit count returned in bit perm_rot_count
const int perm_shleft         = 0x2000;  // permutation pattern fits shift left within lanes. negative count returned in bit perm_rot_count
const int perm_rotate_big     = 0x4000;  // permutation pattern fits rotation across lanes. 6 bit count returned in bit perm_rot_count
const int perm_broadcast      = 0x8000;  // permutation pattern fits broadcast of a single element.
const int perm_zext          = 0x10000;  // permutation pattern fits zero extension
const int perm_compress      = 0x20000;  // permutation pattern fits vpcompress instruction
const int perm_expand        = 0x40000;  // permutation pattern fits vpexpand instruction
const int perm_outofrange = 0x10000000;  // index out of range
const int perm_rot_count          = 32;  // rotate or shift count is in bits perm_rot_count to perm_rot_count+3
const int perm_ipattern           = 40;  // pattern for pshufd is in bit perm_ipattern to perm_ipattern + 7 if perm_same_pattern and elementsize >= 4

template <typename V>
constexpr uint64_t perm_flags(int const (&a)[V::size()]) {
    // a is a reference to a constexpr array of permutation indexes
    // V is a vector class
    constexpr int N = V::size();                           // number of elements
    uint64_t r = perm_largeblock | perm_same_pattern | perm_allzero; // return value
    uint32_t i = 0;                                        // loop counter
    int      j = 0;                                        // loop counter
    int ix = 0;                                            // index number i
    const uint32_t nlanes = sizeof(V) / 16;                // number of 128-bit lanes
    const uint32_t lanesize = N / nlanes;                  // elements per lane
    const uint32_t elementsize = sizeof(V) / N;            // size of each vector element
    uint32_t lane = 0;                                     // current lane
    uint32_t rot = 999;                                    // rotate left count
    int32_t  broadc = 999;                                 // index to broadcasted element
    uint32_t patfail = 0;                                  // remember certain patterns that do not fit
    uint32_t addz2 = 0;                                    // remember certain patterns need extra zeroing
    int32_t  compresslasti = -1;                           // last index in perm_compress fit 
    int32_t  compresslastp = -1;                           // last position in perm_compress fit 
    int32_t  expandlasti = -1;                             // last index in perm_expand fit 
    int32_t  expandlastp = -1;                             // last position in perm_expand fit 

    int lanepattern[lanesize] = {0};                       // pattern in each lane

    for (i = 0; i < N; i++) {                              // loop through indexes
        ix = a[i];                                         // current index
        // meaning of ix: -1 = set to zero, V_DC = don't care, non-negative value = permute.
        if (ix == -1) {
            r |= perm_zeroing;                             // zeroing requested
        }
        else if (ix != V_DC && uint32_t(ix) >= N) {
            r |= perm_outofrange;                          // index out of range
        }
        if (ix >= 0) {
            r &= ~ perm_allzero;                           // not all zero
            if (ix != (int)i) r |= perm_perm;              // needs permutation
            if (broadc == 999) broadc = ix;                // remember broadcast index
            else if (broadc != ix) broadc = 1000;          // does not fit broadcast
        }
        // check if pattern fits a larger block size:
        // even indexes must be even, odd indexes must fit the preceding even index + 1
        if ((i & 1) == 0) {                                // even index
            if (ix >= 0 && (ix & 1)) r &= ~perm_largeblock;// not even. does not fit larger block size 
            int iy = a[i + 1];                             // next odd index
            if (iy >= 0 && (iy & 1) == 0) r &= ~ perm_largeblock; // not odd. does not fit larger block size 
            if (ix >= 0 && iy >= 0 && iy != ix+1) r &= ~ perm_largeblock; // does not fit preceding index + 1
            if (ix == -1 && iy >= 0) r |= perm_addz;       // needs additional zeroing at current block size
            if (iy == -1 && ix >= 0) r |= perm_addz;       // needs additional zeroing at current block size
        } 
        lane = i / lanesize;                               // current lane
        if (lane == 0) {                                   // first lane, or no pattern yet
            lanepattern[i] = ix;                           // save pattern
        }
        // check if crossing lanes
        if (ix >= 0) {
            uint32_t lanei = (uint32_t)ix / lanesize;      // source lane
            if (lanei != lane) r |= perm_cross_lane;       // crossing lane
        }
        // check if same pattern in all lanes
        if (lane != 0 && ix >= 0) {                        // not first lane
            int j  = i - int(lane * lanesize);             // index into lanepattern
            int jx = ix - int(lane * lanesize);            // pattern within lane
            if (jx < 0 || jx >= (int)lanesize) r &= ~perm_same_pattern; // source is in another lane
            if (lanepattern[j] < 0) {
                lanepattern[j] = jx;                       // pattern not known from previous lane
            }
            else {
                if (lanepattern[j] != jx) r &= ~perm_same_pattern; // not same pattern
            }
        }
        if (ix >= 0) {
            // check if pattern fits zero extension (perm_zext)
            if (uint32_t(ix*2) != i) {
                patfail |= 1;                              // does not fit zero extension
            }
            // check if pattern fits compress (perm_compress)
            if (ix > compresslasti && ix - compresslasti >= (int)i - compresslastp) {
                if ((int)i - compresslastp > 1) addz2 |= 2;// perm_compress may need additional zeroing 
                compresslasti = ix;  compresslastp = i;
            }
            else {
                patfail |= 2;                              // does not fit perm_compress
            }
            // check if pattern fits expand (perm_expand)
            if (ix > expandlasti && ix - expandlasti <= (int)i - expandlastp) {
                if (ix - expandlasti > 1) addz2 |= 4;      // perm_expand may need additional zeroing 
                expandlasti = ix;  expandlastp = i;
            }
            else {
                patfail |= 4;                              // does not fit perm_compress
            }
        }
        else if (ix == -1) {
            if ((i & 1) == 0) addz2 |= 1;                  // zero extension needs additional zeroing
        }
    }
    if (!(r & perm_perm)) return r;                        // more checks are superfluous

    if (!(r & perm_largeblock)) r &= ~ perm_addz;          // remove irrelevant flag
    if (r & perm_cross_lane) r &= ~ perm_same_pattern;     // remove irrelevant flag
    if ((patfail & 1) == 0) {
        r |= perm_zext;                                    // fits zero extension
        if ((addz2 & 1) != 0) r |= perm_addz2;
    }
    else if ((patfail & 2) == 0) {
        r |= perm_compress;                                // fits compression
        if ((addz2 & 2) != 0) {                            // check if additional zeroing needed
            for (j = 0; j < compresslastp; j++) {
                if (a[j] == -1) r |= perm_addz2;
            }            
        }
    }
    else if ((patfail & 4) == 0) {
        r |= perm_expand;                                  // fits expansion
        if ((addz2 & 4) != 0) {                            // check if additional zeroing needed
            for (j = 0; j < expandlastp; j++) {
                if (a[j] == -1) r |= perm_addz2;
            }            
        }
    }

    if (r & perm_same_pattern) {
        // same pattern in all lanes. check if it fits specific patterns
        bool fit = true;
        // fit shift or rotate
        for (i = 0; i < lanesize; i++) {
            if (lanepattern[i] >= 0) {
                uint32_t rot1 = uint32_t(lanepattern[i] + lanesize - i) % lanesize;
                if (rot == 999) {
                    rot = rot1;
                }
                else { // check if fit
                    if (rot != rot1) fit = false;
                }
            } 
        }
        rot &= lanesize-1;  // prevent out of range values
        if (fit) {   // fits rotate, and possibly shift
            uint64_t rot2 = (rot * elementsize) & 0xF;     // rotate right count in bytes
            r |= rot2 << perm_rot_count;                   // put shift/rotate count in output bit 16-19
#if INSTRSET >= 4  // SSSE3
            r |= perm_rotate;                              // allow palignr
#endif
            // fit shift left
            fit = true;
            for (i = 0; i < lanesize-rot; i++) {           // check if first rot elements are zero or don't care
                if (lanepattern[i] >= 0) fit = false;                
            }
            if (fit) {
                r |= perm_shleft;
                for (; i < lanesize; i++) if (lanepattern[i] == -1) r |= perm_addz; // additional zeroing needed
            }
            // fit shift right
            fit = true;
            for (i = lanesize-(uint32_t)rot; i < lanesize; i++) {    // check if last (lanesize-rot) elements are zero or don't care
                if (lanepattern[i] >= 0) fit = false;
            }
            if (fit) {
                r |= perm_shright;
                for (i = 0; i < lanesize-rot; i++) {
                    if (lanepattern[i] == -1) r |= perm_addz; // additional zeroing needed
                }
            }
        }    
        // fit punpckhi
        fit = true;
        uint32_t j = lanesize / 2;
        for (i = 0; i < lanesize; i++) {
            if (lanepattern[i] >= 0 && lanepattern[i] != (int)j) fit = false;
            if ((i & 1) != 0) j++;
        }
        if (fit) r |= perm_punpckh;
        // fit punpcklo
        fit = true;
        j = 0;
        for (i = 0; i < lanesize; i++) {
            if (lanepattern[i] >= 0 && lanepattern[i] != (int)j) fit = false;
            if ((i & 1) != 0) j++;
        }
        if (fit) r |= perm_punpckl;
        // fit pshufd
        if (elementsize >= 4) {
            uint64_t p = 0;
            for (i = 0; i < lanesize; i++) {
                if (lanesize == 4) {
                    p |= (lanepattern[i] & 3) << 2 * i;
                }
                else {  // lanesize = 2
                    p |= ((lanepattern[i] & 1) * 10 + 4) << 4 * i;
                }
            }
            r |= p << perm_ipattern;
        }
    }
#if INSTRSET >= 7
    else {  // not same pattern in all lanes
        if constexpr (nlanes > 1) {                        // Try if it fits big rotate
            for (i = 0; i < N; i++) {
                ix = a[i];
                if (ix >= 0) {
                    uint32_t rot2 = (ix + N - i) % N;      // rotate count
                    if (rot == 999) {
                        rot = rot2;                        // save rotate count
                    }
                    else if (rot != rot2) {
                        rot = 1000; break;                 // does not fit big rotate
                    }
                }
            }
            if (rot < N) {                                 // fits big rotate
                r |= perm_rotate_big | (uint64_t)rot << perm_rot_count;
            }
        }
    }
#endif
    if (broadc < 999 && (r & (perm_rotate|perm_shright|perm_shleft|perm_rotate_big)) == 0) { 
        r |= perm_broadcast | (uint64_t)broadc << perm_rot_count; // fits broadcast
    }
    return r;
}


// compress_mask: returns a bit mask to use for compression instruction.
// It is presupposed that perm_flags indicates perm_compress.
// Additional zeroing is needed if perm_flags indicates perm_addz2
template <int N>
constexpr uint64_t compress_mask(int const (&a)[N]) {
    // a is a reference to a constexpr array of permutation indexes
    int ix = 0, lasti = -1, lastp = -1;
    uint64_t m = 0;
    int i = 0; int j = 1;                                  // loop counters
    for (i = 0; i < N; i++) {
        ix = a[i];                                         // permutation index
        if (ix >= 0) {
            m |= (uint64_t)1 << ix;                        // mask for compression source
            for (j = 1; j < i - lastp; j++) {
                m |= (uint64_t)1 << (lasti + j);           // dummy filling source
            }
            lastp = i; lasti = ix;
        }
    }
    return m;
}

// expand_mask: returns a bit mask to use for expansion instruction.
// It is presupposed that perm_flags indicates perm_expand.
// Additional zeroing is needed if perm_flags indicates perm_addz2
template <int N>
constexpr uint64_t expand_mask(int const (&a)[N]) {
    // a is a reference to a constexpr array of permutation indexes
    int ix = 0, lasti = -1, lastp = -1;
    uint64_t m = 0;
    int i = 0; int j = 1;
    for (i = 0; i < N; i++) {
        ix = a[i];                                         // permutation index
        if (ix >= 0) {
            m |= (uint64_t)1 << i;                         // mask for expansion destination
            for (j = 1; j < ix - lasti; j++) {
                m |= (uint64_t)1 << (lastp + j);           // dummy filling destination
            }
            lastp = i; lasti = ix;
        }
    }
    return m;
}

// perm16_flags: returns information about how to permute a vector of 16-bit integers
// Note: It is presupposed that perm_flags reports perm_same_pattern
// The return value is composed of these bits:
// 1:  data from low  64 bits to low  64 bits. pattern in bit 32-39
// 2:  data from high 64 bits to high 64 bits. pattern in bit 40-47
// 4:  data from high 64 bits to low  64 bits. pattern in bit 48-55
// 8:  data from low  64 bits to high 64 bits. pattern in bit 56-63
template <typename V>
constexpr uint64_t perm16_flags(int const (&a)[V::size()]) {
    // a is a reference to a constexpr array of permutation indexes
    // V is a vector class
    constexpr int N = V::size();                           // number of elements

    uint64_t retval = 0;                                   // return value
    uint32_t pat[4] = {0,0,0,0};                           // permute patterns
    uint32_t i = 0;                                        // loop counter
    int ix = 0;                                            // index number i
    const uint32_t lanesize = 8;                           // elements per lane
    uint32_t lane = 0;                                     // current lane
    int lanepattern[lanesize] = {0};                       // pattern in each lane

    for (i = 0; i < N; i++) {
        ix = a[i];
        lane = i / lanesize;                               // current lane
        if (lane == 0) {
            lanepattern[i] = ix;                           // save pattern
        }
        else if (ix >= 0) {                                // not first lane
            uint32_t j = i - lane * lanesize;              // index into lanepattern
            int jx = ix - lane * lanesize;                 // pattern within lane
            if (lanepattern[j] < 0) {
                lanepattern[j] = jx;                       // pattern not known from previous lane
            } 
        }
    }
    // four patterns: low2low, high2high, high2low, low2high
    for (i = 0; i < 4; i++) {
        // loop through low pattern
        if (lanepattern[i] >= 0) {
            if (lanepattern[i] < 4) { // low2low
                retval |= 1;
                pat[0] |= uint32_t(lanepattern[i] & 3) << (2 * i);
            }
            else {  // high2low
                retval |= 4;
                pat[2] |= uint32_t(lanepattern[i] & 3) << (2 * i);
            }
        }
        // loop through high pattern
        if (lanepattern[i+4] >= 0) {
            if (lanepattern[i+4] < 4) { // low2high
                retval |= 8;
                pat[3] |= uint32_t(lanepattern[i+4] & 3) << (2 * i);
            }
            else {  // high2high
                retval |= 2;
                pat[1] |= uint32_t(lanepattern[i+4] & 3) << (2 * i);
            }
        }
    }
    // join return data
    for (i = 0; i < 4; i++) {
        retval |= (uint64_t)pat[i] << (32 + i*8);
    }
    return retval;
}


// pshufb_mask: return a broad byte mask for permutation within lanes
// for use with the pshufb instruction (_mm..._shuffle_epi8).
// The pshufb instruction provides fast permutation and zeroing,
// allowing different patterns in each lane but no crossing of lane boundaries
template <typename V, int oppos = 0>
constexpr auto pshufb_mask(int const (&A)[V::size()]) {
    // Parameter a is a reference to a constexpr array of permutation indexes
    // V is a vector class
    // oppos = 1 for data from the opposite 128-bit lane in 256-bit vectors
    constexpr uint32_t N = V::size();                      // number of vector elements
    constexpr uint32_t elementsize = sizeof(V) / N;        // size of each vector element
    constexpr uint32_t nlanes = sizeof(V) / 16;            // number of 128 bit lanes in vector
    constexpr uint32_t elements_per_lane = N / nlanes;     // number of vector elements per lane

    EList <int8_t, sizeof(V)> u = {{0}};                   // list for returning

    uint32_t i = 0;                                        // loop counters
    uint32_t j = 0;
    int m = 0;
    int k = 0;
    uint32_t lane = 0;

    for (lane = 0; lane < nlanes; lane++) {                // loop through lanes
        for (i = 0; i < elements_per_lane; i++) {          // loop through elements in lane
            // permutation index for element within lane
            int8_t p = -1;
            int ix = A[m];
            if (ix >= 0) {
                ix ^= oppos * elements_per_lane;           // flip bit if opposite lane
            }
            ix -= int(lane * elements_per_lane);           // index relative to lane
            if (ix >= 0 && ix < (int)elements_per_lane) {  // index points to desired lane
                p = ix * elementsize;
            }
            for (j = 0; j < elementsize; j++) {            // loop through bytes in element
                u.a[k++] = p < 0 ? -1 : p + j;             // store byte permutation index
            }
            m++;
        }
    }
    return u;                                              // return encapsulated array
}


// largeblock_perm: return indexes for replacing a permute or blend with 
// a certain block size by a permute or blend with the double block size. 
// Note: it is presupposed that perm_flags() indicates perm_largeblock
// It is required that additional zeroing is added if perm_flags() indicates perm_addz
template <int N>
constexpr EList<int, N/2> largeblock_perm(int const (&a)[N]) {
    // Parameter a is a reference to a constexpr array of permutation indexes
    EList<int, N/2> list = {{0}};                 // result indexes
    int ix = 0;                                  // even index
    int iy = 0;                                  // odd index
    int iz = 0;                                  // combined index
    bool fit_addz = false;                       // additional zeroing needed at the lower block level
    int i = 0;                                   // loop counter

    // check if additional zeroing is needed at current block size
    for (i = 0; i < N; i += 2) {
        ix = a[i];                               // even index
        iy = a[i+1];                             // odd index
        if ((ix == -1 && iy >= 0) || (iy == -1 && ix >= 0)) {
            fit_addz = true;
        }
    }

    // loop through indexes
    for (i = 0; i < N; i += 2) {
        ix = a[i];                               // even index
        iy = a[i+1];                             // odd index
        if (ix >= 0) {
            iz = ix / 2;                         // half index
        }
        else if (iy >= 0) {
            iz = iy / 2;
        }
        else {
            iz = ix | iy;                        // -1 or V_DC. -1 takes precedence
            if (fit_addz) iz = V_DC;             // V_DC, because result will be zeroed later
        }
        list.a[i/2] = iz;                        // save to list
    }
    return list;
}


// blend_flags: returns information about how a blend function can be implemented
// The return value is composed of these flag bits:
const int blend_zeroing            = 1;  // needs zeroing
const int blend_allzero            = 2;  // all is zero or don't care
const int blend_largeblock         = 4;  // fits blend with a larger block size (e.g permute Vec2q instead of Vec4i)
const int blend_addz               = 8;  // additional zeroing needed after blend with larger block size or shift
const int blend_a               = 0x10;  // has data from a
const int blend_b               = 0x20;  // has data from b
const int blend_perma           = 0x40;  // permutation of a needed
const int blend_permb           = 0x80;  // permutation of b needed
const int blend_cross_lane     = 0x100;  // permutation crossing 128-bit lanes
const int blend_same_pattern   = 0x200;  // same permute/blend pattern in all 128-bit lanes
const int blend_punpckhab     = 0x1000;  // pattern fits punpckh(a,b)
const int blend_punpckhba     = 0x2000;  // pattern fits punpckh(b,a)
const int blend_punpcklab     = 0x4000;  // pattern fits punpckl(a,b)
const int blend_punpcklba     = 0x8000;  // pattern fits punpckl(b,a)
const int blend_rotateab     = 0x10000;  // pattern fits palignr(a,b)
const int blend_rotateba     = 0x20000;  // pattern fits palignr(b,a)
const int blend_shufab       = 0x40000;  // pattern fits shufps/shufpd(a,b)
const int blend_shufba       = 0x80000;  // pattern fits shufps/shufpd(b,a)
const int blend_rotate_big  = 0x100000;  // pattern fits rotation across lanes. count returned in bits blend_rotpattern
const int blend_outofrange= 0x10000000;  // index out of range
const int blend_shufpattern       = 32;  // pattern for shufps/shufpd is in bit blend_shufpattern to blend_shufpattern + 7
const int blend_rotpattern        = 40;  // pattern for palignr is in bit blend_rotpattern to blend_rotpattern + 7

template <typename V>
constexpr uint64_t blend_flags(int const (&a)[V::size()]) {
    // a is a reference to a constexpr array of permutation indexes
    // V is a vector class
    constexpr int N = V::size();                           // number of elements
    uint64_t r = blend_largeblock | blend_same_pattern | blend_allzero; // return value
    uint32_t iu = 0;                                       // loop counter
    int32_t ii = 0;                                        // loop counter
    int ix = 0;                                            // index number i
    const uint32_t nlanes = sizeof(V) / 16;                // number of 128-bit lanes
    const uint32_t lanesize = N / nlanes;                  // elements per lane
    uint32_t lane = 0;                                     // current lane
    uint32_t rot = 999;                                    // rotate left count
    int lanepattern[lanesize] = {0};                       // pattern in each lane
    if (lanesize == 2 && N <= 8) {
        r |= blend_shufab | blend_shufba;                  // check if it fits shufpd
    }

    for (ii = 0; ii < N; ii++) {                           // loop through indexes
        ix = a[ii];                                        // index
        if (ix < 0) {
            if (ix == -1) r |= blend_zeroing;              // set to zero
            else if (ix != V_DC) {
                r = blend_outofrange;  break;              // illegal index
            }
        }
        else {  // ix >= 0
            r &= ~ blend_allzero;
            if (ix < N) {
                r |= blend_a;                              // data from a
                if (ix != ii) r |= blend_perma;            // permutation of a
            }
            else if (ix < 2*N) {
                r |= blend_b;                              // data from b
                if (ix != ii + N) r |= blend_permb;        // permutation of b
            }
            else {
                r = blend_outofrange;  break;              // illegal index
            }
        }
        // check if pattern fits a larger block size:
        // even indexes must be even, odd indexes must fit the preceding even index + 1
        if ((ii & 1) == 0) {                               // even index
            if (ix >= 0 && (ix&1)) r &= ~blend_largeblock; // not even. does not fit larger block size 
            int iy = a[ii+1];                              // next odd index
            if (iy >= 0 && (iy & 1) == 0) r &= ~ blend_largeblock; // not odd. does not fit larger block size 
            if (ix >= 0 && iy >= 0 && iy != ix+1) r &= ~ blend_largeblock; // does not fit preceding index + 1
            if (ix == -1 && iy >= 0) r |= blend_addz;      // needs additional zeroing at current block size
            if (iy == -1 && ix >= 0) r |= blend_addz;      // needs additional zeroing at current block size
        } 
        lane = (uint32_t)ii / lanesize;                    // current lane
        if (lane == 0) {                                   // first lane, or no pattern yet
            lanepattern[ii] = ix;                          // save pattern
        }
        // check if crossing lanes
        if (ix >= 0) {
            uint32_t lanei = uint32_t(ix & ~N) / lanesize; // source lane
            if (lanei != lane) {
                r |= blend_cross_lane;                     // crossing lane                 
            }
            if (lanesize == 2) {   // check if it fits pshufd
                if (lanei != lane) r &= ~(blend_shufab | blend_shufba);
                if ((((ix & N) != 0) ^ ii) & 1) r &= ~blend_shufab;
                else r &= ~blend_shufba;
            }
        }
        // check if same pattern in all lanes
        if (lane != 0 && ix >= 0) {                        // not first lane
            int j  = ii - int(lane * lanesize);            // index into lanepattern
            int jx = ix - int(lane * lanesize);            // pattern within lane
            if (jx < 0 || (jx & ~N) >= (int)lanesize) r &= ~blend_same_pattern; // source is in another lane
            if (lanepattern[j] < 0) {
                lanepattern[j] = jx;                       // pattern not known from previous lane
            }
            else {
                if (lanepattern[j] != jx) r &= ~blend_same_pattern; // not same pattern
            }
        }
    }
    if (!(r & blend_largeblock)) r &= ~ blend_addz;        // remove irrelevant flag
    if (r & blend_cross_lane) r &= ~ blend_same_pattern;   // remove irrelevant flag
    if (!(r & (blend_perma | blend_permb))) {
        return r;                                          // no permutation. more checks are superfluous
    }
    if (r & blend_same_pattern) {
        // same pattern in all lanes. check if it fits unpack patterns
        r |= blend_punpckhab | blend_punpckhba | blend_punpcklab | blend_punpcklba;
        for (iu = 0; iu < lanesize; iu++) {                // loop through lanepattern
            ix = lanepattern[iu];
            if (ix >= 0) {
                if ((uint32_t)ix != iu / 2 + (iu & 1) * N)                    r &= ~ blend_punpcklab;
                if ((uint32_t)ix != iu / 2 + ((iu & 1) ^ 1) * N)              r &= ~ blend_punpcklba;
                if ((uint32_t)ix != (iu + lanesize) / 2 + (iu & 1) * N)       r &= ~ blend_punpckhab;
                if ((uint32_t)ix != (iu + lanesize) / 2 + ((iu & 1) ^ 1) * N) r &= ~ blend_punpckhba;
            }
        }
#if INSTRSET >= 4  // SSSE3. check if it fits palignr 
        for (iu = 0; iu < lanesize; iu++) {
            ix = lanepattern[iu];
            if (ix >= 0) {
                uint32_t t = ix & ~N;
                if (ix & N) t += lanesize;
                uint32_t tb = (t + 2*lanesize - iu) % (lanesize * 2);
                if (rot == 999) {
                    rot = tb;
                }
                else { // check if fit
                    if (rot != tb) rot = 1000;
                }
            }
        }
        if (rot < 999) { // firs palignr
            if (rot < lanesize) {
                r |= blend_rotateba;
            }
            else {
                r |= blend_rotateab;
            }
            const uint32_t elementsize = sizeof(V) / N;
            r |= uint64_t((rot & (lanesize - 1)) * elementsize) << blend_rotpattern;
        }
#endif
        if (lanesize == 4) {
            // check if it fits shufps
            r |= blend_shufab | blend_shufba;
            for (ii = 0; ii < 2; ii++) {
                ix = lanepattern[ii];
                if (ix >= 0) {
                    if (ix & N) r &= ~ blend_shufab;
                    else        r &= ~ blend_shufba;
                }
            }
            for (; ii < 4; ii++) {
                ix = lanepattern[ii];
                if (ix >= 0) {
                    if (ix & N) r &= ~ blend_shufba;
                    else        r &= ~ blend_shufab;
                }
            }
            if (r & (blend_shufab | blend_shufba)) {       // fits shufps/shufpd
                uint8_t shufpattern = 0;                   // get pattern
                for (iu = 0; iu < lanesize; iu++) {
                    shufpattern |= (lanepattern[iu] & 3) << iu * 2;
                }
                r |= (uint64_t)shufpattern << blend_shufpattern; // return pattern
            }
        }
    }
    else if  (nlanes > 1) {  // not same pattern in all lanes
        rot = 999;                                         // check if it fits big rotate
        for (ii = 0; ii < N; ii++) {
            ix = a[ii];
            if (ix >= 0) {
                uint32_t rot2 = (ix + 2 * N - ii) % (2 * N);// rotate count
                if (rot == 999) {
                    rot = rot2;                            // save rotate count
                }
                else if (rot != rot2) {
                    rot = 1000; break;                     // does not fit big rotate
                }
            }
        }
        if (rot < 2 * N) {                                 // fits big rotate
            r |= blend_rotate_big | (uint64_t)rot << blend_rotpattern;
        }
    }
    if (lanesize == 2 && (r & (blend_shufab | blend_shufba))) {  // fits shufpd. Get pattern
        for (ii = 0; ii < N; ii++) {
            r |= uint64_t(a[ii] & 1) << (blend_shufpattern + ii);
        }
    }
    return r;
}

// blend_perm_indexes: return an Indexlist for implementing a blend function as
// two permutations. N = vector size. 
// dozero = 0: let unused elements be don't care. The two permutation results must be blended
// dozero = 1: zero unused elements in each permutation. The two permutation results can be OR'ed
// dozero = 2: indexes that are -1 or V_DC are preserved
template <int N, int dozero>
constexpr EList<int, 2*N> blend_perm_indexes(int const (&a)[N]) {
    // a is a reference to a constexpr array of permutation indexes
    EList<int, 2*N> list = {{0}};       // list to return
    int u = dozero ? -1 : V_DC;        // value to use for unused entries
    int j = 0;

    for (j = 0; j < N; j++) {          // loop through indexes
        int ix = a[j];                 // current index
        if (ix < 0) {                  // zero or don't care
            if (dozero == 2) {
                // list.a[j] = list.a[j + N] = ix;  // fails in gcc in complicated cases
                list.a[j] = ix;
                list.a[j + N] = ix;
            }
            else {
                // list.a[j] = list.a[j + N] = u;
                list.a[j] = u;
                list.a[j + N] = u;
            }
        }
        else if (ix < N) {             // value from a
            list.a[j]   = ix;  
            list.a[j+N] = u;
        }
        else {
            list.a[j]   = u;           // value from b  
            list.a[j+N] = ix - N;
        }
    }
    return list;
}

// largeblock_indexes: return indexes for replacing a permute or blend with a  
// certain block size by a permute or blend with the double block size. 
// Note: it is presupposed that perm_flags or blend_flags indicates _largeblock
// It is required that additional zeroing is added if perm_flags or blend_flags 
// indicates _addz
template <int N>
constexpr EList<int, N/2> largeblock_indexes(int const (&a)[N]) {
    // Parameter a is a reference to a constexpr array of N permutation indexes
    EList<int, N/2> list = {{0}};                 // list to return

    bool fit_addz = false;                       // additional zeroing needed at the lower block level
    int ix = 0;                                  // even index
    int iy = 0;                                  // odd index
    int iz = 0;                                  // combined index
    int i  = 0;                                  // loop counter

    for (i = 0; i < N; i += 2) {
        ix = a[i];                               // even index
        iy = a[i+1];                             // odd index
        if (ix >= 0) {             
            iz = ix / 2;                         // half index
        }
        else if (iy >= 0) {
            iz = iy / 2;                         // half index
        }
        else iz = ix | iy;                       // -1 or V_DC. -1 takes precedence
        list.a[i/2] = iz;                        // save to list
        // check if additional zeroing is needed at current block size
        if ((ix == -1 && iy >= 0) || (iy == -1 && ix >= 0)) {
            fit_addz = true;
        }
    }
    // replace -1 by V_DC if fit_addz
    if (fit_addz) {
        for (i = 0; i < N/2; i++) {
            if (list.a[i] < 0) list.a[i] = V_DC;
        }
    }
    return list;
}


/****************************************************************************************
*
*          Vector blend helper function templates
*
* These templates are for emulating a blend with a vector size that is not supported by 
* the instruction set, using multiple blends or permutations of half the vector size
*
****************************************************************************************/

// Make dummy blend function templates to avoid error messages when the blend functions are not yet defined
template <typename dummy> void blend2(){}
template <typename dummy> void blend4(){}
template <typename dummy> void blend8(){}
template <typename dummy> void blend16(){}
template <typename dummy> void blend32(){} 

// blend_half_indexes: return an Indexlist for emulating a blend function as
// blends or permutations from multiple sources
// dozero = 0: let unused elements be don't care. Multiple permutation results must be blended
// dozero = 1: zero unused elements in each permutation. Multiple permutation results can be OR'ed
// dozero = 2: indexes that are -1 or V_DC are preserved
// src1, src2: sources to blend in a partial implementation
template <int N, int dozero, int src1, int src2>
constexpr EList<int, N> blend_half_indexes(int const (&a)[N]) {
    // a is a reference to a constexpr array of permutation indexes
    EList<int, N> list = {{0}};         // list to return
    int u = dozero ? -1 : V_DC;        // value to use for unused entries
    int j = 0;                         // loop counter

    for (j = 0; j < N; j++) {          // loop through indexes
        int ix = a[j];                 // current index
        if (ix < 0) {                  // zero or don't care                
            list.a[j] = (dozero == 2) ? ix : u;
        }
        else {
            int src = ix / N;          // source
            if (src == src1) {
                list.a[j] = ix & (N - 1);
            }
            else if (src == src2) {
                list.a[j] = (ix & (N - 1)) + N;
            }
            else list.a[j] = u;
        }
    }
    return list;
}

// selectblend: select one of four sources for blending
template <typename W, int s>
static inline auto selectblend(W const a, W const b) {
    if      constexpr (s == 0) return a.get_low();
    else if constexpr (s == 1) return a.get_high();
    else if constexpr (s == 2) return b.get_low();
    else                       return b.get_high();
}

// blend_half: Emulate a blend with a vector size that is not supported
// by multiple blends with half the vector size.
// blend_half is called twice, to give the low and high half of the result
// Parameters: W: type of full-size vector
// i0...: indexes for low or high half
// a, b: full size input vectors
// return value: half-size vector for lower or upper part
template <typename W, int ... i0>
auto blend_half(W const& a, W const& b) {
    typedef decltype(a.get_low()) V;             // type for half-size vector
    constexpr int N = V::size();                 // size of half-size vector
    static_assert(sizeof...(i0) == N, "wrong number of indexes in blend_half");
    constexpr int ind[N] = { i0... };            // array of indexes

    // lambda to find which of the four possible sources are used
    // return: EList<int, 5> containing a list of up to 4 sources. The last element is the number of sources used
    auto listsources = [](int const n, int const (&ind)[N]) constexpr {
        bool source_used[4] = { false,false,false,false }; // list of sources used
        int i = 0;
        for (i = 0; i < n; i++) {
            int ix = ind[i];                     // index
            if (ix >= 0) {
                int src = ix / n;                // source used
                source_used[src & 3] = true;
            }
        }
        // return a list of sources used. The last element is the number of sources used
        EList<int, 5> sources = {{0}};
        int nsrc = 0;                            // number of sources
        for (i = 0; i < 4; i++) {
            if (source_used[i]) {
                sources.a[nsrc++] = i;
            }
        }
        sources.a[4] = nsrc;
        return sources;
    };
    // list of sources used
    constexpr EList<int, 5> sources = listsources(N, ind);
    constexpr int nsrc = sources.a[4];           // number of sources used

    if constexpr (nsrc == 0) {                   // no sources
        return V(0);
    }
    // get indexes for the first one or two sources
    constexpr int uindex = (nsrc > 2) ? 1 : 2;   // unused elements set to zero if two blends are combined
    constexpr EList<int, N> L = blend_half_indexes<N, uindex, sources.a[0], sources.a[1]>(ind);
    V x0;
    V src0 = selectblend<W, sources.a[0]>(a, b); // first source
    V src1 = selectblend<W, sources.a[1]>(a, b); // second source
    if constexpr (N == 2) {
        x0 = blend2  <L.a[0], L.a[1]> (src0, src1);
    }
    else if constexpr (N == 4) {
        x0 = blend4  <L.a[0], L.a[1], L.a[2], L.a[3]> (src0, src1);
    }
    else if constexpr (N == 8) {
        x0 = blend8  <L.a[0], L.a[1], L.a[2], L.a[3], L.a[4], L.a[5], L.a[6], L.a[7]> (src0, src1);
    }
    else if constexpr (N == 16) {
        x0 = blend16 <L.a[0], L.a[1], L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
            L.a[8], L.a[9], L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15] > (src0, src1);
    }
    else if constexpr (N == 32) {
        x0 = blend32 <L.a[0], L.a[1],  L.a[2],  L.a[3],  L.a[4],  L.a[5],  L.a[6],  L.a[7],
            L.a[8],  L.a[9],  L.a[10], L.a[11], L.a[12], L.a[13], L.a[14], L.a[15],
            L.a[16], L.a[17], L.a[18], L.a[19], L.a[20], L.a[21], L.a[22], L.a[23],
            L.a[24], L.a[25], L.a[26], L.a[27], L.a[28], L.a[29], L.a[30], L.a[31] > (src0, src1);
    }
    if constexpr (nsrc > 2) {    // get last one or two sources
        constexpr EList<int, N> M = blend_half_indexes<N, 1, sources.a[2], sources.a[3]>(ind);
        V x1;
        V src2 = selectblend<W, sources.a[2]>(a, b);  // third source
        V src3 = selectblend<W, sources.a[3]>(a, b);  // fourth source
        if constexpr (N == 2) {
            x1 = blend2  <M.a[0], M.a[1]> (src0, src1);
        }
        else if constexpr (N == 4) {
            x1 = blend4  <M.a[0], M.a[1], M.a[2], M.a[3]> (src2, src3);
        }
        else if constexpr (N == 8) {
            x1 = blend8  <M.a[0], M.a[1], M.a[2], M.a[3], M.a[4], M.a[5], M.a[6], M.a[7]> (src2, src3);
        }
        else if constexpr (N == 16) {
            x1 = blend16 <M.a[0], M.a[1], M.a[2],  M.a[3],  M.a[4],  M.a[5],  M.a[6],  M.a[7],
                M.a[8], M.a[9], M.a[10], M.a[11], M.a[12], M.a[13], M.a[14], M.a[15] > (src2, src3);
        }
        else if constexpr (N == 32) {
            x1 = blend32 <M.a[0], M.a[1],  M.a[2],   M.a[3],  M.a[4],  M.a[5],  M.a[6],  M.a[7],
                M.a[8], M.a[9],  M.a[10],  M.a[11], M.a[12], M.a[13], M.a[14], M.a[15],
                M.a[16], M.a[17], M.a[18], M.a[19], M.a[20], M.a[21], M.a[22], M.a[23],
                M.a[24], M.a[25], M.a[26], M.a[27], M.a[28], M.a[29], M.a[30], M.a[31] > (src2, src3);
        }   
        x0 |= x1;      // combine result of two blends. Unused elements are zero
    }
    return x0;
}


#ifdef VCL_NAMESPACE
}
#endif 


#endif // INSTRSET_H
