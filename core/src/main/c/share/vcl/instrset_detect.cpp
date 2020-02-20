/**************************  instrset_detect.cpp   ****************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-08-01
* Version:       2.00.00
* Project:       vector class library
* Description:
* Functions for checking which instruction sets are supported.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/

#include "instrset.h"

#ifdef VCL_NAMESPACE
namespace VCL_NAMESPACE {
#endif


// Define interface to xgetbv instruction
static inline uint64_t xgetbv (int ctr) {	
#if (defined (_MSC_FULL_VER) && _MSC_FULL_VER >= 160040000) || (defined (__INTEL_COMPILER) && __INTEL_COMPILER >= 1200) 
    // Microsoft or Intel compiler supporting _xgetbv intrinsic

    return uint64_t(_xgetbv(ctr));                    // intrinsic function for XGETBV

#elif defined(__GNUC__) ||  defined (__clang__)       // use inline assembly, Gnu/AT&T syntax

   uint32_t a, d;
   __asm("xgetbv" : "=a"(a),"=d"(d) : "c"(ctr) : );
   return a | (uint64_t(d) << 32);

#else  // #elif defined (_WIN32)                      // other compiler. try inline assembly with masm/intel/MS syntax
   uint32_t a, d;
    __asm {
        mov ecx, ctr
        _emit 0x0f
        _emit 0x01
        _emit 0xd0 ; // xgetbv
        mov a, eax
        mov d, edx
    }
   return a | (uint64_t(d) << 32);

#endif
}

/* find supported instruction set
    return value:
    0           = 80386 instruction set
    1  or above = SSE (XMM) supported by CPU (not testing for OS support)
    2  or above = SSE2
    3  or above = SSE3
    4  or above = Supplementary SSE3 (SSSE3)
    5  or above = SSE4.1
    6  or above = SSE4.2
    7  or above = AVX supported by CPU and operating system
    8  or above = AVX2
    9  or above = AVX512F
   10  or above = AVX512VL, AVX512BW, AVX512DQ
*/
int instrset_detect(void) {

    static int iset = -1;                                  // remember value for next call
    if (iset >= 0) {
        return iset;                                       // called before
    }
    iset = 0;                                              // default value
    int abcd[4] = {0,0,0,0};                               // cpuid results
    cpuid(abcd, 0);                                        // call cpuid function 0
    if (abcd[0] == 0) return iset;                         // no further cpuid function supported
    cpuid(abcd, 1);                                        // call cpuid function 1 for feature flags
    if ((abcd[3] & (1 <<  0)) == 0) return iset;           // no floating point
    if ((abcd[3] & (1 << 23)) == 0) return iset;           // no MMX
    if ((abcd[3] & (1 << 15)) == 0) return iset;           // no conditional move
    if ((abcd[3] & (1 << 24)) == 0) return iset;           // no FXSAVE
    if ((abcd[3] & (1 << 25)) == 0) return iset;           // no SSE
    iset = 1;                                              // 1: SSE supported
    if ((abcd[3] & (1 << 26)) == 0) return iset;           // no SSE2
    iset = 2;                                              // 2: SSE2 supported
    if ((abcd[2] & (1 <<  0)) == 0) return iset;           // no SSE3
    iset = 3;                                              // 3: SSE3 supported
    if ((abcd[2] & (1 <<  9)) == 0) return iset;           // no SSSE3
    iset = 4;                                              // 4: SSSE3 supported
    if ((abcd[2] & (1 << 19)) == 0) return iset;           // no SSE4.1
    iset = 5;                                              // 5: SSE4.1 supported
    if ((abcd[2] & (1 << 23)) == 0) return iset;           // no POPCNT
    if ((abcd[2] & (1 << 20)) == 0) return iset;           // no SSE4.2
    iset = 6;                                              // 6: SSE4.2 supported
    if ((abcd[2] & (1 << 27)) == 0) return iset;           // no OSXSAVE
    if ((xgetbv(0) & 6) != 6)       return iset;           // AVX not enabled in O.S.
    if ((abcd[2] & (1 << 28)) == 0) return iset;           // no AVX
    iset = 7;                                              // 7: AVX supported
    cpuid(abcd, 7);                                        // call cpuid leaf 7 for feature flags
    if ((abcd[1] & (1 <<  5)) == 0) return iset;           // no AVX2
    iset = 8;
    if ((abcd[1] & (1 << 16)) == 0) return iset;           // no AVX512
    cpuid(abcd, 0xD);                                      // call cpuid leaf 0xD for feature flags
    if ((abcd[0] & 0x60) != 0x60)   return iset;           // no AVX512
    iset = 9; 
    cpuid(abcd, 7);                                        // call cpuid leaf 7 for feature flags
    if ((abcd[1] & (1 << 31)) == 0) return iset;           // no AVX512VL
    if ((abcd[1] & 0x40020000) != 0x40020000) return iset; // no AVX512BW, AVX512DQ
    iset = 10; 
    return iset;
}

// detect if CPU supports the FMA3 instruction set
bool hasFMA3(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 1);                                        // call cpuid function 1
    return ((abcd[2] & (1 << 12)) != 0);                   // ecx bit 12 indicates FMA3
}

// detect if CPU supports the FMA4 instruction set
bool hasFMA4(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 0x80000001);                               // call cpuid function 0x80000001
    return ((abcd[2] & (1 << 16)) != 0);                   // ecx bit 16 indicates FMA4
}

// detect if CPU supports the XOP instruction set
bool hasXOP(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 0x80000001);                               // call cpuid function 0x80000001
    return ((abcd[2] & (1 << 11)) != 0);                   // ecx bit 11 indicates XOP
}

// detect if CPU supports the F16C instruction set
bool hasF16C(void) {
    if (instrset_detect() < 7) return false;               // must have AVX
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 1);                                        // call cpuid function 1
    return ((abcd[2] & (1 << 29)) != 0);                   // ecx bit 29 indicates F16C
}

// detect if CPU supports the AVX512ER instruction set
bool hasAVX512ER(void) {
    if (instrset_detect() < 9) return false;               // must have AVX512F
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 7);                                        // call cpuid function 7
    return ((abcd[1] & (1 << 27)) != 0);                   // ebx bit 27 indicates AVX512ER
}

// detect if CPU supports the AVX512VBMI instruction set
bool hasAVX512VBMI(void) {
    if (instrset_detect() < 10) return false;              // must have AVX512BW
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 7);                                        // call cpuid function 7
    return ((abcd[2] & (1 << 1)) != 0);                    // ecx bit 1 indicates AVX512VBMI
}

// detect if CPU supports the AVX512VBMI2 instruction set
bool hasAVX512VBMI2(void) {
    if (instrset_detect() < 10) return false;              // must have AVX512BW
    int abcd[4];                                           // cpuid results
    cpuid(abcd, 7);                                        // call cpuid function 7
    return ((abcd[2] & (1 << 6)) != 0);                    // ecx bit 6 indicates AVX512VBMI2
}

#ifdef VCL_NAMESPACE
}
#endif
