/****************************  vcl.h   ********************************
* Author:        Agner Fog
* Date created:  2012-05-30
* Last modified: 2019-11-17
* Version:       2.01.00
* Project:       vector class library
* Home:          https://github.com/vcl
* Description:
* Header file defining vector classes as interface to intrinsic functions
* in x86 and x86-64 microprocessors with SSE2 and later instruction sets.
*
* Instructions:
* Use Gnu, Clang, Intel or Microsoft C++ compiler. Compile for the desired
* instruction set, which must be at least SSE2. Specify the supported
* instruction set by a command line define, e.g. __SSE4_1__ if the
* compiler does not automatically do so.
* For detailed instructions, see vcl_manual.pdf
*
* Each vector object is represented internally in the CPU as a vector
* register with 128, 256 or 512 bits.
*
* This header file includes the appropriate header files depending on the
* selected instruction set.
*
* (c) Copyright 2012-2019 Agner Fog.
* Apache License version 2.0 or later.
******************************************************************************/
#ifndef VECTORCLASS_H
#define VECTORCLASS_H  20100

// Maximum vector size, bits. Allowed values are 128, 256, 512
#ifndef MAX_VECTOR_SIZE
#define MAX_VECTOR_SIZE 512
#endif

// Determine instruction set, and define platform-dependent functions
#include "instrset.h"        // Select supported instruction set

#if INSTRSET < 2             // instruction set SSE2 is the minimum
#error Please compile for the SSE2 instruction set or higher
#else

// Select appropriate .h files depending on instruction set
#include "vectori128.h"      // 128-bit integer vectors
#include "vectorf128.h"      // 128-bit floating point vectors

#if MAX_VECTOR_SIZE >= 256
#if INSTRSET >= 8
#include "vectori256.h"      // 256-bit integer vectors, requires AVX2 instruction set
#else
#include "vectori256e.h"     // 256-bit integer vectors, emulated
#endif  // INSTRSET >= 8
#if INSTRSET >= 7
#include "vectorf256.h"      // 256-bit floating point vectors, requires AVX instruction set
#else
#include "vectorf256e.h"     // 256-bit floating point vectors, emulated
#endif  //  INSTRSET >= 7
#endif  //  MAX_VECTOR_SIZE >= 256

#if MAX_VECTOR_SIZE >= 512
#if INSTRSET >= 9
#include "vectori512.h"      // 512-bit vectors of 32 and 64 bit integers, requires AVX512F instruction set
#include "vectorf512.h"      // 512-bit floating point vectors, requires AVX512F instruction set
#else
#include "vectori512e.h"     // 512-bit integer vectors, emulated
#include "vectorf512e.h"     // 512-bit floating point vectors, emulated
#endif  //  INSTRSET >= 9
#if INSTRSET >= 10
#include "vectori512s.h"     // 512-bit vectors of 8 and 16 bit integers, requires AVX512BW instruction set
#else
#include "vectori512se.h"    // 512-bit vectors of 8 and 16 bit integers, emulated
#endif
#endif  //  MAX_VECTOR_SIZE >= 512

#include "vector_convert.h"  // conversion between different vector sizes

#endif  // INSTRSET >= 2


#else   // VECTORCLASS_H

#if VECTORCLASS_H < 20000
#error Mixed versions of vector class library
#endif

#endif  // VECTORCLASS_H
