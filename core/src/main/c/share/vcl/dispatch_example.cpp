/*************************  dispatch_example.cpp   ****************************
Author:        Agner Fog
Date created:  2012-05-30
Last modified: 2017-07-01
Version:       2.00.00
Project:       vector class library
Description:   Example of automatic CPU dispatching

# Example of compiling this with GCC compiler:
# Compile dispatch_example.cpp four times for different instruction sets:

# Compile for SSE4.1
g++ -O3 -msse4.1     -std=c++17 -c dispatch_example.cpp -od5.o

# Compile for AVX2
g++ -O3 -mavx2 -mfma -std=c++17 -c dispatch_example.cpp -od8.o

# Compile for AVX512
g++ -O3 -mavx512f -mfma -mavx512vl -mavx512bw -mavx512dq -std=c++17 -c dispatch_example.cpp -od10.o

# The last compilation uses the lowest supported instruction set (SSE2)
# This includes the main program, and links all versions together:
g++ -O3 -msse2 -std=c++17 -otest dispatch_example.cpp instrset_detect.cpp d5.o d8.o d10.o

# Run the program
./test

(c) Copyright 2012-2019 Agner Fog.
Apache License version 2.0 or later.
******************************************************************************/

#include <stdio.h>

#define MAX_VECTOR_SIZE 512

#include "vectorclass.h"

// Define function type
// Change this to fit your purpose. Should not contain vector types:
typedef float MyFuncType(float*);

// function prototypes for each version
MyFuncType  myfunc, myfunc_SSE2, myfunc_SSE41, myfunc_AVX2, myfunc_AVX512, myfunc_dispatch; 

// Define function name depending on which instruction set we compile for
#if   INSTRSET >= 10                   // AVX512VL
#define FUNCNAME myfunc_AVX512
#elif INSTRSET >= 8                    // AVX2
#define FUNCNAME myfunc_AVX2
#elif INSTRSET >= 5                    // SSE4.1
#define FUNCNAME myfunc_SSE41
#elif INSTRSET == 2
#define FUNCNAME myfunc_SSE2           // SSE2
#else 
#error Unsupported instruction set
#endif

// Dispatched version of the function. Compile this once for each instruction set:
float FUNCNAME (float * f) {
    // This example adds 16 floats
    Vec16f a;                          // vector of 16 floats
    a.load(f);                         // load array into vector
    return horizontal_add(a);          // return sum of 16 elements
}


#if INSTRSET == 2
// make dispatcher in only the lowest of the compiled versions

// This function pointer initially points to the dispatcher.
// After the first call it points to the selected version:
MyFuncType * myfunc_pointer = &myfunc_dispatch;            // function pointer

// Dispatcher
float myfunc_dispatch(float * f) {
    int iset = instrset_detect();                          // Detect supported instruction set
    if      (iset >= 10) myfunc_pointer = &myfunc_AVX512;  // AVX512 version
    else if (iset >=  8) myfunc_pointer = &myfunc_AVX2;    // AVX2 version
    else if (iset >=  5) myfunc_pointer = &myfunc_SSE41;   // SSE4.1 version
    else if (iset >=  2) myfunc_pointer = &myfunc_SSE2;    // SSE2 version
    else {
        // Error: lowest instruction set not supported
        fprintf(stderr, "\nError: Instruction set SSE2 not supported on this computer");
        return 0.f;
    }
    // continue in dispatched version of the function
    return (*myfunc_pointer)(f);
}


// Entry to dispatched function call
inline float myfunc(float * f) {
    return (*myfunc_pointer)(f);                           // go to dispatched version
}


// Example: main calls myfunc
int main() {

    float a[16]={1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16};  // array of 16 floats

    float sum = myfunc(a);                                 // call function with dispatching

    printf("\nsum = %8.2f \n", sum);                       // print result
    return 0;
}

#endif  // INSTRSET == 2
