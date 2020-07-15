//
// Created by blues on 16/06/2020.
//

#ifndef VEC_DISPATCH_H
#define VEC_DISPATCH_H

#define POINTER_NAME(func) func ## _pointer
#define F_AVX512(func) func ## _AVX512
#define F_AVX2(func) func ## _AVX2
#define F_SSE41(func) func ## _SSE41
#define F_SSE2(func) func ## _SSE2
#define F_VANILLA(func) func ## _Vanilla
#define F_DISPATCH(func) func ## _dispatch

#endif //VEC_DISPATCH_H
