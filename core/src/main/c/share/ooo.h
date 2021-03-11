//
// Created by Alexander on 11/03/2021.
//

#ifndef QUESTDB_OOO_H
#define QUESTDB_OOO_H

#include "jni.h"
#include <cstring>
#include <xmmintrin.h>
#include "util.h"
#include "simd.h"
#include "ooo_multiversion.h"

//#ifdef MV_VANILA
extern "C" {

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_freeMergedIndex(JNIEnv *env, jclass cl, jlong pIndex);

#ifndef ENABLE_MULTIVERSION

#endif

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle32Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle64Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle16Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                             jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_indexReshuffle8Bit(JNIEnv *env, jclass cl, jlong pSrc, jlong pDest, jlong pIndex,
                                            jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle8Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                          jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle16Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle32Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffle64Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest, jlong index,
                                           jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffleWithTop64Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest,
                                                  jlong index, jlong count, jlong topOffset);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffleWithTop32Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest,
                                                  jlong index,
                                                  jlong count, jlong topOffset);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffleWithTop16Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest,
                                                  jlong index,
                                                  jlong count, jlong topOffset);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_mergeShuffleWithTop8Bit(JNIEnv *env, jclass cl, jlong src1, jlong src2, jlong dest,
                                                 jlong index,
                                                 jlong count, jlong topOffset);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_flattenIndex(JNIEnv *env, jclass cl, jlong pIndex,
                                      jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_binarySearch64Bit(JNIEnv *env, jclass cl, jlong pData, jlong value, jlong low,
                                           jlong high, jint scan_dir);

__SIMD_MULTIVERSION__
JNIEXPORT jlong JNICALL
Java_io_questdb_std_Vect_binarySearchIndexT(JNIEnv *env, jclass cl, jlong pData, jlong value, jlong low,
                                            jlong high, jint scan_dir);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_makeTimestampIndex(JNIEnv *env, jclass cl, jlong pData, jlong low,
                                            jlong high, jlong pIndex);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryLong(JNIEnv *env, jclass cl, jlong pData, jlong value,
                                       jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryInt(JNIEnv *env, jclass cl, jlong pData, jint value,
                                      jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryDouble(JNIEnv *env, jclass cl, jlong pData, jdouble value,
                                         jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryFloat(JNIEnv *env, jclass cl, jlong pData, jfloat value,
                                        jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setMemoryShort(JNIEnv *env, jclass cl, jlong pData, jshort value,
                                        jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setVarColumnRefs32Bit(JNIEnv *env, jclass cl, jlong pData, jlong offset,
                                               jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_setVarColumnRefs64Bit(JNIEnv *env, jclass cl, jlong pData, jlong offset,
                                               jlong count);

__SIMD_MULTIVERSION__
JNIEXPORT void JNICALL
Java_io_questdb_std_Vect_oooCopyIndex(JNIEnv *env, jclass cl, jlong pIndex, jlong index_size,
                                      jlong pDest);

}



#endif //QUESTDB_OOO_H
