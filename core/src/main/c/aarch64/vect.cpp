/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <jni.h>
#include <cstdint>
#include "../share/vec_agg_vanilla.h"

extern "C" {

// DOUBLE
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return countDouble_Vanilla((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDouble_Vanilla((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleAcc(JNIEnv *env, jclass cl, jlong pDouble, jlong count, jlong pAccCount) {
    return sumDoubleAcc_Vanilla((double *) pDouble, count, (int64_t*) pAccCount);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleKahan(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDoubleKahan_Vanilla((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleNeumaier(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDoubleNeumaier_Vanilla((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_minDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return minDouble_Vanilla((double *) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_maxDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return maxDouble_Vanilla((double *) pDouble, count);
}

// INT

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return countInt_Vanilla((int *) pInt, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return sumInt_Vanilla((int *) pInt, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumIntAcc(JNIEnv *env, jclass cl, jlong pInt, jlong count, jlong pAccCount) {
    return sumIntAcc_Vanilla((int32_t *) pInt, count, (int64_t *) pAccCount);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return minInt_Vanilla((int *) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return maxInt_Vanilla((int *) pInt, count);
}

// LONG

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return countLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return sumLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumLongAcc(JNIEnv *env, jclass cl, jlong pLong, jlong count, jlong pAccCount) {
    return sumLongAcc_Vanilla((int64_t *) pLong, count, (int64_t *) pAccCount);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return minLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return maxLong_Vanilla((int64_t *) pLong, count);
}

// SHORT

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumShort(JNIEnv *env, jclass cl, jlong pShort, jlong count) {
    return sumShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minShort(JNIEnv *env, jclass cl, jlong pShort, jlong count) {
    return minShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxShort(JNIEnv *env, jclass cl, jlong pShort, jlong count) {
    return maxShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *env, jclass cl) {
    return 0;
}

// Bitmap-null-aware aggregation functions

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countShortBitmapNull(JNIEnv *env, jclass cl, jlong pBitmap, jlong bitOffset, jlong count) {
    return countShortBitmapNull_Vanilla((uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumShortBitmapNull(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) {
    return sumShortBitmapNull_Vanilla((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumUInt16BitmapNull(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) {
    return sumUInt16BitmapNull_Vanilla((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumUInt32BitmapNull(JNIEnv *env, jclass cl, jlong pInt, jlong pBitmap, jlong bitOffset, jlong count) {
    return sumUInt32BitmapNull_Vanilla((int32_t *) pInt, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minUInt32BitmapNull(JNIEnv *env, jclass cl, jlong pInt, jlong pBitmap, jlong bitOffset, jlong count) {
    return minUInt32BitmapNull_Vanilla((int32_t *) pInt, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxUInt32BitmapNull(JNIEnv *env, jclass cl, jlong pInt, jlong pBitmap, jlong bitOffset, jlong count) {
    return maxUInt32BitmapNull_Vanilla((int32_t *) pInt, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumUInt64BitmapNull(JNIEnv *env, jclass cl, jlong pLong, jlong pBitmap, jlong bitOffset, jlong count) {
    return sumUInt64BitmapNull_Vanilla((int64_t *) pLong, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minUInt64BitmapNull(JNIEnv *env, jclass cl, jlong pLong, jlong pBitmap, jlong bitOffset, jlong count) {
    return minUInt64BitmapNull_Vanilla((int64_t *) pLong, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxUInt64BitmapNull(JNIEnv *env, jclass cl, jlong pLong, jlong pBitmap, jlong bitOffset, jlong count) {
    return maxUInt64BitmapNull_Vanilla((int64_t *) pLong, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minUInt16BitmapNull(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) {
    return minUInt16BitmapNull_Vanilla((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxUInt16BitmapNull(JNIEnv *env, jclass cl, jlong pShort, jlong pBitmap, jlong bitOffset, jlong count) {
    return maxUInt16BitmapNull_Vanilla((int16_t *) pShort, (uint8_t *) pBitmap, bitOffset, count);
}

}
