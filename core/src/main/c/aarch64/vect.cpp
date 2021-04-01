/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

#include <cstdio>
#include <jni.h>
#include <cstdint>
#include "../share/vec_agg_vanilla.h"


extern "C" {

// DOUBLE

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleKahan(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDoubleKahan_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleNeumaier(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDoubleNeumaier_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_avgDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return avgDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_minDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return minDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_maxDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return maxDouble_Vanilla((double*) pDouble, count);
}

// INT

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return sumInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_avgInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return avgInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return minInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return maxInt_Vanilla((int*) pInt, count);
}

// LONG

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return sumLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_avgLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return avgLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return minLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) {
    return maxLong_Vanilla((int64_t *) pLong, count);
}

// null check
JNIEXPORT jboolean JNICALL Java_io_questdb_std_Vect_hasNull(JNIEnv *env, jclass cl, jlong pInt, jlong count) {
    return hasNull_Vanilla((int32_t *) pInt, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *env, jclass cl) {
    return 0.0;
}

}
