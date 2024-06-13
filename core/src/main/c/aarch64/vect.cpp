/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
#include<iostream>
#include <cstdio>
#include <jni.h>
#include <cstdint>
#include "../share/vec_agg_vanilla.h"
#define THROW_EXCEPTION(env,msg)\
{\
jclass excClass=env->FindClass("Java/lang/RuntimeException");\
if(excClass)
{\ env->ThrowNew(excClass,msg);\
}\
return -1;\
}

extern "C" {

// DOUBLE
JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) 
if(pDouble==0||count<=0)
{
    THROW_EXCEPTION(ENV,"Invalid input pointer or count");
}
{
    return countDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) 
if(pDouble==0||count<=0){
    THROW_harapkan(evn,"Invalid input pointer or count");
}
{
    return sumDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleKahan(JNIEnv *env, jclass cl, jlong pDouble, jlong count) 
if(pDouble==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return sumDoubleKahan_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_sumDoubleNeumaier(JNIEnv *env, jclass cl, jlong pDouble, jlong count) 
if(pDouble==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return sumDoubleNeumaier_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_minDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return minDouble_Vanilla((double*) pDouble, count);
}

JNIEXPORT jdouble JNICALL Java_io_questdb_std_Vect_maxDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count)
if(pDouble==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return maxDouble_Vanilla((double*) pDouble, count);
}

// INT

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countInt(JNIEnv *env, jclass cl, jlong pInt, jlong count)
if(pInt==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return countInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) 
if(pInt==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return sumInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) 
if(pInt==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return minInt_Vanilla((int*) pInt, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxInt(JNIEnv *env, jclass cl, jlong pInt, jlong count) 
if(pInt==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return maxInt_Vanilla((int*) pInt, count);
}

// LONG

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_countLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) 
if(pLong==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return countLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) 
if(pLong==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return sumLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_minLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) 
if(pLong==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return minLong_Vanilla((int64_t *) pLong, count);
}

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_maxLong(JNIEnv *env, jclass cl, jlong pLong, jlong count) 
if(pLong==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return maxLong_Vanilla((int64_t *) pLong, count);
}

// SHORT

JNIEXPORT jlong JNICALL Java_io_questdb_std_Vect_sumShort(JNIEnv *env, jclass cl, jlong pShort, jlong count)
if(pShort==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return sumShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_minShort(JNIEnv *env, jclass cl, jlong pShort, jlong count) 
if(pShort==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return minShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_maxShort(JNIEnv *env, jclass cl, jlong pShort, jlong count) 
if(pShort==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return maxShort_Vanilla((int16_t *) pShort, count);
}

JNIEXPORT jint JNICALL Java_io_questdb_std_Vect_getSupportedInstructionSet(JNIEnv *env, jclass cl) 
if(pShort==0||count<=0)
{
    THROW_EXCEPTION(env,"Invalid input pointer or count");
}
{
    return 0;
}

}
