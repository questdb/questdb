
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

#include "array.h"
#include "jni.h"
#include "jni_md.h"
#include <assert.h>
#include <cstring>
#include <type_traits>

// line sender execption
void throwLineSenderException(JNIEnv *env, const char *msg) {
  jclass exClass = env->FindClass("io/questdb/LineSenderException");
  env->ThrowNew(exClass, msg);
}

template <int DIM, typename T> struct ProcessArray {
  static jlong process(JNIEnv *env, jlong addr, jobject checkCapacity,
                       jobject array) {
    jsize length = -1;
    for (jsize i = 0, size = env->GetArrayLength(static_cast<jarray>(array));
         i < size; i++) {
      jobject subArray =
          env->GetObjectArrayElement(static_cast<jobjectArray>(array), i);

      if (length == -1) {
        length = env->GetArrayLength(static_cast<jarray>(subArray));
      } else if (length != env->GetArrayLength(static_cast<jarray>(subArray))) {
        throwLineSenderException(env, "array is not regular");
        return 0;
      }

      addr =
          ProcessArray<DIM - 1, T>::process(env, addr, checkCapacity, subArray);
      env->DeleteLocalRef(subArray);
      if (env->ExceptionCheck())
        return 0;
    }

    return addr;
  }
};

template <typename T> struct ProcessArray<1, T> {
  static jlong process(JNIEnv *env, jlong addr, jobject checkCapacity,
                       jobject array) {
    jclass checkCapacityClass = env->GetObjectClass(checkCapacity);
    jmethodID checkMethod =
        env->GetMethodID(checkCapacityClass, "check", "(J)V");
    jsize length = env->GetArrayLength(static_cast<jarray>(array));
    jlong requiredBytes = static_cast<jlong>(length) * sizeof(T);
    env->CallVoidMethod(checkCapacity, checkMethod, requiredBytes);
    if (env->ExceptionCheck())
      return 0;

    T *elements = static_cast<T *>(
        env->GetPrimitiveArrayCritical(static_cast<jarray>(array), nullptr));
    memcpy(reinterpret_cast<void *>(addr), elements, length * sizeof(T));
    env->ReleasePrimitiveArrayCritical(static_cast<jarray>(array), elements,
                                       JNI_ABORT);
    return addr + length * sizeof(T);
  }
};

typedef jlong (*process_array_t)(JNIEnv *env, jlong addr, jobject checkCapacity,
                                 jobject array);

static process_array_t process_arrays[6][32] = {
    // boolean
    {ProcessArray<1, jboolean>::process,  ProcessArray<2, jboolean>::process,
     ProcessArray<3, jboolean>::process,  ProcessArray<4, jboolean>::process,
     ProcessArray<5, jboolean>::process,  ProcessArray<6, jboolean>::process,
     ProcessArray<7, jboolean>::process,  ProcessArray<8, jboolean>::process,
     ProcessArray<9, jboolean>::process,  ProcessArray<10, jboolean>::process,
     ProcessArray<11, jboolean>::process, ProcessArray<12, jboolean>::process,
     ProcessArray<13, jboolean>::process, ProcessArray<14, jboolean>::process,
     ProcessArray<15, jboolean>::process, ProcessArray<16, jboolean>::process,
     ProcessArray<17, jboolean>::process, ProcessArray<18, jboolean>::process,
     ProcessArray<19, jboolean>::process, ProcessArray<20, jboolean>::process,
     ProcessArray<21, jboolean>::process, ProcessArray<22, jboolean>::process,
     ProcessArray<23, jboolean>::process, ProcessArray<24, jboolean>::process,
     ProcessArray<25, jboolean>::process, ProcessArray<26, jboolean>::process,
     ProcessArray<27, jboolean>::process, ProcessArray<28, jboolean>::process,
     ProcessArray<29, jboolean>::process, ProcessArray<30, jboolean>::process,
     ProcessArray<31, jboolean>::process, ProcessArray<32, jboolean>::process},
    // byte
    {ProcessArray<1, jbyte>::process,  ProcessArray<2, jbyte>::process,
     ProcessArray<3, jbyte>::process,  ProcessArray<4, jbyte>::process,
     ProcessArray<5, jbyte>::process,  ProcessArray<6, jbyte>::process,
     ProcessArray<7, jbyte>::process,  ProcessArray<8, jbyte>::process,
     ProcessArray<9, jbyte>::process,  ProcessArray<10, jbyte>::process,
     ProcessArray<11, jbyte>::process, ProcessArray<12, jbyte>::process,
     ProcessArray<13, jbyte>::process, ProcessArray<14, jbyte>::process,
     ProcessArray<15, jbyte>::process, ProcessArray<16, jbyte>::process,
     ProcessArray<17, jbyte>::process, ProcessArray<18, jbyte>::process,
     ProcessArray<19, jbyte>::process, ProcessArray<20, jbyte>::process,
     ProcessArray<21, jbyte>::process, ProcessArray<22, jbyte>::process,
     ProcessArray<23, jbyte>::process, ProcessArray<24, jbyte>::process,
     ProcessArray<25, jbyte>::process, ProcessArray<26, jbyte>::process,
     ProcessArray<27, jbyte>::process, ProcessArray<28, jbyte>::process,
     ProcessArray<29, jbyte>::process, ProcessArray<30, jbyte>::process,
     ProcessArray<31, jbyte>::process, ProcessArray<32, jbyte>::process},
    // int
    {ProcessArray<1, jint>::process,  ProcessArray<2, jint>::process,
     ProcessArray<3, jint>::process,  ProcessArray<4, jint>::process,
     ProcessArray<5, jint>::process,  ProcessArray<6, jint>::process,
     ProcessArray<7, jint>::process,  ProcessArray<8, jint>::process,
     ProcessArray<9, jint>::process,  ProcessArray<10, jint>::process,
     ProcessArray<11, jint>::process, ProcessArray<12, jint>::process,
     ProcessArray<13, jint>::process, ProcessArray<14, jint>::process,
     ProcessArray<15, jint>::process, ProcessArray<16, jint>::process,
     ProcessArray<17, jint>::process, ProcessArray<18, jint>::process,
     ProcessArray<19, jint>::process, ProcessArray<20, jint>::process,
     ProcessArray<21, jint>::process, ProcessArray<22, jint>::process,
     ProcessArray<23, jint>::process, ProcessArray<24, jint>::process,
     ProcessArray<25, jint>::process, ProcessArray<26, jint>::process,
     ProcessArray<27, jint>::process, ProcessArray<28, jint>::process,
     ProcessArray<29, jint>::process, ProcessArray<30, jint>::process,
     ProcessArray<31, jint>::process, ProcessArray<32, jint>::process},
    // long
    {ProcessArray<1, jlong>::process,  ProcessArray<2, jlong>::process,
     ProcessArray<3, jlong>::process,  ProcessArray<4, jlong>::process,
     ProcessArray<5, jlong>::process,  ProcessArray<6, jlong>::process,
     ProcessArray<7, jlong>::process,  ProcessArray<8, jlong>::process,
     ProcessArray<9, jlong>::process,  ProcessArray<10, jlong>::process,
     ProcessArray<11, jlong>::process, ProcessArray<12, jlong>::process,
     ProcessArray<13, jlong>::process, ProcessArray<14, jlong>::process,
     ProcessArray<15, jlong>::process, ProcessArray<16, jlong>::process,
     ProcessArray<17, jlong>::process, ProcessArray<18, jlong>::process,
     ProcessArray<19, jlong>::process, ProcessArray<20, jlong>::process,
     ProcessArray<21, jlong>::process, ProcessArray<22, jlong>::process,
     ProcessArray<23, jlong>::process, ProcessArray<24, jlong>::process,
     ProcessArray<25, jlong>::process, ProcessArray<26, jlong>::process,
     ProcessArray<27, jlong>::process, ProcessArray<28, jlong>::process,
     ProcessArray<29, jlong>::process, ProcessArray<30, jlong>::process,
     ProcessArray<31, jlong>::process, ProcessArray<32, jlong>::process},
    // float
    {ProcessArray<1, jfloat>::process,  ProcessArray<2, jfloat>::process,
     ProcessArray<3, jfloat>::process,  ProcessArray<4, jfloat>::process,
     ProcessArray<5, jfloat>::process,  ProcessArray<6, jfloat>::process,
     ProcessArray<7, jfloat>::process,  ProcessArray<8, jfloat>::process,
     ProcessArray<9, jfloat>::process,  ProcessArray<10, jfloat>::process,
     ProcessArray<11, jfloat>::process, ProcessArray<12, jfloat>::process,
     ProcessArray<13, jfloat>::process, ProcessArray<14, jfloat>::process,
     ProcessArray<15, jfloat>::process, ProcessArray<16, jfloat>::process,
     ProcessArray<17, jfloat>::process, ProcessArray<18, jfloat>::process,
     ProcessArray<19, jfloat>::process, ProcessArray<20, jfloat>::process,
     ProcessArray<21, jfloat>::process, ProcessArray<22, jfloat>::process,
     ProcessArray<23, jfloat>::process, ProcessArray<24, jfloat>::process,
     ProcessArray<25, jfloat>::process, ProcessArray<26, jfloat>::process,
     ProcessArray<27, jfloat>::process, ProcessArray<28, jfloat>::process,
     ProcessArray<29, jfloat>::process, ProcessArray<30, jfloat>::process,
     ProcessArray<31, jfloat>::process, ProcessArray<32, jfloat>::process},
    // double
    {ProcessArray<1, jdouble>::process,  ProcessArray<2, jdouble>::process,
     ProcessArray<3, jdouble>::process,  ProcessArray<4, jdouble>::process,
     ProcessArray<5, jdouble>::process,  ProcessArray<6, jdouble>::process,
     ProcessArray<7, jdouble>::process,  ProcessArray<8, jdouble>::process,
     ProcessArray<9, jdouble>::process,  ProcessArray<10, jdouble>::process,
     ProcessArray<11, jdouble>::process, ProcessArray<12, jdouble>::process,
     ProcessArray<13, jdouble>::process, ProcessArray<14, jdouble>::process,
     ProcessArray<15, jdouble>::process, ProcessArray<16, jdouble>::process,
     ProcessArray<17, jdouble>::process, ProcessArray<18, jdouble>::process,
     ProcessArray<19, jdouble>::process, ProcessArray<20, jdouble>::process,
     ProcessArray<21, jdouble>::process, ProcessArray<22, jdouble>::process,
     ProcessArray<23, jdouble>::process, ProcessArray<24, jdouble>::process,
     ProcessArray<25, jdouble>::process, ProcessArray<26, jdouble>::process,
     ProcessArray<27, jdouble>::process, ProcessArray<28, jdouble>::process,
     ProcessArray<29, jdouble>::process, ProcessArray<30, jdouble>::process,
     ProcessArray<31, jdouble>::process, ProcessArray<32, jdouble>::process}};

extern "C" JNIEXPORT jlong JNICALL Java_ArrayProcessorJNI_processArrayData(
    JNIEnv *env, jclass, jlong addr, jobject checkCapacity, jobject array,
    jint dims, jint elementType) {
  assert(elementType < 6);
  assert(0 < dims && dims <= 32);
  return process_arrays[elementType][dims](env, addr, checkCapacity, array);
}
