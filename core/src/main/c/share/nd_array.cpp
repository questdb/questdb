
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

#include "nd_array.h"
#include "dispatcher.h"
#include <assert.h>
#include <cstring>

#define MAX_ARRAY_DIMS 32
#define MAX_ARRAY_TYPE_INDEX 5

template <jint DIM, typename T> struct ProcessData {
  static jlong process(JNIEnv *env, jlong addr, jobject array) {
    jsize length = -1;
    for (jsize i = 0, size = env->GetArrayLength(static_cast<jarray>(array));
         i < size; i++) {
      jobject subArray =
          env->GetObjectArrayElement(static_cast<jobjectArray>(array), i);

      if (length == -1) {
        length = env->GetArrayLength(static_cast<jarray>(subArray));
      } else if (length != env->GetArrayLength(static_cast<jarray>(subArray))) {
        env->DeleteLocalRef(subArray);
        return 0;
      }
      addr = ProcessData<DIM - 1, T>::process(env, addr, subArray);
      env->DeleteLocalRef(subArray);
      if (addr == 0)
        return 0;
    }

    return addr;
  }
};

DECLARE_DISPATCHER_TYPE(platform_memcpy, void *dst, const void *src,
                        const size_t len);
template <typename T> struct ProcessData<1, T> {
  static jlong process(JNIEnv *env, jlong addr, jobject array) {
    jsize length = env->GetArrayLength(static_cast<jarray>(array));
    if (length == 0)
      return 0;

    T *elements = static_cast<T *>(
        env->GetPrimitiveArrayCritical(static_cast<jarray>(array), nullptr));
    platform_memcpy(reinterpret_cast<void *>(addr), elements,
                    length * sizeof(T));
    env->ReleasePrimitiveArrayCritical(static_cast<jarray>(array), elements,
                                       JNI_ABORT);
    return addr + length * sizeof(T);
  }
};

template <jint DIM> jint processShape(JNIEnv *env, jlong addr, jobject array) {

  jsize length = env->GetArrayLength(static_cast<jarray>(array));
  if (length == 0)
    return 0;
  *reinterpret_cast<jint *>(addr) = static_cast<jint>(length);
  jobject subArray =
      env->GetObjectArrayElement(static_cast<jobjectArray>(array), 0);
  return length * processShape<DIM - 1>(env, addr + sizeof(jint), subArray);
}

template <> jint processShape<1>(JNIEnv *env, jlong addr, jobject array) {
  jsize length = env->GetArrayLength(static_cast<jarray>(array));
  if (length == 0)
    return 0;
  *reinterpret_cast<jint *>(addr) = static_cast<jint>(length);
  return length;
}

typedef jlong (*process_array_data_t)(JNIEnv *env, jlong addr, jobject array);

typedef jint (*process_array_shape_t)(JNIEnv *env, jlong addr, jobject array);

static process_array_data_t process_array_datas[6][32] = {
    // boolean
    {ProcessData<1, jboolean>::process,  ProcessData<2, jboolean>::process,
     ProcessData<3, jboolean>::process,  ProcessData<4, jboolean>::process,
     ProcessData<5, jboolean>::process,  ProcessData<6, jboolean>::process,
     ProcessData<7, jboolean>::process,  ProcessData<8, jboolean>::process,
     ProcessData<9, jboolean>::process,  ProcessData<10, jboolean>::process,
     ProcessData<11, jboolean>::process, ProcessData<12, jboolean>::process,
     ProcessData<13, jboolean>::process, ProcessData<14, jboolean>::process,
     ProcessData<15, jboolean>::process, ProcessData<16, jboolean>::process,
     ProcessData<17, jboolean>::process, ProcessData<18, jboolean>::process,
     ProcessData<19, jboolean>::process, ProcessData<20, jboolean>::process,
     ProcessData<21, jboolean>::process, ProcessData<22, jboolean>::process,
     ProcessData<23, jboolean>::process, ProcessData<24, jboolean>::process,
     ProcessData<25, jboolean>::process, ProcessData<26, jboolean>::process,
     ProcessData<27, jboolean>::process, ProcessData<28, jboolean>::process,
     ProcessData<29, jboolean>::process, ProcessData<30, jboolean>::process,
     ProcessData<31, jboolean>::process, ProcessData<32, jboolean>::process},
    // byte
    {ProcessData<1, jbyte>::process,  ProcessData<2, jbyte>::process,
     ProcessData<3, jbyte>::process,  ProcessData<4, jbyte>::process,
     ProcessData<5, jbyte>::process,  ProcessData<6, jbyte>::process,
     ProcessData<7, jbyte>::process,  ProcessData<8, jbyte>::process,
     ProcessData<9, jbyte>::process,  ProcessData<10, jbyte>::process,
     ProcessData<11, jbyte>::process, ProcessData<12, jbyte>::process,
     ProcessData<13, jbyte>::process, ProcessData<14, jbyte>::process,
     ProcessData<15, jbyte>::process, ProcessData<16, jbyte>::process,
     ProcessData<17, jbyte>::process, ProcessData<18, jbyte>::process,
     ProcessData<19, jbyte>::process, ProcessData<20, jbyte>::process,
     ProcessData<21, jbyte>::process, ProcessData<22, jbyte>::process,
     ProcessData<23, jbyte>::process, ProcessData<24, jbyte>::process,
     ProcessData<25, jbyte>::process, ProcessData<26, jbyte>::process,
     ProcessData<27, jbyte>::process, ProcessData<28, jbyte>::process,
     ProcessData<29, jbyte>::process, ProcessData<30, jbyte>::process,
     ProcessData<31, jbyte>::process, ProcessData<32, jbyte>::process},
    // int
    {ProcessData<1, jint>::process,  ProcessData<2, jint>::process,
     ProcessData<3, jint>::process,  ProcessData<4, jint>::process,
     ProcessData<5, jint>::process,  ProcessData<6, jint>::process,
     ProcessData<7, jint>::process,  ProcessData<8, jint>::process,
     ProcessData<9, jint>::process,  ProcessData<10, jint>::process,
     ProcessData<11, jint>::process, ProcessData<12, jint>::process,
     ProcessData<13, jint>::process, ProcessData<14, jint>::process,
     ProcessData<15, jint>::process, ProcessData<16, jint>::process,
     ProcessData<17, jint>::process, ProcessData<18, jint>::process,
     ProcessData<19, jint>::process, ProcessData<20, jint>::process,
     ProcessData<21, jint>::process, ProcessData<22, jint>::process,
     ProcessData<23, jint>::process, ProcessData<24, jint>::process,
     ProcessData<25, jint>::process, ProcessData<26, jint>::process,
     ProcessData<27, jint>::process, ProcessData<28, jint>::process,
     ProcessData<29, jint>::process, ProcessData<30, jint>::process,
     ProcessData<31, jint>::process, ProcessData<32, jint>::process},
    // long
    {ProcessData<1, jlong>::process,  ProcessData<2, jlong>::process,
     ProcessData<3, jlong>::process,  ProcessData<4, jlong>::process,
     ProcessData<5, jlong>::process,  ProcessData<6, jlong>::process,
     ProcessData<7, jlong>::process,  ProcessData<8, jlong>::process,
     ProcessData<9, jlong>::process,  ProcessData<10, jlong>::process,
     ProcessData<11, jlong>::process, ProcessData<12, jlong>::process,
     ProcessData<13, jlong>::process, ProcessData<14, jlong>::process,
     ProcessData<15, jlong>::process, ProcessData<16, jlong>::process,
     ProcessData<17, jlong>::process, ProcessData<18, jlong>::process,
     ProcessData<19, jlong>::process, ProcessData<20, jlong>::process,
     ProcessData<21, jlong>::process, ProcessData<22, jlong>::process,
     ProcessData<23, jlong>::process, ProcessData<24, jlong>::process,
     ProcessData<25, jlong>::process, ProcessData<26, jlong>::process,
     ProcessData<27, jlong>::process, ProcessData<28, jlong>::process,
     ProcessData<29, jlong>::process, ProcessData<30, jlong>::process,
     ProcessData<31, jlong>::process, ProcessData<32, jlong>::process},
    // float
    {ProcessData<1, jfloat>::process,  ProcessData<2, jfloat>::process,
     ProcessData<3, jfloat>::process,  ProcessData<4, jfloat>::process,
     ProcessData<5, jfloat>::process,  ProcessData<6, jfloat>::process,
     ProcessData<7, jfloat>::process,  ProcessData<8, jfloat>::process,
     ProcessData<9, jfloat>::process,  ProcessData<10, jfloat>::process,
     ProcessData<11, jfloat>::process, ProcessData<12, jfloat>::process,
     ProcessData<13, jfloat>::process, ProcessData<14, jfloat>::process,
     ProcessData<15, jfloat>::process, ProcessData<16, jfloat>::process,
     ProcessData<17, jfloat>::process, ProcessData<18, jfloat>::process,
     ProcessData<19, jfloat>::process, ProcessData<20, jfloat>::process,
     ProcessData<21, jfloat>::process, ProcessData<22, jfloat>::process,
     ProcessData<23, jfloat>::process, ProcessData<24, jfloat>::process,
     ProcessData<25, jfloat>::process, ProcessData<26, jfloat>::process,
     ProcessData<27, jfloat>::process, ProcessData<28, jfloat>::process,
     ProcessData<29, jfloat>::process, ProcessData<30, jfloat>::process,
     ProcessData<31, jfloat>::process, ProcessData<32, jfloat>::process},
    // double
    {ProcessData<1, jdouble>::process,  ProcessData<2, jdouble>::process,
     ProcessData<3, jdouble>::process,  ProcessData<4, jdouble>::process,
     ProcessData<5, jdouble>::process,  ProcessData<6, jdouble>::process,
     ProcessData<7, jdouble>::process,  ProcessData<8, jdouble>::process,
     ProcessData<9, jdouble>::process,  ProcessData<10, jdouble>::process,
     ProcessData<11, jdouble>::process, ProcessData<12, jdouble>::process,
     ProcessData<13, jdouble>::process, ProcessData<14, jdouble>::process,
     ProcessData<15, jdouble>::process, ProcessData<16, jdouble>::process,
     ProcessData<17, jdouble>::process, ProcessData<18, jdouble>::process,
     ProcessData<19, jdouble>::process, ProcessData<20, jdouble>::process,
     ProcessData<21, jdouble>::process, ProcessData<22, jdouble>::process,
     ProcessData<23, jdouble>::process, ProcessData<24, jdouble>::process,
     ProcessData<25, jdouble>::process, ProcessData<26, jdouble>::process,
     ProcessData<27, jdouble>::process, ProcessData<28, jdouble>::process,
     ProcessData<29, jdouble>::process, ProcessData<30, jdouble>::process,
     ProcessData<31, jdouble>::process, ProcessData<32, jdouble>::process}};

static process_array_shape_t process_array_shapes[32] = {
    processShape<1>,  processShape<2>,  processShape<3>,  processShape<4>,
    processShape<5>,  processShape<6>,  processShape<7>,  processShape<8>,
    processShape<9>,  processShape<10>, processShape<11>, processShape<12>,
    processShape<13>, processShape<14>, processShape<15>, processShape<16>,
    processShape<17>, processShape<18>, processShape<19>, processShape<20>,
    processShape<21>, processShape<22>, processShape<23>, processShape<24>,
    processShape<25>, processShape<26>, processShape<27>, processShape<28>,
    processShape<29>, processShape<30>, processShape<31>, processShape<32>,
};

extern "C" JNIEXPORT jlong JNICALL
Java_io_questdb_cutlass_line_array_NDArrayFlattener_processArrayData(
    JNIEnv *env, jclass, jlong addr, jobject array, jint dims,
    jint elementType) {
  assert(elementType <= MAX_ARRAY_TYPE_INDEX);
  assert(0 < dims && dims <= MAX_ARRAY_DIMS);
  return process_array_datas[elementType][dims - 1](env, addr, array);
}

extern "C" JNIEXPORT jlong JNICALL
Java_io_questdb_cutlass_line_array_NDArrayFlattener_processArrayShape(
    JNIEnv *env, jclass, jlong addr, jobject array, jint dims) {
  assert(0 < dims && dims <= MAX_ARRAY_DIMS);
  return process_array_shapes[dims - 1](env, addr, array);
}
