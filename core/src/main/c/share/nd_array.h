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

#ifndef QUESTDB_ND_ARRAY_H
#define QUESTDB_ND_ARRAY_H

#include <jni.h>

extern "C" JNIEXPORT jlong JNICALL
Java_io_questdb_cutlass_line_array_DoubleArray_processArrayData(
    JNIEnv *env, jclass, jlong addr, jobject array, jint dims, jint elemIndex);

extern "C" JNIEXPORT jlong JNICALL
Java_io_questdb_cutlass_line_array_NDArrayFlattener_processArrayShape(
    JNIEnv *env, jclass, jlong addr, jobject array, jint dims);

#endif // QUESTDB_ARRAY_H
