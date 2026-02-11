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

#ifndef QUESTDB_COMPILER_H
#define QUESTDB_COMPILER_H

#include <jni.h>

extern "C" {

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_compileFunction(JNIEnv *e,
                                                                           jclass cl,
                                                                           jlong filterAddress,
                                                                           jlong filterSize,
                                                                           jint options,
                                                                           jobject error);

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_compileCountOnlyFunction(JNIEnv *e,
                                                                                     jclass cl,
                                                                                     jlong filterAddress,
                                                                                     jlong filterSize,
                                                                                     jint options,
                                                                                     jobject error);

JNIEXPORT void JNICALL Java_io_questdb_jit_FiltersCompiler_freeFunction(JNIEnv *e, jclass cl, jlong fnAddress);

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callFunction(JNIEnv *e,
                                                                         jclass cl,
                                                                         jlong fnAddress,
                                                                         jlong colsAddress,
                                                                         jlong colsSize,
                                                                         jlong varSizeIndexesAddress,
                                                                         jlong varsAddress,
                                                                         jlong varsSize,
                                                                         jlong rowsAddress,
                                                                         jlong rowsCount);

JNIEXPORT jlong JNICALL Java_io_questdb_jit_FiltersCompiler_callCountOnlyFunction(JNIEnv *e,
                                                                                  jclass cl,
                                                                                  jlong fnAddress,
                                                                                  jlong colsAddress,
                                                                                  jlong colsSize,
                                                                                  jlong varSizeIndexesAddress,
                                                                                  jlong varsAddress,
                                                                                  jlong varsSize,
                                                                                  jlong rowsCount);

JNIEXPORT void JNICALL Java_io_questdb_jit_FiltersCompiler_runTests(JNIEnv *e, jclass cl);

}

#endif //QUESTDB_COMPILER_H
