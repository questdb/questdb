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

inline double sumDouble_Vanilla(double *d, long count) {
    const double *ext = d + count;
    double result = 0;
    double *pd = d;
    for (; pd < ext; pd++) {
        result += *pd;
    }
    return result;
}

extern "C" {


JNIEXPORT jdouble
JNICALL Java_io_questdb_std_Vect_sumDouble(JNIEnv *env, jclass cl, jlong pDouble, jlong count) {
    return sumDouble_Vanilla((double*) pDouble, count);
}

}
