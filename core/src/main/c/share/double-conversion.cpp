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

#include "double-conversion.h"
#include "double-conversion/double-to-string.h"

#define BUFFER_LENGTH 50

using namespace double_conversion;

JNIEXPORT jstring JNICALL Java_io_questdb_std_DoubleConversion_append
  (JNIEnv *env, jclass clazz, jdouble value, jint scale) {
    char buffer[BUFFER_LENGTH];
    bool sign;
    int length;
    int point;
    DoubleToStringConverter::DoubleToAscii(value, DoubleToStringConverter::DtoaMode::SHORTEST, scale, buffer, BUFFER_LENGTH, &sign, &length, &point);
    char *pointInBuffer = buffer + point;
    memmove(pointInBuffer + 1, pointInBuffer, length - point);
    *pointInBuffer = '.';
    return env->NewStringUTF(buffer);
}
