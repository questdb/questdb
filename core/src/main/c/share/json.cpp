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

#include <jni.h>
#include "simdjson.h"

extern "C" {

JNIEXPORT jboolean JNICALL
Java_io_questdb_std_Json_loaded(
        JNIEnv */*env*/,
        jclass /*cl*/
) {
    simdjson::ondemand::parser parser;
    auto json = R"(
  { "str" : { "123" : {"abc" : 3.14 } } }
)"_padded;
    auto doc = parser.iterate(json);
    double val;
    auto error = doc["str"]["123"]["abc"].get(val);
    return error;
}

JNIEXPORT jboolean JNICALL
Java_io_questdb_std_Json_isValid(
        JNIEnv */*env*/,
        jclass /*cl*/,
        jlong pUtf8Json,
        jint pUtd8JsonSize
) {
    simdjson::dom::parser parser;
    auto d = simdjson::dom::element();
    auto ss = simdjson::padded_string((char *) pUtf8Json, pUtd8JsonSize);
    auto doc = parser.parse(ss).get(d);
    return !doc;
}

} // extern "C"