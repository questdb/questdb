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

package io.questdb.std.json;

import io.questdb.std.Os;

// Port of the C++ `simdjson::error_code` enum.
public class SimdJsonError {
    public static final int SUCCESS = 0;  // 0
    public static final int CAPACITY = SUCCESS + 1;  // 1
    public static final int MEMALLOC = CAPACITY + 1;  // 2
    public static final int TAPE_ERROR = MEMALLOC + 1;  // 3
    public static final int DEPTH_ERROR = TAPE_ERROR + 1;  // 4
    public static final int STRING_ERROR = DEPTH_ERROR + 1;  // 5
    public static final int T_ATOM_ERROR = STRING_ERROR + 1;  // 6
    public static final int F_ATOM_ERROR = T_ATOM_ERROR + 1;  // 7
    public static final int N_ATOM_ERROR = F_ATOM_ERROR + 1;  // 8
    public static final int NUMBER_ERROR = N_ATOM_ERROR + 1;  // 9
    public static final int BIGINT_ERROR = NUMBER_ERROR + 1;  // 10
    public static final int UTF8_ERROR = BIGINT_ERROR + 1;  // 11
    public static final int UNINITIALIZED = UTF8_ERROR + 1;  // 12
    public static final int EMPTY = UNINITIALIZED + 1;  // 13
    public static final int UNESCAPED_CHARS = EMPTY + 1;  // 14
    public static final int UNCLOSED_STRING = UNESCAPED_CHARS + 1;  // 15
    public static final int UNSUPPORTED_ARCHITECTURE = UNCLOSED_STRING + 1;  // 16
    public static final int INCORRECT_TYPE = UNSUPPORTED_ARCHITECTURE + 1;  // 17
    public static final int NUMBER_OUT_OF_RANGE = INCORRECT_TYPE + 1;  // 18
    public static final int INDEX_OUT_OF_BOUNDS = NUMBER_OUT_OF_RANGE + 1;  // 19
    public static final int NO_SUCH_FIELD = INDEX_OUT_OF_BOUNDS + 1;  // 20
    public static final int IO_ERROR = NO_SUCH_FIELD + 1;  // 21
    public static final int INVALID_JSON_POINTER = IO_ERROR + 1;  // 22
    public static final int INVALID_URI_FRAGMENT = INVALID_JSON_POINTER + 1;  // 23
    public static final int UNEXPECTED_ERROR = INVALID_URI_FRAGMENT + 1;  // 24
    public static final int PARSER_IN_USE = UNEXPECTED_ERROR + 1;  // 25
    public static final int OUT_OF_ORDER_ITERATION = PARSER_IN_USE + 1;  // 26
    public static final int INSUFFICIENT_PADDING = OUT_OF_ORDER_ITERATION + 1;  // 27
    public static final int INCOMPLETE_ARRAY_OR_OBJECT = INSUFFICIENT_PADDING + 1;  // 28
    public static final int SCALAR_DOCUMENT_AS_VALUE = INCOMPLETE_ARRAY_OR_OBJECT + 1;  // 29
    public static final int OUT_OF_BOUNDS = SCALAR_DOCUMENT_AS_VALUE + 1;  // 30
    public static final int TRAILING_CONTENT = OUT_OF_BOUNDS + 1;  // 31
    public static final int NUM_ERROR_CODES = TRAILING_CONTENT + 1;

    private static final String[] messages = new String[NUM_ERROR_CODES];

    public static String getMessage(int code) {
        if ((code >= NUM_ERROR_CODES) || (code < 0)) {
            return "Unknown error code " + code;
        }
        return messages[code];
    }

    public static void init() {
    }

    private static native String errorMessage(int code);

    static {
        Os.init();
        for (int i = 0; i < NUM_ERROR_CODES; i++) {
            messages[i] = errorMessage(i);
        }
    }
}
