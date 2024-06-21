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

package io.questdb.std.json;

import io.questdb.std.Os;

// Port of the C++ `simdjson::error_code` enum.
public class JsonError {
    public static final int SUCCESS = 0;
    public static final int CAPACITY = 1;
    public static final int MEMALLOC = 2;
    public static final int TAPE_ERROR = 3;
    public static final int DEPTH_ERROR = 4;
    public static final int STRING_ERROR = 5;
    public static final int T_ATOM_ERROR = 6;
    public static final int F_ATOM_ERROR = 7;
    public static final int N_ATOM_ERROR = 8;
    public static final int NUMBER_ERROR = 9;
    public static final int BIGINT_ERROR = 10;
    public static final int UTF8_ERROR = 11;
    public static final int UNINITIALIZED = 12;
    public static final int EMPTY = 13;
    public static final int UNESCAPED_CHARS = 14;
    public static final int UNCLOSED_STRING = 15;
    public static final int UNSUPPORTED_ARCHITECTURE = 16;
    public static final int INCORRECT_TYPE = 17;
    public static final int NUMBER_OUT_OF_RANGE = 18;
    public static final int INDEX_OUT_OF_BOUNDS = 19;
    public static final int NO_SUCH_FIELD = 20;
    public static final int IO_ERROR = 21;
    public static final int INVALID_JSON_POINTER = 22;
    public static final int INVALID_URI_FRAGMENT = 23;
    public static final int UNEXPECTED_ERROR = 24;
    public static final int PARSER_IN_USE = 25;
    public static final int OUT_OF_ORDER_ITERATION = 26;
    public static final int INSUFFICIENT_PADDING = 27;
    public static final int INCOMPLETE_ARRAY_OR_OBJECT = 28;
    public static final int SCALAR_DOCUMENT_AS_VALUE = 29;
    public static final int OUT_OF_BOUNDS = 30;
    public static final int TRAILING_CONTENT = 31;
    public static final int NUM_ERROR_CODES = 32;

    private static final String[] messages = new String[NUM_ERROR_CODES];

    public static String getMessage(int code) {
        if ((code >= NUM_ERROR_CODES) || (code < 0)) {
            return "Unknown error code " + code;
        }
        return messages[code];
    }

    private static native String errorMessage(int code);

    static {
        Os.init();
        for (int i = 0; i < NUM_ERROR_CODES; i++) {
            messages[i] = errorMessage(i);
        }
    }

    public static void init() {}
}
