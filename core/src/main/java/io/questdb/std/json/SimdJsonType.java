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


import org.jetbrains.annotations.TestOnly;

/**
 * Maps to the constants of the C++ `simdjson::ondemand::json_type` enum.
 */
public class SimdJsonType {
    /**
     * An unset `SimdJsonType`.
     */
    public static final int UNSET = 0;  // 0

    /**
     * A JSON array   ( [ 1, 2, 3 ... ] )
     */
    public static final int ARRAY = UNSET + 1;  // 1

    /**
     * A JSON object  ( { "a": 1, "b" 2, ... } )
     */
    public static final int OBJECT = ARRAY + 1;  // 2

    /**
     * A JSON number  ( 1 or -2.3 or 4.5e6 ...)
     */
    public static final int NUMBER = OBJECT + 1;  // 3

    /**
     * A JSON string  ( "a" or "hello world\n" ...)
     */
    public static final int STRING = NUMBER + 1;  // 4

    /**
     * A JSON boolean (true or false)
     */
    public static final int BOOLEAN = STRING + 1;  // 5

    /**
     * A JSON null    (null)
     */
    public static final int NULL = BOOLEAN + 1;  // 6

    @TestOnly
    public static String nameOf(int type) {
        switch (type) {
            case UNSET:
                return "UNSET";
            case ARRAY:
                return "ARRAY";
            case OBJECT:
                return "OBJECT";
            case NUMBER:
                return "NUMBER";
            case STRING:
                return "STRING";
            case BOOLEAN:
                return "BOOLEAN";
            case NULL:
                return "NULL";
            default:
                return "UNKNOWN";
        }
    }
}
