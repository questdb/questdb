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


// Maps to the constants of the `simdjson::ondemand::json_type` enum.
public class JsonType {
    /** An unset `JsonType` or the result of accessing a non-existent path. */
    public static final int UNSET = 0;

    /** A JSON array   ( [ 1, 2, 3 ... ] ) */
    public static final int ARRAY = 1;

    /** A JSON object  ( { "a": 1, "b" 2, ... } ) */
    public static final int OBJECT = 2;

    /** A JSON number  ( 1 or -2.3 or 4.5e6 ...) */
    public static final int NUMBER = 3;

    /** A JSON string  ( "a" or "hello world\n" ...) */
    public static final int STRING = 4;

    /** A JSON boolean (true or false) */
    public static final int BOOLEAN = 5;

    /** A JSON null    (null) */
    public static final int NULL = 6;
}
