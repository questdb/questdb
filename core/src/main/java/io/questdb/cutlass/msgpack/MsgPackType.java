/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package io.questdb.cutlass.msgpack;

public final class MsgPackType {

    /** Illegal token, detected MsgPackEncodingType._NEVER_USED. */
    public static final int _INVALID = 1;

    /**
     * A token NULL value.
     * See: MsgPackNil.
     */
    public static final int NIL = 2;

    /**
     * A `true` or `false` value.
     * See: MsgPackBool.
     */
    public static final int BOOL = 3;

    /**
     * A 64-bit signed integer.
     * See: MsgPackInt.
     */
    public static final int INT = 4;

    /**
     * A 64-bit IEEE 754 64-bit precision floating point number.
     * See: MsgPackFloat.
     */
    public static final int FLOAT = 5;

    /**
     * A UTF-8 length-encoded string.
     * See: MsgPackStr.
     */
    public static final int STR = 6;
    
    /**
     * A length-encoded binary blob.
     * See: MsgPackBin.
     */
    public static final int BIN = 7;
    
    /**
     * A list of values, potentially of different types.
     * See: MsgPackArray.
     */
    public static final int ARRAY = 8;    

    /**
     * A list of key-value pairs (a.k.a. A dictionary).
     * See: MsgPackMap.
     */
    public static final int MAP = 9;

    /**
     * A custom extension type.
     */
    public static final int EXT = 10;

    private MsgPackType() {}
}
