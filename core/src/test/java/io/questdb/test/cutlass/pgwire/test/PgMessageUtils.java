/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.test.cutlass.pgwire.test;

import io.questdb.std.str.StringSink;

class PgMessageUtils {
    private static final char[] HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    static void appendByteAsHex(StringSink sb, byte b) {
        sb.put(HEX_DIGITS[(b >> 4) & 0x0F]);
        sb.put(HEX_DIGITS[b & 0x0F]);
    }

    static void appendShortAsHex(StringSink sink, short s) {
        appendByteAsHex(sink, (byte) (s >> 8));
        appendByteAsHex(sink, (byte) (s));
    }

    static void appendIntAsHex(StringSink sb, int i) {
        appendByteAsHex(sb, (byte) (i >> 24));
        appendByteAsHex(sb, (byte) (i >> 16));
        appendByteAsHex(sb, (byte) (i >> 8));
        appendByteAsHex(sb, (byte) (i));
    }

    static void appendBytesAsHexZ(StringSink sb, byte[] bytes) {
        for (byte b : bytes) {
            appendByteAsHex(sb, b);
        }
        appendByteAsHex(sb, (byte) 0);
    }

    static void appendBytesAsHex(StringSink sb, byte[] bytes) {
        for (byte b : bytes) {
            appendByteAsHex(sb, b);
        }
    }
}
