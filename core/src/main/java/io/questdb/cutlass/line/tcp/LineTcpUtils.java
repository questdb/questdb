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

package io.questdb.cutlass.line.tcp;

import io.questdb.cairo.CairoException;
import io.questdb.std.Chars;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.MutableCharSink;

final class LineTcpUtils {

    public static void utf8ToUtf16Unchecked(DirectByteCharSequence utf8CharSeq, MutableCharSink tempSink) {
        tempSink.clear();
        if (!Chars.utf8Decode(utf8CharSeq.getLo(), utf8CharSeq.getHi(), tempSink)) {
            throw CairoException.nonCritical().put("invalid UTF8 in value for ").put(utf8CharSeq);
        }
    }

    static CharSequence utf8ToUtf16(DirectByteCharSequence utf8CharSeq, MutableCharSink tempSink, boolean hasNonAsciiChars) {
        if (hasNonAsciiChars) {
            utf8ToUtf16Unchecked(utf8CharSeq, tempSink);
            return tempSink;
        }
        return utf8CharSeq;
    }

    static String utf8BytesToString(DirectByteCharSequence utf8CharSeq, MutableCharSink tempSink) {
        tempSink.clear();
        for (int i = 0, n = utf8CharSeq.length(); i < n; i++) {
            tempSink.put(utf8CharSeq.charAt(i));
        }
        return tempSink.toString();
    }
}
