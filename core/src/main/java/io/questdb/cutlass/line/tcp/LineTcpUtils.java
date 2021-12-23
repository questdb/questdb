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
import io.questdb.std.str.StringSink;

final class LineTcpUtils {
    private final static StringSink tempSink = new StringSink();
    private final static MangledUtf8Sink mangledUtf8Sink = new MangledUtf8Sink(tempSink);

    static CharSequence utf8ToUtf16(DirectByteCharSequence utf8CharSeq, boolean hasNonAsciiChars) {
        if (hasNonAsciiChars) {
            tempSink.clear();
            if (!Chars.utf8Decode(utf8CharSeq.getLo(), utf8CharSeq.getHi(), tempSink)) {
                throw CairoException.instance(0).put("invalid UTF8 in value for ").put(utf8CharSeq);
            }
            return tempSink;
        }
        return utf8CharSeq;
    }

    static CharSequence toMangledUtf8String(String str, boolean hasNonAsciiChars) {
        if (hasNonAsciiChars) {
            CharSequence mangledUtf8Representation = mangledUtf8Sink.encodeMangledUtf8(str);
            // it is still worth checking if mangled utf8 length is different from original
            // if they are the same it means original string is ASCII, we can just return it
            return mangledUtf8Representation.length() != str.length() ? tempSink.toString() : str;
        }
        return str;
    }
}
