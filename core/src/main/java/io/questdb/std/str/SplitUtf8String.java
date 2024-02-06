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

package io.questdb.std.str;

import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.ObjectFactory;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.NotNull;

/**
 * An immutable flyweight for a UTF-8 string stored in native memory.
 */
public class SplitUtf8String implements Utf8Sequence, Mutable {
    public static final Factory FACTORY = new Factory();
    private final AsciiCharSequence asciiCharSequence = new AsciiCharSequence();
    private long lo1;
    private long lo2;
    private int size;

    @Override
    public @NotNull CharSequence asAsciiCharSequence() {
        return asciiCharSequence.of(this);
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(index < 16 ? lo1 + index : lo2 + index - 15);
    }

    @Override
    public void clear() {
        this.lo1 = this.lo2 = 0;
    }


    public SplitUtf8String of(long lo1, long lo2, int size) {
        this.lo1 = lo1;
        this.lo2 = lo2;
        this.size = size;
        return this;
    }

    @Override
    public int size() {
        return size;
    }

    @NotNull
    @Override
    public String toString() {
        Utf16Sink utf16Sink = Misc.getThreadLocalSink();
        Utf8s.utf8ToUtf16(lo1, lo1 + 15, utf16Sink);
        Utf8s.utf8ToUtf16(lo2, lo2 + size - 15, utf16Sink);
        return utf16Sink.toString();
    }

    public static final class Factory implements ObjectFactory<SplitUtf8String> {
        @Override
        public SplitUtf8String newInstance() {
            return new SplitUtf8String();
        }
    }
}
