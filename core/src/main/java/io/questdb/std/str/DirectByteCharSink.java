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

import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;
import io.questdb.std.bytes.DirectByteSink;
import io.questdb.std.bytes.NativeByteSink;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class DirectByteCharSink extends AbstractCharSink implements Mutable, ByteSequence, Closeable, DirectUtf8CharSink {
    private final DirectByteSink sink;

    public DirectByteCharSink(long capacity) {
        sink = new DirectByteSink(capacity) {
            @Override
            protected int memoryTag() {
                return MemoryTag.NATIVE_DIRECT_CHAR_SINK;
            }
        };
    }

    @Override
    public NativeByteSink borrowDirectByteSink() {
        return sink.borrowDirectByteSink();
    }

    @Override
    public byte byteAt(int index) {
        return sink.byteAt(index);
    }

    @Override
    public void clear() {
        sink.clear();
    }

    @Override
    public void close() {
        sink.close();
    }

    @TestOnly
    public long capacity() {
        return sink.capacity();
    }

    @Override
    public int length() {
        // TODO: This is incorrect as it returns the number of bytes, not UTF-16 code units.
        // As such it breaks the contract of CharSequence.
        return size();
    }

    public long ptr() {
        return sink.ptr();
    }

    public DirectByteCharSink put(ByteSequence bs) {
        if (bs != null) {
            final int bsLen = bs.length();
            final long dest = sink.book(bsLen);
            for (int i = 0; i < bsLen; i++) {
                Unsafe.getUnsafe().putByte(dest + i, bs.byteAt(i));
            }
            sink.advance(bsLen);
        }
        return this;
    }

    public DirectByteCharSink put(byte b) {
        sink.put(b);
        return this;
    }

    @Override
    public CharSink put(char c) {
        this.put((byte) c);
        return this;
    }

    public int size() {
        return sink.size();
    }

    @NotNull
    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(this);
    }

    @Override
    public DirectByteCharSink put(CharSequence cs) {
        // Note that this implementation is not UTF-8 safe: It assumes `cs` is ASCII without checks.
        final int charCount = cs.length();
        final long destPtr = sink.book(charCount);
        Chars.asciiStrCpy(cs, charCount, destPtr);
        sink.advance(charCount);
        return this;
    }
}
