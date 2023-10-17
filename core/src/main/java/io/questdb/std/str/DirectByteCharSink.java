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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.Closeable;

public class DirectByteCharSink extends AbstractCharSink implements Mutable, ByteSequence, Closeable, AsDirectByteSink {
    /**
     * Pointer to the C structure implementing the byte sink. See `byte_sink.h` for the definition.
     */
    private final long impl;
    private long lastCapacity;
    private final DirectByteSink byteSink = new DirectByteSink() {
        @Override
        public void close() {
            closeByteSink();
        }

        @Override
        public long ptr() {
            return impl;
        }
    };

    public DirectByteCharSink(long capacity) {
        this.impl = create(capacity);
        Unsafe.recordMemAlloc(capacity, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        Unsafe.incrMallocCount();
    }

    /**
     * Java_io_questdb_std_str_DirectByteCharSink_book
     */
    private static native long book(long impl, long len);

    /**
     * Java_io_questdb_std_str_DirectByteCharSink_create
     */
    private static native long create(long capacity);

    /**
     * Java_io_questdb_std_str_DirectByteCharSink_destroy
     */
    private static native void destroy(long impl);

    @Override
    public DirectByteSink asDirectByteSink() {
        lastCapacity = capacity();
        return byteSink;
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(ptr() + index);
    }

    @Override
    public void clear() {
        pos(0);
    }

    @Override
    public void close() {
        if (impl == 0) {
            return;
        }
        final long capAdjustment = -1 * capacity();
        destroy(impl);
        Unsafe.incrFreeCount();
        Unsafe.recordMemAlloc(capAdjustment, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
    }

    @TestOnly
    public long getCapacity() {
        return capacity();
    }

    public long getPtr() {
        return ptr();
    }

    @Override
    public int length() {
        return (int) pos();
    }

    public DirectByteCharSink put(ByteSequence cs) {
        if (cs != null) {
            int l = cs.length();

            final long dest = bookAvailable(l);
            for (int i = 0; i < l; i++) {
                Unsafe.getUnsafe().putByte(dest + i, cs.byteAt(i));
            }
            pos(pos() + l);
        }
        return this;
    }

    public DirectByteCharSink put(byte b) {
        final long dest = bookAvailable(1);
        Unsafe.getUnsafe().putByte(dest, b);
        pos(pos() + 1);
        return this;
    }

    @Override
    public CharSink put(char c) {
        this.put((byte) c);
        return this;
    }

    @NotNull
    @Override
    public String toString() {
        return Chars.stringFromUtf8Bytes(this);
    }

    private long bookAvailable(long required) {
        final long initCapacity = capacity();
        final long available = initCapacity - pos();
        if (available >= required) {
            return ptr() + pos();
        }
        final long writePtr = book(impl, required);
        final long newCapacity = capacity();
        if (newCapacity > initCapacity) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(newCapacity - initCapacity, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        }
        return writePtr;
    }

    private long capacity() {
        return Unsafe.getUnsafe().getLong(impl + 16);
    }

    private void closeByteSink() {
        final long capacityChange = capacity() - lastCapacity;
        if (capacityChange != 0) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(capacityChange, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        }
    }

    private long pos() {
        return Unsafe.getUnsafe().getLong(impl + 8);
    }

    private void pos(long pos) {
        Unsafe.getUnsafe().putLong(impl + 8, pos);
    }

    private long ptr() {
        return Unsafe.getUnsafe().getLong(impl);
    }
}
