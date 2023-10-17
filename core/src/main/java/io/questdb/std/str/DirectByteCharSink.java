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

public class DirectByteCharSink extends AbstractCharSink implements Mutable, ByteSequence, Closeable, BorrowableDirectByteSink {
    /**
     * Pointer to the C `questdb_byte_sink_t` structure.
     * See `byte_sink.h`.
     */
    private final long impl;
    private long lastCapacity;
    private final DirectByteSink byteSink = new DirectByteSink() {
        @Override
        public void close() {
            closeByteSink();
        }

        @Override
        public long getPtr() {
            return impl;
        }
    };

    public DirectByteCharSink(long capacity) {
        impl = DirectByteSink.create(capacity);
        if (impl == 0) {
            throw new OutOfMemoryError("Cannot allocate " + capacity + " bytes");
        }
        Unsafe.recordMemAlloc(this.capacity(), MemoryTag.NATIVE_DIRECT_CHAR_SINK);
        Unsafe.incrMallocCount();
    }

    @Override
    public DirectByteSink borrowDirectByteSink() {
        lastCapacity = capacity();
        return byteSink;
    }

    @Override
    public byte byteAt(int index) {
        return Unsafe.getUnsafe().getByte(getImplPtr() + index);
    }

    @Override
    public void clear() {
        setImplPos(0);
    }

    @Override
    public void close() {
        if (impl == 0) {
            return;
        }
        final long capAdjustment = -1 * capacity();
        DirectByteSink.destroy(impl);
        Unsafe.incrFreeCount();
        Unsafe.recordMemAlloc(capAdjustment, MemoryTag.NATIVE_DIRECT_CHAR_SINK);
    }

    @TestOnly
    public long getCapacity() {
        return capacity();
    }

    public long getPtr() {
        return getImplPtr();
    }

    @Override
    public int length() {
        return (int) getImplPos();
    }

    public DirectByteCharSink put(ByteSequence bs) {
        if (bs != null) {
            final int bsLen = bs.length();
            final long dest = book(bsLen);
            for (int i = 0; i < bsLen; i++) {
                Unsafe.getUnsafe().putByte(dest + i, bs.byteAt(i));
            }
            setImplPos(getImplPos() + bsLen);
        }
        return this;
    }

    public DirectByteCharSink put(byte b) {
        final long dest = book(1);
        Unsafe.getUnsafe().putByte(dest, b);
        setImplPos(getImplPos() + 1);
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

    private long book(long required) {
        final long initCapacity = capacity();
        final long available = initCapacity - getImplPos();
        if (available >= required) {
            return getImplPtr() + getImplPos();
        }
        final long writePtr = DirectByteSink.book(impl, required);
        if (writePtr == 0) {
            throw new OutOfMemoryError("Cannot allocate " + required + " bytes");
        }
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

    private long getImplPos() {
        return Unsafe.getUnsafe().getLong(impl + 8);
    }

    private long getImplPtr() {
        return Unsafe.getUnsafe().getLong(impl);
    }

    private void setImplPos(long pos) {
        Unsafe.getUnsafe().putLong(impl + 8, pos);
    }
}
