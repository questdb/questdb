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

package io.questdb.std.bytes;

import io.questdb.std.*;
import org.jetbrains.annotations.TestOnly;

public class DirectByteSink implements DirectByteSequence, BorrowableAsNativeByteSink, QuietCloseable, Mutable {
    /**
     * Pointer to the C `questdb_byte_sink_t` structure.
     * See `byte_sink.h`.
     */
    private long impl;

    /**
     * Capacity before borrowing out as {@link NativeByteSink}.
     */
    private long lastCapacity;

    private final NativeByteSink byteSink = new NativeByteSink() {
        @Override
        public void close() {
            closeByteSink();
        }

        @Override
        public long ptr() {
            return impl;
        }
    };

    public DirectByteSink(long capacity) {
        impl = NativeByteSink.create(capacity);
        if (impl == 0) {
            throw new OutOfMemoryError("Cannot allocate " + capacity + " bytes");
        }
        Unsafe.recordMemAlloc(this.capacity(), memoryTag());
        Unsafe.incrMallocCount();
    }

    public DirectByteSink append(byte b) {
        final long dest = book(1);
        Unsafe.getUnsafe().putByte(dest, b);
        advance(1);
        return this;
    }

    public DirectByteSink append(ByteSequence bs) {
        if (bs != null) {
            final long bsSize = bs.byteSize();
            final long dest = book(bsSize);
            for (long i = 0; i < bsSize; i++) {
                Unsafe.getUnsafe().putByte(dest + i, bs.byteAt(i));
            }
            advance(bsSize);
        }
        return this;
    }

    public DirectByteSink append(DirectByteSequence dbs) {
        if (dbs == null) {
            return this;
        }
        return append(dbs.ptr(), dbs.byteSize());
    }

    public DirectByteSink append(long src, long len) {
        final long dest = book(len);
        Vect.memcpy(dest, src, len);
        advance(len);
        return this;
    }

    @Override
    public NativeByteSink borrowDirectByteSink() {
        lastCapacity = capacity();
        return byteSink;
    }

    /**
     * Number of readable bytes in the sequence.
     */
    @Override
    public long byteSize() {
        return getImplPtr() - getImplLo();
    }

    @Override
    public void clear() {
        setImplPtr(getImplLo());
    }

    @Override
    public void close() {
        if (impl == 0) {
            return;
        }
        final long capAdjustment = -1 * capacity();
        NativeByteSink.destroy(impl);
        Unsafe.incrFreeCount();
        Unsafe.recordMemAlloc(capAdjustment, memoryTag());
        impl = 0;
    }

    @TestOnly
    public long capacity() {
        return getImplHi() - getImplLo();
    }

    /**
     * One past the last readable byte.
     */
    @Override
    public long hi() {
        return getImplPtr();
    }

    /**
     * First readable byte in the sequence.
     */
    @Override
    public long ptr() {
        return getImplLo();
    }

    /**
     * Low-level access to advance the internal write cursor by `written` bytes.
     * Use in conjunction with {@link #book(long)}.
     */
    public void advance(long written) {
        setImplPtr(getImplPtr() + written);
    }

    /**
     * Low-level access to ensure that at least `required` bytes are available for writing.
     * Returns the address of the first writable byte.
     * Use in conjunction with {@link #advance(long)}.
     */
    public long book(long required) {
        assert required >= 0;
        long p = getImplPtr();
        final long available = getImplHi() - p;
        if (available >= required) {
            return p;
        }
        final long initCapacity = capacity();
        p = NativeByteSink.book(impl, required);
        if (p == 0) {
            throw new OutOfMemoryError("Cannot allocate " + required + " bytes");
        }
        final long newCapacity = capacity();
        if (newCapacity > initCapacity) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(newCapacity - initCapacity, memoryTag());
        }
        return p;
    }

    private void closeByteSink() {
        final long capacityChange = capacity() - lastCapacity;
        if (capacityChange != 0) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(capacityChange, memoryTag());
        }
    }

    private long getImplHi() {
        return Unsafe.getUnsafe().getLong(impl + 16);
    }

    private long getImplLo() {
        return Unsafe.getUnsafe().getLong(impl + 8);
    }

    private long getImplPtr() {
        return Unsafe.getUnsafe().getLong(impl);
    }

    private void setImplPtr(long ptr) {
        Unsafe.getUnsafe().putLong(impl, ptr);
    }

    protected int memoryTag() {
        return MemoryTag.NATIVE_DIRECT_BYTE_SINK;
    }
}
