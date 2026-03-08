/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

import io.questdb.cairo.CairoException;
import io.questdb.std.Mutable;
import io.questdb.std.Os;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import org.jetbrains.annotations.NotNull;

public class DirectByteSink implements DirectByteSequence, BorrowableAsNativeByteSink, QuietCloseable, Mutable {
    private static final int BYTE_SINK_PTR_OFFSET = 0;  // 0
    private static final int BYTE_SINK_LO_OFFSET = BYTE_SINK_PTR_OFFSET + 8;  // 8
    private static final int BYTE_SINK_HI_OFFSET = BYTE_SINK_LO_OFFSET + 8;  // 16
    private static final int BYTE_SINK_OVERFLOW_OFFSET = BYTE_SINK_HI_OFFSET + 8;  // 24
    private static final int BYTE_SINK_ASCII_OFFSET = BYTE_SINK_OVERFLOW_OFFSET + 4;  // 28
    private final long initialCapacity;
    private final int memoryTag;
    /**
     * Pointer to the C `questdb_byte_sink_t` structure. See `byte_sink.h`.
     * <p>
     * Fields accessible from `impl` via `GetLong`/`PutLong`:
     * * `ptr` - pointer to the first writable byte, offset: 0
     * * `lo` - pointer to the first byte in the buffer, offset: 8
     * * `hi` - pointer to the last byte in the buffer, offset: 16
     * * `overflow` - bool flag set to true if the buffer was asked to resize beyond 2GiB.
     * * `ascii` - bool flag set to true if the buffer is a UTF-8 buffer and contains non-ASCII characters.
     * <p>
     * These indirect fields are get/set by {@link #getImplPtr()},
     * {@link #setImplPtr(long)}, {@link #getImplLo()}, {@link #getImplHi()}.
     * <p>
     * The {@link #ensureCapacity(long)} method updates `impl`'s `lo` and `hi` fields.
     */
    private long impl;
    /**
     * Capacity before borrowing out as {@link NativeByteSink}.
     */
    private long lastAllocatedCapacity;
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

    public DirectByteSink(long initialCapacity, int memoryTag) {
        this(initialCapacity, memoryTag, false);
    }

    public DirectByteSink(long initialCapacity, int memoryTag, boolean keepClosed) {
        assert initialCapacity >= 0;
        assert initialCapacity <= Integer.MAX_VALUE;
        // this will allocate a minimum of 32 bytes of "allocated capacity"
        this.initialCapacity = initialCapacity;
        this.memoryTag = memoryTag;
        if (!keepClosed) {
            inflate();
        }
    }

    public static native long implBook(long impl, long len);

    public static native long implCreate(long capacity);

    public static native void implDestroy(long impl);

    /**
     * Low-level access to advance the internal write cursor by `written` bytes.
     * Use in conjunction with {@link #ensureCapacity(long)}.
     */
    public void advance(long written) {
        setImplPtr(getImplPtr() + written);
    }

    public long allocatedCapacity() {
        return getImplHi() - getImplLo();
    }

    @Override
    public @NotNull NativeByteSink borrowDirectByteSink() {
        assert impl != 0;
        lastAllocatedCapacity = allocatedCapacity();
        return byteSink;
    }

    @Override
    public void clear() {
        if (impl != 0) {
            setImplPtr(getImplLo());
            setAscii(true);
        }
    }

    @Override
    public void close() {
        deflate();
    }

    /**
     * Low-level access to ensure that at least `required` bytes are available for writing.
     * Returns the address of the first writable byte.
     * Use in conjunction with {@link #advance(long)}.
     */
    public long ensureCapacity(long required) {
        assert required >= 0;
        long p = getImplPtr();
        final long available = getImplHi() - p;
        if (available >= required) {
            return p;
        }
        final long initCapacity = allocatedCapacity();
        p = implBook(impl, required);
        if (p == 0) {
            if (getImplOverflow()) {
                throw CairoException.critical(0).put("buffer overflow, buffer capacity is requested to be over 2 GiB");
            } else {
                throw CairoException.nonCritical().setOutOfMemory(true).put("could not allocate DirectByteSink [required=").put(required).put(']');
            }
        }
        final long newCapacity = allocatedCapacity();
        if (newCapacity > initCapacity) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(newCapacity - initCapacity, memoryTag);
        }
        return p;
    }

    /**
     * One past the last readable byte.
     */
    @Override
    public long hi() {
        return getImplPtr();
    }

    /**
     * Returns true when the buffer contains a UTF-8 encoded string containing non-ASCII characters.
     */
    public boolean isAscii() {
        return Unsafe.getUnsafe().getByte(impl + BYTE_SINK_ASCII_OFFSET) != 0;
    }

    /**
     * First readable byte in the sequence.
     */
    @Override
    public long ptr() {
        return getImplLo();
    }

    public DirectByteSink put(byte b) {
        final long dest = ensureCapacity(1);
        Unsafe.getUnsafe().putByte(dest, b);
        advance(1);
        return this;
    }

    public DirectByteSink put(ByteSequence bs) {
        if (bs != null) {
            final int bsSize = bs.size();
            final long dest = ensureCapacity(bsSize);
            for (int i = 0; i < bsSize; i++) {
                Unsafe.getUnsafe().putByte(dest + i, bs.byteAt(i));
            }
            advance(bsSize);
        }
        return this;
    }

    public DirectByteSink put(DirectByteSequence dbs) {
        if (dbs == null) {
            return this;
        }
        return put(dbs.lo(), dbs.hi());
    }

    public DirectByteSink put(long lo, long hi) {
        final long len = hi - lo;
        final long dest = ensureCapacity(len);
        Vect.memcpy(dest, lo, len);
        advance(len);
        return this;
    }

    public DirectByteSink putByte(byte value) {
        Unsafe.getUnsafe().putByte(ensureCapacity(Byte.BYTES), value);
        advance(Byte.BYTES);
        return this;
    }

    public DirectByteSink putDouble(double value) {
        Unsafe.getUnsafe().putDouble(ensureCapacity(Double.BYTES), value);
        advance(Double.BYTES);
        return this;
    }

    public DirectByteSink putFloat(float value) {
        Unsafe.getUnsafe().putFloat(ensureCapacity(Float.BYTES), value);
        advance(Float.BYTES);
        return this;
    }

    public DirectByteSink putInt(int value) {
        Unsafe.getUnsafe().putInt(ensureCapacity(Integer.BYTES), value);
        advance(Integer.BYTES);
        return this;
    }

    public DirectByteSink putLong(long value) {
        Unsafe.getUnsafe().putLong(ensureCapacity(Long.BYTES), value);
        advance(Long.BYTES);
        return this;
    }

    public DirectByteSink putShort(short value) {
        Unsafe.getUnsafe().putShort(ensureCapacity(Short.BYTES), value);
        advance(Short.BYTES);
        return this;
    }

    public void reopen() {
        if (impl == 0) {
            inflate();
        } else {
            clear();
        }
    }

    /**
     * Ensure that the buffer has at least `minCapacity`.
     * <p>
     * After this call, `capacity() &gt;= minCapacity` is guaranteed.
     */
    public void reserve(long minCapacity) {
        if (minCapacity > allocatedCapacity()) {
            ensureCapacity(minCapacity);
        }
    }

    public void resetCapacity() {
        if (allocatedCapacity() > initialCapacity) {
            deflate();
            inflate();
        } else {
            clear();
        }
    }

    public void setAscii(boolean ascii) {
        Unsafe.getUnsafe().putByte(impl + BYTE_SINK_ASCII_OFFSET, (byte) (ascii ? 1 : 0));
    }

    /**
     * Number of readable bytes in the sequence.
     */
    @Override
    public int size() {
        return (int) (getImplPtr() - getImplLo());
    }

    public long tailPadding() {
        return getImplHi() - getImplPtr();
    }

    private void closeByteSink() {
        final long capacityChange = allocatedCapacity() - lastAllocatedCapacity;
        if (capacityChange != 0) {
            Unsafe.incrReallocCount();
            Unsafe.recordMemAlloc(capacityChange, memoryTag);
        }
    }

    private void deflate() {
        if (impl == 0) {
            return;
        }
        final long capAdjustment = -1 * allocatedCapacity();
        implDestroy(impl);
        Unsafe.incrFreeCount();
        Unsafe.recordMemAlloc(capAdjustment, memoryTag);
        impl = 0;
    }

    private long getImplHi() {
        assert impl != 0;
        return Unsafe.getUnsafe().getLong(impl + BYTE_SINK_HI_OFFSET);
    }

    private long getImplLo() {
        assert impl != 0;
        return Unsafe.getUnsafe().getLong(impl + BYTE_SINK_LO_OFFSET);
    }

    private boolean getImplOverflow() {
        return Unsafe.getUnsafe().getByte(impl + BYTE_SINK_OVERFLOW_OFFSET) != 0;
    }

    private long getImplPtr() {
        return Unsafe.getUnsafe().getLong(impl + BYTE_SINK_PTR_OFFSET);
    }

    private void inflate() {
        impl = implCreate(initialCapacity);
        if (impl == 0) {
            throw CairoException.nonCritical().setOutOfMemory(true).put("could not allocate direct byte sink [maxCapacity=").put(initialCapacity).put(']');
        }
        Unsafe.recordMemAlloc(this.allocatedCapacity(), memoryTag);
        Unsafe.incrMallocCount();
    }

    private void setImplPtr(long ptr) {
        Unsafe.getUnsafe().putLong(impl + BYTE_SINK_PTR_OFFSET, ptr);
    }

    static {
        Os.init();
    }
}
