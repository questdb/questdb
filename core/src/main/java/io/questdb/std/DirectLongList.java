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

package io.questdb.std;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.Reopenable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.Utf16Sink;

import java.io.Closeable;

import static io.questdb.std.Numbers.MAX_SAFE_INT_POW_2;

public class DirectLongList implements Mutable, Closeable, Reopenable {
    private static final Log LOG = LogFactory.getLog(DirectLongList.class);
    private final long initialCapacity;
    private final int memoryTag;
    private long address;
    private long capacity;
    private long limit;
    private long pos;

    /**
     * Creates a DirectLongList with optional deferred memory allocation.
     * <p>
     * When alloc=false, this constructor uses deferred allocation pattern:
     * - No memory is allocated immediately (address remains 0)
     * - Memory will be allocated later when reopen() is called
     * <p>
     *
     * @param capacity  the initial capacity in number of long elements (not bytes)
     * @param memoryTag memory tag for tracking allocations
     */
    public DirectLongList(long capacity, int memoryTag) {
        this(capacity, memoryTag, false);
    }

    public DirectLongList(long capacity, int memoryTag, boolean keepClosed) {
        this.memoryTag = memoryTag;
        final long capacityBytes = capacity * Long.BYTES;
        this.initialCapacity = capacityBytes;
        if (!keepClosed) {
            this.address = capacityBytes > 0 ? Unsafe.malloc(capacityBytes, memoryTag) : 0;
            this.capacity = capacityBytes;
            this.pos = address;
            this.limit = pos + capacityBytes;
        }
    }

    public void add(long value) {
        checkCapacity();
        assert pos < limit;
        Unsafe.getUnsafe().putLong(pos, value);
        pos += Long.BYTES;
    }

    public final void addAll(DirectLongList that) {
        long thatSize = that.pos - that.address;
        if (limit - pos < thatSize) {
            setCapacityBytes(this.capacity + thatSize - (limit - pos));
        }
        Vect.memcpy(this.pos, that.address, thatSize);
        this.pos += thatSize;
    }

    public long binarySearch(long value, int scanDir) {
        final long high = (pos - address) / 8;
        if (high > 0) {
            return Vect.binarySearch64Bit(address, value, 0, high - 1, scanDir);
        }
        return -1;
    }

    // clear without "zeroing" memory
    public void clear() {
        pos = address;
    }

    @Override
    public void close() {
        if (address != 0) {
            address = Unsafe.free(address, capacity, memoryTag);
            limit = 0;
            pos = 0;
            capacity = 0;
        }
    }

    // allocates space for the required number of long values
    public void ensureCapacity(long required) {
        final long requiredBytes = required << 3;
        if (pos + requiredBytes <= limit) {
            return;
        }
        setCapacityBytes(Math.max(capacity << 1, capacity + requiredBytes));
    }

    public void fill(int v) {
        Vect.memset(address, capacity, v);
    }

    public long get(long p) {
        return Unsafe.getUnsafe().getLong(address + (p << 3));
    }

    // base address of native memory
    public long getAddress() {
        return address;
    }

    public long getAppendAddress() {
        return pos;
    }

    // capacity in LONGs
    public long getCapacity() {
        return capacity >>> 3;
    }

    @Override
    public void reopen() {
        if (address == 0) {
            resetCapacity();
        }
    }

    public void resetCapacity() {
        setCapacityBytes(initialCapacity);
    }

    public long scanSearch(long v, long low, long high) {
        for (long i = low; i < high; i++) {
            long f = get(i);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(high + 1);
    }

    public void set(long p, long v) {
        assert p >= 0 && p <= (limit - address) >> 3;
        Unsafe.getUnsafe().putLong(address + (p << 3), v);
    }

    // Desired capacity in LONGs (not count of bytes).
    // Safe to call on a closed list - it will allocate memory.
    public void setCapacity(long capacity) {
        assert capacity > 0;
        setCapacityBytes(capacity << 3);
    }

    public void setPos(long p) {
        assert p * Long.BYTES <= capacity;
        pos = address + (p << 3);
    }

    public void shrink(long newCapacity) {
        // deallocates memory but keeps reusable
        if (newCapacity < capacity) {
            setCapacityBytes(newCapacity << 3);
        }
    }

    public long size() {
        return (pos - address) >>> 3;
    }

    public void skip(long p) {
        assert pos + p * Long.BYTES <= limit;
        pos += p << 3;
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(address, size());
    }

    @Override
    public String toString() {
        Utf16Sink sb = Misc.getThreadLocalSink();
        sb.put('[');
        final int maxElementsToPrint = 1000; // Do not try to print too much, it can hang IntelliJ debugger.
        for (int i = 0, n = (int) Math.min(maxElementsToPrint, size()); i < n; i++) {
            if (i > 0) {
                sb.put(',').put(' ');
            }
            sb.put(get(i));
        }
        if (size() > maxElementsToPrint) {
            sb.put(", .. ");
        }
        sb.put(']');
        return sb.toString();
    }

    public void zero() {
        fill(0);
    }

    // desired capacity in bytes (not count of LONG values)
    private void setCapacityBytes(long capacity) {
        if (this.capacity != capacity) {
            if ((capacity >>> 3) > MAX_SAFE_INT_POW_2) {
                throw CairoException.nonCritical().put("long list capacity overflow");
            }
            final long oldCapacity = this.capacity;
            final long oldSize = this.pos - this.address;
            final long address = Unsafe.realloc(this.address, oldCapacity, capacity, memoryTag);
            this.capacity = capacity;
            this.address = address;
            this.limit = address + capacity;
            this.pos = Math.min(this.limit, address + oldSize);
            LOG.debug().$("resized [old=").$(oldCapacity).$(", new=").$(this.capacity).$(']').$();
        }
    }

    void checkCapacity() {
        if (pos < limit) {
            return;
        }
        setCapacityBytes(capacity << 1);
    }
}
