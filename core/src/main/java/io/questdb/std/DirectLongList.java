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

package io.questdb.std;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public class DirectLongList implements Mutable, Closeable {

    private static final Log LOG = LogFactory.getLog(DirectLongList.class);
    private final int memoryTag;
    private final long initialCapacity;
    private long pos;
    private long start;
    private long limit;
    private long address;
    private long capacity;

    public DirectLongList(long capacity, int memoryTag) {
        this.memoryTag = memoryTag;
        this.capacity = (capacity * Long.BYTES);
        this.address = Unsafe.malloc(this.capacity, memoryTag);
        this.start = this.pos = address;
        this.limit = pos + this.capacity;
        this.initialCapacity = this.capacity;
    }

    public void add(long x) {
        ensureCapacity();
        assert pos < limit;
        Unsafe.getUnsafe().putLong(pos, x);
        pos += Long.BYTES;
    }

    public final void add(DirectLongList that) {
        long thatSize = that.pos - that.start;
        if (limit - pos < thatSize) {
            setCapacityBytes(this.capacity + thatSize - (limit - pos));
        }
        Vect.memcpy(this.pos, that.start, thatSize);
        this.pos += thatSize;
    }

    public long binarySearch(long value, int scanDir) {
        final long high = (pos - start) / 8;
        if (high > 0) {
            return Vect.binarySearch64Bit(start, value, 0, high - 1, scanDir);
        }
        return -1;
    }

    // clear without "zeroing" memory
    public void clear() {
        pos = start;
    }

    public void clear(long b) {
        zero(b);
        pos = start;
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, capacity, memoryTag);
            address = 0;
        }
    }

    public long get(long p) {
        return Unsafe.getUnsafe().getLong(start + (p << 3));
    }

    // base address of native memory
    public long getAddress() {
        return address;
    }

    // capacity in LONGs
    public long getCapacity() {
        return capacity / Long.BYTES;
    }

    // desired capacity in LONGs (not count of bytes)
    public void setCapacity(long capacity) {
        setCapacityBytes(capacity * Long.BYTES);
    }

    public void resetCapacity() {
        setCapacityBytes(initialCapacity);
    }

    public void shrink(long newCapacity) {
        // deallocates memory but keeps reusable
        if (newCapacity < capacity) {
            setCapacityBytes(newCapacity << 3);
        }
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
        assert p >= 0 && p <= (limit - start) >> 3;
        Unsafe.getUnsafe().putLong(start + (p << 3), v);
    }

    public void setPos(long p) {
        assert p * Long.BYTES <= capacity;
        pos = start + p * Long.BYTES;
    }

    public long size() {
        return (int) ((pos - start) / Long.BYTES);
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(address, size());
    }

    @Override
    public String toString() {
        CharSink sb = Misc.getThreadLocalBuilder();
        sb.put('{');
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
        sb.put('}');
        return sb.toString();
    }

    public void zero(long v) {
        Vect.memset(start, pos - start, (int) v);
    }

    void ensureCapacity() {
        if (this.pos < limit) {
            return;
        }
        setCapacityBytes(this.capacity * 2);
    }

    // desired capacity in bytes (not count of LONG values)
    private void setCapacityBytes(long capacity) {
        if (this.capacity != capacity) {
            final long oldCapacity = this.capacity;
            this.capacity = capacity;
            long address = Unsafe.realloc(this.address, oldCapacity, capacity, memoryTag);
            this.pos = address + (this.pos - this.start);
            this.address = address;
            this.start = address;
            this.limit = address + capacity;
            LOG.debug().$("resized [old=").$(oldCapacity).$(", new=").$(this.capacity).$(']').$();
        }
    }
}
