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

package io.questdb.std;

import io.questdb.cairo.Reopenable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.str.CharSink;

import java.io.Closeable;

public class DirectIntList implements Mutable, Closeable, Reopenable {

    private static final Log LOG = LogFactory.getLog(DirectIntList.class);
    private final long initialCapacity;
    private final int memoryTag;
    private long address;
    private long capacity;
    private long limit;
    private long pos;
    private long start;

    public DirectIntList(long capacity, int memoryTag) {
        this.memoryTag = memoryTag;
        this.capacity = (capacity * Integer.BYTES);
        this.address = Unsafe.malloc(this.capacity, memoryTag);
        this.start = this.pos = address;
        this.limit = pos + this.capacity;
        this.initialCapacity = this.capacity;
    }

    public void add(int x) {
        ensureCapacity();
        assert pos < limit;
        Unsafe.getUnsafe().putInt(pos, x);
        pos += Integer.BYTES;
    }

    public final void add(DirectIntList that) {
        long thatSize = that.pos - that.start;
        if (limit - pos < thatSize) {
            setCapacityBytes(this.capacity + thatSize - (limit - pos));
        }
        Vect.memcpy(this.pos, that.start, thatSize);
        this.pos += thatSize;
    }

    // clear without "zeroing" memory
    public void clear() {
        pos = start;
    }

    public void clear(int b) {
        zero(b);
        pos = start;
    }

    @Override
    public void close() {
        if (address != 0) {
            Unsafe.free(address, capacity, memoryTag);
            address = 0;
            start = 0;
            limit = 0;
            pos = 0;
            capacity = 0;
        }
    }

    public int get(long p) {
        return Unsafe.getUnsafe().getInt(start + (p << 2));
    }

    // base address of native memory
    public long getAddress() {
        return address;
    }

    // capacity in INTs
    public long getCapacity() {
        return capacity / Integer.BYTES;
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

    public void set(long p, int v) {
        assert p >= 0 && p <= (limit - start) >> 2;
        Unsafe.getUnsafe().putInt(start + (p << 2), v);
    }

    // desired capacity in LONGs (not count of bytes)
    public void setCapacity(long capacity) {
        setCapacityBytes(capacity * Integer.BYTES);
    }

    public void setPos(long p) {
        assert p * Integer.BYTES <= capacity;
        pos = start + p * Integer.BYTES;
    }

    public void shrink(long newCapacity) {
        // deallocates memory but keeps reusable
        if (newCapacity < capacity) {
            setCapacityBytes(newCapacity << 2);
        }
    }

    public long size() {
        return (int) ((pos - start) / Integer.BYTES);
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

    public void zero(int v) {
        Vect.memset(start, pos - start, v);
    }

    // desired capacity in bytes (not count of INT values)
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

    void ensureCapacity() {
        if (this.pos < limit) {
            return;
        }
        setCapacityBytes(this.capacity * 2);
    }
}
