/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

public class DirectIntList implements Mutable, Closeable, Reopenable {

    private static final Log LOG = LogFactory.getLog(DirectIntList.class);
    private final long initialCapacity;
    private final int memoryTag;
    private long address;
    private long capacity;
    private long limit;
    private long pos;

    public DirectIntList(long capacity, int memoryTag) {
        this.memoryTag = memoryTag;
        this.capacity = (capacity * Integer.BYTES);
        this.address = Unsafe.malloc(this.capacity, memoryTag);
        this.pos = address;
        this.limit = pos + this.capacity;
        this.initialCapacity = this.capacity;
    }

    public void add(int x) {
        checkCapacity();
        Unsafe.getUnsafe().putInt(pos, x);
        pos += Integer.BYTES;
    }

    public final void addAll(DirectIntList that) {
        long thatSize = that.pos - that.address;
        if (limit - pos < thatSize) {
            setCapacityBytes(this.capacity + thatSize - (limit - pos));
        }
        Vect.memcpy(this.pos, that.address, thatSize);
        this.pos += thatSize;
    }

    // clear without "zeroing" memory
    public void clear() {
        pos = address;
    }

    public void clear(int b) {
        zero(b);
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

    public int get(long p) {
        return Unsafe.getUnsafe().getInt(address + (p << 2));
    }

    // base address of native memory
    public long getAddress() {
        return address;
    }

    // capacity in INTs
    public long getCapacity() {
        return capacity >>> 2;
    }

    public void removeLast() {
        if (pos == address) {
            return;
        }
        pos -= Integer.BYTES;
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
        Unsafe.getUnsafe().putInt(address + (p << 2), v);
    }

    // desired capacity in INTs (not count of bytes)
    public void setCapacity(long capacity) {
        assert capacity > 0;
        setCapacityBytes(capacity << 2);
    }

    public void setPos(long p) {
        assert p * Integer.BYTES <= capacity;
        pos = address + (p << 2);
    }

    public void shrink(long newCapacity) {
        // deallocates memory but keeps reusable
        if (newCapacity < capacity) {
            setCapacityBytes(newCapacity << 2);
        }
    }

    public long size() {
        return (pos - address) >>> 2;
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

    public void zero(int v) {
        Vect.memset(address, pos - address, v);
    }

    // desired capacity in bytes (not count of INT values)
    private void setCapacityBytes(long capacity) {
        if (this.capacity != capacity) {
            if ((capacity >>> 2) > MAX_SAFE_INT_POW_2) {
                throw CairoException.nonCritical().put("int list capacity overflow");
            }
            final long oldCapacity = this.capacity;
            final long oldSize = this.pos - this.address;
            try {
                long address = Unsafe.realloc(this.address, oldCapacity, capacity, memoryTag);
                this.capacity = capacity;
                this.address = address;
                this.limit = address + capacity;
                this.pos = Math.min(this.limit, address + oldSize);
                LOG.debug().$("resized [old=").$(oldCapacity).$(", new=").$(this.capacity).$(']').$();
            } catch (Throwable t) {
                close();
                throw t;
            }
        }
    }

    void checkCapacity() {
        if (pos < limit) {
            return;
        }
        setCapacityBytes(capacity << 1);
    }
}
