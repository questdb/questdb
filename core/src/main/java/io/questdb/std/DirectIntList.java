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

public class DirectIntList implements Mutable, Closeable, Reopenable {
    private static final Log LOG = LogFactory.getLog(DirectIntList.class);
    private final long initialCapacity;
    private final int memoryTag;
    private long address;
    private long capacity;
    private long limit;
    private long pos;

    public DirectIntList(long capacity, int memoryTag) {
        this(capacity, memoryTag, false);
    }

    public DirectIntList(long capacity, int memoryTag, boolean keepClosed) {
        assert capacity >= 0;
        this.memoryTag = memoryTag;
        final long capacityBytes = capacity * Integer.BYTES;
        this.initialCapacity = capacityBytes;
        if (!keepClosed) {
            this.address = capacityBytes > 0 ? Unsafe.malloc(capacityBytes, memoryTag) : 0;
            this.capacity = capacityBytes;
            this.pos = address;
            this.limit = pos + capacityBytes;
        }
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

    /**
     * Overwrites the range from `address` to `pos` (exclusive) with the provided
     * value, and then resets `pos` to `address`. The value is interpreted as a
     * single byte, so this sets all the involved bytes to that value.
     *
     * @param b the byte value to set
     */
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

    public void removeIndexBlock(long index, long length) {
        long size = size();
        assert index > -1 && length > -1 && (index + length) <= size;
        Vect.memcpy(address + (index << 2), address + ((index + length) << 2), (size - index - length) << 2);
        pos -= (length << 2);
        assert pos >= address;
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
        if (initialCapacity == 0) {
            close();
        } else {
            setCapacityBytes(initialCapacity);
        }
    }

    public void reverse() {
        final long len = size();
        for (long index = 0, mid = len / 2; index < mid; ++index) {
            final int temp = get(index);
            set(index, get(len - index - 1));
            set(len - index - 1, temp);
        }
    }

    public void set(long p, int v) {
        assert p > -1 && p < ((pos - address) >>> 2);
        Unsafe.getUnsafe().putInt(address + (p << 2), v);
    }

    // Desired capacity in INTs (not count of bytes).
    // Safe to call on a closed list - it will allocate memory.
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
        assert capacity > 0;
        if (this.capacity != capacity) {
            if ((capacity >>> 2) > MAX_SAFE_INT_POW_2) {
                throw CairoException.nonCritical().put("int list capacity overflow");
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
        if (pos >= limit) {
            setCapacityBytes((Math.max(capacity, Integer.BYTES)) << 1);
        }
    }
}
