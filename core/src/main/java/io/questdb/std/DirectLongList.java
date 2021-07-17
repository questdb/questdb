/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

    long pos;
    long start;
    long limit;
    private long address;
    private long capacity;

    public DirectLongList(long capacity) {
        this.capacity = (capacity * Long.BYTES);
        this.address = Unsafe.malloc(this.capacity);
        this.start = this.pos = address;
        this.limit = pos + this.capacity;
    }

    // base address of native memory
    public long getAddress() {
        return address;
    }

    // capacity in LONGs
    public long getCapacity() {
        return capacity / Long.BYTES;
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
            extendBytes(this.capacity + thatSize - (limit - pos));
        }
        Vect.memcpy(that.start, this.pos, thatSize);
        this.pos += thatSize;
    }

    public void sortAsUnsigned() {
        Vect.sortULongAscInPlace(address, size());
    }

    public long binarySearch(long v) {
        long low = 0;
        long high = ((pos - start) >> 3) - 1;

        while (low <= high) {

            if (high - low < 65) {
                return scanSearch(v, low, high);
            }

            long mid = (low + high) >>> 1;
            long midVal = Unsafe.getUnsafe().getLong(start + (mid << 3));

            if (midVal < v)
                low = mid + 1;
            else if (midVal > v)
                high = mid - 1;
            else
                return mid;
        }
        return -(low + 1);
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
            Unsafe.free(address, capacity);
            address = 0;
        }
    }

    public long get(long p) {
        return Unsafe.getUnsafe().getLong(start + (p << 3));
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

    @Override
    public String toString() {
        CharSink sb = Misc.getThreadLocalBuilder();
        sb.put('{');
        for (int i = 0; i < size(); i++) {
            if (i > 0) {
                sb.put(',').put(' ');
            }
            sb.put(get(i));
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
        extendBytes(this.capacity * 2);
    }

    // desired capacity in LONGs (not count of bytes)
    public void extend(long capacity) {
        extendBytes(capacity * Long.BYTES);
    }

    // desired capacity in bytes (not count of LONG values)
    private void extendBytes(long capacity) {
        final long oldCapacity = this.capacity;
        this.capacity = capacity;
        long address = Unsafe.realloc(this.address, oldCapacity, capacity);
        this.pos = address + (this.pos - this.start);
        this.address = address;
        this.start = address;
        this.limit = address + capacity;
        LOG.debug().$("resized [old=").$(oldCapacity).$(", new=").$(this.capacity).$(']').$();
    }
}
