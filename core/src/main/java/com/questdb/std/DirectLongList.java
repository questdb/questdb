/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.std;

import com.questdb.std.str.CharSink;

import java.io.Closeable;

public class DirectLongList implements Mutable, Closeable {

    public static final int CACHE_LINE_SIZE = 64;
    private final int pow2;
    private final int onePow2;
    long pos;
    long start;
    long limit;
    private long address;
    private long capacity;

    public DirectLongList(long capacity) {
        this.pow2 = 3;
        this.address = Unsafe.malloc(this.capacity = ((capacity << 3) + CACHE_LINE_SIZE));
        this.start = this.pos = address + (address & (CACHE_LINE_SIZE - 1));
        this.limit = pos + ((capacity - 1) << 3);
        this.onePow2 = (1 << 3);
    }

    public void add(long x) {
        ensureCapacity();
        Unsafe.getUnsafe().putLong(pos, x);
        pos += 8;
    }

    public final void add(DirectLongList that) {
        int count = (int) (that.pos - that.start);
        if (limit - pos < count) {
            extend((int) (this.limit - this.start + count) >> 1);
        }
        Unsafe.getUnsafe().copyMemory(that.start, this.pos, count);
        this.pos += count;
    }

    public int binarySearch(long v) {
        int low = 0;
        int high = (int) ((pos - start) >> 3) - 1;

        while (low <= high) {

            if (high - low < 65) {
                return scanSearch(v);
            }

            int mid = (low + high) >>> 1;
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

    public void clear() {
        clear(0);
    }

    public void clear(long b) {
        pos = start;
        zero(b);
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

    public int scanSearch(long v) {
        int sz = size();
        for (int i = 0; i < sz; i++) {
            long f = get(i);
            if (f == v) {
                return i;
            }
            if (f > v) {
                return -(i + 1);
            }
        }
        return -(sz + 1);
    }

    public void set(long p, long v) {
        assert p >= 0 && p <= (limit - start) >> 3;
        Unsafe.getUnsafe().putLong(start + (p << 3), v);
    }

    public void setCapacity(long capacity) {
        if (capacity << pow2 > limit - start) {
            extend(capacity);
        }
    }

    public void setPos(long p) {
        pos = start + (p << pow2);
    }

    public int size() {
        return (int) ((pos - start) >> pow2);
    }

    public DirectLongList subset(int lo, int hi) {
        DirectLongList that = new DirectLongList(hi - lo);
        Unsafe.getUnsafe().copyMemory(start + (lo << 3), that.start, (hi - lo) << 3);
        that.pos += (hi - lo) << 3;
        return that;
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
        Unsafe.getUnsafe().setMemory(start, limit - start + onePow2, (byte) v);
    }

    void ensureCapacity() {
        if (this.pos > limit) {
            extend((int) ((limit - start + onePow2) >> (pow2 - 1)));
        }
    }

    private void extend(long capacity) {
        long address = Unsafe.malloc(this.capacity = ((capacity << pow2) + CACHE_LINE_SIZE));
        long start = address + (address & (CACHE_LINE_SIZE - 1));
        Unsafe.getUnsafe().copyMemory(this.start, start, limit + onePow2 - this.start);
        if (this.address != 0) {
            Unsafe.free(this.address, this.capacity);
        }
        this.pos = this.pos - this.start + start;
        this.limit = start + ((capacity - 1) << pow2);
        this.address = address;
        this.start = start;
    }
}
