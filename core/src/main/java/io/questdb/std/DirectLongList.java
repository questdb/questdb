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

package io.questdb.std;

import io.questdb.std.str.CharSink;

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
