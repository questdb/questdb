/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nfsdb.journal.collections;

import com.nfsdb.journal.utils.Unsafe;

import java.io.Closeable;

public class AbstractDirectList implements Closeable {
    public static final int CACHE_LINE_SIZE = 64;
    private final int pow2;
    private final int onePow2;
    protected long pos;
    protected long start;
    protected long limit;
    private long address;

    public AbstractDirectList(int pow2, long capacity) {
        this.pow2 = pow2;
        this.address = Unsafe.getUnsafe().allocateMemory((capacity << pow2) + CACHE_LINE_SIZE);
        this.start = this.pos = address + (address & (CACHE_LINE_SIZE - 1));
        this.limit = pos + ((capacity - 1) << pow2);
        this.onePow2 = (1 << pow2);
    }

    public AbstractDirectList(int pow2, AbstractDirectList that) {
        this(pow2, (int) ((that.pos - that.start) >> pow2));
        add(that);
    }

    public void add(AbstractDirectList that) {
        int count = (int) (that.pos - that.start);
        if (limit - pos < count) {
            extend((int) (this.limit - this.start + count) >> 1);
        }
        Unsafe.getUnsafe().copyMemory(that.start, this.pos, count);
        this.pos += count;

    }

    public void reset() {
        pos = start;
    }

    public void reset(int capacity) {
        setCapacity(capacity);
        reset();
    }

    public void setCapacity(long capacity) {
        if (capacity << pow2 > limit - start) {
            extend(capacity);
        }
    }

    public void addCapacity(long capacity) {
        if (capacity << pow2 > limit - pos + onePow2) {
            extend((int) (((limit - start + onePow2) >> pow2) + capacity));
        }
    }

    public void setPos(long p) {
        pos = start + (p << pow2);
    }

    protected void ensureCapacity() {
        if (this.pos > limit) {
            extend((int) ((limit - start + onePow2) >> (pow2 - 1)));
        }
    }

    private void extend(long capacity) {
        long address = Unsafe.getUnsafe().allocateMemory((capacity << pow2) + CACHE_LINE_SIZE);
        long start = address + (address & (CACHE_LINE_SIZE - 1));
        Unsafe.getUnsafe().copyMemory(this.start, start, limit + onePow2 - this.start);
        if (this.address != 0) {
            Unsafe.getUnsafe().freeMemory(this.address);
        }
        this.pos = this.pos - this.start + start;
        this.limit = start + ((capacity - 1) << pow2);
        this.address = address;
        this.start = start;
    }

    public int size() {
        return (int) ((pos - start) >> pow2);
    }

    public void free() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;
        }
    }

    public void clear() {
        clear((byte) 0);
    }

    public void clear(byte b) {
        pos = start;
        zero(b);
    }

    public void zero(byte v) {
        Unsafe.getUnsafe().setMemory(start, limit - start + onePow2, v);
    }

    public void close() {
        free();
    }

    @Override
    protected void finalize() throws Throwable {
        free();
        super.finalize();
    }
}
