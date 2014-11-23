/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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
    private final int primSize;
    protected long pos;
    protected long start;
    private long address;
    private long limit;

    public AbstractDirectList(int primitiveSize, int capacity) {
        this.primSize = primitiveSize;
        this.address = Unsafe.getUnsafe().allocateMemory((capacity << primitiveSize) + CACHE_LINE_SIZE);
        this.start = this.pos = address + (address & (CACHE_LINE_SIZE - 1));
        this.limit = pos + (capacity << primitiveSize);
    }

    public AbstractDirectList(int primitiveSize, AbstractDirectList that) {
        this(primitiveSize, (int) ((that.pos - that.start) >> primitiveSize));
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
        if (capacity << primSize < limit - start) {
            extend(capacity);
        }
        reset();
    }

    protected void ensureCapacity() {
        if (this.pos >= limit) {
            extend((int) ((limit - start) >> (primSize - 1)));
        }
    }

    private void extend(int capacity) {
        long address = Unsafe.getUnsafe().allocateMemory((capacity << primSize) + CACHE_LINE_SIZE);
        long start = address + (address & (CACHE_LINE_SIZE - 1));
        Unsafe.getUnsafe().copyMemory(this.start, start, capacity << (primSize - 1)); // copy half of future capacity
        Unsafe.getUnsafe().freeMemory(this.address);
        this.pos = this.pos - this.start + start;
        this.limit = start + (capacity << primSize);
        this.address = address;
        this.start = start;
    }

    public int size() {
        return (int) ((pos - start) >> primSize);
    }

    private void free() {
        if (address != 0) {
            Unsafe.getUnsafe().freeMemory(address);
            address = 0;
        }
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
