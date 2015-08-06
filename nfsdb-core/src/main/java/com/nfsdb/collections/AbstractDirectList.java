/*
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

package com.nfsdb.collections;

import com.nfsdb.utils.Unsafe;

public class AbstractDirectList extends DirectMemoryStructure implements Mutable {
    public static final int CACHE_LINE_SIZE = 64;
    private final int pow2;
    private final int onePow2;
    long pos;
    long start;
    long limit;

    AbstractDirectList(int pow2, long capacity) {
        this.pow2 = pow2;
        this.address = Unsafe.getUnsafe().allocateMemory((capacity << pow2) + CACHE_LINE_SIZE);
        this.start = this.pos = address + (address & (CACHE_LINE_SIZE - 1));
        this.limit = pos + ((capacity - 1) << pow2);
        this.onePow2 = (1 << pow2);
    }

    public final void add(AbstractDirectList that) {
        int count = (int) (that.pos - that.start);
        if (limit - pos < count) {
            extend((int) (this.limit - this.start + count) >> 1);
        }
        Unsafe.getUnsafe().copyMemory(that.start, this.pos, count);
        this.pos += count;

    }

    public void clear() {
        clear((byte) 0);
    }

    public void clear(byte b) {
        pos = start;
        zero(b);
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

    void ensureCapacity() {
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

    private void zero(byte v) {
        Unsafe.getUnsafe().setMemory(start, limit - start + onePow2, v);
    }
}
