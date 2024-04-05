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

package io.questdb.network;

import io.questdb.std.*;

public class FDSet implements QuietCloseable, Mutable {
    private long _wptr;
    private long address;
    private long lim;
    private int size;

    public FDSet(int size) {
        int l = SelectAccessor.ARRAY_OFFSET + 8 * size;
        this.address = Unsafe.malloc(l, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        this.size = size;
        this._wptr = address + SelectAccessor.ARRAY_OFFSET;
        this.lim = address + l;
    }

    public void add(int fd) {
        if (_wptr == lim) {
            resize();
        }
        long p = _wptr;
        Unsafe.getUnsafe().putLong(p, fd);
        _wptr = p + 8;
    }

    public long address() {
        return address;
    }

    @Override
    public void clear() {
        _wptr = address + SelectAccessor.ARRAY_OFFSET;
    }

    @Override
    public void close() {
        if (address != 0) {
            address = Unsafe.free(address, lim - address, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        }
    }

    public long get(int index) {
        return Unsafe.getUnsafe().getLong(address + SelectAccessor.ARRAY_OFFSET + index * 8L);
    }

    public int getCount() {
        return Unsafe.getUnsafe().getInt(address + SelectAccessor.COUNT_OFFSET);
    }

    public void setCount(int count) {
        Unsafe.getUnsafe().putInt(address + SelectAccessor.COUNT_OFFSET, count);
    }

    private void resize() {
        int sz = size * 2;
        int l = SelectAccessor.ARRAY_OFFSET + 8 * sz;
        long _addr = Unsafe.malloc(l, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        Vect.memcpy(_addr, address, lim - address);
        Unsafe.free(address, lim - address, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        lim = _addr + l;
        size = sz;
        _wptr = _addr + (_wptr - address);
        address = _addr;
    }
}
