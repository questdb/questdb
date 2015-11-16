/*******************************************************************************
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
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;

import java.io.Closeable;

public class RequestHeaderBuffer implements Mutable, Closeable {
    private final ObjectPool<DirectByteCharSequence> pool;
    private final CharSequenceObjHashMap<CharSequence> headers = new CharSequenceObjHashMap<>();
    private final long hi;
    private long _wptr;
    private long headerPtr;
    private DirectByteCharSequence method;
    private DirectByteCharSequence url;
    private boolean needMethod;
    private long _lo;
    private DirectByteCharSequence n;
    private boolean incomplete;

    public RequestHeaderBuffer(int size, ObjectPool<DirectByteCharSequence> pool) {
        int sz = Numbers.ceilPow2(size);
        this.headerPtr = Unsafe.getUnsafe().allocateMemory(sz);
        this._wptr = headerPtr;
        this.hi = this.headerPtr + sz;
        this.pool = pool;
        clear();
    }

    @Override
    public void clear() {
        this.needMethod = true;
        this._wptr = this._lo = this.headerPtr;
        pool.clear();
        this.incomplete = true;
        this.headers.clear();
        this.method = null;
        this.url = null;
        this.n = null;
    }

    @Override
    public void close() {
        if (this.headerPtr != 0) {
            Unsafe.getUnsafe().freeMemory(this.headerPtr);
            this.headerPtr = 0;
        }
    }

    public CharSequence get(CharSequence name) {
        return headers.get(name);
    }

    public CharSequence getMethod() {
        return method;
    }

    public CharSequence getUrl() {
        return url;
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    public int size() {
        return headers.size();
    }

    public long write(long ptr, int len, boolean _method) throws HeadersTooLargeException {
        if (_method && needMethod) {
            int l = parseMethod(ptr, len);
            len -= l;
            ptr += l;
            _lo += l - 1;
            needMethod = false;
        }

        long p = ptr;
        long hi = p + len;

        DirectByteCharSequence v;

        while (p < hi) {
            if (_wptr == this.hi) {
                throw HeadersTooLargeException.INSTANCE;
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);

            switch (b) {
                case ':':
                    if (n == null) {
                        n = pool.next().of(_lo, _wptr - 1);
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (n == null) {
                        incomplete = false;
                        return p;
                    }
                    v = pool.next().of(_lo, _wptr - 1);
                    _lo = _wptr;
                    headers.put(n, v);
                    n = null;
                    break;
            }
        }

        return p;
    }

    private int parseMethod(long lo, int len) {
        long p = lo;
        long hi = lo + len;
        long _lo = _wptr;

        boolean m = true;
        boolean u = true;

        while (p < hi) {
            if (_wptr == this.hi) {
                throw new IllegalArgumentException("URL is too long");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);

            switch (b) {
                case ' ':
                    if (m) {
                        method = pool.next().of(_lo, _wptr - 1);
                        _lo = _wptr;
                        m = false;
                    } else if (u) {
                        url = pool.next().of(_lo, _wptr - 1);
                        u = false;
                    }
                    break;
                case '\n':
                    return (int) (p - lo);
            }
        }
        return (int) (p - lo);
    }
}
