/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
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
 ******************************************************************************/
package com.nfsdb.net;

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.ObjList;
import com.nfsdb.utils.Unsafe;

import java.io.IOException;

public class Request {
    private final long address;

    private final DirectByteCharSequence method = new DirectByteCharSequence();
    private final DirectByteCharSequence url = new DirectByteCharSequence();
    private final ObjList<DirectByteCharSequence> pool = new ObjList<>(64);
    private final CharSequenceObjHashMap<CharSequence> headers = new CharSequenceObjHashMap<>();
    private final long hi;
    private boolean needMethod = true;
    private long ptr;

    public Request() {
        long size;
        this.address = Unsafe.getUnsafe().allocateMemory(size = 128 * 1024);
        this.ptr = address;
        this.hi = this.address + size;
        extendPool();
    }

    public CharSequenceObjHashMap<CharSequence> getHeaders() {
        return headers;
    }

    public CharSequence getMethod() {
        return method;
    }

    public CharSequence getUrl() {
        return url;
    }

    public void init() {
        needMethod = true;
        this.ptr = this.address;
    }

    public long parse(long lo, long hi) throws IOException {
        if (needMethod) {
            lo = parseMethod(lo, hi);
            needMethod = false;
        }

        long p = lo;
        long _lo = ptr;
        int idx = 0;

        DirectByteCharSequence n = null;
        DirectByteCharSequence v;

        while (p < hi) {
            if (ptr == this.hi) {
                throw new IOException("Request is too long (128K max)");
            }

            byte b = Unsafe.getUnsafe().getByte(p++);
            Unsafe.getUnsafe().putByte(ptr++, b);

            switch (b) {
                case '\n':
                    _lo = ptr;
                    break;
                case ':':
                    if (n == null) {
                        if (idx == pool.size()) {
                            extendPool();
                        }
                        n = pool.getQuick(idx++);
                        n.init(_lo, ptr - 1);
                        _lo = ptr + 1;
                    }
                    break;
                case '\r':

                    if (n != null) {
                        if (idx == pool.size()) {
                            extendPool();
                        }
                        v = pool.getQuick(idx++);
                        v.init(_lo, ptr - 1);
                        _lo = ptr + 1;
                        headers.put(n, v);
                        n = null;
                        break;
                    } else {
                        return p;
                    }
            }
        }
        return p;
    }

    private void extendPool() {
        for (int i = 0; i < 4; i++) {
            pool.add(new DirectByteCharSequence());
        }
    }

    private long parseMethod(long lo, long hi) {
        long p = lo;
        long _lo = ptr;

        boolean m = true;
        boolean u = true;

        while (p < hi) {
            if (ptr == this.hi) {
                throw new IllegalArgumentException("URL is too long");
            }

            byte b = Unsafe.getUnsafe().getByte(p++);
            Unsafe.getUnsafe().putByte(ptr++, b);
            switch ((char) b) {
                case ' ':
                    if (m) {
                        method.init(_lo, ptr);
                        _lo = ptr;
                        m = false;
                    } else if (u) {
                        url.init(_lo, ptr - 1);
                        u = false;
                    }
                    break;
                case '\r':
                    return p;

            }
        }
        return p;
    }
}