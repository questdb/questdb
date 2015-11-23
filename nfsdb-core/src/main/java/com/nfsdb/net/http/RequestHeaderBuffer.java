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
import com.nfsdb.exceptions.MalformedHeaderException;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;

public class RequestHeaderBuffer implements Mutable, Closeable {
    private final ObjectPool<DirectByteCharSequence> pool;
    private final CharSequenceObjHashMap<CharSequence> headers = new CharSequenceObjHashMap<>();
    private final long hi;
    private long _wptr;
    private long headerPtr;
    private CharSequence method;
    private CharSequence url;
    private boolean needMethod;
    private long _lo;
    private CharSequence n;
    private boolean incomplete;
    private CharSequence contentType;
    private CharSequence encoding;
    private CharSequence boundary;
    private CharSequence contentDispositionName;
    private CharSequence contentDispositionFilename;

    public RequestHeaderBuffer(int size, ObjectPool<DirectByteCharSequence> pool) {
        int sz = Numbers.ceilPow2(size);
        this.headerPtr = Unsafe.getUnsafe().allocateMemory(sz);
        this._wptr = headerPtr;
        this.hi = this.headerPtr + sz;
        this.pool = pool;
        clear();
    }

    @Override
    public final void clear() {
        this.needMethod = true;
        this._wptr = this._lo = this.headerPtr;
        this.incomplete = true;
        this.headers.clear();
        this.method = null;
        this.url = null;
        this.n = null;
        this.contentType = null;
        this.boundary = null;
        this.encoding = null;
        this.contentDispositionName = null;
        this.contentDispositionFilename = null;
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

    public CharSequence getBoundary() {
        return boundary;
    }

    public CharSequence getContentDispositionFilename() {
        return contentDispositionFilename;
    }

    public CharSequence getContentDispositionName() {
        return contentDispositionName;
    }

    public CharSequence getContentType() {
        return contentType;
    }

    public CharSequence getEncoding() {
        return encoding;
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

    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT")
    public long write(long ptr, int len, boolean _method) throws HeadersTooLargeException, MalformedHeaderException {
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
                        parseKnownHeaders();
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

    private static DirectByteCharSequence unquote(DirectByteCharSequence that) throws MalformedHeaderException {
        int len = that.length();
        if (len == 0) {
            // zero length mandatory field
            throw MalformedHeaderException.INSTANCE;
        }

        if (that.charAt(0) == '"') {
            if (that.charAt(len - 1) == '"') {
                return that.of(that.getLo() + 1, that.getHi() - 1);
            } else {
                // unclosed quote
                throw MalformedHeaderException.INSTANCE;
            }
        } else {
            return that;
        }
    }

    private void parseContentDisposition() throws MalformedHeaderException {
        CharSequence contentDisposition = get("Content-Disposition");
        if (contentDisposition == null) {
            return;
        }

        long p = ((DirectByteCharSequence) contentDisposition).getLo();
        long _lo = p;
        long hi = ((DirectByteCharSequence) contentDisposition).getHi();

        boolean expectFormData = true;
        boolean swallowSpace = true;

        DirectByteCharSequence name = null;

        while (p <= hi) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == ' ' && swallowSpace) {
                _lo = p;
                continue;
            }

            if (p > hi || b == ';') {
                if (expectFormData) {
                    _lo = p;
                    expectFormData = false;
                    continue;
                }

                if (name == null) {
                    throw MalformedHeaderException.INSTANCE;
                }

                if (Chars.equals("name", name)) {
                    this.contentDispositionName = unquote(pool.next().of(_lo, p - 1));
                    swallowSpace = true;
                    _lo = p;
                    continue;
                }

                if (Chars.equals("filename", name)) {
                    this.contentDispositionFilename = unquote(pool.next().of(_lo, p - 1));
                    _lo = p;
                    continue;
                }

                if (p > hi) {
                    break;
                }
            } else if (b == '=') {
                name = name == null ? pool.next().of(_lo, p - 1) : name.of(_lo, p - 1);
                _lo = p;
                swallowSpace = false;
            }
        }
    }

    private void parseContentType() throws MalformedHeaderException {
        CharSequence seq = get("Content-Type");
        if (seq == null) {
            return;
        }

        long p = ((DirectByteCharSequence) seq).getLo();
        long _lo = p;
        long hi = ((DirectByteCharSequence) seq).getHi();

        DirectByteCharSequence name = null;
        boolean contentType = true;
        boolean swallowSpace = true;

        while (p <= hi) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == ' ' && swallowSpace) {
                _lo = p;
                continue;
            }

            if (p > hi || b == ';') {
                if (contentType) {
                    this.contentType = pool.next().of(_lo, p - 1);
                    _lo = p;
                    contentType = false;
                    continue;
                }

                if (name == null) {
                    throw MalformedHeaderException.INSTANCE;
                }

                if (Chars.equals("encoding", name)) {
                    this.encoding = pool.next().of(_lo, p - 1);
                    _lo = p;
                    continue;
                }

                if (Chars.equals("boundary", name)) {
                    this.boundary = pool.next().of(_lo, p - 1);
                    _lo = p;
                    continue;
                }

                if (p > hi) {
                    break;
                }
            } else if (b == '=') {
                name = name == null ? pool.next().of(_lo, p - 1) : name.of(_lo, p - 1);
                _lo = p;
                swallowSpace = false;
            }
        }
    }

    private void parseKnownHeaders() throws MalformedHeaderException {
        parseContentType();
        parseContentDisposition();
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
