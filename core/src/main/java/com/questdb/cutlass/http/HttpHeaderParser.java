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

package com.questdb.cutlass.http;

import com.questdb.std.*;
import com.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class HttpHeaderParser implements Mutable, Closeable, HttpRequestHeader {
    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private final ObjectPool<DirectByteCharSequence> pool;
    private final CharSequenceObjHashMap<DirectByteCharSequence> headers = new CharSequenceObjHashMap<>();
    private final CharSequenceObjHashMap<DirectByteCharSequence> urlParams = new CharSequenceObjHashMap<>();
    private final long hi;
    private final DirectByteCharSequence temp = new DirectByteCharSequence();
    private long _wptr;
    private long headerPtr;
    private DirectByteCharSequence method;
    private DirectByteCharSequence url;
    private DirectByteCharSequence methodLine;
    private boolean needMethod;
    private long _lo;
    private DirectByteCharSequence headerName;
    private boolean incomplete;
    private DirectByteCharSequence contentType;
    private DirectByteCharSequence boundary;
    private CharSequence contentDispositionName;
    private CharSequence contentDisposition;
    private CharSequence contentDispositionFilename;
    private boolean m = true;
    private boolean u = true;
    private boolean q = false;
    private DirectByteCharSequence charset;

    public HttpHeaderParser(int bufferLen, ObjectPool<DirectByteCharSequence> pool) {
        final int sz = Numbers.ceilPow2(bufferLen);
        this.headerPtr = Unsafe.malloc(sz);
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
        this.headerName = null;
        this.contentType = null;
        this.boundary = null;
        this.contentDisposition = null;
        this.contentDispositionName = null;
        this.contentDispositionFilename = null;
        this.urlParams.clear();
        this.m = true;
        this.u = true;
        this.q = false;
        this.pool.clear();
    }

    @Override
    public void close() {
        if (this.headerPtr != 0) {
            Unsafe.free(this.headerPtr, this.hi - this.headerPtr);
            this.headerPtr = 0;
        }
    }

    @Override
    public CharSequence getBoundary() {
        return boundary;
    }

    @Override
    public DirectByteCharSequence getCharset() {
        return charset;
    }

    @Override
    public CharSequence getContentDisposition() {
        return contentDisposition;
    }

    @Override
    public CharSequence getContentDispositionFilename() {
        return contentDispositionFilename;
    }

    @Override
    public CharSequence getContentDispositionName() {
        return contentDispositionName;
    }

    @Override
    public CharSequence getContentType() {
        return contentType;
    }

    @Override
    public DirectByteCharSequence getHeader(CharSequence name) {
        return headers.get(name);
    }

    @Override
    public ObjList<CharSequence> getHeaderNames() {
        return headers.keys();
    }

    @Override
    public CharSequence getMethod() {
        return method;
    }

    @Override
    public CharSequence getMethodLine() {
        return methodLine;
    }

    @Override
    public CharSequence getUrl() {
        return url;
    }

    @Override
    public CharSequence getUrlParam(CharSequence name) {
        return urlParams.get(name);
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    public long parse(long ptr, long hi, boolean _method) {
        long p;
        if (_method && needMethod) {
            int l = parseMethod(ptr, hi);
            p = ptr + l;
        } else {
            p = ptr;
        }

        DirectByteCharSequence v;

        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("header is too large");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);

            switch (b) {
                case ':':
                    if (headerName == null) {
                        headerName = pool.next().of(_lo, _wptr - 1);
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (headerName == null) {
                        incomplete = false;
                        parseKnownHeaders();
                        return p;
                    }
                    v = pool.next().of(_lo, _wptr - 1);
                    _lo = _wptr;
                    headers.put(headerName, v);
                    headerName = null;
                    break;
                default:
                    break;
            }
        }

        return p;
    }

    public int size() {
        return headers.size();
    }

    private static DirectByteCharSequence unquote(CharSequence key, DirectByteCharSequence that) {
        int len = that.length();
        if (len == 0) {
            throw HttpException.instance("missing value [key=").put(key).put(']');
        }

        if (that.charAt(0) == '"') {
            if (that.charAt(len - 1) == '"') {
                return that.of(that.getLo() + 1, that.getHi() - 1);
            } else {
                throw HttpException.instance("unclosed quote [key=").put(key).put(']');
            }
        } else {
            return that;
        }
    }

    private void parseContentDisposition() {
        DirectByteCharSequence contentDisposition = getHeader(CONTENT_DISPOSITION_HEADER);
        if (contentDisposition == null) {
            return;
        }

        long p = contentDisposition.getLo();
        long _lo = p;
        long hi = contentDisposition.getHi();

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
                    this.contentDisposition = pool.next().of(_lo, p - 1);
                    _lo = p;
                    expectFormData = false;
                    continue;
                }

                if (name == null) {
                    throw HttpException.instance("Malformed ").put(CONTENT_DISPOSITION_HEADER).put(" header");
                }

                if (Chars.equals("name", name)) {
                    this.contentDispositionName = unquote("name", pool.next().of(_lo, p - 1));
                    swallowSpace = true;
                    _lo = p;
                    name = null;
                    continue;
                }

                if (Chars.equals("filename", name)) {
                    this.contentDispositionFilename = unquote("filename", pool.next().of(_lo, p - 1));
                    _lo = p;
                    name = null;
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

    private void parseContentType() {
        DirectByteCharSequence seq = getHeader(CONTENT_TYPE_HEADER);
        if (seq == null) {
            return;
        }

        long p = seq.getLo();
        long _lo = p;
        long hi = seq.getHi();

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
                    throw HttpException.instance("Malformed ").put(CONTENT_TYPE_HEADER).put(" header");
                }

                if (Chars.equals("charset", name)) {
                    this.charset = pool.next().of(_lo, p - 1);
                    name = null;
                    _lo = p;
                    continue;
                }

                if (Chars.equals("boundary", name)) {
                    this.boundary = pool.next().of(_lo, p - 1);
                    _lo = p;
                    name = null;
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

    private void parseKnownHeaders() {
        parseContentType();
        parseContentDisposition();
    }

    private int parseMethod(long lo, long hi) {
        long p = lo;
        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("url is too long");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            switch (b) {
                case ' ':
                    if (m) {
                        method = pool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        m = false;
                    } else if (u) {
                        url = pool.next().of(_lo, _wptr);
                        u = false;
                        _lo = _wptr + 1;
                    } else if (q) {
                        int o = urlDecode(_lo, _wptr, urlParams);
                        q = false;
                        _lo = _wptr;
                        _wptr -= o;
                    }
                    break;
                case '?':
                    url = pool.next().of(_lo, _wptr);
                    u = false;
                    q = true;
                    _lo = _wptr + 1;
                    break;
                case '\n':
                    if (method == null) {
                        throw HttpException.instance("bad method");
                    }
                    methodLine = pool.next().of(method.getLo(), _wptr);
                    needMethod = false;
                    this._lo = _wptr;
                    return (int) (p - lo);
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);
        }
        return (int) (p - lo);
    }

    private int urlDecode(long lo, long hi, CharSequenceObjHashMap<DirectByteCharSequence> map) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        int offset = 0;

        CharSequence name = null;

        while (rp < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(rp++);

            switch (b) {
                case '=':
                    if (_lo < wp) {
                        name = pool.next().of(_lo, wp);
                    }
                    _lo = rp - offset;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, pool.next().of(_lo, wp));
                        name = null;
                    } else if (_lo < wp) {
                        map.put(pool.next().of(_lo, wp), null);
                    }
                    _lo = rp - offset;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp++, (byte) ' ');
                    continue;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            Unsafe.getUnsafe().putByte(wp++, (byte) Numbers.parseHexInt(temp.of(rp, rp += 2)));
                            offset += 2;
                            continue;
                        }
                    } catch (NumericException ignore) {
                    }
                    throw HttpException.instance("invalid query encoding");
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(wp++, (byte) b);
        }

        if (_lo < wp) {
            if (name != null) {
                map.put(name, pool.next().of(_lo, wp));
            } else {
                map.put(pool.next().of(_lo, wp), null);
            }
        }

        return offset;
    }
}
