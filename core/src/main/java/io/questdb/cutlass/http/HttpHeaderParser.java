/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

package io.questdb.cutlass.http;

import io.questdb.std.*;
import io.questdb.std.str.DirectByteCharSequence;

import java.io.Closeable;

public class HttpHeaderParser implements Mutable, Closeable, HttpRequestHeader {
    private static final String CONTENT_DISPOSITION_HEADER = "Content-Disposition";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private final BoundaryAugmenter boundaryAugmenter = new BoundaryAugmenter();
    private final LowerCaseAsciiCharSequenceObjHashMap<DirectByteCharSequence> headers = new LowerCaseAsciiCharSequenceObjHashMap<>();
    private final long hi;
    private final ObjectPool<DirectByteCharSequence> pool;
    private final DirectByteCharSequence temp = new DirectByteCharSequence();
    private final CharSequenceObjHashMap<DirectByteCharSequence> urlParams = new CharSequenceObjHashMap<>();
    private long _lo;
    private long _wptr;
    private DirectByteCharSequence boundary;
    private DirectByteCharSequence charset;
    private CharSequence contentDisposition;
    private CharSequence contentDispositionFilename;
    private CharSequence contentDispositionName;
    private DirectByteCharSequence contentType;
    private DirectByteCharSequence headerName;
    private long headerPtr;
    private boolean incomplete;
    private boolean isMethod = true;
    private boolean isQueryParams = false;
    private boolean isUrl = true;
    private DirectByteCharSequence method;
    private DirectByteCharSequence methodLine;
    private boolean needMethod;
    private long statementTimeout = -1L;
    private DirectByteCharSequence url;

    public HttpHeaderParser(int bufferLen, ObjectPool<DirectByteCharSequence> pool) {
        final int sz = Numbers.ceilPow2(bufferLen);
        this.headerPtr = Unsafe.malloc(sz, MemoryTag.NATIVE_HTTP_CONN);
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
        this.isMethod = true;
        this.isUrl = true;
        this.isQueryParams = false;
        this.statementTimeout = -1L;
        // do not clear the pool
        // this.pool.clear();
    }

    @Override
    public void close() {
        if (this.headerPtr != 0) {
            this.headerPtr = Unsafe.free(this.headerPtr, this.hi - this.headerPtr, MemoryTag.NATIVE_HTTP_CONN);
            this.boundaryAugmenter.close();
        }
    }

    @Override
    public DirectByteCharSequence getBoundary() {
        return boundaryAugmenter.of(boundary);
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
    public long getStatementTimeout() {
        return statementTimeout;
    }

    @Override
    public CharSequence getUrl() {
        return url;
    }

    @Override
    public DirectByteCharSequence getUrlParam(CharSequence name) {
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
        parseStatementTimeout();
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
                    if (isMethod) {
                        method = pool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isMethod = false;
                    } else if (isUrl) {
                        url = pool.next().of(_lo, _wptr);
                        isUrl = false;
                        _lo = _wptr + 1;
                    } else if (isQueryParams) {
                        int o = urlDecode(_lo, _wptr, urlParams);
                        isQueryParams = false;
                        _lo = _wptr;
                        _wptr -= o;
                    }
                    break;
                case '?':
                    url = pool.next().of(_lo, _wptr);
                    isUrl = false;
                    isQueryParams = true;
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

    private void parseStatementTimeout() {
        statementTimeout = -1L;
        DirectByteCharSequence timeout = getHeader("Statement-Timeout");
        if (timeout == null) {
            return;
        }

        try {
            statementTimeout = Numbers.parseLong(timeout);
        } catch (NumericException ex) {
            // keep -1L as default
        }
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
                    }
                    _lo = rp - offset;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp++, (byte) ' ');
                    continue;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            byte bb = (byte) Numbers.parseHexInt(temp.of(rp, rp += 2));
                            Unsafe.getUnsafe().putByte(wp++, bb);
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

        if (_lo < wp && name != null) {
            map.put(name, pool.next().of(_lo, wp));
        }

        return offset;
    }

    public static class BoundaryAugmenter implements Closeable {
        private static final String BOUNDARY_PREFIX = "\r\n--";
        private final DirectByteCharSequence export = new DirectByteCharSequence();
        private long _wptr;
        private long lim;
        private long lo;

        public BoundaryAugmenter() {
            this.lim = 64;
            this.lo = this._wptr = Unsafe.malloc(this.lim, MemoryTag.NATIVE_HTTP_CONN);
            of0(BOUNDARY_PREFIX);
        }

        @Override
        public void close() {
            if (lo > 0) {
                this.lo = this._wptr = Unsafe.free(this.lo, this.lim, MemoryTag.NATIVE_HTTP_CONN);
            }
        }

        public DirectByteCharSequence of(CharSequence value) {
            int len = value.length() + BOUNDARY_PREFIX.length();
            if (len > lim) {
                resize(len);
            }
            _wptr = lo + BOUNDARY_PREFIX.length();
            of0(value);
            return export.of(lo, _wptr);
        }

        private void of0(CharSequence value) {
            int len = value.length();
            Chars.asciiStrCpy(value, len, _wptr);
            _wptr += len;
        }

        private void resize(int lim) {
            final long prevLim = this.lim;
            this.lim = Numbers.ceilPow2(lim);
            this.lo = this._wptr = Unsafe.realloc(this.lo, prevLim, this.lim, MemoryTag.NATIVE_HTTP_CONN);
            of0(BOUNDARY_PREFIX);
        }
    }

}
