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

package io.questdb.cutlass.http;

import io.questdb.std.*;
import io.questdb.std.str.*;

import static io.questdb.cutlass.http.HttpConstants.*;

public class HttpHeaderParser implements Mutable, QuietCloseable, HttpRequestHeader {
    private final BoundaryAugmenter boundaryAugmenter = new BoundaryAugmenter();
    // in theory, it is possible to send multiple cookies on separate lines in the header
    // if we used more cookies, the below map would need to hold a list of CharSequences
    private final LowerCaseUtf8SequenceObjHashMap<DirectUtf8String> headers = new LowerCaseUtf8SequenceObjHashMap<>();
    private final long hi;
    private final ObjectPool<DirectUtf8String> pool;
    private final DirectUtf8String temp = new DirectUtf8String();
    private final Utf8SequenceObjHashMap<DirectUtf8String> urlParams = new Utf8SequenceObjHashMap<>();
    protected boolean incomplete;
    protected Utf8Sequence url;
    private long _lo;
    private long _wptr;
    private DirectUtf8String boundary;
    private DirectUtf8String charset;
    private DirectUtf8String contentDisposition;
    private DirectUtf8String contentDispositionFilename;
    private DirectUtf8String contentDispositionName;
    private long contentLength;
    private DirectUtf8String contentType;
    private DirectUtf8String headerName;
    private long headerPtr;
    private boolean isMethod = true;
    private boolean isProtocol = true;
    private boolean isQueryParams = false;
    private boolean isStatusCode = true;
    private boolean isStatusText = true;
    private boolean isUrl = true;
    private DirectUtf8String method;
    private DirectUtf8String methodLine;
    private boolean needMethod;
    private boolean needProtocol = true;
    private DirectUtf8String protocol;
    private DirectUtf8String protocolLine;
    private long statementTimeout = -1L;
    private DirectUtf8String statusCode;
    private DirectUtf8String statusText;

    public HttpHeaderParser(int bufferLen, ObjectPool<DirectUtf8String> pool) {
        int bufferSize = Numbers.ceilPow2(bufferLen);
        this.headerPtr = this._wptr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.hi = headerPtr + bufferSize;
        this.pool = pool;
        clear();
    }

    @Override
    public void clear() {
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
        this.protocol = null;
        this.statusCode = null;
        this.statusText = null;
        this.isProtocol = true;
        this.isStatusCode = true;
        this.isStatusText = true;
        this.needProtocol = true;
        this.contentLength = -1;
        // do not clear the pool
        // this.pool.clear();
    }

    @Override
    public void close() {
        clear();
        if (headerPtr != 0) {
            headerPtr = _wptr = Unsafe.free(headerPtr, hi - headerPtr, MemoryTag.NATIVE_HTTP_CONN);
            boundaryAugmenter.close();
        }
    }

    @Override
    public DirectUtf8Sequence getBoundary() {
        return boundaryAugmenter.of(boundary);
    }

    @Override
    public DirectUtf8Sequence getCharset() {
        return charset;
    }

    @Override
    public DirectUtf8Sequence getContentDisposition() {
        return contentDisposition;
    }

    @Override
    public DirectUtf8Sequence getContentDispositionFilename() {
        return contentDispositionFilename;
    }

    @Override
    public DirectUtf8Sequence getContentDispositionName() {
        return contentDispositionName;
    }

    @Override
    public long getContentLength() {
        return contentLength;
    }

    @Override
    public DirectUtf8Sequence getContentType() {
        return contentType;
    }

    @Override
    public DirectUtf8Sequence getHeader(Utf8Sequence name) {
        return headers.get(name);
    }

    @Override
    public ObjList<? extends Utf8Sequence> getHeaderNames() {
        return headers.keys();
    }

    @Override
    public DirectUtf8Sequence getMethod() {
        return method;
    }

    @Override
    public DirectUtf8Sequence getMethodLine() {
        return methodLine;
    }

    public DirectUtf8Sequence getProtocolLine() {
        return protocolLine;
    }

    @Override
    public long getStatementTimeout() {
        return statementTimeout;
    }

    public DirectUtf8Sequence getStatusCode() {
        return statusCode;
    }

    public DirectUtf8Sequence getStatusText() {
        return statusText;
    }

    @Override
    public Utf8Sequence getUrl() {
        return url;
    }

    @Override
    public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
        return urlParams.get(name);
    }

    public boolean hasBoundary() {
        return boundary != null;
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    /**
     * Called when there is an error reading from socket.
     *
     * @param err return of the latest read operation
     * @return true if error is recoverable, false if not
     */
    public boolean onRecvError(int err) {
        return false;
    }

    public long parse(long ptr, long hi, boolean _method, boolean _protocol) {
        long p;
        if (_method && needMethod) {
            int l = parseMethod(ptr, hi);
            p = ptr + l;
        } else if (_protocol && needProtocol) {
            int l = parseProtocol(ptr, hi);
            p = ptr + l;
        } else {
            p = ptr;
        }

        DirectUtf8String v;

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
                    boolean added = headers.put(headerName, v);
                    assert added : "duplicate header [" + headerName + "]";
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

    private static DirectUtf8String unquote(CharSequence key, DirectUtf8String that) {
        int len = that.size();
        if (len == 0) {
            throw HttpException.instance("missing value [key=").put(key).put(']');
        }

        if (that.byteAt(0) == '"') {
            if (that.byteAt(len - 1) == '"') {
                return that.of(that.lo() + 1, that.hi() - 1);
            } else {
                throw HttpException.instance("unclosed quote [key=").put(key).put(']');
            }
        } else {
            return that;
        }
    }

    private void parseContentDisposition() {
        DirectUtf8Sequence contentDisposition = getHeader(HEADER_CONTENT_DISPOSITION);
        if (contentDisposition == null) {
            return;
        }

        long p = contentDisposition.lo();
        long _lo = p;
        long hi = contentDisposition.hi();

        boolean expectFormData = true;
        boolean swallowSpace = true;

        DirectUtf8String name = null;

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
                    throw HttpException.instance("Malformed ").put(HEADER_CONTENT_DISPOSITION).put(" header");
                }

                if (Utf8s.equalsAscii("name", name)) {
                    this.contentDispositionName = unquote("name", pool.next().of(_lo, p - 1));
                    swallowSpace = true;
                    _lo = p;
                    name = null;
                    continue;
                }

                if (Utf8s.equalsAscii("filename", name)) {
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

    private void parseContentLength() {
        contentLength = -1;
        DirectUtf8Sequence seq = getHeader(HEADER_CONTENT_LENGTH);
        if (seq == null) {
            return;
        }

        try {
            contentLength = Numbers.parseLong(seq);
        } catch (NumericException ignore) {
            throw HttpException.instance("Malformed ").put(HEADER_CONTENT_LENGTH).put(" header");
        }
    }

    private void parseContentType() {
        DirectUtf8Sequence seq = getHeader(HEADER_CONTENT_TYPE);
        if (seq == null) {
            return;
        }

        long p = seq.lo();
        long _lo = p;
        long hi = seq.hi();

        DirectUtf8String name = null;
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
                    throw HttpException.instance("Malformed ").put(HEADER_CONTENT_TYPE).put(" header");
                }

                if (Utf8s.equalsAscii("charset", name)) {
                    this.charset = pool.next().of(_lo, p - 1);
                    name = null;
                    _lo = p;
                    continue;
                }

                if (Utf8s.equalsAscii("boundary", name)) {
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
        parseContentLength();
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
                    methodLine = pool.next().of(method.lo(), _wptr);
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

    private int parseProtocol(long lo, long hi) {
        long p = lo;
        while (p < hi) {
            if (_wptr == this.hi) {
                throw HttpException.instance("protocol line is too long");
            }

            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == '\r') {
                continue;
            }

            switch (b) {
                case ' ':
                    if (isProtocol) {
                        protocol = pool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isProtocol = false;
                    } else if (isStatusCode) {
                        statusCode = pool.next().of(_lo, _wptr);
                        isStatusCode = false;
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (isStatusText) {
                        statusText = pool.next().of(_lo, _wptr);
                        isStatusText = false;
                    }
                    if (protocol == null) {
                        throw HttpException.instance("bad protocol");
                    }
                    protocolLine = pool.next().of(protocol.lo(), _wptr);
                    needProtocol = false;
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
        DirectUtf8Sequence timeout = getHeader(HEADER_STATEMENT_TIMEOUT);
        if (timeout == null) {
            return;
        }

        try {
            statementTimeout = Numbers.parseLong(timeout);
        } catch (NumericException ex) {
            // keep -1L as default
        }
    }

    private int urlDecode(long lo, long hi, Utf8SequenceObjHashMap<DirectUtf8String> map) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        int offset = 0;

        DirectUtf8String name = null;
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
                            byte bb = (byte) Numbers.parseHexInt(temp.of(rp, rp += 2).asAsciiCharSequence());
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

    public static class BoundaryAugmenter implements QuietCloseable {
        private static final Utf8String BOUNDARY_PREFIX = new Utf8String("\r\n--");
        private final DirectUtf8String export = new DirectUtf8String();
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
                lo = _wptr = Unsafe.free(lo, lim, MemoryTag.NATIVE_HTTP_CONN);
            }
        }

        public DirectUtf8String of(Utf8Sequence value) {
            int len = value.size() + BOUNDARY_PREFIX.size();
            if (len > lim) {
                resize(len);
            }
            _wptr = lo + BOUNDARY_PREFIX.size();
            of0(value);
            return export.of(lo, _wptr);
        }

        private void of0(Utf8Sequence value) {
            int len = value.size();
            Utf8s.strCpy(value, len, _wptr);
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
