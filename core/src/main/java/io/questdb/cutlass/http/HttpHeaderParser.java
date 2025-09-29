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

import io.questdb.cairo.Reopenable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.LowerCaseUtf8SequenceObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.ObjList;
import io.questdb.std.ObjectPool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Utf8SequenceObjHashMap;
import io.questdb.std.Vect;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.DirectUtf8Sequence;
import io.questdb.std.str.DirectUtf8Sink;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Comparator;

import static io.questdb.cutlass.http.HttpConstants.*;

public class HttpHeaderParser implements Mutable, QuietCloseable, HttpRequestHeader {
    private static final Comparator<HttpCookie> COOKIE_COMPARATOR = HttpHeaderParser::cookieComparator;
    private static final Log LOG = LogFactory.getLog(HttpHeaderParser.class);
    private final BoundaryAugmenter boundaryAugmenter = new BoundaryAugmenter();
    private final ObjList<HttpCookie> cookieList = new ObjList<>();
    private final ObjectPool<HttpCookie> cookiePool;
    private final Utf8SequenceObjHashMap<HttpCookie> cookies = new Utf8SequenceObjHashMap<>();
    private final ObjectPool<DirectUtf8String> csPool;
    private final LowerCaseUtf8SequenceObjHashMap<DirectUtf8String> headers = new LowerCaseUtf8SequenceObjHashMap<>();
    private final HttpHeaderParameterValue parameterValue = new HttpHeaderParameterValue();
    private final DirectUtf8Sink sink = new DirectUtf8Sink(0, true);
    private final DirectUtf8String temp = new DirectUtf8String();
    private final Utf8SequenceObjHashMap<DirectUtf8String> urlParams = new Utf8SequenceObjHashMap<>();
    protected boolean incomplete;
    protected DirectUtf8String url;
    private long _lo;
    private long _wptr;
    private DirectUtf8String boundary;
    private DirectUtf8String charset;
    private DirectUtf8String contentDisposition;
    private DirectUtf8String contentDispositionFilename;
    private DirectUtf8String contentDispositionName;
    private long contentLength;
    private DirectUtf8String contentType;
    private boolean getRequest = false;
    private DirectUtf8String headerName;
    private long headerPtr;
    private long hi;
    private int ignoredCookieCount;
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
    private boolean postRequest = false;
    private DirectUtf8String protocol;
    private DirectUtf8String protocolLine;
    private boolean putRequest = false;
    private DirectUtf8String query;
    private long statementTimeout = -1L;
    private DirectUtf8String statusCode;
    private DirectUtf8String statusText;

    public HttpHeaderParser(int bufferSize, ObjectPool<DirectUtf8String> csPool) {
        this.headerPtr = this._wptr = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
        this.hi = headerPtr + bufferSize;
        this.csPool = csPool;
        this.cookiePool = new ObjectPool<>(HttpCookie::new, 16);
        clear();
    }

    @Override
    public void clear() {
        this.needMethod = true;
        this._wptr = this._lo = this.headerPtr;
        this.incomplete = true;
        this.headers.clear();
        this.method = null;
        this.methodLine = null;
        this.url = null;
        this.query = null;
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
        this.cookieList.clear();
        this.cookiePool.clear();
        this.ignoredCookieCount = 0;
        // do not clear the pool
        // this.pool.clear();
    }

    @Override
    public void close() {
        clear();
        if (headerPtr != 0) {
            headerPtr = _wptr = hi = Unsafe.free(headerPtr, hi - headerPtr, MemoryTag.NATIVE_HTTP_CONN);
            boundaryAugmenter.close();
        }
        sink.close();
        csPool.clear();
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

    public HttpCookie getCookie(Utf8Sequence cookieName) {
        return cookies.get(cookieName);
    }

    public @NotNull ObjList<HttpCookie> getCookieList() {
        return cookieList;
    }

    @Override
    public DirectUtf8Sequence getHeader(Utf8Sequence name) {
        return headers.get(name);
    }

    @Override
    public ObjList<? extends Utf8Sequence> getHeaderNames() {
        return headers.keys();
    }

    public int getIgnoredCookieCount() {
        return ignoredCookieCount;
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
    public @Nullable DirectUtf8String getQuery() {
        return query;
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
    public DirectUtf8String getUrl() {
        return url;
    }

    @Override
    public DirectUtf8Sequence getUrlParam(Utf8Sequence name) {
        return urlParams.get(name);
    }

    public boolean hasBoundary() {
        return boundary != null;
    }

    @Override
    public boolean isGetRequest() {
        return getRequest;
    }

    public boolean isIncomplete() {
        return incomplete;
    }

    @Override
    public boolean isPostRequest() {
        return postRequest;
    }

    @Override
    public boolean isPutRequest() {
        return putRequest;
    }

    /**
     * Called when there is an error reading from socket.
     *
     * @param err return of the latest read operation
     * @return true if error is recoverable, false if not
     */
    @SuppressWarnings("unused")
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
                        headerName = csPool.next().of(_lo, _wptr - 1);
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (headerName == null) {
                        incomplete = false;
                        parseKnownHeaders();
                        return p;
                    }
                    if (HttpKeywords.isHeaderSetCookie(headerName)) {
                        cookieParse(_lo, _wptr - 1);
                    } else {
                        headers.put(headerName, csPool.next().of(_lo, _wptr - 1));
                    }
                    headerName = null;
                    _lo = _wptr;
                    break;
                default:
                    break;
            }
        }
        return p;
    }

    public void reopen(int bufferSize) {
        if (headerPtr == 0) {
            this.headerPtr = this._wptr = this._lo = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_HTTP_CONN);
            this.hi = headerPtr + bufferSize;
        }
        boundaryAugmenter.reopen();
    }

    public int size() {
        return headers.size();
    }

    private static int cookieComparator(HttpCookie o1, HttpCookie o2) {
        int pathLen1 = o1.path == null ? 0 : o1.path.size();
        int pathLen2 = o2.path == null ? 0 : o2.path.size();
        int diff = pathLen2 - pathLen1;
        return diff != 0 ? diff : Long.compare(o2.expires, o1.expires);
    }

    private static long cookieSkipBytes(long p, long hi) {
        while (p < hi && Unsafe.getUnsafe().getByte(p) != ';') {
            p++;
        }
        return p;
    }

    private static boolean isEquals(long p) {
        return Unsafe.getUnsafe().getByte(p) == '=';
    }

    private static int lowercaseByte(long p) {
        return Unsafe.getUnsafe().getByte(p) | 0x20;
    }

    private static int swarLowercaseInt(long p) {
        return Unsafe.getUnsafe().getInt(p) | 0x20202020;
    }

    private static long swarLowercaseLong(long p) {
        return Unsafe.getUnsafe().getLong(p) | 0x2020202020202020L;
    }

    private static int swarLowercaseShort(long p) {
        return Unsafe.getUnsafe().getShort(p) | 0x2020;
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

    private long cookieLogUnknownAttributeError(long p, long lo, long hi) {
        long pnext = cookieSkipBytes(p, hi);
        LOG.error()
                .$("unknown cookie attribute [attribute=").$(csPool.next().of(p, pnext))
                .$(", cookie=").$(csPool.next().of(lo, hi))
                .I$();
        return pnext;
    }

    private void cookieParse(long lo, long hi) {

        // let's be pessimistic in case the switch exists early

        ignoredCookieCount++;
        HttpCookie cookie = null;
        boolean attributeArea = false;
        long p0 = lo;
        int nonSpaceCount = 0; // non-space character count since p0
        for (long p = lo; p < hi; p++) {
            char c = (char) Unsafe.getUnsafe().getByte(p);
            switch (c | 32) {
                case '=':
                    if (p0 == p) {
                        LOG.error().$("cookie name is missing").$();
                        return;
                    }
                    if (cookie != null) {
                        // this means that we have an attribute with name, which we did not
                        // recognize.
                        p = cookieLogUnknownAttributeError(p0, lo, hi);
                    } else {
                        cookie = cookiePool.next();
                        cookie.cookieName = csPool.next().of(p0, p);
                    }
                    p0 = p + 1;
                    nonSpaceCount = 0;
                    break;
                case ';':
                    if (cookie == null) {
                        LOG.error().$("cookie name is missing").$();
                        return;
                    }
                    cookie.value = csPool.next().of(p0, p);
                    attributeArea = true;

                    p0 = p + 1;
                    nonSpaceCount = 0;
                    break;
                case 'd':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Domain=<domain-value>
                        // 0x69616d6f = "omai" from Domain
                        if (
                                p + 6 < hi
                                        && swarLowercaseInt(p + 1) == 0x69616d6f
                                        && lowercaseByte(p + 5) == 'n'
                                        && isEquals(p + 6)
                        ) {
                            p += 7;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.domain = csPool.next().of(p0, p);
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'p':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Path=<path-value>
                        if (p + 4 < hi && swarLowercaseInt(p) == 0x68746170 && isEquals(p + 4)) {
                            p += 5;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.path = csPool.next().of(p0, p);
                        } else if (p + 10 < hi && swarLowercaseLong(p + 1) == 0x6e6f697469747261L && swarLowercaseShort(p + 9) == 0x6465) {
                            // Partitioned, len = 11
                            p += 11;
                            p = cookieSkipBytes(p, hi);
                            cookie.partitioned = true;
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 's':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Secure, len = 6, 'S' + 0x72756365 + 'e'
                        if (p + 5 < hi && swarLowercaseInt(p + 1) == 0x72756365 && lowercaseByte(p + 5) == 'e') {
                            // Secure
                            p += 6;
                            p = cookieSkipBytes(p, hi);
                            cookie.secure = true;
                        } else if (p + 8 < hi && swarLowercaseLong(p) == 0x65746973656d6173L && isEquals(p + 8)) {
                            // SameSite=<value>
                            p += 9;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            cookie.sameSite = csPool.next().of(p0, p);
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'h':
                    if (attributeArea && nonSpaceCount == 0) {
                        // HttpOnly, len = 8, as long 0x796c6e4f70747448L
                        if (p + 7 < hi && swarLowercaseLong(p) == 0x796c6e6f70747468L) {
                            // HttpOnly
                            p += 8;
                            p = cookieSkipBytes(p, hi);
                            cookie.httpOnly = true;
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'm':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Max-Age=<number>, key len = 7
                        if (p + 7 < hi && swarLowercaseInt(p + 1) == 0x612d7861 && swarLowercaseShort(p + 5) == 0x6567 && isEquals(p + 7)) {
                            p += 8;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            Utf8Sequence v = csPool.next().of(p0, p);
                            try {
                                cookie.maxAge = Numbers.parseLong(v);
                            } catch (NumericException e) {
                                LOG.error().$("invalid cookie Max-Age value [value=").$(v).I$();
                            }
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case 'e':
                    if (attributeArea && nonSpaceCount == 0) {
                        // Expires=<date>
                        // 0x69727078 = "irpx" from Expires
                        if (
                                p + 7 < hi
                                        && swarLowercaseInt(p + 1) == 0x72697078
                                        && lowercaseByte(p + 6) == 's'
                                        && isEquals(p + 7)
                        ) {
                            p += 8;
                            p0 = p;
                            p = cookieSkipBytes(p, hi);
                            Utf8Sequence v = csPool.next().of(p0, p);
                            try {
                                cookie.expires = MicrosFormatUtils.parseHTTP(v.asAsciiCharSequence());
                            } catch (NumericException e) {
                                LOG.error().$("invalid cookie Expires value [value=").$(v).I$();
                            }
                        } else {
                            p = cookieLogUnknownAttributeError(p, lo, hi);
                        }
                        p0 = p + 1;
                    } else {
                        nonSpaceCount++;
                    }
                    break;
                case ' ':
                    if (nonSpaceCount == 0) {
                        break;
                    }
                    // fallthrough
                default:
                    nonSpaceCount++;
                    break;
            }
        }
        if (cookie == null) {
            LOG.error().$("malformed cookie [value=").$safe(csPool.next().of(lo, hi)).I$();
            return;
        }
        if (cookie.cookieName != null && cookie.value == null) {
            cookie.value = csPool.next().of(p0, hi);
        }
        ignoredCookieCount--;
        cookieList.add(cookie);
    }

    private void cookieSortAndMap() {
        cookieList.sort(COOKIE_COMPARATOR);
        for (int i = 0, n = cookieList.size(); i < n; i++) {
            HttpCookie cookie = cookieList.getQuick(i);
            int index = cookies.keyIndex(cookie.cookieName);
            if (index > -1) {
                cookies.putAt(index, cookie.cookieName, cookie);
            }
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
                    this.contentDisposition = csPool.next().of(_lo, p - 1);
                    _lo = p;
                    expectFormData = false;
                    continue;
                }

                if (name == null) {
                    throw HttpException.instance("Malformed ").put(HEADER_CONTENT_DISPOSITION).put(" header");
                }

                if (Utf8s.equalsAscii("name", name)) {
                    this.contentDispositionName = unquote("name", csPool.next().of(_lo, p - 1));
                    swallowSpace = true;
                    _lo = p;
                    name = null;
                    continue;
                }

                if (Utf8s.equalsAscii("filename", name)) {
                    this.contentDispositionFilename = unquote("filename", csPool.next().of(_lo, p - 1));
                    _lo = p;
                    name = null;
                    continue;
                }

                if (p > hi) {
                    break;
                }
            } else if (b == '=') {
                name = name == null ? csPool.next().of(_lo, p - 1) : name.of(_lo, p - 1);
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
        final long hi = seq.hi();

        long lo = HttpSemantics.swallowOWS(p, hi);
        p = parseMediaType(lo, hi);
        this.contentType = csPool.next().of(lo, p);
        p = HttpSemantics.swallowOWS(p, hi);

        // Parsing parameters (charset, boundary).
        while (p < hi) {
            p = HttpSemantics.swallowNextDelimiter(p, hi);
            p = HttpSemantics.swallowOWS(p, hi);

            final long nameLo = p;
            final long nameHi = HttpSemantics.swallowTokens(p, hi);
            if (nameHi == nameLo) {
                throw HttpException.instance("Malformed ").put(HEADER_CONTENT_TYPE).put(" header");
            }

            p = HttpSemantics.swallowOWS(nameHi, hi);
            if ((char) Unsafe.getUnsafe().getByte(p) != '=') {
                throw HttpException.instance("Malformed ")
                        .put(HEADER_CONTENT_TYPE)
                        .put(" header, expected '=' but got '")
                        .put((char) Unsafe.getUnsafe().getByte(p))
                        .put('\'');
            }
            p = HttpSemantics.swallowOWS(p + 1, hi);

            HttpHeaderParameterValue value = parseParameterValue(p, hi);
            if (Utf8s.equalsAscii("charset", nameLo, nameHi)) {
                this.charset = value.getStr();
            } else if (Utf8s.equalsAscii("boundary", nameLo, nameHi)) {
                this.boundary = value.getStr();
            }

            p = value.getHi();
        }
    }

    private void parseKnownHeaders() {
        parseContentType();
        parseContentDisposition();
        parseStatementTimeout();
        parseContentLength();
        cookieSortAndMap();
    }

    private long parseMediaType(long lo, long hi) {
        // media-type format is: type "/" subtype
        // type and subtype are tokens
        long p = HttpSemantics.swallowTokens(lo, hi);
        if (p > hi || ((char) Unsafe.getUnsafe().getByte(p) != '/')) {
            return p;
        }
        return HttpSemantics.swallowTokens(p + 1, hi);
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
                        method = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isMethod = false;
                    } else if (isUrl) {
                        url = csPool.next().of(_lo, _wptr);
                        isUrl = false;
                        _lo = _wptr + 1;
                    } else if (isQueryParams) {
                        query = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isQueryParams = false;
                        break;
                    }
                    break;
                case '?':
                    url = csPool.next().of(_lo, _wptr);
                    isUrl = false;
                    isQueryParams = true;
                    _lo = _wptr + 1;
                    break;
                case '\n':
                    if (method == null) {
                        throw HttpException.instance("bad method");
                    }
                    methodLine = csPool.next().of(method.lo(), _wptr);
                    needMethod = false;

                    getRequest = HttpKeywords.isGET(method);
                    postRequest = HttpKeywords.isPOST(method);
                    putRequest = HttpKeywords.isPUT(method);

                    // parse and decode query string
                    if (query != null) {
                        final int querySize = query.size();
                        final long newBoundary = _wptr + querySize;
                        if (querySize > 0 && newBoundary < this.hi) {
                            Vect.memcpy(_wptr, query.ptr(), querySize);
                            int o = urlDecode(_wptr, newBoundary, urlParams);
                            _wptr = newBoundary - o;
                        } else {
                            throw HttpException.instance("URL query string is too long");
                        }
                    }
                    this._lo = _wptr;
                    return (int) (p - lo);
                default:
                    break;
            }
            Unsafe.getUnsafe().putByte(_wptr++, (byte) b);
        }
        return (int) (p - lo);
    }

    private HttpHeaderParameterValue parseParameterValue(long lo, long hi) {
        if (lo > hi) {
            return parameterValue;
        }

        long p = lo;
        char b = (char) Unsafe.getUnsafe().getByte(p++);
        // fast path for unquoted tokens
        if (b != '"') {
            // Instead of relying on swallowTokens we are being more lenient and allow any characters until the ';' delimiter.
            while (p < hi && ((char) Unsafe.getUnsafe().getByte(p) != ';')) {
                p++;
            }
            parameterValue.of(p, csPool.next().of(lo, p));
            return parameterValue;
        }

        final long s_lo = sink.size();
        boolean escaped = false;
        while (p <= hi) {
            b = (char) Unsafe.getUnsafe().getByte(p++);

            if (escaped || (b != '\\' && b != '"')) {
                sink.put(b);
                escaped = false;
            } else if (b == '\\') {
                escaped = true;
            } else {
                p = HttpSemantics.swallowOWS(p, hi);
                break;
            }
        }

        parameterValue.of(p, csPool.next().of(sink.ptr() + s_lo, sink.hi()));
        return parameterValue;
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
                        protocol = csPool.next().of(_lo, _wptr);
                        _lo = _wptr + 1;
                        isProtocol = false;
                    } else if (isStatusCode) {
                        statusCode = csPool.next().of(_lo, _wptr);
                        isStatusCode = false;
                        _lo = _wptr + 1;
                    }
                    break;
                case '\n':
                    if (isStatusText) {
                        statusText = csPool.next().of(_lo, _wptr);
                        isStatusText = false;
                    }
                    if (protocol == null) {
                        throw HttpException.instance("bad protocol");
                    }
                    protocolLine = csPool.next().of(protocol.lo(), _wptr);
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
                        name = csPool.next().of(_lo, wp);
                    }
                    _lo = rp - offset;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, csPool.next().of(_lo, wp));
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
            map.put(name, csPool.next().of(_lo, wp));
        }

        return offset;
    }

    public static class BoundaryAugmenter implements Reopenable, QuietCloseable {
        private static final Utf8String BOUNDARY_PREFIX = new Utf8String("\r\n--");
        private final DirectUtf8String export = new DirectUtf8String();
        private long _wptr;
        private long lim;
        private long lo;

        public BoundaryAugmenter() {
            this.lim = 64;
            this.lo = this._wptr = Unsafe.malloc(lim, MemoryTag.NATIVE_HTTP_CONN);
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

        @Override
        public void reopen() {
            if (lo == 0) {
                this.lo = this._wptr = Unsafe.malloc(lim, MemoryTag.NATIVE_HTTP_CONN);
                of0(BOUNDARY_PREFIX);
            }
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
