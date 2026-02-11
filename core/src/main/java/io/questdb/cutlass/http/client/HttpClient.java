/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

package io.questdb.cutlass.http.client;

import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.cutlass.http.HttpKeywords;
import io.questdb.cutlass.line.array.ArrayBufferAppender;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacade;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.network.TlsSessionInitFailedException;
import io.questdb.std.BinarySequence;
import io.questdb.std.Chars;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjectPool;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectUtf8String;
import io.questdb.std.str.StringSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8StringSink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

import java.net.HttpURLConnection;

import static io.questdb.cutlass.http.HttpConstants.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public abstract class HttpClient implements QuietCloseable {
    private static final String HEADER_CONTENT_LENGTH = "Content-Length: ";
    private static final String HTTP_NO_CONTENT = String.valueOf(HttpURLConnection.HTTP_NO_CONTENT);
    private static final Log LOG = LogFactory.getLog(HttpClient.class);
    protected final NetworkFacade nf;
    protected final Socket socket;
    private final HttpClientCookieHandler cookieHandler;
    private final ObjectPool<DirectUtf8String> csPool = new ObjectPool<>(DirectUtf8String.FACTORY, 64);
    private final int defaultTimeout;
    private final boolean fixBrokenConnection;
    private final int maxBufferSize;
    private final Request request = new Request();
    private final ResponseHeaders responseHeaders;
    private final int responseParserBufSize;
    private long bufLo;
    private int bufferSize;
    private long contentStart = -1;
    private CharSequence host;
    private int port;
    private long ptr = bufLo;
    private long responseParserBufLo;

    public HttpClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        this.nf = configuration.getNetworkFacade();
        this.socket = socketFactory.newInstance(nf, LOG);
        this.defaultTimeout = configuration.getTimeout();
        this.cookieHandler = configuration.getCookieHandlerFactory().getInstance();
        this.bufferSize = configuration.getInitialRequestBufferSize();
        this.maxBufferSize = configuration.getMaximumRequestBufferSize();
        this.responseParserBufSize = configuration.getResponseBufferSize();
        this.fixBrokenConnection = configuration.fixBrokenConnection();
        this.bufLo = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        this.responseParserBufLo = Unsafe.malloc(responseParserBufSize, MemoryTag.NATIVE_DEFAULT);
        this.responseHeaders = new ResponseHeaders(responseParserBufLo, responseParserBufSize, defaultTimeout, 4096, csPool);
    }

    @Override
    public void close() {
        disconnect();
        if (bufLo != 0) {
            Unsafe.free(bufLo, bufferSize, MemoryTag.NATIVE_DEFAULT);
            bufLo = 0;
            assert responseParserBufLo != 0;
            Unsafe.free(responseParserBufLo, responseParserBufSize, MemoryTag.NATIVE_DEFAULT);
            responseParserBufLo = 0;
        }
        responseHeaders.free();
    }

    public void disconnect() {
        Misc.free(socket);
    }

    @TestOnly
    public ResponseHeaders getResponseHeaders() {
        return responseHeaders;
    }

    public Request newRequest(CharSequence host, int port) {
        if (!Chars.equalsNc(host, this.host) || port != this.port) {
            // Can't reuse the existing connection, if any.
            socket.close();
        }
        this.host = host;
        this.port = port;
        ptr = bufLo;
        contentStart = -1;
        request.contentLengthHeaderReserved = 0;
        request.state = Request.STATE_REQUEST;
        return request;
    }

    private void checkCapacity(long capacity) {
        long usedBytes = ptr - bufLo;
        final long requiredSize = usedBytes + capacity;
        if (requiredSize > bufferSize) {
            growBuffer(requiredSize);
        }
    }

    private int dieIfNegative(int byteCount) {
        if (byteCount < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
    }

    private int dieIfNotPositive(int byteCount) {
        if (byteCount < 0) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        if (byteCount == 0) {
            throw new HttpClientException("timed out [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
    }

    private void growBuffer(long requiredSize) {
        if (requiredSize > maxBufferSize) {
            throw new HttpClientException("transaction is too large, either flush more frequently or " +
                    "increase buffer size \"max_buf_size\" [maxBufferSize=")
                    .putSize(maxBufferSize)
                    .put(", transactionSize=")
                    .putSize(requiredSize)
                    .put(']');
        }
        long newBufferSize = Math.min(Numbers.ceilPow2((int) requiredSize), maxBufferSize);
        long newBufLo = Unsafe.realloc(bufLo, bufferSize, newBufferSize, MemoryTag.NATIVE_DEFAULT);

        long offset = newBufLo - bufLo;

        ptr += offset;
        bufLo = newBufLo;
        bufferSize = (int) newBufferSize;
        if (contentStart > -1) {
            contentStart += offset;
        }
    }

    private int recvOrDie(long lo, int len, int timeout) {
        long startTimeNanos = System.nanoTime();
        int n = dieIfNegative(socket.recv(lo, len));

        if (n == 0) {
            ioWait(remainingTime(timeout, startTimeNanos), IOOperation.READ);
            n = dieIfNegative(socket.recv(lo, len));
        }
        return n;
    }

    private int recvOrDie(long addr, int timeout) {
        return recvOrDie(addr, (int) (responseParserBufSize - (addr - responseParserBufLo)), timeout);
    }

    private int remainingTime(int timeoutMillis, long startTimeNanos) {
        timeoutMillis -= (int) NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);
        if (timeoutMillis <= 0) {
            throw new HttpClientException("timed out [errno=").errno(nf.errno()).put(']');
        }
        return timeoutMillis;
    }

    private int sendOrDie(long lo, int len, int timeoutMillis) {
        long startTimeNanos = System.nanoTime();
        ioWait(timeoutMillis, IOOperation.WRITE);
        int n = dieIfNotPositive(socket.send(lo, len));
        while (socket.wantsTlsWrite()) {
            timeoutMillis = remainingTime(timeoutMillis, startTimeNanos);
            ioWait(timeoutMillis, IOOperation.WRITE);
            dieIfNegative(socket.tlsIO(Socket.WRITE_FLAG));
        }
        return n;
    }

    protected void dieWaiting(int n) {
        if (n == 1) {
            return;
        }

        if (n == 0) {
            throw new HttpClientException("timed out [errno=").put(nf.errno()).put(']');
        }

        throw new HttpClientException("queue error [errno=").put(nf.errno()).put(']');
    }

    protected abstract void ioWait(int timeout, int op);

    protected abstract void setupIoWait();

    private static class BinarySequenceAdapter implements BinarySequence, Mutable {
        private final Utf8StringSink baseSink = new Utf8StringSink();

        @Override
        public byte byteAt(long index) {
            return baseSink.byteAt((int) index);
        }

        @Override
        public void clear() {
            baseSink.clear();
        }

        @Override
        public long length() {
            return baseSink.size();
        }

        BinarySequenceAdapter colon() {
            baseSink.putAscii(':');
            return this;
        }

        BinarySequenceAdapter put(CharSequence value) {
            baseSink.put(value);
            return this;
        }
    }

    private class ChunkedResponseImpl extends AbstractChunkedResponse {
        public ChunkedResponseImpl(long bufLo, long bufHi, int defaultTimeout) {
            super(bufLo, bufHi, defaultTimeout);
        }

        @Override
        protected int recvOrDie(long bufLo, long bufHi, int timeout) {
            return HttpClient.this.recvOrDie(bufLo, timeout);
        }
    }

    public class Request implements Utf8Sink, ArrayBufferAppender {
        private static final int STATE_CONTENT = 5;
        private static final int STATE_HEADER = 4;
        private static final int STATE_QUERY = 3;
        private static final int STATE_REQUEST = 0;
        private static final int STATE_URL = 1;
        private static final int STATE_URL_DONE = 2;
        private BinarySequenceAdapter binarySequenceAdapter;
        private int contentLengthHeaderReserved = 0;
        private int state;
        private boolean urlEncode = false;

        public Request DELETE() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return putAscii("DELETE ");
        }

        public Request GET() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return putAscii("GET ");
        }

        public Request POST() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return putAscii("POST ");
        }

        public Request PUT() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return putAscii("PUT ");
        }

        public Request authBasic(CharSequence username, CharSequence password) {
            beforeHeader();
            putAsciiInternal("Authorization: Basic ");
            if (binarySequenceAdapter == null) {
                binarySequenceAdapter = new BinarySequenceAdapter();
            }
            binarySequenceAdapter.clear();
            binarySequenceAdapter.put(username).colon().put(password);
            Chars.base64Encode(binarySequenceAdapter, (int) binarySequenceAdapter.length(), this);
            eol();
            if (cookieHandler != null) {
                cookieHandler.setCookies(this, username);
            }
            return this;
        }

        public Request authToken(CharSequence username, CharSequence token) {
            beforeHeader();
            putAsciiInternal("Authorization: Bearer ");
            putAsciiInternal(token);
            eol();
            if (cookieHandler != null) {
                cookieHandler.setCookies(this, username);
            }
            return this;
        }

        public int getContentLength() {
            if (contentStart > -1) {
                return (int) (ptr - contentStart);
            } else {
                return 0;
            }
        }

        public long getContentStart() {
            return contentStart;
        }

        public long getPtr() {
            return ptr;
        }

        public Request header(CharSequence name, CharSequence value) {
            beforeHeader();
            put(name).putAsciiInternal(": ").put(value);
            return eol();
        }

        @Override
        public Request put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                checkCapacity(size);
                Utf8s.strCpy(us, size, ptr);
                ptr += size;
            }
            return this;
        }

        @Override
        public Request put(byte b) {
            checkCapacity(1);
            Unsafe.getUnsafe().putByte(ptr, b);
            ptr++;
            return this;
        }

        @Override
        public Request put(@Nullable CharSequence cs) {
            Utf8Sink.super.put(cs);
            return this;
        }

        @Override
        public Request put(char c) {
            Utf8Sink.super.put(c);
            return this;
        }

        @Override
        public Request putAscii(char c) {
            if (urlEncode) {
                putUrlEncoded(c);
            } else {
                putAsciiInternal(c);
            }
            return this;
        }

        @Override
        public Request putAscii(@Nullable CharSequence cs) {
            Utf8Sink.super.putAscii(cs);
            return this;
        }

        @Override
        public Request putAsciiQuoted(@NotNull CharSequence cs) {
            putAsciiInternal('\"').putAscii(cs).putAsciiInternal('\"');
            return this;
        }

        @Override
        public void putBlockOfBytes(long from, long len) {
            checkCapacity(len);
            Vect.memcpy(ptr, from, len);
            ptr += len;
        }

        @Override
        public void putByte(byte value) {
            put(value);
        }

        @Override
        public void putDouble(double value) {
            checkCapacity(Double.BYTES);
            Unsafe.getUnsafe().putDouble(ptr, value);
            ptr += Double.BYTES;
        }

        @Override
        public void putInt(int value) {
            checkCapacity(Integer.BYTES);
            Unsafe.getUnsafe().putInt(ptr, value);
            ptr += Integer.BYTES;
        }

        @Override
        public void putLong(long value) {
            checkCapacity(Long.BYTES);
            Unsafe.getUnsafe().putLong(ptr, value);
            ptr += Long.BYTES;
        }

        @Override
        public Request putNonAscii(long lo, long hi) {
            final long size = hi - lo;
            checkCapacity(size);
            Vect.memcpy(ptr, lo, size);
            ptr += size;
            return this;
        }

        @Override
        public Request putQuoted(@NotNull CharSequence cs) {
            putAsciiInternal('\"').put(cs).putAsciiInternal('\"');
            return this;
        }

        public Request query(CharSequence name, CharSequence value) {
            assert state == STATE_URL_DONE || state == STATE_QUERY;
            if (state == STATE_URL_DONE) {
                putAsciiInternal('?');
            } else {
                putAsciiInternal('&');
            }
            state = STATE_QUERY;
            urlEncode = true;
            try {
                put(name).putAsciiInternal('=').put(value);
            } finally {
                urlEncode = false;
            }
            return this;
        }

        public Request query(CharSequence name, Utf8Sequence value) {
            assert state == STATE_URL_DONE || state == STATE_QUERY;
            if (state == STATE_URL_DONE) {
                putAsciiInternal('?');
            } else {
                putAsciiInternal('&');
            }
            state = STATE_QUERY;
            urlEncode = true;
            try {
                put(name).putAsciiInternal('=').put(value);
            } finally {
                urlEncode = false;
            }
            return this;
        }

        public ResponseHeaders send() {
            return send(defaultTimeout);
        }

        /**
         * Sends the HTTP request to the specified host and port with connection management.
         * <p>
         * This method intelligently manages the underlying socket connection:
         * <ul>
         *   <li>Reuses the existing connection if already connected to the same host:port</li>
         *   <li>Establishes a new connection if not connected or connecting to a different host:port</li>
         *   <li>Automatically reconnects if the existing connection is closed or broken (when configured)</li>
         * </ul>
         * <p>
         * The request must be in a valid state (URL set, optional query parameters, headers, or content added)
         * before calling this method. The HTTP version (1.1) and Host header are automatically appended.
         * <p>
         * Common use cases include:
         * <ul>
         *   <li>Failover scenarios - retry the same request on a different server</li>
         *   <li>Multi-publishing - send the same data to multiple endpoints</li>
         * </ul>
         * Important: If the request buffer already contains an HTTP request header with a host
         * then the host will not change! This means reverse proxies routing requests to different
         * host based on the Host header will not work! Routing on TLS SNI will not be affected.
         *
         * @param host    the hostname or IP address to connect to
         * @param port    the port number to connect on
         * @param timeout the request timeout in milliseconds for socket operations
         * @return the parsed response headers from the server
         * @throws AssertionError if the request is not in a valid state
         */
        public ResponseHeaders send(CharSequence host, int port, int timeout) {
            assert state == STATE_URL_DONE || state == STATE_QUERY || state == STATE_HEADER || state == STATE_CONTENT;
            if (socket == null || socket.isClosed()) {
                connect(host, port);
            } else if (fixBrokenConnection && nf.testConnection(socket.getFd(), responseParserBufLo, 1)) {
                socket.close();
                connect(host, port);
            } else if (!Chars.equalsNc(host, HttpClient.this.host) || (port != HttpClient.this.port)) {
                socket.close();
                connect(host, port);
                HttpClient.this.host = host;
                HttpClient.this.port = port;
            }

            if (state == STATE_URL_DONE || state == STATE_QUERY) {
                putAsciiInternal(" HTTP/1.1").putEOL();
                putAsciiInternal("Host: ").put(host).putAscii(':').put(port).putEOL();
                state = STATE_HEADER;
            }

            if (contentStart > -1) {
                assert state == STATE_CONTENT;
                sendHeaderAndContent(Integer.MAX_VALUE, timeout);
            } else {
                eol();
                doSend(bufLo, ptr, timeout);
            }
            responseHeaders.clear();
            return responseHeaders;
        }

        public ResponseHeaders send(int timeout) {
            return send(host, port, timeout);
        }

        public void sendPartialContent(int maxContentLen, int timeout) {
            if (state != STATE_CONTENT || contentStart == -1) {
                throw new IllegalStateException("No content to send");
            }
            if (socket == null || socket.isClosed()) {
                connect(host, port);
            }

            sendHeaderAndContent(maxContentLen, timeout);
        }

        public Request setCookie(CharSequence name, CharSequence value) {
            beforeHeader();
            put(HEADER_COOKIE).putAscii(": ").put(name);
            if (value != null) {
                putAscii(COOKIE_VALUE_SEPARATOR).put(value);
            }
            eol();
            return this;
        }

        @Override
        public String toString() {
            StringSink ss = new StringSink();
            DirectUtf8String s = new DirectUtf8String();
            s.of(bufLo, ptr);
            ss.put(s);
            return ss.toString();
        }

        public void trimContentToLen(int contentLen) {
            ptr = contentStart + contentLen;
        }

        public void truncate() {
            throw new UnsupportedOperationException();
        }

        public Request url(CharSequence url) {
            assert state == STATE_URL;
            state = STATE_URL_DONE;
            return put(url);
        }

        public Request withChunkedContent() {
            beforeHeader();

            header("Transfer-Encoding", "chunked");
            putEOL();

            contentLengthHeaderReserved = 0;
            contentStart = ptr;
            state = STATE_CONTENT;
            return this;
        }

        public Request withContent() {
            beforeHeader();

            putAscii(HEADER_CONTENT_LENGTH);
            contentLengthHeaderReserved = ((int) Math.log10(maxBufferSize) + 2) + 4; // length + 2 x EOL
            checkCapacity(contentLengthHeaderReserved);
            ptr += contentLengthHeaderReserved;
            contentStart = ptr;
            state = STATE_CONTENT;
            return this;
        }

        private void beforeHeader() {
            assert state == STATE_QUERY || state == STATE_URL_DONE || state == STATE_HEADER;
            switch (state) {
                case STATE_QUERY:
                case STATE_URL_DONE:
                    put(" HTTP/1.1").eol();
                    putAscii("Host").putAscii(": ").put(host).put(':').put(port).putEOL();
                    state = STATE_HEADER;
                    break;
                case STATE_HEADER:
                    break;
                default:
                    eol();
                    break;
            }
        }

        private void connect(CharSequence host, int port) {
            long fd = nf.socketTcp(true);
            if (fd < 0) {
                throw new HttpClientException("could not allocate a file descriptor").errno(nf.errno());
            }
            if (nf.setTcpNoDelay(fd, true) < 0) {
                LOG.info().$("could not turn off Nagle's algorithm [fd=").$(fd)
                        .$(", errno=").$(nf.errno()).I$();
            }
            socket.of(fd);

            nf.configureKeepAlive(fd);
            long addrInfo = nf.getAddrInfo(host, port);
            if (addrInfo == -1) {
                disconnect();
                throw new HttpClientException("could not resolve host ").put("[host=").put(host).put("]");
            }

            if (nf.connectAddrInfo(fd, addrInfo) != 0) {
                int errno = nf.errno();
                nf.freeAddrInfo(addrInfo);
                disconnect();
                throw new HttpClientException("could not connect to host ").put("[host=").put(host).put(", port=").put(port).put(", errno=").put(errno).put(']');
            }
            nf.freeAddrInfo(addrInfo);

            if (nf.configureNonBlocking(fd) < 0) {
                int errno = nf.errno();
                disconnect();
                throw new HttpClientException("could not configure socket to be non-blocking [fd=").put(fd).put(", errno=").put(errno).put(']');
            }

            if (socket.supportsTls()) {
                try {
                    socket.startTlsSession(host);
                } catch (TlsSessionInitFailedException e) {
                    int errno = nf.errno();
                    disconnect();
                    throw new HttpClientException("could not start TLS session [fd=").put(fd)
                            .put(", error=").put(e.getFlyweightMessage())
                            .put(", errno=").put(errno)
                            .put(']');
                }
            }
            setupIoWait();
        }

        private void doSend(long lo, long hi, int timeoutMillis) {
            int len = (int) (hi - lo);
            if (len > 0) {
                long p = lo;
                do {
                    final int sent = sendOrDie(p, len, timeoutMillis);
                    if (sent > 0) {
                        p += sent;
                        len -= sent;
                    }
                } while (len > 0);
            }
        }

        private Request eol() {
            putEOL();
            return this;
        }

        private Request putAsciiInternal(char c) {
            Utf8Sink.super.putAscii(c);
            return this;
        }

        private Request putAsciiInternal(@Nullable CharSequence cs) {
            if (cs != null) {
                int l = cs.length();
                for (int i = 0; i < l; i++) {
                    Utf8Sink.super.putAscii(cs.charAt(i));
                }
            }
            return this;
        }

        private void putUrlEncoded(char c) {
            switch (c) {
                case ' ':
                    putAsciiInternal("%20");
                    break;
                case '!':
                    putAsciiInternal("%21");
                    break;
                case '"':
                    putAsciiInternal("%22");
                    break;
                case '#':
                    putAsciiInternal("%23");
                    break;
                case '$':
                    putAsciiInternal("%24");
                    break;
                case '%':
                    putAsciiInternal("%25");
                    break;
                case '&':
                    putAsciiInternal("%26");
                    break;
                case '\'':
                    putAsciiInternal("%27");
                    break;
                case '(':
                    putAsciiInternal("%28");
                    break;
                case ')':
                    putAsciiInternal("%29");
                    break;
                case '*':
                    putAsciiInternal("%2A");
                    break;
                case '+':
                    putAsciiInternal("%2B");
                    break;
                case ',':
                    putAsciiInternal("%2C");
                    break;
                case '-':
                    putAsciiInternal("%2D");
                    break;
                case '.':
                    putAsciiInternal("%2E");
                    break;
                case '/':
                    putAsciiInternal("%2F");
                    break;
                case ':':
                    putAsciiInternal("%3A");
                    break;
                case ';':
                    putAsciiInternal("%3B");
                    break;
                case '<':
                    putAsciiInternal("%3C");
                    break;
                case '=':
                    putAsciiInternal("%3D");
                    break;
                case '>':
                    putAsciiInternal("%3E");
                    break;
                case '?':
                    putAsciiInternal("%3F");
                    break;
                case '@':
                    putAsciiInternal("%40");
                    break;
                case '[':
                    putAsciiInternal("%5B");
                    break;
                case '\\':
                    putAsciiInternal("%5C");
                    break;
                case ']':
                    putAsciiInternal("%5D");
                    break;
                case '^':
                    putAsciiInternal("%5E");
                    break;
                case '_':
                    putAsciiInternal("%5F");
                    break;
                case '`':
                    putAsciiInternal("%60");
                    break;
                case '{':
                    putAsciiInternal("%7B");
                    break;
                case '|':
                    putAsciiInternal("%7C");
                    break;
                case '}':
                    putAsciiInternal("%7D");
                    break;
                case '\n':
                    putAsciiInternal("%0A");
                    break;
                case '\r':
                    putAsciiInternal("%0D");
                    break;
                case '\t':
                    putAsciiInternal("%09");
                    break;
                default:
                    // there are symbols to escape, but those we do not tend to use at all
                    // https://www.w3schools.com/tags/ref_urlencode.ASP
                    putAsciiInternal(c);
                    break;
            }
        }

        private void sendHeaderAndContent(int maxContentLen, int timeout) {
            final int contentLength = (int) (ptr - contentStart);

            // Add content bytes into the header.
            final long hi = ptr;
            final long headerHi;
            if (contentLengthHeaderReserved > 0) {
                ptr = contentStart - contentLengthHeaderReserved;
                put(contentLength);
                eol();
                eol();
                headerHi = ptr;
                assert headerHi < contentStart;
                ptr = hi;
            } else {
                headerHi = contentStart;
            }

            // Send header.
            doSend(bufLo, headerHi, timeout);

            // Send content.
            doSend(contentStart, contentStart + Math.min(hi - contentStart, maxContentLen), timeout);
        }
    }

    public class ResponseHeaders extends HttpHeaderParser {
        private final ChunkedResponseImpl chunkedResponse;
        private final int defaultTimeout;
        private final ResponseImpl response;

        public ResponseHeaders(long respParserBufLo, int respParserBufSize, int defaultTimeout, int headerBufSize, ObjectPool<DirectUtf8String> pool) {
            super(headerBufSize, pool);
            this.defaultTimeout = defaultTimeout;
            this.response = new ResponseImpl(respParserBufLo, respParserBufLo + respParserBufSize, defaultTimeout);
            this.chunkedResponse = new ChunkedResponseImpl(respParserBufLo, respParserBufLo + respParserBufSize, defaultTimeout);
        }

        public void await() {
            await(defaultTimeout);
        }

        public void await(int timeout) {
            int totalBytesReceived = 0;
            long unprocessedLo = responseParserBufLo;
            while (isIncomplete()) {
                final int len = recvOrDie(responseParserBufLo + totalBytesReceived, timeout);
                if (len > 0) {
                    totalBytesReceived += len;
                    unprocessedLo = parse(unprocessedLo, responseParserBufLo + totalBytesReceived, false, true);
                    if (!isIncomplete()) {
                        if (isChunked()) {
                            chunkedResponse.begin(unprocessedLo, responseParserBufLo + totalBytesReceived);
                        } else {
                            long contentLength = getContentLength();
                            if (contentLength > bufferSize) {
                                throw new HttpClientException("insufficient http client buffer size: " + contentLength);
                            }
                            response.begin(unprocessedLo, responseParserBufLo + totalBytesReceived, contentLength);
                        }
                        final Utf8Sequence statusCode = getStatusCode();
                        if (statusCode != null && Utf8s.equalsNcAscii(HTTP_NO_CONTENT, getStatusCode())) {
                            incomplete = false;
                        }
                    }
                }
            }

            if (cookieHandler != null) {
                cookieHandler.processCookies(this);
            }
        }

        @Override
        public void clear() {
            super.clear();
            csPool.clear();
        }

        // Note: we don't free parser memory here.
        // Instead, the client does it when it's closed by calling free() method.
        @Override
        public void close() {
            disconnect();
            clear();
        }

        public Response getResponse() {
            if (isChunked()) {
                return chunkedResponse;
            }
            return response;
        }

        public boolean isChunked() {
            if (isIncomplete()) {
                throw new HttpClientException("http response headers not yet received");
            }
            return HttpKeywords.isChunked(getHeader(HEADER_TRANSFER_ENCODING));
        }

        private void free() {
            super.close();
        }
    }

    private class ResponseImpl extends AbstractResponse {
        public ResponseImpl(long bufLo, long bufHi, int defaultTimeout) {
            super(bufLo, bufHi, defaultTimeout);
        }

        @Override
        protected int recvOrDie(long bufLo, long bufHi, int timeout) {
            return HttpClient.this.recvOrDie(bufLo, timeout);
        }
    }
}
