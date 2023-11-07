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

package io.questdb.cutlass.http.client;

import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.cutlass.http.ex.BufferOverflowException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.IOOperation;
import io.questdb.network.NetworkFacade;
import io.questdb.network.Socket;
import io.questdb.network.SocketFactory;
import io.questdb.std.*;
import io.questdb.std.str.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.questdb.cutlass.http.HttpConstants.*;

public abstract class HttpClient implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(HttpClient.class);
    protected final NetworkFacade nf;
    protected final Socket socket;
    private final int bufferSize;
    private final HttpClientCookieHandler cookieHandler;
    private final ObjectPool<DirectUtf8String> csPool = new ObjectPool<>(DirectUtf8String.FACTORY, 64);
    private final int defaultTimeout;
    private final Request request = new Request();
    private long bufLo;
    private long ptr = bufLo;
    private ResponseHeaders responseHeaders;

    public HttpClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        this.nf = configuration.getNetworkFacade();
        this.socket = socketFactory.newInstance(configuration.getNetworkFacade(), LOG);
        this.defaultTimeout = configuration.getTimeout();
        this.cookieHandler = configuration.getCookieHandlerFactory().getInstance();
        this.bufferSize = configuration.getBufferSize();
        this.bufLo = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        this.responseHeaders = new ResponseHeaders(bufLo, bufferSize, defaultTimeout, 4096, csPool);
    }

    @Override
    public void close() {
        disconnect();
        if (bufLo != 0) {
            Unsafe.free(bufLo, bufferSize, MemoryTag.NATIVE_DEFAULT);
            bufLo = 0;
        }
        responseHeaders = Misc.free(responseHeaders);
    }

    public void disconnect() {
        socket.close();
    }

    public Request newRequest() {
        ptr = bufLo;
        request.state = Request.STATE_REQUEST;
        return request;
    }

    private int die(int byteCount) {
        if (byteCount < 1) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
    }

    private void ensureCapacity(long capacity) {
        final long requiredSize = ptr - bufLo + capacity;
        if (requiredSize > bufferSize) {
            throw BufferOverflowException.INSTANCE;
        }
    }

    private int recvOrDie(long addr, int timeout) {
        return recvOrDie(addr, (int) (bufferSize - (addr - bufLo)), timeout);
    }

    private int recvOrDie(long lo, int len, int timeout) {
        ioWait(timeout, IOOperation.READ);
        return die(socket.recv(lo, len));
    }

    private int sendOrDie(long lo, int len, int timeout) {
        ioWait(timeout, IOOperation.WRITE);
        return die(socket.send(lo, len));
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

    public class Request implements Utf8Sink {
        private static final int STATE_HEADER = 4;
        private static final int STATE_QUERY = 3;
        private static final int STATE_REQUEST = 0;
        private static final int STATE_URL = 1;
        private static final int STATE_URL_DONE = 2;
        private BinarySequenceAdapter binarySequenceAdapter;
        private int state;
        private boolean urlEncode = false;

        public Request GET() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return put("GET ");
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

        public Request header(CharSequence name, CharSequence value) {
            beforeHeader();
            put(name).putAsciiInternal(": ").put(value);
            return eol();
        }

        @Override
        public Request put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                ensureCapacity(size);
                Utf8s.strCpy(us, size, ptr);
                ptr += size;
            }
            return this;
        }

        @Override
        public Request put(byte b) {
            ensureCapacity(1);
            Unsafe.getUnsafe().putByte(ptr, b);
            ptr++;
            return this;
        }

        @Override
        public Request put(long lo, long hi) {
            final long size = hi - lo;
            ensureCapacity(size);
            Vect.memcpy(ptr, lo, size);
            ptr += size;
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

        public ResponseHeaders send(CharSequence host, int port) {
            return send(host, port, defaultTimeout);
        }

        public ResponseHeaders send(CharSequence host, int port, int timeout) {
            assert state == STATE_URL_DONE || state == STATE_QUERY || state == STATE_HEADER;
            if (socket.isClosed()) {
                connect(host, port);
            }

            if (state == STATE_URL_DONE || state == STATE_QUERY) {
                putAscii(" HTTP/1.1").putEOL();
            }

            eol();
            doSend(timeout);
            responseHeaders.clear();
            return responseHeaders;
        }

        public void setCookie(CharSequence name, CharSequence value) {
            beforeHeader();
            put(HEADER_COOKIE).putAscii(": ").put(name).putAscii(COOKIE_VALUE_SEPARATOR).put(value);
            eol();
        }

        public Request url(CharSequence url) {
            assert state == STATE_URL;
            state = STATE_URL_DONE;
            return put(url);
        }

        private void beforeHeader() {
            assert state == STATE_QUERY || state == STATE_URL_DONE || state == STATE_HEADER;
            switch (state) {
                case STATE_QUERY:
                case STATE_URL_DONE:
                    put(" HTTP/1.1").eol();
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
            int fd = nf.socketTcp(true);
            if (fd < 0) {
                throw new HttpClientException("could not allocate a file descriptor").errno(nf.errno());
            }
            nf.configureKeepAlive(fd);
            long addrInfo = nf.getAddrInfo(host, port);
            if (addrInfo == -1) {
                disconnect();
                throw new HttpClientException("could not resolve host ").put("[host=").put(host).put("]");
            }
            if (nf.connectAddrInfo(fd, addrInfo) != 0) {
                int errno = nf.errno();
                disconnect();
                nf.freeAddrInfo(addrInfo);
                throw new HttpClientException("could not connect to host ").put("[host=").put(host).put(", port=").put(port).put(", errno=").put(errno).put(']');
            }
            nf.freeAddrInfo(addrInfo);
            socket.of(fd);
            setupIoWait();
        }

        private void doSend(int timeout) {
            int len = (int) (ptr - bufLo);
            if (len > 0) {
                long p = bufLo;
                do {
                    final int sent = sendOrDie(p, len, timeout);
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
                default:
                    // there are symbols to escape, but those we do not tend to use at all
                    // https://www.w3schools.com/tags/ref_urlencode.ASP
                    putAsciiInternal(c);
                    break;
            }
        }
    }

    public class ResponseHeaders extends HttpHeaderParser {
        private final long bufLo;
        private final ChunkedResponseImpl chunkedResponse;
        private final int defaultTimeout;

        public ResponseHeaders(long respParserBufLo, int respParserBufSize, int defaultTimeout, int headerBufSize, ObjectPool<DirectUtf8String> pool) {
            super(headerBufSize, pool);
            this.bufLo = respParserBufLo;
            this.defaultTimeout = defaultTimeout;
            this.chunkedResponse = new ChunkedResponseImpl(respParserBufLo, respParserBufLo + respParserBufSize, defaultTimeout);
        }

        public void await() {
            await(defaultTimeout);
        }

        public void await(int timeout) {
            while (isIncomplete()) {
                final int len = recvOrDie(bufLo, timeout);
                if (len > 0) {
                    // dataLo & dataHi are boundaries of unprocessed data left in the buffer
                    chunkedResponse.begin(parse(bufLo, bufLo + len, false, true), bufLo + len);
                }
            }

            if (cookieHandler != null) {
                cookieHandler.processCookies(this);
            }
        }

        @Override
        public void clear() {
            csPool.clear();
            super.clear();
        }

        public ChunkedResponse getChunkedResponse() {
            return chunkedResponse;
        }

        public boolean isChunked() {
            if (isIncomplete()) {
                throw new HttpClientException("http response headers not yet received");
            }
            return Utf8s.equalsNcAscii("chunked", getHeader(HEADER_TRANSFER_ENCODING));
        }
    }
}
