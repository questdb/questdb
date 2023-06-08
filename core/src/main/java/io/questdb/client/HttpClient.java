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

package io.questdb.client;

import io.questdb.DefaultHttpClientConfiguration;
import io.questdb.HttpClientConfiguration;
import io.questdb.cutlass.http.HttpHeaderParser;
import io.questdb.cutlass.http.HttpRequestHeader;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Epoll;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.NetworkFacade;
import io.questdb.std.*;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HttpClient implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(HttpClient.class);
    private final int bufferSize;
    private final int defaultTimeout;
    private final NetworkFacade nf;
    private final Request request = new Request();
    private long bufLo;
    private long dataHi;
    private long dataLo;
    private Epoll epoll = new Epoll(EpollFacadeImpl.INSTANCE, 128);
    private int fd = -1;
    private long ptr = bufLo;
    private Response response = new Response();

    public HttpClient() {
        this(DefaultHttpClientConfiguration.INSTANCE);
    }

    public HttpClient(HttpClientConfiguration configuration) {
        this.nf = configuration.getNetworkFacade();
        this.defaultTimeout = configuration.getTimeout();
        this.bufferSize = configuration.getBufferSize();
        this.bufLo = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
    }

    public static void main(String[] args) {

        HttpClient client = new HttpClient();

        Request req = client.newRequest();

        Response rsp = req
                .GET()
                .url("/exec")
//                .query("query", "cpu%20limit%20400000")
                .query("query", "cpu")
                .header("Accept", "gzip, deflate, br")
                .send("localhost", 9000);

        rsp.awaitHeaders();

        if (rsp.isChunked()) {
            Response.Chunk chunk;

            long t = System.currentTimeMillis();
            while ((chunk = rsp.recv()) != null) {
                System.out.println("addr: " + chunk.addr + ", size: " + chunk.size + ", consumed: " + chunk.consumed + ", available: " + chunk.available);
            }
            System.out.println(System.currentTimeMillis() - t);
        }
    }

    @Override
    public void close() {
        if (bufLo != 0) {
            Unsafe.free(bufLo, bufferSize, MemoryTag.NATIVE_DEFAULT);
            bufLo = 0;
        }

        if (fd != -1) {
            nf.close(fd);
            fd = -1;
        }

        epoll = Misc.free(epoll);
        response = Misc.free(response);
    }

    public Request newRequest() {
        ptr = bufLo;
        request.state = Request.STATE_REQUEST;
        return request;
    }

    private void compactBuffer() {
        // move unprocessed data to the front of the buffer
        // to maximise
        if (dataLo > bufLo) {
            final long len = dataHi - dataLo;
            assert len > -1;
            if (len > 0) {
                Vect.memmove(bufLo, dataLo, len);
            }
            dataLo = bufLo;
            dataHi = bufLo + len;
        }
    }

    private void setupEpoll() {
        if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLOUT) < 0) {
            throw new HttpClientException("internal error: epoll_ctl failure [cmd=add, errno=").put(nf.errno()).put(']');
        }
    }

    private void epollMod(int event) {
        if (epoll.control(fd, 0, EpollAccessor.EPOLL_CTL_MOD, event) < 0) {
            throw new HttpClientException("internal error: epoll_ctl failure [cmd=mod, errno=").put(nf.errno()).put(']');
        }
    }

    private void poll(int timeout) {
        if (epoll.poll(timeout) != 1) {
            throw new HttpClientException("timed out").errno(nf.errno());
        }
    }

    public class Request extends AbstractCharSink {
        private static final int STATE_HEADER = 4;
        private static final int STATE_QUERY = 3;
        private static final int STATE_REQUEST = 0;
        private static final int STATE_URL = 1;
        private static final int STATE_URL_DONE = 2;
        private int state;

        public Request GET() {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            return put("GET ");
        }

        public Request header(CharSequence name, CharSequence value) {
            assert state == STATE_QUERY || state == STATE_URL_DONE || state == STATE_HEADER;

            if (state == STATE_QUERY || state == STATE_URL_DONE) {
                put(" HTTP/1.1").crlf();
                state = STATE_HEADER;
            } else {
                crlf();
            }
            return put(name).put(": ").put(value).crlf();
        }

        @Override
        public Request put(CharSequence str) {
            int len = str.length();
            Chars.asciiStrCpy(str, len, ptr);
            ptr += len;
            return this;
        }

        @Override
        public CharSink put(char c) {
            Unsafe.getUnsafe().putByte(ptr, (byte) c);
            ptr++;
            return this;
        }

        public Request query(CharSequence name, CharSequence value) {
            assert state == STATE_URL_DONE || state == STATE_QUERY;
            if (state == STATE_URL_DONE) {
                put('?');
            } else {
                put('&');
            }
            state = STATE_QUERY;
            put(name).put('=').put(value);
            return this;
        }

        public Response send(CharSequence host, int port) {
            return send(host, port, defaultTimeout);
        }

        public Response send(CharSequence host, int port, int timeout) {
            assert state == STATE_URL_DONE || state == STATE_QUERY || state == STATE_HEADER;
            if (fd == -1) {
                connect(host, port);
            }


            if (state == STATE_URL_DONE || state == STATE_QUERY) {
                put(" HTTP/1.1").crlf();
            }

            crlf();
            doSend(timeout);
            response.init();
            return response;
        }

        public Request url(CharSequence url) {
            assert state == STATE_URL;
            state = STATE_URL_DONE;
            return put(url);
        }

        private void connect(CharSequence host, int port) {
            fd = nf.socketTcp(true);
            if (fd < 0) {
                throw new HttpClientException("could not allocate a file descriptor").errno(nf.errno());
            }
            long addrInfo = nf.getAddrInfo(host, port);
            if (addrInfo == -1) {
                nf.close(fd, LOG);
                throw new HttpClientException("could not resolve host ")
                        .put("[host=").put(host).put("]");
            }
            if (nf.connectAddrInfo(fd, addrInfo) != 0) {
                int errno = nf.errno();
                nf.close(fd, LOG);
                nf.freeAddrInfo(addrInfo);
                throw new HttpClientException("could not connect to host ")
                        .put("[host=").put(host)
                        .put(", port=").put(port)
                        .put(", errno=").put(errno)
                        .put(']');
            }
            nf.freeAddrInfo(addrInfo);
        }

        private Request crlf() {
            String CRLF = "\r\n";
            return put(CRLF);
        }

        private void doSend(int timeout) {

            setupEpoll();

            int len = (int) (ptr - bufLo);
            if (len > 0) {
                long p = bufLo;
                while (len > 0) {
                    // todo: re-arm epoll
                    final int sent = nf.send(fd, p, len);
                    if (sent < 0) {
                        throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
                    }
                    if (sent > 0) {
                        p += sent;
                        len -= sent;
                    }

                    if (len > 0) {
                        epollMod(EpollAccessor.EPOLLOUT);
                        poll(timeout);
                    }
                }
            }
        }
    }

    public class Response implements QuietCloseable {
        private final Chunk chunk = new Chunk();
        private final DirectByteCharSequence chunkSize = new DirectByteCharSequence();
        private final ObjectPool<DirectByteCharSequence> csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
        private HttpHeaderParser headerParser = new HttpHeaderParser(4096, csPool);

        public void awaitHeaders() {
            awaitHeaders(defaultTimeout);
        }

        public void awaitHeaders(int timeout) {
            // prepare chunk sink ready for reuse
            //
            chunk.clear();
            while (headerParser.isIncomplete()) {
                // read response header; if we manage to read header
                // fully in one shot, the outer while loop exists;
                // otherwise loop re
                poll(timeout);

                final int len = nf.recv(fd, bufLo, bufferSize);
                if (len < 0) {
                    throw new HttpClientException("peer disconnect").errno(nf.errno());
                }
                // dataLo & dataHi are boundaries of unprocessed data left in the buffer
                dataLo = headerParser.parse(bufLo, bufLo + len, false, true);
                dataHi = bufLo + len;

                if (headerParser.isIncomplete()) {
                    epollMod(EpollAccessor.EPOLLOUT);
                }
            }
        }

        @Override
        public void close() {
            headerParser = Misc.free(headerParser);
        }

        public HttpRequestHeader header() {
            return headerParser;
        }

        public boolean isChunked() {
            if (headerParser.isIncomplete()) {
                throw new HttpClientException("http response headers not yet received");
            }
            return Chars.equalsNc("chunked", header().getHeader("Transfer-Encoding"));
        }

        public Chunk recv() {
            return recv(defaultTimeout);
        }

        public Chunk recv(int timeout) {
            if (chunk.endOfChunk) {
                return chunkStart(timeout);
            }
            return chunkContinue(timeout);
        }

        @NotNull
        private HttpClient.Response.Chunk chunkContinue(int timeout) {
            chunk.consumed += chunk.available;
            compactBuffer();
            while (true) {
                epollMod(EpollAccessor.EPOLLIN);
                poll(timeout);
                int len = recvOrDie();
                if (len > 0) {
                    // we are consuming the remaining chunk bytes;
                    // chunk size includes `\r\n\`, which must not be included in
                    // "available" bytes of the last chunk

                    // configure chunk boundaries
                    boolean endOfChunk = chunk.size - chunk.consumed <= len;
                    chunk.endOfChunk = endOfChunk;
                    chunk.addr = dataHi;
                    chunk.available = endOfChunk ? chunk.size - chunk.consumed - 2 : len;

                    if (endOfChunk) {
                        dataHi += len;
                        dataLo = chunk.addr + chunk.available + 2;
                    } else {
                        // we just consumed the entire buffer
                        dataLo = bufLo;
                        dataHi = bufLo;
                    }
                    return chunk;
                }
            }
        }

        @Nullable
        private HttpClient.Response.Chunk chunkStart(int timeout) {
            if (parseChunkSize0(dataLo, dataHi)) {
                return chunk.size > 2 ? chunk : null;
            }

            compactBuffer();
            // re-arm epoll to read more from the server
            while (true) {
                epollMod(EpollAccessor.EPOLLIN);
                poll(timeout);
                int len = recvOrDie();
                if (len > 0) {
                    dataHi += len;
                    // try to read chunk prefix
                    if (parseChunkSize0(dataLo, dataHi)) {
                        return chunk.size > 0 ? chunk : null;
                    }
                }
            }
        }

        private void init() {
            csPool.clear();
            headerParser.clear();
        }

        private boolean parseChunkSize0(long lo, long hi) {
            long p = lo;
            long x = -1;
            long len;
            while (p < hi) {
                char b = (char) Unsafe.getUnsafe().getByte(p++);
                switch (b) {
                    case '\r':
                        x = p;
                        break;
                    case '\n':
                        if (x == -1) {
                            throw new HttpClientException("malformed chunk");
                        }
                        // parse the hex chunk length
                        // last char, at(x) is `\r`, exclude
                        chunkSize.of(lo, x - 1);
                        try {
                            len = Numbers.parseHexLong(chunkSize);
                            chunk.addr = p;
                            // the chunk length does NOT include trailing chars `\r\n`
                            chunk.size = len + 2;
                            chunk.available = Math.min(len, hi - p);
                            chunk.endOfChunk = len == chunk.available;
                            chunk.consumed = 0;

                            assert chunk.available > 0;

                            // if chunk size is smaller that the unprocessed data size
                            // we will reduce unprocessed data size by chunk size; otherwise
                            // we clear the unprocessed data
                            long chunkHi = chunk.addr + chunk.available;
                            if (chunkHi < dataHi) {
                                dataLo = chunkHi;
                            } else {
                                dataLo = bufLo;
                                dataHi = bufLo;
                            }

                            return true;
                        } catch (NumericException e) {
                            throw new HttpClientException("could not parse chunk size");
                        }
                    default:
                        break;
                }
            }
            return false;
        }

        private int recvOrDie() {
            final int len = nf.recv(fd, dataHi, (int) (bufferSize - (dataHi - bufLo)));
            if (len < 0) {
                throw new HttpClientException("peer disconnect [errno=").put(nf.errno()).put(']');
            }
            return len;
        }

        public class Chunk implements Mutable {
            long addr;
            long available;
            long consumed = 0;
            boolean endOfChunk;
            long size;

            @Override
            public void clear() {
                endOfChunk = true;
            }
        }
    }
}
