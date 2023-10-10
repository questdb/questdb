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
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.datetime.microtime.MicrosecondClockImpl;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectByteCharSequence;
import io.questdb.std.str.StringSink;
import org.jetbrains.annotations.Nullable;

public abstract class HttpClient implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(HttpClient.class);
    protected final NetworkFacade nf;
    protected final Socket socket;
    private final int bufferSize;
    private final ObjectPool<DirectByteCharSequence> csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
    private final int defaultTimeout;
    private final Request request = new Request();
    private final Rnd rnd;
    private long bufHi;
    private long bufLo;
    private long ptr = bufLo;
    private ResponseHeaders responseHeaders;

    public HttpClient(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        this.nf = configuration.getNetworkFacade();
        this.socket = socketFactory.newInstance(configuration.getNetworkFacade(), LOG);
        this.defaultTimeout = configuration.getTimeout();
        this.bufferSize = configuration.getBufferSize();
        this.bufLo = Unsafe.malloc(bufferSize, MemoryTag.NATIVE_DEFAULT);
        this.bufHi = bufLo + bufferSize;
        this.responseHeaders = new ResponseHeaders(bufLo, bufferSize, defaultTimeout, 4096, csPool);
        // random is used to generate multipart boundary
        this.rnd = new Rnd(NanosecondClockImpl.INSTANCE.getTicks(), MicrosecondClockImpl.INSTANCE.getTicks());
    }

    @Override
    public void close() {
        disconnect();
        if (bufLo != 0) {
            Unsafe.free(bufLo, bufferSize, MemoryTag.NATIVE_DEFAULT);
            bufLo = 0;
            bufHi = 0;
        }
        responseHeaders = Misc.free(responseHeaders);
    }

    public void disconnect() {
        socket.close();
    }

    public Request newRequest() {
        ptr = bufLo;
        // todo: init() ?
        request.state = Request.STATE_REQUEST;
        request.boundary = 0;
        return request;
    }

    private int die(int byteCount) {
        if (byteCount < 1) {
            throw new HttpClientException("peer disconnect [errno=").errno(nf.errno()).put(']');
        }
        return byteCount;
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

    public interface FormData extends CharSink {
    }

    public interface MultipartRequest {
        FormData formData(CharSequence name, @Nullable CharSequence fileName, @Nullable CharSequence contentType);

        default FormData formData(CharSequence fieldName, @Nullable CharSequence fileName) {
            return formData(fieldName, fileName, null);
        }

        default FormData formData(CharSequence fieldName) {
            return formData(fieldName, null);
        }

        Response send();
    }

    public interface Response {
        void await();

        void await(int timeout);

        ChunkedResponse getChunkedResponse();

        boolean isChunked();
    }

    private static class BinarySequenceAdapter implements BinarySequence, Mutable {
        private final StringSink asciiSink = new StringSink();

        @Override
        public byte byteAt(long index) {
            return (byte) asciiSink.charAt((int) index);
        }

        @Override
        public void clear() {
            asciiSink.clear();
        }

        @Override
        public long length() {
            return asciiSink.length();
        }

        BinarySequenceAdapter colon() {
            asciiSink.put(':');
            return this;
        }

        BinarySequenceAdapter put(CharSequence value) {
            asciiSink.put(value);
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

    private class FormDataImpl extends AbstractCharSink implements FormData {
        @Override
        public CharSink put(char c) {
            return request.put(c);
        }

        @Override
        public CharSink put(CharSequence cs) {
            return request.put(cs);
        }
    }

    private class MultipartRequestImpl implements MultipartRequest {
        private final FormData formData = new FormDataImpl();

        @Override
        public FormData formData(CharSequence fieldName, @Nullable CharSequence fileName, @Nullable CharSequence contentType) {
            request.eol();
            request.put("--").put(Request.BOUNDARY_PREFIX).put(request.boundary);
            request.eol();
            request.put("Content-Disposition: form-data; name=\"").put(fieldName).put('\"');
            if (fileName != null) {
                request.put("; filename=\"").put(fileName).put('\"');
            }
            request.eol();
            if (contentType != null) {
                request.put("Content-Type: ").put(contentType);
                request.eol();
            }

            request.eol();
            return formData;
        }

        @Override
        public Response send() {
            request.eol();
            request.put("--").put(Request.BOUNDARY_PREFIX).put(request.boundary).put("--");
            request.eol();
            return request.send();
        }
    }

    public class Request extends AbstractCharSink {
        private static final String BOUNDARY_PREFIX = "---------------------------";
        private static final int STATE_HEADER = 4;
        private static final int STATE_QUERY = 3;
        private static final int STATE_REQUEST = 0;
        private static final int STATE_URL = 1;
        private static final int STATE_URL_DONE = 2;
        private final MultipartRequestImpl multipartRequest = new MultipartRequestImpl();
        private BinarySequenceAdapter binarySequenceAdapter;
        private long boundary = 0;
        private int requestTimeout;
        private int state;
        private boolean urlEncode = false;

        public Request GET(CharSequence host, int port) {
            return GET(host, port, defaultTimeout);
        }

        public Request GET(CharSequence host, int port, int requestTimeout) {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            connect(host, port);
            this.requestTimeout = requestTimeout;
            return put("GET ");
        }

        public Request POST(CharSequence host, int port) {
            return POST(host, port, defaultTimeout);
        }

        public Request POST(CharSequence host, int port, int requestTimeout) {
            assert state == STATE_REQUEST;
            state = STATE_URL;
            connect(host, port);
            this.requestTimeout = requestTimeout;
            return put("POST ");
        }

        public Request authBasic(CharSequence username, CharSequence password) {
            beforeHeader();
            put("Authorization").put(": ").put("Basic ");
            if (binarySequenceAdapter == null) {
                binarySequenceAdapter = new BinarySequenceAdapter();
            }
            binarySequenceAdapter.clear();
            binarySequenceAdapter.put(username).colon().put(password);
            Chars.base64Encode(binarySequenceAdapter, (int) binarySequenceAdapter.length(), this);
            return eol();
        }

        public Request header(CharSequence name, CharSequence value) {
            beforeHeader();
            encodeUtf8(name).put(": ").encodeUtf8(value);
            return eol();
        }

        public MultipartRequest multipart() {
            beforeHeader();
            this.boundary = rnd.nextPositiveLong();
            put("Content-Type").put(": ").put("multipart/form-data; boundary=").put(BOUNDARY_PREFIX).put(boundary);
            eol();
            return multipartRequest;
        }

        @Override
        public Request put(CharSequence str) {
            int len = str.length();
            if (ptr + len >= bufHi) {
                doSend();
            }
            Chars.asciiStrCpy(str, len, ptr);
            ptr += len;
            return this;
        }

        @Override
        public CharSink put(char c) {
            if (ptr + 1 >= bufHi) {
                doSend();
            }
            Unsafe.getUnsafe().putByte(ptr, (byte) c);
            ptr++;
            return this;
        }

        @Override
        public void putUtf8Special(char c) {
            if (urlEncode) {
                putUrlEncoded(c);
            } else {
                put(c);
            }
        }

        public Request query(CharSequence name, CharSequence value) {
            assert state == STATE_URL_DONE || state == STATE_QUERY;
            if (state == STATE_URL_DONE) {
                put('?');
            } else {
                put('&');
            }
            state = STATE_QUERY;
            urlEncode = true;
            try {
                encodeUtf8(name).put('=');
                if (value != null) {
                    encodeUtf8(value);
                }
            } finally {
                urlEncode = false;
            }
            return this;
        }

        public Response send() {
            assert state == STATE_URL_DONE || state == STATE_QUERY || state == STATE_HEADER;

            if (state == STATE_URL_DONE || state == STATE_QUERY) {
                put(" HTTP/1.1").eol();
            }

            eol();
            doSend();
            responseHeaders.clear();
            return responseHeaders;
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
            if (socket.isClosed()) {
                int fd = nf.socketTcp(true);
                if (fd < 0) {
                    throw new HttpClientException("could not allocate a file descriptor").errno(nf.errno());
                }
                nf.configureKeepAlive(fd);long addrInfo = nf.getAddrInfo(host, port);
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
        }

        private void doSend() {
//            System.out.println("send()");
//            Net.dumpAscii(bufLo, (int) (ptr - bufLo));
            int len = (int) (ptr - bufLo);
            if (len > 0) {
                long p = bufLo;
                do {
                    final int sent = sendOrDie(p, len, requestTimeout);
                    if (sent > 0) {
                        p += sent;
                        len -= sent;
                    }
                } while (len > 0);
                ptr = bufLo;
            }
        }

        private Request eol() {
            return put(Misc.EOL);
        }

        private void putUrlEncoded(char c) {
            switch (c) {
                case ' ':
                    put("%20");
                    break;
                case '!':
                    put("%21");
                    break;
                case '"':
                    put("%22");
                    break;
                case '#':
                    put("%23");
                    break;
                case '$':
                    put("%24");
                    break;
                case '%':
                    put("%25");
                    break;
                case '&':
                    put("%26");
                    break;
                case '\'':
                    put("%27");
                    break;
                case '(':
                    put("%28");
                    break;
                case ')':
                    put("%29");
                    break;
                case '*':
                    put("%2A");
                    break;
                case '+':
                    put("%2B");
                    break;
                case ',':
                    put("%2C");
                    break;
                case '-':
                    put("%2D");
                    break;
                case '.':
                    put("%2E");
                    break;
                case '/':
                    put("%2F");
                    break;
                case ':':
                    put("%3A");
                    break;
                case ';':
                    put("%3B");
                    break;
                case '<':
                    put("%3C");
                    break;
                case '=':
                    put("%3D");
                    break;
                case '>':
                    put("%3E");
                    break;
                case '?':
                    put("%3F");
                    break;
                case '@':
                    put("%40");
                    break;
                case '[':
                    put("%5B");
                    break;
                case '\\':
                    put("%5C");
                    break;
                case ']':
                    put("%5D");
                    break;
                case '^':
                    put("%5E");
                    break;
                case '_':
                    put("%5F");
                    break;
                case '`':
                    put("%60");
                    break;

                case '{':
                    put("%7B");
                    break;
                case '|':
                    put("%7C");
                    break;
                case '}':
                    put("%7D");
                    break;
                default:
                    // there are symbols to escape, but those we do not tend to use at all
                    // https://www.w3schools.com/tags/ref_urlencode.ASP
                    put(c);
                    break;
            }
        }
    }

    public class ResponseHeaders extends HttpHeaderParser implements Response {
        private final long bufLo;
        private final ChunkedResponseImpl chunkedResponse;
        private final int defaultTimeout;

        public ResponseHeaders(long respParserBufLo, int respParserBufSize, int defaultTimeout, int headerBufSize, ObjectPool<DirectByteCharSequence> pool) {
            super(headerBufSize, pool);
            this.bufLo = respParserBufLo;
            this.defaultTimeout = defaultTimeout;
            this.chunkedResponse = new ChunkedResponseImpl(respParserBufLo, respParserBufLo + respParserBufSize, defaultTimeout);
        }

        @Override
        public void await() {
            await(defaultTimeout);
        }

        @Override
        public void await(int timeout) {
            while (isIncomplete()) {
                final int len = recvOrDie(bufLo, timeout);
                if (len > 0) {
                    // dataLo & dataHi are boundaries of unprocessed data left in the buffer
                    chunkedResponse.begin(parse(bufLo, bufLo + len, false, true), bufLo + len);
                }
            }
        }

        @Override
        public void clear() {
            csPool.clear();
            super.clear();
        }

        @Override
        public ChunkedResponse getChunkedResponse() {
            return chunkedResponse;
        }

        @Override
        public boolean isChunked() {
            if (isIncomplete()) {
                throw new HttpClientException("http response headers not yet received");
            }
            return Chars.equalsNc("chunked", getHeader("Transfer-Encoding"));
        }
    }
}
