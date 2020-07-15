/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.ex.ZLibException;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.DirectUnboundedByteSink;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.time.DateFormatUtils;
import io.questdb.std.time.MillisecondClock;

import java.io.Closeable;

public class HttpResponseSink implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(HttpResponseSink.class);

    private static final int CHUNK_HEAD = 1;
    private static final int CHUNK_DATA = 2;
    private static final int FIN = 3;
    private static final int MULTI_CHUNK = 4;
    private static final int DEFLATE = 5;
    private static final int MULTI_BUF_CHUNK = 6;
    private static final int END_CHUNK = 7;
    private static final int DONE = 8;
    private static final int FLUSH = 9;
    private static final int SEND_DEFLATED_CONT = 10;
    private static final int SEND_DEFLATED_END = 11;
    private static final IntObjHashMap<String> httpStatusMap = new IntObjHashMap<>();

    static {
        httpStatusMap.put(200, "OK");
        httpStatusMap.put(206, "Partial content");
        httpStatusMap.put(304, "Not Modified");
        httpStatusMap.put(400, "Bad request");
        httpStatusMap.put(404, "Not Found");
        httpStatusMap.put(416, "Request range not satisfiable");
        httpStatusMap.put(431, "Headers too large");
        httpStatusMap.put(500, "Internal server error");
    }

    private final long out;
    private final long outPtr;
    private final long limit;
    private final long chunkHeaderBuf;
    private final DirectUnboundedByteSink chunkSink;
    private final HttpResponseHeaderImpl headerImpl;
    private final SimpleResponseImpl simple = new SimpleResponseImpl();
    private final ResponseSinkImpl sink = new ResponseSinkImpl();
    private final ChunkedResponseImpl chunkedResponse = new ChunkedResponseImpl();
    private final HttpRawSocketImpl rawSocket = new HttpRawSocketImpl();
    private final NetworkFacade nf;
    private final int responseBufferSize;
    private long fd;
    private long _wPtr;
    private long flushBuf;
    private int flushBufSize;
    private int state;
    private long z_streamp = 0;
    private boolean deflateBeforeSend = false;
    private long pzout = 0;
    private int zpos;
    private int zlimit;
    private int crc = 0;
    private long total = 0;
    private boolean header = true;
    private final boolean dumpNetworkTraffic;
    private long totalBytesSent = 0;

    public HttpResponseSink(HttpServerConfiguration configuration) {
        this.responseBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.nf = configuration.getDispatcherConfiguration().getNetworkFacade();
        this.out = Unsafe.calloc(responseBufferSize);
        this.headerImpl = new HttpResponseHeaderImpl(configuration.getResponseHeaderBufferSize(), configuration.getClock());
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeaderBuf = Unsafe.calloc(8 + 2L * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedByteSink(chunkHeaderBuf);
        this.chunkSink.put(Misc.EOL);
        this.outPtr = this._wPtr = out;
        this.limit = outPtr + responseBufferSize;
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
    }

    public HttpChunkedResponseSocket getChunkedSocket() {
        return chunkedResponse;
    }

    @Override
    public void clear() {
        Unsafe.getUnsafe().setMemory(out, responseBufferSize, (byte) 0);
        headerImpl.clear();
        this._wPtr = outPtr;
        this.zpos = this.zlimit = 0;
        header = true;
        totalBytesSent = 0;
        resetZip();
    }

    @Override
    public void close() {
        Unsafe.free(out, responseBufferSize);
        Unsafe.free(chunkHeaderBuf, 8 + 2L * Misc.EOL.length());
        headerImpl.close();
        if (pzout != 0) {
            Unsafe.free(pzout, responseBufferSize);
        }
        if (z_streamp != 0) {
            Zip.deflateEnd(z_streamp);
            z_streamp = 0;
        }
    }

    public void setDeflateBeforeSend(boolean deflateBeforeSend) {
        this.deflateBeforeSend = deflateBeforeSend;
    }

    public int getCode() {
        return headerImpl.getCode();
    }

    public SimpleResponseImpl getSimple() {
        return simple;
    }

    public void resumeSend() throws PeerDisconnectedException, PeerIsSlowToReadException {
        while (true) {

            if (flushBufSize > 0) {
                send();
            }

            switch (state) {
                case MULTI_CHUNK:
                    if (deflateBeforeSend) {
                        prepareCompressedBody();
                        state = DEFLATE;
                    } else {
                        prepareBody();
                        state = DONE;
                    }
                    break;
                case DEFLATE:
                    state = deflate(false);
                    break;
                case SEND_DEFLATED_END:
                    flushBuf = pzout + zpos;
                    flushBufSize = zlimit - zpos;
                    state = END_CHUNK;
                    break;
                case SEND_DEFLATED_CONT:
                    flushBuf = pzout + zpos;
                    flushBufSize = zlimit - zpos;
                    state = DONE;
                    break;
                case MULTI_BUF_CHUNK:
                    flushBuf = out;
                    state = DONE;
                    break;
                case CHUNK_HEAD:
                    prepareChunk((int) (_wPtr - outPtr));
                    state = CHUNK_DATA;
                    break;
                case CHUNK_DATA:
                    prepareBody();
                    state = END_CHUNK;
                    break;
                case END_CHUNK:
                    prepareChunk(0);
                    state = FIN;
                    break;
                case FIN:
                    sink.put(Misc.EOL);
                    prepareBody();
                    state = DONE;
                    break;
                case FLUSH:
                    state = deflate(true);
                    break;
                case DONE:
                    return;
                default:
                    break;
            }
        }
    }

    private int deflate(boolean flush) {

        final int sz = this.responseBufferSize - 8;
        long p = pzout + Zip.gzipHeaderLen;

        int ret;
        int len;
        int availIn;
        // compress input until we run out of either input or output
        do {
            ret = Zip.deflate(z_streamp, p, sz, flush);
            if (ret < 0) {
                throw HttpException.instance("could not deflate [ret=").put(ret);
            }

            len = sz - Zip.availOut(z_streamp);
            availIn = Zip.availIn(z_streamp);
        } while (len == 0 && availIn > 0);

        // zip did not write anything out, nothing to send
        // we assume that input was too small and needs flushing

        if (len == 0) {
            return DONE;
        }

        // this is ZLib error, can't continue
        if (len < 0) {
            throw ZLibException.INSTANCE;
        }

        // augment zout with header and trailer and prepare for flush
        // header
        if (header) {
            Unsafe.getUnsafe().copyMemory(Zip.gzipHeader, pzout, Zip.gzipHeaderLen);
            header = false;
            zpos = 0;
        } else {
            zpos = Zip.gzipHeaderLen;
        }

        // trailer
        if (flush && ret == 1) {
            Unsafe.getUnsafe().putInt(p + len, crc); // crc
            Unsafe.getUnsafe().putInt(p + len + 4, (int) total); // total
            zlimit = Zip.gzipHeaderLen + len + 8;
        } else {
            zlimit = Zip.gzipHeaderLen + len;
        }

        // first we need to flush chunk header
        prepareChunk(zlimit - zpos);

        // if there is input remaining, don't change
        return flush && ret == 1 ? SEND_DEFLATED_END : SEND_DEFLATED_CONT;
    }

    private void flushSingle() throws PeerDisconnectedException, PeerIsSlowToReadException {
        state = DONE;
        send();
    }

    HttpResponseHeader getHeader() {
        return headerImpl;
    }

    HttpRawSocket getRawSocket() {
        return rawSocket;
    }

    void of(long fd) {
        this.fd = fd;
    }

    private void prepareBody() {
        flushBuf = out;
        flushBufSize = (int) (_wPtr - outPtr);
        _wPtr = outPtr;
    }

    private void prepareChunk(int len) {
        chunkSink.clear(Misc.EOL.length());
        Numbers.appendHex(chunkSink, len);
        chunkSink.put(Misc.EOL);
        flushBuf = chunkHeaderBuf;
        flushBufSize = chunkSink.length();
    }

    private void prepareCompressedBody() {
        if (z_streamp == 0) {
            z_streamp = Zip.deflateInit();
            pzout = Unsafe.malloc(responseBufferSize);
            zpos = zlimit = 0;
        }
        int r = (int) (_wPtr - outPtr);
        Zip.setInput(z_streamp, outPtr, r);
        this.crc = Zip.crc32(this.crc, outPtr, r);
        this.total += r;
        _wPtr = outPtr;
    }

    private void prepareHeaderSink() {
        headerImpl.prepareToSend();
    }

    private void resetZip() {
        if (z_streamp != 0) {
            Zip.deflateReset(z_streamp);
        }
        this.crc = 0;
        this.total = 0;
    }

    private void resumeSend(int nextState) throws PeerDisconnectedException, PeerIsSlowToReadException {
        state = nextState;
        resumeSend();
    }

    private void send() throws PeerDisconnectedException, PeerIsSlowToReadException {
        int sent = 0;
        while (sent < flushBufSize) {
            int n = nf.send(fd, flushBuf + sent, flushBufSize - sent);
            if (n < 0) {
                // disconnected
                LOG.info().$("disconnected [errno=").$(nf.errno()).$(']').$();
                throw PeerDisconnectedException.INSTANCE;
            }
            if (n == 0) {
                // test how many times we tried to send before parking up
                flushBuf += sent;
                flushBufSize -= sent;
                throw PeerIsSlowToReadException.INSTANCE;
            } else {
                dumpBuffer('<', flushBuf + sent, n);
                sent += n;
            }
        }
        totalBytesSent += sent;
    }

    private void dumpBuffer(char direction, long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put(direction);
            Net.dump(buffer, size);
        }
    }

    long getTotalBytesSent() {
        return totalBytesSent;
    }

    public class HttpResponseHeaderImpl extends AbstractCharSink implements Closeable, Mutable, HttpResponseHeader {
        private final long headerPtr;
        private final long limit;
        private final MillisecondClock clock;
        private long _wptr;
        private boolean chunky;
        private int code;

        public HttpResponseHeaderImpl(int bufferSize, MillisecondClock clock) {
            this.clock = clock;
            int sz = Numbers.ceilPow2(bufferSize);
            this.headerPtr = _wptr = Unsafe.calloc(sz);
            this.limit = headerPtr + sz;
        }

        @Override
        public void clear() {
            Unsafe.getUnsafe().setMemory(headerPtr, limit - headerPtr, (byte) 0);
            _wptr = headerPtr;
            chunky = false;
        }

        @Override
        public void close() {
            Unsafe.free(headerPtr, limit - headerPtr);
        }

        // this is used for HTTP access logging
        public int getCode() {
            return code;
        }

        @Override
        public CharSink put(CharSequence cs) {
            int len = cs.length();
            long p = _wptr;
            if (p + len < limit) {
                Chars.asciiStrCpy(cs, len, p);
                _wptr += len;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            if (_wptr < limit) {
                Unsafe.getUnsafe().putByte(_wptr++, (byte) c);
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            if (_wPtr + len < limit) {
                Chars.asciiCopyTo(chars, start, len, _wPtr);
                _wPtr += len;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public void send() throws PeerDisconnectedException, PeerIsSlowToReadException {
            headerImpl.prepareToSend();
            flushSingle();
        }

        @Override
        public String status(int code, CharSequence contentType, long contentLength) {
            this.code = code;
            String status = httpStatusMap.get(code);
            if (status == null) {
                throw new IllegalArgumentException("Illegal status code: " + code);
            }
            put("HTTP/1.1 ").put(code).put(' ').put(status).put(Misc.EOL);
            put("Server: ").put("questDB/1.0").put(Misc.EOL);
            put("Date: ");
            DateFormatUtils.formatHTTP(this, clock.getTicks());
            put(Misc.EOL);
            if (contentLength > -2) {
                if (this.chunky = (contentLength == -1)) {
                    put("Transfer-Encoding: ").put("chunked").put(Misc.EOL);
                } else {
                    put("Content-Length: ").put(contentLength).put(Misc.EOL);
                }
            }
            if (contentType != null) {
                put("Content-Type: ").put(contentType).put(Misc.EOL);
            }

            return status;
        }

        private void prepareToSend() {
            if (!chunky) {
                put(Misc.EOL);
            }
            flushBuf = headerPtr;
            flushBufSize = (int) (_wptr - headerPtr);
        }

    }

    public class SimpleResponseImpl {

        public void sendStatus(int code, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
            final String std = headerImpl.status(code, "text/html; charset=utf-8", -1L);
            sink.put(message == null ? std : message).put(Misc.EOL);
            prepareHeaderSink();
            resumeSend(CHUNK_HEAD);
        }

        public void sendStatus(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            headerImpl.status(code, "text/html; charset=utf-8", -2L);
            prepareHeaderSink();
            flushSingle();
        }

        public void sendStatusWithDefaultMessage(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, null);
        }
    }

    private class ResponseSinkImpl extends AbstractCharSink {

        @Override
        public CharSink put(CharSequence seq) {
            int len = seq.length();
            long p = _wPtr;
            if (p + len < limit) {
                Chars.asciiStrCpy(seq, len, p);
                _wPtr = p + len;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
            return this;
        }

        @Override
        public CharSink put(CharSequence cs, int lo, int hi) {
            int len = hi - lo;
            long p = _wPtr;
            if (p + len < limit) {
                Chars.asciiStrCpy(cs, lo, len, p);
                _wPtr = p + len;
            } else {
                throw NoSpaceLeftInResponseBufferException.INSTANCE;
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            if (_wPtr < limit) {
                Unsafe.getUnsafe().putByte(_wPtr++, (byte) c);
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            if (_wPtr + len < limit) {
                Chars.asciiCopyTo(chars, start, len, _wPtr);
                _wPtr += len;
                return this;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        @Override
        public CharSink put(float value, int scale) {
            if (Float.isNaN(value)) {
                put("null");
                return this;
            }
            return super.put(value, scale);
        }

        @Override
        public CharSink put(double value, int scale) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                put("null");
                return this;
            }
            return super.put(value, scale);
        }

        @Override
        protected void putUtf8Special(char c) {
            if (c < 32) {
                escapeSpace(c);
            } else {
                switch (c) {
                    case '/':
                    case '\"':
                    case '\\':
                        put('\\');
                        // intentional fall through
                    default:
                        put(c);
                        break;
                }
            }
        }

        public void status(int status, CharSequence contentType) {
            headerImpl.status(status, contentType, -1);
        }

        private void escapeSpace(char c) {
            switch (c) {
                case '\0':
                    break;
                case '\b':
                    put("\\b");
                    break;
                case '\f':
                    put("\\f");
                    break;
                case '\n':
                    put("\\n");
                    break;
                case '\r':
                    put("\\r");
                    break;
                case '\t':
                    put("\\t");
                    break;
                default:
                    put(c);
                    break;
            }
        }
    }

    public class HttpRawSocketImpl implements HttpRawSocket {

        @Override
        public long getBufferAddress() {
            return out;
        }

        @Override
        public int getBufferSize() {
            return responseBufferSize;
        }

        @Override
        public void send(int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
            flushBuf = out;
            flushBufSize = size;
            flushSingle();
        }
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl implements HttpChunkedResponseSocket {

        private long bookmark = outPtr;

        @Override
        public void bookmark() {
            bookmark = _wPtr;
        }

        @Override
        public void done() throws PeerDisconnectedException, PeerIsSlowToReadException {
            flushBufSize = 0;
            if (deflateBeforeSend) {
                resumeSend(FLUSH);
            } else {
                resumeSend(END_CHUNK);
                LOG.debug().$("end chunk sent").$();
            }
        }

        @Override
        public HttpResponseHeader headers() {
            return headerImpl;
        }

        @Override
        public boolean resetToBookmark() {
            _wPtr = bookmark;
            return bookmark != outPtr;
        }

        @Override
        public void sendChunk() throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (outPtr != _wPtr) {
                if (deflateBeforeSend) {
                    flushBufSize = 0;
                } else {
                    prepareChunk((int) (_wPtr - outPtr));
                }
                resumeSend(MULTI_CHUNK);
            }
        }

        @Override
        public void sendHeader() throws PeerDisconnectedException, PeerIsSlowToReadException {
            prepareHeaderSink();
            flushSingle();
        }

        @Override
        public void status(int status, CharSequence contentType) {
            super.status(status, contentType);
            if (deflateBeforeSend) {
                headerImpl.put("Content-Encoding: gzip").put(Misc.EOL);
            }
        }
    }
}
