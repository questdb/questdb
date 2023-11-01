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

import io.questdb.cairo.Reopenable;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.*;
import io.questdb.std.*;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.ex.ZLibException;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.std.Chars.isBlank;

public class HttpResponseSink implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(HttpResponseSink.class);
    private static final IntObjHashMap<String> httpStatusMap = new IntObjHashMap<>();
    private final ChunkBuffer buffer;
    private final ChunkedResponseImpl chunkedResponse = new ChunkedResponseImpl();
    private final ChunkBuffer compressOutBuffer;
    private final boolean connectionCloseHeader;
    private final boolean cookiesEnabled;
    private final boolean dumpNetworkTraffic;
    private final HttpResponseHeaderImpl headerImpl;
    private final String httpVersion;
    private final NetworkFacade nf;
    private final HttpRawSocketImpl rawSocket = new HttpRawSocketImpl();
    private final SimpleResponseImpl simple = new SimpleResponseImpl();
    private final ResponseSinkImpl sink = new ResponseSinkImpl();
    private boolean chunkedRequestDone;
    private boolean compressedHeaderDone;
    private boolean compressedOutputReady;
    private boolean compressionComplete;
    private int crc = 0;
    private boolean deflateBeforeSend = false;
    private boolean headersSent;
    private Socket socket;
    private long total = 0;
    private long totalBytesSent = 0;
    private long zStreamPtr = 0;

    public HttpResponseSink(HttpContextConfiguration configuration) {
        final int responseBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.nf = configuration.getNetworkFacade();
        this.buffer = new ChunkBuffer(responseBufferSize);
        this.compressOutBuffer = new ChunkBuffer(responseBufferSize);
        this.headerImpl = new HttpResponseHeaderImpl(configuration.getClock());
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.httpVersion = configuration.getHttpVersion();
        this.connectionCloseHeader = !configuration.getServerKeepAlive();
        this.cookiesEnabled = configuration.areCookiesEnabled();
    }

    @Override
    public void clear() {
        headerImpl.clear();
        totalBytesSent = 0;
        headersSent = false;
        chunkedRequestDone = false;
        resetZip();
    }

    @Override
    public void close() {
        if (zStreamPtr != 0) {
            Zip.deflateEnd(zStreamPtr);
            zStreamPtr = 0;
            compressOutBuffer.close();
        }
        buffer.close();
        socket = null;
    }

    public HttpChunkedResponseSocket getChunkedSocket() {
        return chunkedResponse;
    }

    public int getCode() {
        return headerImpl.getCode();
    }

    public SimpleResponseImpl getSimple() {
        return simple;
    }

    public void resumeSend() throws PeerDisconnectedException, PeerIsSlowToReadException {
        if (!headersSent || !deflateBeforeSend) {
            sendBuffer(buffer);
            return;
        }

        while (true) {
            if (!compressedOutputReady && !compressionComplete) {
                deflate();
            }

            if (compressedOutputReady) {
                sendBuffer(compressOutBuffer);
                compressedOutputReady = false;
                if (compressionComplete) {
                    break;
                }
            } else {
                break;
            }
        }
    }

    public void setDeflateBeforeSend(boolean deflateBeforeSend) {
        this.deflateBeforeSend = deflateBeforeSend;
        if (zStreamPtr == 0 && deflateBeforeSend) {
            zStreamPtr = Zip.deflateInit();
            compressOutBuffer.reopen();
        }
    }

    private void deflate() {
        if (!compressedHeaderDone) {
            int len = Zip.gzipHeaderLen;
            Vect.memcpy(compressOutBuffer.getWriteAddress(len), Zip.gzipHeader, len);
            compressOutBuffer.onWrite(len);
            compressedHeaderDone = true;
        }

        int nInAvailable = (int) buffer.getReadNAvailable();
        if (nInAvailable > 0) {
            long inAddress = buffer.getReadAddress();
            LOG.debug().$("Zip.setInput [inAddress=").$(inAddress).$(", nInAvailable=").$(nInAvailable).I$();
            buffer.write64BitZeroPadding();
            Zip.setInput(zStreamPtr, inAddress, nInAvailable);
        }

        int ret;
        int len;
        // compress input until we run out of either input or output
        do {
            int sz = (int) compressOutBuffer.getWriteNAvailable() - 8;
            long p = compressOutBuffer.getWriteAddress(0);
            LOG.debug().$("deflate starting [p=").$(p).$(", sz=").$(sz).$(", chunkedRequestDone=").$(chunkedRequestDone).I$();
            ret = Zip.deflate(zStreamPtr, p, sz, chunkedRequestDone);
            len = sz - Zip.availOut(zStreamPtr);
            compressOutBuffer.onWrite(len);
            if (ret < 0) {
                // This is not an error, zlib just couldn't do any work with the input/output buffers it was provided.
                // This happens often (will depend on output buffer size) when there is no new input and zlib has finished generating
                // output from previously provided input
                if (ret != Zip.Z_BUF_ERROR || len != 0) {
                    throw HttpException.instance("could not deflate [ret=").put(ret);
                }
            }

            int availIn = Zip.availIn(zStreamPtr);
            int nInConsumed = nInAvailable - availIn;
            if (nInConsumed > 0) {
                this.crc = Zip.crc32(this.crc, buffer.getReadAddress(), nInConsumed);
                this.total += nInConsumed;
                buffer.onRead(nInConsumed);
                nInAvailable = availIn;
            }

            LOG.debug().$("deflate finished [ret=").$(ret).$(", len=").$(len).$(", availIn=").$(availIn).I$();
        } while (len == 0 && nInAvailable > 0);

        if (nInAvailable == 0) {
            buffer.clearAndPrepareToWriteToBuffer();
        }

        if (len == 0) {
            compressedOutputReady = false;
            return;
        }
        compressedOutputReady = true;

        // this is ZLib error, can't continue
        if (len < 0) {
            throw ZLibException.INSTANCE;
        }

        // trailer
        boolean finished = chunkedRequestDone && ret == Zip.Z_STREAM_END;
        if (finished) {
            long p = compressOutBuffer.getWriteAddress(0);
            Unsafe.getUnsafe().putInt(p, crc); // crc
            Unsafe.getUnsafe().putInt(p + 4, (int) total); // total
            compressOutBuffer.onWrite(8);
            compressionComplete = true;
        }
        compressOutBuffer.prepareToReadFromBuffer(true, finished);
    }

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('<');
            Net.dump(buffer, size);
        }
    }

    private void flushSingle() throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendBuffer(buffer);
    }

    private int getFd() {
        return socket != null ? socket.getFd() : -1;
    }

    private void prepareHeaderSink() {
        buffer.prepareToReadFromBuffer(false, false);
        headerImpl.prepareToSend();
    }

    private void resetZip() {
        if (zStreamPtr != 0) {
            Zip.deflateReset(zStreamPtr);
            compressOutBuffer.clear();
            crc = 0;
            total = 0;
            compressedHeaderDone = false;
            compressedOutputReady = false;
            compressionComplete = false;
        }
    }

    private void sendBuffer(ChunkBuffer sendBuf) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int nSend = (int) sendBuf.getReadNAvailable();
        while (nSend > 0) {
            int n = socket.send(sendBuf.getReadAddress(), nSend);
            if (n < 0) {
                // disconnected
                LOG.error()
                        .$("disconnected [errno=").$(nf.errno())
                        .$(", fd=").$(socket.getFd())
                        .I$();
                throw PeerDisconnectedException.INSTANCE;
            }
            if (n == 0) {
                // test how many times we tried to send before parking up
                throw PeerIsSlowToReadException.INSTANCE;
            } else {
                dumpBuffer(sendBuf.getReadAddress(), n);
                sendBuf.onRead(n);
                nSend -= n;
                totalBytesSent += n;
            }
        }
        assert sendBuf.getReadNAvailable() == 0;
        sendBuf.clearAndPrepareToWriteToBuffer();
    }

    HttpResponseHeader getHeader() {
        return headerImpl;
    }

    HttpRawSocket getRawSocket() {
        return rawSocket;
    }

    long getTotalBytesSent() {
        return totalBytesSent;
    }

    void of(Socket socket) {
        this.socket = socket;
        if (socket != null) {
            this.buffer.reopen();
        }
    }

    private class ChunkBuffer implements Utf8Sink, Closeable, Mutable, Reopenable {
        private static final String EOF_CHUNK = "\r\n00\r\n\r\n";
        private static final int MAX_CHUNK_HEADER_SIZE = 12;
        private final long bufSize;
        private long _rptr;
        private long _wptr;
        private long bufStart;
        private long bufStartOfData;

        private ChunkBuffer(int bufSize) {
            this.bufSize = bufSize;
        }

        @Override
        public void clear() {
            _wptr = _rptr = bufStartOfData;
        }

        @Override
        public void close() {
            if (bufStart != 0) {
                Unsafe.free(bufStart, bufSize + MAX_CHUNK_HEADER_SIZE + EOF_CHUNK.length(), MemoryTag.NATIVE_HTTP_CONN);
                bufStart = bufStartOfData = _wptr = _rptr = 0;
            }
        }

        @Override
        public Utf8Sink put(byte b) {
            Unsafe.getUnsafe().putByte(getWriteAddress(1), b);
            onWrite(1);
            return this;
        }

        @Override
        public Utf8Sink put(long lo, long hi) {
            final int size = Bytes.checkedLoHiSize(lo, hi, 0);
            final long dest = getWriteAddress(size);
            Vect.memcpy(dest, lo, size);
            onWrite(size);
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                Utf8s.strCpy(us, size, getWriteAddress(size));
                onWrite(size);
            }
            return this;
        }

        @Override
        public void reopen() {
            if (bufStart == 0) {
                bufStart = Unsafe.malloc(bufSize + MAX_CHUNK_HEADER_SIZE + EOF_CHUNK.length(), MemoryTag.NATIVE_HTTP_CONN);
                bufStartOfData = bufStart + MAX_CHUNK_HEADER_SIZE;
                clear();
            }
        }

        void clearAndPrepareToWriteToBuffer() {
            _rptr = _wptr = bufStartOfData;
        }

        long getReadAddress() {
            assert _rptr != 0;
            return _rptr;
        }

        long getReadNAvailable() {
            return _wptr - _rptr;
        }

        long getWriteAddress(long len) {
            assert _wptr != 0;
            if (getWriteNAvailable() >= len) {
                return _wptr;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        long getWriteNAvailable() {
            return bufStartOfData + bufSize - _wptr;
        }

        void onRead(int nRead) {
            assert nRead >= 0 && nRead <= getReadNAvailable();
            _rptr += nRead;
        }

        void onWrite(int nWrite) {
            assert nWrite >= 0 && nWrite <= getWriteNAvailable();
            _wptr += nWrite;
        }

        void prepareToReadFromBuffer(boolean addChunkHeader, boolean addEofChunk) {
            if (addChunkHeader) {
                int len = (int) (_wptr - bufStartOfData);
                int padding = len == 0 ? 6 : (Integer.numberOfLeadingZeros(len) >> 3) << 1;
                long tmp = _wptr;
                _rptr = _wptr = bufStart + padding;
                putEOL();
                Numbers.appendHex(this, len);
                putEOL();
                _wptr = tmp;
            }
            if (addEofChunk) {
                int len = EOF_CHUNK.length();
                Utf8s.strCpyAscii(EOF_CHUNK, len, _wptr);
                _wptr += len;
                LOG.debug().$("end chunk sent [fd=").$(getFd()).I$();
            }
        }

        void write64BitZeroPadding() {
            Unsafe.getUnsafe().putLong(bufStartOfData - 8, 0);
            Unsafe.getUnsafe().putLong(_wptr, 0);
        }
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl implements HttpChunkedResponseSocket {
        private long bookmark = 0;

        @Override
        public void bookmark() {
            bookmark = buffer._wptr;
        }

        @Override
        public void done() throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (!chunkedRequestDone) {
                sendChunk(true);
            }
        }

        @Override
        public HttpResponseHeader headers() {
            return headerImpl;
        }

        @Override
        public boolean resetToBookmark() {
            buffer._wptr = bookmark;
            return bookmark != buffer.bufStartOfData;
        }

        @Override
        public void sendChunk(boolean done) throws PeerDisconnectedException, PeerIsSlowToReadException {
            headersSent = true;
            chunkedRequestDone = done;
            if (buffer.getReadNAvailable() > 0 || done) {
                if (!deflateBeforeSend) {
                    buffer.prepareToReadFromBuffer(true, chunkedRequestDone);
                }
                resumeSend();
            }
        }

        @Override
        public void sendHeader() throws PeerDisconnectedException, PeerIsSlowToReadException {
            chunkedRequestDone = false;
            prepareHeaderSink();
            flushSingle();
            buffer.clearAndPrepareToWriteToBuffer();
        }

        @Override
        public void shutdownWrite() {
            socket.shutdown(Net.SHUT_WR);
        }

        @Override
        public void status(int status, CharSequence contentType) {
            super.status(status, contentType);
            if (deflateBeforeSend) {
                headerImpl.putAscii("Content-Encoding: gzip").putEOL();
            }
        }

        /**
         * Variant of `put(long lo, long hi)` that writes up to the available space in the buffer.
         * If there isn't enough space to write the whole length, the written length is returned.
         */
        @Override
        public int writeBytes(long srcAddr, int len) {
            assert len > 0;
            len = (int) Math.min(len, buffer.getWriteNAvailable());
            put(srcAddr, srcAddr + len);
            return len;
        }
    }

    public class HttpRawSocketImpl implements HttpRawSocket {

        @Override
        public long getBufferAddress() {
            return buffer.getWriteAddress(1);
        }

        @Override
        public int getBufferSize() {
            return (int) buffer.getWriteNAvailable();
        }

        @Override
        public void send(int size) throws PeerDisconnectedException, PeerIsSlowToReadException {
            buffer.onWrite(size);
            buffer.prepareToReadFromBuffer(false, false);
            flushSingle();
            buffer.clearAndPrepareToWriteToBuffer();
        }
    }

    public class HttpResponseHeaderImpl implements Utf8Sink, HttpResponseHeader, Mutable {
        private final MillisecondClock clock;
        private boolean chunky;
        private int code;

        public HttpResponseHeaderImpl(MillisecondClock clock) {
            this.clock = clock;
        }

        @Override
        public void clear() {
            buffer.clearAndPrepareToWriteToBuffer();
            chunky = false;
        }

        // this is used for HTTP access logging
        public int getCode() {
            return code;
        }

        @Override
        public Utf8Sink put(long lo, long hi) {
            buffer.put(lo, hi);
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                Utf8s.strCpy(us, size, buffer.getWriteAddress(size));
                buffer.onWrite(size);
            }
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            Unsafe.getUnsafe().putByte(buffer.getWriteAddress(1), b);
            buffer.onWrite(1);
            return this;
        }

        @Override
        public void send() throws PeerDisconnectedException, PeerIsSlowToReadException {
            headerImpl.prepareToSend();
            flushSingle();
        }

        @Override
        public void setCookie(CharSequence name, CharSequence value) {
            if (cookiesEnabled) {
                put(HEADER_SET_COOKIE).putAscii(": ").put(name).putAscii(COOKIE_VALUE_SEPARATOR).put(value).putEOL();
            }
        }

        @Override
        public String status(CharSequence httpProtocolVersion, int code, CharSequence contentType, long contentLength) {
            this.code = code;
            String status = httpStatusMap.get(code);
            if (status == null) {
                throw new IllegalArgumentException("Illegal status code: " + code);
            }
            buffer.clearAndPrepareToWriteToBuffer();
            putAscii(httpProtocolVersion).put(code).put(' ').putAscii(status).putEOL();
            putAscii("Server: ").putAscii("questDB/1.0").putEOL();
            putAscii("Date: ");
            DateFormatUtils.formatHTTP(this, clock.getTicks());
            putEOL();
            if (contentLength > -2) {
                this.chunky = (contentLength == -1);
                if (this.chunky) {
                    putAscii("Transfer-Encoding: chunked").putEOL();
                } else {
                    putAscii("Content-Length: ").put(contentLength).putEOL();
                }
            }
            if (contentType != null) {
                putAscii("Content-Type: ").put(contentType).putEOL();
            }

            if (connectionCloseHeader) {
                putAscii("Connection: close").putEOL();
            }

            return status;
        }

        private void prepareToSend() {
            if (!chunky) {
                putEOL();
            }
        }
    }

    private class ResponseSinkImpl implements Utf8Sink {

        @Override
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            buffer.put(us);
            return this;
        }

        @Override
        public Utf8Sink put(byte b) {
            buffer.put(b);
            return this;
        }

        @Override
        public Utf8Sink put(float value, int scale) {
            if (Float.isNaN(value) || Float.isInfinite(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value, scale);
        }

        @Override
        public Utf8Sink put(double value, int scale) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value, scale);
        }

        @Override
        public Utf8Sink put(@NotNull CharSequence cs, int lo, int hi) {
            int i = lo;
            while (i < hi) {
                char c = cs.charAt(i++);
                if (c < 32) {
                    escapeSpace(c);
                } else if (c < 128) {
                    switch (c) {
                        case '\"':
                        case '\\':
                            putAscii('\\');
                            // intentional fall through
                        default:
                            putAscii(c);
                            break;
                    }
                } else {
                    i = Utf8s.encodeUtf16Char(this, cs, hi, i, c);
                }
            }
            return this;
        }

        @Override
        public Utf8Sink put(long lo, long hi) {
            buffer.put(lo, hi);
            return this;
        }

        @Override
        public Utf8Sink put(@Nullable CharSequence cs) {
            if (cs != null) {
                put(cs, 0, cs.length());
            }
            return this;
        }

        @Override
        public Utf8Sink put(char c) {
            if (c < 32) {
                escapeSpace(c);
            } else {
                switch (c) {
                    case '\"':
                    case '\\':
                        putAscii('\\');
                        // intentional fall through
                    default:
                        Utf8Sink.super.put(c);
                        break;
                }
            }
            return this;
        }

        public void status(int status, CharSequence contentType) {
            buffer.clearAndPrepareToWriteToBuffer();
            headerImpl.status("HTTP/1.1 ", status, contentType, -1);
        }

        private void escapeSpace(char c) {
            switch (c) {
                case '\b':
                    putAsciiInternal("\\b");
                    break;
                case '\f':
                    putAsciiInternal("\\f");
                    break;
                case '\n':
                    putAsciiInternal("\\n");
                    break;
                case '\r':
                    putAsciiInternal("\\r");
                    break;
                case '\t':
                    putAsciiInternal("\\t");
                    break;
                default:
                    putAsciiInternal("\\u00");
                    put(c >> 4);
                    putAsciiInternal(Numbers.hexDigits[c & 15]);
                    break;
            }
        }

        private void putAsciiInternal(char c) {
            Utf8Sink.super.putAscii(c);
        }

        private void putAsciiInternal(@Nullable CharSequence cs) {
            if (cs != null) {
                int l = cs.length();
                for (int i = 0; i < l; i++) {
                    putAsciiInternal(cs.charAt(i));
                }
            }
        }
    }

    public class SimpleResponseImpl {
        public void sendStatus(int code, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, message, null);
        }

        public void sendStatus(int code, CharSequence message, CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, message, header, null, null);
        }

        public void sendStatus(int code, CharSequence message, CharSequence header, CharSequence cookieName, CharSequence cookieValue) throws PeerDisconnectedException, PeerIsSlowToReadException {
            buffer.clearAndPrepareToWriteToBuffer();
            final String std = headerImpl.status(httpVersion, code, CONTENT_TYPE_TEXT, -1L);
            if (header != null) {
                headerImpl.put(header).put(Misc.EOL);
            }
            if (cookieName != null) {
                setCookie(cookieName, cookieValue);
            }
            prepareHeaderSink();
            flushSingle();
            buffer.clearAndPrepareToWriteToBuffer();
            sink.put(message == null ? std : message).putEOL();
            buffer.prepareToReadFromBuffer(true, true);
            resumeSend();
        }

        public void sendStatus(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            buffer.clearAndPrepareToWriteToBuffer();
            headerImpl.status(httpVersion, code, CONTENT_TYPE_HTML, -2L);
            prepareHeaderSink();
            flushSingle();
        }

        public void sendStatusWithCookie(int code, CharSequence message, CharSequence cookieName, CharSequence cookieValue) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, message, null, cookieName, cookieValue);
        }

        public void sendStatusWithDefaultMessage(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, null);
        }

        public void sendStatusWithHeader(int code, CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatus(code, null, header);
        }

        private void setCookie(CharSequence name, CharSequence value) {
            if (cookiesEnabled) {
                headerImpl.put(HEADER_SET_COOKIE).putAscii(": ").put(name).putAscii(COOKIE_VALUE_SEPARATOR).put(!isBlank(value) ? value : "").putEOL();
            }
        }
    }

    static {
        httpStatusMap.put(200, "OK");
        httpStatusMap.put(206, "Partial content");
        httpStatusMap.put(304, "Not Modified");
        httpStatusMap.put(400, "Bad request");
        httpStatusMap.put(401, "Unauthorized");
        httpStatusMap.put(403, "Forbidden");
        httpStatusMap.put(404, "Not Found");
        httpStatusMap.put(416, "Request range not satisfiable");
        httpStatusMap.put(431, "Headers too large");
        httpStatusMap.put(500, "Internal server error");
    }
}
