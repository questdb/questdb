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
import io.questdb.network.*;
import io.questdb.std.ThreadLocal;
import io.questdb.std.*;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.ex.ZLibException;
import io.questdb.std.str.*;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.std.Chars.isBlank;

public class HttpResponseSink implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(HttpResponseSink.class);
    private static final IntObjHashMap<String> httpStatusMap = new IntObjHashMap<>();
    private static final ThreadLocal<Utf8StringSink> tlSink = new ThreadLocal<>(Utf8StringSink::new);
    private final ChunkUtf8Sink buffer;
    private final ChunkedResponseImpl chunkedResponse = new ChunkedResponseImpl();
    private final ChunkUtf8Sink compressOutBuffer;
    private final boolean connectionCloseHeader;
    private final boolean cookiesEnabled;
    private final boolean dumpNetworkTraffic;
    private final int forceSendFragmentationChunkSize;
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
        this.buffer = new ChunkUtf8Sink(responseBufferSize);
        this.compressOutBuffer = new ChunkUtf8Sink(responseBufferSize);
        this.headerImpl = new HttpResponseHeaderImpl(configuration.getMillisecondClock());
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.httpVersion = configuration.getHttpVersion();
        this.connectionCloseHeader = !configuration.getServerKeepAlive();
        this.cookiesEnabled = configuration.areCookiesEnabled();
        this.forceSendFragmentationChunkSize = configuration.getForceSendFragmentationChunkSize();
    }

    @Override
    public void clear() {
        headerImpl.clear();
        totalBytesSent = 0;
        headersSent = false;
        chunkedRequestDone = false;
        simple.clear();
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

    public HttpChunkedResponse getChunkedResponse() {
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

    private long getFd() {
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

    private void sendBuffer(ChunkUtf8Sink sendBuf) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int available = (int) sendBuf.getReadNAvailable();
        int nSend = Math.min(forceSendFragmentationChunkSize, available);
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
            } else if (available <= forceSendFragmentationChunkSize) {
                dumpBuffer(sendBuf.getReadAddress(), n);
                sendBuf.onRead(n);
                nSend -= n;
                totalBytesSent += n;
            } else {
                // This branch is for tests only
                sendBuf.onRead(n);
                throw PeerIsSlowToReadException.INSTANCE;
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

    void open() {
        this.buffer.reopen();
    }

    private class ChunkUtf8Sink implements Utf8Sink, Closeable, Mutable, Reopenable {
        private static final String EOF_CHUNK = "\r\n00\r\n\r\n";
        private static final int MAX_CHUNK_HEADER_SIZE = 12;
        private final long bufSize;
        private long _rptr;
        private long _wptr;
        private long bufStart;
        private long bufStartOfData;

        private ChunkUtf8Sink(int bufSize) {
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
        public Utf8Sink put(@Nullable Utf8Sequence us) {
            if (us != null) {
                int size = us.size();
                Utf8s.strCpy(us, size, getWriteAddress(size));
                onWrite(size);
            }
            return this;
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            final int size = Bytes.checkedLoHiSize(lo, hi, 0);
            final long dest = getWriteAddress(size);
            Vect.memcpy(dest, lo, size);
            onWrite(size);
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

    private class ChunkedResponseImpl extends ResponseSinkImpl implements HttpChunkedResponse {
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
            putNonAscii(srcAddr, srcAddr + len);
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
        private boolean chunked;
        private int code;

        public HttpResponseHeaderImpl(MillisecondClock clock) {
            this.clock = clock;
        }

        @Override
        public void clear() {
            buffer.clearAndPrepareToWriteToBuffer();
            chunked = false;
        }

        // this is used for HTTP access logging
        public int getCode() {
            return code;
        }

        public boolean isChunked() {
            return chunked;
        }

        @Override
        public Utf8Sink put(byte b) {
            Unsafe.getUnsafe().putByte(buffer.getWriteAddress(1), b);
            buffer.onWrite(1);
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
        public Utf8Sink putNonAscii(long lo, long hi) {
            buffer.putNonAscii(lo, hi);
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
                chunked = (contentLength == -1);
                if (chunked) {
                    putAscii("Transfer-Encoding: chunked").putEOL();
                } else {
                    putAscii("Content-Length: ").put(contentLength).putEOL();
                }
                putAscii("Content-Type: ").put(contentType).putEOL();
            }

            if (connectionCloseHeader) {
                putAscii("Connection: close").putEOL();
            }

            return status;
        }

        private void prepareToSend() {
            if (!chunked) {
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
            if (Numbers.isNull(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value, scale);
        }

        @Override
        public Utf8Sink put(double value, int scale) {
            if (Numbers.isNull(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value, scale);
        }

        @Override
        public Utf8Sink putNonAscii(long lo, long hi) {
            buffer.putNonAscii(lo, hi);
            return this;
        }

        public void status(int status, CharSequence contentType) {
            buffer.clearAndPrepareToWriteToBuffer();
            headerImpl.status("HTTP/1.1 ", status, contentType, -1);
        }
    }

    public class SimpleResponseImpl {
        private boolean contentSent = false;
        private boolean headerSent = false;

        public void clear() {
            contentSent = false;
            headerSent = false;
        }

        public void sendStatusJsonContent(
                int code
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusJsonContent(code, null, null, null, null);
        }

        public void sendStatusJsonContent(
                int code,
                @Nullable CharSequence message
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusJsonContent(code, message, null, null, null);
        }

        public void sendStatusJsonContent(
                int code,
                @Nullable CharSequence message,
                @Nullable CharSequence header,
                @Nullable CharSequence cookieName,
                @Nullable CharSequence cookieValue
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusWithContent(CONTENT_TYPE_JSON, code, message, header, cookieName, cookieValue, message != null ? message.length() : -1);
        }

        public void sendStatusNoContent(int code, @Nullable CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (!headerSent) {
                buffer.clearAndPrepareToWriteToBuffer();
                headerImpl.status(httpVersion, code, null, -2L);
                if (header != null) {
                    headerImpl.put(header).put(Misc.EOL);
                }
                prepareHeaderSink();
                headerSent = true;
            }
            flushSingle();
        }

        public void sendStatusNoContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusNoContent(code, null);
        }

        /**
         * Sends "text/plain" content type response with customised message and
         * optional additional header and cookie.
         *
         * @param code        response code, has to be compatible with "text" response type
         * @param message     optional message, if not provided, a standard message for the response code will be used
         * @param header      optional header
         * @param cookieName  optional cookie name, when name is not null the value must be not-null too
         * @param cookieValue optional cookie value
         * @throws PeerDisconnectedException exception if HTTP client disconnects during us sending
         * @throws PeerIsSlowToReadException exception if HTTP client does not keep up with us sending
         */
        public void sendStatusTextContent(
                int code,
                @Nullable CharSequence message,
                @Nullable CharSequence header,
                @Nullable CharSequence cookieName,
                @Nullable CharSequence cookieValue
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusWithContent(CONTENT_TYPE_TEXT, code, message, header, cookieName, cookieValue, -1);
        }

        public void sendStatusTextContent(
                int code,
                CharSequence message,
                CharSequence header
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusTextContent(code, message, header, null, null);
        }

        public void sendStatusTextContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusTextContent(code, null, null);
        }

        public void sendStatusTextContent(int code, CharSequence header) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusTextContent(code, null, header);
        }

        public void sendStatusWithCookie(int code, CharSequence message, CharSequence cookieName, CharSequence cookieValue) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusTextContent(code, message, null, cookieName, cookieValue);
        }

        private void sendStatusWithContent(
                String contentType,
                int code,
                @Nullable CharSequence message,
                @Nullable CharSequence header,
                @Nullable CharSequence cookieName,
                @Nullable CharSequence cookieValue,
                long contentLength
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (!headerSent) {
                buffer.clearAndPrepareToWriteToBuffer();
                headerImpl.status(httpVersion, code, contentType, contentLength);
                if (header != null) {
                    headerImpl.put(header).put(Misc.EOL);
                }
                if (cookieName != null) {
                    setCookie(cookieName, cookieValue);
                }
                prepareHeaderSink();
                headerSent = true;
            }

            if (!contentSent) {
                flushSingle();
                buffer.clearAndPrepareToWriteToBuffer();
                if (message == null) {
                    sink.put(httpStatusMap.get(code)).putEOL();
                } else {
                    // this is ugly, add a putUtf16() method to the response sink?
                    final Utf8StringSink utf8Sink = tlSink.get();
                    utf8Sink.clear();
                    utf8Sink.put(message);
                    sink.put(utf8Sink).putEOL();
                }
                final boolean chunked = headerImpl.isChunked();
                buffer.prepareToReadFromBuffer(chunked, chunked);
                contentSent = true;
            }
            resumeSend();
        }

        private void setCookie(CharSequence name, CharSequence value) {
            if (cookiesEnabled) {
                headerImpl.put(HEADER_SET_COOKIE).putAscii(": ").put(name).putAscii(COOKIE_VALUE_SEPARATOR).put(!isBlank(value) ? value : "").putEOL();
            }
        }
    }

    static {
        httpStatusMap.put(200, "OK");
        httpStatusMap.put(204, "OK");
        httpStatusMap.put(206, "Partial content");
        httpStatusMap.put(302, "Temporarily Moved");
        httpStatusMap.put(304, "Not Modified");
        httpStatusMap.put(400, "Bad request");
        httpStatusMap.put(401, "Unauthorized");
        httpStatusMap.put(403, "Forbidden");
        httpStatusMap.put(404, "Not Found");
        httpStatusMap.put(408, "Request Timeout");
        httpStatusMap.put(411, "Length Required");
        httpStatusMap.put(413, "Content Too Large");
        httpStatusMap.put(415, "Bad request");
        httpStatusMap.put(416, "Request range not satisfiable");
        httpStatusMap.put(431, "Headers too large");
        httpStatusMap.put(500, "Internal server error");
    }
}
