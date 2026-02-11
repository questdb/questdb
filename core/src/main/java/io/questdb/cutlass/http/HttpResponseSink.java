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

package io.questdb.cutlass.http;

import io.questdb.cairo.CairoException;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.network.Net;
import io.questdb.network.NetworkFacade;
import io.questdb.network.NoSpaceLeftInResponseBufferException;
import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.Socket;
import io.questdb.std.IntObjHashMap;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Mutable;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.Zip;
import io.questdb.std.bytes.Bytes;
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.ex.ZLibException;
import io.questdb.std.str.StdoutSink;
import io.questdb.std.str.Utf8Sequence;
import io.questdb.std.str.Utf8Sink;
import io.questdb.std.str.Utf8String;
import io.questdb.std.str.Utf8s;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

import static io.questdb.cutlass.http.HttpConstants.*;
import static io.questdb.std.Chars.isBlank;
import static java.net.HttpURLConnection.*;

public class HttpResponseSink implements Closeable, Mutable {
    public static final int HTTP_MISDIRECTED_REQUEST = 421;
    public static final int HTTP_SERVICE_UNAVAILABLE = 503;
    public static final int HTTP_TOO_MANY_REQUESTS = 429;
    private static final Utf8String EMPTY_JSON = new Utf8String("{}");
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final int HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE = 431;
    private static final Log LOG = LogFactory.getLog(HttpResponseSink.class);
    private static final IntObjHashMap<Utf8Sequence> httpJsonStatusMap = new IntObjHashMap<>();
    private static final IntObjHashMap<Utf8Sequence> httpStatusMap = new IntObjHashMap<>();
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
    private final SimpleResponseImpl simpleResponse = new SimpleResponseImpl();
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

    public HttpResponseSink(HttpServerConfiguration configuration) {
        final int responseBufferSize = configuration.getSendBufferSize();
        this.nf = configuration.getNetworkFacade();
        this.buffer = new ChunkUtf8Sink(responseBufferSize);
        this.compressOutBuffer = new ChunkUtf8Sink(responseBufferSize);
        final HttpContextConfiguration contextConfiguration = configuration.getHttpContextConfiguration();
        this.headerImpl = new HttpResponseHeaderImpl(contextConfiguration.getMillisecondClock());
        this.dumpNetworkTraffic = contextConfiguration.getDumpNetworkTraffic();
        this.httpVersion = contextConfiguration.getHttpVersion();
        this.connectionCloseHeader = !contextConfiguration.getServerKeepAlive();
        this.cookiesEnabled = contextConfiguration.areCookiesEnabled();
        this.forceSendFragmentationChunkSize = contextConfiguration.getForceSendFragmentationChunkSize();
    }

    @Override
    public void clear() {
        headerImpl.clear();
        totalBytesSent = 0;
        headersSent = false;
        chunkedRequestDone = false;
        simpleResponse.clear();
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

    public void setDeflateBeforeSend(boolean deflateBeforeSend, long bufferSize) {
        this.deflateBeforeSend = deflateBeforeSend;
        if (zStreamPtr == 0 && deflateBeforeSend) {
            zStreamPtr = Zip.deflateInit();
            compressOutBuffer.reopen(bufferSize);
        }
    }

    public SimpleResponseImpl simpleResponse() {
        return simpleResponse;
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
                totalBytesSent += n;
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

    void of(Socket socket, long bufferSize) {
        this.socket = socket;
        if (socket != null) {
            buffer.reopen(bufferSize);
        }
    }

    void open(long bufferSize) {
        buffer.reopen(bufferSize);
    }

    private class ChunkUtf8Sink implements Utf8Sink, Closeable, Mutable {
        // the last chunk is a chunk with size zero. it happens to have exactly 8 ascii chars so it fits to long nicely: \r\n00\r\n\r\n
        private static final long EOF_CHUNK_LONG = (long) '\r' << 56 | (long) '\n' << 48 | (long) '0' << 40 | (long) '0' << 32 | (long) '\r' << 24 | (long) '\n' << 16 | (long) '\r' << 8 | (long) '\n';
        private static final long EOF_CHUNK_LONG_BE = Numbers.bswap(EOF_CHUNK_LONG);
        private static final int MAX_CHUNK_HEADER_SIZE = 12;
        private long _rptr;
        private long _wptr;
        private long bufSize;
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
                Unsafe.free(bufStart, bufSize + MAX_CHUNK_HEADER_SIZE + Long.BYTES, MemoryTag.NATIVE_HTTP_CONN);
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

        public void reopen(long bufSize) {
            if (bufStart == 0) {
                this.bufSize = bufSize;
                // note: we reserve extra space for:
                // 1. chunk header size: MAX_CHUNK_HEADER_SIZE bytes
                // 2. last chunk marker: single Long, 8 bytes
                bufStart = Unsafe.malloc(bufSize + MAX_CHUNK_HEADER_SIZE + Long.BYTES, MemoryTag.NATIVE_HTTP_CONN);
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

        long getWriteAddress(long size) {
            assert _wptr != 0;
            long available = getWriteNAvailable();
            if (available >= size) {
                return _wptr;
            }
            throw NoSpaceLeftInResponseBufferException.instance(size, available, bufSize);
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
                // When len=0 and addEofChunk=true, skip writing zero-length chunk header to avoid
                // duplicate: \r\n00\r\n (header) + \r\n00\r\n\r\n (EOF) = \r\n00\r\n\r\n00\r\n\r\n
                // If this 14-byte sequence is split across packets (e.g., 10 bytes + 4 bytes), the client
                // may see \r\n00\r\n\r\n as a complete termination and stop reading, leaving the remaining
                // 00\r\n\r\n in the socket buffer, which pollutes the next HTTP request.
                // The EOF chunk already contains the complete termination sequence.
                if (len > 0 || !addEofChunk) {
                    int padding = len == 0 ? 6 : (Integer.numberOfLeadingZeros(len) >> 3) << 1;
                    long tmp = _wptr;
                    _rptr = _wptr = bufStart + padding;
                    putEOL();
                    Numbers.appendHex(this, len);
                    putEOL();
                    _wptr = tmp;
                } else {
                    // len=0 && addEofChunk=true: skip chunk header, set read pointer to data start
                    // where EOF chunk will be written
                    _rptr = bufStartOfData;
                }
            }
            if (addEofChunk) {
                // safety: unchecked store, but we reserve space for the last chunk -> this is sound as long as all
                //         other writes use getWriteAddress() which does not allow anyone else to use the space reserved
                //         for the last chunk.
                Unsafe.getUnsafe().putLong(_wptr, EOF_CHUNK_LONG_BE);
                _wptr += Long.BYTES;
                LOG.debug().$("end chunk sent [fd=").$(getFd()).I$();
            }
        }

        void write64BitZeroPadding() {
            Unsafe.getUnsafe().putLong(bufStartOfData - 8, 0);
            Unsafe.getUnsafe().putLong(_wptr, 0);
        }
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl implements HttpChunkedResponse {
        private long bookmark = 0L;

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
            if (bookmark != 0) {
                buffer._wptr = bookmark;
                return bookmark != buffer.bufStartOfData;
            }
            return false;
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
        public void status(CharSequence httpProtocolVersion, int code, CharSequence contentType, long contentLength) {
            this.code = code;
            Utf8Sequence status = httpStatusMap.get(code);
            if (status == null) {
                throw new IllegalArgumentException("Illegal status code: " + code);
            }
            buffer.clearAndPrepareToWriteToBuffer();
            putAscii(httpProtocolVersion).put(code).put(' ').put(status).putEOL();
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
        public Utf8Sink put(double value) {
            if (Numbers.isNull(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value);
        }

        @Override
        public Utf8Sink put(float value) {
            if (Numbers.isNull(value)) {
                putAscii("null");
                return this;
            }
            return Utf8Sink.super.put(value);
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
            final Utf8Sequence message = httpJsonStatusMap.get(code);
            sendStatusJsonContent(code, message != null ? message : EMPTY_JSON, true);
        }

        public void sendStatusJsonContent(
                int code,
                @NotNull Utf8Sequence message
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusJsonContent(code, message, true);
        }

        public void sendStatusJsonContent(
                int code,
                @NotNull Utf8Sequence message,
                boolean appendEOL
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            final long contentLength = message.size() + (appendEOL ? 2 : 0);
            assert message.size() > 0 : "json content is missing";
            sendStatusWithContent(CONTENT_TYPE_JSON, code, message, null, null, null, contentLength, appendEOL);
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

        public void sendStatusNoContent(int code, @NotNull Utf8Sequence header) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (!headerSent) {
                buffer.clearAndPrepareToWriteToBuffer();
                headerImpl.status(httpVersion, code, null, -2L);
                headerImpl.put(header).put(Misc.EOL);
                prepareHeaderSink();
                headerSent = true;
            }
            flushSingle();
        }

        public void sendStatusNoContent(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusNoContent(code, (CharSequence) null);
        }

        /**
         * Sends "text/plain" content type response with customised message and
         * optional additional header and cookie.
         *
         * @param code         response code, has to be compatible with "text" response type
         * @param message      optional message, if not provided, a standard message for the response code will be used
         * @param header       optional header
         * @param cookieNames  optional cookie names, cookie names and values must be provided in pairs
         * @param cookieValues optional cookie values
         * @throws PeerDisconnectedException exception if HTTP client disconnects during us sending
         * @throws PeerIsSlowToReadException exception if HTTP client does not keep up with us sending
         */
        public void sendStatusTextContent(
                int code,
                @Nullable Utf8Sequence message,
                @Nullable CharSequence header,
                @Nullable ObjList<CharSequence> cookieNames,
                @Nullable ObjList<CharSequence> cookieValues
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusWithContent(CONTENT_TYPE_TEXT, code, message, header, cookieNames, cookieValues, -1L, true);
        }

        public void sendStatusTextContent(
                int code,
                Utf8Sequence message,
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

        public void sendStatusWithCookie(
                int code,
                Utf8Sequence message,
                @Nullable ObjList<CharSequence> cookieNames,
                @Nullable ObjList<CharSequence> cookieValues
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            sendStatusTextContent(code, message, null, cookieNames, cookieValues);
        }

        public void shutdownWrite() {
            socket.shutdown(Net.SHUT_WR);
        }

        private void sendStatusWithContent(
                String contentType,
                int code,
                @Nullable Utf8Sequence message,
                @Nullable CharSequence header,
                @Nullable ObjList<CharSequence> cookieNames,
                @Nullable ObjList<CharSequence> cookieValues,
                long contentLength,
                boolean appendEOL
        ) throws PeerDisconnectedException, PeerIsSlowToReadException {
            if (!headerSent) {
                buffer.clearAndPrepareToWriteToBuffer();
                headerImpl.status(httpVersion, code, contentType, contentLength);
                if (header != null) {
                    headerImpl.put(header).put(Misc.EOL);
                }
                if (cookieNames != null) {
                    if (cookieValues == null) {
                        throw CairoException.critical(0)
                                .put("Cookie values are missing [namesCount=").put(cookieNames.size())
                                .put(", cookieValues=null]");
                    }
                    if (cookieValues.size() != cookieNames.size()) {
                        throw CairoException.critical(0)
                                .put("The number of cookie names and values are not matching [namesCount=").put(cookieNames.size())
                                .put(", valuesCount=").put(cookieValues.size())
                                .put(']');
                    }
                    for (int i = 0, n = cookieNames.size(); i < n; i++) {
                        setCookie(cookieNames.getQuick(i), cookieValues.getQuick(i));
                    }
                }
                prepareHeaderSink();
                headerSent = true;
            }

            if (!contentSent) {
                flushSingle();
                buffer.clearAndPrepareToWriteToBuffer();
                sink.put(message == null ? httpStatusMap.get(code) : message);
                if (appendEOL) {
                    sink.putEOL();
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
        httpStatusMap.put(HTTP_OK, new Utf8String("OK"));
        httpStatusMap.put(HTTP_NO_CONTENT, new Utf8String("OK"));
        httpStatusMap.put(HTTP_PARTIAL, new Utf8String("Partial content"));
        httpStatusMap.put(HTTP_MOVED_PERM, new Utf8String("Moved Permanently"));
        httpStatusMap.put(HTTP_MOVED_TEMP, new Utf8String("Temporarily Moved"));
        httpStatusMap.put(HTTP_NOT_MODIFIED, new Utf8String("Not Modified"));
        httpStatusMap.put(HTTP_BAD_REQUEST, new Utf8String("Bad request"));
        httpStatusMap.put(HTTP_UNAUTHORIZED, new Utf8String("Unauthorized"));
        httpStatusMap.put(HTTP_FORBIDDEN, new Utf8String("Forbidden"));
        httpStatusMap.put(HTTP_NOT_FOUND, new Utf8String("Not Found"));
        httpStatusMap.put(HTTP_BAD_METHOD, new Utf8String("Method Not Allowed"));
        httpStatusMap.put(HTTP_CONFLICT, new Utf8String("Conflict"));
        httpStatusMap.put(HTTP_CLIENT_TIMEOUT, new Utf8String("Request Timeout"));
        httpStatusMap.put(HTTP_LENGTH_REQUIRED, new Utf8String("Length Required"));
        httpStatusMap.put(HTTP_ENTITY_TOO_LARGE, new Utf8String("Content Too Large"));
        httpStatusMap.put(HTTP_UNSUPPORTED_TYPE, new Utf8String("Bad request"));
        httpStatusMap.put(HTTP_RANGE_NOT_SATISFIABLE, new Utf8String("Request range not satisfiable"));
        httpStatusMap.put(HTTP_REQUEST_HEADER_FIELDS_TOO_LARGE, new Utf8String("Headers too large"));
        httpStatusMap.put(HTTP_INTERNAL_ERROR, new Utf8String("Internal server error"));
        httpStatusMap.put(HTTP_MISDIRECTED_REQUEST, new Utf8String("Misdirected Request"));
        httpStatusMap.put(HTTP_SERVICE_UNAVAILABLE, new Utf8String("Service Unavailable"));
        httpStatusMap.put(HTTP_TOO_MANY_REQUESTS, new Utf8String("Too Many Requests"));
    }

    static {
        httpJsonStatusMap.put(HTTP_OK, new Utf8String("{\"status\":\"OK\"}"));
        httpJsonStatusMap.put(HTTP_UNAUTHORIZED, new Utf8String("{\"status\":\"Unauthorized\"}"));
        httpJsonStatusMap.put(HTTP_FORBIDDEN, new Utf8String("{\"status\":\"Forbidden\"}"));
    }
}
