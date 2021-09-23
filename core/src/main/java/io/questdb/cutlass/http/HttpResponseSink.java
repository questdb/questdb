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
import io.questdb.std.datetime.millitime.DateFormatUtils;
import io.questdb.std.datetime.millitime.MillisecondClock;
import io.questdb.std.ex.ZLibException;
import io.questdb.std.str.AbstractCharSink;
import io.questdb.std.str.CharSink;
import io.questdb.std.str.StdoutSink;

import java.io.Closeable;

public class HttpResponseSink implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(HttpResponseSink.class);
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

    private final ChunkBuffer buffer;
    private ChunkBuffer compressOutBuffer;
    private final HttpResponseHeaderImpl headerImpl;
    private final SimpleResponseImpl simple = new SimpleResponseImpl();
    private final ResponseSinkImpl sink = new ResponseSinkImpl();
    private final ChunkedResponseImpl chunkedResponse = new ChunkedResponseImpl();
    private final HttpRawSocketImpl rawSocket = new HttpRawSocketImpl();
    private final NetworkFacade nf;
    private final int responseBufferSize;
    private final boolean dumpNetworkTraffic;
    private final String httpVersion;
    private long fd;
    private long z_streamp = 0;
    private boolean deflateBeforeSend = false;
    private int crc = 0;
    private long total = 0;
    private long totalBytesSent = 0;
    private final boolean connectionCloseHeader;
    private boolean headersSent;
    private boolean chunkedRequestDone;
    private boolean compressedHeaderDone;
    private boolean compressedOutputReady;
    private boolean compressionComplete;

    public HttpResponseSink(HttpContextConfiguration configuration) {
        this.responseBufferSize = Numbers.ceilPow2(configuration.getSendBufferSize());
        this.nf = configuration.getNetworkFacade();
        this.buffer = new ChunkBuffer(responseBufferSize);
        this.headerImpl = new HttpResponseHeaderImpl(configuration.getClock());
        this.dumpNetworkTraffic = configuration.getDumpNetworkTraffic();
        this.httpVersion = configuration.getHttpVersion();
        this.connectionCloseHeader = !configuration.getServerKeepAlive();
    }

    public HttpChunkedResponseSocket getChunkedSocket() {
        return chunkedResponse;
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
        if (z_streamp != 0) {
            Zip.deflateEnd(z_streamp);
            z_streamp = 0;
            compressOutBuffer.close();
            compressOutBuffer = null;
        }
        buffer.close();
    }

    public void setDeflateBeforeSend(boolean deflateBeforeSend) {
        this.deflateBeforeSend = deflateBeforeSend;
        if (z_streamp == 0 && deflateBeforeSend) {
            z_streamp = Zip.deflateInit();
            compressOutBuffer = new ChunkBuffer(responseBufferSize);
        }
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

    private void deflate() {
        if (!compressedHeaderDone) {
            int len = Zip.gzipHeaderLen;
            Vect.memcpy(Zip.gzipHeader, compressOutBuffer.getWriteAddress(len), len);
            compressOutBuffer.onWrite(len);
            compressedHeaderDone = true;
        }

        int nInAvailable = (int) buffer.getReadNAvailable();
        if (nInAvailable > 0) {
            long inAddress = buffer.getReadAddress();
            LOG.debug().$("Zip.setInput [inAddress=").$(inAddress).$(", nInAvailable=").$(nInAvailable).$(']').$();
            buffer.write64BitZeroPadding();
            Zip.setInput(z_streamp, inAddress, nInAvailable);
        }

        int ret;
        int len;
        // compress input until we run out of either input or output
        do {
            int sz = (int) compressOutBuffer.getWriteNAvailable() - 8;
            long p = compressOutBuffer.getWriteAddress(0);
            LOG.debug().$("deflate starting [p=").$(p).$(", sz=").$(sz).$(", chunkedRequestDone=").$(chunkedRequestDone).$(']').$();
            ret = Zip.deflate(z_streamp, p, sz, chunkedRequestDone);
            len = sz - Zip.availOut(z_streamp);
            compressOutBuffer.onWrite(len);
            if (ret < 0) {
                // This is not an error, zlib just couldn't do any work with the input/output buffers it was provided.
                // This happens often (will depend on output buffer size) when there is no new input and zlib has finished generating
                // output from previously provided input
                if (ret != Zip.Z_BUF_ERROR || len != 0) {
                    throw HttpException.instance("could not deflate [ret=").put(ret);
                }
            }

            int availIn = Zip.availIn(z_streamp);
            int nInConsumed = nInAvailable - availIn;
            if (nInConsumed > 0) {
                this.crc = Zip.crc32(this.crc, buffer.getReadAddress(), nInConsumed);
                this.total += nInConsumed;
                buffer.onRead(nInConsumed);
                nInAvailable = availIn;
            }

            LOG.debug().$("deflate finished [ret=").$(ret).$(", len=").$(len).$(", availIn=").$(availIn).$(']').$();
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

    private void flushSingle() throws PeerDisconnectedException, PeerIsSlowToReadException {
        sendBuffer(buffer);
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

    private void prepareHeaderSink() {
        buffer.prepareToReadFromBuffer(false, false);
        headerImpl.prepareToSend();
    }

    private void resetZip() {
        if (z_streamp != 0) {
            Zip.deflateReset(z_streamp);
            compressOutBuffer.clear();
            this.crc = 0;
            this.total = 0;
            compressedHeaderDone = false;
            compressedOutputReady = false;
            compressionComplete = false;
        }
    }

    private void sendBuffer(ChunkBuffer sendBuf) throws PeerDisconnectedException, PeerIsSlowToReadException {
        int nSend = (int) sendBuf.getReadNAvailable();
        while (nSend > 0) {
            int n = nf.send(fd, sendBuf.getReadAddress(), nSend);
            if (n < 0) {
                // disconnected
                LOG.error()
                        .$("disconnected [errno=").$(nf.errno())
                        .$(", fd=").$(fd)
                        .$(']').$();
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

    private void dumpBuffer(long buffer, int size) {
        if (dumpNetworkTraffic && size > 0) {
            StdoutSink.INSTANCE.put('<');
            Net.dump(buffer, size);
        }
    }

    long getTotalBytesSent() {
        return totalBytesSent;
    }

    public class HttpResponseHeaderImpl extends AbstractCharSink implements Mutable, HttpResponseHeader {
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
        public CharSink put(CharSequence cs) {
            int len = cs.length();
            Chars.asciiStrCpy(cs, len, buffer.getWriteAddress(len));
            buffer.onWrite(len);
            return this;
        }

        @Override
        public CharSink put(char c) {
            Unsafe.getUnsafe().putByte(buffer.getWriteAddress(1), (byte) c);
            buffer.onWrite(1);
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void send() throws PeerDisconnectedException, PeerIsSlowToReadException {
            headerImpl.prepareToSend();
            flushSingle();
        }

        @Override
        public String status(CharSequence httpProtocolVersion, int code, CharSequence contentType, long contentLength) {
            this.code = code;
            String status = httpStatusMap.get(code);
            if (status == null) {
                throw new IllegalArgumentException("Illegal status code: " + code);
            }
            buffer.clearAndPrepareToWriteToBuffer();
            put(httpProtocolVersion).put(code).put(' ').put(status).put(Misc.EOL);
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

            if (connectionCloseHeader) {
                put("Connection: close").put(Misc.EOL);
            }

            return status;
        }

        private void prepareToSend() {
            if (!chunky) {
                put(Misc.EOL);
            }
        }

    }

    public class SimpleResponseImpl {

        public void sendStatus(int code, CharSequence message) throws PeerDisconnectedException, PeerIsSlowToReadException {
            buffer.clearAndPrepareToWriteToBuffer();
            final String std = headerImpl.status(httpVersion, code, "text/plain; charset=utf-8", -1L);
            prepareHeaderSink();
            flushSingle();
            buffer.clearAndPrepareToWriteToBuffer();
            sink.put(message == null ? std : message).put(Misc.EOL);
            buffer.prepareToReadFromBuffer(true, true);
            resumeSend();
        }

        public void sendStatus(int code) throws PeerDisconnectedException, PeerIsSlowToReadException {
            buffer.clearAndPrepareToWriteToBuffer();
            headerImpl.status(httpVersion, code, "text/html; charset=utf-8", -2L);
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
            buffer.put(seq);
            return this;
        }

        @Override
        public CharSink put(CharSequence cs, int lo, int hi) {
            buffer.put(cs, lo, hi);
            return this;
        }

        @Override
        public CharSink put(char c) {
            buffer.put(c);
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            buffer.put(chars, start, len);
            return this;
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
            buffer.clearAndPrepareToWriteToBuffer();
            headerImpl.status("HTTP/1.1 ", status, contentType, -1);
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
        public void status(int status, CharSequence contentType) {
            super.status(status, contentType);
            if (deflateBeforeSend) {
                headerImpl.put("Content-Encoding: gzip").put(Misc.EOL);
            }
        }

        @Override
        public void shutdownWrite() {
            nf.shutdown(fd, Net.SHUT_WR);
        }
    }

    private class ChunkBuffer extends AbstractCharSink implements Closeable {
        private static final int MAX_CHUNK_HEADER_SIZE = 12;
        private static final String EOF_CHUNK = "\r\n00\r\n\r\n";
        private long bufStart;
        private final long bufStartOfData;
        private long bufEndOfData;
        private long _wptr;
        private long _rptr;

        private ChunkBuffer(int sz) {
            bufStart = Unsafe.malloc(sz + MAX_CHUNK_HEADER_SIZE + EOF_CHUNK.length(), MemoryTag.NATIVE_HTTP_CONN);
            bufStartOfData = bufStart + MAX_CHUNK_HEADER_SIZE;
            bufEndOfData = bufStartOfData + sz;
            clear();
        }

        @Override
        public void close() {
            if (0 != bufStart) {
                Unsafe.free(bufStart, bufEndOfData - bufStart + EOF_CHUNK.length(), MemoryTag.NATIVE_HTTP_CONN);
                bufStart = bufEndOfData = _wptr = _rptr = 0;
            }
        }

        void clear() {
            _wptr = _rptr = bufStartOfData;
        }

        long getReadAddress() {
            assert _rptr != 0;
            return _rptr;
        }

        long getReadNAvailable() {
            return _wptr - _rptr;
        }

        void onRead(int nRead) {
            assert nRead >= 0 && nRead <= getReadNAvailable();
            _rptr += nRead;
        }

        long getWriteAddress(int len) {
            assert _wptr != 0;
            if (getWriteNAvailable() >= len) {
                return _wptr;
            }
            throw NoSpaceLeftInResponseBufferException.INSTANCE;
        }

        long getWriteNAvailable() {
            return bufEndOfData - _wptr;
        }

        void onWrite(int nWrite) {
            assert nWrite >= 0 && nWrite <= getWriteNAvailable();
            _wptr += nWrite;
        }

        void write64BitZeroPadding() {
            Unsafe.getUnsafe().putLong(bufStartOfData - 8, 0);
            Unsafe.getUnsafe().putLong(_wptr, 0);
        }

        @Override
        public CharSink put(CharSequence cs) {
            int len = cs.length();
            Chars.asciiStrCpy(cs, len, getWriteAddress(len));
            onWrite(len);
            return this;
        }

        @Override
        public CharSink put(char c) {
            Unsafe.getUnsafe().putByte(getWriteAddress(1), (byte) c);
            onWrite(1);
            return this;
        }

        @Override
        public CharSink put(char[] chars, int start, int len) {
            Chars.asciiCopyTo(chars, start, len, getWriteAddress(len));
            onWrite(len);
            return this;
        }

        @Override
        public CharSink put(CharSequence cs, int lo, int hi) {
            int len = hi - lo;
            Chars.asciiStrCpy(cs, lo, len, getWriteAddress(len));
            onWrite(len);
            return this;
        }

        void clearAndPrepareToWriteToBuffer() {
            _rptr = _wptr = bufStartOfData;
        }

        void prepareToReadFromBuffer(boolean addChunkHeader, boolean addEofChunk) {
            if (addChunkHeader) {
                int len = (int) (_wptr - bufStartOfData);
                int padding = len == 0 ? 6 : (Integer.numberOfLeadingZeros(len) >> 3) << 1;
                long tmp = _wptr;
                _rptr = _wptr = bufStart + padding;
                put(Misc.EOL);
                Numbers.appendHex(this, len);
                put(Misc.EOL);
                _wptr = tmp;
            }
            if (addEofChunk) {
                int len = EOF_CHUNK.length();
                Chars.asciiStrCpy(EOF_CHUNK, len, _wptr);
                _wptr += len;
                LOG.debug().$("end chunk sent [fd=").$(fd).$(']').$();
            }
        }
    }
}
