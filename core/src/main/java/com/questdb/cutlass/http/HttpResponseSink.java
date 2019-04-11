/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.network.NetworkFacade;
import com.questdb.std.*;
import com.questdb.std.ex.ZLibException;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectUnboundedByteSink;

import java.io.Closeable;
import java.nio.ByteBuffer;

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
    private final long out;
    private final long outPtr;
    private final long limit;
    private final long chunkHeaderBuf;
    private final DirectUnboundedByteSink chunkSink;
    private final HttpResponseHeaderSink headerSink;
    private final SimpleResponseImpl simple = new SimpleResponseImpl();
    private final ResponseSinkImpl sink = new ResponseSinkImpl();
    private final FixedSizeResponseImpl fixedSize = new FixedSizeResponseImpl();
    private final ChunkedResponseImpl chunkedResponse = new ChunkedResponseImpl();
    private final DirectBufferResponse directBufferResponse = new DirectBufferResponse();
    private final NetworkFacade nf;
    private final int responseBufferSize;
    private final long fd;
    private final HeaderOnlyResponse headerOnlyResponse = new HeaderOnlyResponse();
    private long _wPtr;
    private ByteBuffer zout;
    private long flushBuf;
    private int flushBufSize;
    private int state;
    private long z_streamp = 0;
    private boolean compressed = false;
    private long pzout;
    private int crc = 0;
    private long total = 0;
    private boolean header = true;

    public HttpResponseSink(HttpServerConfiguration configuration, long fd) {
        this.responseBufferSize = Numbers.ceilPow2(configuration.getConnectionSendBufferSize());
        this.nf = configuration.getDispatcherConfiguration().getNetworkFacade();
        this.out = Unsafe.calloc(responseBufferSize);
        this.headerSink = new HttpResponseHeaderSink(1024, configuration.getClock());
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeaderBuf = Unsafe.calloc(8 + 2 * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedByteSink(chunkHeaderBuf);
        this.chunkSink.put(Misc.EOL);
        this.outPtr = this._wPtr = out;
        this.limit = outPtr + responseBufferSize;
        this.fd = fd;
    }

    @Override
    public void clear() {
        Unsafe.getUnsafe().setMemory(out, responseBufferSize, (byte) 0);
        headerSink.clear();
        this._wPtr = outPtr;
        if (zout != null) {
            zout.clear();
        }
        resetZip();
    }

    @Override
    public void close() {
        Unsafe.free(out, responseBufferSize);
        Unsafe.free(chunkHeaderBuf, 8 + 2 * Misc.EOL.length());
        headerSink.close();
        ByteBuffers.release(zout);
        if (z_streamp != 0) {
            Zip.deflateEnd(z_streamp);
            z_streamp = 0;
        }
    }

    public int getCode() {
        return headerSink.getCode();
    }

    public DirectBufferResponse getDirectBufferResponse() {
        return directBufferResponse;
    }

    public HeaderOnlyResponse getHeaderOnlyResponse() {
        return headerOnlyResponse;
    }

    public SimpleResponseImpl getSimple() {
        return simple;
    }

    public void resume() {
        machine0();
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
            zout.position(0);
        } else {
            zout.position(Zip.gzipHeaderLen);
        }

        // trailer
        if (flush && ret == 1) {
            Unsafe.getUnsafe().putInt(p + len, crc); // crc
            Unsafe.getUnsafe().putInt(p + len + 4, (int) total); // total
            zout.limit(Zip.gzipHeaderLen + len + 8);
        } else {
            zout.limit(Zip.gzipHeaderLen + len);
        }

        // first we need to flush chunk header
        prepareChunk(zout.remaining());

        // if there is input remaining, don't change
        return flush && ret == 1 ? SEND_DEFLATED_END : SEND_DEFLATED_CONT;
    }

    private void flush() {
        int sent = 0;
        while (sent < flushBufSize) {
            int n = nf.send(fd, flushBuf + sent, flushBufSize - sent);
            if (n < 0) {
                // disconnected
                LOG.info().$("disconnected [errno=").$(Os.errno()).$(']').$();
                throw PeerDisconnectedException.INSTANCE;
            }
            if (n == 0) {
                // test how many times we tried to send before parking up
                flushBuf += sent;
                flushBufSize -= sent;
                throw PeerIsSlowException.INSTANCE;
            } else {
                sent += n;
            }
        }
    }

    private void flushSingle() {
        state = DONE;
        flush();
    }

    private void machine(int nextState) {
        state = nextState;
        machine0();
    }

    private void machine0() {
        while (true) {

            if (flushBufSize > 0) {
                flush();
            }

            switch (state) {
                case MULTI_CHUNK:
                    if (compressed) {
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
                    flushBuf = ByteBuffers.getAddress(zout) + zout.position();
                    flushBufSize = zout.limit() - zout.position();
                    state = END_CHUNK;
                    break;
                case SEND_DEFLATED_CONT:
                    flushBuf = ByteBuffers.getAddress(zout) + zout.position();
                    flushBufSize = zout.limit() - zout.position();
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
            zout = ByteBuffer.allocateDirect(responseBufferSize);
            pzout = ByteBuffers.getAddress(zout);
        }
        int r = (int) (_wPtr - outPtr);
        Zip.setInput(z_streamp, outPtr, r);
        this.crc = Zip.crc32(this.crc, outPtr, r);
        this.total += r;
        _wPtr = outPtr;
    }

    private void prepareHeaderSink() {
        ByteBuffer that = headerSink.prepareBuffer();
        flushBuf = ByteBuffers.getAddress(that);
        flushBufSize = that.limit();
    }

    private void resetZip() {
        if (z_streamp != 0) {
            Zip.deflateReset(z_streamp);
        }
        this.crc = 0;
        this.total = 0;
    }

    public class SimpleResponseImpl {

        public void send(int code) {
            send(code, null);
        }

        public void send(int code, CharSequence message) {
            final String std = headerSink.status(code, "text/html; charset=utf-8", -1L);
            sink.put(message == null ? std : message).put(Misc.EOL);
            prepareHeaderSink();
            machine(CHUNK_HEAD);
        }

        public void sendEmptyBody(int code) {
            headerSink.status(code, "text/html; charset=utf-8", -2);
            prepareHeaderSink();
            flushSingle();
        }
    }

    private class ResponseSinkImpl extends AbstractCharSink {

        @Override
        public void flush() {
            prepareHeaderSink();
            machine(CHUNK_HEAD);
        }

        @Override
        public CharSink put(CharSequence seq) {
            int len = seq.length();
            long p = _wPtr;
            if (p + len < limit) {
                Chars.strcpy(seq, len, p);
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
        public CharSink put(float value, int scale) {
            if (value == value) {
                return super.put(value, scale);
            }
            put("null");
            return this;
        }

        @Override
        public CharSink put(double value, int scale) {
            if (value == value) {
                return super.put(value, scale);
            }
            put("null");
            return this;
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
            headerSink.status(status, contentType, -1);
        }

        private void escapeSpace(char c) {
            switch (c) {
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

    public class FixedSizeResponseImpl {

        public void done() {
        }

        public CharSink headers() {
            return headerSink;
        }

        public void sendChunk() {
            flushBuf = out;
            flushBufSize = 0;
            flushSingle();
        }

        public void sendHeader() {
            ByteBuffer that = headerSink.prepareBuffer();
            flushBuf = ByteBuffers.getAddress(that);
            flushBufSize = that.limit();
            flushSingle();
        }

        public void status(int status, CharSequence contentType, long len) {
            headerSink.status(status, contentType, len);
        }
    }

    public class DirectBufferResponse {

        public long getBuffer() {
            return out;
        }

        public int getBufferSize() {
            return responseBufferSize;
        }

        public CharSink headerSink() {
            return headerSink;
        }

        public void send(int size) {
            flushBuf = out;
            flushBufSize = size;
            flushSingle();
        }

        public void sendHeader() {
            ByteBuffer that = headerSink.prepareBuffer();
            flushBuf = ByteBuffers.getAddress(that);
            flushBufSize = that.limit();
            flushSingle();
        }

        public void status(int status, CharSequence contentType, long len) {
            headerSink.status(status, contentType, len);
        }
    }

    public class HeaderOnlyResponse {
        public HttpResponseHeaderSink getHeaderSink() {
            return headerSink;
        }

        public void send() {
            ByteBuffer that = headerSink.prepareBuffer();
            flushBuf = ByteBuffers.getAddress(that);
            flushBufSize = that.limit();
            flushSingle();
        }
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl {

        private long bookmark = outPtr;

        public void bookmark() {
            bookmark = _wPtr;
        }

        public void done() {
            flushBufSize = 0;
            if (compressed) {
                machine(FLUSH);
            } else {
                machine(END_CHUNK);
            }
        }

        @Override
        public void flush() {
            sendChunk();
        }

        @Override
        public void status(int status, CharSequence contentType) {
            super.status(status, contentType);
            if (compressed) {
                headerSink.put("Content-Encoding: gzip").put(Misc.EOL);
            }
        }

        public CharSink headers() {
            return headerSink;
        }

        public boolean resetToBookmark() {
            _wPtr = bookmark;
            return bookmark != outPtr;
        }

        public void sendChunk() {
            if (outPtr != _wPtr) {
                if (compressed) {
                    flushBufSize = 0;
                    machine(MULTI_CHUNK);
                } else {
                    prepareChunk((int) (_wPtr - outPtr));
                    machine(MULTI_CHUNK);
                }
            }
        }

        public void sendHeader() {
            ByteBuffer that = headerSink.prepareBuffer();
            flushBuf = ByteBuffers.getAddress(that);
            flushBufSize = that.limit();
            flushSingle();
        }

        public void setCompressed(boolean compressed) {
            HttpResponseSink.this.compressed = compressed;
        }
    }
}
