/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

package com.questdb.tuck.http;

import com.questdb.ex.ResponseContentBufferTooSmallException;
import com.questdb.ex.ZLibException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.log.LogRecord;
import com.questdb.std.*;
import com.questdb.std.ex.DisconnectedChannelException;
import com.questdb.std.ex.SlowWritableChannelException;
import com.questdb.std.str.AbstractCharSink;
import com.questdb.std.str.CharSink;
import com.questdb.std.str.DirectUnboundedByteSink;
import com.questdb.std.time.MillisecondClock;
import com.questdb.tuck.NonBlockingSecureSocketChannel;
import com.questdb.tuck.ServerConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Response implements Closeable, Mutable {
    private static final Log LOG = LogFactory.getLog(Response.class);
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
    private final ByteBuffer out;
    private final long outPtr;
    private final long limit;
    private final ByteBuffer chunkHeader;
    private final DirectUnboundedByteSink chunkSink;
    private final ResponseHeaderBuffer hb;
    private final WritableByteChannel channel;
    private final SimpleResponse simple = new SimpleResponseImpl();
    private final ResponseSink sink = new ResponseSinkImpl();
    private final FixedSizeResponse fixedSize = new FixedSizeResponseImpl();
    private final ChunkedResponse chunkedResponse = new ChunkedResponseImpl();
    private final int sz;
    private long _wPtr;
    private ByteBuffer zout;
    private ByteBuffer _flushBuf;
    private int state;
    private long z_streamp = 0;
    private boolean compressed = false;
    private long pzout;
    private int crc = 0;
    private long total = 0;
    private boolean header = true;

    public Response(WritableByteChannel channel, ServerConfiguration configuration, MillisecondClock clock) {
        if (configuration.getHttpBufRespHeader() <= 0) {
            throw new IllegalArgumentException("headerBufferSize");
        }

        if (configuration.getHttpBufRespContent() <= 0) {
            throw new IllegalArgumentException("contentBufferSize");
        }

        this.channel = channel;
        this.sz = Numbers.ceilPow2(configuration.getHttpBufRespContent());
        this.out = ByteBuffer.allocateDirect(sz);
        this.hb = new ResponseHeaderBuffer(configuration.getHttpBufRespHeader(), clock);
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeader = ByteBuffer.allocateDirect(8 + 2 * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedByteSink(ByteBuffers.getAddress(chunkHeader));
        this.chunkSink.put(Misc.EOL);
        this.outPtr = this._wPtr = ByteBuffers.getAddress(out);
        this.limit = outPtr + sz;
    }

    @Override
    public void clear() {
        out.clear();
        hb.clear();
        this._wPtr = outPtr;
        if (zout != null) {
            zout.clear();
        }
        resetZip();
    }

    @Override
    public void close() {
        ByteBuffers.release(out);
        ByteBuffers.release(chunkHeader);
        hb.close();
        ByteBuffers.release(zout);
        if (z_streamp != 0) {
            Zip.deflateEnd(z_streamp);
            z_streamp = 0;
        }
    }

    public int getCode() {
        return hb.getCode();
    }

    public void resume() throws DisconnectedChannelException, SlowWritableChannelException {
        machine0();
    }

    final ChunkedResponse asChunked() {
        return chunkedResponse;
    }

    final FixedSizeResponse asFixedSize() {
        return fixedSize;
    }

    final SimpleResponse asSimple() {
        return simple;
    }

    final ResponseSink asSink() {
        return sink;
    }

    private int deflate(boolean flush) throws DisconnectedChannelException {

        final int sz = this.sz - 8;
        long p = pzout + Zip.gzipHeaderLen;

        int ret;
        int len;
        int availIn;
        // compress input until we run out of either input or output
        do {
            ret = Zip.deflate(z_streamp, p, sz, flush);
            if (ret < 0) {
                LOG.error().$("ZLib error: ").$(ret).$();
                throw DisconnectedChannelException.INSTANCE;
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
        _flushBuf = prepareChunk(zout.remaining());

        // if there is input remaining, don't change
        return flush && ret == 1 ? SEND_DEFLATED_END : SEND_DEFLATED_CONT;
    }

    private void flush(ByteBuffer buf) throws DisconnectedChannelException, SlowWritableChannelException {
        this._flushBuf = buf;
        ByteBuffers.copyNonBlocking(buf, channel, channel instanceof NonBlockingSecureSocketChannel ? 1 : IOHttpJob.SO_WRITE_RETRY_COUNT);
        buf.clear();
        this._flushBuf = null;
    }

    private void flushSingle(ByteBuffer buf) throws DisconnectedChannelException, SlowWritableChannelException {
        state = DONE;
        flush(buf);
    }

    private void machine(ByteBuffer buf, int nextState) throws DisconnectedChannelException, SlowWritableChannelException {
        _flushBuf = buf;
        state = nextState;
        machine0();
    }

    private void machine0() throws DisconnectedChannelException, SlowWritableChannelException {
        while (true) {

            if (_flushBuf != null) {
                flush(_flushBuf);
            }

            switch (state) {
                case MULTI_CHUNK:
                    if (compressed) {
                        prepareCompressedBody();
                        state = DEFLATE;
                    } else {
                        _flushBuf = prepareBody();
                        state = DONE;
                    }
                    break;
                case DEFLATE:
                    state = deflate(false);
                    break;
                case SEND_DEFLATED_END:
                    _flushBuf = zout;
                    state = END_CHUNK;
                    break;
                case SEND_DEFLATED_CONT:
                    _flushBuf = zout;
                    state = DONE;
                    break;
                case MULTI_BUF_CHUNK:
                    _flushBuf = out;
                    state = DONE;
                    break;
                case CHUNK_HEAD:
                    _flushBuf = prepareChunk((int) (_wPtr - outPtr));
                    state = CHUNK_DATA;
                    break;
                case CHUNK_DATA:
                    _flushBuf = prepareBody();
                    state = END_CHUNK;
                    break;
                case END_CHUNK:
                    _flushBuf = prepareChunk(0);
                    state = FIN;
                    break;
                case FIN:
                    sink.put(Misc.EOL);
                    _flushBuf = prepareBody();
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

    private ByteBuffer prepareBody() {
        out.limit((int) (_wPtr - outPtr));
        _wPtr = outPtr;
        return out;
    }

    private ByteBuffer prepareChunk(int len) {
        chunkSink.clear(Misc.EOL.length());
        Numbers.appendHex(chunkSink, len);
        chunkSink.put(Misc.EOL);
        chunkHeader.limit(chunkSink.length());
        return chunkHeader;
    }

    private void prepareCompressedBody() {
        if (z_streamp == 0) {
            z_streamp = Zip.deflateInit();
            zout = ByteBuffer.allocateDirect(sz);
            pzout = ByteBuffers.getAddress(zout);
        }
        int r = (int) (_wPtr - outPtr);
        Zip.setInput(z_streamp, outPtr, r);
        this.crc = Zip.crc32(this.crc, outPtr, r);
        this.total += r;
        _wPtr = outPtr;
    }

    private void resetZip() {
        if (z_streamp != 0) {
            Zip.deflateReset(z_streamp);
        }
        this.crc = 0;
        this.total = 0;
    }

    private class SimpleResponseImpl implements SimpleResponse {

        public void send(int code) throws DisconnectedChannelException, SlowWritableChannelException {
            send(code, null);
        }

        @Override
        public void send(int code, CharSequence message) throws DisconnectedChannelException, SlowWritableChannelException {

            final LogRecord log = LOG.info().$("Sending ").$(code);
            if (message != null) {
                log.$(": ").$(message);
            }
            log.$();

            final String std = hb.status(code, "text/html; charset=utf-8", -1L);
            sink.put(message == null ? std : message).put(Misc.EOL);
            machine(hb.prepareBuffer(), CHUNK_HEAD);
        }

        @Override
        public void sendEmptyBody(int code) throws DisconnectedChannelException, SlowWritableChannelException {
            hb.status(code, "text/html; charset=utf-8", -2);
            flushSingle(hb.prepareBuffer());
        }
    }

    private class ResponseSinkImpl extends AbstractCharSink implements ResponseSink {

        @Override
        public void flush() throws IOException {
            machine(hb.prepareBuffer(), CHUNK_HEAD);
        }

        @Override
        public CharSink put(CharSequence seq) {
            int len = seq.length();
            long p = _wPtr;
            if (p + len < limit) {
                Chars.strcpy(seq, len, p);
                _wPtr = p + len;
            } else {
                throw ResponseContentBufferTooSmallException.INSTANCE;
            }
            return this;
        }

        @Override
        public CharSink put(char c) {
            if (_wPtr < limit) {
                Unsafe.getUnsafe().putByte(_wPtr++, (byte) c);
                return this;
            }
            throw ResponseContentBufferTooSmallException.INSTANCE;
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

        @Override
        public void status(int status, CharSequence contentType) {
            hb.status(status, contentType, -1);
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

    private class FixedSizeResponseImpl implements FixedSizeResponse {

        @Override
        public void done() {
        }

        @Override
        public CharSink headers() {
            return hb;
        }

        @Override
        public ByteBuffer out() {
            return out;
        }

        @Override
        public void sendChunk() throws DisconnectedChannelException, SlowWritableChannelException {
            flushSingle(out);
        }

        @Override
        public void sendHeader() throws DisconnectedChannelException, SlowWritableChannelException {
            flushSingle(hb.prepareBuffer());
        }

        @Override
        public void status(int status, CharSequence contentType, long len) {
            hb.status(status, contentType, len);
        }
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl implements ChunkedResponse {

        private long bookmark = outPtr;

        @Override
        public void bookmark() {
            bookmark = _wPtr;
        }

        @Override
        public boolean resetToBookmark() {
            _wPtr = bookmark;
            return bookmark != outPtr;
        }

        @Override
        public void setCompressed(boolean compressed) {
            Response.this.compressed = compressed;
        }

        @Override
        public void done() throws DisconnectedChannelException, SlowWritableChannelException {
            if (compressed) {
                machine(null, FLUSH);
            } else {
                machine(null, END_CHUNK);
            }
        }

        @Override
        public CharSink headers() {
            return hb;
        }

        @Override
        public ByteBuffer out() {
            return out;
        }

        @Override
        public void sendChunk() throws DisconnectedChannelException, SlowWritableChannelException {
            if (outPtr != _wPtr) {
                if (compressed) {
                    machine(null, MULTI_CHUNK);
                } else {
                    machine(prepareChunk((int) (_wPtr - outPtr)), MULTI_CHUNK);
                }
            }
        }

        @Override
        public void sendHeader() throws DisconnectedChannelException, SlowWritableChannelException {
            flushSingle(hb.prepareBuffer());
        }

        @Override
        public void flush() throws IOException {
            sendChunk();
        }

        @Override
        public void status(int status, CharSequence contentType) {
            super.status(status, contentType);
            if (compressed) {
                hb.put("Content-Encoding: gzip").put(Misc.EOL);
            }
        }
    }
}
