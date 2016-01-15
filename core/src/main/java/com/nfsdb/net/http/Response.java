/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net.http;

import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.DisconnectedChannelException;
import com.nfsdb.exceptions.ResponseContentBufferTooSmallException;
import com.nfsdb.exceptions.SlowWritableChannelException;
import com.nfsdb.exceptions.ZLibException;
import com.nfsdb.io.sink.AbstractCharSink;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.DirectUnboundedAnsiSink;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Response implements Closeable, Mutable {
    private final ByteBuffer out;
    private final long outPtr;
    private final long limit;
    private final ByteBuffer chunkHeader;
    private final DirectUnboundedAnsiSink chunkSink;
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
    private ResponseState state;
    private long z_streamp = 0;
    private boolean compressed = false;
    private long pzout;
    private int crc = 0;
    private long total = 0;
    private boolean header = true;

    public Response(WritableByteChannel channel, int headerBufferSize, int contentBufferSize, Clock clock) {
        if (headerBufferSize <= 0) {
            throw new IllegalArgumentException("headerBufferSize");
        }

        if (contentBufferSize <= 0) {
            throw new IllegalArgumentException("contentBufferSize");
        }

        this.channel = channel;
        this.sz = Numbers.ceilPow2(contentBufferSize);
        this.out = ByteBuffer.allocateDirect(sz);
        this.hb = new ResponseHeaderBuffer(headerBufferSize, clock);
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeader = ByteBuffer.allocateDirect(8 + 2 * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedAnsiSink(ByteBuffers.getAddress(chunkHeader));
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

    public void resume() throws DisconnectedChannelException, SlowWritableChannelException {
        machine0();
    }

    private ByteBuffer _prepareBody() {
        out.limit((int) (_wPtr - outPtr));
        _wPtr = outPtr;
        return out;
    }

    private ByteBuffer _prepareChunk(int len) {
        chunkSink.clear(Misc.EOL.length());
        Numbers.appendHex(chunkSink, len);
        chunkSink.put(Misc.EOL);
        chunkHeader.limit(chunkSink.length());
        return chunkHeader;
    }

    private void _prepareCompressedBody() throws DisconnectedChannelException, SlowWritableChannelException {
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

    private ResponseState deflate(boolean flush) throws DisconnectedChannelException, SlowWritableChannelException {

        final int sz = this.sz - 8;
        long p = pzout + Zip.gzipHeaderLen;

        int ret;
        int len;
        int availIn;
        // compress input until we run out of either input or output
        do {
            ret = Zip.deflate(z_streamp, p, sz, flush);
            if (ret < 0) {
                // todo: log this event
                throw DisconnectedChannelException.INSTANCE;
            }

            len = sz - Zip.availOut(z_streamp);
            availIn = Zip.availIn(z_streamp);
        } while (len == 0 && availIn > 0);

        // zip did not write anything out, nothing to send
        // we assume that input was too small and needs flushing

        if (len == 0) {
            return ResponseState.DONE;
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
        _flushBuf = _prepareChunk(zout.remaining());

        // if there is input remaining, don't change
        return flush && ret == 1 ? ResponseState.SEND_DEFLATED_END : ResponseState.SEND_DEFLATED_CONT;
    }

    private void flush(ByteBuffer buf) throws DisconnectedChannelException, SlowWritableChannelException {
        this._flushBuf = buf;
        ByteBuffers.copyNonBlocking(buf, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
        buf.clear();
        this._flushBuf = null;
    }

    private void flushSingle(ByteBuffer buf) throws DisconnectedChannelException, SlowWritableChannelException {
        state = ResponseState.DONE;
        flush(buf);
    }

    private void machine(ByteBuffer buf, ResponseState next) throws DisconnectedChannelException, SlowWritableChannelException {
        _flushBuf = buf;
        state = next;
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
                        _prepareCompressedBody();
                        state = ResponseState.DEFLATE;
                    } else {
                        _flushBuf = _prepareBody();
                        state = ResponseState.DONE;
                    }
                    break;
                case DEFLATE:
                    state = deflate(false);
                    break;
                case SEND_DEFLATED_END:
                    _flushBuf = zout;
                    state = ResponseState.END_CHUNK;
                    break;
                case SEND_DEFLATED_CONT:
                    _flushBuf = zout;
                    state = ResponseState.DONE;
                    break;
                case MULTI_BUF_CHUNK:
                    _flushBuf = out;
                    state = ResponseState.DONE;
                    break;
                case CHUNK_HEAD:
                    _flushBuf = _prepareChunk((int) (_wPtr - outPtr));
                    state = ResponseState.CHUNK_DATA;
                    break;
                case CHUNK_DATA:
                    _flushBuf = _prepareBody();
                    state = ResponseState.END_CHUNK;
                    break;
                case END_CHUNK:
                    _flushBuf = _prepareChunk(0);
                    state = ResponseState.FIN;
                    break;
                case FIN:
                    sink.put(Misc.EOL);
                    _flushBuf = _prepareBody();
                    state = ResponseState.DONE;
                    break;
                case FLUSH:
                    state = deflate(true);
                    break;
                case DONE:
                    return;
            }
        }

    }

    private void resetZip() {
        if (z_streamp != 0) {
            Zip.deflateReset(z_streamp);
        }
        this.crc = 0;
        this.total = 0;
    }

    private enum ResponseState {
        CHUNK_HEAD,
        CHUNK_DATA,
        FIN, MULTI_CHUNK,
        DEFLATE,
        MULTI_BUF_CHUNK,
        END_CHUNK,
        DONE,
        FLUSH,
        SEND_DEFLATED_CONT,
        SEND_DEFLATED_END
    }

    private class SimpleResponseImpl implements SimpleResponse {

        public void send(int code) throws DisconnectedChannelException, SlowWritableChannelException {
            send(code, null);
        }

        @Override
        public void send(int code, CharSequence message) throws DisconnectedChannelException, SlowWritableChannelException {
            final String std = hb.status(code, "text/html; charset=utf-8", -1L);
            sink.put(message == null ? std : message).put(Misc.EOL);
            machine(hb.prepareBuffer(), ResponseState.CHUNK_HEAD);
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
            machine(hb.prepareBuffer(), ResponseState.CHUNK_HEAD);
        }

        @Override
        public CharSink put(CharSequence seq) {
            int len = seq.length();
            long p = _wPtr;
            if (p + len < limit) {
                for (int i = 0; i < len; i++) {
                    Unsafe.getUnsafe().putByte(p++, (byte) seq.charAt(i));
                }
                _wPtr = p;
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
        public void status(int status, CharSequence contentType) {
            hb.status(status, contentType, -1);
        }
    }

    private class FixedSizeResponseImpl implements FixedSizeResponse {

        @Override
        public void done() throws DisconnectedChannelException, SlowWritableChannelException {
        }

        @Override
        public void status(int status, CharSequence contentType, long len) {
            hb.status(status, contentType, len);
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
    }

    private class ChunkedResponseImpl extends ResponseSinkImpl implements ChunkedResponse {

        private long bookmark = outPtr;

        @Override
        public long bookmark() {
            return bookmark;
        }

        @Override
        public void resetToBookmark(long bookmark) {
            _wPtr = bookmark;
        }

        @Override
        public void setCompressed(boolean compressed) {
            Response.this.compressed = compressed;
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

        @Override
        public ByteBuffer out() {
            return out;
        }

        @Override
        public void done() throws DisconnectedChannelException, SlowWritableChannelException {
            if (compressed) {
                machine(null, ResponseState.FLUSH);
            } else {
                machine(null, ResponseState.END_CHUNK);
            }
        }

        @Override
        public CharSink headers() {
            return hb;
        }

        @Override
        public void sendChunk() throws DisconnectedChannelException, SlowWritableChannelException {
            if (compressed) {
                machine(null, ResponseState.MULTI_CHUNK);
            } else {
                machine(_prepareChunk((int) (_wPtr - outPtr)), ResponseState.MULTI_CHUNK);
            }
        }

        @Override
        public void sendHeader() throws DisconnectedChannelException, SlowWritableChannelException {
            flushSingle(hb.prepareBuffer());
        }
    }
}
