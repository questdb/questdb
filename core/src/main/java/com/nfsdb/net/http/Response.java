/*
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
 */

package com.nfsdb.net.http;

import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.ResponseContentBufferTooSmallException;
import com.nfsdb.io.sink.AbstractCharSink;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.DirectUnboundedAnsiSink;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Response extends AbstractCharSink implements Closeable, Mutable {
    private final ByteBuffer out;
    private final long outPtr;
    private final long limit;
    private final ByteBuffer chunkHeader;
    private final DirectUnboundedAnsiSink chunkSink;
    private final ResponseHeaderBuffer hb;
    private final WritableByteChannel channel;
    private long _wPtr;
    private ByteBuffer _flushBuf;
    private ResponseState state;
    private boolean fragmented = false;

    public Response(WritableByteChannel channel, int headerBufferSize, int contentBufferSize, Clock clock) {
        if (headerBufferSize <= 0) {
            throw new IllegalArgumentException("headerBufferSize");
        }

        if (contentBufferSize <= 0) {
            throw new IllegalArgumentException("contentBufferSize");
        }

        this.channel = channel;
        int sz = Numbers.ceilPow2(contentBufferSize);
        this.out = ByteBuffer.allocateDirect(sz);
        this.hb = new ResponseHeaderBuffer(headerBufferSize, clock);
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeader = ByteBuffer.allocateDirect(8 + 2 * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedAnsiSink(ByteBuffers.getAddress(chunkHeader));
        this.chunkSink.put(Misc.EOL);
        this.outPtr = this._wPtr = ByteBuffers.getAddress(out);
        this.limit = outPtr + sz;
    }

    public void _continue() throws IOException {
        boolean chunky = hb.isChunky();
        while (true) {
            if (_flushBuf != null) {
                flush(_flushBuf);
            }

            switch (state) {
                case HEADER:
                    if (fragmented) {
                        return;
                    }
                case BODY_PART:
                    if (chunky) {
                        state = ResponseState.CHUNK;
                        _flushBuf = _prepareChunk((int) (_wPtr - outPtr));
                        break;
                    }
                    // fall through
                case CHUNK:
                    if (fragmented) {
                        return;
                    }
                    _flushBuf = _prepareBody();
                    state = ResponseState.BODY;
                    break;
                case BODY:
                    if (chunky) {
                        _flushBuf = _prepareChunk(0);
                        state = ResponseState.END_CHUNK;
                        break;
                    }
                    // fall through
                case END_CHUNK:
                    if (chunky) {
                        put(Misc.EOL);
                        _flushBuf = _prepareBody();
                        state = ResponseState.END_BODY;
                        break;
                    }
                    // fall through
                case END_BODY:
                    return;
            }
        }
    }

    @Override
    public void clear() {
        out.clear();
        hb.clear();
        this._wPtr = outPtr;
    }

    @Override
    public void close() {
        ByteBuffers.release(out);
        ByteBuffers.release(chunkHeader);
        hb.close();
    }

    public void end() throws IOException {
        if (fragmented) {
            return;
        }
        sendHeader();
    }

    public void flush() throws IOException {
    }

    public Response put(CharSequence seq) {
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

    public ByteBuffer getOut() {
        return out;
    }

    public CharSink headers() {
        return hb;
    }

    public void sendBody() throws IOException {
        state = ResponseState.BODY_PART;
        _flushBuf = out;
        _continue();
    }

    public void sendHeader() throws IOException {
        state = ResponseState.HEADER;
        _flushBuf = hb.prepareBuffer();
        _continue();
    }

    public void setFragmented(boolean fragmented) {
        this.fragmented = fragmented;
    }

    public ChannelStatus simple(int code) {
        return simple(code, null);
    }

    public ChannelStatus simple(int code, CharSequence message) {
        try {
            String std = status(code, "text/html; charset=utf-8");
            put(message == null ? std : message).put(Misc.EOL);
            end();
            return ChannelStatus.READ;
        } catch (IOException ignored) {
            return ChannelStatus.DISCONNECTED;
        }
    }

    public String status(int status, CharSequence contentType) {
        return status(status, contentType, -1);
    }

    public String status(int status, CharSequence contentType, long len) {
        return this.hb.status(status, contentType, len);
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

    private void flush(ByteBuffer buf) throws IOException {
        this._flushBuf = buf;
        ByteBuffers.copyNonBlocking(buf, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
        buf.clear();
        this._flushBuf = null;
    }

    private enum ResponseState {
        HEADER, CHUNK, BODY, BODY_PART, END_CHUNK, END_BODY
    }
}
