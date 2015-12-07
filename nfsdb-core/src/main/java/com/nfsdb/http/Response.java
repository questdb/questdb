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

package com.nfsdb.http;

import com.nfsdb.collections.Mutable;
import com.nfsdb.exceptions.ResponseHeaderBufferTooSmallException;
import com.nfsdb.io.sink.AbstractCharSink;
import com.nfsdb.io.sink.CharSink;
import com.nfsdb.io.sink.DirectUnboundedAnsiSink;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Misc;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class Response extends AbstractCharSink implements Closeable, Mutable {
    private final ByteBuffer out;
    private final long outPtr;
    private final long limit;
    private final ByteBuffer chunkHeader;
    private final DirectUnboundedAnsiSink chunkSink;
    private final ResponseHeaderBuffer hb;
    private WritableByteChannel channel;
    private long _wPtr;
    private ByteBuffer _flushBuf;
    private RandomAccessFile raf = null;
    private FileChannel rafCh;

    public Response(int headerBufferSize, int contentBufferSize, Clock clock) {
        if (headerBufferSize <= 0) {
            throw new IllegalArgumentException("headerBufferSize");
        }

        if (contentBufferSize <= 0) {
            throw new IllegalArgumentException("contentBufferSize");
        }

        int sz = Numbers.ceilPow2(contentBufferSize);
        this.out = ByteBuffer.allocateDirect(sz);
        this.hb = new ResponseHeaderBuffer(headerBufferSize, clock);
        // size is 32bit int, as hex string max 8 bytes
        this.chunkHeader = ByteBuffer.allocateDirect(8 + 2 * Misc.EOL.length());
        this.chunkSink = new DirectUnboundedAnsiSink(((DirectBuffer) chunkHeader).address());
        this.chunkSink.put(Misc.EOL);
        this.outPtr = this._wPtr = ((DirectBuffer) out).address();
        this.limit = outPtr + sz;
    }

    @Override
    public void clear() {
        out.clear();
        hb.clear();
        this._wPtr = outPtr;
        this.raf = Misc.free(raf);
    }

    @Override
    public void close() {
        ByteBuffers.release(out);
        ByteBuffers.release(chunkHeader);
        hb.close();
        this.raf = Misc.free(raf);
    }

    public void end() throws IOException {
        flush();
        chunk(0);
        put(Misc.EOL);
        int lim = (int) (_wPtr - outPtr);
        out.limit(lim);
        channel.write(out);
        out.clear();
        _wPtr = outPtr;
    }

    public void flush() throws IOException {
        int lim = (int) (_wPtr - outPtr);
        if (lim > 0) {
            chunk(lim);
            out.limit(lim);
            flush(out);
            _wPtr = outPtr;
        }
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
            throw ResponseHeaderBufferTooSmallException.INSTANCE;
        }
        return this;
    }

    @Override
    public CharSink put(char c) {
        if (_wPtr < limit) {
            Unsafe.getUnsafe().putByte(_wPtr++, (byte) c);
            return this;
        }
        throw ResponseHeaderBufferTooSmallException.INSTANCE;
    }

    public void flushHeader() throws IOException {
        hb.flush(channel);
    }

    public void flushRemaining() throws IOException {
        if (_flushBuf != null) {
            flush(_flushBuf);
        }

        if (rafCh != null) {
            _flushBuf = out;
            while (rafCh.read(out) > 0) {
                out.flip();
                ByteBuffers.copyNonBlocking(out, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
                out.clear();
            }
            _flushBuf = null;
            rafCh = Misc.free(rafCh);
            raf = Misc.free(raf);
        }
    }

    public void send(String fileName) throws IOException {
        raf = new RandomAccessFile(fileName, "r");
        rafCh = raf.getChannel();
        hb.status(200, "text/plain; charset=utf-8", raf.length());
        flushHeader();
        put(Misc.EOL);
        out.position(Misc.EOL.length());
        flushRemaining();
    }

    public void setChannel(WritableByteChannel channel) {
        this.channel = channel;
    }

    public ChannelStatus simple(int code) {
        return simple(code, null);
    }

    public ChannelStatus simple(int code, CharSequence message) {
        try {
            String std = status(code, "text/html; charset=utf-8");
            flushHeader();
            put(message == null ? std : message).put(Misc.EOL);
            end();
            return ChannelStatus.READ;
        } catch (IOException ignored) {
            return ChannelStatus.DISCONNECTED;
        }
    }

    public String status(int status, CharSequence contentType) {
        return this.hb.status(status, contentType);
    }

    private void chunk(int len) throws IOException {
        chunkSink.clear(Misc.EOL.length());
        Numbers.appendHex(chunkSink, len);
        chunkSink.put(Misc.EOL);
        chunkHeader.limit(chunkSink.length());
        flush(chunkHeader);
    }

    private void flush(ByteBuffer buf) throws IOException {
        this._flushBuf = buf;
        ByteBuffers.copyNonBlocking(buf, channel, IOHttpJob.SO_WRITE_RETRY_COUNT);
        this._flushBuf.clear();
        this._flushBuf = null;
    }
}
