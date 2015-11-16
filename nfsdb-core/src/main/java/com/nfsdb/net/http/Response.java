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
import com.nfsdb.exceptions.ResponseHeaderBufferTooSmallException;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class Response implements Closeable, Mutable {
    public static final String EOL = "\r\n";
    private final ByteBuffer out;
    private final long outPtr;
    private final long limit;
    private final ByteBuffer chunkHeader;
    private final ResponseHeaderBuffer hb;
    private WritableByteChannel channel;
    private long _wptr;

    public Response(int headerBufferSize, int contentBufferSize) {
        if (headerBufferSize <= 0) {
            throw new IllegalArgumentException("headerBufferSize");
        }

        if (contentBufferSize <= 0) {
            throw new IllegalArgumentException("contentBufferSize");
        }


        int sz = Numbers.ceilPow2(contentBufferSize);
        this.out = ByteBuffer.allocateDirect(sz);
        this.hb = new ResponseHeaderBuffer(headerBufferSize);
        this.chunkHeader = ByteBuffer.allocateDirect((int) Math.log10(sz) + 5);
        this.outPtr = this._wptr = ((DirectBuffer) out).address();
        this.limit = outPtr + sz;
    }

    @Override
    public void clear() {
        out.clear();
        hb.clear();
        this._wptr = outPtr;
    }

    @Override
    public void close() {
        ByteBuffers.release(out);
        ByteBuffers.release(chunkHeader);
        hb.close();
    }

    public void end() throws IOException {
        flush();
        chunk(0);
        write(EOL);
        int lim = (int) (_wptr - outPtr);
        out.limit(lim);
        channel.write(out);
        out.clear();
        _wptr = outPtr;
    }

    public void flush() throws IOException {
        int lim = (int) (_wptr - outPtr);
        if (lim > 0) {
            chunk(lim);
            out.limit(lim);
            channel.write(out);
            out.clear();
            _wptr = outPtr;
        }
    }

    public void flushHeader() throws IOException {
        hb.flush(channel);
    }

    public void setChannel(WritableByteChannel channel) {
        this.channel = channel;
    }

    public void status(int status, CharSequence contentType) {
        this.hb.status(status, contentType);
    }

    // todo: this function should be able to send any length char sequence
    public Response write(CharSequence seq) {
        int len = seq.length();
        long p = _wptr;
        if (p + len < limit) {
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(p++, (byte) seq.charAt(i));
            }
            _wptr = p;
        } else {
            throw ResponseHeaderBufferTooSmallException.INSTANCE;
        }
        return this;
    }

    //todo: optimise
    private void chunk(int len) throws IOException {
        chunkHeader.clear();
        String h = EOL + Integer.toHexString(len) + EOL;
        for (int i = 0; i < h.length(); i++) {
            chunkHeader.put((byte) h.charAt(i));
        }
        chunkHeader.flip();
        channel.write(chunkHeader);
    }
}
