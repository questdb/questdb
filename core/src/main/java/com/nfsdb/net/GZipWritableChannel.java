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

package com.nfsdb.net;

import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.misc.Zip;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class GZipWritableChannel<T extends WritableByteChannel> implements WritableByteChannel {
    private final int outAvail = 64 * 1024;
    private final ByteBuffer out = ByteBuffer.allocateDirect(outAvail);
    private final long outAddr = ByteBuffers.getAddress(out);
    private T channel;
    private long z_streamp;
    private boolean flushed = false;
    private int crc = 0;
    private long total = 0;

    public GZipWritableChannel() {
        this.z_streamp = Zip.deflateInit(-1, true);
        if (z_streamp <= 0) {
            throw new OutOfMemoryError();
        }
    }

    public void flush() throws IOException {
        if (flushed) {
            return;
        }

        deflate(true);
        Unsafe.getUnsafe().putInt(outAddr, crc);
        Unsafe.getUnsafe().putInt(outAddr + 4, (int) total);
        out.limit(8).position(0);
        channel.write(out);

        flushed = true;
    }

    @Override
    public boolean isOpen() {
        return z_streamp != 0;
    }

    @Override
    public void close() throws IOException {

        flush();

        if (channel.isOpen()) {
            channel.close();
        }

        if (z_streamp != 0) {
            Zip.deflateEnd(z_streamp);
            z_streamp = 0;
        }
        ByteBuffers.release(out);
    }

    public GZipWritableChannel<T> of(T channel) throws IOException {
        reset();
        this.channel = channel;
        Unsafe.getUnsafe().copyMemory(Zip.gzipHeader, outAddr, Zip.gzipHeaderLen);
        out.limit(Zip.gzipHeaderLen).position(0);
        channel.write(out);
        return this;
    }

    public void reset() {
        Zip.deflateReset(z_streamp);
        crc = 0;
        total = 0;
        flushed = false;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (!(src instanceof DirectBuffer)) {
            throw new IllegalArgumentException("Heap buffers are not supported");
        }

        int result = src.remaining();
        int pos = src.position();
        Zip.setInput(z_streamp, ByteBuffers.getAddress(src) + pos, result);
        crc = Zip.crc32(crc, ByteBuffers.getAddress(src) + pos, result);
        deflate(false);
        src.position(pos + result);
        total += result;
        return result;
    }

    private void deflate(boolean flush) throws IOException {
        do {
            int len = Zip.deflate(z_streamp, outAddr, outAvail, flush);
            if (len < 0) {
                throw new IOException("Deflater error: " + len);
            }
            if (len > 0) {
                out.limit(len).position(0);
                channel.write(out);
            }
        } while (Zip.remainingInput(z_streamp) > 0);
    }
}