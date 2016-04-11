/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (C) 2014-2016 Appsicle
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
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects for
 * all of the code used other than as permitted herein. If you modify file(s)
 * with this exception, you may extend this exception to your version of the
 * file(s), but you are not obligated to do so. If you do not wish to do so,
 * delete this exception statement from your version. If you delete this
 * exception statement from all source files in the program, then also delete
 * it in the license file.
 *
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
    private static final int outAvail = 64 * 1024;
    private final ByteBuffer out = ByteBuffer.allocateDirect(outAvail);
    private final long outAddr = ByteBuffers.getAddress(out);
    private T channel;
    private long z_streamp;
    private boolean flushed = false;
    private int crc = 0;
    private long total = 0;

    public GZipWritableChannel() {
        this.z_streamp = Zip.deflateInit();
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
        int ret;
        do {
            ret = Zip.deflate(z_streamp, outAddr, outAvail, flush);
            if (ret < 0) {
                throw new IOException("Deflater error: " + ret);
            }

            int len = outAvail - Zip.availOut(z_streamp);
            if (len > 0) {
                out.limit(len).position(0);
                channel.write(out);
            }
        } while (Zip.availIn(z_streamp) > 0 || (flush && ret != 1));
    }

    private void reset() {
        Zip.deflateReset(z_streamp);
        crc = 0;
        total = 0;
        flushed = false;
    }
}