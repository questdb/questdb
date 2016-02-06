/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
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

import com.nfsdb.ex.HeadersTooLargeException;
import com.nfsdb.ex.MalformedHeaderException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import com.nfsdb.net.NetworkChannel;
import com.nfsdb.std.DirectByteCharSequence;
import com.nfsdb.std.Mutable;
import com.nfsdb.std.ObjectPool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

@SuppressFBWarnings("CD_CIRCULAR_DEPENDENCY")
public class Request implements Closeable, Mutable {
    public static final int SO_RVCBUF_DOWNLD = 128 * 1024;
    private static final int SO_RCVBUF_UPLOAD = 4 * 1024 * 1024;
    private static final int SO_READ_RETRY_COUNT = 1000;

    private final ByteBuffer in;
    private final long inAddr;
    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
    private final RequestHeaderBuffer hb;
    private final MultipartParser multipartParser;
    private final BoundaryAugmenter augmenter = new BoundaryAugmenter();
    private final NetworkChannel channel;

    public Request(NetworkChannel channel, int headerBufferSize, int contentBufferSize, int multipartHeaderBufferSize) {
        this.channel = channel;
        this.hb = new RequestHeaderBuffer(headerBufferSize, pool);
        this.in = ByteBuffer.allocateDirect(Numbers.ceilPow2(contentBufferSize));
        this.inAddr = ByteBuffers.getAddress(in);
        this.multipartParser = new MultipartParser(multipartHeaderBufferSize, pool);
    }

    @Override
    public void clear() {
        this.hb.clear();
        this.pool.clear();
        this.in.clear();
        this.multipartParser.clear();
    }

    @Override
    public void close() {
        hb.close();
        multipartParser.close();
        ByteBuffers.release(in);
        augmenter.close();
    }

    public CharSequence getHeader(CharSequence name) {
        return hb.get(name);
    }

//    public SocketAddress getSocketAddress() {
//        try {
//            return channel.getChannel().getRemoteAddress();
//        } catch (IOException ignore) {
//            return null;
//        }
//    }

    public CharSequence getUrl() {
        return hb.getUrl();
    }

    public CharSequence getUrlParam(CharSequence name) {
        return hb.getUrlParam(name);
    }

    public boolean isIncomplete() {
        return hb.isIncomplete();
    }

    public boolean isMultipart() {
        return hb.getContentType() != null && Chars.equals("multipart/form-data", hb.getContentType());
    }

    public void parseMultipart(IOContext context, MultipartListener handler)
            throws HeadersTooLargeException, IOException, MalformedHeaderException {
        //todo: add support for socket options
//        channel.getChannel().setOption(StandardSocketOptions.SO_RCVBUF, SO_RCVBUF_UPLOAD);
        try {
            MultipartParser parser = getMultipartParser().of(getBoundary());
            while (true) {
                int sz = in.remaining();
                if (sz > 0 && parser.parse(context, ByteBuffers.getAddress(in) + in.position(), sz, handler)) {
                    break;
                }
                drainChannel();
            }
        } finally {
//            channel.getChannel().setOption(StandardSocketOptions.SO_RCVBUF, SO_RVCBUF_DOWNLD);
        }
    }

    public void read() throws HeadersTooLargeException, IOException, MalformedHeaderException {
        drainChannel();
        if (isIncomplete()) {
            readHeaders();
        }
    }

    private void drainChannel() throws IOException {
        in.clear();
        ByteBuffers.copyNonBlocking(channel, in, SO_READ_RETRY_COUNT);
        in.flip();
    }

    private DirectByteCharSequence getBoundary() {
        return augmenter.of(hb.getBoundary());
    }

    private MultipartParser getMultipartParser() {
        return multipartParser;
    }

    private void readHeaders() throws HeadersTooLargeException, IOException, MalformedHeaderException {
        do {
            in.position((int) (hb.write(inAddr, in.remaining(), true) - inAddr));
            if (hb.isIncomplete()) {
                drainChannel();
            } else {
                break;
            }
        } while (true);
    }

    public static class BoundaryAugmenter implements Closeable {
        private static final String BOUNDARY_PREFIX = "\r\n--";
        private final DirectByteCharSequence export = new DirectByteCharSequence();
        private long lo;
        private long lim;
        private long _wptr;

        public BoundaryAugmenter() {
            this.lim = 64;
            this.lo = this._wptr = Unsafe.getUnsafe().allocateMemory(this.lim);
            _of(BOUNDARY_PREFIX);
        }

        public DirectByteCharSequence of(CharSequence value) {
            int len = value.length() + BOUNDARY_PREFIX.length();
            if (len > lim) {
                resize(len);
            }
            _wptr = lo + BOUNDARY_PREFIX.length();
            _of(value);
            return export.of(lo, _wptr);
        }

        private void _of(CharSequence value) {
            int len = value.length();
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(_wptr++, (byte) value.charAt(i));
            }
        }

        private void resize(int lim) {
            Unsafe.getUnsafe().freeMemory(this.lo);
            this.lim = Numbers.ceilPow2(lim);
            this.lo = _wptr = Unsafe.getUnsafe().allocateMemory(this.lim);
            _of(BOUNDARY_PREFIX);
        }

        @Override
        public void close() {
            if (lo > 0) {
                Unsafe.getUnsafe().freeMemory(this.lo);
                this.lo = 0;
            }
        }
    }
}
