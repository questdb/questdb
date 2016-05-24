/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
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

package com.questdb.net.http;

import com.questdb.ex.HeadersTooLargeException;
import com.questdb.ex.MalformedHeaderException;
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.misc.*;
import com.questdb.net.NetworkChannel;
import com.questdb.std.DirectByteCharSequence;
import com.questdb.std.Mutable;
import com.questdb.std.ObjectPool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

@SuppressFBWarnings("CD_CIRCULAR_DEPENDENCY")
public class Request implements Closeable, Mutable {
    private final static Log LOG = LogFactory.getLog(Request.class);
    private final ByteBuffer in;
    private final long inAddr;
    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
    private final RequestHeaderBuffer hb;
    private final MultipartParser multipartParser;
    private final BoundaryAugmenter augmenter = new BoundaryAugmenter();
    private final NetworkChannel channel;
    private final int soRcvSmall;
    private final int soRcvLarge;
    private final int soRetries;

    public Request(NetworkChannel channel, int headerBufferSize, int contentBufferSize, int multipartHeaderBufferSize, int soRcvSmall, int soRcvLarge, int soRetries) {
        this.channel = channel;
        this.hb = new RequestHeaderBuffer(headerBufferSize, pool);
        this.in = ByteBuffer.allocateDirect(Numbers.ceilPow2(contentBufferSize));
        this.inAddr = ByteBuffers.getAddress(in);
        this.multipartParser = new MultipartParser(multipartHeaderBufferSize, pool);
        this.soRcvSmall = soRcvSmall;
        this.soRcvLarge = soRcvLarge;
        this.soRetries = soRetries;
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

    public CharSequence getMethodLine() {
        return hb.getMethodLine();
    }

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
        return Chars.equalsNc("multipart/form-data", hb.getContentType());
    }

    public void parseMultipart(IOContext context, MultipartListener handler)
            throws HeadersTooLargeException, IOException, MalformedHeaderException {
        final long fd = channel.getFd();
        if (Net.setRcvBuf(fd, soRcvLarge) != 0) {
            LOG.error().$("Could not set SO_RCVBUF on ").$(fd).$();
        }
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
            if (Net.setRcvBuf(fd, soRcvSmall) != 0) {
                LOG.error().$("Could not reset SO_RCVBUF on ").$(fd).$();
            }
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
        ByteBuffers.copyNonBlocking(channel, in, soRetries);
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
            of0(BOUNDARY_PREFIX);
        }

        public DirectByteCharSequence of(CharSequence value) {
            int len = value.length() + BOUNDARY_PREFIX.length();
            if (len > lim) {
                resize(len);
            }
            _wptr = lo + BOUNDARY_PREFIX.length();
            of0(value);
            return export.of(lo, _wptr);
        }

        private void of0(CharSequence value) {
            int len = value.length();
            for (int i = 0; i < len; i++) {
                Unsafe.getUnsafe().putByte(_wptr++, (byte) value.charAt(i));
            }
        }

        private void resize(int lim) {
            Unsafe.getUnsafe().freeMemory(this.lo);
            this.lim = Numbers.ceilPow2(lim);
            this.lo = _wptr = Unsafe.getUnsafe().allocateMemory(this.lim);
            of0(BOUNDARY_PREFIX);
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
