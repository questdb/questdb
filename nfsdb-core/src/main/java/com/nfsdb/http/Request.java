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

import com.nfsdb.collections.CharSequenceObjHashMap;
import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.exceptions.MalformedHeaderException;
import com.nfsdb.exceptions.NumericException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.misc.Unsafe;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

@SuppressFBWarnings("CD_CIRCULAR_DEPENDENCY")
public class Request implements Closeable, Mutable {
    public final ByteBuffer in;
    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
    private final RequestHeaderBuffer hb;
    private final MultipartParser multipartParser;
    private final BoundaryAugmenter augmenter = new BoundaryAugmenter();

    public Request(int headerBufferSize, int contentBufferSize, int multipartHeaderBufferSize) {
        this.hb = new RequestHeaderBuffer(headerBufferSize, pool);
        this.in = ByteBuffer.allocateDirect(Numbers.ceilPow2(contentBufferSize));
        this.multipartParser = new MultipartParser(multipartHeaderBufferSize, pool);
    }

    public static void urlDecode(long lo, long hi, CharSequenceObjHashMap<CharSequence> map, ObjectPool<DirectByteCharSequence> pool) {
        long _lo = lo;
        long rp = lo;
        long wp = lo;
        final DirectByteCharSequence temp = pool.next();

        CharSequence name = null;

        while (rp < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(rp++);

            switch (b) {
                case '=':
                    if (_lo < wp) {
                        name = pool.next().of(_lo, wp);
                    }
                    _lo = rp;
                    wp = rp - 1;
                    break;
                case '&':
                    if (name != null) {
                        map.put(name, pool.next().of(_lo, wp));
                        name = null;
                    }
                    _lo = rp;
                    wp = rp - 1;
                    break;
                case '+':
                    Unsafe.getUnsafe().putByte(wp, (byte) ' ');
                    break;
                case '%':
                    try {
                        if (rp + 1 < hi) {
                            Unsafe.getUnsafe().putByte(wp++, (byte) Numbers.parseHexInt(temp.of(rp, rp += 2)));
                            continue;
                        }
                    } catch (NumericException ignore) {
                    }
                    name = null;
                    break;
                default:
                    Unsafe.getUnsafe().putByte(wp, (byte) b);
                    break;
            }
            wp++;
        }

        if (_lo < wp && name != null) {
            map.put(name, pool.next().of(_lo, wp));
        }
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

    public DirectByteCharSequence getBoundary() {
        return augmenter.of(hb.getBoundary());
    }

    public CharSequence getHeader(CharSequence name) {
        return hb.get(name);
    }

    public MultipartParser getMultipartParser() {
        return multipartParser;
    }

    public CharSequence getUrl() {
        return hb.getUrl();
    }

    public boolean isIncomplete() {
        return hb.isIncomplete();
    }

    public boolean isMultipart() {
        return hb.getContentType() != null && Chars.equals("multipart/form-data", hb.getContentType());
    }

    public ChannelStatus read(ReadableByteChannel channel) throws HeadersTooLargeException, IOException, MalformedHeaderException {
        ByteBuffers.copyNonBlocking(channel, in, IOHttpJob.SO_READ_RETRY_COUNT);
        long address = ByteBuffers.getAddress(in);
        in.position((int) (hb.write(address, in.remaining(), true) - address));

        if (hb.isIncomplete()) {
            return ChannelStatus.NEED_REQUEST;
        }
        return ChannelStatus.READ;
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
