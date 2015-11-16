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

import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.exceptions.SlowChannelException;
import com.nfsdb.exceptions.UnsupportedContentTypeException;
import com.nfsdb.utils.ByteBuffers;
import com.nfsdb.utils.Chars;
import com.nfsdb.utils.Numbers;
import com.nfsdb.utils.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class Request implements Closeable, Mutable {
    public final ByteBuffer in;
    private final ObjectPool<DirectByteCharSequence> pool = new ObjectPool<>(DirectByteCharSequence.FACTORY, 64);
    private final RequestHeaderBuffer hb;
    private final MultipartParser multipartParser;
    private DirectByteCharSequence contentType;
    private DirectByteCharSequence encoding;
    private DirectByteCharSequence boundary;

    public Request(int headerBufferSize, int contentBufferSize, int multipartHeaderBufferSize) {
        this.hb = new RequestHeaderBuffer(headerBufferSize, pool);
        this.in = ByteBuffer.allocateDirect(Numbers.ceilPow2(contentBufferSize));
        this.multipartParser = new MultipartParser(multipartHeaderBufferSize, pool);
    }

    @Override
    public void clear() {
        this.hb.clear();
        this.pool.clear();
        this.in.clear();
        this.contentType = null;
        this.boundary = null;
        this.encoding = null;
    }

    @Override
    public void close() {
        hb.close();
        multipartParser.close();
        ByteBuffers.release(in);
    }

    public DirectByteCharSequence getBoundary() {
        return boundary;
    }

    public DirectByteCharSequence getContentType() {
        return contentType;
    }

    public DirectByteCharSequence getEncoding() {
        return encoding;
    }

    public MultipartParser getMultipartParser() {
        return multipartParser;
    }

    public CharSequence getUrl() {
        return hb.getUrl();
    }

    public boolean isMultipart() {
        return contentType != null && Chars.equals("multipart/form-data", contentType);
    }

    public ChannelStatus read(ReadableByteChannel channel) throws HeadersTooLargeException, SlowChannelException, IOException, UnsupportedContentTypeException {
        if (hb.isIncomplete()) {
            ByteBuffers.copyNonBlocking(channel, in);
            if (in.remaining() == 0) {
                // nothing sent, browsers do that when user clicks "stop" button
                return ChannelStatus.DISCONNECTED;
            }

            long address = ((DirectBuffer) in).address();
            in.position((int) (hb.write(address, in.remaining(), true) - address));

            if (hb.isIncomplete()) {
                return ChannelStatus.NEED_REQUEST;
            }

            parseContentType();
        }
        return ChannelStatus.READY;
    }

    private void parseContentType() throws UnsupportedContentTypeException {
        CharSequence seq = hb.get("Content-Type");
        if (seq == null) {
            return;
        }

        long p = ((DirectByteCharSequence) seq).getLo();
        long _lo = p;
        long hi = ((DirectByteCharSequence) seq).getHi();

        DirectByteCharSequence name = null;
        boolean contentType = true;
        boolean swallowSpace = true;

        while (p <= hi) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);

            if (b == ' ' && swallowSpace) {
                _lo = p;
                continue;
            }

            if (p > hi || b == ';') {
                if (contentType) {
                    this.contentType = pool.next().of(_lo, p - 1);
                    _lo = p;
                    contentType = false;
                    continue;
                }

                if (name == null) {
                    throw UnsupportedContentTypeException.INSTANCE;
                }

                if (Chars.equals("encoding", name)) {
                    encoding = pool.next().of(_lo, p - 1);
                    _lo = p;
                    continue;
                }

                if (Chars.equals("boundary", name)) {
                    boundary = pool.next().of(_lo, p - 1);
                    _lo = p;
                    continue;
                }

                if (p > hi) {
                    break;
                }
            } else if (b == '=') {
                name = name == null ? pool.next().of(_lo, p - 1) : name.of(_lo, p - 1);
                _lo = p;
                swallowSpace = false;
            }
        }
    }

    public enum ChannelStatus {
        READY, NEED_REQUEST, DISCONNECTED
    }
}
