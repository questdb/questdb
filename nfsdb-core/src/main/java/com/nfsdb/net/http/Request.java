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

import com.nfsdb.collections.DirectByteCharSequence;
import com.nfsdb.collections.Mutable;
import com.nfsdb.collections.ObjectPool;
import com.nfsdb.exceptions.DisconnectedChannelException;
import com.nfsdb.exceptions.HeadersTooLargeException;
import com.nfsdb.exceptions.MalformedHeaderException;
import com.nfsdb.exceptions.SlowChannelException;
import com.nfsdb.misc.ByteBuffers;
import com.nfsdb.misc.Chars;
import com.nfsdb.misc.Numbers;
import com.nfsdb.net.IOHttpRunnable;
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
    private final StringBuilder boundaryAugmenter = new StringBuilder();

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
        this.multipartParser.clear();
    }

    @Override
    public void close() {
        hb.close();
        multipartParser.close();
        ByteBuffers.release(in);
    }

    public CharSequence getBoundary() {
        boundaryAugmenter.setLength(0);
        boundaryAugmenter.append("\r\n--");
        boundaryAugmenter.append(hb.getBoundary());
        return boundaryAugmenter;
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

    public ChannelStatus read(ReadableByteChannel channel) throws HeadersTooLargeException, SlowChannelException, IOException, MalformedHeaderException, DisconnectedChannelException {
        ByteBuffers.copyNonBlocking(channel, in, IOHttpRunnable.SO_READ_RETRY_COUNT);
        long address = ((DirectBuffer) in).address();
        in.position((int) (hb.write(address, in.remaining(), true) - address));

        if (hb.isIncomplete()) {
            return ChannelStatus.NEED_REQUEST;
        }
        return ChannelStatus.READY;
    }

    public enum ChannelStatus {
        READY, NEED_REQUEST, DISCONNECTED
    }
}
