/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2019 Appsicle
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
 ******************************************************************************/

package com.questdb.cutlass.http;

import com.questdb.cutlass.http.io.IOContext;
import com.questdb.std.ObjectPool;
import com.questdb.std.Unsafe;
import com.questdb.std.str.DirectByteCharSequence;

public class HttpConnectionContext implements IOContext {
    public final HttpHeaderParser headerParser;
    public final long recvBuffer;
    public final int recvBufferSize;
    final HttpMultipartContentParser multipartContentParser;
    final HttpHeaderParser multipartContentHeaderParser;
    final ObjectPool<DirectByteCharSequence> csPool;
    final long sendBuffer;
    final HttpServerConfiguration configuration;
    final long fd;
    final long ipv4;

    public HttpConnectionContext(HttpServerConfiguration configuration, long fd, long ipv4) {
        this.configuration = configuration;
        this.csPool = new ObjectPool<>(DirectByteCharSequence.FACTORY, configuration.getConnectionWrapperObjPoolSize());
        this.headerParser = new HttpHeaderParser(configuration.getConnectionHeaderBufferSize(), csPool);
        this.multipartContentHeaderParser = new HttpHeaderParser(configuration.getConnectionMultipartHeaderBufferSize(), csPool);
        this.multipartContentParser = new HttpMultipartContentParser(multipartContentHeaderParser);
        this.recvBufferSize = configuration.getConnectionRecvBufferSize();
        this.recvBuffer = Unsafe.malloc(recvBufferSize);
        this.sendBuffer = Unsafe.malloc(configuration.getConnectionSendBufferSize());
        this.fd = fd;
        this.ipv4 = ipv4;
    }

    @Override
    public void close() {
        csPool.clear();
        multipartContentParser.close();
        multipartContentHeaderParser.close();
        headerParser.close();
        Unsafe.free(recvBuffer, recvBufferSize);
        Unsafe.free(sendBuffer, configuration.getConnectionSendBufferSize());
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public long getIp() {
        return ipv4;
    }
}
