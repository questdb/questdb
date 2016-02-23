/*
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
 */

package com.nfsdb.net.http;

import com.nfsdb.ex.DisconnectedChannelException;
import com.nfsdb.ex.SlowWritableChannelException;
import com.nfsdb.iter.clock.Clock;
import com.nfsdb.misc.Misc;
import com.nfsdb.net.NetworkChannel;
import com.nfsdb.std.FlyweightCharSequence;
import com.nfsdb.std.LocalValueMap;
import com.nfsdb.std.Locality;
import com.nfsdb.std.Mutable;

import java.io.Closeable;

public class IOContext implements Closeable, Mutable, Locality {
    public final NetworkChannel channel;
    public final Request request;
    public final FlyweightCharSequence ext = new FlyweightCharSequence();
    private final LocalValueMap map = new LocalValueMap();
    private final Response response;


    public IOContext(NetworkChannel channel, Clock clock, int reqHeaderSize, int reqContentSize, int reqMultipartHeaderSize, int respHeaderSize, int respContentSize) {
        this.channel = channel;
        this.request = new Request(channel, reqHeaderSize, reqContentSize, reqMultipartHeaderSize);
        this.response = new Response(channel, respHeaderSize, respContentSize, clock);
    }

    public ChunkedResponse chunkedResponse() {
        return response.asChunked();
    }

    @Override
    public void clear() {
        request.clear();
        response.clear();
        map.clear();
    }

    @Override
    public void close() {
        Misc.free(channel);
        request.close();
        response.close();
        map.close();
    }

    public FixedSizeResponse fixedSizeResponse() {
        return response.asFixedSize();
    }

    @Override
    public LocalValueMap getMap() {
        return map;
    }

    public int getResponseCode() {
        return response.getCode();
    }

    public ResponseSink responseSink() {
        return response.asSink();
    }

    public void resume() throws DisconnectedChannelException, SlowWritableChannelException {
        response.resume();
    }

    public SimpleResponse simpleResponse() {
        return response.asSimple();
    }
}
