/*******************************************************************************
 * ___                  _   ____  ____
 * / _ \ _   _  ___  ___| |_|  _ \| __ )
 * | | | | | | |/ _ \/ __| __| | | |  _ \
 * | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 * \__\_\\__,_|\___||___/\__|____/|____/
 * <p>
 * Copyright (C) 2014-2016 Appsicle
 * <p>
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * <p>
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
 ******************************************************************************/

package com.questdb.net.http;

import com.questdb.ex.DisconnectedChannelException;
import com.questdb.ex.SlowWritableChannelException;
import com.questdb.iter.clock.Clock;
import com.questdb.misc.Misc;
import com.questdb.net.NetworkChannel;
import com.questdb.std.FlyweightCharSequence;
import com.questdb.std.LocalValueMap;
import com.questdb.std.Locality;
import com.questdb.std.Mutable;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

public class IOContext implements Closeable, Mutable, Locality {
    public final NetworkChannel channel;
    public final Request request;
    public final FlyweightCharSequence ext = new FlyweightCharSequence();
    private final LocalValueMap map = new LocalValueMap();
    private final Response response;
    private final AtomicBoolean open = new AtomicBoolean(true);

    public IOContext(NetworkChannel channel,
                     Clock clock,
                     int reqHeaderSize,
                     int reqContentSize,
                     int reqMultipartHeaderSize,
                     int respHeaderSize,
                     int respContentSize,
                     int soRcvDownload,
                     int soRcvuUpload,
                     int soRetries) {
        this.channel = channel;
        this.request = new Request(channel, reqHeaderSize, reqContentSize, reqMultipartHeaderSize, soRcvDownload, soRcvuUpload, soRetries);
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
        if (open.compareAndSet(true, false)) {
            Misc.free(channel);
            Misc.free(request);
            Misc.free(response);
            Misc.free(map);
        }
    }

    public SimpleResponse emergencyResponse() {
        response.clear();
        return response.asSimple();
    }

    public FixedSizeResponse fixedSizeResponse() {
        return response.asFixedSize();
    }

    @Override
    public LocalValueMap getMap() {
        return map;
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

    int getResponseCode() {
        return response.getCode();
    }
}
