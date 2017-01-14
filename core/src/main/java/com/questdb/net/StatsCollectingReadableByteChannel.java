/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2017 Appsicle
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

package com.questdb.net;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public class StatsCollectingReadableByteChannel implements ReadableByteChannel {

    private final static Log LOG = LogFactory.getLog(StatsCollectingReadableByteChannel.class);

    private final SocketAddress socketAddress;
    private ReadableByteChannel delegate;
    private long startTime;
    private long byteCount;
    private long callCount;

    public StatsCollectingReadableByteChannel(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    public void logStats() {
        if (byteCount > 10) {
            long endTime = System.currentTimeMillis();
            LOG.info().$("received ").$(byteCount).$(" bytes @ ").$((double) (byteCount * 1000) / ((endTime - startTime)) / 1024 / 1024).$(" MB/s from: ").$(socketAddress.toString()).$(" [").$(callCount).$(" calls]").$();
        }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        callCount++;
        int count = delegate.read(dst);
        this.byteCount += count;
        return count;
    }

    public void setDelegate(ReadableByteChannel delegate) {
        this.delegate = delegate;
        this.startTime = System.currentTimeMillis();
        this.byteCount = 0;
        this.callCount = 0;
    }

}
