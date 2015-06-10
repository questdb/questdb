/*******************************************************************************
 *   _  _ ___ ___     _ _
 *  | \| | __/ __| __| | |__
 *  | .` | _|\__ \/ _` | '_ \
 *  |_|\_|_| |___/\__,_|_.__/
 *
 *  Copyright (c) 2014-2015. The NFSdb project and its contributors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 ******************************************************************************/
package com.nfsdb.ha;

import com.nfsdb.logging.Logger;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

class StatsCollectingReadableByteChannel implements ReadableByteChannel {

    private final static Logger LOGGER = Logger.getLogger(StatsCollectingReadableByteChannel.class);

    private final SocketAddress socketAddress;
    private ReadableByteChannel delegate;
    private long startTime;
    private long byteCount;
    private long callCount;

    public StatsCollectingReadableByteChannel(SocketAddress socketAddress) {
        this.socketAddress = socketAddress;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    public void logStats() {
        if (byteCount > 10) {
            long endTime = System.currentTimeMillis();
            LOGGER.info("received %d bytes @ %f MB/s from: %s [%d calls]", byteCount, (double) (byteCount * 1000) / ((endTime - startTime)) / 1024 / 1024, socketAddress, callCount);
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
