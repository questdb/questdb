/*
 * Copyright (c) 2014. Vlad Ilyushchenko
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

package com.nfsdb.journal.net.config;

import com.nfsdb.journal.exceptions.JournalNetworkException;
import com.nfsdb.journal.logging.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.TimeUnit;

public class ServerConfig extends NetworkConfig {
    public static final long SYNC_TIMEOUT = TimeUnit.SECONDS.toMillis(15);
    private static final long DEFAULT_HEARTBEAT_FREQUENCY = TimeUnit.SECONDS.toMillis(5);
    private static final int RING_BUFFER_SIZE = 1024;
    private static final Logger LOGGER = Logger.getLogger(ServerConfig.class);
    private long heartbeatFrequency = DEFAULT_HEARTBEAT_FREQUENCY;
    private int eventBufferSize = RING_BUFFER_SIZE;

    public long getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public void setHeartbeatFrequency(long heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public int getEventBufferSize() {
        return eventBufferSize;
    }

    public void setEventBufferSize(int eventBufferSize) {
        this.eventBufferSize = eventBufferSize;
    }

    public InetSocketAddress getSocketAddress() throws JournalNetworkException {
        // must call this to have hostAddress populated
        InetSocketAddress address = getInterfaceSocketAddress();
        return hostAddress == null ? address : new InetSocketAddress(hostAddress, getPort());
    }

    public ServerSocketChannel openServerSocketChannel() throws JournalNetworkException {
        try {
            ServerSocketChannel channel;
            InetSocketAddress address = getSocketAddress();
            channel = ServerSocketChannel.open();
            channel.socket().bind(address);
            channel.socket().setReceiveBufferSize(getSoRcvBuf());

            NetworkInterface ifn = getNetworkInterface();
            LOGGER.info("Server is now listening on %s [%s]", address, ifn == null ? "all" : ifn.getName());

            return channel;
        } catch (IOException e) {
            throw new JournalNetworkException("Cannot open server socket", e);
        }
    }
}
