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

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.channels.ServerSocketChannel;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;

public class ServerConfig extends NetworkConfig {
    public static final long SYNC_TIMEOUT = TimeUnit.SECONDS.toMillis(15);
    private static final long DEFAULT_HEARTBEAT_FREQUENCY = TimeUnit.SECONDS.toMillis(5);
    private long heartbeatFrequency = DEFAULT_HEARTBEAT_FREQUENCY;
    private static final int RING_BUFFER_SIZE = 1024;
    private int eventBufferSize = RING_BUFFER_SIZE;
    private static final Logger LOGGER = Logger.getLogger(ServerConfig.class);

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
        InetSocketAddress address;
        if (getIfName() == null) {
            String host = getHostname();
            if (host == null) {
                address = new InetSocketAddress(getPort());
            } else {
                address = new InetSocketAddress(host, getPort());
            }
        } else {
            NetworkInterface ifn = getNetworkInterface();
            Enumeration<InetAddress> addresses = ifn.getInetAddresses();
            if (addresses.hasMoreElements()) {
                address = new InetSocketAddress(addresses.nextElement(), getPort());
            } else {
                throw new JournalNetworkException("Interface does not have addresses: " + getIfName());
            }
        }
        return address;
    }

    public ServerSocketChannel openServerSocketChannel() throws JournalNetworkException {
        try {
            ServerSocketChannel channel;
            InetSocketAddress address = getSocketAddress();
            if (!getSslConfig().isSecure()) {
                channel = ServerSocketChannel.open();
                channel.socket().bind(address);
            } else {
                SSLContext sslContext = getSslConfig().getSslContext();
                SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
                SSLServerSocket socket = (SSLServerSocket) factory.createServerSocket();
                socket.setNeedClientAuth(getSslConfig().isRequireClientAuth());
                socket.bind(address);
                channel = socket.getChannel();
            }
            channel.socket().setSoTimeout(getSoTimeout());
            channel.socket().setReceiveBufferSize(getSoRcvBuf());
            channel.socket().setReuseAddress(isReuseAddress());

            NetworkInterface ifn = NetworkInterface.getByInetAddress(address.getAddress());
            LOGGER.info("Server is now listening on %s [%s]", address, ifn == null ? "all" : ifn.getName());

            return channel;
        } catch (IOException e) {
            throw new JournalNetworkException("Cannot open server socket", e);
        }
    }
}
