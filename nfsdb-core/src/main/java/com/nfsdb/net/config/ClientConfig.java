/*
 * Copyright (c) 2014-2015. Vlad Ilyushchenko
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

package com.nfsdb.net.config;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.logging.Logger;
import com.nfsdb.net.mcast.OnDemandAddressPoller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.TimeUnit;

public class ClientConfig extends NetworkConfig {

    public static final Logger LOGGER = Logger.getLogger(ClientConfig.class);
    private final ClientReconnectPolicy reconnectPolicy = new ClientReconnectPolicy();
    private int soSndBuf = 8192;
    private boolean keepAlive = true;
    private boolean tcpNoDelay = true;
    private int linger = 0;

    public ClientConfig() {
        this(null);
    }

    public ClientConfig(String hostname) {
        getSslConfig().setClient(true);
        setHostname(hostname);
    }


    public int getSoSndBuf() {
        return soSndBuf;
    }

    public void setSoSndBuf(int soSndBuf) {
        this.soSndBuf = soSndBuf;
    }

    public boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public int getLinger() {
        return linger;
    }

    public void setLinger(int linger) {
        this.linger = linger;
    }

    public ClientReconnectPolicy getReconnectPolicy() {
        return reconnectPolicy;
    }

    public SocketChannel openSocketChannel() throws JournalNetworkException {
        String host = getHostname();
        InetSocketAddress address = host != null ? new InetSocketAddress(host, getPort()) : pollServerAddress();

        try {
            SocketChannel channel = SocketChannel.open(address)
                    .setOption(StandardSocketOptions.TCP_NODELAY, isTcpNoDelay())
                    .setOption(StandardSocketOptions.SO_KEEPALIVE, getKeepAlive())
                    .setOption(StandardSocketOptions.SO_SNDBUF, getSoSndBuf())
                    .setOption(StandardSocketOptions.SO_RCVBUF, getSoRcvBuf())
                    .setOption(StandardSocketOptions.SO_LINGER, getLinger());

            // linux doubles buffer size, which is returned
            if (channel.getOption(StandardSocketOptions.SO_SNDBUF) != getSoSndBuf()
                    && channel.getOption(StandardSocketOptions.SO_SNDBUF) != getSoSndBuf() * 2) {
                LOGGER.warn("SO_SNDBUF value is ignored");
            }
            if (channel.getOption(StandardSocketOptions.SO_RCVBUF) != getSoRcvBuf()
                    && channel.getOption(StandardSocketOptions.SO_RCVBUF) != getSoRcvBuf() * 2) {
                LOGGER.warn("SO_RCVBUF value is ignored");
            }

            LOGGER.info("Connected to %s", address);
            return channel;
        } catch (UnresolvedAddressException e) {
            throw new JournalNetworkException("DNS lookup error: " + address);
        } catch (IOException e) {
            throw new JournalNetworkException(address.toString(), e);
        }
    }

    private InetSocketAddress pollServerAddress() throws JournalNetworkException {
        return new OnDemandAddressPoller(this, 235, 230).poll(3, 500, TimeUnit.MILLISECONDS);
    }
}
