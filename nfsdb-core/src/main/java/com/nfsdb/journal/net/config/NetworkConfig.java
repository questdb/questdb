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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;

public class NetworkConfig {
    public static final int DEFAULT_DATA_PORT = 7075;
    private int port = DEFAULT_DATA_PORT;
    public static final int DEFAULT_DATA_SO_TIMEOUT = 2000;
    private int soTimeout = DEFAULT_DATA_SO_TIMEOUT;
    public static final int DEFAULT_MULTICAST_PORT = 4446;
    private int multicastPort = DEFAULT_MULTICAST_PORT;
    public static final int DEFAULT_MULTICAST_TIME_TO_LIVE = 16;
    private int multicastTimeToLive = DEFAULT_MULTICAST_TIME_TO_LIVE;
    public static final String DEFAULT_MULTICAST_ADDRESS = "230.100.12.1";
    private String multicastAddress = DEFAULT_MULTICAST_ADDRESS;
    public static final byte ADDRESS_REQUEST_PREFIX = 0x3F;
    public static final byte ADDRESS_RESPONSE_PREFIX = 0x4F;
    public static final int DEFAULT_SO_RCVBUF = 8192;
    private int soRcvBuf = DEFAULT_SO_RCVBUF;
    private final SslConfig sslConfig = new SslConfig();
    private String ifName = null;
    private String hostname = null;
    private boolean reuseAddress = true;


    public NetworkConfig() {
    }

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSoTimeout() {
        return soTimeout;
    }

    public void setSoTimeout(int soTimeout) {
        this.soTimeout = soTimeout;
    }

    public int getMulticastPort() {
        return multicastPort;
    }

    public void setMulticastPort(int multicastPort) {
        this.multicastPort = multicastPort;
    }

    public String getMulticastAddress() {
        return multicastAddress;
    }

    public void setMulticastAddress(String multicastAddress) {
        this.multicastAddress = multicastAddress;
    }

    public String getIfName() {
        return ifName;
    }

    public void setIfName(String ifName) {
        this.ifName = ifName;
    }

    public int getMulticastTimeToLive() {
        return multicastTimeToLive;
    }

    public void setMulticastTimeToLive(int multicastTimeToLive) {
        this.multicastTimeToLive = multicastTimeToLive;
    }

    public int getSoRcvBuf() {
        return soRcvBuf;
    }

    public void setSoRcvBuf(int soRcvBuf) {
        this.soRcvBuf = soRcvBuf;
    }

    public boolean isReuseAddress() {
        return reuseAddress;
    }

    public void setReuseAddress(boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public NetworkInterface getNetworkInterface() throws JournalNetworkException {
        if (ifName == null) {
            throw new JournalNetworkException("Interface name is not set");
        }
        try {
            NetworkInterface ifn = NetworkInterface.getByName(ifName);
            if (ifn == null) {
                throw new JournalNetworkException("Cannot find interface: " + getIfName());
            }

            if (!ifn.isUp()) {
                throw new JournalNetworkException("Interface is DOWN: " + getIfName());
            }
            return ifn;
        } catch (SocketException e) {
            throw new JournalNetworkException("Cannot get network interface", e);
        }
    }

    public InetSocketAddress getMulticastSocketAddress() {
        return new InetSocketAddress(getMulticastAddress(), getMulticastPort());
    }

    public MulticastSocket openMulticastSocket() throws JournalNetworkException {
        try {
            InetSocketAddress multicastAddress = getMulticastSocketAddress();
            MulticastSocket socket = new MulticastSocket(new InetSocketAddress("0.0.0.0", multicastAddress.getPort()));
            socket.setTimeToLive(getMulticastTimeToLive());
            if (getIfName() == null) {
                socket.joinGroup(multicastAddress.getAddress());
            } else {
                NetworkInterface ifn = getNetworkInterface();
                socket.joinGroup(multicastAddress, ifn);
            }
            return socket;
        } catch (IOException e) {
            throw new JournalNetworkException("Cannot open multicast socket", e);
        }
    }

    public SslConfig getSslConfig() {
        return sslConfig;
    }
}
