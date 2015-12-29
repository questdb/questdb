/*
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
 */

package com.nfsdb.net.ha.config;

import com.nfsdb.exceptions.JournalNetworkException;
import com.nfsdb.logging.Logger;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING", "LII_LIST_INDEXED_ITERATING"})
public class ServerConfig extends NetworkConfig {
    public static final long SYNC_TIMEOUT = TimeUnit.SECONDS.toMillis(15);
    private static final long DEFAULT_HEARTBEAT_FREQUENCY = TimeUnit.SECONDS.toMillis(5);
    private static final Logger LOGGER = Logger.getLogger(ServerConfig.class);

    private long heartbeatFrequency = DEFAULT_HEARTBEAT_FREQUENCY;

    public long getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    public void setHeartbeatFrequency(long heartbeatFrequency) {
        this.heartbeatFrequency = heartbeatFrequency;
    }

    public NetworkInterface getMultiCastInterface(int instance) throws JournalNetworkException {
        NetworkInterface ifn = getMultiCastInterface0(instance);
        try {
            if (ifn.isUp()) {
                return ifn;
            }
        } catch (SocketException e) {
            throw new JournalNetworkException(e);
        }

        throw new JournalNetworkException("Network interface " + ifn.getName() + " is down");
    }

    @SuppressFBWarnings({"MDM_INETADDRESS_GETLOCALHOST"})
    public InetSocketAddress getSocketAddress(int instance) throws JournalNetworkException {
        ServerNode node = getNodeByUID(instance);

        try {
            // default IP address and port
            if (node == null && getIfName() == null) {
                NetworkInterface ifn = NetworkInterface.getByInetAddress(
                        InetAddress.getByName(InetAddress.getLocalHost().getHostName()
                        ));

                return ifn == null || hasIPv4Address(ifn) ? new InetSocketAddress("0.0.0.0", DEFAULT_DATA_PORT) : new InetSocketAddress(InetAddress.getByName("0:0:0:0:0:0:0:0"), DEFAULT_DATA_PORT);
            }

            // we have hostname
            if (node != null) {
                return new InetSocketAddress(node.getHostname(), node.getPort());
            }

            // find first IPv4 or IPv6 address on given interface
            List<InterfaceAddress> addresses = NetworkInterface.getByName(getIfName()).getInterfaceAddresses();
            for (int i = 0, sz = addresses.size(); i < sz; i++) {
                InetAddress addr = addresses.get(i).getAddress();
                if (addr instanceof Inet4Address || addr instanceof Inet6Address) {
                    return new InetSocketAddress(addr, DEFAULT_DATA_PORT);
                }
            }
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }

        throw new JournalNetworkException("There are no usable IP addresses on " + getIfName());
    }

    public DatagramChannelWrapper openDatagramChannel(int instance) throws JournalNetworkException {
        return openDatagramChannel(getMultiCastInterface(instance));
    }

    public ServerSocketChannel openServerSocketChannel(int instance) throws JournalNetworkException {
        InetSocketAddress address = null;
        try {
            address = getSocketAddress(instance);
            ServerSocketChannel channel = ServerSocketChannel.open().bind(address).setOption(StandardSocketOptions.SO_RCVBUF, getSoRcvBuf());
            LOGGER.info("Server is now listening on %s", address);
            return channel;
        } catch (IOException e) {

            throw new JournalNetworkException("Cannot open server socket [" + address + ']', e);
        }
    }

    @SuppressFBWarnings({"MDM_INETADDRESS_GETLOCALHOST"})
    private NetworkInterface getMultiCastInterface0(int instance) throws JournalNetworkException {
        ServerNode node = getNodeByUID(instance);

        try {
            if (node == null && getIfName() == null) {
                return findExternalNic();
            }

            if (node != null && getIfName() == null) {
                InetAddress address = InetAddress.getByName(node.getHostname());
                if (!address.isAnyLocalAddress()) {
                    return NetworkInterface.getByInetAddress(address);
                } else {
                    return findExternalNic();
                }
            }

            return NetworkInterface.getByName(getIfName());
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    private boolean hasIPv4Address(NetworkInterface ifn) {
        List<InterfaceAddress> addresses = ifn.getInterfaceAddresses();
        for (int i = 0, sz = addresses.size(); i < sz; i++) {
            if (addresses.get(i).getAddress() instanceof Inet4Address) {
                return true;
            }
        }
        return false;
    }
}
