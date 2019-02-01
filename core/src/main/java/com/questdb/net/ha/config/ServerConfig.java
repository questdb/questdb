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

package com.questdb.net.ha.config;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.ex.JournalNetworkException;

import java.io.IOException;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ServerConfig extends NetworkConfig {
    public static final long SYNC_TIMEOUT = TimeUnit.SECONDS.toMillis(15);
    private static final long DEFAULT_HEARTBEAT_FREQUENCY = TimeUnit.SECONDS.toMillis(5);
    private static final Log LOG = LogFactory.getLog(ServerConfig.class);

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
            LOG.info().$("Server is now listening on ").$(address).$();
            return channel;
        } catch (IOException e) {

            throw new JournalNetworkException("Cannot open server socket [" + address + ']', e);
        }
    }

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
