/*******************************************************************************
 *    ___                  _   ____  ____
 *   / _ \ _   _  ___  ___| |_|  _ \| __ )
 *  | | | | | | |/ _ \/ __| __| | | |  _ \
 *  | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *   \__\_\\__,_|\___||___/\__|____/|____/
 *
 * Copyright (C) 2014-2018 Appsicle
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

import com.questdb.net.SslConfig;
import com.questdb.std.IntIntHashMap;
import com.questdb.std.ex.JournalNetworkException;

import java.io.IOException;
import java.net.*;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class NetworkConfig {
    public static final int DEFAULT_DATA_PORT = 7075;
    private static final String DEFAULT_MULTICAST_ADDRESS_IPV4 = "230.100.12.4";
    private static final String DEFAULT_MULTICAST_ADDRESS_IPV6_1 = "FF02:231::4500";

    private static final int DEFAULT_MULTICAST_PORT = 4446;
    private static final int DEFAULT_SO_RCVBUF = 1024 * 1024;
    private final List<ServerNode> serverNodes = new ArrayList<>();
    private final IntIntHashMap nodeLookup = new IntIntHashMap();

    private final SslConfig sslConfig = new SslConfig();
    private InetAddress multiCastAddress;
    private int multiCastPort = DEFAULT_MULTICAST_PORT;
    private int soRcvBuf = DEFAULT_SO_RCVBUF;
    private boolean enableMultiCast = true;
    private String ifName = null;
    private NetworkInterface defaultInterface = null;

    public void addNode(ServerNode node) {
        serverNodes.add(node);
        nodeLookup.put(node.getId(), serverNodes.size() - 1);
    }

    public void clearNodes() {
        serverNodes.clear();
        nodeLookup.clear();
    }

    public ServerNode getNodeByPosition(int pos) {
        return serverNodes.get(pos);
    }

    public ServerNode getNodeByUID(int uid) {
        int pos = getNodePosition(uid);
        if (pos == -1) {
            return null;
        }
        return serverNodes.get(pos);
    }

    public int getNodeCount() {
        return serverNodes.size();
    }

    public int getNodePosition(int uid) {
        return nodeLookup.get(uid);
    }

    public SslConfig getSslConfig() {
        return sslConfig;
    }

    public boolean isMultiCastEnabled() {
        return enableMultiCast;
    }

    public void setEnableMultiCast(boolean enableMultiCast) {
        this.enableMultiCast = enableMultiCast;
    }

    private static boolean isInet6(InetAddress address) {
        return address instanceof Inet6Address;
    }

    NetworkInterface findExternalNic() throws JournalNetworkException {
        if (defaultInterface != null) {
            return defaultInterface;
        }
        try {
            Enumeration<NetworkInterface> ifs = NetworkInterface.getNetworkInterfaces();
            int index = Integer.MAX_VALUE;

            while (ifs.hasMoreElements()) {
                NetworkInterface q = ifs.nextElement();
                if (!q.isLoopback() && q.isUp() && q.getIndex() < index) {
                    defaultInterface = q;
                    index = q.getIndex();
                }
            }

            if (defaultInterface == null) {
                throw new JournalNetworkException("Could not find multicast-capable network interfaces");
            }

            return defaultInterface;
        } catch (SocketException e) {
            throw new JournalNetworkException(e);
        }

    }

    private InetAddress getDefaultMultiCastAddress(NetworkInterface ifn) throws JournalNetworkException {
        try {
            if (!ifn.supportsMulticast()) {
                throw new JournalNetworkException("Multicast is not supported on " + ifn.getName());
            }

            if (ifn.getInterfaceAddresses().isEmpty()) {
                throw new JournalNetworkException("No IP addresses assigned to " + ifn.getName());
            }

            if (isInet6(ifn.getInterfaceAddresses().get(0).getAddress())) {
                return InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS_IPV6_1);
            } else {
                return InetAddress.getByName(DEFAULT_MULTICAST_ADDRESS_IPV4);
            }
        } catch (Exception e) {
            throw new JournalNetworkException(e);
        }
    }

    String getIfName() {
        return ifName;
    }

    public void setIfName(String ifName) {
        this.ifName = ifName;
    }

    private InetAddress getMultiCastAddress() {
        return multiCastAddress;
    }

    public void setMultiCastAddress(InetAddress multiCastAddress) {
        this.multiCastAddress = multiCastAddress;
    }

    private int getMultiCastPort() {
        return multiCastPort;
    }

    public void setMultiCastPort(int multiCastPort) {
        this.multiCastPort = multiCastPort;
    }

    List<ServerNode> getServerNodes() {
        return serverNodes;
    }

    int getSoRcvBuf() {
        return soRcvBuf;
    }

    public void setSoRcvBuf(int soRcvBuf) {
        this.soRcvBuf = soRcvBuf;
    }

    DatagramChannelWrapper openDatagramChannel(NetworkInterface ifn) throws JournalNetworkException {
        InetAddress address = getMultiCastAddress();
        if (address == null) {
            address = getDefaultMultiCastAddress(ifn);
        }
        ProtocolFamily family = NetworkConfig.isInet6(address) ? StandardProtocolFamily.INET6 : StandardProtocolFamily.INET;

        try {
            DatagramChannel dc = DatagramChannel.open(family)
                    .setOption(StandardSocketOptions.SO_REUSEADDR, Boolean.TRUE)
                    .setOption(StandardSocketOptions.IP_MULTICAST_IF, ifn)
                    .bind(new InetSocketAddress(getMultiCastPort()));

            dc.join(address, ifn);
            return new DatagramChannelWrapper(dc, new InetSocketAddress(address, getMultiCastPort()));
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    void parseNodes(String nodes) {
        clearNodes();
        String[] parts = nodes.split(",");
        for (int i = 0; i < parts.length; i++) {
            addNode(new ServerNode(i, parts[i]));
        }
    }

}
