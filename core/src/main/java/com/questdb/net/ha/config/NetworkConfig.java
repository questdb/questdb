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

package com.questdb.net.ha.config;

import com.questdb.ex.JournalNetworkException;
import com.questdb.net.SslConfig;
import com.questdb.std.IntIntHashMap;

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
    private final List<ServerNode> nodes2 = new ArrayList<>();
    private final IntIntHashMap nodeLookup = new IntIntHashMap();

    private final SslConfig sslConfig = new SslConfig();
    private InetAddress multiCastAddress;
    private int multiCastPort = DEFAULT_MULTICAST_PORT;
    private int soRcvBuf = DEFAULT_SO_RCVBUF;
    private boolean enableMultiCast = true;
    private String ifName = null;
    private NetworkInterface defaultInterface = null;

    public void addNode(ServerNode node) {
        nodes2.add(node);
        nodeLookup.put(node.getId(), nodes2.size() - 1);
    }

    public void clearNodes() {
        nodes2.clear();
        nodeLookup.clear();
    }

    public ServerNode getNodeByPosition(int pos) {
        return nodes2.get(pos);
    }

    public ServerNode getNodeByUID(int uid) {
        int pos = getNodePosition(uid);
        if (pos == -1) {
            return null;
        }
        return nodes2.get(pos);
    }

    public int getNodeCount() {
        return nodes2.size();
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

    List<ServerNode> getNodes() {
        return nodes2;
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
        String parts[] = nodes.split(",");
        for (int i = 0; i < parts.length; i++) {
            addNode(new ServerNode(i, parts[i]));
        }
    }

}
