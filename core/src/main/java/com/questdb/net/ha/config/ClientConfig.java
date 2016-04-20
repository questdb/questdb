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
import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.net.ha.mcast.OnDemandAddressPoller;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@SuppressFBWarnings("CD_CIRCULAR_DEPENDENCY")
public class ClientConfig extends NetworkConfig {

    private static final Log LOG = LogFactory.getLog(ClientConfig.class);
    private final ClientReconnectPolicy reconnectPolicy = new ClientReconnectPolicy();
    private int soSndBuf = 8192;
    private boolean keepAlive = true;
    private boolean tcpNoDelay = true;
    private int linger = 0;
    private long connectionTimeout = 500; //millis

    public ClientConfig() {
        this(null);
    }

    public ClientConfig(String hosts) {
        super();
        getSslConfig().setClient(true);
        if (hosts != null && hosts.length() > 0) {
            parseNodes(hosts);
        }
    }

    public long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public ClientReconnectPolicy getReconnectPolicy() {
        return reconnectPolicy;
    }

    public DatagramChannelWrapper openDatagramChannel() throws JournalNetworkException {
        return openDatagramChannel(getMultiCastInterface());
    }

    @SuppressFBWarnings({"LII_LIST_INDEXED_ITERATING"})
    public SocketChannel openSocketChannel() throws JournalNetworkException {
        if (getNodeCount() == 0) {
            if (isMultiCastEnabled()) {
                addNode(pollServerAddress());
            } else {
                throw new JournalNetworkException("No server nodes");
            }
        }

        List<ServerNode> nodes = getNodes();

        for (int i = 0, k = nodes.size(); i < k; i++) {
            ServerNode node = nodes.get(i);
            try {
                return openSocketChannel0(node);
            } catch (UnresolvedAddressException | IOException e) {
                LOG.info().$("Node ").$(node).$(" is unavailable [").$(e.getMessage()).$(']').$();
            }
        }

        throw new JournalNetworkException("Could not connect to any node");
    }

    public SocketChannel openSocketChannel(ServerNode node) throws JournalNetworkException {
        try {
            return openSocketChannel0(node);
        } catch (UnresolvedAddressException | IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    private boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    private int getLinger() {
        return linger;
    }

    public void setLinger(int linger) {
        this.linger = linger;
    }

    @SuppressFBWarnings({"MDM_INETADDRESS_GETLOCALHOST"})
    private NetworkInterface getMultiCastInterface() throws JournalNetworkException {
        try {
            if (getIfName() == null) {
                return findExternalNic();
            }

            return NetworkInterface.getByName(getIfName());
        } catch (IOException e) {
            throw new JournalNetworkException(e);
        }
    }

    private int getSoSndBuf() {
        return soSndBuf;
    }

    public void setSoSndBuf(int soSndBuf) {
        this.soSndBuf = soSndBuf;
    }

    private boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public void setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    private SocketChannel openSocketChannel0(ServerNode node) throws IOException {
        InetSocketAddress address = new InetSocketAddress(node.getHostname(), node.getPort());
        SocketChannel channel = SocketChannel.open()
                .setOption(StandardSocketOptions.TCP_NODELAY, isTcpNoDelay())
                .setOption(StandardSocketOptions.SO_KEEPALIVE, getKeepAlive())
                .setOption(StandardSocketOptions.SO_SNDBUF, getSoSndBuf())
                .setOption(StandardSocketOptions.SO_RCVBUF, getSoRcvBuf())
                .setOption(StandardSocketOptions.SO_LINGER, getLinger());

        channel.configureBlocking(false);
        try {
            channel.connect(address);
            long t = System.currentTimeMillis();

            while (!channel.finishConnect()) {
                LockSupport.parkNanos(500000L);
                if (System.currentTimeMillis() - t > connectionTimeout) {
                    throw new IOException("Connection timeout");
                }
            }

            channel.configureBlocking(true);

            LOG.info().$("Connected to ").$(node).$(" [").$(channel.getLocalAddress()).$(']').$();
            return channel;
        } catch (IOException e) {
            channel.close();
            throw e;
        }
    }

    private ServerNode pollServerAddress() throws JournalNetworkException {
        return new OnDemandAddressPoller(this, 235, 230).poll(3, 500, TimeUnit.MILLISECONDS);
    }
}
