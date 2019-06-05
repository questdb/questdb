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

package com.questdb.network;

import com.questdb.log.Log;

public interface NetworkFacade {

    void abortAccept(long fd);

    long accept(long serverFd);

    boolean bindTcp(long fd, int address, int port);

    boolean bindTcp(long fd, CharSequence ipv4Address, int port);

    int close(long fd);

    void close(long fd, Log logger);

    void configureNoLinger(long fd);

    int configureNonBlocking(long fd);

    long connect(long fd, long sockaddr);

    void freeSockAddr(long socketAddress);

    long getPeerIP(long fd);

    void listen(long serverFd, int backlog);

    int recv(long fd, long buffer, int bufferLen);

    int send(long fd, long buffer, int bufferLen);

    int errno();

    long sockaddr(int address, int port);

    int sendTo(long fd, long lo, int len, long socketAddress);

    long socketTcp(boolean blocking);

    long socketUdp();

    boolean bindUdp(long fd, int port);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    boolean join(long fd, int bindIPv4, int groupIPv4);

    long sockaddr(CharSequence address, int port);

    int setMulticastInterface(long fd, CharSequence address);

    int setMulticastInterface(long fd, int ipv4Address);

    int setMulticastLoop(long fd, boolean loop);

    int parseIPv4(CharSequence ipv4Address);

    int setReusePort(long fd);

    int setTcpNoDelay(long fd, boolean noDelay);

    int setRcvBuf(long fd, int size);

    void freeMsgHeaders(long msgVec);

    long getMMsgBuf(long msg);

    long getMMsgBufLen(long msg);

    long msgHeaders(int msgBufferSize, int msgCount);

    @SuppressWarnings("SpellCheckingInspection")
    int recvmmsg(long fd, long msgVec, int msgCount);

    boolean setSndBuf(long fd, int size);

    int getSndBuf(long fd);
}
