/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.network;

import io.questdb.log.Log;
import io.questdb.std.str.LPSZ;

public interface NetworkFacade {

    void abortAccept(long fd);

    long accept(long serverFd);

    boolean bindTcp(long fd, int address, int port);

    boolean bindTcp(long fd, CharSequence ipv4Address, int port);

    int close(long fd);

    void close(long fd, Log logger);

    void configureNoLinger(long fd);

    int configureLinger(long fd, int seconds);

    int configureNonBlocking(long fd);

    int connect(long fd, long pSockaddr);

    int connectAddrInfo(long fd, long pAddrInfo);

    void freeSockAddr(long pSockaddr);

    void freeAddrInfo(long pAddrInfo);

    long getPeerIP(long fd);

    void listen(long serverFd, int backlog);

    int recv(long fd, long buffer, int bufferLen);

    int peek(long fd, long buffer, int bufferLen);

    int send(long fd, long buffer, int bufferLen);

    int errno();

    long sockaddr(int address, int port);

    int sendTo(long fd, long lo, int len, long socketAddress);

    long socketTcp(boolean blocking);

    long socketUdp();

    boolean bindUdp(long fd, int ipv4Address, int port);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    boolean join(long fd, int bindIPv4, int groupIPv4);

    long sockaddr(CharSequence address, int port);

    long getAddrInfo(LPSZ host, int port);

    long getAddrInfo(CharSequence host, int port);

    int setMulticastInterface(long fd, CharSequence address);

    int setMulticastInterface(long fd, int ipv4Address);

    int setMulticastLoop(long fd, boolean loop);

    int shutdown(long fd, int how);

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

    int resolvePort(long fd);

    boolean setSndBuf(long fd, int size);

    int getSndBuf(long fd);

    int setMulticastTtl(long fd, int ttl);
}
