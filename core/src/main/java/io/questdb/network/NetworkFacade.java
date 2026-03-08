/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
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

    boolean bindUdp(long fd, int ipv4Address, int port);

    long bumpFdCount(int fd);

    int close(long fd);

    void close(long fd, Log logger);

    void configureKeepAlive(long fd);

    int configureLinger(long fd, int seconds);

    void configureNoLinger(long fd);

    int configureNonBlocking(long fd);

    int connect(long fd, long pSockaddr);

    int connectAddrInfo(long fd, long pAddrInfo);

    int errno();

    void freeAddrInfo(long pAddrInfo);

    void freeMsgHeaders(long msgVec);

    void freeSockAddr(long pSockaddr);

    long getAddrInfo(LPSZ host, int port);

    long getAddrInfo(CharSequence host, int port);

    long getMMsgBuf(long msg);

    long getMMsgBufLen(long msg);

    long getPeerIP(long fd);

    int getSndBuf(long fd);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    boolean join(long fd, int bindIPv4, int groupIPv4);

    void listen(long serverFd, int backlog);

    long msgHeaders(int msgBufferSize, int msgCount);

    int parseIPv4(CharSequence ipv4Address);

    int peekRaw(long fd, long buffer, int bufferLen);

    int recvRaw(long fd, long buffer, int bufferLen);

    @SuppressWarnings("SpellCheckingInspection")
    int recvmmsgRaw(long fd, long msgVec, int msgCount);

    int resolvePort(long fd);

    int sendRaw(long fd, long buffer, int bufferLen);

    int sendToRaw(long fd, long lo, int len, long socketAddress);

    int setMulticastInterface(long fd, CharSequence address);

    int setMulticastInterface(long fd, int ipv4Address);

    int setMulticastLoop(long fd, boolean loop);

    int setMulticastTtl(long fd, int ttl);

    int setRcvBuf(long fd, int size);

    int setReusePort(long fd);

    boolean setSndBuf(long fd, int size);

    int setTcpNoDelay(long fd, boolean noDelay);

    int shutdown(long fd, int how);

    long sockaddr(int address, int port);

    long sockaddr(CharSequence address, int port);

    long socketTcp(boolean blocking);

    long socketUdp();

    /**
     * Returns true if a disconnect happened, false otherwise.
     *
     * @param fd         file descriptor
     * @param buffer     test buffer
     * @param bufferSize test buffer size
     * @return true if a disconnect happened, false otherwise
     */
    boolean testConnection(long fd, long buffer, int bufferSize);
}
