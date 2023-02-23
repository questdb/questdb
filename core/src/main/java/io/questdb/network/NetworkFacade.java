/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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

    void abortAccept(int fd);

    int accept(int serverFd);

    boolean bindTcp(int fd, int address, int port);

    boolean bindTcp(int fd, CharSequence ipv4Address, int port);

    boolean bindUdp(int fd, int ipv4Address, int port);

    void bumpFdCount(int fd);

    int close(int fd);

    void close(int fd, Log logger);

    int configureLinger(int fd, int seconds);

    void configureNoLinger(int fd);

    int configureNonBlocking(int fd);

    int connect(int fd, long pSockaddr);

    int connectAddrInfo(int fd, long pAddrInfo);

    int errno();

    void freeAddrInfo(long pAddrInfo);

    void freeMsgHeaders(long msgVec);

    void freeSockAddr(long pSockaddr);

    long getAddrInfo(LPSZ host, int port);

    long getAddrInfo(CharSequence host, int port);

    long getMMsgBuf(long msg);

    long getMMsgBufLen(long msg);

    long getPeerIP(int fd);

    int getSndBuf(int fd);

    boolean join(int fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    boolean join(int fd, int bindIPv4, int groupIPv4);

    void listen(int serverFd, int backlog);

    long msgHeaders(int msgBufferSize, int msgCount);

    int parseIPv4(CharSequence ipv4Address);

    int peek(int fd, long buffer, int bufferLen);

    int recv(int fd, long buffer, int bufferLen);

    @SuppressWarnings("SpellCheckingInspection")
    int recvmmsg(int fd, long msgVec, int msgCount);

    int resolvePort(int fd);

    int send(int fd, long buffer, int bufferLen);

    int sendTo(int fd, long lo, int len, long socketAddress);

    int setMulticastInterface(int fd, CharSequence address);

    int setMulticastInterface(int fd, int ipv4Address);

    int setMulticastLoop(int fd, boolean loop);

    int setMulticastTtl(int fd, int ttl);

    int setRcvBuf(int fd, int size);

    int setReusePort(int fd);

    boolean setSndBuf(int fd, int size);

    int setTcpNoDelay(int fd, boolean noDelay);

    int shutdown(int fd, int how);

    long sockaddr(int address, int port);

    long sockaddr(CharSequence address, int port);

    int socketTcp(boolean blocking);

    int socketUdp();

    /**
     * Returns true if a disconnect happened, false otherwise.
     *
     * @param fd         file descriptor
     * @param buffer     test buffer
     * @param bufferSize test buffer size
     * @return true if a disconnect happened, false otherwise
     */
    boolean testConnection(int fd, long buffer, int bufferSize);
}
