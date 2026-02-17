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
import io.questdb.std.Files;
import io.questdb.std.Os;
import io.questdb.std.str.LPSZ;

public class NetworkFacadeImpl implements NetworkFacade {
    public static final NetworkFacade INSTANCE = new NetworkFacadeImpl();

    @Override
    public void abortAccept(long fd) {
        Net.abortAccept(fd);
    }

    @Override
    public long accept(long serverFd) {
        return Net.accept(serverFd);
    }

    @Override
    public boolean bindTcp(long fd, int address, int port) {
        return Net.bindTcp(fd, address, port);
    }

    @Override
    public boolean bindTcp(long fd, CharSequence ipv4Address, int port) {
        return Net.bindTcp(fd, ipv4Address, port);
    }

    @Override
    public boolean bindUdp(long fd, int ipv4Address, int port) {
        return Net.bindUdp(fd, ipv4Address, port);
    }

    @Override
    public long bumpFdCount(int fd) {
        return Files.createUniqueFd(fd);
    }

    @Override
    public int close(long fd) {
        return Net.close(fd);
    }

    @Override
    public void close(long fd, Log logger) {
        if (close(fd) != 0) {
            logger.error().$("could not close [fd=").$(fd).$(", errno=").$(errno()).I$();
        }
    }

    @Override
    public void configureKeepAlive(long fd) {
        Net.configureKeepAlive(fd);
    }

    @Override
    public int configureLinger(long fd, int seconds) {
        return Net.configureLinger(fd, seconds);
    }

    @Override
    public void configureNoLinger(long fd) {
        Net.configureNoLinger(fd);
    }

    @Override
    public int configureNonBlocking(long fd) {
        return Net.configureNonBlocking(fd);
    }

    @Override
    public int connect(long fd, long pSockaddr) {
        return Net.connect(fd, pSockaddr);
    }

    @Override
    public int connectAddrInfo(long fd, long pAddrInfo) {
        return Net.connectAddrInfo(fd, pAddrInfo);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public void freeAddrInfo(long pAddrInfo) {
        Net.freeAddrInfo(pAddrInfo);
    }

    @Override
    public void freeMsgHeaders(long msgVec) {
        Net.freeMsgHeaders(msgVec);
    }

    @Override
    public void freeSockAddr(long pSockaddr) {
        Net.freeSockAddr(pSockaddr);
    }

    @Override
    public long getAddrInfo(LPSZ host, int port) {
        return Net.getAddrInfo(host, port);
    }

    @Override
    public long getAddrInfo(CharSequence host, int port) {
        return Net.getAddrInfo(host, port);
    }

    @Override
    public long getMMsgBuf(long msg) {
        return Net.getMMsgBuf(msg);
    }

    @Override
    public long getMMsgBufLen(long msg) {
        return Net.getMMsgBufLen(msg);
    }

    @Override
    public long getPeerIP(long fd) {
        return Net.getPeerIP(fd);
    }

    @Override
    public int getSndBuf(long fd) {
        return Net.getSndBuf(fd);
    }

    @Override
    public boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return Net.join(fd, bindIPv4Address, groupIPv4Address);
    }

    @Override
    public boolean join(long fd, int bindIPv4, int groupIPv4) {
        return Net.join(fd, bindIPv4, groupIPv4);
    }

    @Override
    public void listen(long serverFd, int backlog) {
        Net.listen(serverFd, backlog);
    }

    @Override
    public long msgHeaders(int msgBufferSize, int msgCount) {
        return Net.msgHeaders(msgBufferSize, msgCount);
    }

    @Override
    public int parseIPv4(CharSequence ipv4Address) {
        return Net.parseIPv4(ipv4Address);
    }

    @Override
    public int peekRaw(long fd, long buffer, int bufferLen) {
        return Net.peek(fd, buffer, bufferLen);
    }

    @Override
    public int recvRaw(long fd, long buffer, int bufferLen) {
        return Net.recv(fd, buffer, bufferLen);
    }

    @Override
    public int recvmmsgRaw(long fd, long msgVec, int msgCount) {
        return Net.recvmmsg(fd, msgVec, msgCount);
    }

    @Override
    public int resolvePort(long fd) {
        return Net.resolvePort(fd);
    }

    @Override
    public int sendRaw(long fd, long buffer, int bufferLen) {
        return Net.send(fd, buffer, bufferLen);
    }

    @Override
    public int sendToRaw(long fd, long ptr, int len, long socketAddress) {
        return Net.sendTo(fd, ptr, len, socketAddress);
    }

    @Override
    public int setMulticastInterface(long fd, CharSequence address) {
        return Net.setMulticastInterface(fd, Net.parseIPv4(address));
    }

    @Override
    public int setMulticastInterface(long fd, int ipv4Address) {
        return Net.setMulticastInterface(fd, ipv4Address);
    }

    @Override
    public int setMulticastLoop(long fd, boolean loop) {
        return Net.setMulticastLoop(fd, loop);
    }

    @Override
    public int setMulticastTtl(long fd, int ttl) {
        return Net.setMulticastTtl(fd, ttl);
    }

    @Override
    public int setRcvBuf(long fd, int size) {
        return Net.setRcvBuf(fd, size);
    }

    @Override
    public int setReusePort(long fd) {
        return Net.setReusePort(fd);
    }

    @Override
    public boolean setSndBuf(long fd, int size) {
        return Net.setSndBuf(fd, size) == 0;
    }

    @Override
    public int setTcpNoDelay(long fd, boolean noDelay) {
        return Net.setTcpNoDelay(fd, noDelay);
    }

    @Override
    public int shutdown(long fd, int how) {
        return Net.shutdown(fd, how);
    }

    @Override
    public long sockaddr(int address, int port) {
        return Net.sockaddr(address, port);
    }

    @Override
    public long sockaddr(CharSequence address, int port) {
        return Net.sockaddr(address, port);
    }

    @Override
    public long socketTcp(boolean blocking) {
        return Net.socketTcp(blocking);
    }

    @Override
    public long socketUdp() {
        return Net.socketUdp();
    }

    /**
     * Return true if a disconnect happened, false otherwise.
     **/
    @Override
    public boolean testConnection(long fd, long buffer, int bufferSize) {
        if (fd == -1) {
            return true;
        }

        final int nRead = Net.peek(fd, buffer, bufferSize);
        return nRead < 0;
    }
}
