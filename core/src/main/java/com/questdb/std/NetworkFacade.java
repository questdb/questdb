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

package com.questdb.std;

public interface NetworkFacade {
    void abortAccept(long fd);

    long accept(long serverFd);

    boolean bindTcp(long fd, int address, int port);

    boolean bindTcp(long fd, CharSequence ipv4Address, int port);

    int close(long fd);

    void configureNoLinger(long fd);

    int configureNonBlocking(long fd);

    int connect(long fd, long sockaddr);

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

    boolean bindUdp(long fd, CharSequence address, int port);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    long sockaddr(CharSequence address, int port);

    int setMulticastInterface(long fd, CharSequence address);

    int setMulticastInterface(long fd, int ipv4Address);

    int setMulticastLoop(long fd, boolean loop);
}
