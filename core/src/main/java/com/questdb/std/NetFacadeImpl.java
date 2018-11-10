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

package com.questdb.std;

public class NetFacadeImpl implements NetFacade {
    public static final NetFacadeImpl INSTANCE = new NetFacadeImpl();

    @Override
    public boolean bindTcp(long fd, CharSequence IPv4Address, int port) {
        return Net.bindTcp(fd, IPv4Address, port);
    }

    @Override
    public boolean bindUdp(long fd, CharSequence IPv4Address, int port) {
        return Net.bindUdp(fd, IPv4Address, port);
    }

    @Override
    public int close(long fd) {
        return Net.close(fd);
    }

    @Override
    public void freeMsgHeaders(long msgVec) {
        Net.freeMsgHeaders(msgVec);
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
    public boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address) {
        return Net.join(fd, bindIPv4Address, groupIPv4Address);
    }

    @Override
    public long msgHeaders(int msgBufferSize, int msgCount) {
        return Net.msgHeaders(msgBufferSize, msgCount);
    }

    @Override
    public int recv(long fd, long buf, int bufLen) {
        return Net.recv(fd, buf, bufLen);
    }

    @Override
    public int recvmmsg(long fd, long msgVec, int msgCount) {
        return Net.recvmmsg(fd, msgVec, msgCount);
    }

    @Override
    public int setRcvBuf(long fd, int size) {
        return Net.setRcvBuf(fd, size);
    }

    @Override
    public long socketUdp() {
        return Net.socketUdp();
    }
}
