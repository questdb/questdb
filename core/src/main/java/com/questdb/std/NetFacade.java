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

public interface NetFacade {
    boolean bindTcp(long fd, CharSequence IPv4Address, int port);

    boolean bindUdp(long fd, CharSequence IPv4Address, int port);

    int close(long fd);

    void freeMsgHeaders(long msgVec);

    long getMMsgBuf(long msg);

    long getMMsgBufLen(long msg);

    boolean join(long fd, CharSequence bindIPv4Address, CharSequence groupIPv4Address);

    long msgHeaders(int msgBufferSize, int msgCount);

    int recv(long fd, long buf, int bufLen);

    @SuppressWarnings("SpellCheckingInspection")
    int recvmmsg(long fd, long msgVec, int msgCount);

    int setRcvBuf(long fd, int size);

    long socketUdp();
}
