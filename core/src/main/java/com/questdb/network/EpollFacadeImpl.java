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

import com.questdb.std.Os;

public class EpollFacadeImpl implements EpollFacade {
    public static final EpollFacadeImpl INSTANCE = new EpollFacadeImpl();

    @Override
    public long epollCreate() {
        return EpollAccessor.epollCreate();
    }

    @Override
    public int epollCtl(long epFd, int op, long fd, long eventPtr) {
        return EpollAccessor.epollCtl(epFd, op, fd, eventPtr);
    }

    @Override
    public int epollWait(long epfd, long eventPtr, int eventCount, int timeout) {
        return EpollAccessor.epollWait(epfd, eventPtr, eventCount, timeout);
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public int errno() {
        return Os.errno();
    }
}
