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

import io.questdb.std.Os;

public class EpollFacadeImpl implements EpollFacade {
    public static final EpollFacadeImpl INSTANCE = new EpollFacadeImpl();

    @Override
    public int epollCreate() {
        return EpollAccessor.epollCreate();
    }

    @Override
    public int epollCtl(int epFd, int op, int fd, long eventPtr) {
        return EpollAccessor.epollCtl(epFd, op, fd, eventPtr);
    }

    @Override
    public int epollWait(int epfd, long eventPtr, int eventCount, int timeout) {
        return EpollAccessor.epollWait(epfd, eventPtr, eventCount, timeout);
    }

    @Override
    public int errno() {
        return Os.errno();
    }

    @Override
    public int eventFd() {
        return EpollAccessor.eventFd();
    }

    @Override
    public NetworkFacade getNetworkFacade() {
        return NetworkFacadeImpl.INSTANCE;
    }

    @Override
    public long readEventFd(int fd) {
        return EpollAccessor.readEventFd(fd);
    }

    @Override
    public int writeEventFd(int fd) {
        return EpollAccessor.writeEventFd(fd);
    }
}
