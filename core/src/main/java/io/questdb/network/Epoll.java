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
import io.questdb.log.LogFactory;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public final class Epoll implements Closeable {
    private static final Log LOG = LogFactory.getLog(Epoll.class);

    private final int capacity;
    private final EpollFacade epf;
    private final long epollFd;
    private final long events;
    private long _rPtr;
    private boolean closed = false;

    public Epoll(EpollFacade epf, int capacity) {
        this.epf = epf;
        this.capacity = capacity;
        long size = EpollAccessor.SIZEOF_EVENT * (long) capacity;
        this.events = this._rPtr = Unsafe.calloc(size, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        int epollFd = epf.epollCreate();
        if (epollFd < 0) {
            Unsafe.free(events, size, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
            throw NetworkError.instance(epf.errno(), "could not create epoll");
        }
        this.epollFd = Files.createUniqueFd(epollFd);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        epf.getNetworkFacade().close(epollFd, LOG);
        Unsafe.free(events, EpollAccessor.SIZEOF_EVENT * (long) capacity, MemoryTag.NATIVE_IO_DISPATCHER_RSS);
        closed = true;
    }

    public int control(long fd, long id, int cmd, int event) {
        Unsafe.getUnsafe().putInt(events + EpollAccessor.EVENTS_OFFSET, event | EpollAccessor.EPOLLET | EpollAccessor.EPOLLONESHOT);
        Unsafe.getUnsafe().putLong(events + EpollAccessor.DATA_OFFSET, id);
        return epf.epollCtl(epollFd, cmd, fd, events);
    }

    public long getData() {
        return Unsafe.getUnsafe().getLong(_rPtr + EpollAccessor.DATA_OFFSET);
    }

    public int getEvent() {
        return Unsafe.getUnsafe().getInt(_rPtr + EpollAccessor.EVENTS_OFFSET);
    }

    public void listen(long sfd) {
        Unsafe.getUnsafe().putInt(events + EpollAccessor.EVENTS_OFFSET, EpollAccessor.EPOLLIN | EpollAccessor.EPOLLET);
        Unsafe.getUnsafe().putLong(events + EpollAccessor.DATA_OFFSET, 0);
        if (epf.epollCtl(epollFd, EpollAccessor.EPOLL_CTL_ADD, sfd, events) != 0) {
            throw NetworkError.instance(epf.errno(), "epoll_ctl");
        }
    }

    public int poll(int timeout) {
        return epf.epollWait(epollFd, events, capacity, timeout);
    }

    public int poll() {
        return poll(0);
    }

    public void removeListen(long sfd) {
        Unsafe.getUnsafe().putInt(events + EpollAccessor.EVENTS_OFFSET, EpollAccessor.EPOLLIN | EpollAccessor.EPOLLET);
        Unsafe.getUnsafe().putLong(events + EpollAccessor.DATA_OFFSET, 0);
        if (epf.epollCtl(epollFd, EpollAccessor.EPOLL_CTL_DEL, sfd, events) != 0) {
            throw NetworkError.instance(epf.errno(), "epoll_ctl");
        }
    }

    public void setOffset(int offset) {
        this._rPtr = this.events + offset;
    }
}
