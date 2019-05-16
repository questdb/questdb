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

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Files;
import com.questdb.std.Unsafe;

import java.io.Closeable;

public final class Epoll implements Closeable {
    private static final Log LOG = LogFactory.getLog(Epoll.class);
    private final long events;
    private final long epollFd;
    private final int capacity;
    private final EpollFacade epf;
    private boolean closed = false;
    private long _rPtr;

    public Epoll(EpollFacade epf, int capacity) {
        this.epf = epf;
        this.capacity = capacity;
        this.events = _rPtr = Unsafe.calloc(EpollAccessor.SIZEOF_EVENT * (long) capacity);
        // todo: this can be unsuccessful
        this.epollFd = epf.epollCreate();
        if (this.epollFd != -1) {
            Files.bumpFileCount();
        }
    }

    public Epoll(int capacity) {
        this(EpollFacadeImpl.INSTANCE, capacity);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        epf.getNetworkFacade().close(epollFd, LOG);
        Unsafe.free(events, EpollAccessor.SIZEOF_EVENT * (long) capacity);
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

    public int poll() {
        return epf.epollWait(epollFd, events, capacity, 0);
    }

    public void setOffset(int offset) {
        this._rPtr = this.events + (long) offset;
    }
}
