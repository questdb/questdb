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

public final class Kqueue implements Closeable {
    private static final Log LOG = LogFactory.getLog(Kqueue.class);
    private final long eventList;
    private final int kq;
    private final int capacity;
    private final KqueueFacade kqf;
    private long _rPtr;

    public Kqueue(int capacity) {
        this(KqueueFacadeImpl.INSTANCE, capacity);
    }

    public Kqueue(KqueueFacade kqf, int capacity) {
        this.kqf = kqf;
        this.capacity = capacity;
        this.eventList = this._rPtr = Unsafe.malloc(KqueueAccessor.SIZEOF_KEVENT * (long) capacity);
        this.kq = kqf.kqueue();
        if (this.kq != -1) {
            Files.bumpFileCount();
        }
    }

    @Override
    public void close() {
        kqf.getNetworkFacade().close(kq, LOG);
        Unsafe.free(this.eventList, KqueueAccessor.SIZEOF_KEVENT * (long) capacity);
    }

    public long getData() {
        return Unsafe.getUnsafe().getLong(_rPtr + KqueueAccessor.DATA_OFFSET);
    }

    public int getFd() {
        return (int) Unsafe.getUnsafe().getLong(_rPtr + KqueueAccessor.FD_OFFSET);
    }

    public int getFilter() {
        return Unsafe.getUnsafe().getShort(_rPtr + KqueueAccessor.FILTER_OFFSET);
    }

    public int getFlags() {
        return Unsafe.getUnsafe().getShort(_rPtr + KqueueAccessor.FLAGS_OFFSET);
    }

    public int listen(long sfd) {
        _rPtr = eventList;
        commonFd(sfd, 0);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FLAGS_OFFSET, KqueueAccessor.EV_ADD);
        return register(1);
    }

    public int poll() {
        return kqf.kevent(kq, 0, 0, eventList, capacity);
    }

    public void readFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    public int register(int n) {
        return kqf.kevent(kq, eventList, n, 0, 0);
    }

    public void setOffset(int offset) {
        this._rPtr = eventList + offset;
    }

    public void writeFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_WRITE);
        Unsafe.getUnsafe().putShort(_rPtr + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    private void commonFd(long fd, long data) {
        Unsafe.getUnsafe().putLong(_rPtr + KqueueAccessor.FD_OFFSET, fd);
        Unsafe.getUnsafe().putLong(_rPtr + KqueueAccessor.DATA_OFFSET, data);
    }
}
