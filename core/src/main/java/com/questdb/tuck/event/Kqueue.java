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

package com.questdb.tuck.event;

import com.questdb.log.Log;
import com.questdb.log.LogFactory;
import com.questdb.std.Net;
import com.questdb.std.Unsafe;

import java.io.Closeable;

public final class Kqueue implements Closeable {
    public static final short EVFILT_READ;
    public static final short SIZEOF_KEVENT;
    public static final int EV_EOF = -32751;
    private static final short EVFILT_WRITE;
    private static final Log LOG = LogFactory.getLog(Kqueue.class);
    private static final short FD_OFFSET;
    private static final short FILTER_OFFSET;
    private static final short FLAGS_OFFSET;
    private static final short DATA_OFFSET;
    private static final short EV_ADD;
    private static final short EV_ONESHOT;
    private final long eventList;
    private final int kq;
    private final int capacity;
    private long _rPtr;

    public Kqueue(int capacity) {
        this.capacity = capacity;
        this.eventList = this._rPtr = Unsafe.malloc(SIZEOF_KEVENT * (long) capacity);
        this.kq = kqueue();
    }

    public static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents);

    @Override
    public void close() {
        if (Net.close(this.kq) < 0) {
            LOG.error().$("Cannot close kqueue ").$(this.kq).$();
        }
    }

    public long getData() {
        return Unsafe.getUnsafe().getLong(_rPtr + DATA_OFFSET);
    }

    public int getFd() {
        return (int) Unsafe.getUnsafe().getLong(_rPtr + FD_OFFSET);
    }

    public int getFilter() {
        return Unsafe.getUnsafe().getShort(_rPtr + FILTER_OFFSET);
    }

    public int getFlags() {
        return Unsafe.getUnsafe().getShort(_rPtr + FLAGS_OFFSET);
    }

    public void listen(long sfd) {
        _rPtr = eventList;
        commonFd(sfd, 0);
        Unsafe.getUnsafe().putShort(_rPtr + FILTER_OFFSET, EVFILT_READ);
        Unsafe.getUnsafe().putShort(_rPtr + FLAGS_OFFSET, EV_ADD);
        register(1);
    }

    public int poll() {
        return kevent(kq, 0, 0, eventList, capacity);
    }

    public void readFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(_rPtr + FILTER_OFFSET, EVFILT_READ);
        Unsafe.getUnsafe().putShort(_rPtr + FLAGS_OFFSET, (short) (EV_ADD | EV_ONESHOT));
    }

    public void register(int n) {
        kevent(kq, eventList, n, 0, 0);
    }

    public void setOffset(int offset) {
        this._rPtr = eventList + offset;
    }

    public void writeFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(_rPtr + FILTER_OFFSET, EVFILT_WRITE);
        Unsafe.getUnsafe().putShort(_rPtr + FLAGS_OFFSET, (short) (EV_ADD | EV_ONESHOT));
    }

    private static native int kqueue();

    private static native short getEvfiltRead();

    private static native short getEvfiltWrite();

    private static native short getSizeofKevent();

    private static native short getFdOffset();

    private static native short getFilterOffset();

    private static native short getEvAdd();

    private static native short getEvOneshot();

    private static native short getFlagsOffset();

    private static native short getDataOffset();

    private void commonFd(long fd, long data) {
        Unsafe.getUnsafe().putLong(_rPtr + FD_OFFSET, fd);
        Unsafe.getUnsafe().putLong(_rPtr + DATA_OFFSET, data);
    }

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
    }

}
