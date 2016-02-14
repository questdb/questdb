/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2016. The NFSdb project and its contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.nfsdb.net;

import com.nfsdb.ex.NetworkError;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;

import java.io.Closeable;

public final class Epoll implements Closeable {
    public static final short SIZEOF_EVENT;
    public static final int EPOLLIN;
    public static final int EPOLLOUT;
    public static final int EPOLL_CTL_ADD;
    public static final int EPOLL_CTL_MOD;
    private static final int NUM_KEVENTS = 1024;
    private static final short DATA_OFFSET;
    private static final short EVENTS_OFFSET;
    private static final int EPOLLONESHOT;
    private static final int EPOLLET;
    private final long events;
    private final long epfd;
    private boolean closed = false;
    private long _rPtr;

    public Epoll() {
        this.events = _rPtr = Unsafe.getUnsafe().allocateMemory(SIZEOF_EVENT * NUM_KEVENTS);
        this.epfd = epollCreate();
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        Files.close(epfd);
        Unsafe.getUnsafe().freeMemory(events);
        closed = true;
    }

    public int control(long fd, long id, int cmd, int event) {
        Unsafe.getUnsafe().putInt(events + EVENTS_OFFSET, event | EPOLLET | EPOLLONESHOT);
        Unsafe.getUnsafe().putLong(events + DATA_OFFSET, id);
        return epollCtl(epfd, cmd, fd, events);
    }

    public long getData() {
        return Unsafe.getUnsafe().getLong(_rPtr + DATA_OFFSET);
    }

    public int getEvent() {
        return Unsafe.getUnsafe().getInt(_rPtr + EVENTS_OFFSET);
    }

    public long getFd() {
        return Unsafe.getUnsafe().getInt(_rPtr + DATA_OFFSET);
    }

    public void listen(long sfd) {
        Unsafe.getUnsafe().putInt(events + EVENTS_OFFSET, EPOLLIN | EPOLLET);
        Unsafe.getUnsafe().putLong(events + DATA_OFFSET, 0);

        if (epollCtl(epfd, EPOLL_CTL_ADD, sfd, events) < 0) {
            throw new NetworkError("Error in epoll_ctl: " + Os.errno());
        }
    }

    public int poll() {
        return epollWait(epfd, events, NUM_KEVENTS, 0);
    }

    public void setOffset(int offset) {
        this._rPtr = this.events + (long) offset;
    }

    private static native long epollCreate();

    private static native int epollCtl(long epfd, int op, long fd, long eventPtr);

    private static native int epollWait(long epfd, long eventPtr, int eventCount, int timeout);

    private static native short getDataOffset();

    private static native short getEventsOffset();

    private static native short getEventSize();

    private static native int getEPOLLIN();

    private static native int getEPOLLET();

    private static native int getEPOLLOUT();

    private static native int getEPOLLONESHOT();

    private static native int getCtlAdd();

    private static native int getCtlMod();

    static {
        Os.init();
        DATA_OFFSET = getDataOffset();
        EVENTS_OFFSET = getEventsOffset();
        SIZEOF_EVENT = getEventSize();
        EPOLLIN = getEPOLLIN();
        EPOLLET = getEPOLLET();
        EPOLLOUT = getEPOLLOUT();
        EPOLLONESHOT = getEPOLLONESHOT();
        EPOLL_CTL_ADD = getCtlAdd();
        EPOLL_CTL_MOD = getCtlMod();
    }
}
