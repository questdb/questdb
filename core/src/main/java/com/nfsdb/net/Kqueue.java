/*******************************************************************************
 *  _  _ ___ ___     _ _
 * | \| | __/ __| __| | |__
 * | .` | _|\__ \/ _` | '_ \
 * |_|\_|_| |___/\__,_|_.__/
 *
 * Copyright (c) 2014-2015. The NFSdb project and its contributors.
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

import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;

public final class Kqueue {
    private static final short EVFILT_READ;
    private static final short EVFILT_WRITE;
    private static final short SIZEOF_KEVENT;
    private static final short FD_OFFSET;
    private static final short FILTER_OFFSET;
    private static final short FLAGS_OFFSET;
    private static final short EV_ADD;
    private static final short EV_ONESHOT;
    private static final int NUM_KEVENTS = 1024;

    private final long eventList;
    private final int kq;

    public Kqueue() {
        this.eventList = Unsafe.getUnsafe().allocateMemory(SIZEOF_KEVENT * NUM_KEVENTS);
        this.kq = kqueue();
    }

    public static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents);

    public int getFd(int index) {
        final int offset = SIZEOF_KEVENT * index + FD_OFFSET;
        return (int) Unsafe.getUnsafe().getLong(eventList + offset);
    }

    public int getFlags(int index) {
        final int offset = SIZEOF_KEVENT * index + FLAGS_OFFSET;
        return Unsafe.getUnsafe().getInt(eventList + offset);
    }

    public int poll() {
        return kevent(kq, 0, 0, eventList, NUM_KEVENTS);
    }

    public void readFD(int index, int fd) {
        final long p = eventList + SIZEOF_KEVENT * index;
        Unsafe.getUnsafe().putLong(p + FD_OFFSET, fd);
        Unsafe.getUnsafe().putInt(p + FILTER_OFFSET, EVFILT_READ);
        Unsafe.getUnsafe().putInt(p + FLAGS_OFFSET, EV_ADD);
    }

    public int register(int n) {
        return kevent(kq, eventList, n, 0, 0);
    }

    public void writeFD(int index, int fd) {
        final long p = eventList + SIZEOF_KEVENT * index;
        Unsafe.getUnsafe().putLong(p + FD_OFFSET, fd);
        Unsafe.getUnsafe().putInt(p + FILTER_OFFSET, EVFILT_WRITE);
        Unsafe.getUnsafe().putInt(p + FLAGS_OFFSET, EV_ADD | EV_ONESHOT);
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

    static {
        Os.init();
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
    }

}
