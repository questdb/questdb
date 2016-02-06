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

import com.nfsdb.log.Log;
import com.nfsdb.log.LogFactory;
import com.nfsdb.misc.Files;
import com.nfsdb.misc.Os;
import com.nfsdb.misc.Unsafe;

import java.io.Closeable;

public final class Kqueue implements Closeable {
    public static final short EVFILT_READ;
    public static final int EV_EOF = -32751;
    public static final int NUM_KEVENTS = 1024;
    private static final short EVFILT_WRITE;
    private static final Log LOG = LogFactory.getLog(Kqueue.class);
    private static final short SIZEOF_KEVENT;
    private static final short FD_OFFSET;
    private static final short FILTER_OFFSET;
    private static final short FLAGS_OFFSET;
    private static final short DATA_OFFSET;
    private static final short EV_ADD;
    private static final short EV_ONESHOT;
    private final long eventList;
    private final int kq;

    public Kqueue() {
        this.eventList = Unsafe.getUnsafe().allocateMemory(SIZEOF_KEVENT * NUM_KEVENTS);
        this.kq = kqueue();
    }

    public static native int kevent(int kq, long changeList, int nChanges, long eventList, int nEvents);

    @Override
    public void close() {
        if (Files.close(this.kq) < 0) {
            LOG.error().$("Cannot close kqueue ").$(this.kq).$();
        }
    }

    public long getData(int index) {
        return Unsafe.getUnsafe().getLong(offset(index) + DATA_OFFSET);
    }

    public int getFd(int index) {
        return (int) Unsafe.getUnsafe().getLong(offset(index) + FD_OFFSET);
    }

    public int getFilter(int index) {
        return Unsafe.getUnsafe().getShort(offset(index) + FILTER_OFFSET);
    }

    public int getFlags(int index) {
        return Unsafe.getUnsafe().getShort(offset(index) + FLAGS_OFFSET);
    }

    public void listen(int sfd) {
        final long p = commonFd(0, sfd, 0);
        Unsafe.getUnsafe().putShort(p + FILTER_OFFSET, EVFILT_READ);
        Unsafe.getUnsafe().putShort(p + FLAGS_OFFSET, EV_ADD);
        register(1);
    }

    public int poll() {
        return kevent(kq, 0, 0, eventList, NUM_KEVENTS);
    }

    public void readFD(int index, int fd, long data) {
        final long p = commonFd(index, fd, data);
        Unsafe.getUnsafe().putShort(p + FILTER_OFFSET, EVFILT_READ);
        Unsafe.getUnsafe().putShort(p + FLAGS_OFFSET, (short) (EV_ADD | EV_ONESHOT));
    }

    public void register(int n) {
        kevent(kq, eventList, n, 0, 0);
    }

    public void writeFD(int index, int fd, long data) {
        final long p = commonFd(index, fd, data);
        Unsafe.getUnsafe().putShort(p + FILTER_OFFSET, EVFILT_WRITE);
        Unsafe.getUnsafe().putShort(p + FLAGS_OFFSET, (short) (EV_ADD | EV_ONESHOT));
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

    private long commonFd(int index, int fd, long data) {
        final long p = offset(index);
        Unsafe.getUnsafe().putLong(p + FD_OFFSET, fd);
        Unsafe.getUnsafe().putLong(p + DATA_OFFSET, data);
        return p;
    }

    private long offset(int index) {
        return eventList + SIZEOF_KEVENT * (long) index;
    }

    static {
        Os.init();
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
