/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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
import io.questdb.std.Unsafe;

import java.io.Closeable;

public final class Kqueue implements Closeable {
    private static final Log LOG = LogFactory.getLog(Kqueue.class);
    private final long changeList;
    private final long eventList;
    private final int kq;
    private final int capacity;
    private final KqueueFacade kqf;
    private final int bufferSize;
    private long writeAddress;
    private long readAddress;

    public Kqueue(int capacity) {
        this(KqueueFacadeImpl.INSTANCE, capacity);
    }

    public Kqueue(KqueueFacade kqf, int capacity) {
        this.kqf = kqf;
        this.capacity = capacity;
        this.bufferSize = KqueueAccessor.SIZEOF_KEVENT * capacity;
        this.changeList = this.writeAddress = Unsafe.calloc(bufferSize);
        this.eventList = this.readAddress = Unsafe.calloc(bufferSize);
        this.kq = kqf.kqueue();
        Files.bumpFileCount(this.kq);
    }

    @Override
    public void close() {
        kqf.getNetworkFacade().close(kq, LOG);
        Unsafe.free(this.changeList, bufferSize);
        Unsafe.free(this.eventList, bufferSize);
    }

    public long getData() {
        return Unsafe.getUnsafe().getLong(readAddress + KqueueAccessor.DATA_OFFSET);
    }

    public int getFd() {
        return (int) Unsafe.getUnsafe().getLong(readAddress + KqueueAccessor.FD_OFFSET);
    }

    public int getFilter() {
        return Unsafe.getUnsafe().getShort(readAddress + KqueueAccessor.FILTER_OFFSET);
    }

    public int listen(long sfd) {
        writeAddress = changeList;
        commonFd(sfd, 0);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, KqueueAccessor.EV_ADD);
        return register(1);
    }

    public int removeListen(long sfd) {
        writeAddress = changeList;
        commonFd(sfd, 0);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, KqueueAccessor.EV_DELETE);
        return register(1);
    }

    public int poll() {
        return kqf.kevent(kq, 0, 0, eventList, capacity);
    }

    public void readFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_READ);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    public int register(int n) {
        return kqf.kevent(kq, changeList, n, 0, 0);
    }

    public void setReadOffset(int offset) {
        this.readAddress = eventList + offset;
    }

    public void setWriteOffset(int offset) {
        this.writeAddress = changeList + offset;
    }

    public void writeFD(int fd, long data) {
        commonFd(fd, data);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FILTER_OFFSET, KqueueAccessor.EVFILT_WRITE);
        Unsafe.getUnsafe().putShort(writeAddress + KqueueAccessor.FLAGS_OFFSET, (short) (KqueueAccessor.EV_ADD | KqueueAccessor.EV_ONESHOT));
    }

    private void commonFd(long fd, long data) {
        Unsafe.getUnsafe().putLong(writeAddress + KqueueAccessor.FD_OFFSET, fd);
        Unsafe.getUnsafe().putLong(writeAddress + KqueueAccessor.DATA_OFFSET, data);
    }
}
