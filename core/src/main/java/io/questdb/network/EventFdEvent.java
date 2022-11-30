/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.std.Unsafe;

/**
 * eventfd(2)-based suspend event object. Used on Linux in combination with epoll.
 */
public class EventFdEvent implements SuspendEvent {

    private static final long REF_COUNT_OFFSET;

    private final EpollFacade epf;
    private final int fd;
    @SuppressWarnings("unused")
    private volatile int refCount;

    public EventFdEvent(EpollFacade epf) {
        this.epf = epf;
        int fd = epf.eventFd();
        if (fd < 0) {
            throw NetworkError.instance(epf.errno(), "cannot create eventfd event");
        }
        this.fd = fd;
        epf.getNetworkFacade().bumpFdCount(fd);
        this.refCount = 2;
    }

    @Override
    public void checkTriggered() {
        long value = epf.readEventFd(fd);
        if (value != 1) {
            throw NetworkError.instance(epf.errno())
                .put("unexpected eventfd read value [value=").put(value)
                .put(", fd=").put(fd)
                .put(']');
        }
    }

    @Override
    public void close() {
        final int prevRefCount = Unsafe.getUnsafe().getAndAddInt(this, REF_COUNT_OFFSET, -1);
        if (prevRefCount == 1) {
            epf.getNetworkFacade().close(fd);
        }
    }

    @Override
    public int getFd() {
        return fd;
    }

    @Override
    public void trigger() {
        if (epf.writeEventFd(fd) < 0) {
            throw NetworkError.instance(epf.errno())
                .put("could not write to eventfd [fd=").put(fd)
                .put(']');
        }
    }

    static {
        REF_COUNT_OFFSET = Unsafe.getFieldOffset(EventFdEvent.class, "refCount");
    }
}
