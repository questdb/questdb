/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

/**
 * eventfd(2)-based suspend event object. Used on Linux in combination with epoll.
 */
public class EventFdSuspendEvent extends SuspendEvent {

    private final EpollFacade epf;
    private final long fd;

    public EventFdSuspendEvent(EpollFacade epf) {
        this.epf = epf;
        int fd = epf.eventFd();
        if (fd < 0) {
            throw NetworkError.instance(epf.errno(), "could not create EventFdSuspendEvent");
        }
        this.fd = epf.getNetworkFacade().bumpFdCount(fd);
    }

    @Override
    public void _close() {
        epf.getNetworkFacade().close(fd);
    }

    @Override
    public boolean checkTriggered() {
        return epf.readEventFd(fd) == 1;
    }

    @Override
    public long getFd() {
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
}
