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

package io.questdb.cutlass.http.client;

import io.questdb.HttpClientConfiguration;
import io.questdb.network.Epoll;
import io.questdb.network.EpollAccessor;
import io.questdb.network.IOOperation;
import io.questdb.network.SocketFactory;
import io.questdb.std.Misc;

public class HttpClientLinux extends HttpClient {
    private Epoll epoll;

    public HttpClientLinux(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        super(configuration, socketFactory);
        epoll = new Epoll(
                configuration.getEpollFacade(),
                configuration.getWaitQueueCapacity()
        );
    }

    @Override
    public void close() {
        super.close();
        epoll = Misc.free(epoll);
    }

    protected void ioWait(int timeout, int op) {
        final int event = op == IOOperation.WRITE ? EpollAccessor.EPOLLOUT : EpollAccessor.EPOLLIN;
        if (epoll.control(socket.getFd(), 0, EpollAccessor.EPOLL_CTL_MOD, event) < 0) {
            throw new HttpClientException("internal error: epoll_ctl failure [op=").put(op)
                    .put(", errno=").put(nf.errno())
                    .put(']');
        }
        dieWaiting(epoll.poll(timeout));
    }

    protected void setupIoWait() {
        if (epoll.control(socket.getFd(), 0, EpollAccessor.EPOLL_CTL_ADD, EpollAccessor.EPOLLOUT) < 0) {
            throw new HttpClientException("internal error: epoll_ctl failure [cmd=add, errno=").put(nf.errno()).put(']');
        }
    }
}
