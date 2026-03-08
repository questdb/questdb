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
import io.questdb.network.IOOperation;
import io.questdb.network.Kqueue;
import io.questdb.network.SocketFactory;
import io.questdb.std.Misc;

public class HttpClientOsx extends HttpClient {
    private Kqueue kqueue;

    public HttpClientOsx(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        super(configuration, socketFactory);
        this.kqueue = new Kqueue(
                configuration.getKQueueFacade(),
                configuration.getWaitQueueCapacity()
        );
    }

    @Override
    public void close() {
        super.close();
        this.kqueue = Misc.free(kqueue);
    }

    @Override
    protected void ioWait(int timeout, int op) {
        kqueue.setWriteOffset(0);
        if (op == IOOperation.READ) {
            kqueue.readFD(socket.getFd(), 0);
        } else {
            kqueue.writeFD(socket.getFd(), 0);
        }

        // 1 = always one FD, we are a single threaded network client
        if (kqueue.register(1) != 0) {
            throw new HttpClientException("could not register with kqueue [op=").put(op)
                    .put(", errno=").errno(nf.errno())
                    .put(']');
        }
        dieWaiting(kqueue.poll(timeout));
    }

    @Override
    protected void setupIoWait() {
        // no-op on OSX
    }
}
