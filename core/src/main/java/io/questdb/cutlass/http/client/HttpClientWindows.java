/*+*****************************************************************************
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
import io.questdb.network.FDSet;
import io.questdb.network.IOOperation;
import io.questdb.network.SelectFacade;
import io.questdb.network.SocketFactory;
import io.questdb.std.Misc;

public class HttpClientWindows extends HttpClient {
    private final SelectFacade sf;
    private FDSet fdSet;

    public HttpClientWindows(HttpClientConfiguration configuration, SocketFactory socketFactory) {
        super(configuration, socketFactory);
        try {
            // Read the facade before taking the FD set, so nothing between that acquisition and the
            // end of the constructor can throw. That leaves the catch with only the base class to
            // release, and drops a partial-FDSet branch that Linux and macOS CI could never reach:
            // FDSet's first statement touches the Windows-only SelectAccessor natives, so a test
            // cannot get past it to fail anything later.
            this.sf = configuration.getSelectFacade();
            this.fdSet = new FDSet(configuration.getWaitQueueCapacity());
        } catch (Throwable th) {
            // super() has already taken the socket, both buffers and the response parser. A throw
            // here leaves a half-built client the caller never receives, so nothing would close it.
            closeBaseQuietly(th);
            throw th;
        }
    }

    @Override
    public void close() {
        // Free the FD set in a finally: super.close() disconnects first, and an extension socket that
        // throws there used to leak it for good, since close() is the only thing that frees it.
        try {
            super.close();
        } finally {
            this.fdSet = Misc.free(fdSet);
        }
    }

    @Override
    protected void ioWait(int timeout, int op) {
        final long readAddr;
        final long writeAddr;
        fdSet.clear();
        fdSet.add(socket.getFd());
        fdSet.setCount(1);
        if (op == IOOperation.READ) {
            readAddr = fdSet.address();
            writeAddr = 0;
        } else {
            readAddr = 0;
            writeAddr = fdSet.address();
        }
        dieWaiting(sf.select(readAddr, writeAddr, 0, timeout));
    }

    @Override
    protected void setupIoWait() {
    }
}
