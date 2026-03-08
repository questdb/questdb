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

package io.questdb.network;

import io.questdb.log.Log;

public class PlainSocket implements Socket {
    private final Log log;
    private final NetworkFacade nf;
    private long fd = -1;

    public PlainSocket(NetworkFacade nf, Log log) {
        this.nf = nf;
        this.log = log;
    }

    @Override
    public void close() {
        if (fd != -1) {
            nf.close(fd, log);
            fd = -1;
        }
    }

    @Override
    public long getFd() {
        return fd;
    }

    @Override
    public boolean isClosed() {
        return fd == -1;
    }

    @Override
    public boolean isMorePlaintextBuffered() {
        return false;
    }

    @Override
    public boolean isTlsSessionStarted() {
        return false;
    }

    @Override
    public void of(long fd) {
        assert this.fd == -1;
        this.fd = fd;
    }

    @Override
    public int recv(long bufferPtr, int bufferLen) {
        return nf.recvRaw(fd, bufferPtr, bufferLen);
    }

    @Override
    public int send(long bufferPtr, int bufferLen) {
        return nf.sendRaw(fd, bufferPtr, bufferLen);
    }

    @Override
    public int shutdown(int how) {
        return nf.shutdown(fd, how);
    }

    @Override
    public void startTlsSession(CharSequence peerName) throws TlsSessionInitFailedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTls() {
        return false;
    }

    @Override
    public int tlsIO(int readinessFlags) {
        return 0;
    }

    @Override
    public boolean wantsTlsRead() {
        return false;
    }

    @Override
    public boolean wantsTlsWrite() {
        return false;
    }
}
