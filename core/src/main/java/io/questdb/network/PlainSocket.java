/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2023 QuestDB
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
    private final int fd;
    private final Log log;
    private final NetworkFacade nf;

    public PlainSocket(NetworkFacade nf, int fd, Log log) {
        this.nf = nf;
        this.fd = fd;
        this.log = log;
    }

    @Override
    public void close() {
        nf.close(fd, log);
    }

    @Override
    public int getFd() {
        return fd;
    }

    @Override
    public boolean isTlsSessionStarted() {
        return false;
    }

    @Override
    public int read() {
        return 0;
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
    public void shutdown(int how) {
        nf.shutdown(fd, how);
    }

    @Override
    public boolean startTlsSession() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean supportsTls() {
        return false;
    }

    @Override
    public boolean wantsRead() {
        return false;
    }

    @Override
    public boolean wantsWrite() {
        return false;
    }

    @Override
    public int write() {
        return 0;
    }
}
