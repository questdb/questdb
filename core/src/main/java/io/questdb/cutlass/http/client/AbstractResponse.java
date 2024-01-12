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

package io.questdb.cutlass.http.client;

import io.questdb.std.Vect;

public abstract class AbstractResponse implements Response {
    private final long bufHi;
    private final long bufLo;
    private final int defaultTimeout;
    private int contentLength;
    private long dataHi;
    private long dataLo;
    private boolean receive = true;

    public AbstractResponse(long bufLo, long bufHi, int defaultTimeout) {
        this.bufLo = bufLo;
        this.bufHi = bufHi;
        this.defaultTimeout = defaultTimeout;
    }

    public void begin(long lo, long hi, int contentLength) {
        this.dataLo = lo;
        this.dataHi = hi;
        this.contentLength = contentLength;
        this.receive = (hi - lo) < contentLength;
    }

    @Override
    public long hi() {
        return dataHi;
    }

    @Override
    public long lo() {
        return dataLo;
    }

    @Override
    public void recv(int timeout) {
        while (receive) {
            compactBuffer();
            dataHi += recvOrDie(dataHi, bufHi, timeout);
            receive = (dataHi - dataLo) < contentLength;
        }
    }

    @Override
    public void recv() {
        recv(defaultTimeout);
    }

    private void compactBuffer() {
        // move unprocessed data to the front of the buffer
        // to maximise
        if (dataLo > bufLo) {
            final long len = dataHi - dataLo;
            assert len > -1;
            if (len > 0) {
                Vect.memmove(bufLo, dataLo, len);
            }
            dataLo = bufLo;
            dataHi = bufLo + len;
        }
    }

    protected abstract int recvOrDie(long bufLo, long bufHi, int timeout);
}
