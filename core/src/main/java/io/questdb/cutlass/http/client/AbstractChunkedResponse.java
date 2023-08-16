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

import io.questdb.std.Numbers;
import io.questdb.std.NumericException;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;
import io.questdb.std.str.DirectByteCharSequence;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractChunkedResponse implements HttpClient.Chunk, ChunkedResponse {
    private final long bufLo;
    private final DirectByteCharSequence chunkSize = new DirectByteCharSequence();
    private final int defaultTimeout;
    long available;
    long consumed = 0;
    long dataAddr;
    boolean endOfChunk;
    long size;
    private long dataHi;
    private long dataLo;

    public AbstractChunkedResponse(long bufLo, int defaultTimeout) {
        this.bufLo = bufLo;
        this.defaultTimeout = defaultTimeout;
    }

    public void begin(long lo, long hi) {
        this.endOfChunk = true;
        this.dataLo = lo;
        this.dataHi = hi;
    }

    @Override
    public long hi() {
        return dataAddr + available;
    }

    @Override
    public long lo() {
        return dataAddr;
    }

    @Override
    public HttpClient.Chunk recv(int timeout) {
        return endOfChunk ? chunkStart(timeout) : chunkContinue(timeout);
    }

    @Override
    public HttpClient.Chunk recv() {
        return recv(defaultTimeout);
    }

    @NotNull
    private AbstractChunkedResponse chunkContinue(int timeout) {
        consumed += available;
        compactBuffer();
        final int len = recvOrDie(dataHi, timeout);

        // we are consuming the remaining chunk bytes;
        // chunk size includes `\r\n\`, which must not be included in
        // "available" bytes of the last chunk

        // configure chunk boundaries, chunk size contains two extra bytes for CRLF
        // we must consume and ignore them
        long chunkBytesRemaining = size - consumed;
        boolean endOfChunk = chunkBytesRemaining <= len;
        this.endOfChunk = endOfChunk;
        dataAddr = dataHi;
        available = Math.min(chunkBytesRemaining - 2, len);

        if (endOfChunk) {
            dataHi += len;
            dataLo = dataAddr + available + 2;
        } else {
            // we just consumed the entire buffer
            dataLo = bufLo;
            dataHi = bufLo;
        }
        return this;
    }

    protected abstract int recvOrDie(long buf, int timeout);

    @Nullable
    private AbstractChunkedResponse chunkStart(int timeout) {
        if (parseChunkSize0(dataLo, dataHi)) {
            return size > 2 ? this : null;
        }

        while (true) {
            compactBuffer();
            final int len = recvOrDie(dataHi, timeout);
            dataHi += len;
            // try to read chunk prefix
            if (parseChunkSize0(dataLo, dataHi)) {
                return size > 0 ? this : null;
            }
        }
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

    private boolean parseChunkSize0(long lo, long hi) {
        long p = lo;
        long x = -1;
        long len;
        while (p < hi) {
            char b = (char) Unsafe.getUnsafe().getByte(p++);
            switch (b) {
                case '\r':
                    x = p;
                    break;
                case '\n':
                    if (x == -1) {
                        throw new HttpClientException("malformed chunkedResponse");
                    }
                    // parse the hex chunk length
                    // last char, at(x) is `\r`, exclude
                    chunkSize.of(lo, x - 1);
                    try {
                        len = Numbers.parseHexLong(chunkSize);
                        dataAddr = p;
                        // the chunk length does NOT include trailing chars `\r\n`
                        size = len + 2;
                        available = Math.min(len, hi - p);
                        endOfChunk = len == available;
                        consumed = 0;

                        // if chunk size is smaller that the unprocessed data size
                        // we will reduce unprocessed data size by chunk size; otherwise
                        // we clear the unprocessed data
                        long chunkHi = dataAddr + available;
                        if (chunkHi < dataHi) {
                            dataLo = chunkHi;
                            // this data can also be CRLF only
                            if (available == len) {
                                // skip CRLF
                                dataLo += 2;
                            }
                        } else {
                            dataLo = bufLo;
                            dataHi = bufLo;
                        }

                        return true;
                    } catch (NumericException e) {
                        throw new HttpClientException("could not parse chunkedResponse size");
                    }
                default:
                    break;
            }
        }
        return false;
    }
}
