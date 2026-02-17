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

package io.questdb.cutlass.http;

import io.questdb.network.PeerDisconnectedException;
import io.questdb.network.PeerIsSlowToReadException;
import io.questdb.network.ServerDisconnectException;
import io.questdb.std.Mutable;
import io.questdb.std.Unsafe;

public class ChunkedContentParser implements Mutable {
    private long chunkSize = -1;

    @Override
    public void clear() {
        chunkSize = -1;
    }

    public long handleRecv(long lo, long hi, HttpPostPutProcessor processor)
            throws PeerIsSlowToReadException, ServerDisconnectException, PeerDisconnectedException {

        while (lo < hi) {
            if (chunkSize == 0 || chunkSize == -1) {
                lo = parseChunkLength(lo, hi, chunkSize == -1);
                if (lo > -1) {
                    if (chunkSize == 0) {
                        // Should be all done, left to see trailing \r\n
                        return waitTrailingEol(lo, hi);
                    }
                }
            } else if (chunkSize == -2) {
                return waitTrailingEol(lo, hi);
            }

            if (lo < 0) {
                return lo;
            }

            long n = Math.min(hi - lo, chunkSize);
            processor.onChunk(lo, lo + n);
            chunkSize -= n;
            lo += n;
        }
        return hi;
    }

    private static boolean isEol(long lo, long hi) {
        return hi - lo > 1 && Unsafe.getUnsafe().getByte(lo) == '\r'
                && Unsafe.getUnsafe().getByte(lo + 1) == '\n';
    }

    private long parseChunkLength(long lo, long hi, boolean skipEol) {
        long startLo = lo;
        long chunkSize = 0;

        if (!skipEol) {
            if (!isEol(lo, hi)) {
                // Chunk size must start with \r\n
                return -startLo;
            }
            lo += 2;
        }

        while (lo < hi) {
            byte b = Unsafe.getUnsafe().getByte(lo++);
            if (b >= '0' && b <= '9') {
                chunkSize = chunkSize * 16 + (b - '0');
            } else if (b >= 'a' && b <= 'f') {
                chunkSize = chunkSize * 16 + (b - 'a' + 10);
            } else if (b >= 'A' && b <= 'F') {
                chunkSize = chunkSize * 16 + (b - 'A' + 10);
            } else if (b == '\r' && hi - lo < 1) {
                // Not enough buffer to parse \r\n
                break;
            } else if (isEol(lo - 1, hi) && lo > startLo + 1) {
                // parsed OK
                this.chunkSize = chunkSize;
                return lo + 1;
            } else {
                // Invalid characters in chunk size
                this.chunkSize = -2;
                return Long.MIN_VALUE;
            }
        }
        return -startLo;
    }

    private long waitTrailingEol(long lo, long hi) {
        if (hi - lo == 2) {
            if (isEol(lo, hi)) {
                return Long.MAX_VALUE;
            }
            return Long.MIN_VALUE;
        } else if (hi - lo <= 1) {
            // Missing \r\n
            chunkSize = -2;
            return -lo;
        }
        // Protocol violation
        return Long.MIN_VALUE;
    }
}
