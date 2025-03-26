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

package io.questdb.cutlass.line.tcp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

public class AdaptiveRecvBuffer implements QuietCloseable {
    private static final byte RECV_BUFFER_GROW_FACTOR = 4;
    private static final byte RECV_BUFFER_SHRINK_FACTOR = 2;
    private static Log LOG = LogFactory.getLog(AdaptiveRecvBuffer.class);
    private final LineTcpParser parser;
    private final int tag;
    private long currentBufSize;
    private boolean decrease = false;
    private long initialBufSize;
    private long maxBufferSize;
    private long maxMeasureSize;
    private long recvBufEnd;
    private long recvBufPos;
    private long recvBufStart;
    private long recvBufStartOfMeasurement;

    public AdaptiveRecvBuffer(LineTcpParser parser, int tag) {
        this.parser = parser;
        this.tag = tag;
    }

    public void adjustRecvBuffer(long newBufSize, boolean needShift) {
        LOG.info().$("adjust ILP receive buffer size [currentSize=").$(currentBufSize)
                .$(", newBufferSize=").$(newBufSize)
                .I$();
        long newBufLo = Unsafe.realloc(recvBufStart, currentBufSize, newBufSize, tag);
        long offset = recvBufStart - newBufLo;
        if (needShift) {
            parser.shl(offset);
        }
        recvBufStart = newBufLo;
        recvBufPos -= offset;
        recvBufStartOfMeasurement = recvBufStart;
        recvBufEnd = recvBufStart + newBufSize;
        currentBufSize = newBufSize;
    }

    public void clear() {
        parser.of(recvBufStart);
        recvBufPos = recvBufStart;
        recvBufStartOfMeasurement = recvBufStart;
    }

    @Override
    public void close() {
        Unsafe.free(recvBufStart, currentBufSize, tag);
        recvBufStartOfMeasurement = recvBufEnd = recvBufPos = recvBufStart = 0;
        parser.of(recvBufStart);
    }

    public boolean compactOrGrowBuffer() {
        if (recvBufPos != recvBufEnd) {
            return true;
        }
        // try to compact first
        if (recvBufStartOfMeasurement > recvBufStart) {
            long size = recvBufPos - recvBufStartOfMeasurement;
            long shl = recvBufStartOfMeasurement - recvBufStart;
            Vect.memmove(recvBufStart, recvBufStart + shl, size);
            parser.shl(shl);
            recvBufPos -= shl;
            this.recvBufStartOfMeasurement -= shl;
            recordMaxMeasureSize(size);
            tryToShrinkRecvBuffer(true);
            return true;
        }
        // if the size of a single measurement exceeds maxBufferSize, raise an error
        if (currentBufSize == maxBufferSize) {
            return false;
        }
        // otherwise, grow the current recvBuffer to currentBufSize * RECV_BUFFER_GROW_FACTOR
        adjustRecvBuffer(Math.min(currentBufSize * RECV_BUFFER_GROW_FACTOR, maxBufferSize), true);
        decrease = false;
        return true;
    }

    public long getRecvBufEnd() {
        return recvBufEnd;
    }

    public long getRecvBufPos() {
        return recvBufPos;
    }

    public long getRecvBufStart() {
        return recvBufStart;
    }

    public long getRecvBufStartOfMeasurement() {
        return recvBufStartOfMeasurement;
    }

    public AdaptiveRecvBuffer of(long initialBufSize, long maxBufferSize) {
        assert recvBufStart == 0;
        this.initialBufSize = initialBufSize;
        this.maxBufferSize = maxBufferSize;
        this.recvBufStart = Unsafe.malloc(initialBufSize, tag);
        this.recvBufPos = this.recvBufStart;
        this.recvBufStartOfMeasurement = this.recvBufStart;
        this.recvBufEnd = this.recvBufPos + initialBufSize;
        this.currentBufSize = initialBufSize;
        parser.of(recvBufStart);
        return this;
    }

    public void recordMaxMeasureSize(long maxBufferSize) {
        if (this.maxMeasureSize < maxBufferSize || maxBufferSize == 0) {
            this.maxMeasureSize = maxBufferSize;
        }
    }

    public void setRecvBufPos(long recvBufPos) {
        this.recvBufPos = recvBufPos;
    }

    public void setRecvBufStartOfMeasurement(long recvBufStartOfMeasurement) {
        this.recvBufStartOfMeasurement = recvBufStartOfMeasurement;
    }

    public void startNewMeasurement() {
        recordMaxMeasureSize(parser.getBufferAddress() - recvBufStartOfMeasurement);
        parser.startNextMeasurement();
        recvBufStartOfMeasurement = parser.getBufferAddress();
        // we ran out of buffer, move to start and start parsing new data from socket
        if (recvBufStartOfMeasurement == recvBufPos) {
            tryToShrinkRecvBuffer(false);
            recvBufPos = recvBufStart;
            parser.of(recvBufStart);
            recvBufStartOfMeasurement = recvBufStart;
        }
    }

    /*
     * Attempts to shrink the buffer, if the conditions permit.
     * Condition to shrink: at two consecutive moments (either when the buffer is fully filled or upon commit),
     * no measurement exceeds (currentBufSize / RECV_BUFFER_SHRINK_FACTOR).
     */
    public void tryToShrinkRecvBuffer(boolean shiftParser) {
        if (maxMeasureSize == 0) {
            return;
        }
        if (currentBufSize != initialBufSize && maxMeasureSize < currentBufSize / RECV_BUFFER_SHRINK_FACTOR) {
            if (decrease) {
                adjustRecvBuffer(Math.max(initialBufSize, currentBufSize / RECV_BUFFER_SHRINK_FACTOR), shiftParser);
                decrease = false;
            } else {
                decrease = true;
            }
        } else {
            decrease = false;
        }
        recordMaxMeasureSize(0);
    }
}
