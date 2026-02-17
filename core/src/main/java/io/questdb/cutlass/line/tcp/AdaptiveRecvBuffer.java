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

package io.questdb.cutlass.line.tcp;

import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.QuietCloseable;
import io.questdb.std.Unsafe;
import io.questdb.std.Vect;

/**
 * Implements adaptive memory buffer for ingestion.
 * Designed for high-throughput TCP ingestion with variable payload sizes,
 * and automatic compaction/growth based on measurement patterns.
 */
public class AdaptiveRecvBuffer implements QuietCloseable {
    private static final Log LOG = LogFactory.getLog(AdaptiveRecvBuffer.class);
    private static final byte RECV_BUFFER_GROW_FACTOR = 4;
    private static final byte RECV_BUFFER_SHRINK_FACTOR = 2;
    private final int memoryTag;
    private final LineTcpParser parser;
    private long bufEnd;
    private long bufPos;
    private long bufStart;
    private long bufStartOfMeasurement;
    private long currentBufSize;
    private boolean decrease = false;
    private long initialBufSize;
    private long maxBufSize;
    private long maxRecordMeasureSize;

    public AdaptiveRecvBuffer(LineTcpParser parser, int tag) {
        this.parser = parser;
        this.memoryTag = tag;
    }

    public void clear() {
        parser.of(bufStart);
        bufPos = bufStart;
        bufStartOfMeasurement = bufStart;
    }

    @Override
    public void close() {
        decrease = false;
        Unsafe.free(bufStart, currentBufSize, memoryTag);
        bufStartOfMeasurement = bufEnd = bufPos = bufStart = 0;
        parser.of(bufStart);
    }

    public long copyToLocalBuffer(long lo, long hi) {
        long copyLen = Math.min(hi - lo, bufEnd - bufPos);
        assert copyLen > 0;
        Vect.memcpy(bufPos, lo, copyLen);
        bufPos += copyLen;
        return lo + copyLen;
    }

    public long getBufEnd() {
        return bufEnd;
    }

    public long getBufPos() {
        return bufPos;
    }

    public long getBufStart() {
        return bufStart;
    }

    public long getBufStartOfMeasurement() {
        return bufStartOfMeasurement;
    }

    public long getCurrentBufSize() {
        return currentBufSize;
    }

    public long getMaxBufSize() {
        return maxBufSize;
    }

    public AdaptiveRecvBuffer of(long initialBufSize, long maxBufferSize) {
        assert bufStart == 0;
        this.initialBufSize = initialBufSize;
        this.maxBufSize = maxBufferSize;
        this.bufStart = Unsafe.malloc(initialBufSize, memoryTag);
        this.bufPos = this.bufStart;
        this.bufStartOfMeasurement = this.bufStart;
        this.bufEnd = this.bufPos + initialBufSize;
        this.currentBufSize = initialBufSize;
        parser.of(bufStart);
        return this;
    }

    public void recordMaxMeasureSize(long maxBufferSize) {
        if (this.maxRecordMeasureSize < maxBufferSize || maxBufferSize == 0) {
            this.maxRecordMeasureSize = maxBufferSize;
        }
    }

    public void setBufPos(long bufPos) {
        this.bufPos = bufPos;
    }

    public void setBufStartOfMeasurement(long bufStartOfMeasurement) {
        this.bufStartOfMeasurement = bufStartOfMeasurement;
    }

    public void startNewMeasurement() {
        recordMaxMeasureSize(parser.getBufferAddress() - bufStartOfMeasurement);
        parser.startNextMeasurement();
        bufStartOfMeasurement = parser.getBufferAddress();
        // we ran out of buffer, move to start and start parsing new data from socket
        if (bufStartOfMeasurement == bufPos) {
            tryToShrinkRecvBuffer(false);
            bufPos = bufStart;
            parser.of(bufStart);
            bufStartOfMeasurement = bufStart;
        }
    }

    /**
     * Try to compact or expand memory.
     * First attempts to compact by shifting incomplete measurements to buffer start.
     * If compaction is not enough, grows buffer exponentially up to maxBufSize.
     *
     * @return true if operation succeeded, false if buffer exceeded max size
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean tryCompactOrGrowBuffer() {
        if (bufPos != bufEnd) {
            return true;
        }
        // try to compact first
        if (bufStartOfMeasurement > bufStart) {
            long size = bufPos - bufStartOfMeasurement;
            long shl = bufStartOfMeasurement - bufStart;
            Vect.memmove(bufStart, bufStart + shl, size);
            parser.shl(shl);
            bufPos -= shl;
            this.bufStartOfMeasurement -= shl;
            recordMaxMeasureSize(size);
            tryToShrinkRecvBuffer(true);
            return true;
        }
        // if the size of a single measurement exceeds maxBufferSize, raise an error
        if (currentBufSize == maxBufSize) {
            return false;
        }
        // otherwise, grow the current recvBuffer to currentBufSize * RECV_BUFFER_GROW_FACTOR
        adjustBuffer(Math.min(currentBufSize * RECV_BUFFER_GROW_FACTOR, maxBufSize), true);
        decrease = false;
        return true;
    }

    /*
     * Attempts to shrink the buffer, if the conditions permit.
     * Condition to shrink: at two consecutive moments (either when the buffer is fully filled or upon commit),
     * no measurement exceeds (currentBufSize / RECV_BUFFER_SHRINK_FACTOR).
     */
    public void tryToShrinkRecvBuffer(boolean shiftParser) {
        if (maxRecordMeasureSize == 0) {
            return;
        }
        if (currentBufSize != initialBufSize && maxRecordMeasureSize < currentBufSize / RECV_BUFFER_SHRINK_FACTOR) {
            if (decrease) {
                adjustBuffer(Math.max(initialBufSize, currentBufSize / RECV_BUFFER_SHRINK_FACTOR), shiftParser);
                decrease = false;
            } else {
                decrease = true;
            }
        } else {
            decrease = false;
        }
        recordMaxMeasureSize(0);
    }

    private void adjustBuffer(long newBufSize, boolean needShift) {
        LOG.info().$("adjust ILP receive buffer size [currentSize=").$(currentBufSize)
                .$(", newBufferSize=").$(newBufSize)
                .I$();
        long newBufLo = Unsafe.realloc(bufStart, currentBufSize, newBufSize, memoryTag);
        long offset = bufStart - newBufLo;
        if (needShift) {
            parser.shl(offset);
        }
        bufStart = newBufLo;
        bufPos -= offset;
        bufStartOfMeasurement = bufStart;
        bufEnd = bufStart + newBufSize;
        currentBufSize = newBufSize;
    }
}
