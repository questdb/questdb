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

package io.questdb.cairo;

import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;

/**
 * Native-buffer container surfaced by {@link ArrowStreamSink} to Python
 * consumers. The handle holds, per column, up to three Arrow buffers:
 * <ul>
 *   <li>validity bitmap (always present; 1 bit per row, LSB-first, 1 = valid)</li>
 *   <li>data buffer (always present; raw fixed-width values for fixed
 *       columns, UTF-8 / byte payload for var columns)</li>
 *   <li>offsets buffer (var columns only; int32 offsets of length
 *       {@code rowCount + 1})</li>
 * </ul>
 * Fixed-width columns have {@code offsetsPtr == 0}; Python consumers detect
 * var columns by the non-zero offsets pointer. Python wraps each pointer with
 * {@code pa.foreign_buffer} for zero-copy access and calls {@link #release}
 * when the corresponding {@code pa.RecordBatch} is no longer needed.
 * <p>
 * All allocations live under {@link MemoryTag#NATIVE_IMPORT}. {@link #release}
 * is idempotent and synchronised.
 * <p>
 * An "end sentinel" handle — created via {@link #endSentinel} — has
 * {@code isEnd == true}, a row count of zero, and no backing buffers.
 * Consumers stop iteration when they take a handle for which
 * {@link #isEnd} returns true.
 */
public class ArrowBatchHandle {

    private final int columnCount;
    private final long[] dataPtrs;
    private final long[] dataSizes;
    private final boolean isEnd;
    private final long[] offsetsPtrs;
    private final long[] offsetsSizes;
    private final int rowCount;
    private final long[] validityPtrs;
    private final long[] validitySizes;
    private boolean isReleased;

    ArrowBatchHandle(
            int columnCount,
            long[] dataPtrs,
            long[] dataSizes,
            long[] validityPtrs,
            long[] validitySizes,
            long[] offsetsPtrs,
            long[] offsetsSizes,
            int rowCount,
            boolean isEnd
    ) {
        this.columnCount = columnCount;
        this.dataPtrs = dataPtrs;
        this.dataSizes = dataSizes;
        this.validityPtrs = validityPtrs;
        this.validitySizes = validitySizes;
        this.offsetsPtrs = offsetsPtrs;
        this.offsetsSizes = offsetsSizes;
        this.rowCount = rowCount;
        this.isEnd = isEnd;
    }

    /**
     * Produce the terminator handle signalling end-of-stream. Sentinel handles
     * own no native memory and release to a no-op.
     */
    public static ArrowBatchHandle endSentinel() {
        return new ArrowBatchHandle(0, null, null, null, null, null, null, 0, true);
    }

    public int getColumnCount() {
        return columnCount;
    }

    public long getDataPtr(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read data pointer from end-sentinel handle");
        }
        return dataPtrs[col];
    }

    public long getDataSize(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read data size from end-sentinel handle");
        }
        return dataSizes[col];
    }

    /**
     * Returns 0 for fixed-width columns, non-zero for var-width columns
     * (STRING/VARCHAR/BINARY). Python consumers use this to decide whether
     * to build a 2-buffer (validity+data) or 3-buffer (validity+offsets+data)
     * Arrow array.
     */
    public long getOffsetsPtr(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read offsets pointer from end-sentinel handle");
        }
        return offsetsPtrs[col];
    }

    public long getOffsetsSize(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read offsets size from end-sentinel handle");
        }
        return offsetsSizes[col];
    }

    public int getRowCount() {
        return rowCount;
    }

    public long getValidityPtr(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read validity pointer from end-sentinel handle");
        }
        return validityPtrs[col];
    }

    public long getValiditySize(int col) {
        if (isEnd) {
            throw CairoException.nonCritical().put("cannot read validity size from end-sentinel handle");
        }
        return validitySizes[col];
    }

    public boolean isEnd() {
        return isEnd;
    }

    /**
     * Free every native buffer owned by this handle. Idempotent — subsequent
     * calls do nothing. Safe to call after an abandoned stream.
     */
    public synchronized void release() {
        if (isReleased || isEnd) {
            isReleased = true;
            return;
        }
        isReleased = true;
        for (int c = 0; c < columnCount; c++) {
            if (dataPtrs[c] != 0) {
                Unsafe.free(dataPtrs[c], dataSizes[c], MemoryTag.NATIVE_IMPORT);
                dataPtrs[c] = 0;
            }
            if (validityPtrs[c] != 0) {
                Unsafe.free(validityPtrs[c], validitySizes[c], MemoryTag.NATIVE_IMPORT);
                validityPtrs[c] = 0;
            }
            if (offsetsPtrs[c] != 0) {
                Unsafe.free(offsetsPtrs[c], offsetsSizes[c], MemoryTag.NATIVE_IMPORT);
                offsetsPtrs[c] = 0;
            }
        }
    }
}
