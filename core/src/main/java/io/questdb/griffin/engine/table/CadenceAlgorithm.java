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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;

/**
 * Cadence downsampling: selects one row out of every N (stride), starting
 * from a configurable offset. Does not inspect values.
 * <p>
 * The first row is always emitted. Subsequent selected rows are at positions
 * stride + offset, 2*stride + offset, 3*stride + offset, etc. The last row
 * is pinned when stride <= bufferSize. When stride > bufferSize, only the
 * first row is emitted.
 * <p>
 * This class is stateless. The offset is computed per cursor execution and
 * passed to the select method directly, outside the SubsampleAlgorithm
 * interface.
 */
class CadenceAlgorithm {

    static void select(long buffer, int bufferSize, int stride, int offset,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (bufferSize <= 0 || stride <= 0) {
            return;
        }
        // Always emit first row
        selectedIndices.add(0);
        if (bufferSize == 1) {
            return;
        }
        // stride == 1: emit all rows
        if (stride == 1) {
            for (int i = 1; i < bufferSize; i++) {
                if ((i & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                selectedIndices.add(i);
            }
            return;
        }
        // stride > bufferSize: only first row, no last pinning
        if (stride > bufferSize) {
            return;
        }
        int lastIdx = bufferSize - 1;
        // Emit stride-selected rows: stride + offset, 2*stride + offset, ...
        for (int pos = stride + offset; pos < bufferSize; pos += stride) {
            if ((pos & 0xFFF) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            selectedIndices.add(pos);
        }
        // Pin last row if not already emitted
        if (selectedIndices.size() == 0 || selectedIndices.get(selectedIndices.size() - 1) != lastIdx) {
            selectedIndices.add(lastIdx);
        }
    }
}
