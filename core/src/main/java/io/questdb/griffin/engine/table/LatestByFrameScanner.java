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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameAddressCache;
import io.questdb.cairo.sql.PageFrameMemoryPool;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import org.jetbrains.annotations.NotNull;

final class LatestByFrameScanner {
    private final PageFrameAddressCache addressCache;
    private final Function filter;
    private final PageFrameMemoryPool memoryPool;
    private long batchHi;
    private long batchLo;
    private int frameIndex;
    private long frameRowCount;
    private DirectLongList matches;
    private long rowCount;

    LatestByFrameScanner(@NotNull Function filter, @NotNull PageFrameMemoryPool memoryPool, @NotNull PageFrameAddressCache addressCache) {
        this.filter = filter;
        this.memoryPool = memoryPool;
        this.addressCache = addressCache;
    }

    long getRow(long index) {
        return matches != null ? matches.get(index) + batchLo : index;
    }

    long getRowCount() {
        return rowCount;
    }

    boolean isMatch(Record record) {
        return matches != null || filter.getBool(record);
    }

    boolean nextBatch(SqlExecutionCircuitBreaker circuitBreaker) {
        if (batchHi == 0) {
            return false;
        }
        if (batchHi < frameRowCount) {
            circuitBreaker.statefulThrowExceptionIfTrippedOrYield();
        }
        batchLo = Math.max(0, batchHi - LatestByCompiledFilter.BATCH_SIZE);
        matches = LatestByCompiledFilter.apply(filter, memoryPool, addressCache, frameIndex, batchLo, batchHi);
        if (matches != null) {
            rowCount = matches.size();
        } else {
            batchLo = 0;
            rowCount = batchHi;
        }
        batchHi = batchLo;
        return true;
    }

    void of(int frameIndex, long frameRowCount) {
        this.frameIndex = frameIndex;
        this.frameRowCount = frameRowCount;
        batchHi = frameRowCount;
    }
}
