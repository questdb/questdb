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

import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.std.DirectLongList;
import io.questdb.std.Unsafe;

/**
 * Strategy interface for SUBSAMPLE downsampling algorithms.
 * <p>
 * Implementations receive a native buffer of (rowId, timestamp, value) entries
 * and write selected buffer indices to the output list. The buffer layout per
 * entry is: [rowId: long (8)][timestamp: long (8)][value: double (8)] = 24 bytes.
 */
public interface SubsampleAlgorithm {
    int ENTRY_SIZE = 24;

    /**
     * Select representative points from the buffer and add their indices
     * to {@code selectedIndices}. The list is NOT cleared before this call -
     * implementations must call {@code selectedIndices.clear()} if needed.
     *
     * @param buffer          native memory buffer of entries
     * @param bufferSize      number of entries in the buffer
     * @param targetPoints    desired number of output points
     * @param selectedIndices output list to add selected buffer indices to
     * @param circuitBreaker  for query cancellation during processing
     */
    void select(long buffer, int bufferSize, int targetPoints,
                DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker);

    /**
     * Computes floor(span * bucket / numBuckets) without long overflow, for
     * span >= 0. Decomposing span = q * numBuckets + r gives
     * span * bucket / numBuckets = q * bucket + r * bucket / numBuckets, where
     * q * bucket &lt;= span and r * bucket &lt; numBuckets^2 &lt;= 2^62, so both
     * terms and their sum (&lt;= span) fit in a long.
     * <p>
     * Exact integer boundaries keep full resolution at any epoch. Double math
     * does not: the double ulp near a 2024 nanosecond epoch (~1.7e18) is 256,
     * so absolute-epoch doubles quantize TIMESTAMP_NS boundaries to 256ns
     * steps and can collapse sub-256ns bucket spans to zero.
     */
    static long bucketOffset(long span, int bucket, int numBuckets) {
        return span / numBuckets * bucket + span % numBuckets * bucket / numBuckets;
    }

    /**
     * Read timestamp from buffer entry at the given index.
     */
    static long getTimestamp(long buffer, long index) {
        return Unsafe.getUnsafe().getLong(buffer + index * ENTRY_SIZE + 8);
    }

    /**
     * Read value from buffer entry at the given index.
     */
    static double getValue(long buffer, long index) {
        return Unsafe.getUnsafe().getDouble(buffer + index * ENTRY_SIZE + 16);
    }
}
