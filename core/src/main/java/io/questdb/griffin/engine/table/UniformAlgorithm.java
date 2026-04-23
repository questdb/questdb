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
 * Uniform downsampling: selects targetPoints rows evenly spaced across the
 * input by position. Does not inspect values.
 * <p>
 * First and last rows are always included. Interior rows are picked at
 * evenly spaced positions using integer arithmetic to avoid floating-point
 * drift on large row counts.
 *
 * @see SubsampleAlgorithm
 */
class UniformAlgorithm implements SubsampleAlgorithm {
    static final UniformAlgorithm INSTANCE = new UniformAlgorithm();

    @Override
    public void select(long buffer, int bufferSize, int targetPoints,
                       DirectLongList selectedIndices, SqlExecutionCircuitBreaker circuitBreaker) {
        selectedIndices.clear();
        if (bufferSize <= 0 || targetPoints <= 0) {
            return;
        }
        if (bufferSize <= targetPoints) {
            for (int i = 0; i < bufferSize; i++) {
                if ((i & 0xFFF) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTripped();
                }
                selectedIndices.add(i);
            }
            return;
        }
        // Integer-only evenly spaced selection with rounding.
        // pos = (i * (bufferSize - 1) + (targetPoints - 1) / 2) / (targetPoints - 1)
        // This pins pos(0) = 0 and pos(targetPoints - 1) = bufferSize - 1 exactly.
        long divisor = targetPoints - 1;
        long range = bufferSize - 1;
        long half = divisor / 2;
        for (int i = 0; i < targetPoints; i++) {
            if ((i & 0xFFF) == 0) {
                circuitBreaker.statefulThrowExceptionIfTripped();
            }
            long pos = ((long) i * range + half) / divisor;
            selectedIndices.add(pos);
        }
    }
}
