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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.std.LongList;

/**
 * Instances of this time frame cursor can be used by multiple threads as interactions
 * with the table reader are synchronized. Yet, a single instance can't be called by
 * multiple threads concurrently.
 * <p>
 * The only supported partition order is forward, i.e. navigation
 * should start with a {@link #next()} call.
 */
public interface ConcurrentTimeFrameCursor extends TimeFrameCursor {

    static void populatePartitionTimestamps(
            TablePageFrameCursor frameCursor,
            LongList partitionTimestamps,
            LongList partitionCeilings
    ) {
        partitionTimestamps.clear();
        partitionCeilings.clear();
        final TableReader reader = frameCursor.getTableReader();
        final int partitionCount = reader.getPartitionCount();
        final TimestampDriver.TimestampCeilMethod ceilMethod = PartitionBy.getPartitionCeilMethod(
                reader.getMetadata().getTimestampType(),
                reader.getPartitionedBy()
        );
        for (int i = 0; i < partitionCount; i++) {
            final long tsLo = reader.getPartitionTimestampByIndex(i);
            partitionTimestamps.add(tsLo);
            final long maxTsHi = i < partitionCount - 1 ? reader.getPartitionTimestampByIndex(i + 1) : Long.MAX_VALUE;
            partitionCeilings.add(TimeFrameCursorImpl.estimatePartitionHi(ceilMethod, tsLo, maxTsHi));
        }
    }

    /**
     * Lets {@link #getIndexReaderForCurrentFrame} serve symbol index readers from a table reader of the
     * cursor's own, which the cursor opens from {@code engine} on the first call and checks against the
     * shared state's reader: the shared reader's index readers are not safe for concurrent use. A cursor
     * that does not support it keeps returning null, which tells a caller to scan instead.
     */
    default void enableIndexReaders(CairoEngine engine) {
    }

    /**
     * @return the index reader of the current frame from the cursor's own table reader, see
     * {@link #enableIndexReaders}, or null when there is none: not enabled, no reader at the shared
     * reader's version, a lead frame, or a column without an index
     */
    @Override
    default IndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
        return null;
    }

    ConcurrentTimeFrameCursor of(
            ConcurrentTimeFrameState sharedState,
            TablePageFrameCursor frameCursor,
            int timestampIndex
    );
}
