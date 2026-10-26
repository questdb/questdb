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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.sql.Record;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;

/**
 * Tells whether a master row of a HORIZON JOIN can match any slave row when two key positions
 * compare the same slave column, as in {@code t.s1 = p.s AND t.s2 = p.s}. A slave row writes one
 * value at every position of its column, so only a master row whose key values at those positions
 * encode the same can match. The lookup of any other master row would scan the slave table back
 * to its first row for a key that no row holds.
 * <p>
 * The earliest copier writes, for every key position whose slave column occurs at an earlier
 * position, the master column of the earliest such position, and the later copier writes the
 * master column of the position itself. Both use the encodings of the master key copier of the
 * join, so the two copies are equal exactly when the key map could match the master key.
 * <p>
 * Each instance is thread-unsafe, so every {@link HorizonJoinTimeFrameHelper} creates its own
 * from a {@link HorizonJoinSharedKeyCheckTemplate}. The helper reopens it with the per-query
 * memory tracker and closes it with the cursor.
 */
final class HorizonJoinSharedKeyCheck implements QuietCloseable {
    private final SingleRecordSink earliestCopy;
    private final RecordSink earliestSink;
    private final SingleRecordSink laterCopy;
    private final RecordSink laterSink;

    HorizonJoinSharedKeyCheck(CairoConfiguration configuration, RecordSink earliestSink, RecordSink laterSink) {
        // the copies hold master key values, as the key sink targets of an ASOF join do
        final long maxHeapSize = (long) configuration.getSqlHashJoinValuePageSize() * configuration.getSqlHashJoinValueMaxPages();
        this.earliestSink = earliestSink;
        this.laterSink = laterSink;
        this.earliestCopy = new SingleRecordSink(maxHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_HORIZON_JOIN, SingleRecordSink.CONFIG_KEYS_ASOF_JOIN);
        this.laterCopy = new SingleRecordSink(maxHeapSize, MemoryTag.NATIVE_RECORD_CHAIN, SingleRecordSink.OWNER_HORIZON_JOIN, SingleRecordSink.CONFIG_KEYS_ASOF_JOIN);
    }

    /**
     * Frees the copies. The instance stays reusable: {@link #reopen} allocates them again.
     */
    @Override
    public void close() {
        earliestCopy.close();
        laterCopy.close();
    }

    /**
     * Returns false when the master key values differ at two key positions that compare the same
     * slave column, so that no slave row can match the master row.
     *
     * @param masterKeyRecord the record that the master key copier of the join reads
     */
    boolean isMatchable(Record masterKeyRecord) {
        earliestCopy.clear();
        earliestSink.copy(masterKeyRecord, earliestCopy);
        laterCopy.clear();
        laterSink.copy(masterKeyRecord, laterCopy);
        return earliestCopy.memeq(laterCopy);
    }

    /**
     * Frees the copies under the tracker that allocated them, then allocates them under the
     * tracker of the new execution.
     */
    void reopen(@Nullable MemoryTracker memoryTracker) {
        close();
        earliestCopy.setMemoryTracker(memoryTracker);
        laterCopy.setMemoryTracker(memoryTracker);
        earliestCopy.reopen();
        laterCopy.reopen();
    }
}
