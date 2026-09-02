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

import io.questdb.cairo.ReaderScanProfile;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;

/**
 * Defines a page frame cursor backed with an in-house database table.
 */
public interface TablePageFrameCursor extends PageFrameCursor {

    static void buildColumnMapping(ColumnMapping columnMapping, IntList columnIndexes, RecordMetadata readerMetadata) {
        columnMapping.clear();
        for (int i = 0, n = columnIndexes.size(); i < n; i++) {
            int colIdx = columnIndexes.getQuick(i);
            columnMapping.addColumn(
                    colIdx,
                    readerMetadata.getWriterIndex(colIdx),
                    readerMetadata instanceof TableReaderMetadata trm
                            ? trm.getOriginalWriterIndex(colIdx)
                            : readerMetadata.getWriterIndex(colIdx)
            );
        }
    }

    /**
     * The designated-timestamp intervals this cursor's frames are confined to, or null when
     * it applies no interval filter or cannot describe the one it applies. See
     * {@link PartitionFrameCursor#getIntervals()} for the encoding and the contract; this
     * is the same list, surfaced at the page-frame layer for consumers that never see the
     * partition frame cursor underneath.
     */
    default @Nullable LongList getIntervals() {
        return null;
    }

    default boolean hasIntervalFilter() {
        return false;
    }

    /**
     * Whether this cursor serves LEAD rows - rows that no partition of
     * {@link #getTableReader()}'s table holds, and that sort at or above every row that a
     * partition does hold - and can walk them on their own through {@link #toLeadFrames()}.
     * <p>
     * A plain table scan has none: every row it serves lives in a partition. A live view's
     * read is served by two tiers, and the un-flushed lead its in-memory tier holds is the
     * only copy of those rows that exists anywhere.
     */
    default boolean hasLeadFrames() {
        return false;
    }

    TableReader getTableReader();

    @Override
    default boolean isExternal() {
        return false;
    }

    TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) throws SqlException;

    /**
     * Positions the cursor at the lead frames, the way {@link #toPartition(int)} positions it
     * at one partition: the following {@link #next()} calls hand out those frames in
     * ascending timestamp order, and nothing else. Only call it when {@link #hasLeadFrames()}
     * holds.
     * <p>
     * It exists for the consumers that model a frame source as a TABLE - the time-frame
     * cursors, which address frames by partition index and therefore have to take the
     * partitions and the lead separately, rather than in whatever order the cursor's own
     * unscoped walk happens to interleave them.
     */
    default void toLeadFrames() {
        throw new UnsupportedOperationException();
    }

    /**
     * Positions the cursor at the given partition. The next call to
     * {@link #next()} will return the first page frame for this partition.
     * Iteration is limited to this single partition.
     *
     * @param partitionIndex the target partition index
     */
    default void toPartition(int partitionIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    default void setScanProfile(ReaderScanProfile profile) {
        TableReader reader = getTableReader();
        if (reader != null) {
            reader.setScanProfile(profile);
        }
    }
}
