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

import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderMetadata;
import io.questdb.cairo.sql.ColumnMapping;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;

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

    default boolean hasIntervalFilter() {
        return false;
    }

    TableReader getTableReader();

    @Override
    default boolean isExternal() {
        return false;
    }

    TablePageFrameCursor of(SqlExecutionContext executionContext, PartitionFrameCursor partitionFrameCursor) throws SqlException;

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

    /**
     * Enables or disables streaming mode for the underlying TableReader.
     * When streaming mode is enabled, partitions are opened with MADV_DONTNEED hint
     * to release page cache after reading. This is useful for large sequential scans
     * like Parquet export to avoid page cache exhaustion under memory pressure.
     *
     * @param enabled true to enable streaming mode, false to disable
     */
    default void setStreamingMode(boolean enabled) {
        TableReader reader = getTableReader();
        if (reader != null) {
            reader.setStreamingMode(enabled);
        }
    }
}
