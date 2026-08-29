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

package io.questdb.cairo.wal.seq;

import io.questdb.cairo.TableToken;
import io.questdb.std.IntList;
import io.questdb.std.Mutable;
import io.questdb.std.Transient;

public interface TableRecordMetadataSink extends Mutable {

    /**
     * Legacy overload retained for sinks compiled against the pre-NOT-NULL API.
     */
    default void addColumn(
            String columnName,
            int columnType,
            byte indexType,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex,
            boolean isDedupKey,
            boolean symbolIsCached,
            int symbolCapacity,
            @Transient IntList coveringColumnIndices
    ) {
        addColumn(
                columnName,
                columnType,
                indexType,
                indexValueBlockCapacity,
                symbolTableStatic,
                writerIndex,
                isDedupKey,
                symbolIsCached,
                symbolCapacity,
                false
        );
    }

    default void addColumn(
            String columnName,
            int columnType,
            byte indexType,
            int indexValueBlockCapacity,
            boolean symbolTableStatic,
            int writerIndex,
            boolean isDedupKey,
            boolean symbolIsCached,
            int symbolCapacity,
            boolean isNotNull
    ) {
        addColumn(
                columnName,
                columnType,
                indexType,
                indexValueBlockCapacity,
                symbolTableStatic,
                writerIndex,
                isDedupKey,
                symbolIsCached,
                symbolCapacity,
                null
        );
    }

    /**
     * Supplies covering-index metadata when the source sequencer exposes it.
     * Older sinks may ignore it; the default keeps the sink API compatible.
     */
    default void setColumnCovering(int columnIndex, @Transient IntList coveringColumnIndices) {
    }

    default boolean requiresFullReadColumnOrder() {
        return false;
    }

    void of(
            TableToken tableToken,
            int tableId,
            int timestampIndex,
            int compressedTimestampIndex,
            long structureVersion,
            int columnCount,
            @Transient IntList readColumnOrder
    );
}
