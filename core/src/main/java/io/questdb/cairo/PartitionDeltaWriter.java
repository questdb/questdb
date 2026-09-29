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

import io.questdb.cairo.vm.api.MemoryCR;
import io.questdb.std.QuietCloseable;
import io.questdb.std.ReadOnlyObjList;
import io.questdb.std.str.Path;

public interface PartitionDeltaWriter extends QuietCloseable {

    /**
     * Checks the Delta state in the attachable directory and writes its visible timestamp bounds.
     * Throws when it does not belong to the table, e.g. another base row count or index set.
     *
     * @param boundsAddr caller-owned, 8-byte-aligned 16-byte buffer for min then max;
     *                   only accessed during this call
     */
    void checkAttach(TableWriter writer, long partitionTimestamp, long baseRowCount, long boundsAddr);

    @Override
    default void close() {
    }

    /**
     * Writes the committed catalog of a partition with Delta rows into its detached directory.
     * The live catalog changes in place, so the detached directory needs its own copy.
     */
    void detach(TableWriter writer, int partitionIndex, Path detachedPartitionPath);

    default void dropIndex(
            TableWriter writer,
            int partitionIndex,
            int columnIndex,
            long dropSeqTxn
    ) {
    }

    /**
     * Moves the Delta files of an attached partition into their placement roots, right after
     * ATTACH renames or copies its directory. Throws after it moves them back.
     */
    void install(TableWriter writer, Path partitionDir);

    /**
     * Releases the Delta state of a removed partition version before its directory is deleted.
     * Returns false to keep the directory for the next purge pass.
     */
    boolean purge(Path partitionDir);

    /**
     * Writes the Delta bounds visible at the writer's current sequence transaction.
     *
     * @param boundsAddr caller-owned, 8-byte-aligned 16-byte buffer for min then max;
     *                   only accessed during this call
     */
    void readTimestampBounds(TableWriter writer, int partitionIndex, long boundsAddr);

    void rollback(TableWriter writer, int partitionIndex, long seqTxn);

    void writeCommit(
            TableWriter writer,
            boolean firstDeltaWrite,
            int partitionIndexRaw,
            long partitionTimestamp,
            long partitionNameTxn,
            boolean parquetBase,
            long parquetFileSize,
            long baseRowCount,
            ReadOnlyObjList<? extends MemoryCR> o3Columns,
            long sortedTimestampsAddr,
            long srcOooLo,
            long srcOooHi,
            long seqTxn,
            long commitTimestamp
    );
}
