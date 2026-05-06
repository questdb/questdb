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

package io.questdb.cairo;

import io.questdb.MessageBus;
import io.questdb.cairo.idx.IndexWriter;
import io.questdb.cairo.vm.api.MemoryMA;
import io.questdb.std.FilesFacade;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;


public interface ColumnIndexer extends QuietCloseable {

    default void clearCovering() {
        // no-op by default
    }

    /**
     * Configure covering with column names so the writer can open its own
     * read-only mmaps. Used for both active and historic partitions.
     */
    default void configureCovering(
            ObjList<CharSequence> coveredColumnNames,
            LongList coveredColumnNameTxns,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int timestampColumnIndex
    ) {
        // no-op by default
    }

    default void configureCovering(
            LongList coveredColumnAddrs,
            LongList coveredColumnAuxAddrs,
            LongList coveredColumnTops,
            IntList coveredColumnShifts,
            IntList coveredColumnIndices,
            IntList coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
    ) {
    }

    void configureFollowerAndWriter(
            Path path,
            CharSequence name,
            long columnNameTxn,
            MemoryMA columnMem,
            long columnTop,
            long partitionTimestamp,
            long partitionNameTxn
    );

    void configureWriter(
            Path path,
            CharSequence name,
            long columnNameTxn,
            long columnTop,
            long partitionTimestamp,
            long partitionNameTxn
    );

    default void discardAndClose() {
        close();
    }

    void distress();

    long getFd();

    long getSequence();

    IndexWriter getWriter();

    void index(FilesFacade ff, long dataColumnFd, long loRow, long hiRow);

    boolean isDistressed();

    /**
     * See {@link IndexWriter#mergeTentativeIntoActiveIfAny()}.
     */
    default void mergeTentativeIntoActiveIfAny() {
    }

    default void publishPendingPurges(
            MessageBus messageBus,
            TableToken tableToken,
            int partitionBy,
            int timestampType,
            long currentTableTxn
    ) {
    }

    default void rebuildSidecars() {
    }

    void refreshSourceAndIndex(long loRow, long hiRow);

    void releaseIndexWriter();

    void resetColumnTop();

    void rollback(long maxRow);

    default void seal() {
    }

    default void setCoveredColumnNameTxns(LongList txns) {
    }

    void sync(boolean async);

    boolean tryLock(long expectedSequence);
}
