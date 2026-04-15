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
import io.questdb.std.QuietCloseable;
import io.questdb.std.str.Path;


public interface ColumnIndexer extends QuietCloseable {

    /**
     * Publishes every accumulated purge request (one per superseded
     * sealed-version file set) onto the global {@code PostingSealPurgeQueue}.
     * The background {@code PostingSealPurgeJob} persists each task and,
     * when the {@link TxnScoreboard} confirms safety, calls
     * {@code PostingSealPurgeOperator} to delete the files. Default no-op
     * for index types that do not produce seal-versioned files (BITMAP).
     */
    default void publishPendingPurges(
            MessageBus messageBus,
            TableToken tableToken,
            long partitionTimestamp,
            long partitionNameTxn,
            int partitionBy
    ) {
    }

    default void clearCovering() {
        // no-op by default
    }

    /**
     * Close without sealing. Used when the index is being dropped —
     * sealing would create new files that become orphans.
     */
    default void discardAndClose() {
        close();
    }

    /**
     * Configure covering with column names so the writer can open its own
     * read-only mmaps. Used for both active and historic partitions.
     */
    default void configureCovering(
            String[] coveredColumnNames,
            long[] coveredColumnNameTxns,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount,
            int timestampColumnIndex
    ) {
        // no-op by default
    }

    /**
     * Configure covering with pre-mapped addresses. Used by O3 where column
     * data lives in native memory buffers, not files on disk.
     */
    default void configureCovering(
            long[] coveredColumnAddrs,
            long[] coveredColumnTops,
            int[] coveredColumnShifts,
            int[] coveredColumnIndices,
            int[] coveredColumnTypes,
            int coverCount
    ) {
        // no-op by default
    }

    void configureFollowerAndWriter(
            Path path,
            CharSequence name,
            long columnNameTxn,
            MemoryMA columnMem,
            long columnTop
    );

    void configureWriter(Path path, CharSequence name, long columnNameTxn, long columnTop);

    void distress();

    long getFd();

    long getSequence();

    IndexWriter getWriter();

    void index(FilesFacade ff, long dataColumnFd, long loRow, long hiRow);

    boolean isDistressed();

    void refreshSourceAndIndex(long loRow, long hiRow);

    void releaseIndexWriter();

    void resetColumnTop();

    void rollback(long maxRow);

    void sync(boolean async);

    boolean tryLock(long expectedSequence);
}
