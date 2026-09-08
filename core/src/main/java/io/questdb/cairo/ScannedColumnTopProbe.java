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

import io.questdb.std.LongList;
import io.questdb.std.Vect;
import org.jetbrains.annotations.Nullable;

/**
 * Answers whether any partition a scan reads lacks values for one column -- what QuestDB calls a
 * column top. Reads only {@code _cv} and {@code _txn}, both of which the reader already holds in
 * memory, so it opens no partition and touches no column file.
 * <p>
 * A covering index asks this once per open of a NULL key. The chain holds no posting for a row
 * that has no value, so the sidecar holds nothing to decode for it, and a scan that would read
 * such a partition has to run the plain index scan instead.
 * <p>
 * {@code _cv} drives the search, not the partition list. A column top is rare, so the records that
 * can produce one are few, while the partitions a scan reads are many.
 */
public final class ScannedColumnTopProbe {

    private ScannedColumnTopProbe() {
    }

    /**
     * Whether any partition the scan reads lacks values for {@code writerIndex}.
     *
     * @param cv          column version reader, i.e. {@code _cv}
     * @param txReader    transaction reader, i.e. {@code _txn}, which owns the partition list
     * @param writerIndex writer index of the column in question
     * @param intervals   the scan's designated-timestamp filter as flat (lo, hi) pairs, ascending,
     *                    disjoint and closed at both ends; null when the scan reads the whole
     *                    table, empty when the filter admits nothing
     * @return true when at least one partition the scan reads lacks values, false otherwise. Both
     * directions matter: answering false where a partition does lack values returns wrong rows,
     * and answering true where none does runs a slower plan for nothing.
     */
    public static boolean hasAnyColumnTop(
            ColumnVersionReader cv,
            TxReader txReader,
            int writerIndex,
            @Nullable LongList intervals
    ) {
        final int partitionCount = txReader.getPartitionCount();
        if (partitionCount == 0) {
            return false;
        }
        return hasScannedTopRecord(cv, txReader, partitionCount, writerIndex, intervals)
                || hasPartitionBeforeColumn(cv, txReader, writerIndex, intervals);
    }

    /**
     * Whether the scan reads a partition that came before the column existed. Such a partition
     * owns no {@code _cv} record at all -- the column has no value for any of its rows, which
     * reads as a top equal to its row count.
     * <p>
     * Two facts bound the search before it starts, and on the common shape they answer it outright:
     * the column's add time has to come after the scan's first interval opens, or every partition
     * the scan reads already had the column; and some partition has to start before the add, or
     * there is nothing there to have missed it. A column present since the table was created
     * reports {@link ColumnVersionReader#COL_TOP_DEFAULT_PARTITION} as its add time, which comes
     * after nothing, so it fails the first.
     * <p>
     * Only when both hold does this walk, and then only across the partitions between the scan's
     * opening and the add -- a query reading recent data has none. A partition there that owns a
     * record is decided by that record, not here: an out-of-order write can back-fill a partition
     * that came before the add, which leaves a zero top and means the column is there in full.
     */
    public static boolean hasPartitionBeforeColumn(
            ColumnVersionReader cv,
            TxReader txReader,
            int writerIndex,
            @Nullable LongList intervals
    ) {
        final int partitionCount = txReader.getPartitionCount();
        if (partitionCount == 0) {
            return false;
        }
        final long addedAtPartition = cv.getColumnTopPartitionTimestamp(writerIndex);
        final long firstPartitionTimestamp = txReader.getPartitionTimestampByIndex(0);
        final long scanStart = intervals == null || intervals.size() == 0
                ? firstPartitionTimestamp
                : intervals.getQuick(0);
        if (addedAtPartition <= scanStart || firstPartitionTimestamp >= addedAtPartition) {
            return false;
        }
        for (int p = firstScannedPartitionIndex(txReader, scanStart); p < partitionCount; p++) {
            final long partitionTimestamp = txReader.getPartitionTimestampByIndex(p);
            if (partitionTimestamp >= addedAtPartition) {
                // Partitions ascend, so no later one came before the column either.
                return false;
            }
            if (cv.getRecordIndex(partitionTimestamp, writerIndex) < 0
                    && txReader.getPartitionSize(p) > 0
                    && isPartitionScanned(txReader, partitionCount, partitionTimestamp, intervals)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Index of the first partition the scan can read, i.e. the one holding {@code scanStart}, or
     * the first of all when {@code scanStart} comes before the table's data.
     */
    private static int firstScannedPartitionIndex(TxReader txReader, long scanStart) {
        final int found = txReader.findAttachedPartitionIndexByLoTimestamp(scanStart);
        if (found > -1) {
            return found;
        }
        // A miss encodes the insertion point, which names the first partition starting AFTER
        // scanStart; the one that may hold it is the one before that.
        return Math.max(-found - 2, 0);
    }

    /**
     * Whether the scan reads a partition whose {@code _cv} record states a non-zero top for this
     * column. One pass over the records, testing only this column's.
     */
    public static boolean hasScannedTopRecord(
            ColumnVersionReader cv,
            TxReader txReader,
            int partitionCount,
            int writerIndex,
            @Nullable LongList intervals
    ) {
        final LongList records = cv.getCachedColumnVersionList();
        for (int i = 0, n = records.size(); i < n; i += ColumnVersionReader.BLOCK_SIZE) {
            if (records.getQuick(i + ColumnVersionReader.COLUMN_INDEX_OFFSET) != writerIndex) {
                continue;
            }
            final long partitionTimestamp = records.getQuick(i);
            // Neither pseudo-partition record is a partition, and COL_TOP_DEFAULT_PARTITION keeps
            // the column's ADD TIME in the column-top slot, which a plain "top > 0" test would read
            // as a top. Skipping them is an early-out rather than a correctness requirement:
            // isPartitionScanned below rejects both anyway, since no partition starts at
            // Long.MIN_VALUE or Long.MIN_VALUE + 1. They go by name because timestamps can be
            // negative, so a sign test would not separate them from real partitions.
            if (partitionTimestamp == ColumnVersionReader.COL_TOP_DEFAULT_PARTITION
                    || partitionTimestamp == ColumnVersionReader.SYMBOL_TABLE_VERSION_PARTITION) {
                continue;
            }
            if (records.getQuick(i + ColumnVersionReader.COLUMN_TOP_OFFSET) <= 0) {
                continue;
            }
            if (isPartitionScanned(txReader, partitionCount, partitionTimestamp, intervals)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether the scan reads the partition starting at {@code partitionTimestamp}, i.e. whether any
     * interval overlaps the partition's own time range.
     * <p>
     * A record can outlive its partition, so a timestamp {@code _txn} no longer lists reads as not
     * scanned. The range is {@code [partitionTimestamp, end]} inclusive, and an interval overlaps
     * it in exactly two ways: the partition's start falls inside an interval, or an interval opens
     * after that start and no later than the partition's end.
     */
    public static boolean isPartitionScanned(
            TxReader txReader,
            int partitionCount,
            long partitionTimestamp,
            @Nullable LongList intervals
    ) {
        final int partitionIndex = txReader.findAttachedPartitionIndexByLoTimestamp(partitionTimestamp);
        if (partitionIndex < 0 || partitionIndex >= partitionCount) {
            return false;
        }
        if (intervals == null) {
            return true;
        }
        final int found = intervals.binarySearch(partitionTimestamp, Vect.BIN_SEARCH_SCAN_UP);
        if (found > -1) {
            // Landed on a boundary, which a closed interval includes at either end.
            return true;
        }
        // Flat (lo, hi) pairs: an odd insertion point sits between a lo and its hi, an even one
        // before the next lo.
        final int insertionPoint = -found - 1;
        if ((insertionPoint & 1) == 1) {
            return true;
        }
        return insertionPoint < intervals.size()
                && intervals.getQuick(insertionPoint) <= partitionEndTimestamp(txReader, partitionCount, partitionIndex);
    }

    /**
     * Upper bound of the timestamps the partition at {@code partitionIndex} can hold. Mirrors
     * {@code TableReader.getPartitionMaxTimestampFromMetadata}: the logical ceiling of the
     * partition's own start, pulled back to the next partition's start when a split put one inside
     * the same logical partition.
     * <p>
     * Inclusive for every partition but the last, which returns the ceiling itself -- one past the
     * final timestamp it can hold. That asymmetry is inherited from the method it mirrors and only
     * widens the bound, so an interval opening exactly on the ceiling reads as reaching the last
     * partition when it does not. The effect is an over-report, never a missed column top.
     */
    private static long partitionEndTimestamp(TxReader txReader, int partitionCount, int partitionIndex) {
        final long ceil = txReader.getNextLogicalPartitionTimestamp(txReader.getPartitionTimestampByIndex(partitionIndex));
        final int next = partitionIndex + 1;
        return next < partitionCount
                ? Math.min(txReader.getPartitionTimestampByIndex(next), ceil) - 1
                : ceil;
    }
}
