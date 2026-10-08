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

package io.questdb.cairo.frm;

import io.questdb.cairo.frm.file.RecycleBin;

import java.io.Closeable;

public interface FrameColumn extends Closeable {
    /**
     * Column type for contiguous file columns, usually it means it's a partition or a WAL segment stored on disk
     */
    int COLUMN_CONTIGUOUS_FILE = 0;
    /**
     * Column type for memory columns, usually it comes from uncommitted, sorted data stored in memory or mapped WAL files
     */
    int COLUMN_MEMORY = 1;

    void addTop(long value);

    /**
     * Appends source frame to this frame starting at the specific offset in this frame.
     *
     * @param appendOffsetRowCount offset in number of rows after which data is appended
     * @param sourceColumn         the source frame
     * @param sourceLo             low index in the source frame
     * @param sourceHi             high index in the source frame, exclusive
     * @param commitMode           the commit mode, which drives durability of the change.
     */
    void append(long appendOffsetRowCount, FrameColumn sourceColumn, long sourceLo, long sourceHi, int commitMode);

    /**
     * Appends the MERGE of two sources to this column's tail, interleaved by {@code mergeIndexAddr}.
     *
     * @param mergeIndexAddr native address of the merge index
     * @param mergeIndexRows number of rows the index describes, which is the number of rows appended
     */
    void merge(
            long appendOffsetRowCount,
            FrameColumn sourceColumn1,
            long source1Lo,
            long source1Hi,
            FrameColumn sourceColumn2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            int commitMode
    );

    void appendNulls(long rowCount, long sourceColumnTop, int commitMode);

    void close();

    int getColumnIndex();

    long getColumnTop();

    int getColumnType();

    long getContiguousAuxAddr(long rowHi);

    long getContiguousDataAddr(long rowHi);

    long getPrimaryFd();

    long getSecondaryFd();

    int getStorageType();

    default boolean isTimestampIndex() {
        return false;
    }

    /**
     * Writable file columns only, a no-op for every other kind. Grows this column's files, in one allocation each,
     * to the size the writes about to land on them need, and maps them when needed, so that none of those writes has
     * to allocate or map itself. A plan of several appends and merges against one partition calls this once, ahead of
     * its first action, with the extent the whole plan reaches. The reservation must cover every write: mixed-I/O
     * appends use positioned writes, so growing the file later can force XFS to synchronously flush the dirty tail.
     * Mixed I/O allocates without mapping; mmap I/O keeps the existing allocation-and-map behavior.
     *
     * @param rowLo     the partition row the first write starts at, i.e. the extent the column holds now
     * @param rowHi     the partition row the last write ends at, exclusive
     * @param dataBytes the data bytes the writes bring, for a var-size column; ignored by a fixed-size one
     * @param isDedup   whether the table deduplicates: a dedup merge can write more var-size data than the sources
     *                  the reservation was sized from, so a var-size column grows past it instead of failing
     */
    default void reserve(long rowLo, long rowHi, long dataBytes, boolean isDedup) {
    }

    /**
     * Read-only file columns only, a no-op for every other kind. Lets one column serve several operations of a
     * frame opened once over a whole partition, each reading one piece of it.
     *
     * @param logicalRowHi the end of the row window the next operation reads. The column reports its top as no
     *                     higher than this, exactly as a column opened at this row count did, so code sizing the
     *                     rows below a top sees the same numbers either way. {@code Long.MAX_VALUE} for no window.
     * @param mapRowHi     how far the column's first mapping reaches at the least, so a column kept open across
     *                     operations maps the whole frame once rather than growing piece by piece. {@code 0} maps
     *                     only the rows asked for.
     */
    default void setReadWindow(long logicalRowHi, long mapRowHi) {
    }

    void setRecycleBin(RecycleBin<FrameColumn> pool);

    /**
     * Posting-index hook: tag chain entries published during the next
     * {@link #append} or {@link #appendNulls} with the supplied upcoming
     * {@code _txn}. A value below 0 means "unwired" and the column falls
     * back to its default (legacy) tagging. No-op for column types that
     * do not own a posting index writer.
     */
    default void setUpcomingTableTxn(long upcomingTableTxn) {
    }
}
