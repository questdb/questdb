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
     * <p>
     * The append primitive above carries one source through unchanged; this one carries two, in the order
     * a merge index dictates. That index is the standard 16-bytes-per-row form {@code mergeTwoLongIndexesAsc}
     * produces - timestamp, then a row id whose top bit says which side it came from - so both sources are
     * read in one pass and the result lands contiguously at {@code appendOffsetRowCount}.
     * <p>
     * The row ids in the index are ABSOLUTE within each source, so the bounds below say which rows a source
     * has to be readable over; they are not offsets the implementation adds to anything.
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

    /**
     * Whether this column's data is the 16-bytes-per-row SORTED TIMESTAMP INDEX rather than a column of
     * timestamps. True only for the designated timestamp of an O3 frame, which is where the index lives -
     * the O3 buffers hold no timestamp column of their own, so every read of one goes through the index,
     * exactly as the per-column O3 path does.
     */
    default boolean isTimestampIndex() {
        return false;
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
