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

import java.io.Closeable;

/**
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}.
 */
public interface Frame extends Closeable {

    /**
     * Appends {@code [sourceLo, sourceHi)} of {@code source} to this frame's tail, one column at a time.
     * <p>
     * The frame drives its own per-column work, on the shared column-task pool when it has one. It moves
     * no row count and commits no tops; that is {@link FrameAlgebra#append}'s part, its only caller.
     *
     * @param upcomingTableTxn tags posting-index chain entries published during this append, so a partial
     *                         publish is droppable by recovery. See {@link FrameColumn#setUpcomingTableTxn}.
     */
    void appendColumns(Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode);

    void close();

    int columnCount();

    /**
     * Forwards {@link ColumnTopSink#commitColumnTops()} to this frame's sink, if it has one. A no-op for
     * a frame that tracks its own tops.
     */
    void commitColumnTops();

    FrameColumn createColumn(int columnIndex);

    /**
     * Opens a COVERING posting-indexed column as a plain one, so this frame writes its data but adds no
     * index entries. The caller indexes the rows it appended itself, after every column is on disk, with
     * the covered columns described. Default: index as usual.
     */
    default void setDeferCoveredIndexing(boolean deferCoveredIndexing) {
    }

    long getOffset();

    long getRowCount();

    /**
     * Appends the MERGE of two sources to this frame's tail, interleaved by {@code mergeIndexAddr}. The
     * counterpart of {@link #appendColumns}; only {@link FrameAlgebra#merge} calls it.
     */
    void mergeColumns(
            Frame source1,
            long source1Lo,
            long source1Hi,
            Frame source2,
            long source2Lo,
            long source2Hi,
            long mergeIndexAddr,
            long mergeIndexRows,
            long upcomingTableTxn,
            int commitMode
    );

    /**
     * Reports every column's self-tracked top to {@code sink}, one {@link ColumnTopSink#setColumnTop}
     * call per column this frame actually wrote through (see {@link #saveChanges}). A {@link ColumnTopSink}
     * rather than a {@code ColumnVersionWriter} directly, so a caller can defer applying the values - e.g.
     * record them off the writer thread and push them into the real {@code ColumnVersionWriter} only once
     * it holds the writer - instead of writing straight into a table-wide, non-thread-safe instance.
     */
    void publishColumnTops(ColumnTopSink sink);

    void saveChanges(FrameColumn column);

    void setOffset(long offset);

    void setRowCount(long rowCount);
}
