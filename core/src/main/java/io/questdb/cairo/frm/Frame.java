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
     *
     * @param upcomingTableTxn tags posting-index chain entries published during this append, so a partial publish is
     *                         droppable by recovery.
     */
    void appendColumns(Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode);

    void close();

    int columnCount();

    /**
     * Forwards {@link ColumnTopSink#commitColumnTops()} to this frame's sink, if it has one.
     */
    void commitColumnTops();

    FrameColumn createColumn(int columnIndex);

    default void setDeferCoveredIndexing(boolean deferCoveredIndexing) {
    }

    /**
     * How many of this frame's rows are LIVE - rows something still points at - as opposed to
     * {@link #getRowCount()}, which is where the next append writes. The two differ only for a COMPOSITE
     * partition, whose pieces have moved off part of its extent and left dead rows behind.
     */
    long getLiveRowCount();

    long getOffset();

    /**
     * This frame's physical extent {@code E}: the file row an append starts at, and the row count it reports
     * back once the append has landed. For a PLAIN partition this is also its live row count.
     */
    long getRowCount();

    /**
     * Appends the MERGE of two sources to this frame's tail, interleaved by {@code mergeIndexAddr}.
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
     * Reports every column's self-tracked top to {@code sink}, one {@link ColumnTopSink#setColumnTop} call per column
     * this frame actually wrote through (see {@link #saveChanges}).
     */
    void publishColumnTops(ColumnTopSink sink);

    void saveChanges(FrameColumn column);

    /**
     * States how many of the rows this frame was opened over are LIVE. An opener that opens a COMPOSITE
     * partition at its extent {@code E} states its live count here, because {@code E} counts the dead rows
     * its pieces have moved off as well. A PLAIN partition needs no call: its live count is the row count it
     * was opened at, which is what this defaults to.
     * <p>
     * The gap is what is remembered, so every append that follows advances the live count with the extent -
     * the rows an append lands are live. Scoped to the open that set it: {@link #close()} drops it.
     */
    void setLiveRowCount(long liveRowCount);

    void setOffset(long offset);

    void setRowCount(long rowCount);
}
