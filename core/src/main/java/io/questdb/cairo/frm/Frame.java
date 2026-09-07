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
     * @param upcomingTableTxn tags posting-index chain entries published during this append, so a partial publish is
     * droppable by recovery.
     */
    void appendColumns(Frame source, long sourceLo, long sourceHi, long upcomingTableTxn, int commitMode);

    void close();

    int columnCount();

    /**
     * Forwards {@link ColumnTopSink#commitColumnTops()} to this frame's sink, if it has one.
     */
    void commitColumnTops();

    FrameColumn createColumn(int columnIndex);

    /**
     * Opens a COVERING posting-indexed column as a plain one, so this frame writes its data but adds no index entries.
     */
    default void setDeferCoveredIndexing(boolean deferCoveredIndexing) {
    }

    long getOffset();

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

    void setOffset(long offset);

    void setRowCount(long rowCount);
}
