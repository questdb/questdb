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

import io.questdb.std.LongList;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}.
 */
public interface Frame extends Closeable {

    /**
     * Adds the exact data-vector sizes of the source rows in {@code ranges} to {@code dataBytes}, and extends each
     * column's leading run of rows under the sources' column tops, which a target can take into its own top instead
     * of its files. This lets a caller aggregate reservations across source frames before it starts writing a
     * multi-frame plan: call it once per source, in the order the plan appends them, and hand the list, opaque to
     * the caller, to {@link #reserve(long, LongList)}.
     */
    void addDataBytes(LongList dataBytes, LongList ranges);

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
     * The end of the logical row window {@link #shift} last pointed this frame at, {@code Long.MAX_VALUE} until then.
     */
    long getWindowHi();

    /**
     * The start of the logical row window {@link #shift} last pointed this frame at, {@code 0} until then.
     */
    long getWindowLo();

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
     * One operation's view of column {@code columnIndex}: this frame's own kept-open column while
     * {@link #setKeepColumnsOpen} is on, otherwise a fresh one. A read-only file column reads through the
     * window {@link #shift} set. Pair every call with {@link #releaseColumn}, which closes only a fresh column.
     */
    FrameColumn openColumn(int columnIndex);

    /**
     * Reports every column's self-tracked top to {@code sink}, one {@link ColumnTopSink#setColumnTop} call per column
     * this frame actually wrote through (see {@link #saveChanges}).
     */
    void publishColumnTops(ColumnTopSink sink);

    /**
     * The counterpart of {@link #openColumn}: closes {@code column} unless this frame keeps its columns open.
     */
    void releaseColumn(FrameColumn column);

    /**
     * Grows every column file of this frame, in one allocation per file, to hold the rows a plan of appends and
     * merges is about to write - see {@link FrameColumn#reserve}. Called once ahead of the plan's first action, so
     * no action has to allocate as it writes. Var-size columns are sized off the source rows the plan reads: each
     * {@code (lo, hi)} pair in a ranges list names rows {@code [lo, hi)} of the source it belongs to. A plan with no
     * {@code source2} appends {@code source1}'s ranges in the listed order, so the leading rows under its column tops
     * that a target column will take into its own top are not allocated.
     *
     * @param rowHi         the extent {@code E} the plan reaches at most, exclusive
     * @param source1       the frame the plan appends and merges from - the batch
     * @param source1Ranges the rows of {@code source1} the plan writes, as {@code lo, hi} pairs
     * @param source2       the other side of the plan's merges, or null when it has none
     * @param source2Ranges the rows of {@code source2} the plan rewrites, as {@code lo, hi} pairs; ignored when
     *                      {@code source2} is null
     */
    void reserve(long rowHi, Frame source1, LongList source1Ranges, @Nullable Frame source2, @Nullable LongList source2Ranges);

    /**
     * Grows every target column from its current extent to {@code rowHi}, using exact per-column var-size byte totals
     * accumulated with {@link #addDataBytes}. This overload supports plans that read more than two source frames,
     * which append in the order {@link #addDataBytes} saw them.
     */
    void reserve(long rowHi, LongList dataBytes);

    void saveChanges(FrameColumn column);

    /**
     * When on, every column {@link #openColumn} hands out stays open until {@link #close()}, so a caller that runs
     * several appends or merges against the same partition - a composite partition plan, piece after piece - opens
     * each column file once, and a read-only column maps the frame's whole extent once. Scoped to the open that set
     * it: {@link #close()} turns it off. A frame wider than one batch of columns ignores it.
     */
    void setKeepColumnsOpen(boolean isKeepColumnsOpen);

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

    /**
     * Points this frame at the logical row window {@code [rowLo, rowHi)} of its extent, in place of opening a new
     * frame at {@code rowHi}: a read-only column reports its top as no higher than {@code rowHi}, exactly as a frame
     * opened at that row count did, and every operation reading this frame must stay inside the window. Scoped to
     * the open that set it: {@link #close()} drops it.
     */
    void shift(long rowLo, long rowHi);
}
