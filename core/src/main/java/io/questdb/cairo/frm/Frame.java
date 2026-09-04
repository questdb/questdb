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

import org.jetbrains.annotations.Nullable;

import java.io.Closeable;

/**
 * Used for partition squashing in {@link io.questdb.cairo.TableWriter}.
 */
public interface Frame extends Closeable {

    void close();

    int columnCount();

    /**
     * Forwards {@link ColumnTopSink#commitColumnTops()} to this frame's sink, if it has one. A frame
     * that tracks its own tops has nothing to commit - {@link #saveChanges} already lands the final
     * value - so this is a no-op there.
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

    /**
     * The fan-out this frame's per-column work can run on, or {@code null} when it has none and every
     * operation against it stays on the calling thread. Only a writable frame has one - it is the
     * TARGET of an operation that owns it - and one frame runs one operation at a time, so the same
     * instance is handed out for every call.
     */
    @Nullable
    FrameColumnFanOut getColumnFanOut();

    long getOffset();

    long getRowCount();

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
