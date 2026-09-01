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

/**
 * Where a writable {@link Frame} reports a column's new top, instead of writing straight into a
 * {@code ColumnVersionWriter}. That writer is one instance shared by every worker thread a commit's
 * O3 partition tasks run on, and is not thread safe - a caller reachable from a worker thread must
 * report through a sink like this instead of upserting into it directly.
 * <p>
 * The interface is also shaped so a frame's per-column work can fan out across threads. An
 * implementation that says so through {@link #isThreadSafe} splits into two halves: {@link #setColumnTop},
 * which every column calls and which may only write its own pre-sized slot, and
 * {@link #commitColumnTops}, which one thread calls once the whole frame operation has joined and which
 * is where anything shared gets touched. {@link #ofColumnCount} sizes the slots up front, so no report
 * ever has to grow a buffer.
 */
public interface ColumnTopSink {

    /**
     * Applies everything {@link #setColumnTop} staged since the last call, and is the ONLY place a
     * thread-safe implementation is allowed to touch shared, structurally mutable state - a
     * {@code ColumnVersionReader}'s record list, say. One frame operation calls this once, on one
     * thread, after every column of that operation has reported (see
     * {@link io.questdb.cairo.frm.FrameAlgebra#append}), so it is the join point a per-column fan-out
     * would publish through.
     * <p>
     * A sink that has nothing to stage - one whose {@link #setColumnTop} already lands the final value -
     * leaves this a no-op.
     */
    default void commitColumnTops() {
    }

    /**
     * Whether {@link #setColumnTop} may run concurrently, one thread per DISTINCT column index, once
     * {@link #ofColumnCount} has sized this sink on a single thread. A {@code true} here is a promise
     * about three things: the write lands in a slot of its own, that slot already exists so nothing
     * re-scales, and no shared structure is touched until {@link #commitColumnTops}.
     * <p>
     * Sinks that write straight into a {@code ColumnVersionWriter} return {@code false}: that writer is
     * one instance shared by every worker thread, and an upsert into it can insert into the middle of
     * its record list.
     */
    default boolean isThreadSafe() {
        return false;
    }

    /**
     * Sizes this sink for a frame of {@code columnCount} columns and drops whatever a previous frame
     * left in it. A writable {@link io.questdb.cairo.frm.Frame} calls this the moment it is opened
     * against this sink, so every slot {@link #setColumnTop} can ever address already exists by the
     * time the first column reports and no write has to grow anything.
     */
    default void ofColumnCount(int columnCount) {
    }

    /**
     * Reports one column's top. Addresses only {@code columnIndex}'s own slot - see
     * {@link #isThreadSafe}.
     */
    void setColumnTop(int columnIndex, long columnTop);
}
