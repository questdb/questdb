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

package io.questdb.cairo.sql;

import io.questdb.cairo.TableReader;
import io.questdb.std.LongList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A cursor for navigating through partition frames.
 */
public interface PartitionFrameCursor extends QuietCloseable, SymbolTableSource {

    default void calculateSize(RecordCursor.Counter counter) {
    }

    /**
     * Returns the table reader. The same TableReader is available on each partition frame.
     *
     * @return the table reader
     */
    TableReader getTableReader();

    /**
     * An upper bound on the number of partition frames this cursor can produce, when one is
     * cheap to derive from metadata the cursor has already resolved, or -1 when unknown. An
     * implementation that answers must never under-count: the walk may produce fewer frames
     * than the bound (it skips intersections that hold no rows) but never more. Callers may
     * therefore use the bound to reject frame-proportional work eagerly, but not to size
     * anything exactly.
     */
    default long getFrameCountUpperBound() {
        return -1;
    }

    /**
     * The designated-timestamp intervals this cursor confines its frames to, or null when
     * it applies no interval filter or cannot describe the one it applies. Flat (lo, hi)
     * pairs in the timestamp column's own units, CLOSED at both ends, ascending and
     * disjoint; an EMPTY list means the filter admits nothing, which is distinct from the
     * null "no filter" answer.
     * <p>
     * A caller may only rely on this together with {@link #hasIntervalFilter()}: a non-null
     * list means the cursor's rows are exactly the table rows whose timestamp falls in one
     * of these intervals, so a caller holding rows of its own can apply the same filter to
     * them and stay row-for-row consistent with the scan.
     * <p>
     * The list belongs to the cursor and is not a copy: an implementation may re-point or
     * rewrite it on the next {@code of()} / {@code getCursor()}. Read it, or copy it, before
     * reopening the cursor.
     */
    default @Nullable LongList getIntervals() {
        return null;
    }

    /**
     * Returns true if this cursor applies interval filtering to partitions.
     * When true, the cursor may produce narrowed row ranges per partition
     * and skip partitions entirely, making page frame counts unpredictable
     * from metadata alone.
     */
    default boolean hasIntervalFilter() {
        return false;
    }

    /**
     * @return the next element in the partition frame
     */
    default @Nullable PartitionFrame next() {
        return next(0);
    }

    @Nullable PartitionFrame next(long skipTarget);

    /**
     * Reload the partition frame and return the cursor to the beginning of
     * the partition frame
     *
     * @return true when reload data has changed, false otherwise
     */
    @TestOnly
    boolean reload();

    /**
     * @return number of rows in all partition frames.
     */
    long size();

    /**
     * @return true if cursor supports fast size calculation,
     * i.e. {@link #calculateSize(RecordCursor.Counter)} is properly implemented.
     */
    default boolean supportsSizeCalculation() {
        return false;
    }

    /**
     * Positions the cursor at the given partition index. The next call to
     * {@link #next(long)} will return the frame for this partition. Iteration
     * is limited to this single partition — subsequent {@link #next(long)}
     * calls return {@code null} once the partition is exhausted.
     *
     * @param partitionIndex the target partition index
     */
    default void toPartition(int partitionIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return the cursor to the first partition frame.
     */
    void toTop();
}
