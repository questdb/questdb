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

import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;

/**
 * Represents page frame as a set of per column contiguous memory.
 * For native partitions, it's simply a slice of mmapped memory.
 * For Parquet partitions, it's a deserialized in-memory native format, and so it is for a frame
 * that a covering index serves, whose columns are decoded from the index; see {@link #isStable()}.
 */
public interface PageFrameMemory {

    /**
     * Populates remaining columns (those not in filterColumnIndexes) for filtered rows.
     * Used for late materialization in Parquet partitions.
     *
     * @param filterColumnIndexes columns already loaded (filter columns)
     * @param filteredRows        rows that passed the filter
     * @param fillWithNulls       whether to fill missing columns with nulls
     * @return true if columns were populated, false if no action was needed
     */
    boolean populateRemainingColumns(IntHashSet filterColumnIndexes, DirectLongList filteredRows, boolean fillWithNulls);

    /**
     * Returns aux (index) vector address for a var-size column.
     */
    long getAuxPageAddress(int columnIndex);

    /**
     * Returns flat list of aux page addresses for all frames.
     * Use with {@link #getColumnOffset()} for efficient access.
     */
    DirectLongList getAuxPageAddresses();

    /**
     * Returns flat list of aux page sizes for all frames.
     */
    DirectLongList getAuxPageSizes();

    int getColumnCount();

    /**
     * Returns pre-computed offset into flat column arrays for this frame.
     * Usage: {@code getPageAddresses().getQuick(getColumnOffset() + columnIndex)}
     */
    int getColumnOffset();

    /**
     * Returns the per-column leading column-top count for this frame, or {@code null} when
     * the frame has none (e.g. native frames). Used by {@link PageFrameMemoryRecord} to
     * surface NULL for column-top rows during a lazy fixed-&gt;var conversion, where the
     * decoded source value is an in-band 0 indistinguishable from a real 0.
     */
    default DirectLongList getColumnTops() {
        return null;
    }

    /**
     * Returns frame format: {@link PartitionFormat#NATIVE} or {@link PartitionFormat#PARQUET}.
     */
    byte getFrameFormat();

    int getFrameIndex();

    /**
     * Returns data vector address for a column.
     */
    long getPageAddress(int columnIndex);

    /**
     * Returns flat list of data page addresses for all frames.
     * Use with {@link #getColumnOffset()} for efficient access.
     */
    DirectLongList getPageAddresses();

    /**
     * Returns data vector size for a column.
     */
    long getPageSize(int columnIndex);

    /**
     * Returns flat list of data page sizes for all frames.
     */
    DirectLongList getPageSizes();

    /**
     * Returns the pool that owns this frame memory's parquet decode buffers, or
     * {@code null} when the memory is not owned by a {@link PageFrameMemoryPool}.
     * A {@link PageFrameMemoryRecord} stamps this on bind so that
     * {@link PageFrameMemoryPool#navigateTo(int, PageFrameMemoryRecord)} can tell
     * "still bound to this pool's live buffers" from "bound to another (possibly
     * freed) pool's buffers" and rebind only when necessary.
     */
    PageFrameMemoryPool getPool();

    /**
     * Returns row ID offset used to compute real row IDs.
     */
    long getRowIdOffset();

    /**
     * Returns the source column type tag for a type-cast column, or -1 if
     * the column does not require a type cast. Used by
     * {@link PageFrameMemoryRecord} to perform lazy fixed→var conversion.
     */
    int getSourceColumnType(int columnIndex);

    /**
     * Returns true if any column has a column top (zero address).
     */
    boolean hasColumnTops();

    /**
     * Returns true if any column requires a lazy type cast (e.g. fixed→varchar
     * conversion for parquet partitions with ALTER COLUMN TYPE).
     */
    boolean hasColumnTypeCasts();

    /**
     * Whether the frame's column memory stays valid until the query ends, so that a reader may
     * keep a pointer into it after it moves to another frame: the mapped columns of a native
     * partition, and the columns of a frame that a covering index serves, decoded into a pool that
     * keeps them for the query, see {@link PageFrameMemoryPool#setKeepsCoveredFramesForQuery}. A
     * decoded Parquet frame lives only until its pool releases it, and so does a covered frame of
     * a pool that does not keep it, such as a reduce queue task's, so a reader that keeps a value
     * past the frame copies it. A {@link PageFrameMemoryRecord} bound to this memory reports the
     * same through {@link PageFrameMemoryRecord#isStable()}.
     */
    default boolean isStable() {
        return getFrameFormat() == PartitionFormat.NATIVE;
    }
}
