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

import io.questdb.cairo.NullPolicy;
import io.questdb.std.DirectLongList;
import io.questdb.std.IntHashSet;
import org.jetbrains.annotations.Nullable;

/**
 * Represents page frame as a set of per column contiguous memory.
 * For native partitions, it's simply a slice of mmapped memory.
 * For Parquet partitions, it's a deserialized in-memory native format.
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

    int getColumnCount();

    /**
     * Returns the frame's column-vector descriptor: per column, the data and aux vectors, the
     * NULL policy and the validity fields. Consumers read the frame's column data only through
     * it. The descriptor belongs to this frame memory and changes when the memory moves to
     * another frame; a record that must keep a frame copies it.
     */
    ColumnVectorDescriptor getColumnVectorDescriptor();

    /**
     * Returns the per-column leading column-top count for this frame, or {@code null} when
     * the frame has none (e.g. native frames), indexed like the descriptor's lists (its column
     * offset plus the column index). Used by {@link PageFrameMemoryRecord} to
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
     * Returns the NULL policy of the stored source column for a fixed-to-var type-cast
     * column (a non-negative {@link #getSourceColumnType(int)}), read from the Parquet
     * file's per-column accessor; null for any other column.
     */
    @Nullable
    NullPolicy getSourceColumnNullPolicy(int columnIndex);

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
}
