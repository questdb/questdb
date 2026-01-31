/*******************************************************************************
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
     * Returns row ID offset used to compute real row IDs.
     */
    long getRowIdOffset();

    /**
     * Returns true if any column has a column top (zero address).
     */
    boolean hasColumnTops();
}
