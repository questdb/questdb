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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.std.DirectLongList;
import io.questdb.std.QuietCloseable;

/**
 * Iterator that produces horizon timestamps (master_timestamp + offset) in sorted order.
 * <p>
 * For each master row in a page frame and each offset in the offset list, this generates
 * tuples of (horizonTimestamp, masterRowIndex, offsetIndex). The tuples are produced in
 * horizonTimestamp order, enabling efficient sequential ASOF lookups.
 */
public interface AsyncHorizonTimestampIterator extends QuietCloseable {

    /**
     * Returns the horizon timestamp of the current tuple.
     */
    long getHorizonTimestamp();

    /**
     * Returns the master row index of the current tuple (relative to frame start).
     */
    long getMasterRowIndex();

    /**
     * Returns the offset index of the current tuple.
     */
    int getOffsetIndex();

    /**
     * Advances to the next tuple and returns true, or returns false if there are no more tuples.
     * After a true return, use getHorizonTimestamp(), getMasterRowIndex(), and getOffsetIndex()
     * to access the current tuple's values.
     */
    boolean next();

    /**
     * Initializes the iterator for a new page frame.
     *
     * @param record               record positioned at the frame (used to read timestamps)
     * @param frameRowLo           first row index in the frame
     * @param frameRowCount        number of rows in the frame
     * @param timestampColumnIndex index of the timestamp column in the record
     */
    void of(PageFrameMemoryRecord record, long frameRowLo, long frameRowCount, int timestampColumnIndex);

    /**
     * Initializes the iterator for filtered rows in a page frame.
     *
     * @param record               record positioned at the frame (used to read timestamps)
     * @param filteredRows         list of filtered row indices (relative to frame start)
     * @param timestampColumnIndex index of the timestamp column in the record
     */
    void ofFiltered(PageFrameMemoryRecord record, DirectLongList filteredRows, int timestampColumnIndex);
}
