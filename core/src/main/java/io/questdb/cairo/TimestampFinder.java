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

package io.questdb.cairo;

import io.questdb.std.Vect;

/**
 * Interface for efficiently searching and accessing timestamp values within QuestDB table partitions.
 *
 * <p>This interface provides methods for binary searching timestamp values and retrieving min/max
 * timestamps both from partition metadata (approximate) and actual column data (exact). It is primarily
 * used by QuestDB's time interval intrinsics to quickly locate relevant data ranges without scanning
 * entire partitions.</p>
 *
 * <p>Implementations must support binary search operations on timestamp columns with scan direction
 * {@link io.questdb.std.Vect#BIN_SEARCH_SCAN_DOWN}, returning positive indices that represent the
 * position of values equal to or less than the search target.</p>
 */
public interface TimestampFinder {

    /**
     * Performs search on timestamp column. Effectively, the search is the same as
     * calling {@link Vect#binarySearch64Bit(long, long, long, long, int)} on the partition slice,
     * but the returned result is always positive.
     * <p>
     * The scan direction is {@link Vect#BIN_SEARCH_SCAN_DOWN}.
     *
     * @param value timestamp value to find, the found value is equal or less than this value
     * @param rowLo row low index for the search boundary, inclusive
     * @param rowHi row high index for the search boundary, inclusive
     * @return search result
     */
    long findTimestamp(long value, long rowLo, long rowHi);

    /**
     * Max partition timestamp that can be determined from partition metadata, without having to read the
     * timestamp column data.
     *
     * @return max timestamp as inferred from partition name and metadata
     */
    long maxTimestampApproxFromMetadata();

    /**
     * Max partition timestamp as read from the timestamp column data. This method must only be called after
     * prepare() was invoked.
     *
     * @return max timestamp value from timestamp column
     * @see #prepare()
     */
    long maxTimestampExact();

    /**
     * Min partition timestamp that can be determined from partition metadata, without having to read the
     * timestamp column data.
     *
     * @return min timestamp as inferred from partition name and metadata
     */
    long minTimestampApproxFromMetadata();

    /**
     * Min partition timestamp as read from the timestamp column data. This method must only be called after
     * prepare() was invoked.
     *
     * @return min timestamp value from timestamp column
     * @see #prepare()
     */
    long minTimestampExact();

    /**
     * Ensures timestamp column is ready to be read. Must be called before accessing exact timestamp methods
     * or retrieving timestamp values at specific row indices.
     */
    void prepare();

    /**
     * Retrieves the timestamp value at the specified row index within the partition.
     *
     * @param rowIndex the row index to get the timestamp from
     * @return timestamp value at the given row index
     * @see #prepare()
     */
    long timestampAt(long rowIndex);
}
