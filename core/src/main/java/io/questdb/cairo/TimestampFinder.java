/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
 * Searches designated timestamp values in a table partition.
 * Used in time interval intrinsics.
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

    long maxTimestamp();

    long minTimestamp();

    long timestampAt(long rowIndex);
}
