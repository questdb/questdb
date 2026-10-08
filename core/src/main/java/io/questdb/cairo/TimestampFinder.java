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

package io.questdb.cairo;

/**
 * Counts timestamp values within a table partition.
 *
 * <p>The returned counts use the finder's global partition-relative coordinate
 * space. A physical base finder returns physical row counts; a composite finder
 * may return logical base-plus-delta counts in the same coordinate space used by
 * partition frames.</p>
 */
public interface TimestampFinder {

    /**
     * Counts rows whose timestamp is strictly less than {@code timestamp}.
     */
    long countBefore(long timestamp);

    /**
     * Counts rows whose timestamp is less than or equal to {@code timestamp}.
     */
    long countThrough(long timestamp);

    /**
     * Conservative upper bound for the partition's maximum timestamp, without
     * reading timestamp column data. The returned value must not be less than
     * the exact maximum.
     *
     * @return upper bound for the maximum timestamp
     */
    long maxTimestampUpperBound();

    /**
     * Conservative lower bound for the partition's minimum timestamp, without
     * reading timestamp column data. The returned value must not be greater
     * than the exact minimum.
     *
     * @return lower bound for the minimum timestamp
     */
    long minTimestampLowerBound();

    /**
     * Prepares the currently bound partition for exact counts.
     */
    void prepare();
}
