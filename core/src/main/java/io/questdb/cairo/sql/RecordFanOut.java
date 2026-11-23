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

/**
 * Counts the rows that the factories above a scan return for one row of the scan. UNNEST returns a row for
 * each element of the longest array of a row, and no row for a row whose arrays are all empty or NULL. A
 * filter that collects the last rows of a negative LIMIT counts the rows above it with it instead of its own
 * rows, see io.questdb.griffin.engine.table.AsyncFilteredNegativeLimitRecordCursor.
 */
@FunctionalInterface
public interface RecordFanOut {

    /**
     * Returns at most the rows that the factories above the scan return for the row that the record points
     * at, and 0 exactly when they return none, so that the rows of a scan that add up to N return at least
     * N rows above it.
     *
     * @param scanRecord a record of the scan, which the call may read but not move
     * @return the rows that the factories above the scan return for the row, or fewer
     */
    long getFanOut(Record scanRecord);
}
