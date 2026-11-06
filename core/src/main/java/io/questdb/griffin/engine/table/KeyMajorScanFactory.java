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


package io.questdb.griffin.engine.table;

/**
 * A factory whose record cursor may walk an index key by key across the whole scan, see
 * {@link KeyMajorPageFrameRecordCursor}. Such a cursor emits all rows of one key before any row of
 * the next key, so a consumer that keeps state per key, such as a window partitioned by that key,
 * can process the keys independently of each other.
 */
public interface KeyMajorScanFactory {

    /**
     * The index, in this factory's metadata, of the indexed column whose keys the cursor walks one
     * by one, or -1 when this factory's cursor is not a {@link KeyMajorPageFrameRecordCursor}.
     */
    int getKeyMajorColumnIndex();

    /**
     * How many keys the scan walks, when the plan knows it (an IN list's values), or -1. Lets the
     * planner estimate the share of the table the scan reads.
     */
    default int getKeyMajorKeyCount() {
        return -1;
    }

    /**
     * Whether the cursor's walk visits each value of the key column at most once, so that all
     * rows of a value form one run of the walk. A consumer that restarts its per-key state at
     * each key of the walk, such as a parallel window, relies on this. A factory whose keys may
     * repeat, for example because they come from bind values, must remove the repeats before it
     * answers true.
     */
    default boolean hasDistinctKeys() {
        return false;
    }

    /**
     * The index, in this factory's metadata, of a column whose values ascend within each key of
     * the walk, the table's designated timestamp when the walk reads each key's rows forward, or
     * -1. A window {@code PARTITION BY <key> ORDER BY <that column>} over the walk sees each
     * partition in order already.
     */
    default int getKeyMajorTimestampIndex() {
        return -1;
    }

    /**
     * Whether the walk visits its keys in ascending order of their values, as
     * {@code ORDER BY <key> ASC} sorts them.
     */
    default boolean isKeyMajorAscending() {
        return false;
    }
}
