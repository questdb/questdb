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

package io.questdb.griffin.plan.logical;

/**
 * The order an index access path reading a list of keys emits its rows in.
 */
public enum IndexOrder {
    /**
     * Not a read of several keys: the access path reads no index keys, or a single key in index order.
     */
    NONE,
    /**
     * Ordered by the index key, then by designated timestamp in the index direction, as the consumers request.
     */
    KEY,
    /**
     * Ordered by designated timestamp, as the consumers request, merging the keys row by row.
     */
    TIMESTAMP,
    /**
     * In table order, merging the keys row by row, so that the consumers may stop at any row.
     */
    TABLE,
    /**
     * In table order, merging all keys of a page frame before its first row.
     */
    TABLE_PER_FRAME,
    /**
     * Key by key within each page frame.
     */
    KEY_PER_FRAME;

    /**
     * Whether the order is the one the consumers request.
     */
    public boolean isRequested() {
        return this == KEY || this == TIMESTAMP;
    }

    /**
     * Whether each page frame emits its rows in table order.
     */
    public boolean isTableOrder() {
        return this == TIMESTAMP || this == TABLE || this == TABLE_PER_FRAME;
    }
}
