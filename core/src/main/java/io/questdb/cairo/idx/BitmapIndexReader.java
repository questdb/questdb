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

package io.questdb.cairo.idx;


import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Transient;
import io.questdb.std.str.Path;

import java.io.Closeable;

public interface BitmapIndexReader extends Closeable {

    int DIR_BACKWARD = 2;
    int DIR_FORWARD = 1;
    String NAME_BACKWARD = "backward";
    String NAME_FORWARD = "forward";

    static CharSequence nameOf(int direction) {
        return DIR_FORWARD == direction ? NAME_FORWARD : NAME_BACKWARD;
    }

    @Override
    default void close() {
    }

    /**
     * Bulk-marks all keys present in this partition into the bit set.
     * Walks dense generation strides sequentially (one pass, optimal page access).
     * Returns the number of newly found keys.
     * For bitmap indexes this is a no-op returning -1 (caller uses getCursor fallback).
     *
     * @param foundKeys bit set indexed by index key (0 = NULL, 1..N = symbol keys)
     * @return number of keys newly marked as found, or -1 if not supported
     */
    default int collectDistinctKeys(io.questdb.std.BitSet foundKeys) {
        return -1;
    }

    long getColumnTop();

    long getColumnTxn();

    /**
     * Setup value cursor. Values in this cursor will be bounded by provided
     * minimum and maximum, both of which are inclusive. Order of values is
     * determined by specific implementations of this method.
     *
     * @param slotId   cursor slot identifier (non-negative)
     * @param key      index key
     * @param minValue inclusive minimum value
     * @param maxValue inclusive maximum value
     * @return index value cursor, relative to the minValue
     */
    RowCursor getCursor(int slotId, int key, long minValue, long maxValue);

    default IndexFrameCursor getFrameCursor(int key, long minValue, long maxValue) {
        throw new UnsupportedOperationException();
    }

    long getKeyBaseAddress();

    int getKeyCount();

    long getKeyMemorySize();

    long getPartitionTxn();

    long getValueBaseAddress();

    int getValueBlockCapacity();

    long getValueMemorySize();

    boolean isOpen();

    void of(
            CairoConfiguration configuration,
            @Transient Path path,
            CharSequence columnName,
            long columnNameTxn,
            long partitionTxn,
            long columnTop
    );

    void reloadConditionally();
}
