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

    long getColumnTop();

    long getColumnTxn();

    /**
     * Setup value cursor. Values in this cursor will be bounded by provided
     * minimum and maximum, both of which are inclusive. Order of values is
     * determined by specific implementations of this method.
     *
     * @param cachedInstance when this parameter is true, index reader may return singleton instance of cursor.
     * @param key            index key
     * @param minValue       inclusive minimum value
     * @param maxValue       inclusive maximum value
     * @return index value cursor, relative to the minValue
     */
    RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue);

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
