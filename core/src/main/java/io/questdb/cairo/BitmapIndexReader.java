/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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


import io.questdb.cairo.sql.RowCursor;

import java.io.Closeable;

public interface BitmapIndexReader extends Closeable {

    int DIR_FORWARD = 1;
    int DIR_BACKWARD = 2;

    @Override
    default void close() {
    }

    /**
     * Setup value cursor. Values in this cursor will be bounded by provided
     * minimum and maximum, both of which are inclusive. Order of values is
     * determined by specific implementations of this method.
     *
     * @param cachedInstance when this parameters is true, index reader may return singleton instance of cursor.
     * @param key            index key
     * @param minValue       inclusive minimum value
     * @param maxValue       inclusive maximum value
     * @return index value cursor
     */
    RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue);

    int getKeyCount();

    default IndexFrameCursor getFrameCursor(int key, long minValue, long maxValue) {
        throw new UnsupportedOperationException();
    }

    boolean isOpen();

    long getKeyBaseAddress();

    long getKeyMemorySize();

    long getValueBaseAddress();

    long getValueMemorySize();

    long getUnIndexedNullCount();

    int getValueBlockCapacity();
}