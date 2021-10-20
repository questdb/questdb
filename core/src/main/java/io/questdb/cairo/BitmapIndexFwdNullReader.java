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


import io.questdb.NullIndexFrameCursor;
import io.questdb.cairo.sql.RowCursor;

public class BitmapIndexFwdNullReader implements BitmapIndexReader {

    private final NullCursor cursor = new NullCursor();

    @Override
    public RowCursor getCursor(boolean cachedInstance, int key, long minValue, long maxValue) {
        final NullCursor cursor = getCursor(cachedInstance);
        cursor.max = maxValue + 1;
        cursor.value = 0;
        return cursor;
    }

    @Override
    public int getKeyCount() {
        return 1;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return 0;
    }

    @Override
    public long getUnIndexedNullCount() {
        return 0;
    }

    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    private NullCursor getCursor(boolean cachedInstance) {
        return cachedInstance ? cursor : new NullCursor();
    }

    @Override
    public IndexFrameCursor getFrameCursor(int key, long minValue, long maxValue) {
        return NullIndexFrameCursor.INSTANCE;
    }

    private static class NullCursor implements RowCursor {
        private long max;
        private long value;

        @Override
        public boolean hasNext() {
            return value < max;
        }

        @Override
        public long next() {
            return value++;
        }
    }
}
