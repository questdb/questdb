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


import io.questdb.NullIndexFrameCursor;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnVersionReader;
import io.questdb.cairo.IndexFrameCursor;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.RowCursor;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.Path;

public class IndexFwdNullReader implements IndexReader {
    private final ObjList<NullCursor> freeCursors = new ObjList<>();
    private long columnTxn;
    private long partitionTxn;

    public IndexFwdNullReader(long columnTxn, long partitionTxn) {
        this.columnTxn = columnTxn;
        this.partitionTxn = partitionTxn;
    }

    @Override
    public void close() {
        Misc.clear(freeCursors);
    }

    @Override
    public long getColumnTop() {
        return 0;
    }

    @Override
    public long getColumnTxn() {
        return columnTxn;
    }

    @Override
    public RowCursor getCursor(int key, long minValue, long maxValue) {
        NullCursor c;
        if (freeCursors.size() > 0) {
            c = freeCursors.popLast();
            c.isPooled = false;
        } else {
            c = new NullCursor(this.freeCursors);
        }
        c.maxValue = key == 0 ? maxValue - minValue + 1 : 0;
        c.value = 0;
        return c;
    }

    @Override
    public IndexFrameCursor getFrameCursor(int key, long minValue, long maxValue) {
        return NullIndexFrameCursor.INSTANCE;
    }

    @Override
    public long getKeyBaseAddress() {
        return 0;
    }

    @Override
    public int getKeyCount() {
        return 1;
    }

    @Override
    public long getKeyMemorySize() {
        return 0;
    }

    @Override
    public long getPartitionTxn() {
        return partitionTxn;
    }

    @Override
    public long getValueBaseAddress() {
        return 0;
    }

    @Override
    public int getValueBlockCapacity() {
        return 0;
    }

    @Override
    public long getValueMemorySize() {
        return 0;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void of(CairoConfiguration configuration, Path path, CharSequence columnName, long columnNameTxn,
                   long partitionTxn, long columnTop, RecordMetadata metadata,
                   ColumnVersionReader columnVersionReader, long partitionTimestamp) {
        this.partitionTxn = partitionTxn;
        this.columnTxn = columnNameTxn;
    }

    @Override
    public void reloadConditionally() {
        // no-op
    }


    private static class NullCursor implements RowCursor {
        private final ObjList<NullCursor> pool;
        private boolean isPooled;
        private long maxValue;
        private long value;

        NullCursor(ObjList<NullCursor> freeCursors) {
            this.pool = freeCursors;
        }

        @Override
        public void close() {
            if (!isPooled && pool.size() < MAX_CACHED_FREE_CURSORS) {
                pool.add(this);
                isPooled = true;
            }
        }

        @Override
        public boolean hasNext() {
            return value < maxValue;
        }

        @Override
        public long next() {
            return value++;
        }
    }
}
