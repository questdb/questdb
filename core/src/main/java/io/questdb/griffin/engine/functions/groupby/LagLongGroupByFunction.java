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

package io.questdb.griffin.engine.functions.groupby;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

public class LagLongGroupByFunction extends LongFunction implements GroupByFunction {
    private final Function column;
    private final int offset;
    private final long defaultValue;
    private int valueIndex;
    private final ObjList<LongRingBuffer> groupIdxToBuffer;
    private LongRingBuffer currBuffer;
    private int readerGroupIdx;

    public LagLongGroupByFunction(@NotNull Function column, int offset, long defaultValue) {
        assert offset > 0;
        this.column = column;
        this.offset = offset;
        this.defaultValue = defaultValue;
        groupIdxToBuffer = new ObjList<>();
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        groupIdxToBuffer.add(currBuffer = new LongRingBuffer(mapValue));
        currBuffer.putLong(column.getLong(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        currBuffer.putLong(column.getLong(record));
    }

    @Override
    public void setNull(MapValue mapValue) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long getLong(Record record) {
        currBuffer = groupIdxToBuffer.get(readerGroupIdx++);
        return currBuffer.getLong();
    }

    @Override
    public void close() {
        groupIdxToBuffer.clear();
        currBuffer = null;
    }

    private class LongRingBuffer {
        private final MapValue mapValue;
        private final int capacity;
        private final long[] buf;
        private int appendOffset;
        private long result;

        LongRingBuffer(MapValue mapValue) {
            this.mapValue = mapValue;
            capacity = offset + 1;
            buf = new long[capacity];
            Arrays.fill(buf, defaultValue);
        }

        void putLong(long value) {
            buf[appendOffset++ % capacity] = value;
        }

        long getLong() {
            if (appendOffset > capacity) {
                result = buf[appendOffset % capacity];
            } else {
                int idx = appendOffset - offset - 1;
                result = idx < 0 ? defaultValue : buf[idx];
            }
            if (result == Numbers.LONG_NaN) {
                result = defaultValue;
            }
            mapValue.putLong(valueIndex, result);
            return result;
        }
    }
}
