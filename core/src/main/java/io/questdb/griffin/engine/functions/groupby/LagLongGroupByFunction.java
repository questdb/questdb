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
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

// TODO: other types are missing (whichever types are generally accepted as args by aggregation functions)
public class LagLongGroupByFunction extends LongFunction implements GroupByFunction {
    private final Function column;
    private final int offset;
    private final long defaultValue;
    private int valueIndex;
    // visible for testing
    final LongRingCache cache;

    public LagLongGroupByFunction(@NotNull Function column, int offset, long defaultValue) {
        assert offset > 0;
        this.column = column;
        this.offset = offset;
        this.defaultValue = defaultValue;
        this.cache = new LongRingCache();
    }

    @Override
    public void computeFirst(MapValue mapValue, Record record) {
        cache.reset(mapValue);
        cache.putLong(column.getLong(record));
    }

    @Override
    public void computeNext(MapValue mapValue, Record record) {
        cache.putLong(column.getLong(record));
    }

    @Override
    public void pushValueTypes(ArrayColumnTypes columnTypes) {
        valueIndex = columnTypes.getColumnCount();
        columnTypes.add(ColumnType.LONG);
    }

    @Override
    public void setNull(MapValue mapValue) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long getLong(Record record) {
        return cache.getLong();
    }

    // visible for testing
    class LongRingCache {
        private final int size;
        private final long[] buf;
        private int nextValIdx;
        private MapValue mapValue; // reset method sets it

        LongRingCache() {
            size = offset + 1;
            buf = new long[size];
            Arrays.fill(buf, defaultValue);
            nextValIdx = 0;
        }

        void reset(MapValue mapValue) {
            System.out.printf("RESET INVOKED%n");
            this.mapValue = mapValue;
            Arrays.fill(buf, defaultValue);
            this.nextValIdx = 0;
        }

        void putLong(long value) {
            buf[nextValIdx++ % size] = value;
        }

        long getLong() {
            long value;
            if (nextValIdx > size) {
                value = buf[nextValIdx % size];
            } else {
                int idx = nextValIdx - offset - 1;
                value = idx < 0 ? defaultValue : buf[idx];
            }
            value = value == Numbers.LONG_NaN ? defaultValue : value;
            mapValue.putLong(valueIndex, value);
            return value;
        }
    }
}
