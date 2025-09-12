/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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

package io.questdb.test.tools;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkSPI;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.vm.MemoryCARWImpl;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.engine.functions.IntFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.functions.TimestampFunction;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;

/**
 * Methods for quick creation of useful objects with reasonable defaults
 */
public final class TestDefaults {
    public static Function createIntFunction(java.util.function.Function<Record, Integer> f) {
        return new IntFunction() {
            @Override
            public int getInt(Record rec) {
                return f.apply(rec);
            }
        };
    }

    public static Function createLongFunction(java.util.function.Function<Record, Long> f) {
        return new LongFunction() {
            @Override
            public long getLong(Record rec) {
                return f.apply(rec);
            }
        };
    }

    public static MemoryCARW createMemoryCARW() {
        return new MemoryCARWImpl(4 * 1024, 1024, MemoryTag.NATIVE_DEFAULT);
    }

    public static OrderedMap createOrderedMap(ColumnTypes keyColumnTypes, ColumnTypes valueColumnTypes) {
        return new OrderedMap(4 * 1024, keyColumnTypes, valueColumnTypes, 64, 0.8, 24);
    }

    public static Record createRecord(short[] columnTypes, Object... values) {
        assert columnTypes.length == values.length;

        ObjList<Function> functions = new ObjList<>();
        for (int i = 0, n = values.length; i < n; i++) {
            final int index = i;
            switch (columnTypes[i]) {
                case ColumnType.INT:
                    functions.add(createIntFunction(x -> (int) values[index]));
                    break;
                case ColumnType.LONG:
                    functions.add(createLongFunction(x -> (long) values[index]));
                    break;
                case ColumnType.TIMESTAMP:
                    functions.add(createTimestampFunction(x -> (long) values[index]));
                    break;
                default:
                    throw new RuntimeException("unsupported column type: " + columnTypes[i]);
            }
        }
        return new VirtualRecord(functions);
    }

    public static RecordSink createRecordSink(java.util.function.BiConsumer<Record, RecordSinkSPI> f) {
        return new RecordSink() {
            @Override
            public void copy(Record r, RecordSinkSPI w) {
                f.accept(r, w);
            }

            @Override
            public void setFunctions(ObjList<Function> keyFunctions) {
            }
        };
    }

    public static Function createTimestampFunction(java.util.function.Function<Record, Long> f) {
        return new TimestampFunction(ColumnType.TIMESTAMP_MICRO) {
            @Override
            public long getTimestamp(Record rec) {
                return f.apply(rec);
            }
        };
    }

    public static VirtualRecord createVirtualRecord(Function... functions) {
        return new VirtualRecord(new ObjList<>(functions));
    }
}
