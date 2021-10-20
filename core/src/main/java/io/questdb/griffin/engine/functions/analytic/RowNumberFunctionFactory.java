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

package io.questdb.griffin.engine.functions.analytic;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.SingleColumnType;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

import java.io.Closeable;

public class RowNumberFunctionFactory implements FunctionFactory {

    private static final SingleColumnType LONG_COLUMN_TYPE = new SingleColumnType(ColumnType.LONG);

    @Override
    public String getSignature() {
        return "row_number()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final AnalyticContext analyticContext = sqlExecutionContext.getAnalyticContext();

        if (analyticContext.getPartitionByRecord() != null) {
            Map map = MapFactory.createMap(
                    configuration,
                    analyticContext.getPartitionByKeyTypes(),
                    LONG_COLUMN_TYPE
            );
            return new RowNumberFunction(
                    map,
                    analyticContext.getPartitionByRecord(),
                    analyticContext.getPartitionBySink()
            );
        }
        return null;
    }

    private static class RowNumberFunction extends LongFunction implements ScalarFunction, AnalyticFunction, Closeable {
        private final Map map;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private int columnIndex;

        public RowNumberFunction(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink) {
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
        }

        @Override
        public void close() {
            Misc.free(map);
            Misc.free(partitionByRecord.getFunctions());
        }

        @Override
        public long getLong(Record rec) {
            // not called
            throw new UnsupportedOperationException();
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long x;
            if (value.isNew()) {
                x = 0;
            } else {
                x = value.getLong(0);
            }
            value.putLong(0, x + 1);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), x);
        }

        @Override
        public void preparePass2(RecordCursor cursor) {
        }

        @Override
        public void pass2(Record record) {
        }

        @Override
        public void reset() {
            map.clear();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }
    }
}
