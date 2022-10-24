/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

import io.questdb.cairo.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.orderby.RecordComparatorCompiler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class RowNumberFunctionFactory implements FunctionFactory {

    private static final SingleColumnType LONG_COLUMN_TYPE = new SingleColumnType(ColumnType.LONG);

    @Override
    public String getSignature() {
        return "row_number()";
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final AnalyticContext analyticContext = sqlExecutionContext.getAnalyticContext();
        if (analyticContext.isEmpty()) {
            throw SqlException.$(position, "analytic function called in non-analytic context, make sure to add OVER clause");
        }

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
        if (analyticContext.isOrdered()) {
            return new OrderRowNumberFunction();
        }
        return new SequenceRowNumberFunction();
    }

    private static class RowNumberFunction extends LongFunction implements ScalarFunction, AnalyticFunction, Reopenable {
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
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public long getLong(Record rec) {
            // not called
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {
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
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), x + 1);
        }

        @Override
        public void preparePass2(RecordCursor cursor) {
        }

        @Override
        public void pass2(Record record) {
        }

        @Override
        public void reopen() {
            map.reopen();
        }

        @Override
        public void reset() {
            map.close();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }
    }

    private static class OrderRowNumberFunction extends LongFunction implements ScalarFunction, AnalyticFunction, Reopenable {
        private long next = 1;
        private int columnIndex;

        public OrderRowNumberFunction() {
        }

        @Override
        public void close() {
        }

        @Override
        public long getLong(Record rec) {
            // not called
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isReadThreadSafe() {
            return false;
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), next);
            next++;
        }

        @Override
        public void preparePass2(RecordCursor cursor) {
        }

        @Override
        public void pass2(Record record) {
        }

        @Override
        public void reopen() {
            reset();
        }

        @Override
        public void reset() {
            next = 1;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }
    }

    private static class SequenceRowNumberFunction extends LongFunction implements ScalarFunction, AnalyticFunction, Reopenable {
        private long next = 1;
        private int columnIndex;

        @Override
        public long getLong(Record rec) {
            // not called
            throw new UnsupportedOperationException();
        }

        @Override
        public void toTop() {
            next = 1;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            toTop();
        }

        @Override
        public void reopen() {
            toTop();
        }

        @Override
        public void initRecordComparator(RecordComparatorCompiler recordComparatorCompiler, ArrayColumnTypes chainTypes, IntList order) {
        }

        @Override
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), next++);
        }

        @Override
        public void preparePass2(RecordCursor cursor) {
        }

        @Override
        public void pass2(Record record) {
        }

        @Override
        public void reset() {
            toTop();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }
    }
}
