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
import io.questdb.cairo.map.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.*;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.analytic.AnalyticContext;
import io.questdb.griffin.engine.analytic.AnalyticFunction;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.std.*;

public class RankFunctionFactory implements FunctionFactory {

    private static final SingleColumnType LONG_COLUMN_TYPE = new SingleColumnType(ColumnType.LONG);

    @Override
    public String getSignature() {
        return "rank()";
    }

    @Override
    public Function newInstance(int position, ObjList<Function> args, IntList argPositions, CairoConfiguration configuration, SqlExecutionContext sqlExecutionContext) {
        final AnalyticContext analyticContext = sqlExecutionContext.getAnalyticContext();

        if (analyticContext.getPartitionByRecord() != null && analyticContext.isOrdered()) {
            Map map = MapFactory.createMap(
                    configuration,
                    analyticContext.getPartitionByKeyTypes(),
                    LONG_COLUMN_TYPE
            );
            Map rankMap = MapFactory.createMap(
                    configuration,
                    analyticContext.getPartitionByKeyTypes(),
                    LONG_COLUMN_TYPE
            );
            Map offsetMap = MapFactory.createMap(
                    configuration,
                    analyticContext.getPartitionByKeyTypes(),
                    LONG_COLUMN_TYPE
            );
            return new RankFunction(map, rankMap, offsetMap, analyticContext.getPartitionByRecord(), analyticContext.getPartitionBySink());
        }
        return new SequenceRankFunction();
    }

    private static class SequenceRankFunction extends LongFunction implements ScalarFunction {
        private long next = 1;

        @Override
        public long getLong(Record rec) {
            return next;
        }

        @Override
        public void toTop() {
            next = 1;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            toTop();
        }
    }

    private static class RankFunction extends LongFunction implements ScalarFunction, AnalyticFunction, Reopenable {
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private final Map map;
        private final Map rankMap;
        private final Map offsetMap;
        private int columnIndex;
        private RecordComparator recordComparator;


        public RankFunction(Map map, Map rankMap, Map offsetMap, VirtualRecord partitionByRecord, RecordSink partitionBySink) {
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.map = map;
            this.rankMap = rankMap;
            this.offsetMap = offsetMap;
        }

        @Override
        public void close() {
            Misc.free(map);
            Misc.free(rankMap);
            Misc.free(offsetMap);
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
        public void pass1(Record record, long recordOffset, AnalyticSPI spi) {
            partitionByRecord.of(record);

            MapKey maxIndexKey = map.withKey();
            maxIndexKey.put(partitionByRecord, partitionBySink);
            MapValue maxIndexValue = maxIndexKey.createValue();
            long maxIndex;
            if (maxIndexValue.isNew()) {
                maxIndex = 0;
            } else {
                maxIndex = maxIndexValue.getLong(0);
            }

            MapKey currentIndexKey = rankMap.withKey();
            currentIndexKey.put(partitionByRecord, partitionBySink);
            MapValue currentIndexValue = currentIndexKey.createValue();
            long currentIndex;
            if (currentIndexValue.isNew()) {
                currentIndex = 0;
            } else {
                currentIndex = currentIndexValue.getLong(0);
            }

            MapKey offsetKey = offsetMap.withKey();
            offsetKey.put(partitionByRecord, partitionBySink);
            MapValue offsetValue = offsetKey.createValue();
            if (offsetValue.isNew()) {
                offsetValue.putLong(0, recordOffset);
                currentIndexValue.putLong(0, currentIndex + 1);
            } else {
                // compare with prev record
                long offset = offsetValue.getLong(0);
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.cloneRecord(offset)) != 0) {
                    offsetValue.putLong(0, recordOffset);
                    currentIndexValue.putLong(0, maxIndex + 1);
                }
            }
            maxIndexValue.putLong(0, maxIndex + 1);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), currentIndexValue.getLong(0));
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
            rankMap.reopen();
            offsetMap.reopen();
        }

        @Override
        public void reset() {
            map.close();
            rankMap.close();
            offsetMap.close();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void setRecordComparator(RecordComparator recordComparator) {
            this.recordComparator = recordComparator;
        }
    }
}