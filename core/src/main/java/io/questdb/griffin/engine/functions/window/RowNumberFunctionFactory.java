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

package io.questdb.griffin.engine.functions.window;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class RowNumberFunctionFactory implements FunctionFactory {

    public static final String NAME = "row_number";
    // Value layout: [rowNumber:LONG, lastActivityTs:LONG]. The second slot drives
    // live view Phase 5 partition-state eviction — see
    // {@link RowNumberFunction#evictStalePartitionState}. For non-live-view
    // queries it is written but never read; the storage overhead is 8 bytes per
    // partition key for the lifetime of the query.
    private static final int ROW_NUMBER_VALUE_INDEX = 0;
    private static final int LAST_ACTIVITY_TS_VALUE_INDEX = 1;
    private static final ArrayColumnTypes VALUE_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isWindow() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getPartitionByRecord() != null) {
            // The WindowContext's partitionByKeyTypes is a transient buffer owned by
            // SqlCodeGenerator that gets cleared on every window function compile. Phase 5
            // partition eviction needs to allocate a scratch Map with the same key shape
            // after compilation has moved on, so take our own copy.
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            ColumnTypes contextKeyTypes = windowContext.getPartitionByKeyTypes();
            for (int i = 0, n = contextKeyTypes.getColumnCount(); i < n; i++) {
                keyTypes.add(contextKeyTypes.getColumnType(i));
            }
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    keyTypes,
                    VALUE_COLUMN_TYPES
            );
            return new RowNumberFunction(
                    map,
                    windowContext.getPartitionByRecord(),
                    windowContext.getPartitionBySink(),
                    windowContext.getTimestampIndex(),
                    keyTypes,
                    configuration
            );
        }

        return new SequenceRowNumberFunction();
    }

    private static class RowNumberFunction extends LongFunction implements WindowFunction, Reopenable {
        private final CairoConfiguration configuration;
        private final ColumnTypes keyColumnTypes;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private final int tsColumnIndex;
        private int columnIndex;
        private Map map;
        private long rowNumber;
        private long sizeAfterLastEvict;

        public RowNumberFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                int tsColumnIndex,
                ColumnTypes keyColumnTypes,
                CairoConfiguration configuration
        ) {
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.tsColumnIndex = tsColumnIndex;
            this.keyColumnTypes = keyColumnTypes;
            this.configuration = configuration;
        }

        @Override
        public void close() {
            Misc.free(map);
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long x;
            if (value.isNew()) {
                x = 0;
            } else {
                x = value.getLong(ROW_NUMBER_VALUE_INDEX);
            }
            rowNumber = x + 1;
            value.putLong(ROW_NUMBER_VALUE_INDEX, rowNumber);
            // Track per-key last-activity-ts for live view retention-driven eviction.
            // tsColumnIndex is -1 when the window is defined over a source with no
            // designated timestamp; writing Long.MIN_VALUE keeps those keys below any
            // eviction cutoff, so they never get evicted (live views always have one).
            value.putLong(LAST_ACTIVITY_TS_VALUE_INDEX,
                    tsColumnIndex >= 0 ? record.getTimestamp(tsColumnIndex) : Long.MIN_VALUE);
        }

        @Override
        public void evictStalePartitionState(long cutoffTs) {
            long size = map.size();
            if (size == 0 || size < sizeAfterLastEvict * 2) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, VALUE_COLUMN_TYPES);
            try {
                scratch.setKeyCapacity((int) Math.min(size, Integer.MAX_VALUE));
                PartitionStateEvictor.rebuildKeeping(map, scratch, LAST_ACTIVITY_TS_VALUE_INDEX, cutoffTs);
                Misc.free(map);
                map = scratch;
                scratch = null;
                sizeAfterLastEvict = map.size();
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public long getLong(Record rec) {
            return rowNumber;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rowNumber);
        }

        @Override
        public void reopen() {
            rowNumber = 0;
            sizeAfterLastEvict = 0;
            map.reopen();
        }

        @Override
        public void reset() {
            map.close();
            sizeAfterLastEvict = 0;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            rowNumber = 0;
            sizeAfterLastEvict = 0;
            map.clear();
        }
    }

    private static class SequenceRowNumberFunction extends LongFunction implements WindowFunction, Reopenable {
        private int columnIndex;
        private long rowNumber = 0;

        @Override
        public void computeNext(Record record) {
            ++rowNumber;
        }

        @Override
        public long getLong(Record rec) {
            return rowNumber;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            toTop();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), ++rowNumber);
        }

        @Override
        public void reopen() {
            toTop();
        }

        @Override
        public void reset() {
            toTop();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        @Override
        public void toTop() {
            rowNumber = 0;
        }
    }

    static {
        VALUE_COLUMN_TYPES = new ArrayColumnTypes();
        VALUE_COLUMN_TYPES.add(ColumnType.LONG); // rowNumber
        VALUE_COLUMN_TYPES.add(ColumnType.LONG); // lastActivityTs (live view Phase 5)
    }
}
