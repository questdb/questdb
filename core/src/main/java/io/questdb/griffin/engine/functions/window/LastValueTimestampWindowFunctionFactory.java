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

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * last_value() over a TIMESTAMP argument. Registers the {@code last_value(N)} signature and supplies
 * the thin TIMESTAMP subclasses of the shared {@link LastValueWindowFunctionFactoryHelper} bases. Each
 * value subclass caches the timestamp driver once and exposes {@code getTimestamp()} as the stored value
 * plus a {@code getDate()} that converts ticks to DATE milliseconds. Shapes that write the result column
 * directly during a backward pass expose only {@code getType()}.
 */
public class LastValueTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = LastValueWindowFunctionFactoryHelper.NAME + "(N)";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        return LastValueWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                configuration,
                sqlExecutionContext,
                supportNullsDesc(),
                PartitionTimestamp::new,
                PartitionRangeTimestamp::new,
                PartitionRowsTimestamp::new,
                RangeTimestamp::new,
                RowsTimestamp::new,
                WholeResultSetTimestamp::new,
                IncludeCurrentTimestamp::new,
                IncludeCurrentPartitionRowsTimestamp::new,
                NotNullPartitionTimestamp::new,
                NotNullUnboundedPartitionRowsTimestamp::new,
                NotNullPartitionRangeTimestamp::new,
                NotNullPartitionRowsTimestamp::new,
                NotNullCurrentRowTimestamp::new,
                NotNullWholeResultSetTimestamp::new,
                NotNullUnboundedRowsTimestamp::new,
                NotNullRangeTimestamp::new,
                NotNullRowsTimestamp::new
        );
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    static final class IncludeCurrentPartitionRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueIncludeCurrentPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        IncludeCurrentPartitionRowsTimestamp(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(rowsLo, isRange, partitionByRecord, partitionBySink, arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(value);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class IncludeCurrentTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueIncludeCurrentFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        IncludeCurrentTimestamp(long rowsLo, boolean isRange, Function arg) {
            super(rowsLo, isRange, arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(value);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullCurrentRowTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverCurrentRowBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullCurrentRowTimestamp(Function arg) {
            super(arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(value);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullPartitionRangeTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullPartitionRangeTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx,
                    partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullPartitionRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullPartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullPartitionTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionBase implements WindowTimestampFunction {

        NotNullPartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullRangeTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullRangeTimestamp(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullRowsTimestamp(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullUnboundedPartitionRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverUnboundedPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullUnboundedPartitionRowsTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(value);
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullUnboundedRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullOverUnboundedRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullUnboundedRowsTimestamp(Function arg) {
            super(arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullWholeResultSetTimestamp extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverWholeResultSetBase implements WindowTimestampFunction {

        NotNullWholeResultSetTimestamp(Function arg) {
            super(arg);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRangeTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRangeTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                int initialBufferSize,
                int timestampIdx,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx,
                    partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionBase implements WindowTimestampFunction {

        PartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RangeTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RangeTimestamp(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RowsTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RowsTimestamp(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lastValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lastValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class WholeResultSetTimestamp extends LastValueWindowFunctionFactoryHelper.LastValueOverWholeResultSetBase implements WindowTimestampFunction {

        WholeResultSetTimestamp(Function arg) {
            super(arg);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }
}
