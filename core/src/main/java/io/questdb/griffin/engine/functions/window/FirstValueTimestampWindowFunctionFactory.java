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
 * first_value() over a TIMESTAMP argument. Registers the {@code first_value(N)} signature and supplies
 * the thin TIMESTAMP subclasses of the shared {@link FirstValueWindowFunctionFactoryHelper} bases. Each
 * value subclass caches the timestamp driver once and exposes {@code getTimestamp()} as the stored value
 * plus a {@code getDate()} that converts ticks to DATE milliseconds. Two-pass shapes expose only
 * {@code getType()}.
 */
public class FirstValueTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = FirstValueWindowFunctionFactoryHelper.NAME + "(N)";

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
        return FirstValueWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                configuration,
                sqlExecutionContext,
                supportNullsDesc(),
                CurrentRowTimestamp::new,
                PartitionTimestamp::new,
                PartitionRangeTimestamp::new,
                PartitionRowsTimestamp::new,
                UnboundedPartitionRowsTimestamp::new,
                RangeTimestamp::new,
                RowsTimestamp::new,
                WholeResultSetTimestamp::new,
                NotNullPartitionTimestamp::new,
                NotNullPartitionRangeTimestamp::new,
                NotNullPartitionRowsTimestamp::new,
                NotNullUnboundedPartitionRowsTimestamp::new,
                NotNullRangeTimestamp::new,
                NotNullRowsTimestamp::new,
                NotNullWholeResultSetTimestamp::new
        );
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    static final class CurrentRowTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverCurrentRowBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        CurrentRowTimestamp(Function arg, boolean ignoreNulls) {
            super(arg, ignoreNulls);
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

    static final class NotNullPartitionRangeTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionRangeFrameBase implements WindowTimestampFunction {
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
                int timestampIdx
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullPartitionRowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullPartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullPartitionTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionBase implements WindowTimestampFunction {

        NotNullPartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullRangeTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverRangeFrameBase implements WindowTimestampFunction {
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
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullRowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullRowsTimestamp(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class NotNullUnboundedPartitionRowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverUnboundedPartitionRowsFrameBase implements WindowTimestampFunction {
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

    static final class NotNullWholeResultSetTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverWholeResultSetBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        NotNullWholeResultSetTimestamp(Function arg) {
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

    static final class PartitionRangeTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionRangeFrameBase implements WindowTimestampFunction {
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
                int timestampIdx
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RangeTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverRangeFrameBase implements WindowTimestampFunction {
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
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RowsTimestamp(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(firstValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return firstValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class UnboundedPartitionRowsTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverUnboundedPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        UnboundedPartitionRowsTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
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

    static final class WholeResultSetTimestamp extends FirstValueWindowFunctionFactoryHelper.FirstValueOverWholeResultSetBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        WholeResultSetTimestamp(Function arg) {
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
}
