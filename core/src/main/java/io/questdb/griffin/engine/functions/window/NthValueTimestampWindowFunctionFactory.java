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
 * nth_value() over a TIMESTAMP argument. Registers the {@code nth_value(NL)} signature and supplies the
 * thin TIMESTAMP subclasses of the shared {@link NthValueWindowFunctionFactoryHelper} bases. Each value
 * subclass caches the timestamp driver once and exposes {@code getTimestamp()} as the stored value plus a
 * {@code getDate()} that converts ticks to DATE milliseconds. Two-pass shapes expose only {@code getType()}.
 */
public class NthValueTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = NthValueWindowFunctionFactoryHelper.NAME + "(NL)";

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
        return NthValueWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                argPositions,
                configuration,
                sqlExecutionContext,
                supportNullsDesc(),
                CurrentRowTimestamp::new,
                PartitionTimestamp::new,
                UnboundedPartitionTimestamp::new,
                PartitionRangeTimestamp::new,
                PartitionRowsUnboundedTimestamp::new,
                PartitionRowsTimestamp::new,
                WholeResultSetTimestamp::new,
                UnboundedRowsTimestamp::new,
                RangeTimestamp::new,
                RowsUnboundedTimestamp::new,
                RowsTimestamp::new
        );
    }

    static final class CurrentRowTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverCurrentRowBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        CurrentRowTimestamp(Function arg, int n) {
            super(arg, n);
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

    static final class PartitionRangeTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRangeFrameBase implements WindowTimestampFunction {
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
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, initialBufferSize, timestampIdx, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRowsTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRowsUnboundedTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRowsFrameUnboundedBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRowsUnboundedTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, rowsHi, arg, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionBase implements WindowTimestampFunction {

        PartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n) {
            super(map, partitionByRecord, partitionBySink, arg, n);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RangeTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RangeTimestamp(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RowsTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RowsTimestamp(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n) {
            super(arg, rowsLo, rowsHi, memory, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RowsUnboundedTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverRowsFrameUnboundedBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RowsUnboundedTimestamp(Function arg, long rowsHi, int n) {
            super(arg, rowsHi, n);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(nthValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return nthValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class UnboundedPartitionTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverUnboundedPartitionFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        UnboundedPartitionTimestamp(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange) {
            super(map, partitionByRecord, partitionBySink, arg, n, isRange);
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

    static final class UnboundedRowsTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverUnboundedRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        UnboundedRowsTimestamp(Function arg, int n) {
            super(arg, n);
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

    static final class WholeResultSetTimestamp extends NthValueWindowFunctionFactoryHelper.NthValueOverWholeResultSetBase implements WindowTimestampFunction {

        WholeResultSetTimestamp(Function arg, int n) {
            super(arg, n);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }
}
