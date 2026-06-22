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
import io.questdb.griffin.engine.functions.window.MaxMinWindowFunctionFactoryHelper.TimestampComparator;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * max() over a TIMESTAMP argument. Registers the {@code max(N)} signature and supplies the thin
 * TIMESTAMP subclasses of the shared {@link MaxMinWindowFunctionFactoryHelper} bases. Each subclass
 * caches the timestamp driver once and exposes {@code getTimestamp()} as the stored value plus a
 * {@code getDate()} that converts ticks to DATE milliseconds. {@code min} reuses these subclasses
 * with a {@code LESS_THAN} comparator.
 */
public class MaxTimestampWindowFunctionFactory extends AbstractWindowFunctionFactory {

    public static final TimestampComparator GREATER_THAN = (a, b) -> a > b;
    public static final String NAME = "max";
    private static final String SIGNATURE = NAME + "(N)";

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
        return MaxMinWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                configuration,
                sqlExecutionContext,
                supportNullsDesc(),
                GREATER_THAN,
                NAME,
                CurrentRowTimestamp::new,
                PartitionTimestamp::new,
                PartitionRangeTimestamp::new,
                PartitionRowsTimestamp::new,
                UnboundedPartitionRowsTimestamp::new,
                RangeTimestamp::new,
                RowsTimestamp::new,
                UnboundedRowsTimestamp::new,
                WholeResultSetTimestamp::new
        );
    }

    static final class CurrentRowTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverCurrentRowBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        CurrentRowTimestamp(Function arg, String name) {
            super(arg, name);
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

    static final class PartitionRangeTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRangeTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rangeLo,
                long rangeHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int initialBufferSize,
                int timestampIdx,
                TimestampComparator comparator,
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, dequeMemory, initialBufferSize, timestampIdx, comparator, name, partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class PartitionRowsTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        PartitionRowsTimestamp(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                TimestampComparator comparator,
                String name,
                ColumnTypes partitionByKeyTypes,
                boolean liveView
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, dequeMemory, comparator, name, partitionByKeyTypes, liveView);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static final class PartitionTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionBase implements WindowTimestampFunction {

        PartitionTimestamp(Map map,
                           VirtualRecord partitionByRecord,
                           RecordSink partitionBySink,
                           Function arg,
                           TimestampComparator comparator,
                           String name) {
            super(map, partitionByRecord, partitionBySink, arg, comparator, name);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RangeTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverRangeFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RangeTimestamp(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                MemoryARW memory,
                MemoryARW dequeMemory,
                int timestampIdx,
                TimestampComparator comparator,
                String name
        ) {
            super(rangeLo, rangeHi, arg, configuration, memory, dequeMemory, timestampIdx, comparator, name);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class RowsTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        RowsTimestamp(Function arg,
                      long rowsLo,
                      long rowsHi,
                      MemoryARW memory,
                      MemoryARW dequeMemory,
                      TimestampComparator comparator,
                      String name) {
            super(arg, rowsLo, rowsHi, memory, dequeMemory, comparator, name);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class UnboundedPartitionRowsTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverUnboundedPartitionRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        UnboundedPartitionRowsTimestamp(Map map,
                                        VirtualRecord partitionByRecord,
                                        RecordSink partitionBySink,
                                        Function arg,
                                        TimestampComparator comparator,
                                        String name,
                                        ColumnTypes partitionByKeyTypes,
                                        boolean liveView,
                                        CairoConfiguration configuration) {
            super(map, partitionByRecord, partitionBySink, arg, comparator, name, partitionByKeyTypes, liveView, configuration);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    static final class UnboundedRowsTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverUnboundedRowsFrameBase implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;

        UnboundedRowsTimestamp(Function arg, TimestampComparator comparator, String name) {
            super(arg, comparator, name);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(maxMin);
        }

        @Override
        public long getTimestamp(Record rec) {
            return maxMin;
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    static final class WholeResultSetTimestamp extends MaxMinWindowFunctionFactoryHelper.MaxMinOverWholeResultSetBase implements WindowTimestampFunction {

        WholeResultSetTimestamp(Function arg, TimestampComparator comparator, String name) {
            super(arg, comparator, name);
        }

        @Override
        public int getType() {
            return arg.getType();
        }
    }
}
