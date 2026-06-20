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
import io.questdb.cairo.RecordSink;
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
 * last_value() over a DATE argument. Registers the {@code last_value(M)} signature and supplies the thin
 * DATE subclasses of the shared {@link LastValueWindowFunctionFactoryHelper} bases. The bases store and
 * write the value in its native unit (DATE milliseconds here), so each value subclass only exposes the
 * stored value through {@code getDate()}; {@link WindowDateFunction} derives the rest. Shapes that write
 * the result column directly during a backward pass need no accessor at all.
 */
public class LastValueDateWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = LastValueWindowFunctionFactoryHelper.NAME + "(M)";

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
                PartitionDate::new,
                PartitionRangeDate::new,
                PartitionRowsDate::new,
                RangeDate::new,
                RowsDate::new,
                WholeResultSetDate::new,
                IncludeCurrentDate::new,
                IncludeCurrentPartitionRowsDate::new,
                NotNullPartitionDate::new,
                NotNullUnboundedPartitionRowsDate::new,
                NotNullPartitionRangeDate::new,
                NotNullPartitionRowsDate::new,
                NotNullCurrentRowDate::new,
                NotNullWholeResultSetDate::new,
                NotNullUnboundedRowsDate::new,
                NotNullRangeDate::new,
                NotNullRowsDate::new
        );
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    static final class IncludeCurrentDate extends LastValueWindowFunctionFactoryHelper.LastValueIncludeCurrentFrameBase implements WindowDateFunction {

        IncludeCurrentDate(long rowsLo, boolean isRange, Function arg) {
            super(rowsLo, isRange, arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class IncludeCurrentPartitionRowsDate extends LastValueWindowFunctionFactoryHelper.LastValueIncludeCurrentPartitionRowsFrameBase implements WindowDateFunction {

        IncludeCurrentPartitionRowsDate(long rowsLo, boolean isRange, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(rowsLo, isRange, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class NotNullCurrentRowDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverCurrentRowBase implements WindowDateFunction {

        NotNullCurrentRowDate(Function arg) {
            super(arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class NotNullPartitionDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionBase implements WindowDateFunction {

        NotNullPartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }
    }

    static final class NotNullPartitionRangeDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionRangeFrameBase implements WindowDateFunction {

        NotNullPartitionRangeDate(
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
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class NotNullPartitionRowsDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverPartitionRowsFrameBase implements WindowDateFunction {

        NotNullPartitionRowsDate(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class NotNullRangeDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverRangeFrameBase implements WindowDateFunction {

        NotNullRangeDate(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class NotNullRowsDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverRowsFrameBase implements WindowDateFunction {

        NotNullRowsDate(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class NotNullUnboundedPartitionRowsDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverUnboundedPartitionRowsFrameBase implements WindowDateFunction {

        NotNullUnboundedPartitionRowsDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class NotNullUnboundedRowsDate extends LastValueWindowFunctionFactoryHelper.LastNotNullOverUnboundedRowsFrameBase implements WindowDateFunction {

        NotNullUnboundedRowsDate(Function arg) {
            super(arg);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class NotNullWholeResultSetDate extends LastValueWindowFunctionFactoryHelper.LastNotNullValueOverWholeResultSetBase implements WindowDateFunction {

        NotNullWholeResultSetDate(Function arg) {
            super(arg);
        }
    }

    static final class PartitionDate extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionBase implements WindowDateFunction {

        PartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }
    }

    static final class PartitionRangeDate extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionRangeFrameBase implements WindowDateFunction {

        PartitionRangeDate(
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
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class PartitionRowsDate extends LastValueWindowFunctionFactoryHelper.LastValueOverPartitionRowsFrameBase implements WindowDateFunction {

        PartitionRowsDate(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class RangeDate extends LastValueWindowFunctionFactoryHelper.LastValueOverRangeFrameBase implements WindowDateFunction {

        RangeDate(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class RowsDate extends LastValueWindowFunctionFactoryHelper.LastValueOverRowsFrameBase implements WindowDateFunction {

        RowsDate(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public long getDate(Record rec) {
            return lastValue;
        }
    }

    static final class WholeResultSetDate extends LastValueWindowFunctionFactoryHelper.LastValueOverWholeResultSetBase implements WindowDateFunction {

        WholeResultSetDate(Function arg) {
            super(arg);
        }
    }
}
