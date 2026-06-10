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
 * first_value() over a DATE argument. Registers the {@code first_value(M)} signature and supplies the
 * thin DATE subclasses of the shared {@link FirstValueWindowFunctionFactoryHelper} bases. The bases store
 * and write the value in its native unit (DATE milliseconds here), so each value subclass only exposes the
 * stored value through {@code getDate()}; {@link WindowDateFunction} derives the rest. Two-pass shapes need
 * no accessor at all.
 */
public class FirstValueDateWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = FirstValueWindowFunctionFactoryHelper.NAME + "(M)";

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
                CurrentRowDate::new,
                PartitionDate::new,
                PartitionRangeDate::new,
                PartitionRowsDate::new,
                UnboundedPartitionRowsDate::new,
                RangeDate::new,
                RowsDate::new,
                WholeResultSetDate::new,
                NotNullPartitionDate::new,
                NotNullPartitionRangeDate::new,
                NotNullPartitionRowsDate::new,
                NotNullUnboundedPartitionRowsDate::new,
                NotNullRangeDate::new,
                NotNullRowsDate::new,
                NotNullWholeResultSetDate::new
        );
    }

    @Override
    protected boolean supportNullsDesc() {
        return true;
    }

    static final class CurrentRowDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverCurrentRowBase implements WindowDateFunction {

        CurrentRowDate(Function arg, boolean ignoreNulls) {
            super(arg, ignoreNulls);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class NotNullPartitionDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionBase implements WindowDateFunction {

        NotNullPartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }
    }

    static final class NotNullPartitionRangeDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionRangeFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class NotNullPartitionRowsDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverPartitionRowsFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class NotNullRangeDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverRangeFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class NotNullRowsDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverRowsFrameBase implements WindowDateFunction {

        NotNullRowsDate(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public long getDate(Record rec) {
            return firstValue;
        }
    }

    static final class NotNullUnboundedPartitionRowsDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverUnboundedPartitionRowsFrameBase implements WindowDateFunction {

        NotNullUnboundedPartitionRowsDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class NotNullWholeResultSetDate extends FirstValueWindowFunctionFactoryHelper.FirstNotNullValueOverWholeResultSetBase implements WindowDateFunction {

        NotNullWholeResultSetDate(Function arg) {
            super(arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class PartitionDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionBase implements WindowDateFunction {

        PartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public long getDate(Record rec) {
            return firstValue;
        }
    }

    static final class PartitionRangeDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionRangeFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class PartitionRowsDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverPartitionRowsFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class RangeDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverRangeFrameBase implements WindowDateFunction {

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
            return firstValue;
        }
    }

    static final class RowsDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverRowsFrameBase implements WindowDateFunction {

        RowsDate(Function arg, long rowsLo, long rowsHi, MemoryARW memory) {
            super(arg, rowsLo, rowsHi, memory);
        }

        @Override
        public long getDate(Record rec) {
            return firstValue;
        }
    }

    static final class UnboundedPartitionRowsDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverUnboundedPartitionRowsFrameBase implements WindowDateFunction {

        UnboundedPartitionRowsDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg) {
            super(map, partitionByRecord, partitionBySink, arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class WholeResultSetDate extends FirstValueWindowFunctionFactoryHelper.FirstValueOverWholeResultSetBase implements WindowDateFunction {

        WholeResultSetDate(Function arg) {
            super(arg);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }
}
