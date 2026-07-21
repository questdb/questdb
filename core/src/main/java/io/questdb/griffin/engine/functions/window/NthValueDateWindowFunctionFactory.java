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
 * nth_value() over a DATE argument. Registers the {@code nth_value(ML)} signature and supplies the thin
 * DATE subclasses of the shared {@link NthValueWindowFunctionFactoryHelper} bases. The bases store and
 * write the value in its native unit (DATE milliseconds here), so each value subclass only exposes the
 * stored value through {@code getDate()}; {@link WindowDateFunction} derives the rest. Two-pass shapes
 * need no accessor at all.
 */
public class NthValueDateWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = NthValueWindowFunctionFactoryHelper.NAME + "(ML)";

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
                CurrentRowDate::new,
                PartitionDate::new,
                UnboundedPartitionDate::new,
                PartitionRangeDate::new,
                PartitionRowsUnboundedDate::new,
                PartitionRowsDate::new,
                WholeResultSetDate::new,
                UnboundedRowsDate::new,
                RangeDate::new,
                RowsUnboundedDate::new,
                RowsDate::new
        );
    }

    static final class CurrentRowDate extends NthValueWindowFunctionFactoryHelper.NthValueOverCurrentRowBase implements WindowDateFunction {

        CurrentRowDate(Function arg, int n) {
            super(arg, n);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class PartitionDate extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionBase implements WindowDateFunction {

        PartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n) {
            super(map, partitionByRecord, partitionBySink, arg, n);
        }
    }

    static final class PartitionRangeDate extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRangeFrameBase implements WindowDateFunction {

        PartitionRangeDate(
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
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class PartitionRowsDate extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRowsFrameBase implements WindowDateFunction {

        PartitionRowsDate(
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
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class PartitionRowsUnboundedDate extends NthValueWindowFunctionFactoryHelper.NthValueOverPartitionRowsFrameUnboundedBase implements WindowDateFunction {

        PartitionRowsUnboundedDate(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsHi,
                Function arg,
                int n
        ) {
            super(map, partitionByRecord, partitionBySink, rowsHi, arg, n);
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class RangeDate extends NthValueWindowFunctionFactoryHelper.NthValueOverRangeFrameBase implements WindowDateFunction {

        RangeDate(
                long rangeLo,
                long rangeHi,
                Function arg,
                CairoConfiguration configuration,
                int timestampIdx,
                int n
        ) {
            super(rangeLo, rangeHi, arg, configuration, timestampIdx, n);
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class RowsDate extends NthValueWindowFunctionFactoryHelper.NthValueOverRowsFrameBase implements WindowDateFunction {

        RowsDate(Function arg, long rowsLo, long rowsHi, MemoryARW memory, int n) {
            super(arg, rowsLo, rowsHi, memory, n);
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class RowsUnboundedDate extends NthValueWindowFunctionFactoryHelper.NthValueOverRowsFrameUnboundedBase implements WindowDateFunction {

        RowsUnboundedDate(Function arg, long rowsHi, int n) {
            super(arg, rowsHi, n);
        }

        @Override
        public long getDate(Record rec) {
            return nthValue;
        }
    }

    static final class UnboundedPartitionDate extends NthValueWindowFunctionFactoryHelper.NthValueOverUnboundedPartitionFrameBase implements WindowDateFunction {

        UnboundedPartitionDate(Map map, VirtualRecord partitionByRecord, RecordSink partitionBySink, Function arg, int n, boolean isRange) {
            super(map, partitionByRecord, partitionBySink, arg, n, isRange);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class UnboundedRowsDate extends NthValueWindowFunctionFactoryHelper.NthValueOverUnboundedRowsFrameBase implements WindowDateFunction {

        UnboundedRowsDate(Function arg, int n) {
            super(arg, n);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    static final class WholeResultSetDate extends NthValueWindowFunctionFactoryHelper.NthValueOverWholeResultSetBase implements WindowDateFunction {

        WholeResultSetDate(Function arg, int n) {
            super(arg, n);
        }
    }
}
