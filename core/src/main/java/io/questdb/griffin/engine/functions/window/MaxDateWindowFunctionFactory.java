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
import io.questdb.griffin.engine.functions.window.MaxMinWindowFunctionFactoryHelper.TimestampComparator;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/**
 * max() over a DATE argument. Registers the {@code max(M)} signature and supplies the thin DATE
 * subclasses of the shared {@link MaxMinWindowFunctionFactoryHelper} bases. The bases store and
 * write the value in its native unit (DATE milliseconds here), so each DATE subclass only exposes
 * the stored value through {@code getDate()}; {@link WindowDateFunction} derives the rest. {@code min}
 * reuses these subclasses with a {@code LESS_THAN} comparator.
 */
public class MaxDateWindowFunctionFactory extends AbstractWindowFunctionFactory {
    private static final String SIGNATURE = MaxTimestampWindowFunctionFactory.NAME + "(M)";

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
                MaxTimestampWindowFunctionFactory.GREATER_THAN,
                MaxTimestampWindowFunctionFactory.NAME,
                CurrentRowDate::new,
                PartitionDate::new,
                PartitionRangeDate::new,
                PartitionRowsDate::new,
                UnboundedPartitionRowsDate::new,
                RangeDate::new,
                RowsDate::new,
                UnboundedRowsDate::new,
                WholeResultSetDate::new
        );
    }

    static final class CurrentRowDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverCurrentRowBase implements WindowDateFunction {

        CurrentRowDate(Function arg, String name) {
            super(arg, name);
        }

        @Override
        public long getDate(Record rec) {
            return value;
        }
    }

    // handles max() over (partition by x)
    // order by is absent so default frame mode includes all rows in partition
    static final class PartitionDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionBase implements WindowDateFunction {

        PartitionDate(Map map,
                      VirtualRecord partitionByRecord,
                      RecordSink partitionBySink,
                      Function arg,
                      TimestampComparator comparator,
                      String name) {
            super(map, partitionByRecord, partitionBySink, arg, comparator, name);
        }
    }

    static final class PartitionRangeDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionRangeFrameBase implements WindowDateFunction {

        PartitionRangeDate(
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
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, rangeLo, rangeHi, arg, memory, dequeMemory, initialBufferSize, timestampIdx, comparator, name);
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    static final class PartitionRowsDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverPartitionRowsFrameBase implements WindowDateFunction {

        PartitionRowsDate(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                long rowsLo,
                long rowsHi,
                Function arg,
                MemoryARW memory,
                MemoryARW dequeMemory,
                TimestampComparator comparator,
                String name
        ) {
            super(map, partitionByRecord, partitionBySink, rowsLo, rowsHi, arg, memory, dequeMemory, comparator, name);
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    static final class RangeDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverRangeFrameBase implements WindowDateFunction {

        RangeDate(
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
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    static final class RowsDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverRowsFrameBase implements WindowDateFunction {

        RowsDate(Function arg,
                 long rowsLo,
                 long rowsHi,
                 MemoryARW memory,
                 MemoryARW dequeMemory,
                 TimestampComparator comparator,
                 String name) {
            super(arg, rowsLo, rowsHi, memory, dequeMemory, comparator, name);
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    static final class UnboundedPartitionRowsDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverUnboundedPartitionRowsFrameBase implements WindowDateFunction {

        UnboundedPartitionRowsDate(Map map,
                                   VirtualRecord partitionByRecord,
                                   RecordSink partitionBySink,
                                   Function arg,
                                   TimestampComparator comparator,
                                   String name) {
            super(map, partitionByRecord, partitionBySink, arg, comparator, name);
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    static final class UnboundedRowsDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverUnboundedRowsFrameBase implements WindowDateFunction {

        UnboundedRowsDate(Function arg, TimestampComparator comparator, String name) {
            super(arg, comparator, name);
        }

        @Override
        public long getDate(Record rec) {
            return maxMin;
        }
    }

    // max() over () - empty clause, no partition by no order by, no frame == default frame
    static final class WholeResultSetDate extends MaxMinWindowFunctionFactoryHelper.MaxMinOverWholeResultSetBase implements WindowDateFunction {

        WholeResultSetDate(Function arg, TimestampComparator comparator, String name) {
            super(arg, comparator, name);
        }
    }
}
