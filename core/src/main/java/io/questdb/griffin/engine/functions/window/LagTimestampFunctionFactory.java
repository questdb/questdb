/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2024 QuestDB
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
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.std.IntList;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LagTimestampFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LAG_NAME + "(NV)";

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
        return LeadLagWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                argPositions,
                configuration,
                sqlExecutionContext,
                (defaultValue) -> {
                    if (!ColumnType.isAssignableFrom(defaultValue.getType(), args.getQuick(0).getType())) {
                        throw SqlException.$(argPositions.getQuick(2), "default value must be can cast to timestamp");
                    }
                },
                LagFunction::new,
                LeadLagValueCurrentRow::new,
                LagOverPartitionFunction::new
        );
    }

    public static class LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction implements WindowTimestampFunction {

        private final int defaultValueTimestampType;
        private final TimestampDriver timestampDriver;
        private long lagValue;

        public LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
            if (defaultValueFunc != null) {
                this.defaultValueTimestampType = ColumnType.getTimestampType(defaultValueFunc.getType());
            } else {
                this.defaultValueTimestampType = ColumnType.UNDEFINED;
            }
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Numbers.LONG_NULL : timestampDriver.from(defaultValue.getTimestamp(record), defaultValueTimestampType);
            } else {
                lagValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getTimestamp(record);
            boolean respectNulls = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNulls) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            return respectNulls;
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lagValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction implements WindowTimestampFunction {

        private final int defaultValueTimestampType;
        private final TimestampDriver timestampDriver;
        private long lagValue;

        public LagOverPartitionFunction(Map map,
                                        VirtualRecord partitionByRecord,
                                        RecordSink partitionBySink,
                                        MemoryARW memory,
                                        Function arg,
                                        boolean ignoreNulls,
                                        Function defaultValue,
                                        long offset) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
            if (defaultValue != null) {
                this.defaultValueTimestampType = ColumnType.getTimestampType(defaultValue.getType());
            } else {
                this.defaultValueTimestampType = ColumnType.UNDEFINED;
            }
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(lagValue);
        }

        @Override
        public long getTimestamp(Record rec) {
            return lagValue;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            long l = arg.getTimestamp(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Numbers.LONG_NULL : timestampDriver.from(defaultValue.getTimestamp(record), defaultValueTimestampType);
            } else {
                lagValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Numbers.LONG_NULL != l;
            if (respectNulls) {
                memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            }
            return respectNulls;
        }
    }

    static class LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow implements WindowTimestampFunction {
        private final TimestampDriver timestampDriver;
        private long value;

        public LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls) {
            super(partitionByRecord, arg, name, ignoreNulls);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getTimestamp(record);
        }

        @Override
        public long getDate(Record rec) {
            return timestampDriver.toDate(arg.getTimestamp(rec));
        }

        @Override
        public long getTimestamp(Record rec) {
            return value;
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }
}
