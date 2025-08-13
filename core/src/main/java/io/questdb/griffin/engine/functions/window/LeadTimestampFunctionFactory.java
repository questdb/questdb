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
import io.questdb.cairo.Reopenable;
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

public class LeadTimestampFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LEAD_NAME + "(NV)";

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
                LeadFunction::new,
                LagLongFunctionFactory.LeadLagValueCurrentRow::new,
                LeadOverPartitionFunction::new
        );
    }

    static class LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowTimestampFunction {
        private final int defaultValueTimestampType;
        private final TimestampDriver timestampDriver;

        public LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
            this.timestampDriver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(arg.getType()));
            if (defaultValueFunc != null) {
                this.defaultValueTimestampType = ColumnType.getTimestampType(defaultValueFunc.getType());
            } else {
                this.defaultValueTimestampType = ColumnType.UNDEFINED;
            }
        }

        @Override
        public int getType() {
            return arg.getType();
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : timestampDriver.from(defaultValue.getTimestamp(record), defaultValueTimestampType);
            } else {
                leadValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getTimestamp(record);
            boolean respectNull = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNull) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowTimestampFunction {
        private final int defaultValueTimestampType;
        private final TimestampDriver timestampDriver;

        public LeadOverPartitionFunction(Map map,
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
        public int getType() {
            return arg.getType();
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            long l = arg.getTimestamp(record);
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : timestampDriver.from(defaultValue.getTimestamp(record), defaultValueTimestampType);
            } else {
                leadValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNulls) {
                memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            }
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }
}
