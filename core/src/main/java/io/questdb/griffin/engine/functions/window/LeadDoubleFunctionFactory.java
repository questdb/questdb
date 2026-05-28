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
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LeadDoubleFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LEAD_NAME + "(DV)";

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
        Function streaming = tryNewStreamingInstance(args, argPositions, configuration, sqlExecutionContext);
        if (streaming != null) {
            return streaming;
        }

        return LeadLagWindowFunctionFactoryHelper.newInstance(
                position,
                args,
                argPositions,
                configuration,
                sqlExecutionContext,
                (defaultValue) -> {
                    if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), ColumnType.DOUBLE)) {
                        throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to double");
                    }
                },
                LeadFunction::new,
                LagDoubleFunctionFactory.LeadLagValueCurrentRow::new,
                LeadOverPartitionFunction::new
        );
    }

    private static Function tryNewStreamingInstance(
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (!configuration.getSqlWindowStreamingLeadEnabled()) return null;
        final WindowContext wc = sqlExecutionContext.getWindowContext();
        if (wc.isEmpty()) return null;
        if (wc.isIgnoreNulls()) return null;
        if (args.size() > 3) return null;

        long offset = 1;
        if (args.size() >= 2) {
            Function offsetFunc = args.getQuick(1);
            if (!offsetFunc.isConstant()) return null;
            offset = offsetFunc.getLong(null);
            if (offset <= 0) return null;
        }
        if (offset > 63) return null;

        Function defaultValue = null;
        if (args.size() == 3) {
            Function dv = args.getQuick(2);
            if (dv instanceof WindowFunction) {
                throw SqlException.$(argPositions.getQuick(2), "default value can not be a window function");
            }
            if (!dv.isConstant()) return null;
            if (!ColumnType.isSameOrBuiltInWideningCast(dv.getType(), ColumnType.DOUBLE)) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to double");
            }
            defaultValue = dv;
        }

        if (wc.getPartitionByRecord() != null) {
            return new StreamingLeadOverPartitionFunction(
                    configuration,
                    wc.getPartitionByKeyTypes(),
                    wc.getPartitionByRecord(),
                    wc.getPartitionBySink(),
                    args.get(0), defaultValue, offset
            );
        }

        return new StreamingLeadFunction(configuration, args.get(0), defaultValue, offset);
    }

    static final class StreamingLeadFunction extends LeadFunction {
        private final CairoConfiguration configuration;
        private final double defaultDoubleValue;

        StreamingLeadFunction(CairoConfiguration configuration, Function arg, Function defaultValueFunc, long offset) {
            super(arg, defaultValueFunc, offset, null, false);
            this.configuration = configuration;
            this.defaultDoubleValue = defaultValueFunc == null ? Double.NaN : defaultValueFunc.getDouble(null);
        }

        @Override
        public int getLookahead() {
            return (int) offset;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (buffer == null) {
                buffer = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
            }
            super.pass1(record, recordOffset, spi);
        }

        @Override
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(pendingSlot, columnIndex), arg.getDouble(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(pendingSlot, columnIndex), defaultDoubleValue);
        }
    }

    static final class StreamingLeadOverPartitionFunction extends LeadOverPartitionFunction {
        private final CairoConfiguration configuration;
        private final double defaultDoubleValue;
        private final ColumnTypes keyTypes;

        StreamingLeadOverPartitionFunction(
                CairoConfiguration configuration,
                ColumnTypes keyTypes,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                Function arg,
                Function defaultValue,
                long offset
        ) {
            super(null, partitionByRecord, partitionBySink, null, arg, false, defaultValue, offset);
            this.configuration = configuration;
            this.keyTypes = keyTypes;
            this.defaultDoubleValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(null);
        }

        @Override
        public int getLookahead() {
            return (int) offset;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (map == null) {
                map = MapFactory.createUnorderedMap(
                        configuration,
                        keyTypes,
                        LeadLagWindowFunctionFactoryHelper.LAG_COLUMN_TYPES
                );
                memory = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
            }
            super.pass1(record, recordOffset, spi);
        }

        @Override
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(pendingSlot, columnIndex), arg.getDouble(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putDouble(spi.getAddress(pendingSlot, columnIndex), defaultDoubleValue);
        }
    }

    static class LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDoubleFunction {
        public LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            double leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                leadValue = buffer.getDouble((long) loIdx * Double.BYTES);
            }
            double d = arg.getDouble(record);
            boolean respectNull = !ignoreNulls || Numbers.isFinite(d);
            if (respectNull) {
                buffer.putDouble((long) loIdx * Double.BYTES, d);
            }
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDoubleFunction {
        public LeadOverPartitionFunction(Map map,
                                         VirtualRecord partitionByRecord,
                                         RecordSink partitionBySink,
                                         MemoryARW memory,
                                         Function arg,
                                         boolean ignoreNulls,
                                         Function defaultValue,
                                         long offset) {
            super(map, partitionByRecord, partitionBySink, memory, arg, ignoreNulls, defaultValue, offset);
        }

        @Override
        protected boolean doPass1(long count,
                                  long offset,
                                  long startOffset,
                                  long firstIdx,
                                  Record record,
                                  long recordOffset,
                                  WindowSPI spi) {
            double d = arg.getDouble(record);
            double leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                leadValue = memory.getDouble(startOffset + firstIdx * Double.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Numbers.isFinite(d);
            if (respectNulls) {
                memory.putDouble(startOffset + firstIdx * Double.BYTES, d);
            }
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }
}
