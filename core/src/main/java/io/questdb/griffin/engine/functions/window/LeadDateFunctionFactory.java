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

public class LeadDateFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LEAD_NAME + "(MV)";

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
                    if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), ColumnType.DATE)) {
                        throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to date");
                    }
                },
                LeadFunction::new,
                LagLongFunctionFactory.LeadLagValueCurrentRow::new,
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
            if (!ColumnType.isSameOrBuiltInWideningCast(dv.getType(), ColumnType.DATE)) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to date");
            }
            defaultValue = dv;
        }

        if (wc.getPartitionByRecord() != null) {
            Map cachedMap = null;
            MemoryARW cachedMem = null;
            try {
                cachedMap = MapFactory.createUnorderedMap(
                        configuration,
                        wc.getPartitionByKeyTypes(),
                        LeadLagWindowFunctionFactoryHelper.LAG_COLUMN_TYPES
                );
                cachedMem = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
                return new StreamingLeadOverPartitionFunction(
                        cachedMap, wc.getPartitionByRecord(), wc.getPartitionBySink(), cachedMem,
                        args.get(0), defaultValue, offset
                );
            } catch (Throwable th) {
                Misc.free(cachedMap);
                Misc.free(cachedMem);
                throw th;
            }
        }

        MemoryARW mem = null;
        try {
            mem = Vm.getCARWInstance(
                    configuration.getSqlWindowStorePageSize(),
                    configuration.getSqlWindowStoreMaxPages(),
                    MemoryTag.NATIVE_CIRCULAR_BUFFER
            );
            return new StreamingLeadFunction(args.get(0), defaultValue, offset, mem);
        } catch (Throwable th) {
            Misc.free(mem);
            throw th;
        }
    }

    static final class StreamingLeadFunction extends LeadFunction {
        private final long defaultDateValue;

        StreamingLeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory) {
            super(arg, defaultValueFunc, offset, memory, false);
            this.defaultDateValue = defaultValueFunc == null ? Numbers.LONG_NULL : defaultValueFunc.getDate(null);
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
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getDate(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultDateValue);
        }
    }

    static final class StreamingLeadOverPartitionFunction extends LeadOverPartitionFunction {
        private final long defaultDateValue;

        StreamingLeadOverPartitionFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                MemoryARW memory,
                Function arg,
                Function defaultValue,
                long offset
        ) {
            super(map, partitionByRecord, partitionBySink, memory, arg, false, defaultValue, offset);
            this.defaultDateValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getDate(null);
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
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getDate(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultDateValue);
        }
    }

    static class LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowDateFunction {
        public LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getDate(record);
            } else {
                leadValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getDate(record);
            boolean respectNull = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNull) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowDateFunction {
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
            long l = arg.getDate(record);
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getDate(record);
            } else {
                leadValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            boolean respectNulls = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNulls) {
                memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }
}
