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
import io.questdb.cairo.TimestampDriver;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.SymbolTableSource;
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
                    if (!ColumnType.isSameTagOrBuiltInWideningCast(defaultValue.getType(), args.getQuick(0).getType())) {
                        throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to timestamp");
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
            if (!ColumnType.isSameTagOrBuiltInWideningCast(dv.getType(), args.getQuick(0).getType())) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to timestamp");
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

    private static long resolveTimestampDefault(Function arg, Function defaultValueFunc) {
        if (defaultValueFunc == null) {
            return Numbers.LONG_NULL;
        }
        int argType = arg.getType();
        int defaultType = ColumnType.getTimestampType(defaultValueFunc.getType());
        TimestampDriver driver = ColumnType.getTimestampDriver(ColumnType.getTimestampType(argType));
        return driver.from(defaultValueFunc.getTimestamp(null), defaultType);
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
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
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNulls;
        }
    }

    static final class StreamingLeadFunction extends LeadFunction {
        private final CairoConfiguration configuration;
        // Resolved in init() after super.init() runs defaultValue.init(); see LeadLongFunctionFactory.
        private long defaultTimestampValue;

        StreamingLeadFunction(CairoConfiguration configuration, Function arg, Function defaultValueFunc, long offset) {
            super(arg, defaultValueFunc, offset, null, false);
            this.configuration = configuration;
            this.defaultTimestampValue = Numbers.LONG_NULL;
        }

        @Override
        public void close() {
            super.close();
            buffer = null;
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.defaultTimestampValue = resolveTimestampDefault(arg, defaultValue);
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
        public void reset() {
            super.reset();
            buffer = null;
        }

        @Override
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getTimestamp(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultTimestampValue);
        }
    }

    static final class StreamingLeadOverPartitionFunction extends LeadOverPartitionFunction {
        private final CairoConfiguration configuration;
        private final ColumnTypes keyTypes;
        // Resolved in init() after super.init() runs defaultValue.init(); see LeadLongFunctionFactory.
        private long defaultTimestampValue;

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
            this.defaultTimestampValue = Numbers.LONG_NULL;
        }

        @Override
        public void close() {
            super.close();
            map = null;
            memory = null;
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.defaultTimestampValue = resolveTimestampDefault(arg, defaultValue);
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
        public void reset() {
            super.reset();
            map = null;
            memory = null;
        }

        @Override
        public void streamingBackfill(Record source, long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getTimestamp(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultTimestampValue);
        }
    }
}
