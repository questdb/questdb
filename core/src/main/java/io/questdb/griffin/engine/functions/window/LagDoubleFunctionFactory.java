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
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Numbers;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

public class LagDoubleFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LAG_NAME + "(DV)";

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
                LagFunction::new,
                LeadLagValueCurrentRow::new,
                LagOverPartitionFunction::new
        );
    }

    private static Function tryNewStreamingInstance(
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        if (!configuration.getSqlWindowStreamingLeadEnabled()) {
            return null;
        }
        final WindowContext wc = sqlExecutionContext.getWindowContext();
        if (wc.isEmpty() || wc.isIgnoreNulls() || wc.getPartitionByRecord() == null) {
            return null;
        }
        if (args.size() > 3) {
            return null;
        }

        long offset = 1;
        if (args.size() >= 2) {
            Function offsetFunc = args.getQuick(1);
            if (!offsetFunc.isConstant()) {
                return null;
            }
            offset = offsetFunc.getLong(null);
            if (offset <= 0) {
                return null;
            }
        }

        Function defaultValue = null;
        if (args.size() == 3) {
            Function dv = args.getQuick(2);
            if (dv instanceof io.questdb.griffin.engine.window.WindowFunction) {
                throw SqlException.$(argPositions.getQuick(2), "default value can not be a window function");
            }
            if (!dv.isConstant()) {
                return null;
            }
            if (!ColumnType.isSameOrBuiltInWideningCast(dv.getType(), ColumnType.DOUBLE)) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to double");
            }
            defaultValue = dv;
        }

        return new StreamingLagOverPartitionFunction(
                configuration,
                wc.getPartitionByKeyTypes(),
                wc.getPartitionByRecord(),
                wc.getPartitionBySink(),
                args.get(0), defaultValue, offset
        );
    }

    public static class LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction implements WindowDoubleFunction {

        private double lagValue;

        public LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                lagValue = buffer.getDouble((long) loIdx * Double.BYTES);
            }
            double d = arg.getDouble(record);
            boolean respectNulls = !ignoreNulls || Numbers.isFinite(d);
            if (respectNulls) {
                buffer.putDouble((long) loIdx * Double.BYTES, d);
            }
            return respectNulls;
        }

        @Override
        public double getDouble(Record rec) {
            return lagValue;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction implements WindowDoubleFunction {

        private double lagValue;

        public LagOverPartitionFunction(Map map,
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
        public double getDouble(Record rec) {
            return lagValue;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            double d = arg.getDouble(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(record);
            } else {
                lagValue = memory.getDouble(startOffset + firstIdx * Double.BYTES);
            }
            boolean respectNulls = !ignoreNulls || Numbers.isFinite(d);
            if (respectNulls) {
                memory.putDouble(startOffset + firstIdx * Double.BYTES, d);
            }
            return respectNulls;
        }
    }

    /**
     * Partitioned LAG variant that participates in streaming dispatch. See
     * {@code LagLongFunctionFactory.StreamingLagOverPartitionFunction} for the design rationale.
     */
    static final class StreamingLagOverPartitionFunction extends LagOverPartitionFunction {
        private final CairoConfiguration configuration;
        private final ColumnTypes keyTypes;
        // Resolved in init() after super.init() runs defaultValue.init(); see LagLongFunctionFactory.
        private double defaultDoubleValue;

        public StreamingLagOverPartitionFunction(
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
            this.defaultDoubleValue = Double.NaN;
        }

        @Override
        public void close() {
            super.close();
            map = null;
            memory = null;
        }

        @Override
        public void computeNext(Record record) {
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
            super.computeNext(record);
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            this.defaultDoubleValue = defaultValue == null ? Double.NaN : defaultValue.getDouble(null);
        }

        @Override
        public void reset() {
            super.reset();
            map = null;
            memory = null;
        }

        @Override
        public void streamingPass1(Record record, long recordOffset, WindowSPI spi, long partitionStateAddr) {
            if (partitionStateAddr == 0L) {
                pass1(record, recordOffset, spi);
                return;
            }

            if (memory == null) {
                memory = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
            }

            long startOffset = Unsafe.getUnsafe().getLong(partitionStateAddr);
            long firstIdx = Unsafe.getUnsafe().getLong(partitionStateAddr + Long.BYTES);
            long count = Unsafe.getUnsafe().getLong(partitionStateAddr + 2L * Long.BYTES);

            if (count == 0L && startOffset == 0L) {
                startOffset = memory.appendAddressFor(offset * Double.BYTES) - memory.getPageAddress(0);
            }

            double lagValue;
            if (count < offset) {
                lagValue = defaultDoubleValue;
            } else {
                lagValue = memory.getDouble(startOffset + firstIdx * Double.BYTES);
            }
            double d = arg.getDouble(record);
            memory.putDouble(startOffset + firstIdx * Double.BYTES, d);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), lagValue);

            firstIdx++;
            if (firstIdx == offset) {
                firstIdx = 0;
            }
            count++;

            Unsafe.getUnsafe().putLong(partitionStateAddr, startOffset);
            Unsafe.getUnsafe().putLong(partitionStateAddr + Long.BYTES, firstIdx);
            Unsafe.getUnsafe().putLong(partitionStateAddr + 2L * Long.BYTES, count);
        }
    }

    static class LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow implements WindowDoubleFunction {

        private double value;

        public LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls) {
            super(partitionByRecord, arg, name, ignoreNulls);
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getDouble(record);
        }

        @Override
        public double getDouble(Record rec) {
            return value;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putDouble(spi.getAddress(recordOffset, columnIndex), value);
        }
    }
}
