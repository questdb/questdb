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

public class LagLongFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LAG_NAME + "(LV)";

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
                    if (!ColumnType.isSameOrBuiltInWideningCast(defaultValue.getType(), ColumnType.LONG)) {
                        throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to long");
                    }
                },
                LagFunction::new,
                LeadLagValueCurrentRow::new,
                LagOverPartitionFunction::new
        );
    }

    /**
     * Returns a {@link StreamingLagOverPartitionFunction} when the streaming-lead session flag is
     * on AND the call has a PARTITION BY clause. Non-partitioned LAG stays on the cached path:
     * the cursor's MapValue is per-partition and there is nothing to share when no partition
     * exists.
     */
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
        if (wc.isEmpty()) {
            return null;
        }
        if (wc.isIgnoreNulls()) {
            return null;
        }
        if (wc.getPartitionByRecord() == null) {
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
            if (!ColumnType.isSameOrBuiltInWideningCast(dv.getType(), ColumnType.LONG)) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to long");
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

    public static class LagFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagFunction implements WindowLongFunction {

        private long lagValue;

        public LagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
        }

        @Override
        public boolean computeNext0(Record record) {
            if (count < offset) {
                lagValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(record);
            } else {
                lagValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getLong(record);
            boolean respectNulls = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNulls) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            return respectNulls;
        }

        @Override
        public long getLong(Record rec) {
            return lagValue;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }
    }

    static class LagOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLagOverPartitionFunction implements WindowLongFunction {

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
        }

        @Override
        public long getLong(Record rec) {
            return lagValue;
        }


        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lagValue);
        }

        @Override
        protected boolean computeNext0(long count, long offset, long startOffset, long firstIdx, Record record) {
            long l = arg.getLong(record);
            if (count < offset) {
                lagValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(record);
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

    /**
     * Partitioned LAG variant that participates in streaming dispatch. The deferred-emit cursor
     * invokes {@link #streamingPass1(Record, long, WindowSPI, long)} with a non-zero
     * partitionStateAddr pointing at three contiguous LONG slots in the cursor's MapValue for
     * (startOffset, firstIdx, count); the function reads and writes those slots directly, skipping
     * the second hash probe per row that the cached LAG would do via {@code map.withKey()}.
     * <p>
     * When the planner falls back to CachedWindow, {@link #pass1(Record, long, WindowSPI)} lazy-
     * allocates the inherited {@link LagOverPartitionFunction}'s map and ring memory and delegates
     * to the cached logic.
     */
    static final class StreamingLagOverPartitionFunction extends LagOverPartitionFunction {
        private final CairoConfiguration configuration;
        private final long defaultLongValue;
        private final ColumnTypes keyTypes;

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
            this.defaultLongValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(null);
        }

        @Override
        public void computeNext(Record record) {
            // Cached path entry (WindowRecordCursorFactory invokes computeNext directly; pass1 is
            // only the CachedWindowRecordCursorFactory entry). Lazy-allocate the map and ring
            // memory on first invocation, then defer to the inherited cached logic. Streaming
            // dispatch never reaches this method because the cursor uses streamingPass1.
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
        public void streamingPass1(Record record, long recordOffset, WindowSPI spi, long partitionStateAddr) {
            if (partitionStateAddr == 0L) {
                // Streaming cursor exposed no co-located state (non-partitioned, or cursor cannot
                // share). Fall back to cached pass1; lazy alloc kicks in via computeNext.
                pass1(record, recordOffset, spi);
                return;
            }

            // Lazy-allocate only the ring memory in streaming context (no map needed; the cursor
            // owns partition resolution).
            if (memory == null) {
                memory = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
            }

            // Layout at partitionStateAddr: [0]=startOffset, [8]=firstIdx, [16]=count.
            long startOffset = Unsafe.getUnsafe().getLong(partitionStateAddr);
            long firstIdx = Unsafe.getUnsafe().getLong(partitionStateAddr + Long.BYTES);
            long count = Unsafe.getUnsafe().getLong(partitionStateAddr + 2L * Long.BYTES);

            // First touch of this partition: allocate its slice in the ring memory and record the
            // returned page-relative offset.
            if (count == 0L && startOffset == 0L) {
                startOffset = memory.appendAddressFor(offset * Long.BYTES) - memory.getPageAddress(0);
            }

            long lagValue;
            if (count < offset) {
                lagValue = defaultLongValue;
            } else {
                lagValue = memory.getLong(startOffset + firstIdx * Long.BYTES);
            }
            long l = arg.getLong(record);
            // ignoreNulls is always false in streaming dispatch (rejected by tryNewStreamingInstance);
            // the buffer always advances and respect-NULL semantics fall out automatically.
            memory.putLong(startOffset + firstIdx * Long.BYTES, l);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), lagValue);

            firstIdx = (firstIdx + 1) % offset;
            count++;

            Unsafe.getUnsafe().putLong(partitionStateAddr, startOffset);
            Unsafe.getUnsafe().putLong(partitionStateAddr + Long.BYTES, firstIdx);
            Unsafe.getUnsafe().putLong(partitionStateAddr + 2L * Long.BYTES, count);
        }
    }

    static class LeadLagValueCurrentRow extends LeadLagWindowFunctionFactoryHelper.BaseLeadLagCurrentRow implements WindowLongFunction {

        private long value;

        public LeadLagValueCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls) {
            super(partitionByRecord, arg, name, ignoreNulls);
        }

        @Override
        public void computeNext(Record record) {
            value = arg.getLong(record);
        }

        @Override
        public long getLong(Record rec) {
            return value;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), value);
        }
    }
}
