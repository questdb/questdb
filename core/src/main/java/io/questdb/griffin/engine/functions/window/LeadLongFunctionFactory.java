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
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
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

public class LeadLongFunctionFactory extends AbstractWindowFunctionFactory {

    private static final String SIGNATURE = LeadLagWindowFunctionFactoryHelper.LEAD_NAME + "(LV)";

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
        // When the streaming-lead session flag is on and the call shape fits the Phase 3 streaming
        // path (no PARTITION BY, no IGNORE NULLS, constant offset > 0, constant or absent default),
        // construct StreamingLeadFunction directly so it reports ZERO_PASS + positive lookahead to
        // the planner. Otherwise fall through to the cached helper path.
        Function streaming = tryNewStreamingInstance(
                args,
                argPositions,
                configuration,
                sqlExecutionContext
        );
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
                LeadFunction::new,
                LagLongFunctionFactory.LeadLagValueCurrentRow::new,
                LeadOverPartitionFunction::new
        );
    }

    /**
     * Returns a {@link StreamingLeadFunction} if the call shape qualifies for Phase 3 deferred-emit
     * streaming, otherwise {@code null} to indicate the caller should fall through to the cached
     * helper path. Validates only the constraints relevant to the streaming decision; the helper
     * re-validates everything when it constructs the cached fallback.
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
        // Cursor's filled-bit mask is a single long, so single-function ringCap = offset + 1 must
        // fit in 64 bits, i.e. offset <= 63.
        if (offset > 63) {
            return null;
        }

        Function defaultValue = null;
        if (args.size() == 3) {
            Function dv = args.getQuick(2);
            if (dv instanceof WindowFunction) {
                throw SqlException.$(argPositions.getQuick(2), "default value can not be a window function");
            }
            if (!dv.isConstant()) {
                // Streaming flush evaluates defaultValue without a current base record; require constant.
                return null;
            }
            if (!ColumnType.isSameOrBuiltInWideningCast(dv.getType(), ColumnType.LONG)) {
                throw SqlException.$(argPositions.getQuick(2), "default value cannot be cast to long");
            }
            defaultValue = dv;
        }

        if (wc.getPartitionByRecord() != null) {
            // Partition mode: pass configuration + key types to the streaming variant. The cached-
            // fallback Map and MemoryARW are NOT allocated up front; the streaming variant's pass1
            // override allocates them on first invocation, which only happens if the planner ends
            // up routing this query through CachedWindow.
            return new StreamingLeadOverPartitionFunction(
                    configuration,
                    wc.getPartitionByKeyTypes(),
                    wc.getPartitionByRecord(),
                    wc.getPartitionBySink(),
                    args.get(0), defaultValue, offset
            );
        }

        // No partition by: simple ring buffer variant. Same lazy-allocation pattern — buffer is
        // populated by the streaming variant's pass1 override on first cached-path call.
        return new StreamingLeadFunction(configuration, args.get(0), defaultValue, offset);
    }

    static class LeadFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadFunction implements Reopenable, WindowLongFunction {
        public LeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg, defaultValueFunc, offset, memory, ignoreNulls);
        }

        @Override
        protected boolean doPass1(Record record, long recordOffset, WindowSPI spi) {
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(record);
            } else {
                leadValue = buffer.getLong((long) loIdx * Long.BYTES);
            }
            long l = arg.getLong(record);
            boolean respectNull = !ignoreNulls || l != Numbers.LONG_NULL;
            if (respectNull) {
                buffer.putLong((long) loIdx * Long.BYTES, l);
            }
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), leadValue);
            return respectNull;
        }
    }

    static class LeadOverPartitionFunction extends LeadLagWindowFunctionFactoryHelper.BaseLeadOverPartitionFunction implements WindowLongFunction {
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
            long l = arg.getLong(record);
            long leadValue;
            if (count < offset) {
                leadValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(record);
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

    /**
     * Streaming variant of LEAD that participates in the deferred-emit fast path.
     * <p>
     * Reports {@link WindowFunction#ZERO_PASS} and a positive {@link #getLookahead()} so the planner
     * can choose {@code DeferredEmitWindowRecordCursorFactory} when the rest of the constraints are
     * satisfied. Inherits the cached {@code pass1} implementation from {@link LeadFunction} so that
     * if the planner subsequently picks the cached executor (because some other function in the
     * same query is not ZERO_PASS) the cached fallback still produces correct results.
     * <p>
     * Construction is only valid when {@code ignoreNulls == false} and {@code defaultValue} is either
     * {@code null} or a constant. Phase 3 does not support IGNORE NULLS streaming or row-dependent
     * default expressions.
     */
    static final class StreamingLeadFunction extends LeadFunction {
        private final CairoConfiguration configuration;
        // Resolved in init() after super.init() runs defaultValue.init(); the planner restricts
        // streaming dispatch to constants today, but resolving post-init keeps the contract robust
        // if the constant filter is ever widened.
        private long defaultLongValue;

        StreamingLeadFunction(CairoConfiguration configuration, Function arg, Function defaultValueFunc, long offset) {
            super(arg, defaultValueFunc, offset, null, false);
            this.configuration = configuration;
            this.defaultLongValue = Numbers.LONG_NULL;
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
            this.defaultLongValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(null);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (buffer == null) {
                // Cached path won: lazy-allocate the ring buffer on first invocation. Streaming
                // dispatches never call pass1, so this stays null when the streaming cursor wins.
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
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getLong(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultLongValue);
        }
    }

    /**
     * Partitioned streaming variant of LEAD. Extends the cached {@link LeadOverPartitionFunction} so
     * the cached fallback still works correctly; the streaming protocol methods write directly into
     * the cursor-owned slot via the supplied {@link WindowSPI}.
     */
    static final class StreamingLeadOverPartitionFunction extends LeadOverPartitionFunction {
        private final CairoConfiguration configuration;
        private final ColumnTypes keyTypes;
        // Resolved in init() after super.init() runs defaultValue.init(); see StreamingLeadFunction.
        private long defaultLongValue;

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
            this.defaultLongValue = Numbers.LONG_NULL;
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
            this.defaultLongValue = defaultValue == null ? Numbers.LONG_NULL : defaultValue.getLong(null);
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (map == null) {
                // Cached path won: lazy-allocate the map and ring buffer on first invocation.
                // Streaming dispatches never call pass1, so these stay null when streaming wins.
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
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), arg.getLong(source));
        }

        @Override
        public void streamingFlushDefault(long pendingSlot, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(pendingSlot, columnIndex), defaultLongValue);
        }
    }
}
