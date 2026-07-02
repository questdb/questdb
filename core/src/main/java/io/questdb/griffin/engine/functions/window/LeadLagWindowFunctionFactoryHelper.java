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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

public class LeadLagWindowFunctionFactoryHelper {

    public static final ArrayColumnTypes LAG_COLUMN_TYPES;
    public static final ArrayColumnTypes LAG_COLUMN_TYPES_LV;
    public static final String LAG_NAME = "lag";
    public static final String LEAD_NAME = "lead";

    static Function newInstance(int position,
                                ObjList<Function> args,
                                IntList argPositions,
                                CairoConfiguration configuration,
                                SqlExecutionContext sqlExecutionContext,
                                DefaultValueExtraChecker defaultValueExtraChecker,
                                LagConstructor LagConstructor,
                                LagCurrentRowConstructor lagCurrentRowConstructor,
                                LagOverPartitionConstructor lagOverPartitionConstructor) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (args.size() > 3) {
            throw SqlException.$(argPositions.getQuick(3), "too many arguments");
        }

        long offset = 1;
        if (args.size() >= 2) {
            final Function offsetFunc = args.getQuick(1);
            if (!offsetFunc.isConstant()) {
                throw SqlException.$(argPositions.getQuick(1), "offset must be a constant");
            }

            offset = offsetFunc.getLong(null);
            if (offset < 0) {
                throw SqlException.$(argPositions.getQuick(1), "offset must be a positive integer");
            }
        }

        Function defaultValue = null;
        if (args.size() == 3) {
            defaultValue = args.getQuick(2);
            if (defaultValue instanceof WindowFunction) {
                throw SqlException.$(argPositions.getQuick(2), "default value can not be a window function");
            }

            defaultValueExtraChecker.check(defaultValue);
        }

        if (offset == 0) {
            return lagCurrentRowConstructor.newFunction(windowContext.getPartitionByRecord(), args.get(0),
                    LeadLagWindowFunctionFactoryHelper.LAG_NAME, windowContext.isIgnoreNulls());
        }

        if (windowContext.getPartitionByRecord() != null) {
            final boolean liveView = windowContext.isLiveView();
            final ColumnTypes partitionByKeyTypes = windowContext.getPartitionByKeyTypes();
            Map map = null;
            MemoryARW mem = null;
            try {
                map = MapFactory.createUnorderedMap(
                        configuration,
                        partitionByKeyTypes,
                        liveView ? LeadLagWindowFunctionFactoryHelper.LAG_COLUMN_TYPES_LV
                                : LeadLagWindowFunctionFactoryHelper.LAG_COLUMN_TYPES
                );
                mem = Vm.getCARWInstance(configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(), MemoryTag.NATIVE_CIRCULAR_BUFFER
                );

                return lagOverPartitionConstructor.newFunction(map,
                        windowContext.getPartitionByRecord(),
                        windowContext.getPartitionBySink(),
                        mem,
                        args.get(0),
                        windowContext.isIgnoreNulls(),
                        defaultValue,
                        offset,
                        partitionByKeyTypes,
                        liveView);
            } catch (Throwable th) {
                Misc.free(map);
                Misc.free(mem);
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
            return LagConstructor.newFunction(args.get(0), defaultValue, offset, mem, windowContext.isIgnoreNulls());
        } catch (Throwable th) {
            Misc.free(mem);
            throw th;
        }
    }

    @FunctionalInterface
    interface DefaultValueExtraChecker {
        void check(Function defaultValue) throws SqlException;
    }

    @FunctionalInterface
    interface LagConstructor {
        WindowFunction newFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls);
    }

    @FunctionalInterface
    interface LagCurrentRowConstructor {
        WindowFunction newFunction(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls);
    }

    @FunctionalInterface
    interface LagOverPartitionConstructor {
        WindowFunction newFunction(Map map,
                                   VirtualRecord partitionByRecord,
                                   RecordSink partitionBySink,
                                   MemoryARW memory,
                                   Function arg,
                                   boolean ignoreNulls,
                                   Function defaultValue,
                                   long offset,
                                   ColumnTypes partitionByKeyTypes,
                                   boolean liveView);
    }

    abstract static class BaseLagFunction extends BaseWindowFunction implements Reopenable {
        protected final MemoryARW buffer;
        protected final Function defaultValue;
        protected final boolean ignoreNulls;
        protected final long offset;
        protected long count = 0;
        protected int loIdx = 0;

        public BaseLagFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg);
            this.offset = offset;
            this.buffer = memory;
            this.defaultValue = defaultValueFunc;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
            Misc.free(defaultValue);
        }

        @Override
        public void computeNext(Record record) {
            if (computeNext0(record)) {
                loIdx = (int) ((loIdx + 1) % offset);
                count++;
            }
        }

        @Override
        public String getName() {
            return LAG_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void reopen() {
            loIdx = 0;
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(offset).val(", ");
            if (defaultValue != null) {
                sink.val(defaultValue);
            } else {
                sink.val("NULL");
            }
            sink.val(')');
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            loIdx = 0;
            count = 0;
        }

        protected abstract boolean computeNext0(Record record);
    }

    abstract static class BaseLagOverPartitionFunction extends BasePartitionedWindowFunction {
        // Ring slot width in bytes. All four typed lag variants (Double / Long /
        // Date / Timestamp) store 8-byte values; snapshot/restore treats the
        // ring as a raw byte blob and copies it slot-by-slot via memory.getLong /
        // sink.putLong regardless of the semantic type.
        private static final int RING_SLOT_BYTES = 8;
        protected final Function defaultValue;
        // Reclaim list of ring-slab startOffsets freed when a partition is
        // dropped. Each lag partition's ring is a fixed offset * RING_SLOT_BYTES
        // bytes, so capacity is implicit and only the startOffset is tracked. The
        // next computeNext that creates a partition prefers a reclaimed slab over
        // a fresh memory.appendAddressFor allocation. Cleared in reset / toTop.
        private final LongList freeList = new LongList();
        protected final boolean ignoreNulls;
        protected final MemoryARW memory;
        protected final long offset;
        // Deep copy of the partition-by key column types. The WindowContext
        // buffer the factory hands in gets cleared between compiles; the copy
        // outlives compilation so the snapshot codec can still describe the
        // Map's key shape.
        private final ArrayColumnTypes keyColumnTypes;
        private final boolean liveView;
        // Full value-layout (including the tombstone slot) describing the Map's
        // value shape. Null for non-live-view compiles.
        private final ArrayColumnTypes mapValueTypes;
        // Value-slot index of the per-partition tombstone byte. -1 for non-LV
        // compiles where the slot is omitted.
        // Count of partitions with the tombstone bit set. Single-writer
        // (refresh worker), not volatile.

        public BaseLagOverPartitionFunction(Map map,
                                            VirtualRecord partitionByRecord,
                                            RecordSink partitionBySink,
                                            MemoryARW memory,
                                            Function arg,
                                            boolean ignoreNulls,
                                            Function defaultValue,
                                            long offset,
                                            ColumnTypes partitionByKeyTypes,
                                            boolean liveView) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.memory = memory;
            this.ignoreNulls = ignoreNulls;
            this.liveView = liveView;
            if (liveView) {
                ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = partitionByKeyTypes.getColumnCount(); i < n; i++) {
                    keyTypesCopy.add(partitionByKeyTypes.getColumnType(i));
                }
                this.keyColumnTypes = keyTypesCopy;
                ArrayColumnTypes valueTypesCopy = new ArrayColumnTypes();
                for (int i = 0, n = LAG_COLUMN_TYPES_LV.getColumnCount(); i < n; i++) {
                    valueTypesCopy.add(LAG_COLUMN_TYPES_LV.getColumnType(i));
                }
                this.mapValueTypes = valueTypesCopy;
                this.tombstoneValueIndex = 3;
            } else {
                this.keyColumnTypes = null;
                this.mapValueTypes = null;
                this.tombstoneValueIndex = -1;
            }
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                final int freeN = freeList.size();
                if (freeN > 0) {
                    // Reuse a ring slab reclaimed from a tombstoned partition.
                    // Every lag ring is offset * RING_SLOT_BYTES bytes, so the
                    // capacity is implicit; only the startOffset matters.
                    startOffset = freeList.getQuick(freeN - 1);
                    freeList.setPos(freeN - 1);
                } else {
                    startOffset = memory.appendAddressFor(offset * RING_SLOT_BYTES) - memory.getPageAddress(0);
                }
                firstIdx = 0;
                // Live mode keeps a tombstone byte in the value slots; write it
                // explicitly so the two-pass snapshot walk sees the same byte both
                // times. Maps do not guarantee zeroed value bytes on createValue.
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            if (computeNext0(count, offset, startOffset, firstIdx, record)) {
                firstIdx++;
                count++;
            }
            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public String getName() {
            return LAG_NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : LAG_COLUMN_TYPES.getColumnCount();
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.initPartitionBy(symbolTableSource, executionContext);
            // The third arg of lag (defaultValue) can be a non-constant
            // function over base columns. Each incremental refresh hands the
            // function a fresh WAL-segment-scoped SymbolTableSource, so the
            // cached column / symbol bindings inside defaultValue must rebind
            // every cycle. The full init path runs once at first compile only.
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public boolean isIgnoreNulls() {
            return ignoreNulls;
        }

        @Override
        public void onSnapshotRestoreBegin() {
            super.onSnapshotRestoreBegin();
            memory.truncate();
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(memory);
            freeList.clear();
            tombstoneCount = 0;
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's lag ring to empty:
            // firstIdx=0, count=0 mean the function reports "no prior value yet"
            // until {@code offset} new rows have been observed. The ring buffer's
            // startOffset (slot 0) stays allocated and the next row's write
            // reuses it from index 0.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putLong(1, 0L); // firstIdx
                value.putLong(2, 0L); // count
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            final long ringBytes = this.offset * RING_SLOT_BYTES;
            final long firstIdx = source.getLong(offset);
            offset += Long.BYTES;
            final long count = source.getLong(offset);
            offset += Long.BYTES;
            final long newStartOffset = memory.appendAddressFor(ringBytes) - memory.getPageAddress(0);
            for (long i = 0; i < this.offset; i++) {
                memory.putLong(newStartOffset + i * RING_SLOT_BYTES, source.getLong(offset));
                offset += RING_SLOT_BYTES;
            }
            value.putLong(0, newStartOffset);
            value.putLong(1, firstIdx);
            value.putLong(2, count);
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public int snapshotFormatVersion() {
            return 1;
        }

        @Override
        public int snapshotMinSupportedVersion() {
            return 1;
        }

        @Override
        public void snapshotPartitionState(MemoryA sink, MapValue value) {
            sink.putLong(value.getLong(1)); // firstIdx
            sink.putLong(value.getLong(2)); // count
            final long startOffset = value.getLong(0);
            for (long i = 0; i < offset; i++) {
                sink.putLong(memory.getLong(startOffset + i * RING_SLOT_BYTES));
            }
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(offset).val(", ");
            if (defaultValue != null) {
                sink.val(defaultValue);
            } else {
                sink.val("NULL");
            }
            sink.val(')');
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
            freeList.clear();
            tombstoneCount = 0;
        }

        abstract protected boolean computeNext0(long count,
                                                long offset,
                                                long startOffset,
                                                long firstIdx,
                                                Record record);
    }

    static abstract class BaseLeadFunction extends BaseWindowFunction implements Reopenable {
        protected final MemoryARW buffer;
        protected final Function defaultValue;
        protected final boolean ignoreNulls;
        protected final long offset;
        protected long count = 0;
        protected int loIdx = 0;

        public BaseLeadFunction(Function arg, Function defaultValueFunc, long offset, MemoryARW memory, boolean ignoreNulls) {
            super(arg);
            this.offset = offset;
            this.buffer = memory;
            this.defaultValue = defaultValueFunc;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            buffer.close();
            Misc.free(defaultValue);
        }

        @Override
        public String getName() {
            return LEAD_NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (doPass1(record, recordOffset, spi)) {
                loIdx = (int) ((loIdx + 1) % offset);
                count++;
            }
        }

        @Override
        public void reopen() {
            loIdx = 0;
            count = 0;
        }

        @Override
        public void reset() {
            super.reset();
            buffer.close();
            loIdx = 0;
            count = 0;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(offset).val(", ");
            if (defaultValue != null) {
                sink.val(defaultValue);
            } else {
                sink.val("NULL");
            }
            sink.val(')');
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over ()");
        }

        @Override
        public void toTop() {
            super.toTop();
            loIdx = 0;
            count = 0;
        }

        protected abstract boolean doPass1(Record record, long recordOffset, WindowSPI spi);
    }

    abstract static class BaseLeadLagCurrentRow extends BaseWindowFunction {
        private final boolean ignoreNulls;
        private final String name;
        // keep it to call the partition function's close in the window function's close
        private final VirtualRecord partitionByRecord;

        public BaseLeadLagCurrentRow(VirtualRecord partitionByRecord, Function arg, String name, boolean ignoreNulls) {
            super(arg);
            this.partitionByRecord = partitionByRecord;
            this.name = name;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(0).val(", NULL)");
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over ()");
        }
    }

    abstract static class BaseLeadOverPartitionFunction extends BasePartitionedWindowFunction {
        protected final Function defaultValue;
        protected final boolean ignoreNulls;
        protected final MemoryARW memory;
        protected final long offset;

        public BaseLeadOverPartitionFunction(Map map,
                                             VirtualRecord partitionByRecord,
                                             RecordSink partitionBySink,
                                             MemoryARW memory,
                                             Function arg,
                                             boolean ignoreNulls,
                                             Function defaultValue,
                                             long offset) {
            super(map, partitionByRecord, partitionBySink, arg);
            this.defaultValue = defaultValue;
            this.offset = offset;
            this.memory = memory;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(memory);
            Misc.free(defaultValue);
        }

        @Override
        public String getName() {
            return LEAD_NAME;
        }

        @Override
        public Pass1ScanDirection getPass1ScanDirection() {
            return Pass1ScanDirection.BACKWARD;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (defaultValue != null) {
                defaultValue.init(symbolTableSource, executionContext);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long startOffset;
            long firstIdx;
            long count = 0;

            if (mapValue.isNew()) {
                startOffset = memory.appendAddressFor(offset * Double.BYTES) - memory.getPageAddress(0);
                firstIdx = 0;
            } else {
                startOffset = mapValue.getLong(0);
                firstIdx = mapValue.getLong(1);
                count = mapValue.getLong(2);
            }

            if (doPass1(count, offset, startOffset, firstIdx, record, recordOffset, spi)) {
                firstIdx++;
                count++;
            }

            mapValue.putLong(0, startOffset);
            mapValue.putLong(1, firstIdx % offset);
            mapValue.putLong(2, count);
        }

        @Override
        public void reset() {
            super.reset();
            Misc.free(memory);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val('(').val(arg).val(", ").val(offset).val(", ");
            if (defaultValue != null) {
                sink.val(defaultValue);
            } else {
                sink.val("NULL");
            }
            sink.val(')');
            if (ignoreNulls) {
                sink.val(" ignore nulls");
            }
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            memory.truncate();
        }

        protected abstract boolean doPass1(long count,
                                           long offset,
                                           long startOffset,
                                           long firstIdx,
                                           Record record,
                                           long recordOffset,
                                           WindowSPI spi);
    }

    static {
        LAG_COLUMN_TYPES = new ArrayColumnTypes();
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // start offset of partition's ring inside the shared MemoryARW
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // firstIdx (position of current oldest element in the ring)
        LAG_COLUMN_TYPES.add(ColumnType.LONG); // count of rows the partition has observed

        LAG_COLUMN_TYPES_LV = new ArrayColumnTypes();
        LAG_COLUMN_TYPES_LV.add(ColumnType.LONG); // start offset of partition's ring inside the shared MemoryARW
        LAG_COLUMN_TYPES_LV.add(ColumnType.LONG); // firstIdx (position of current oldest element in the ring)
        LAG_COLUMN_TYPES_LV.add(ColumnType.LONG); // count of rows the partition has observed
        LAG_COLUMN_TYPES_LV.add(ColumnType.BYTE); // tombstone bit (anchor-driven compaction)
    }
}
