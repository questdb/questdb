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
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

public class RowNumberFunctionFactory implements FunctionFactory {

    public static final String NAME = "row_number";
    // Base value layout for regular queries: [rowNumber:LONG]. When compiling
    // inside a live view, RowNumberFunction appends a BYTE tombstone slot
    // consumed by anchor-driven compaction. The slot is omitted for
    // non-live-view queries to avoid the per-partition-key overhead.
    private static final int ROW_NUMBER_VALUE_INDEX = 0;
    private static final String SIGNATURE = NAME + "()";

    @Override
    public String getSignature() {
        return SIGNATURE;
    }

    @Override
    public boolean isWindow() {
        return true;
    }

    @Override
    public Function newInstance(
            int position,
            ObjList<Function> args,
            IntList argPositions,
            CairoConfiguration configuration,
            SqlExecutionContext sqlExecutionContext
    ) throws SqlException {
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getPartitionByRecord() != null) {
            // The WindowContext's partitionByKeyTypes is a transient buffer owned by
            // SqlCodeGenerator that gets cleared on every window function compile.
            // The live-view frontier sweep needs to allocate a scratch Map with the same
            // key shape after compilation has moved on, so take our own copy.
            ArrayColumnTypes keyTypes = new ArrayColumnTypes();
            ColumnTypes contextKeyTypes = windowContext.getPartitionByKeyTypes();
            for (int i = 0, n = contextKeyTypes.getColumnCount(); i < n; i++) {
                keyTypes.add(contextKeyTypes.getColumnType(i));
            }
            ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(ColumnType.LONG); // rowNumber
            int tombstoneValueIndex = -1;
            if (windowContext.isLiveView()) {
                valueTypes.add(ColumnType.BYTE); // tombstone (anchor-driven compaction)
                tombstoneValueIndex = 1;
            }
            Map map = MapFactory.createUnorderedMap(
                    configuration,
                    keyTypes,
                    valueTypes
            );
            return new RowNumberFunction(
                    map,
                    windowContext.getPartitionByRecord(),
                    windowContext.getPartitionBySink(),
                    keyTypes,
                    valueTypes,
                    tombstoneValueIndex,
                    configuration
            );
        }

        return new SequenceRowNumberFunction();
    }

    private static class RowNumberFunction extends LongFunction implements WindowFunction, Reopenable {
        private final CairoConfiguration configuration;
        private final ColumnTypes keyColumnTypes;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        // -1 outside live-view mode; index of the BYTE tombstone slot in LV mode.
        private final int tombstoneValueIndex;
        private final ColumnTypes valueColumnTypes;
        private int columnIndex;
        // Reusable second map for the live-view frontier sweep; ping-pongs with map
        // so a sweep never allocates. Allocated once on the first sweep.
        private Map compactionScratch;
        private Map map;
        // The per-query MemoryTracker bound by setMemoryTracker; retained so
        // retainPartitions can charge the lazily-created compaction scratch too.
        private MemoryTracker memoryTracker;
        private long rowNumber;
        // Single-writer (refresh worker), not volatile.
        private long tombstoneCount;

        public RowNumberFunction(
                Map map,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                ColumnTypes keyColumnTypes,
                ColumnTypes valueColumnTypes,
                int tombstoneValueIndex,
                CairoConfiguration configuration
        ) {
            this.map = map;
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.keyColumnTypes = keyColumnTypes;
            this.valueColumnTypes = valueColumnTypes;
            this.tombstoneValueIndex = tombstoneValueIndex;
            this.configuration = configuration;
            // Start the map closed (lazy): the owning cursor binds the per-query
            // MemoryTracker via setMemoryTracker() before reopen() allocates the
            // backing under it, so a high-cardinality PARTITION BY is charged to the
            // per-query limit. reset() frees it symmetrically at cursor close.
            this.map.close();
        }

        @Override
        public void close() {
            Misc.free(map);
            Misc.free(compactionScratch);
            Misc.freeObjList(partitionByRecord.getFunctions());
        }

        @Override
        public void retainPartitions(Map survivingKeys) {
            // RowNumber implements WindowFunction directly (no BasePartitionedWindowFunction),
            // so it overrides retainPartitions itself. The reusable scratch ping-pongs
            // with map; only the first sweep allocates.
            if (compactionScratch == null) {
                compactionScratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, valueColumnTypes);
                // createUnorderedMap returns an OPEN map allocated under no tracker.
                // Free that untracked backing, bind the tracker, then reopen so the
                // scratch's malloc and free stay symmetric on the per-query counter
                // once the ping-pong swap below promotes it to the live map.
                if (memoryTracker != null) {
                    compactionScratch.close();
                    compactionScratch.setMemoryTracker(memoryTracker);
                    compactionScratch.reopen();
                }
            } else {
                compactionScratch.clear();
            }
            PartitionStateEvictor.rebuildKeepingMembers(map, compactionScratch, survivingKeys);
            Map old = map;
            map = compactionScratch;
            compactionScratch = old;
            tombstoneCount = 0;
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.createValue();
            long x;
            if (value.isNew()) {
                if (tombstoneValueIndex >= 0) {
                    value.putByte(tombstoneValueIndex, (byte) 0);
                }
                x = 0;
            } else {
                x = value.getLong(ROW_NUMBER_VALUE_INDEX);
            }
            rowNumber = x + 1;
            value.putLong(ROW_NUMBER_VALUE_INDEX, rowNumber);
        }

        @Override
        public void resetPartition(Record record) {
            // ANCHOR-driven reset. Drop the partition's row counter back to
            // zero so the next computeNext sees x=0 and emits 1.
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null) {
                value.putLong(ROW_NUMBER_VALUE_INDEX, 0L);
                if (!value.isNew() && tombstoneValueIndex >= 0 && value.getByte(tombstoneValueIndex) != 1) {
                    value.putByte(tombstoneValueIndex, (byte) 1);
                    tombstoneCount++;
                }
            }
        }

        @Override
        public long getLong(Record rec) {
            return rowNumber;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return valueColumnTypes.getColumnCount();
        }

        @Override
        public long getTombstoneCount() {
            return tombstoneCount;
        }

        @Override
        public int getTombstoneValueIndex() {
            return tombstoneValueIndex;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void markPartitionAlive(Record record) {
            if (tombstoneValueIndex < 0 || tombstoneCount == 0) {
                return;
            }
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue value = key.findValue();
            if (value != null && value.getByte(tombstoneValueIndex) == 1) {
                value.putByte(tombstoneValueIndex, (byte) 0);
                tombstoneCount--;
            }
        }

        @Override
        public void onSnapshotRestoreBegin() {
            // The map starts closed (lazy) and the live-view restore path can run
            // before any cursor of()/ofIncremental reopens it, so reopen() first;
            // it allocates the backing when closed and is a no-op when already open.
            map.reopen();
            map.clear();
            tombstoneCount = 0;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            computeNext(record);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), rowNumber);
        }

        @Override
        public void reopen() {
            rowNumber = 0;
            tombstoneCount = 0;
            map.reopen();
        }

        @Override
        public void reset() {
            map.close();
            compactionScratch = Misc.free(compactionScratch);
            tombstoneCount = 0;
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            value.putLong(ROW_NUMBER_VALUE_INDEX, source.getLong(offset));
            offset += Long.BYTES;
            if (tombstoneValueIndex >= 0) {
                value.putByte(tombstoneValueIndex, (byte) 0);
            }
            return offset;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void setMemoryTracker(@Nullable MemoryTracker tracker) {
            // Retain the tracker so retainPartitions can charge the compaction scratch
            // to it, and bind it on the lazily-allocated map before the cursor's
            // reopen() allocates the backing under it.
            this.memoryTracker = tracker;
            map.setMemoryTracker(tracker);
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
            sink.putLong(value.getLong(ROW_NUMBER_VALUE_INDEX));
        }

        @Override
        public boolean supportsSnapshot() {
            // tombstoneValueIndex >= 0 marks a live-view compile; this class has no
            // dedicated liveView flag, so it stands in for the guard the other
            // window families carry.
            return tombstoneValueIndex >= 0
                    && keyColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            rowNumber = 0;
            tombstoneCount = 0;
            map.clear();
        }
    }

    private static class SequenceRowNumberFunction extends LongFunction implements WindowFunction, Reopenable {
        private int columnIndex;
        private long rowNumber = 0;

        @Override
        public void computeNext(Record record) {
            ++rowNumber;
        }

        @Override
        public long getLong(Record rec) {
            return rowNumber;
        }

        @Override
        public int getPassCount() {
            return ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) {
            toTop();
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), ++rowNumber);
        }

        @Override
        public void reopen() {
            toTop();
        }

        @Override
        public void reset() {
            toTop();
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            rowNumber = source.getLong(offset);
            return offset + Long.BYTES;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
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
            sink.putLong(rowNumber);
        }

        @Override
        public boolean supportsSnapshot() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(SIGNATURE);
        }

        @Override
        public void toTop() {
            rowNumber = 0;
        }
    }
}
