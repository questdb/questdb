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
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.Vm;
import io.questdb.cairo.vm.api.MemoryARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.DoubleFunction;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.LongList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

/**
 * cume_dist() window function.
 * Returns the cumulative distribution: (number of rows at or before current) / (total rows).
 * All peer rows (same ORDER BY value) share the same cume_dist, which equals the position
 * of the last peer divided by total rows. Result range is (0, 1].
 * Returns 1.0 if there is no ORDER BY (all rows are peers).
 */
public class CumeDistFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "cume_dist";
    // Map value columns for the partitioned function: lastOffset, rank (pass1) / prevRank (pass2),
    // count, deferredStartOffset, deferredSize, deferredCapacity. See the static initializer below.
    private static final ArrayColumnTypes CUME_DIST_COLUMN_TYPES;
    private static final String SIGNATURE = NAME + "()";

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
        final WindowContext windowContext = sqlExecutionContext.getWindowContext();
        if (windowContext.isEmpty()) {
            throw SqlException.emptyWindowContext(position);
        }

        if (windowContext.getNullsDescPos() > 0) {
            throw SqlException.$(windowContext.getNullsDescPos(), "RESPECT/IGNORE NULLS is not supported for current window function");
        }

        if (!windowContext.isDefaultFrame()) {
            throw SqlException.$(position, "cume_dist() does not support framing; remove the frame clause");
        }

        if (windowContext.isOrdered()) {
            if (windowContext.getPartitionByRecord() != null) {
                MemoryARW memory = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
                try {
                    return new CumeDistOverPartitionFunction(
                            windowContext.getPartitionByKeyTypes(),
                            windowContext.getPartitionByRecord(),
                            windowContext.getPartitionBySink(),
                            configuration,
                            memory,
                            configuration.getSqlWindowInitialRangeBufferSize()
                    );
                } catch (Throwable t) {
                    Misc.free(memory);
                    throw t;
                }
            } else {
                MemoryARW memory = Vm.getCARWInstance(
                        configuration.getSqlWindowStorePageSize(),
                        configuration.getSqlWindowStoreMaxPages(),
                        MemoryTag.NATIVE_CIRCULAR_BUFFER
                );
                try {
                    return new CumeDistFunction(memory);
                } catch (Throwable t) {
                    Misc.free(memory);
                    throw t;
                }
            }
        } else {
            // No ORDER BY: all rows are peers, cume_dist = totalRows / totalRows = 1.0
            return new CumeDistNoOrderFunction(windowContext.getPartitionByRecord());
        }
    }

    // cume_dist() over (order by xxx) - no partition by
    //
    // pass1 stores rank (like percent_rank) for each row. pass2 resolves the
    // cumulative distribution by detecting peer-group boundaries: when the rank
    // changes from r1 to r2, all rows in the previous group get (r2 - 1) / totalRows.
    // The last group is handled by writing a tentative 1.0 that is never overwritten.
    // The deferred-offsets buffer lives in native memory to avoid on-heap growth when
    // a single peer group spans the whole result set (e.g. ORDER BY constant_column).
    static class CumeDistFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private final MemoryARW deferredOffsets;
        private int columnIndex;
        private long count = 1;
        private long deferredSize;
        private long lastRecordOffset;
        private ObjList<ExpressionNode> orderBy;
        private long prevRank;
        private long rank;
        private ObjList<DirectIntList> rankMaps;
        private RecordComparator recordComparator;
        private long totalRows;

        public CumeDistFunction(MemoryARW deferredOffsets) {
            this.deferredOffsets = deferredOffsets;
        }

        @Override
        public void close() {
            super.close();
            Misc.freeObjList(rankMaps);
            deferredOffsets.close();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            SortKeyEncoder.buildRankMaps(symbolTableSource, rankMaps, recordComparator);
        }

        @Override
        public void initRecordComparator(SqlCodeGenerator sqlGenerator,
                                         RecordMetadata metadata,
                                         ArrayColumnTypes chainTypes,
                                         IntList orderIndices,
                                         ObjList<ExpressionNode> orderBy,
                                         IntList orderByDirection) throws SqlException {
            IntList indices = orderIndices != null ? orderIndices : sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, indices);
            this.rankMaps = SortKeyEncoder.createRankMaps(metadata, indices);
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (count == 1) {
                rank = 1;
            } else {
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastRecordOffset)) != 0) {
                    rank = count;
                }
            }
            lastRecordOffset = recordOffset;
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
            count++;
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            long storedRank = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));

            if (prevRank != 0 && storedRank != prevRank) {
                // Peer group boundary: flush all deferred rows with (storedRank - 1) / totalRows
                double cumeDist = (double) (storedRank - 1) / (double) totalRows;
                for (long i = 0; i < deferredSize; i++) {
                    long offset = deferredOffsets.getLong(i * Long.BYTES);
                    Unsafe.getUnsafe().putDouble(spi.getAddress(offset, columnIndex), cumeDist);
                }
                deferredSize = 0;
            }

            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), 1.0);
            deferredOffsets.putLong(deferredSize * Long.BYTES, recordOffset);
            deferredSize++;
            prevRank = storedRank;
        }

        @Override
        public void preparePass2() {
            totalRows = count - 1; // count was incremented after each row
            prevRank = 0;
            deferredSize = 0;
        }

        @Override
        public void reopen() {
            count = 1;
            deferredSize = 0;
        }

        @Override
        public void reset() {
            count = 1;
            totalRows = 0;
            prevRank = 0;
            deferredSize = 0;
            Misc.freeObjListAndKeepObjects(rankMaps);
            deferredOffsets.close();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val("()");
            sink.val(" over (");
            sink.val("order by ");
            sink.val(orderBy);
            sink.val(')');
        }

        @Override
        public void toTop() {
            count = 1;
            totalRows = 0;
            prevRank = 0;
            deferredSize = 0;
            deferredOffsets.truncate();
            super.toTop();
        }
    }

    // cume_dist() without ORDER BY: all rows are peers, so cume_dist = 1.0 for all rows
    static class CumeDistNoOrderFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private static final double CUME_DIST_CONST = 1.0;
        private final VirtualRecord partitionByRecord;
        private int columnIndex;

        public CumeDistNoOrderFunction(VirtualRecord partitionByRecord) {
            this.partitionByRecord = partitionByRecord;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public double getDouble(Record rec) {
            return CUME_DIST_CONST;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (partitionByRecord != null) {
                Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), CUME_DIST_CONST);
        }

        @Override
        public void reopen() {
        }

        @Override
        public void reset() {
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            PercentRankFunctionFactory.PercentRankNoOrderFunction.toSink0(sink, partitionByRecord);
        }
    }

    // cume_dist() over (partition by xxx order by xxx)
    //
    // pass1 stores rank per row and per-partition state in a Map (lastOffset, rank, count,
    // deferredStartOffset, deferredSize, deferredCapacity). Each partition's deferred output
    // offsets live in a slice of a single shared MemoryARW -- no per-partition Java objects.
    // pass2 detects peer-group boundaries per partition, walks the slice and writes the
    // resolved cumulative distribution value, then resets the slice for the next peer group.
    // Slices grow on demand via expandRingBuffer, recycling previously-freed blocks through
    // freeList (same pattern as NthValueOverPartitionRangeFrameFunction and friends).
    // Per-partition slices are not released until close()/toTop(), so worst-case native memory
    // is O(total rows across all partitions) when peer groups span entire partitions.
    static class CumeDistOverPartitionFunction extends DoubleFunction implements Function, WindowFunction, Reopenable {

        private final CairoConfiguration configuration;
        private final MemoryARW deferredOffsets;
        private final LongList freeList = new LongList();
        private final int initialBufferSize;
        private final ColumnTypes keyColumnTypes;
        private final RingBufferDesc memoryDesc = new RingBufferDesc();
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private int columnIndex;
        private Map map;
        private ObjList<ExpressionNode> orderBy;
        private ObjList<DirectIntList> rankMaps;
        private RecordComparator recordComparator;

        public CumeDistOverPartitionFunction(
                ColumnTypes keyColumnTypes,
                VirtualRecord partitionByRecord,
                RecordSink partitionBySink,
                CairoConfiguration configuration,
                MemoryARW deferredOffsets,
                int initialBufferSize
        ) {
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            this.keyColumnTypes = keyColumnTypes;
            this.configuration = configuration;
            this.deferredOffsets = deferredOffsets;
            this.initialBufferSize = initialBufferSize;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
            Misc.freeObjList(partitionByRecord.getFunctions());
            Misc.freeObjList(rankMaps);
            deferredOffsets.close();
            freeList.clear();
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int getPassCount() {
            return WindowFunction.TWO_PASS;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            SortKeyEncoder.buildRankMaps(symbolTableSource, rankMaps, recordComparator);
        }

        @Override
        public void initRecordComparator(SqlCodeGenerator sqlGenerator,
                                         RecordMetadata metadata,
                                         ArrayColumnTypes chainTypes,
                                         IntList orderIndices,
                                         ObjList<ExpressionNode> orderBy,
                                         IntList orderByDirection) throws SqlException {
            IntList indices = orderIndices != null ? orderIndices : sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            try {
                map = MapFactory.createUnorderedMap(
                        configuration,
                        keyColumnTypes,
                        CUME_DIST_COLUMN_TYPES
                );
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, indices);
                this.rankMaps = SortKeyEncoder.createRankMaps(metadata, indices);
            } catch (Throwable t) {
                map = Misc.free(map);
                throw t;
            }
            this.orderBy = orderBy;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long rank;
            long count;
            if (mapValue.isNew()) {
                rank = 1;
                count = 1;
                long startOffset = deferredOffsets.appendAddressFor((long) initialBufferSize * Long.BYTES)
                        - deferredOffsets.getPageAddress(0);
                mapValue.putLong(3, startOffset);
                mapValue.putLong(4, 0L);
                mapValue.putLong(5, initialBufferSize);
            } else {
                long lastOffset = mapValue.getLong(0);
                rank = mapValue.getLong(1);
                count = mapValue.getLong(2);
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastOffset)) != 0) {
                    rank = count;
                }
            }

            mapValue.putLong(0, recordOffset);
            mapValue.putLong(1, rank);
            mapValue.putLong(2, count + 1);
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
        }

        @Override
        public void pass2(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.findValue();

            long storedRank = Unsafe.getUnsafe().getLong(spi.getAddress(recordOffset, columnIndex));
            long totalRows = mapValue.getLong(2) - 1;
            long prevRank = mapValue.getLong(1);
            long startOffset = mapValue.getLong(3);
            long size = mapValue.getLong(4);
            long capacity = mapValue.getLong(5);

            if (prevRank != 0 && storedRank != prevRank) {
                // Peer group boundary for this partition: flush the deferred slice.
                double cumeDist = (double) (storedRank - 1) / (double) totalRows;
                for (long i = 0; i < size; i++) {
                    long offset = deferredOffsets.getLong(startOffset + i * Long.BYTES);
                    Unsafe.getUnsafe().putDouble(spi.getAddress(offset, columnIndex), cumeDist);
                }
                size = 0;
            }

            // Grow the slice if it is full. expandRingBuffer doubles capacity and returns the
            // new slice via memoryDesc; the previous slice is recycled through freeList.
            if (size == capacity) {
                memoryDesc.reset(capacity, startOffset, size, 0, freeList);
                expandRingBuffer(deferredOffsets, memoryDesc, Long.BYTES);
                capacity = memoryDesc.capacity;
                startOffset = memoryDesc.startOffset;
            }

            deferredOffsets.putLong(startOffset + size * Long.BYTES, recordOffset);
            size++;
            Unsafe.getUnsafe().putDouble(spi.getAddress(recordOffset, columnIndex), 1.0);
            mapValue.putLong(1, storedRank);
            mapValue.putLong(3, startOffset);
            mapValue.putLong(4, size);
            mapValue.putLong(5, capacity);
        }

        @Override
        public void preparePass2() {
            // Reset column 1 (rank) to 0 for use as prevRank in pass2, and reset each partition's
            // deferred slice size to 0 (capacity and startOffset remain so that pass2 reuses the
            // already-allocated native slice).
            MapRecordCursor cursor = map.getCursor();
            MapRecord record = cursor.getRecord();
            while (cursor.hasNext()) {
                MapValue value = record.getValue();
                value.putLong(1, 0);
                value.putLong(4, 0L);
            }
        }

        @Override
        public void reopen() {
            if (map != null) {
                map.reopen();
            }
            freeList.clear();
        }

        @Override
        public void reset() {
            Misc.free(map);
            Misc.freeObjListAndKeepObjects(rankMaps);
            deferredOffsets.close();
            freeList.clear();
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(NAME);
            sink.val("()");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(" order by ");
            sink.val(orderBy);
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            Misc.clear(map);
            deferredOffsets.truncate();
            freeList.clear();
        }
    }

    static {
        CUME_DIST_COLUMN_TYPES = new ArrayColumnTypes();
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 0: lastOffset
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 1: rank (pass1) / prevRank (pass2)
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 2: count
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 3: deferredStartOffset (native offset into shared MemoryARW)
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 4: deferredSize (entries in the deferred slice)
        CUME_DIST_COLUMN_TYPES.add(ColumnType.LONG); // slot 5: deferredCapacity (allocated entries in the slice)
    }
}
