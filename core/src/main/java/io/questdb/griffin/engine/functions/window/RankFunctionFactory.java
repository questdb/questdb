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
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.SingleRecordSink;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.lv.LiveViewSnapshotKeyCodec;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapFactory;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.RecordValueSink;
import io.questdb.cairo.map.RecordValueSinkFactory;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.cairo.vm.api.MemoryA;
import io.questdb.cairo.vm.api.MemoryR;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlCodeGenerator;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.griffin.engine.functions.LongFunction;
import io.questdb.griffin.engine.orderby.SortKeyEncoder;
import io.questdb.griffin.engine.window.WindowContext;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.std.DirectIntList;
import io.questdb.std.IntList;
import io.questdb.std.MemoryTag;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.jetbrains.annotations.Nullable;

public class RankFunctionFactory extends AbstractWindowFunctionFactory {

    public static final String NAME = "rank";
    private static final ArrayColumnTypes RANK_COLUMN_TYPES;
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

        if (windowContext.isOrdered()) {
            // Rank() over (partition by xxx order by xxx)
            if (windowContext.getPartitionByRecord() != null) {
                return new RankOverPartitionFunction(
                        windowContext.getPartitionByKeyTypes(),
                        windowContext.getPartitionByRecord(),
                        windowContext.getPartitionBySink(),
                        windowContext.getTimestampIndex(),
                        configuration,
                        false,
                        windowContext.isLiveView(),
                        NAME);
            } else {
                // Rank() over (order by xxx)
                return new RankFunction(configuration, false, NAME);
            }
        } else {
            // Rank() over ([partition by xxx | ]), without ORDER BY, all rows are peers.
            return new RankNoOrderFunction(windowContext.getPartitionByRecord(), NAME);
        }
    }

    protected static class RankFunction extends LongFunction implements Function, WindowFunction, Reopenable {

        private final CairoConfiguration configuration;
        private final boolean dense;
        private final String name;
        private int columnIndex;
        private long count = 1;
        private long lastRecordOffset;
        private long rank;
        private ObjList<DirectIntList> rankMaps;
        private RecordComparator recordComparator;
        private RecordSink recordSink;
        private SingleRecordSink singleRecordSinkA;
        private SingleRecordSink singleRecordSinkB;

        public RankFunction(CairoConfiguration configuration, boolean dense, String name) {
            this.configuration = configuration;
            this.dense = dense;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(singleRecordSinkA);
            Misc.free(singleRecordSinkB);
            Misc.freeObjList(rankMaps);
        }

        @Override
        public void computeNext(Record record) {
            SingleRecordSink singleRecordSink = count % 2 == 0 ? singleRecordSinkA : singleRecordSinkB;
            singleRecordSink.clear();
            recordSink.copy(record, singleRecordSink);
            if (count == 1) {
                rank = 1;
            } else {
                if (!singleRecordSinkA.memeq(singleRecordSinkB)) {
                    rank = dense ? rank + 1 : count;
                }
            }
            count++;
        }

        @Override
        public long getLong(Record rec) {
            return rank;
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
            if (chainTypes.getColumnCount() == 0) {
                ListColumnFilter listColumnFilter = sqlGenerator.getIndexColumnFilter();
                listColumnFilter.clear();
                for (int i = 0, size = orderBy.size(); i < size; i++) {
                    ExpressionNode tok = orderBy.getQuick(i);
                    int index = metadata.getColumnIndexQuiet(tok.token);
                    listColumnFilter.add(index + 1);
                }

                for (int i = 0, size = metadata.getColumnCount(); i < size; i++) {
                    chainTypes.add(metadata.getColumnType(i));
                }
                recordSink = RecordSinkFactory.getInstance(configuration, sqlGenerator.getAsm(), chainTypes, listColumnFilter);
                singleRecordSinkA = new SingleRecordSink((long) configuration.getSqlWindowStorePageSize() * configuration.getSqlWindowStoreMaxPages() / 2, MemoryTag.NATIVE_RECORD_CHAIN);
                singleRecordSinkB = new SingleRecordSink((long) configuration.getSqlWindowStorePageSize() * configuration.getSqlWindowStoreMaxPages() / 2, MemoryTag.NATIVE_RECORD_CHAIN);
            } else {
                if (orderIndices == null) {
                    orderIndices = sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
                }
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, orderIndices);
                this.rankMaps = SortKeyEncoder.createRankMaps(metadata, orderIndices);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            if (count == 1) {
                rank = 1;
            } else {
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastRecordOffset)) != 0) {
                    rank = dense ? rank + 1 : count;
                }
            }
            lastRecordOffset = recordOffset;
            count++;
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), rank);
        }

        @Override
        public void reopen() {
            count = 1;
            if (singleRecordSinkA != null) {
                singleRecordSinkA.reopen();
            }
            if (singleRecordSinkB != null) {
                singleRecordSinkB.reopen();
            }
        }

        @Override
        public void reset() {
            count = 1;
            Misc.freeObjListAndKeepObjects(rankMaps);
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val("() over ()");
        }

        @Override
        public void toTop() {
            count = 1;
            super.toTop();
        }
    }

    protected static class RankNoOrderFunction extends LongFunction implements Function, WindowFunction, Reopenable {

        private static final long RANK_CONST = 1;
        private final String name;
        private final VirtualRecord partitionByRecord;
        private int columnIndex;

        public RankNoOrderFunction(VirtualRecord partitionByRecord, String name) {
            this.partitionByRecord = partitionByRecord;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            if (partitionByRecord != null) {
                Misc.freeObjList(partitionByRecord.getFunctions());
            }
        }

        @Override
        public long getLong(Record rec) {
            return RANK_CONST;
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
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            if (partitionByRecord != null) {
                Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            }
        }

        @Override
        public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (partitionByRecord != null) {
                Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            }
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), RANK_CONST);
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
            sink.val(getName());
            PercentRankFunctionFactory.PercentRankNoOrderFunction.toSink0(sink, partitionByRecord);
        }
    }

    protected static class RankOverPartitionFunction extends LongFunction implements Function, WindowFunction, Reopenable {

        private final CairoConfiguration configuration;
        private final boolean dense;
        private final ColumnTypes keyColumnTypes;
        // True when this function is being compiled as part of a live view's
        // SELECT. Drives opt-in allocation of the lastActivityTs value-layout
        // slot used by partition-state eviction.
        private final boolean liveView;
        private final String name;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private final int tsColumnIndex;
        // Subset of mapValueTypes covering the chain-prefix slots [0, chainTypeIndex).
        // Populated when this function compiles for a live view so the snapshot
        // codec can read the chain bytes back from MapValue at restore time;
        // null for non-live-view compiles where snapshot is never called.
        private ArrayColumnTypes chainColumnTypes;
        private int chainTypeIndex;
        private int columnIndex;
        // Reusable second map for the live-view frontier sweep; ping-pongs with map.
        private Map compactionScratch;
        // Value-layout index of the lastActivityTs slot (live view). Lives
        // at chainTypeIndex + 2 when present, or is -1 for regular queries where
        // the slot is omitted.
        private int lastActivityTsValueIndex = -1;
        private Map map;
        private ArrayColumnTypes mapValueTypes;
        private long rank;
        private ObjList<DirectIntList> rankMaps;
        private RecordComparator recordComparator;
        private RecordValueSink recordValueSink;
        private long sizeAfterLastEvict;
        // For the streaming (WindowRecordCursorFactory) path the MapValue stores only the ORDER BY
        // columns, compacted. This maps each compacted rank-map slot back to its base column index so
        // init() can populate the rank maps from the right symbol tables. Null on the cached path.
        private int[] streamingSymbolTableIndices;
        // Live-view-only: count of partitions whose tombstone byte is set. Tracked
        // on the refresh-worker thread (single writer), read by 2b.1d's compaction
        // path. Not volatile.
        private long tombstoneCount;
        // Value-layout index of the per-partition tombstone byte (live view only).
        // Lives at chainTypeIndex + 3 (one slot past lastActivityTs); -1 for
        // non-live-view compiles where the slot is omitted.
        private int tombstoneValueIndex = -1;

        public RankOverPartitionFunction(ColumnTypes keyColumnTypes,
                                         VirtualRecord partitionByRecord,
                                         RecordSink partitionBySink,
                                         int tsColumnIndex,
                                         CairoConfiguration configuration,
                                         boolean dense,
                                         boolean liveView,
                                         String name) {
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            // Snapshot the key types: the streaming path builds the map lazily in
            // initRecordComparator(), by which point the generator has rebuilt its shared key-types
            // buffer for a later window column's PARTITION BY. Partition-state eviction also needs a
            // stable copy to allocate a scratch Map with the same key shape after compilation moves on.
            this.keyColumnTypes = copyKeyTypes(keyColumnTypes);
            this.tsColumnIndex = tsColumnIndex;
            this.configuration = configuration;
            this.dense = dense;
            this.liveView = liveView;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
            Misc.free(compactionScratch);
            Misc.freeObjList(partitionByRecord.getFunctions());
            Misc.freeObjList(rankMaps);
        }

        @Override
        public void retainPartitions(Map survivingKeys) {
            // RankOverPartitionFunction implements WindowFunction directly (no
            // BasePartitionedWindowFunction), so it overrides retainPartitions itself.
            // The reusable scratch ping-pongs with map; only the first sweep allocates.
            if (compactionScratch == null) {
                compactionScratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
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
            MapValue mapValue = key.createValue();
            // Live mode keeps a tombstone byte alongside the rank slots; write it
            // explicitly on the isNew branch so the two-pass snapshot walk sees the
            // same byte both times. Maps do not guarantee zeroed value bytes on
            // createValue.
            if (mapValue.isNew() && tombstoneValueIndex >= 0) {
                mapValue.putByte(tombstoneValueIndex, (byte) 0);
            }
            long count;
            boolean isNew = mapValue.isNew();
            // count == 0 acts as the implicit "uninitialized" signal - the natural
            // state after createValue() returns a fresh entry, and the state
            // resetPartition restores when the live-view ANCHOR fires.
            boolean fresh = isNew || mapValue.getLong(chainTypeIndex + 1) == 0;
            if (fresh) {
                rank = 1;
                count = 1;
            } else {
                rank = mapValue.getLong(chainTypeIndex);
                count = mapValue.getLong(chainTypeIndex + 1);
                // The MapValue holds the previous row's ORDER BY columns (compacted). setLeft caches
                // them by value, so we can overwrite the slots with the current row and then compare
                // the cached previous row against it - the comparator never touches the live record,
                // which lets the MapValue store only the ORDER BY columns instead of the whole row.
                recordComparator.setLeft(mapValue);
            }
            recordValueSink.copy(record, mapValue);
            if (!fresh && recordComparator.compare(mapValue) != 0) {
                rank = dense ? rank + 1 : count;
            }
            mapValue.putLong(chainTypeIndex, rank);
            mapValue.putLong(chainTypeIndex + 1, count + 1);
            if (lastActivityTsValueIndex >= 0) {
                // Track per-key last-activity-ts for live view retention-driven eviction.
                // tsColumnIndex is -1 when the window is defined over a source with no
                // designated timestamp; writing Long.MIN_VALUE keeps those keys below any
                // eviction cutoff, so they never get evicted (live views always have one).
                mapValue.putLong(lastActivityTsValueIndex,
                        tsColumnIndex >= 0 ? record.getTimestamp(tsColumnIndex) : Long.MIN_VALUE);
            }
        }

        @Override
        public void evictStalePartitionState(long cutoffTs) {
            if (lastActivityTsValueIndex < 0) {
                // Non-live-view queries do not carry the lastActivityTs slot and
                // do not exercise partition-state eviction.
                return;
            }
            long size = map.size();
            if (size == 0 || size < sizeAfterLastEvict * 2) {
                return;
            }
            Map scratch = MapFactory.createUnorderedMap(configuration, keyColumnTypes, mapValueTypes);
            try {
                scratch.setKeyCapacity((int) Math.min(size, Integer.MAX_VALUE));
                PartitionStateEvictor.rebuildKeeping(map, scratch, lastActivityTsValueIndex, cutoffTs);
                Misc.free(map);
                map = scratch;
                scratch = null;
                sizeAfterLastEvict = map.size();
            } finally {
                Misc.free(scratch);
            }
        }

        @Override
        public long getLong(Record rec) {
            return rank;
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
        public Map getPartitionMap() {
            return map;
        }

        @Override
        public ColumnTypes getSnapshotKeyColumnTypes() {
            return keyColumnTypes;
        }

        @Override
        public int getSnapshotKeyStartIndex() {
            return mapValueTypes != null
                    ? mapValueTypes.getColumnCount()
                    : chainTypeIndex + 2;
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
        public void resetPartition(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            // Rank uses createValue intentionally: a partition may not exist yet
            // when the anchor-driven reset fires (the anchor map fires
            // resetPartition before the function map's first computeNext on a
            // fresh partition). Other migrated functions only call findValue
            // because their per-partition state is established by the row loop;
            // rank's "uninitialized" signal is count=0, which must be written
            // explicitly here.
            MapValue mapValue = key.createValue();
            // Set count to 0 — the implicit "uninitialized" signal for computeNext.
            // Rank itself doesn't need an explicit clear; the next computeNext writes
            // both slots before they're read.
            mapValue.putLong(chainTypeIndex + 1, 0);
            if (mapValue.isNew()) {
                // Fresh entry: pin the tombstone byte to 0 since Maps don't guarantee
                // zeroed value bytes on createValue.
                if (tombstoneValueIndex >= 0) {
                    mapValue.putByte(tombstoneValueIndex, (byte) 0);
                }
            } else if (tombstoneValueIndex >= 0 && mapValue.getByte(tombstoneValueIndex) != 1) {
                mapValue.putByte(tombstoneValueIndex, (byte) 1);
                tombstoneCount++;
            }
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            super.init(symbolTableSource, executionContext);
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
            if (streamingSymbolTableIndices != null) {
                // Streaming path: rank maps are indexed by compacted ORDER BY position, but the symbol
                // tables live at the base column indices, so build each one through the mapping.
                if (rankMaps != null) {
                    for (int i = 0, n = rankMaps.size(); i < n; i++) {
                        DirectIntList rankMap = rankMaps.getQuick(i);
                        if (rankMap != null) {
                            SortKeyEncoder.buildRankMap(symbolTableSource.getSymbolTable(streamingSymbolTableIndices[i]), rankMap);
                        }
                    }
                    recordComparator.setRankMaps(rankMaps);
                }
            } else {
                SortKeyEncoder.buildRankMaps(symbolTableSource, rankMaps, recordComparator);
            }
        }

        @Override
        public void initPartitionBy(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            Function.init(partitionByRecord.getFunctions(), symbolTableSource, executionContext, null);
        }

        @Override
        public void initRecordComparator(SqlCodeGenerator sqlGenerator,
                                         RecordMetadata metadata,
                                         ArrayColumnTypes chainTypes,
                                         IntList orderIndices,
                                         ObjList<ExpressionNode> orderBy,
                                         IntList orderByDirection) throws SqlException {
            if (orderIndices == null) {
                orderIndices = sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            }

            if (chainTypes.getColumnCount() == 0) { // for WindowRecordCursorFactory
                // Only the ORDER BY columns decide whether two consecutive rows are peers, so the
                // MapValue holds just those (compacted) plus the running rank and count. Copying the
                // whole projected row would force pass-through columns the MapValue cannot store
                // (UUID, STRING, VARCHAR, BINARY, LONG256, arrays, ...) through RecordValueSinkFactory
                // and crash compilation. The value sink reads the ORDER BY columns from the live
                // record at their base indices and writes them into MapValue slots 0..k-1; the
                // comparator and rank maps are built over a matching compacted metadata so they read
                // those same slots back.
                final int orderByCount = orderIndices.size();

                ListColumnFilter listColumnFilter = sqlGenerator.getIndexColumnFilter();
                listColumnFilter.clear();
                final GenericRecordMetadata orderByMetadata = new GenericRecordMetadata();
                final IntList compactOrderIndices = new IntList(orderByCount);
                streamingSymbolTableIndices = new int[orderByCount];
                for (int i = 0; i < orderByCount; i++) {
                    final int encoded = orderIndices.getQuick(i);
                    final int orderByColumn = Math.abs(encoded) - 1;
                    listColumnFilter.add(orderByColumn + 1);
                    final TableColumnMetadata src = metadata.getColumnMetadata(orderByColumn);
                    final int orderByColumnType = src.getColumnType();
                    // The compacted MapValue holds the previous row's ORDER BY values, and computeNext
                    // caches them through setLeft, overwrites the slots with the current row, then
                    // compares. That is sound only when each value is cached by value: a var-size
                    // column (STRING, VARCHAR, BINARY, array, INTERVAL) or a non-static symbol would be
                    // cached as a flyweight aliasing the record and get corrupted by the overwrite,
                    // silently producing wrong ranks. Only the designated timestamp and static indexed
                    // SYMBOLs reach the streaming path today; assert the cached-by-value invariant so a
                    // future routing change that admits another type fails fast here instead of
                    // returning wrong results.
                    assert ColumnType.isFixedSize(orderByColumnType)
                            || (ColumnType.isSymbol(orderByColumnType) && src.isSymbolTableStatic())
                            : "streaming rank ORDER BY column must be fixed-size or a static symbol, was "
                            + ColumnType.nameOf(orderByColumnType);
                    // Synthetic unique names keep duplicate ORDER BY columns from clashing; only the
                    // type and the static-symbol flag matter to the comparator and the rank maps.
                    orderByMetadata.add(new TableColumnMetadata(
                            "k" + i,
                            orderByColumnType,
                            IndexType.NONE,
                            0,
                            src.isSymbolTableStatic(),
                            null
                    ));
                    compactOrderIndices.add(encoded < 0 ? -(i + 1) : (i + 1));
                    streamingSymbolTableIndices[i] = orderByColumn;
                    chainTypes.add(orderByColumnType);
                }

                chainTypeIndex = orderByCount;
                recordValueSink = RecordValueSinkFactory.getInstance(sqlGenerator.getAsm(), metadata, listColumnFilter);
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(orderByMetadata, compactOrderIndices);
                this.rankMaps = SortKeyEncoder.createRankMaps(orderByMetadata, compactOrderIndices);
                if (liveView) {
                    // Capture the chain-prefix types (the compacted ORDER BY columns) before the
                    // rank/count/lastActivityTs/tombstone slots get appended below. Snapshot/restore
                    // reads those chain bytes back via this typed slice so the live-view checkpoint can
                    // rehydrate the recordComparator's stored "last key image".
                    chainColumnTypes = new ArrayColumnTypes();
                    for (int i = 0, size = chainTypes.getColumnCount(); i < size; i++) {
                        chainColumnTypes.add(chainTypes.getColumnType(i));
                    }
                }
                chainTypes.add(ColumnType.LONG); // rank
                chainTypes.add(ColumnType.LONG); // count
                if (liveView) {
                    chainTypes.add(ColumnType.LONG); // lastActivityTs (live view)
                    chainTypes.add(ColumnType.BYTE); // tombstone (anchor compaction)
                    lastActivityTsValueIndex = chainTypeIndex + 2;
                    tombstoneValueIndex = chainTypeIndex + 3;
                    // Caller reuses the chainTypes buffer across window functions in the
                    // same query; take our own copy so {@link #evictStalePartitionState}
                    // can allocate a scratch Map with the exact same value layout.
                    mapValueTypes = new ArrayColumnTypes();
                    mapValueTypes.addAll(chainTypes);
                }
                // Lazy: reopen() allocates the backing after setMemoryTracker() binds
                // the per-query tracker, keeping malloc/free on the per-query counter.
                this.map = MapFactory.createUnorderedMap(
                        configuration,
                        keyColumnTypes,
                        chainTypes,
                        false,
                        false
                );
            } else { // for CachedWindowRecordCursorFactory
                map = MapFactory.createUnorderedMap(
                        configuration,
                        keyColumnTypes,
                        RANK_COLUMN_TYPES,
                        false,
                        false
                );
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, orderIndices);
                this.rankMaps = SortKeyEncoder.createRankMaps(metadata, orderIndices);
            }
        }

        @Override
        public void onSnapshotRestoreBegin() {
            map.clear();
            tombstoneCount = 0;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long count;
            if (mapValue.isNew()) {
                rank = 1;
                count = 1;
            } else {
                long lastOffset = mapValue.getLong(0);
                rank = mapValue.getLong(1);
                count = mapValue.getLong(2);
                recordComparator.setLeft(record);
                if (recordComparator.compare(spi.getRecordAt(lastOffset)) != 0) {
                    rank = dense ? rank + 1 : count;
                }
            }

            mapValue.putLong(0, recordOffset);
            mapValue.putLong(1, rank);
            mapValue.putLong(2, count + 1);
            Unsafe.putLong(spi.getAddress(recordOffset, columnIndex), rank);
        }

        @Override
        public void reopen() {
            if (map != null) {
                map.reopen();
            }
            rank = 0;
            sizeAfterLastEvict = 0;
            tombstoneCount = 0;
        }

        @Override
        public void reset() {
            Misc.free(map);
            compactionScratch = Misc.free(compactionScratch);
            Misc.freeObjListAndKeepObjects(rankMaps);
            rank = 0;
            sizeAfterLastEvict = 0;
        }

        @Override
        public long restorePartitionState(MemoryR source, long offset, MapValue value, int formatVersion) {
            offset = LiveViewSnapshotKeyCodec.readValueSlots(value, 0, source, offset, chainColumnTypes);
            value.putLong(chainTypeIndex, source.getLong(offset));
            offset += Long.BYTES;
            value.putLong(chainTypeIndex + 1, source.getLong(offset));
            offset += Long.BYTES;
            if (lastActivityTsValueIndex >= 0) {
                value.putLong(lastActivityTsValueIndex, source.getLong(offset));
                offset += Long.BYTES;
            }
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
            if (map != null) {
                map.setMemoryTracker(tracker);
            }
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
            LiveViewSnapshotKeyCodec.writeKey(sink, value, chainColumnTypes, 0);
            sink.putLong(value.getLong(chainTypeIndex));
            sink.putLong(value.getLong(chainTypeIndex + 1));
            if (lastActivityTsValueIndex >= 0) {
                sink.putLong(value.getLong(lastActivityTsValueIndex));
            }
        }

        @Override
        public boolean supportsSnapshot() {
            return liveView
                    && chainColumnTypes != null
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(keyColumnTypes)
                    && LiveViewSnapshotKeyCodec.isAllTypesSupported(chainColumnTypes);
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(getName());
            sink.val("()");
            sink.val(" over (");
            sink.val("partition by ");
            sink.val(partitionByRecord.getFunctions());
            sink.val(')');
        }

        @Override
        public void toTop() {
            super.toTop();
            Misc.clear(map);
            rank = 0;
            sizeAfterLastEvict = 0;
            tombstoneCount = 0;
        }
    }

    static {
        RANK_COLUMN_TYPES = new ArrayColumnTypes();
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // offset
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // rank
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // count
    }
}
