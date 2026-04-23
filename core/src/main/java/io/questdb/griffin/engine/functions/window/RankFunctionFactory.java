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
import io.questdb.cairo.EntityColumnFilter;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.Reopenable;
import io.questdb.cairo.SingleRecordSink;
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
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;

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
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
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
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), RANK_CONST);
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
        private final String name;
        private final VirtualRecord partitionByRecord;
        private final RecordSink partitionBySink;
        private final int tsColumnIndex;
        // Value-layout index of the lastActivityTs slot (live view Phase 5). Lives at
        // chainTypeIndex + 2, i.e. one slot past the existing rank/count pair.
        private int chainTypeIndex;
        private int columnIndex;
        private int lastActivityTsValueIndex;
        private Map map;
        private ArrayColumnTypes mapValueTypes;
        private long rank;
        private ObjList<DirectIntList> rankMaps;
        private RecordComparator recordComparator;
        private RecordValueSink recordValueSink;
        private long sizeAfterLastEvict;

        public RankOverPartitionFunction(ColumnTypes keyColumnTypes,
                                         VirtualRecord partitionByRecord,
                                         RecordSink partitionBySink,
                                         int tsColumnIndex,
                                         CairoConfiguration configuration,
                                         boolean dense,
                                         String name) {
            this.partitionByRecord = partitionByRecord;
            this.partitionBySink = partitionBySink;
            // The WindowContext's partitionByKeyTypes is a transient buffer owned by
            // SqlCodeGenerator that gets cleared on every window function compile. Phase 5
            // partition eviction needs to allocate a scratch Map with the same key shape
            // after compilation has moved on, so take our own copy.
            ArrayColumnTypes keyTypesCopy = new ArrayColumnTypes();
            for (int i = 0, n = keyColumnTypes.getColumnCount(); i < n; i++) {
                keyTypesCopy.add(keyColumnTypes.getColumnType(i));
            }
            this.keyColumnTypes = keyTypesCopy;
            this.tsColumnIndex = tsColumnIndex;
            this.configuration = configuration;
            this.dense = dense;
            this.name = name;
        }

        @Override
        public void close() {
            super.close();
            Misc.free(map);
            Misc.freeObjList(partitionByRecord.getFunctions());
            Misc.freeObjList(rankMaps);
        }

        @Override
        public void computeNext(Record record) {
            partitionByRecord.of(record);
            MapKey key = map.withKey();
            key.put(partitionByRecord, partitionBySink);
            MapValue mapValue = key.createValue();
            long count;
            if (mapValue.isNew()) {
                rank = 1;
                count = 1;
            } else {
                rank = mapValue.getLong(chainTypeIndex);
                count = mapValue.getLong(chainTypeIndex + 1);
                recordComparator.setLeft(mapValue);
                if (recordComparator.compare(record) != 0) {
                    rank = dense ? rank + 1 : count;
                }
            }
            recordValueSink.copy(record, mapValue);
            mapValue.putLong(chainTypeIndex, rank);
            mapValue.putLong(chainTypeIndex + 1, count + 1);
            // Track per-key last-activity-ts for live view retention-driven eviction.
            // tsColumnIndex is -1 when the window is defined over a source with no
            // designated timestamp; writing Long.MIN_VALUE keeps those keys below any
            // eviction cutoff, so they never get evicted (live views always have one).
            mapValue.putLong(lastActivityTsValueIndex,
                    tsColumnIndex >= 0 ? record.getTimestamp(tsColumnIndex) : Long.MIN_VALUE);
        }

        @Override
        public void evictStalePartitionState(long cutoffTs) {
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
            if (orderIndices == null) {
                orderIndices = sqlGenerator.toOrderIndices(metadata, orderBy, orderByDirection);
            }

            if (chainTypes.getColumnCount() == 0) { // for WindowRecordCursorFactory
                EntityColumnFilter entityColumnFilter = sqlGenerator.getEntityColumnFilter();
                for (int i = 0, size = metadata.getColumnCount(); i < size; i++) {
                    chainTypes.add(metadata.getColumnType(i));
                }

                chainTypeIndex = metadata.getColumnCount();
                entityColumnFilter.of(chainTypeIndex);
                recordValueSink = RecordValueSinkFactory.getInstance(sqlGenerator.getAsm(), chainTypes, entityColumnFilter);
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, orderIndices);
                this.rankMaps = SortKeyEncoder.createRankMaps(metadata, orderIndices);
                chainTypes.add(ColumnType.LONG); // rank
                chainTypes.add(ColumnType.LONG); // count
                chainTypes.add(ColumnType.LONG); // lastActivityTs (live view Phase 5)
                lastActivityTsValueIndex = chainTypeIndex + 2;
                // Caller reuses the chainTypes buffer across window functions in the
                // same query; take our own copy so {@link #evictStalePartitionState}
                // can allocate a scratch Map with the exact same value layout.
                mapValueTypes = new ArrayColumnTypes();
                mapValueTypes.addAll(chainTypes);
                this.map = MapFactory.createUnorderedMap(
                        configuration,
                        keyColumnTypes,
                        chainTypes
                );
            } else { // for CachedWindowRecordCursorFactory
                map = MapFactory.createUnorderedMap(
                        configuration,
                        keyColumnTypes,
                        RANK_COLUMN_TYPES
                );
                this.recordComparator = sqlGenerator.getRecordComparatorCompiler().newInstance(metadata, orderIndices);
                this.rankMaps = SortKeyEncoder.createRankMaps(metadata, orderIndices);
            }
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
            Unsafe.getUnsafe().putLong(spi.getAddress(recordOffset, columnIndex), rank);
        }

        @Override
        public void reopen() {
            if (map != null) {
                map.reopen();
            }
            rank = 0;
            sizeAfterLastEvict = 0;
        }

        @Override
        public void reset() {
            Misc.free(map);
            Misc.freeObjListAndKeepObjects(rankMaps);
            rank = 0;
            sizeAfterLastEvict = 0;
        }

        @Override
        public void setColumnIndex(int columnIndex) {
            this.columnIndex = columnIndex;
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
        }
    }

    static {
        RANK_COLUMN_TYPES = new ArrayColumnTypes();
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // offset
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // rank
        RANK_COLUMN_TYPES.add(ColumnType.LONG); // count
    }
}
