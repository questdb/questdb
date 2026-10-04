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

package io.questdb.griffin.engine.join;

import io.questdb.MessageBus;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.map.OrderedMap;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.PageFrameReduceTaskFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.table.AsyncFilteredRecordCursorFactory;
import io.questdb.griffin.engine.table.SymbolTranslatingRecord;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntHashSet;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * An inner hash join whose build (slave) side is unique on the join key, such as a GROUP BY on
 * that key, probed in parallel: the master's page frames go through an
 * {@link AsyncFilteredRecordCursorFactory} whose filter, on the shared query workers, keeps the
 * master rows whose key the build holds. With a unique build every kept row has exactly one match,
 * so the join's output is the kept master rows in master order, which is the order
 * {@link HashJoinLightRecordCursorFactory} emits them in, with the slave record positioned on the
 * query's thread only when a consumer reads a slave column. A projection of master columns alone, a
 * semi-join, never touches the slave.
 * <p>
 * The build is {@link HashJoinLightRecordCursorFactory}'s: a map from the key to a chain of slave
 * row ids, built on the query's thread before any frame is dispatched, with symbol keys translated
 * into the master's symbol domain. A slave symbol the master does not have can match no master row,
 * so its rows are left out. As in the light hash join, the slave's cursor opens before the
 * master's, and the build's frames decode scattered.
 * <p>
 * <b>Uniqueness.</b> The planner chooses this factory only when it can prove the build unique on
 * the join key, but the proof is a performance hint, not what makes the output right. The build
 * counts each key's rows as it goes, and when a key repeats the cursor walks each kept row's chain
 * exactly as the light hash join does, so a wrong proof costs speed (a second probe per kept row,
 * on the query's thread), never rows.
 * <p>
 * A kept row's slave is found by probing the map again on the query's thread: the workers' probe
 * keeps no per-row result. That second probe runs only for the rows whose slave columns are read.
 */
public class AsyncHashJoinLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final int ROWS_PER_BREAKER_CHECK = 64 * 1024;
    private final Build build;
    private final int columnSplit;
    private final JoinCursor cursor;
    private final ObjList<ProbeFilter> filters = new ObjList<>();
    private final JoinContext joinContext;
    private final RecordCursorFactory master;
    private final int workerCount;
    private AsyncFilteredRecordCursorFactory filterFactory;
    private RecordCursorFactory slave;

    /**
     * Takes ownership of {@code master} and {@code slave} as soon as it is entered: a throw frees
     * them.
     *
     * @param masterSinks one master key sink per worker plus one for the query's thread, last;
     *                    sinks must not be shared across threads
     */
    public AsyncHashJoinLightRecordCursorFactory(
            @NotNull CairoEngine engine,
            @NotNull CairoConfiguration configuration,
            @NotNull MessageBus messageBus,
            @NotNull RecordMetadata metadata,
            @NotNull RecordCursorFactory master,
            @NotNull RecordCursorFactory slave,
            @Transient @NotNull ColumnTypes joinColumnTypes,
            @NotNull ObjList<RecordSink> masterSinks,
            @NotNull RecordSink slaveKeySink,
            int columnSplit,
            @NotNull JoinContext joinContext,
            int @Nullable [] masterSymbolKeyColumnIndices,
            int @Nullable [] slaveSymbolKeyColumnIndices,
            @NotNull IntHashSet masterKeyColumns,
            @NotNull ExpressionNode filterExpr,
            @NotNull PageFrameReduceTaskFactory reduceTaskFactory,
            int workerCount
    ) {
        super(metadata);
        this.master = master;
        this.slave = slave;
        this.columnSplit = columnSplit;
        this.joinContext = joinContext;
        this.workerCount = workerCount;
        Build build = null;
        AsyncFilteredRecordCursorFactory filterFactory = null;
        ProbeFilter ownerFilter = null;
        try {
            final ArrayColumnTypes valueTypes = new ArrayColumnTypes();
            valueTypes.add(io.questdb.cairo.ColumnType.INT); // chain head
            valueTypes.add(io.questdb.cairo.ColumnType.INT); // rows of the key
            build = new Build(configuration, joinColumnTypes, valueTypes, slaveKeySink, masterSymbolKeyColumnIndices,
                    slaveSymbolKeyColumnIndices, Math.max(master.getMetadata().getColumnCount(), slave.getMetadata().getColumnCount()));
            ownerFilter = new ProbeFilter(build, masterSinks.getQuick(workerCount), true);
            filters.add(ownerFilter);
            final ObjList<Function> workerFilters = new ObjList<>(workerCount);
            for (int i = 0; i < workerCount; i++) {
                final ProbeFilter workerFilter = new ProbeFilter(build, masterSinks.getQuick(i), false);
                filters.add(workerFilter);
                workerFilters.add(workerFilter);
            }
            filterFactory = new AsyncFilteredRecordCursorFactory(
                    engine,
                    configuration,
                    messageBus,
                    master,
                    ownerFilter,
                    masterKeyColumns,
                    reduceTaskFactory,
                    workerFilters,
                    filterExpr,
                    null,
                    0,
                    workerCount,
                    false
            );
            this.cursor = new JoinCursor(build, masterSinks.getQuick(workerCount));
        } catch (Throwable th) {
            if (filterFactory != null) {
                Misc.free(filterFactory, th);
            } else {
                Misc.freeObjList(filters, th);
                Misc.free(master, th);
            }
            Misc.free(build, th);
            Misc.free(slave, th);
            throw th;
        }
        this.build = build;
        this.filterFactory = filterFactory;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return master.followedOrderByAdvice();
    }

    @TestOnly
    public int getAcquiredSlotCount() {
        final PerWorkerLocks locks = filterFactory.getAtom().getPerWorkerLocks();
        return locks != null ? locks.getAcquiredSlotCount() : 0;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        try {
            // the slave first, as the light hash join opens it: a self-join reads the same
            // snapshots as the serial plan
            build.open(slave, executionContext);
            // the owner's filter builds the hash table when the filter initializes, before any
            // frame is dispatched, see ProbeFilter.init()
            final RecordCursor masterCursor = filterFactory.getCursor(executionContext);
            cursor.of(masterCursor);
            return cursor;
        } catch (Throwable th) {
            if (cursor.isOpen) {
                Misc.free(cursor, th);
            } else {
                Misc.free(build, th);
            }
            throw th;
        }
    }

    @TestOnly
    public boolean isLastBuildUnique() {
        return build.unique;
    }

    @Override
    public int getScanDirection() {
        return master.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return master.getTableToken();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return master.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Async Hash Join Light");
        sink.meta("workers").val(workerCount);
        sink.attr("condition").val(joinContext);
        if (build.translatingRecord != null) {
            sink.attr("symbolKeyJoin").val(true);
        }
        sink.child(master);
        sink.child("Hash", slave);
    }

    @Override
    public boolean usesCompiledFilter() {
        return false;
    }

    @Override
    public boolean usesExternalDataSource() {
        return master.usesExternalDataSource() || (slave != null && slave.usesExternalDataSource());
    }

    @Override
    protected void _close() {
        final AsyncFilteredRecordCursorFactory filterFactory = this.filterFactory;
        this.filterFactory = null;
        final RecordCursorFactory slave = this.slave;
        this.slave = null;
        Throwable failure = Misc.freeIfCloseableBestEffort(null, detachMetadata());
        failure = Misc.freeBestEffort(failure, cursor);
        // owns the master and the probe filters
        failure = Misc.freeBestEffort(failure, filterFactory);
        failure = Misc.freeBestEffort(failure, build);
        failure = Misc.freeBestEffort(failure, slave);
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * The build side of one execution: the slave cursor, kept open for the slave record, the key
     * map and the row id chain. Built on the query's thread by the owner's probe filter, frozen
     * before the first frame is dispatched, and released when the cursor closes.
     */
    private static class Build implements java.io.Closeable {
        private final LongChain chain;
        private final OrderedMap map;
        private final int @Nullable [] masterSymbolKeyColumnIndices;
        private final RecordSink slaveKeySink;
        private final int @Nullable [] slaveSymbolKeyColumnIndices;
        @Nullable
        private final SymbolTranslatingRecord translatingRecord;
        private SqlExecutionContext executionContext;
        private boolean isBuilt;
        private MemoryTracker memoryTracker;
        private RecordCursor slaveCursor;
        private boolean unique;

        private Build(
                CairoConfiguration configuration,
                ColumnTypes keyTypes,
                ColumnTypes valueTypes,
                RecordSink slaveKeySink,
                int @Nullable [] masterSymbolKeyColumnIndices,
                int @Nullable [] slaveSymbolKeyColumnIndices,
                int maxColumnCount
        ) {
            this.slaveKeySink = slaveKeySink;
            this.masterSymbolKeyColumnIndices = masterSymbolKeyColumnIndices;
            this.slaveSymbolKeyColumnIndices = slaveSymbolKeyColumnIndices;
            this.map = new OrderedMap(
                    configuration.getSqlSmallMapPageSize(),
                    keyTypes,
                    valueTypes,
                    configuration.getSqlSmallMapKeyCapacity(),
                    configuration.getSqlFastMapLoadFactor(),
                    configuration.getSqlMapMaxResizes(),
                    false
            );
            this.chain = new LongChain(configuration.getSqlHashJoinLightValuePageSize(), configuration.getSqlHashJoinLightValueMaxPages(), true);
            this.translatingRecord = masterSymbolKeyColumnIndices != null
                    ? new SymbolTranslatingRecord(configuration, maxColumnCount, masterSymbolKeyColumnIndices.length)
                    : null;
        }

        @Override
        public void close() {
            isBuilt = false;
            Throwable failure = null;
            final RecordCursor slaveCursor = this.slaveCursor;
            this.slaveCursor = null;
            failure = Misc.freeBestEffort(failure, slaveCursor);
            failure = Misc.freeBestEffort(failure, map);
            failure = Misc.freeBestEffort(failure, chain);
            failure = Misc.freeBestEffort(failure, translatingRecord);
            executionContext = null;
            CairoException.rethrowCleanupFailure(failure);
        }

        // Runs on the query's thread, from the owner filter's init(), with the master's frames as
        // the symbol table source.
        void build(SymbolTableSource masterSymbolTableSource) throws SqlException {
            if (isBuilt) {
                return;
            }
            map.setMemoryTracker(memoryTracker);
            map.reopen();
            chain.setMemoryTracker(memoryTracker);
            chain.reopen();
            Record keyRecord = slaveCursor.getRecord();
            if (translatingRecord != null) {
                translatingRecord.of(slaveCursor.getRecord());
                translatingRecord.setMemoryTracker(memoryTracker);
                translatingRecord.initSources(slaveCursor, masterSymbolTableSource, slaveSymbolKeyColumnIndices, masterSymbolKeyColumnIndices);
                keyRecord = translatingRecord;
            }
            final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
            final Record record = slaveCursor.getRecord();
            boolean unique = true;
            long rows = 0;
            while (slaveCursor.hasNext()) {
                if ((++rows & (ROWS_PER_BREAKER_CHECK - 1)) == 0) {
                    circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                }
                if (translatingRecord != null) {
                    translatingRecord.resetNonExistentKeyFlag();
                }
                final MapKey key = map.withKey();
                key.put(keyRecord, slaveKeySink);
                if (translatingRecord != null && translatingRecord.hadNonExistentKey()) {
                    // a symbol the master lacks: no master row can match it
                    continue;
                }
                final MapValue value = key.createValue();
                if (value.isNew()) {
                    value.putInt(0, chain.put(record.getRowId(), -1));
                    value.putInt(1, 1);
                } else {
                    value.putInt(0, chain.put(record.getRowId(), value.getInt(0)));
                    value.addInt(1, 1);
                    unique = false;
                }
            }
            this.unique = unique;
            isBuilt = true;
        }

        // Opens the slave's cursor, on the query's thread, before the master opens; the owner's
        // filter builds from it once the master's frames are open, see build().
        void open(RecordCursorFactory slave, SqlExecutionContext executionContext) throws SqlException {
            this.executionContext = executionContext;
            this.memoryTracker = executionContext.getMemoryTracker();
            isBuilt = false;
            assert slaveCursor == null;
            slaveCursor = slave.getCursor(executionContext);
            // the probe positions the slave by row id, in master order
            slaveCursor.setParquetDecodeHint(ParquetDecodeHint.SCATTERED);
        }
    }

    /**
     * Keeps the master rows whose key the build holds. One per slot, each with its own probe view
     * and key sink; the owner's also builds the hash table.
     */
    private static class ProbeFilter extends BooleanFunction {
        private final Build build;
        private final boolean isOwner;
        private final RecordSink keySink;
        private final OrderedMap.ProbeView view = new OrderedMap.ProbeView();

        private ProbeFilter(Build build, RecordSink keySink, boolean isOwner) {
            this.build = build;
            this.keySink = keySink;
            this.isOwner = isOwner;
        }

        @Override
        public void close() {
            view.close();
        }

        @Override
        public boolean getBool(Record rec) {
            view.withKey();
            keySink.copy(rec, view);
            return view.findValue() != null;
        }

        @Override
        public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
            if (isOwner) {
                build.build(symbolTableSource);
            }
            view.setMemoryTracker(executionContext.getMemoryTracker());
            view.of(build.map);
        }

        @Override
        public boolean isThreadSafe() {
            return false;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val("hash join probe");
        }
    }

    private class JoinCursor implements NoRandomAccessRecordCursor, LazySlaveJoinRecord.SlavePositioner {
        private final Build build;
        private final RecordSink keySink;
        private final LazySlaveJoinRecord record;
        private final OrderedMap.ProbeView view = new OrderedMap.ProbeView();
        private LongChain.Cursor chainCursor;
        private boolean isOpen;
        private RecordCursor masterCursor;
        private Record masterRecord;
        private boolean slavePositioned;
        private Record slaveRecord;

        private JoinCursor(Build build, RecordSink keySink) {
            this.build = build;
            this.keySink = keySink;
            this.record = new LazySlaveJoinRecord(columnSplit, this);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (build.unique && chainCursor == null) {
                masterCursor.calculateSize(circuitBreaker, counter);
                return;
            }
            while (hasNext()) {
                counter.inc();
            }
        }

        @Override
        public void close() {
            if (isOpen) {
                isOpen = false;
                try {
                    // drains the workers before the build they probe goes
                    masterCursor = Misc.free(masterCursor);
                } finally {
                    masterRecord = null;
                    slaveRecord = null;
                    chainCursor = null;
                    view.close();
                    for (int i = 0, n = filters.size(); i < n; i++) {
                        filters.getQuick(i).view.close();
                    }
                    build.close();
                }
            }
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.getSymbolTable(columnIndex);
            }
            return build.slaveCursor.getSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public boolean hasNext() {
            if (build.unique) {
                slavePositioned = false;
                return masterCursor.hasNext();
            }
            // a key repeats: walk each kept row's chain, as the light hash join does
            if (chainCursor != null && chainCursor.hasNext()) {
                build.slaveCursor.recordAt(slaveRecord, chainCursor.next());
                return true;
            }
            while (masterCursor.hasNext()) {
                final MapValue value = find();
                if (value != null) {
                    chainCursor = build.chain.getCursor(value.getInt(0));
                    chainCursor.hasNext();
                    build.slaveCursor.recordAt(slaveRecord, chainCursor.next());
                    slavePositioned = true;
                    return true;
                }
            }
            return false;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            if (columnIndex < columnSplit) {
                return masterCursor.newSymbolTable(columnIndex);
            }
            return build.slaveCursor.newSymbolTable(columnIndex - columnSplit);
        }

        @Override
        public void positionSlave() {
            if (!slavePositioned) {
                final MapValue value = find();
                assert value != null && value.getInt(1) == 1;
                build.slaveCursor.recordAt(slaveRecord, build.chain.getCursor(value.getInt(0)).next());
                slavePositioned = true;
            }
        }

        @Override
        public long preComputedStateSize() {
            return masterCursor.preComputedStateSize();
        }

        @Override
        public long size() {
            return -1;
        }

        @Override
        public void toTop() {
            masterCursor.toTop();
            chainCursor = null;
            slavePositioned = false;
        }

        private MapValue find() {
            view.withKey();
            keySink.copy(masterRecord, view);
            return view.findValue();
        }

        void of(RecordCursor masterCursor) {
            this.masterCursor = masterCursor;
            this.isOpen = true;
            masterRecord = masterCursor.getRecord();
            slaveRecord = build.slaveCursor.getRecordB();
            record.of(masterRecord, slaveRecord);
            view.setMemoryTracker(build.memoryTracker);
            view.of(build.map);
            chainCursor = null;
            slavePositioned = false;
        }
    }
}
