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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapKey;
import io.questdb.cairo.map.MapRecord;
import io.questdb.cairo.map.MapRecordCursor;
import io.questdb.cairo.map.MapValue;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SqlExecutionCircuitBreakerWrapper;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns execution backing; functions and filter context are borrowed from the factory.
 * Each acquired slot owns every mutable probe/record/decoder/aggregate view. The
 * frozen build is published by UnorderedPageFrameSequence before reducers run.
 * clear() requires all reducers to have finished and output consumers to be done.
 */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private final int buildKeyColumn;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    private final boolean outer;
    private final PerWorkerLocks perWorkerLocks;
    private final int probeKeyColumn;
    private final ObjList<HashJoinGroupByRecord> records = new ObjList<>();
    private final ObjList<Slot> slots = new ObjList<>();
    private IntHashJoinBuild build;
    private FrozenHashJoinBuild frozen;
    private boolean functionsInitialized;
    private boolean filtersInitialized;
    private GroupByShardingContext shardingContext;

    AsyncHashJoinGroupByAtom(
            CairoEngine engine,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean outer,
            int workerCount
    ) {
        this.functions = functions;
        this.filterContext = filterContext;
        this.outer = outer;
        this.probeKeyColumn = metadata.getProbeKeyColumn();
        this.buildKeyColumn = metadata.getBuildKeyColumn();
        CairoConfiguration configuration = engine.getConfiguration();
        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        try {
            build = new IntHashJoinBuild(metadata.getPayloadMetadata(), metadata.getBuildColumns(),
                    64, 64);
            ObjList<GroupByFunctionsUpdater> workerUpdaters = new ObjList<>();
            for (int i = 0; i < workerCount; i++) {
                workerUpdaters.add(functions.getUpdater(i));
            }
            shardingContext = new GroupByShardingContext(configuration, functions.getKeyTypes(),
                    functions.getValueTypes(), functions.getUpdater(-1), workerUpdaters,
                    perWorkerLocks, workerCount);
            for (int i = -1; i < workerCount; i++) {
                Slot slot = new Slot(engine, metadata.newRecord());
                slots.add(slot);
                records.add(slot.joinedRecord);
            }
            // This reducer filters one logical row at a time and needs no row-id buffers.
            // Release the context's eager buffers; only decoder pools allocate on execution.
            for (int i = -1; i < workerCount; i++) {
                filterContext.getFilteredRows(i).close();
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        Throwable failure = null;
        if (functionsInitialized) {
            functionsInitialized = false;
            try {
                functions.cursorClosed();
            } catch (Throwable th) {
                failure = th;
            }
        }
        if (filtersInitialized) {
            filtersInitialized = false;
            Function ownerFilter = filterContext.getFilter(-1);
            failure = cursorClosed(failure, ownerFilter);
            for (int i = 0; i < slots.size() - 1; i++) {
                Function workerFilter = filterContext.getFilter(i);
                if (workerFilter != ownerFilter) {
                    failure = cursorClosed(failure, workerFilter);
                }
            }
        }
        for (int i = 0; i < slots.size(); i++) {
            try {
                slots.getQuick(i).clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        try {
            filterContext.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        if (shardingContext != null) {
            try {
                shardingContext.clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        frozen = null;
        failure = Misc.freeBestEffort(failure, build);
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = null;
        try {
            clear();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        failure = Misc.freeBestEffort(failure, shardingContext);
        shardingContext = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        try {
            assert frozen != null;
            shardingContext.setMemoryTracker(executionContext.getMemoryTracker());
            for (int i = 0; i < slots.size(); i++) {
                Slot slot = slots.getQuick(i);
                slot.breaker.init(executionContext.getCircuitBreaker());
                slot.probe = frozen.newProbe(slot.breaker);
                slot.probeRecord.of(symbolTableSource);
                slot.joinedRecord.of(slot.probeRecord, slot.probeRecord, slot.probe);
            }
            filtersInitialized = true;
            filterContext.initFilters(symbolTableSource, executionContext);
            functionsInitialized = true;
            functions.init(records, executionContext);
        } catch (Throwable th) {
            // The sequence closes the frame cursor when init throws. Release
            // functions while their borrowed symbol sources are still alive.
            try {
                clear();
            } catch (Throwable cleanup) {
                th.addSuppressed(cleanup);
            }
            throw th;
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable th) {
        if (failure == null) {
            return th;
        }
        if (failure != th) {
            failure.addSuppressed(th);
        }
        return failure;
    }

    private static Throwable cursorClosed(Throwable failure, Function function) {
        if (function != null) {
            try {
                function.cursorClosed();
            } catch (Throwable th) {
                return addFailure(failure, th);
            }
        }
        return failure;
    }

    void build(RecordCursor cursor, SqlExecutionContext executionContext) {
        build.open(executionContext.getMemoryTracker(), executionContext.getCircuitBreaker());
        frozen = build.build(cursor, buildKeyColumn);
    }

    AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    GroupByMapFragment getFragment(int slot) {
        return shardingContext.getFragment(slot);
    }

    HashJoinGroupByFunctions getFunctions() {
        return functions;
    }

    int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    boolean isOuter() {
        return outer;
    }

    int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && owner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    /** Task 4 uses an interruptible owner merge. Sharded merging is task 5. */
    Map merge(SqlExecutionCircuitBreaker breaker) {
        Map dest = getFragment(-1).reopenMap();
        GroupByFunctionsUpdater updater = functions.getUpdater(-1);
        for (int i = 0; i < slots.size() - 1; i++) {
            Map source = getFragment(i).getMap();
            if (source.size() > 0) {
                MapRecordCursor cursor = source.getCursor();
                MapRecord record = cursor.getRecord();
                while (cursor.hasNext()) {
                    breaker.statefulThrowExceptionIfTrippedTimeThrottled();
                    MapKey key = dest.withKey();
                    record.copyToKey(key);
                    MapValue value = key.createValue();
                    if (value.isNew()) {
                        record.copyValue(value);
                    } else {
                        updater.merge(value, record.getValue());
                    }
                }
            }
            source.close();
        }
        return dest;
    }

    void release(int slot) {
        perWorkerLocks.releaseSlot(slot);
    }

    boolean shouldProbe() {
        return outer || frozen.getRowCount() > 0;
    }

    static final class Slot implements QuietCloseable {
        final SqlExecutionCircuitBreakerWrapper breaker;
        final HashJoinGroupByRecord joinedRecord;
        final ProbeRecord probeRecord = new ProbeRecord();
        FrozenHashJoinBuild.Probe probe;

        Slot(CairoEngine engine, HashJoinGroupByRecord joinedRecord) {
            this.joinedRecord = joinedRecord;
            breaker = new SqlExecutionCircuitBreakerWrapper(engine, engine.getConfiguration().getCircuitBreakerConfiguration());
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, probeRecord);
            failure = Misc.freeBestEffort(failure, breaker);
            CairoException.rethrowCleanupFailure(failure);
        }

        void clear() {
            joinedRecord.clear();
            probe = null;
            try {
                probeRecord.of(null);
            } finally {
                breaker.clear();
            }
        }
    }

    private static final class ProbeRecord extends PageFrameMemoryRecord implements SymbolTableSource {
        private SymbolTableSource source;

        ProbeRecord() {
            super(RECORD_A_LETTER);
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return super.getSymbolTable(columnIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return source.newSymbolTable(columnIndex);
        }

        @Override
        public void of(SymbolTableSource source) {
            super.of(source);
            this.source = source;
        }
    }
}
