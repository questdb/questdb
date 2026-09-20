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
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.TestOnly;

/**
 * Owns execution backing; functions, filter context and the build factory are borrowed
 * from the factory. Each acquired slot owns every mutable probe/record/decoder/aggregate
 * view. init() builds on the owner once the probe frame cursor is open, because SYMBOL
 * keys translate through the probe's symbol tables. The build cursor stays open until
 * clear(), because build SYMBOL payloads resolve through its symbol tables. The frozen
 * build is published by UnorderedPageFrameSequence before reducers run. clear()
 * requires all reducers to have finished and output consumers to be done.
 */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    private final RecordCursorFactory buildFactory;
    private final int buildKeyColumn;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    // Null for INT keys.
    private final SymbolKeyTranslator keyTranslator;
    private final boolean outer;
    private final PerWorkerLocks perWorkerLocks;
    private final int probeKeyColumn;
    private final ObjList<HashJoinGroupByRecord> records = new ObjList<>();
    private final ObjList<Slot> slots = new ObjList<>();
    private IntHashJoinBuild build;
    private RecordCursor buildCursor;
    private FrozenHashJoinBuild.IntKeyed frozen;
    private boolean isBuildUnique;
    private boolean functionsInitialized;
    private boolean filtersInitialized;
    private long pairsPerCheck;
    private GroupByShardingContext shardingContext;

    AsyncHashJoinGroupByAtom(
            CairoEngine engine,
            RecordCursorFactory buildFactory,
            HashJoinGroupByMetadata metadata,
            HashJoinGroupByFunctions functions,
            AsyncFilterContext filterContext,
            boolean outer,
            int workerCount
    ) {
        this.buildFactory = buildFactory;
        this.functions = functions;
        this.filterContext = filterContext;
        this.outer = outer;
        this.probeKeyColumn = metadata.getProbeKeyColumn();
        this.buildKeyColumn = metadata.getBuildKeyColumn();
        this.keyTranslator = metadata.isSymbolKey() ? new SymbolKeyTranslator() : null;
        CairoConfiguration configuration = engine.getConfiguration();
        perWorkerLocks = new PerWorkerLocks(configuration, workerCount);
        try {
            build = new IntHashJoinBuild(metadata.getPayloadMetadata(), metadata.getBuildColumns(),
                    64, 64, true);
            if (functions.isKeyed()) {
                ObjList<GroupByFunctionsUpdater> workerUpdaters = new ObjList<>();
                for (int i = 0; i < workerCount; i++) {
                    workerUpdaters.add(functions.getUpdater(i));
                }
                shardingContext = new GroupByShardingContext(configuration, functions.getKeyTypes(),
                        functions.getValueTypes(), functions.getUpdater(-1), workerUpdaters,
                        perWorkerLocks, workerCount);
            }
            for (int i = -1; i < workerCount; i++) {
                Slot slot = new Slot(metadata.newRecord());
                if (!functions.isKeyed()) {
                    slot.value = new SimpleMapValue(functions.getValueTypes().getColumnCount());
                }
                slots.add(slot);
                records.add(slot.joinedRecord);
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
                slots.getQuick(i).clear(functions.getUpdater(i - 1));
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
        isBuildUnique = false;
        failure = Misc.freeBestEffort(failure, build);
        failure = Misc.freeBestEffort(failure, keyTranslator);
        // Functions, slots and the build have released every symbol table view of this cursor.
        failure = Misc.freeBestEffort(failure, buildCursor);
        buildCursor = null;
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

    /** The build published for the open cursor, or null when no cursor is open. */
    @TestOnly
    public FrozenHashJoinBuild getFrozenBuild() {
        return frozen;
    }

    public HashJoinGroupByFunctions getFunctions() {
        return functions;
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        try {
            assert frozen == null && buildCursor == null;
            build(symbolTableSource, executionContext);
            // Join fanout is not bounded by a frame, so reducers check once per page frame of matched pairs.
            pairsPerCheck = Math.max(1, executionContext.getPageFrameMaxRows());
            if (shardingContext != null) {
                shardingContext.setMemoryTracker(executionContext.getMemoryTracker());
                shardingContext.reopen();
            }
            for (int i = 0; i < slots.size(); i++) {
                Slot slot = slots.getQuick(i);
                if (!functions.isKeyed()) {
                    functions.getUpdater(i - 1).updateEmpty(slot.value);
                    slot.value.setNew(true);
                }
                if (slot.probe == null) {
                    slot.probe = frozen.newProbe();
                } else {
                    slot.probe.reopen();
                }
                slot.probeRecord.of(symbolTableSource);
                slot.joinedRecord.of(slot.probeRecord, slot.probeRecord, slot.probe);
            }
            filtersInitialized = true;
            filterContext.initFilters(symbolTableSource, executionContext);
            functionsInitialized = true;
            functions.init(records, executionContext);
        } catch (Throwable th) {
            // The sequence closes the frame cursor when init throws. Release functions
            // while their borrowed symbol sources are still alive, then the build cursor.
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

    // The caller's failure path closes the build, the translator and the build cursor.
    private void build(SymbolTableSource probeSymbols, SqlExecutionContext executionContext) throws SqlException {
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        final SqlExecutionCircuitBreaker circuitBreaker = executionContext.getCircuitBreaker();
        buildCursor = buildFactory.getCursor(executionContext);
        // The child cursor is fresh. Unknown/filtered sizes retain incremental growth.
        final long rowCountHint = buildCursor.size();
        build.open(memoryTracker, circuitBreaker);
        if (keyTranslator != null) {
            keyTranslator.of(
                    (StaticSymbolTable) probeSymbols.newSymbolTable(probeKeyColumn),
                    (StaticSymbolTable) buildCursor.newSymbolTable(buildKeyColumn),
                    memoryTracker,
                    circuitBreaker
            );
        }
        frozen = build.build(buildCursor, buildKeyColumn, rowCountHint, keyTranslator);
        if (keyTranslator != null) {
            // Translation ends with the build; do not hold the cache while probing.
            keyTranslator.close();
        }
        isBuildUnique = frozen.getRowCount() == frozen.getKeyCount();
    }

    AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    GroupByMapFragment getFragment(int slot) {
        return shardingContext != null ? shardingContext.getFragment(slot) : null;
    }

    long getPairsPerCheck() {
        return pairsPerCheck;
    }

    int getProbeKeyColumn() {
        return probeKeyColumn;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    boolean isBuildUnique() {
        return isBuildUnique;
    }

    boolean isOuter() {
        return outer;
    }

    int maybeAcquire(int workerId, boolean owner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && owner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    GroupByShardingContext getShardingContext() {
        return shardingContext;
    }

    public boolean isSharded() {
        return shardingContext != null && shardingContext.isSharded();
    }

    SimpleMapValue mergeScalar() {
        SimpleMapValue dest = getSlot(-1).value;
        GroupByFunctionsUpdater updater = functions.getUpdater(-1);
        for (int i = 1; i < slots.size(); i++) {
            SimpleMapValue src = slots.getQuick(i).value;
            if (!src.isNew()) {
                if (dest.isNew()) {
                    dest.copy(src);
                } else {
                    updater.merge(dest, src);
                }
                dest.setNew(false);
            }
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
        final HashJoinGroupByRecord joinedRecord;
        final ProbeRecord probeRecord = new ProbeRecord();
        FrozenHashJoinBuild.IntProbe probe;
        SimpleMapValue value;

        Slot(HashJoinGroupByRecord joinedRecord) {
            this.joinedRecord = joinedRecord;
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, value);
            value = null;
            failure = Misc.freeBestEffort(failure, probe);
            probe = null;
            failure = Misc.freeBestEffort(failure, probeRecord);
            CairoException.rethrowCleanupFailure(failure);
        }

        void clear(GroupByFunctionsUpdater updater) {
            if (value != null) {
                updater.updateEmpty(value);
                value.setNew(true);
            }
            joinedRecord.clear();
            probeRecord.of(null);
            if (probe != null) {
                // A probe may hold native memory charged to this execution's tracker, so it
                // releases here, while that tracker is still the one that charged it. The
                // object stays: reopen() brings it back for the next execution.
                probe.close();
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
