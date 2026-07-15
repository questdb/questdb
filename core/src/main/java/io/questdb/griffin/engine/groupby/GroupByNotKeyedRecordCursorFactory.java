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

package io.questdb.griffin.engine.groupby;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.VirtualRecordNoRowid;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class GroupByNotKeyedRecordCursorFactory extends AbstractRecordCursorFactory {
    private RecordCursorFactory base;
    private GroupByNotKeyedRecordCursor cursor;
    private ObjList<GroupByFunction> groupByFunctions;
    private @Nullable ObjList<ObjList<Function>> sharedRecordFunctions;
    private SimpleMapValue value;
    private final VirtualRecord virtualRecordA;
    private ObjList<GroupByNotKeyedSharedCursor> sharedCursors;

    public GroupByNotKeyedRecordCursorFactory(
            @Transient @NotNull BytecodeAssembler asm,
            CairoConfiguration configuration,
            RecordCursorFactory base,
            RecordMetadata groupByMetadata,
            ObjList<GroupByFunction> groupByFunctions,
            int valueCount,
            @Nullable ObjList<ObjList<Function>> sharedRecordFunctions
    ) {
        super(groupByMetadata);
        try {
            this.value = new SimpleMapValue(valueCount);
            this.base = base;
            this.groupByFunctions = groupByFunctions;
            this.sharedRecordFunctions = sharedRecordFunctions;
            this.virtualRecordA = new VirtualRecordNoRowid(groupByFunctions);
            this.virtualRecordA.of(value);

            final GroupByFunctionsUpdater updater = GroupByFunctionsUpdaterFactory.getInstance(asm, groupByFunctions);
            boolean earlyExitSupported = GroupByUtils.isEarlyExitSupported(groupByFunctions);

            if (earlyExitSupported) {
                this.cursor = new EarlyExitGroupByNotKeyedRecordCursor(configuration, groupByFunctions, updater);
            } else {
                this.cursor = new GroupByNotKeyedRecordCursor(configuration, groupByFunctions, updater);
            }
        } catch (Throwable e) {
            Misc.free(this, e);
            throw e;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // The base cursor is shared with getSharedCursor and opened by whichever runs first, so
        // close the cursor on a breach only when this call opened it.
        if (cursor.baseCursor == null) {
            cursor.baseCursor = base.getCursor(executionContext);
            try {
                return cursor.of(cursor.baseCursor, executionContext);
            } catch (Throwable th) {
                cursor.close();
                throw th;
            }
        }
        // getSharedCursor opened cursor.baseCursor first (e.g. it sits on the build side of a
        // hash join, which opens before the probe side that holds this primary getCursor). cursor.of()
        // reopens the allocator and then runs Function.init, which can throw; guard it so the reopened
        // allocator is freed under the current per-query tracker instead of being deferred to factory
        // close, by which point the tracker may have been recycled to another query.
        try {
            return cursor.of(cursor.baseCursor, executionContext);
        } catch (Throwable th) {
            cursor.close();
            throw th;
        }
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) throws SqlException {
        final ObjList<ObjList<Function>> sharedRecordFunctions = this.sharedRecordFunctions;
        if (sharedRecordFunctions == null) {
            throw new UnsupportedOperationException();
        }
        if (sharedCursors == null) {
            sharedCursors = new ObjList<>();
        }
        int idx = sharedId - 1;
        GroupByNotKeyedSharedCursor shared = sharedCursors.getQuiet(idx);
        if (shared == null) {
            assert idx < sharedRecordFunctions.size();
            shared = new GroupByNotKeyedSharedCursor(cursor, sharedRecordFunctions.getQuick(idx), value);
            sharedCursors.extendAndSet(idx, shared);
        }
        boolean isNewCursor = cursor.baseCursor == null;
        if (isNewCursor) {
            cursor.baseCursor = base.getCursor(executionContext);
        }
        try {
            // The owner group-by functions must initialize before any shared consumer's clones,
            // regardless of open order: stateful functions inside aggregate arguments - such as
            // cursor comparisons caching a scalar sub-query result - run their expensive and
            // potentially nondeterministic initialization exactly once per query, in the owner,
            // and every consumer inherits that state. Shared consumers can open first (they sit
            // on the build side of the enclosing join), so trigger the owner setup here; the
            // primary getCursor skips the second initialization via areFunctionsInitialized.
            if (!cursor.areFunctionsInitialized) {
                cursor.of(cursor.baseCursor, executionContext);
            }
            // donate the owner state to the consumer's aligned clones before they initialize
            final ObjList<Function> sharedFunctions = sharedRecordFunctions.getQuick(idx);
            assert groupByFunctions.size() == sharedFunctions.size();
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                groupByFunctions.getQuick(i).offerStateTo(sharedFunctions.getQuick(i));
            }
            shared.of(cursor.baseCursor, executionContext);
            return shared;
        } catch (Throwable e) {
            if (isNewCursor) {
                // This call opened the base cursor and may have run the owner setup above. Close
                // the primary cursor outright - freeing the base cursor and the allocator under
                // the current per-query tracker, clearing the functions, and resetting
                // areFunctionsInitialized - so the next execution of this cached factory
                // re-initializes the functions instead of serving stale state. When the primary
                // opened the base cursor, its owner closes it.
                cursor.close();
            }
            throw e;
        }
    }

    /**
     * Returns true when this factory builds an early-exit non-keyed group-by
     * cursor, i.e. one that stops scanning the base cursor as soon as the
     * aggregate value is final (see {@link EarlyExitGroupByNotKeyedRecordCursor}).
     * That happens for aggregates such as {@code count_distinct} over a constant
     * or over a fully-enumerated symbol column, where reading further rows cannot
     * change the result.
     */
    public boolean isEarlyExitSupported() {
        return cursor instanceof EarlyExitGroupByNotKeyedRecordCursor;
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return false;
    }

    @Override
    public boolean supportsSharedCursors() {
        return sharedRecordFunctions != null;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("GroupBy");
        sink.meta("vectorized").val(false);
        sink.optAttr("values", groupByFunctions, true);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    @Override
    public boolean usesIndex() {
        return base.usesIndex();
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final GroupByNotKeyedRecordCursor cursor = this.cursor;
        this.cursor = null;
        final ObjList<GroupByFunction> groupByFunctions = this.groupByFunctions;
        this.groupByFunctions = null;
        final ObjList<GroupByNotKeyedSharedCursor> sharedCursors = this.sharedCursors;
        this.sharedCursors = null;
        final ObjList<ObjList<Function>> sharedRecordFunctions = this.sharedRecordFunctions;
        this.sharedRecordFunctions = null;
        final SimpleMapValue value = this.value;
        this.value = null;

        Throwable cleanupFailure = Misc.freeBestEffort(null, value);
        cleanupFailure = Misc.freeObjListBestEffort(cleanupFailure, groupByFunctions);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, base);
        cleanupFailure = Misc.freeBestEffort(cleanupFailure, cursor);
        cleanupFailure = GroupByRecordCursorFactory.freeSharedRecordFunctionsBestEffort(
                cleanupFailure,
                sharedRecordFunctions
        );
        // Shared cursors hold no native memory; primary state freed above covers it.
        Misc.clear(sharedCursors);
        CairoException.rethrowCleanupFailure(cleanupFailure);
    }

    private static class GroupByNotKeyedSharedCursor implements NoRandomAccessRecordCursor {
        private final ObjList<Function> groupByFunctions;
        private final GroupByNotKeyedRecordCursor primaryCursor;
        private final VirtualRecord record;
        private boolean isExhausted;

        GroupByNotKeyedSharedCursor(GroupByNotKeyedRecordCursor cursor, ObjList<Function> functions, SimpleMapValue value) {
            this.primaryCursor = cursor;
            this.groupByFunctions = functions;
            this.record = new VirtualRecordNoRowid(functions);
            this.record.of(value);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            Misc.clearObjList(groupByFunctions);
        }

        @Override
        public Record getRecord() {
            return record;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) groupByFunctions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            primaryCursor.buildValueConditionally();
            isExhausted = true;
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
        }

        @Override
        public long preComputedStateSize() {
            return 0;
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            isExhausted = false;
            GroupByUtils.toTop(groupByFunctions);
        }

        void of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            Function.init(groupByFunctions, baseCursor, executionContext, null);
            isExhausted = false;
        }

    }

    private class EarlyExitGroupByNotKeyedRecordCursor extends GroupByNotKeyedRecordCursor {

        public EarlyExitGroupByNotKeyedRecordCursor(
                CairoConfiguration configuration,
                ObjList<GroupByFunction> groupByFunctions,
                GroupByFunctionsUpdater groupByFunctionsUpdater
        ) {
            super(configuration, groupByFunctions, groupByFunctionsUpdater);
        }

        @Override
        public boolean earlyExit() {
            boolean earlyExit = true;
            for (int i = 0, n = groupByFunctions.size(); i < n; i++) {
                earlyExit &= groupByFunctions.getQuick(i).earlyExit(value);
            }
            return earlyExit;
        }
    }

    private class GroupByNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
        private final GroupByAllocator allocator;
        private final GroupByFunctionsUpdater groupByFunctionsUpdater;
        // True once of() has initialized the group-by functions for the current execution;
        // getSharedCursor donates owner state to shared consumers only when this is set.
        private boolean areFunctionsInitialized;
        // hold on to reference of base cursor here
        // because we use it as symbol table source for the functions
        private RecordCursor baseCursor;
        private SqlExecutionCircuitBreaker circuitBreaker;
        private boolean isExhausted;
        private boolean isValueBuilt;

        public GroupByNotKeyedRecordCursor(
                CairoConfiguration configuration,
                ObjList<GroupByFunction> groupByFunctions,
                GroupByFunctionsUpdater groupByFunctionsUpdater
        ) {
            this.groupByFunctionsUpdater = groupByFunctionsUpdater;
            // Lazy variant: the allocator's chunk index is not allocated until the
            // first cursor's of() binds a MemoryTracker and calls reopen(), keeping
            // per-query alloc/free accounting symmetric from the very first cursor.
            this.allocator = GroupByAllocatorFactory.createAllocator(configuration, false);
            GroupByUtils.setAllocator(groupByFunctions, allocator);
        }

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            if (!isExhausted) {
                counter.inc();
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            areFunctionsInitialized = false;
            baseCursor = Misc.free(baseCursor);
            Misc.free(allocator);
            Misc.clearObjList(groupByFunctions);
        }

        public boolean earlyExit() {
            return false; // no early exit support here
        }

        @Override
        public Record getRecord() {
            return virtualRecordA;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return (SymbolTable) groupByFunctions.getQuick(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (isExhausted) {
                return false;
            }
            buildValueConditionally();
            isExhausted = true;
            return true;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
        }

        public RecordCursor of(RecordCursor baseCursor, SqlExecutionContext executionContext) throws SqlException {
            this.baseCursor = baseCursor;
            this.isExhausted = false;
            this.isValueBuilt = false;
            this.circuitBreaker = executionContext.getCircuitBreaker();
            allocator.setMemoryTracker(executionContext.getMemoryTracker());
            allocator.reopen();
            // getSharedCursor may have run this setup already (a shared consumer can open before
            // the primary cursor); the functions must not re-run their once-per-query
            // initialization, or stateful functions such as scalar sub-query caches execute again.
            if (!areFunctionsInitialized) {
                Function.init(groupByFunctions, baseCursor, executionContext, null);
                areFunctionsInitialized = true;
            }
            return this;
        }

        @Override
        public long preComputedStateSize() {
            return RecordCursor.fromBool(isValueBuilt);
        }

        @Override
        public long size() {
            return 1;
        }

        @Override
        public void toTop() {
            isExhausted = false;
        }

        void buildValueConditionally() {
            if (!isValueBuilt) {
                // Consult the breaker before aggregating, so an empty base scan (which only calls
                // updateEmpty below, never the row loop) still observes cancellation.
                circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottled();
                final Record baseRecord = baseCursor.getRecord();
                if (baseCursor.hasNext()) {
                    long rowId = 0;
                    groupByFunctionsUpdater.updateNew(value, baseRecord, rowId++);
                    while (baseCursor.hasNext()) {
                        circuitBreaker.statefulThrowExceptionIfTripped();
                        groupByFunctionsUpdater.updateExisting(value, baseRecord, rowId++);
                        if (earlyExit()) {
                            break;
                        }
                    }
                } else {
                    groupByFunctionsUpdater.updateEmpty(value);
                }
                isValueBuilt = true;
            }
        }
    }
}
