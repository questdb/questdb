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

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.NoRandomAccessRecordCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.VirtualRecord;
import io.questdb.cairo.sql.async.UnorderedPageFrameReducer;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.SymbolFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.GroupByUtils;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

class AsyncGroupByNotKeyedRecordCursor implements NoRandomAccessRecordCursor {
    private static final Log LOG = LogFactory.getLog(AsyncGroupByNotKeyedRecordCursor.class);
    // the base of an index gather, which a serial retry reopens, see buildValue()
    private final RecordCursorFactory base;
    // the reducer of an index gather's tasks, null for a GROUP BY over page frames
    private final UnorderedPageFrameReducer gatherReducer;
    private final ObjList<GroupByFunction> groupByFunctions;
    private final VirtualRecord recordA;
    private SqlExecutionContext executionContext;
    private boolean isSerialRetry;
    private long serialRetries;
    private SqlExecutionCircuitBreaker circuitBreaker;
    private UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence;
    private boolean isExhausted;
    private boolean isOpen;
    private boolean isValueBuilt;

    public AsyncGroupByNotKeyedRecordCursor(
            ObjList<GroupByFunction> groupByFunctions,
            @Nullable UnorderedPageFrameReducer gatherReducer,
            RecordCursorFactory base
    ) {
        this.gatherReducer = gatherReducer;
        this.base = base;
        this.groupByFunctions = groupByFunctions;
        this.recordA = new VirtualRecord(groupByFunctions);
        // Start closed so the first of() runs atom.reopen(), which opens the lazy
        // (openOnInit=false) allocators and binds the per-query tracker before any
        // allocation. Skipping reopen() on the first cursor would leave the allocator's
        // chunk index unallocated.
        this.isOpen = false;
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
        if (isOpen) {
            try {
                if (frameSequence != null) {
                    frameSequence.await();
                    frameSequence.reset();
                }
            } finally {
                Misc.clearObjList(groupByFunctions);
                isOpen = false;
                isSerialRetry = false;
                executionContext = null;
            }
        }
    }

    @Override
    public Record getRecord() {
        return recordA;
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

    void buildValueConditionally() {
        if (!isValueBuilt) {
            buildValue();
        }
    }

    @Override
    public SymbolTable newSymbolTable(int columnIndex) {
        return ((SymbolFunction) groupByFunctions.getQuick(columnIndex)).newSymbolTable();
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
        GroupByUtils.toTop(groupByFunctions);
        if (frameSequence != null) {
            frameSequence.getAtom().toTop();
        }
    }

    private void buildValue() {
        // Consult the breaker before dispatching frames, so an empty base scan still observes cancellation.
        circuitBreaker.statefulThrowExceptionIfTrippedTimeThrottledOrYield();
        frameSequence.prepareForDispatch();
        frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
        if (gatherReducer == null) {
            frameSequence.dispatchAndAwait();
        } else {
            gather();
        }

        // Merge the values.
        final AsyncGroupByNotKeyedAtom atom = frameSequence.getAtom();
        final GroupByFunctionsUpdater functionUpdater = atom.getFunctionUpdater(-1);
        final SimpleMapValue destValue = atom.getOwnerMapValue();
        for (int i = 0, n = atom.getPerWorkerMapValues().size(); i < n; i++) {
            final SimpleMapValue srcValue = atom.getPerWorkerMapValues().getQuick(i);
            if (srcValue.isNew()) {
                continue;
            }

            if (destValue.isNew()) {
                destValue.copy(srcValue);
            } else {
                functionUpdater.merge(destValue, srcValue);
            }
            destValue.setNew(false);
        }

        isValueBuilt = true;
    }

    /**
     * Runs the tasks of an index gather on the workers, and, when that hits the query's memory
     * limit, again on the query's thread alone, as the serial GROUP BY would.
     */
    private void gather() {
        final IndexGatherPageFrameCursor gatherCursor = (IndexGatherPageFrameCursor) frameSequence.getPageFrameCursor();
        if (isSerialRetry) {
            frameSequence.runRoundLocally(gatherReducer, gatherCursor.getTaskRowCounts());
            return;
        }
        try {
            frameSequence.dispatchRoundAndAwait(gatherReducer, gatherCursor.getTaskRowCounts());
        } catch (CairoException e) {
            if (!e.isOutOfMemory()) {
                throw e;
            }
            LOG.info().$("index gather GROUP BY hit the memory limit, retrying on the query thread [error=").$safe(e.getFlyweightMessage()).I$();
            serialRetries++;
            // free what the parallel pass built, as close() would, and open the base again
            frameSequence.await();
            frameSequence.reset();
            Misc.clearObjList(groupByFunctions);
            isOpen = false;
            try {
                frameSequence.of(base, executionContext, PartitionFrameCursorFactory.ORDER_ASC);
                isSerialRetry = true;
                of(frameSequence, executionContext);
            } catch (SqlException se) {
                throw CairoException.nonCritical().put(se.getFlyweightMessage());
            }
            frameSequence.prepareForDispatch();
            frameSequence.getAtom().getFilterContext().initMemoryPools(frameSequence.getPageFrameAddressCache(), frameSequence.getMemoryTracker());
            gather();
        }
    }

    @TestOnly
    long getSerialRetries() {
        return serialRetries;
    }

    void of(UnorderedPageFrameSequence<AsyncGroupByNotKeyedAtom> frameSequence, SqlExecutionContext executionContext) throws SqlException {
        final AsyncGroupByNotKeyedAtom atom = frameSequence.getAtom();
        // Assign before reopen() so close() can drain a partially reopened atom on a breach.
        this.frameSequence = frameSequence;
        if (!isOpen) {
            isOpen = true;
            atom.reopen();
        }
        this.circuitBreaker = executionContext.getCircuitBreaker();
        this.executionContext = executionContext;
        this.isValueBuilt = false;
        recordA.of(atom.getOwnerMapValue());
        // The atom's init() has already initialized the owner group by functions (this cursor's
        // groupByFunctions) and donated their state to the per-worker clones. Re-initializing them
        // here would re-run stateful initialization, such as a cursor comparison re-executing its
        // scalar sub-query, and could diverge from the state the workers observe.
        toTop();
    }
}
