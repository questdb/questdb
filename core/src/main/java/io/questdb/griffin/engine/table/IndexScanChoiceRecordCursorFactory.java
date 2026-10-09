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

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.DelegatingRecord;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.log.Log;
import io.questdb.log.LogFactory;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTracker;
import io.questdb.std.Misc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

/**
 * A GROUP BY over a symbol {@code IN}/{@code =} filter that a POSTING index serves, compiled twice:
 * as an index plan, whose base reads the list's rows through the index (see
 * {@link IndexGatherRecordCursorFactory}), and as a scan plan, compiled as if the query carried
 * {@code no_index}. Each time the cursor opens, the posting index's per-key row counts give the
 * share of the scanned rows the list selects ({@link IndexGatherRecordCursorFactory#estimateSelection});
 * up to {@code maxSelectivity} the index plan opens, above it the scan plan. Bind variables are
 * resolved at that point, so one cached plan follows the values each execution binds.
 * <p>
 * Both plans return the same rows; they are the same GROUP BY over the same filter. Floating
 * aggregates may differ in their last bits, as between any two parallel GROUP BY runs.
 * <p>
 * <b>Memory limit.</b> The scan plan holds a map per worker. When the query has a memory limit,
 * the scan plan's cursor is wrapped: should it hit the limit before it returns a row, the index
 * plan opens in its place, which, when it is the index-driven parallel GROUP BY, itself falls back
 * to one map on the query's thread. So the choice never fails a query that the serial index plan
 * completes. Otherwise, and for the index plan, the chosen plan's cursor is returned as it is.
 */
public class IndexScanChoiceRecordCursorFactory extends AbstractRecordCursorFactory {
    private static final Log LOG = LogFactory.getLog(IndexScanChoiceRecordCursorFactory.class);
    private final ChoiceCursor cursor = new ChoiceCursor();
    private final long[] estimate = new long[2];
    private final double maxSelectivity;
    private IndexGatherRecordCursorFactory gather;
    private RecordCursorFactory indexPlan;
    private long indexPlanOpens;
    private long lastScanned;
    private long lastSelected;
    private long memoryLimitFallbacks;
    private RecordCursorFactory scanPlan;
    private long scanPlanOpens;

    public IndexScanChoiceRecordCursorFactory(
            @NotNull RecordCursorFactory indexPlan,
            @NotNull RecordCursorFactory scanPlan,
            @NotNull IndexGatherRecordCursorFactory gather,
            double maxSelectivity
    ) {
        super(indexPlan.getMetadata());
        this.indexPlan = indexPlan;
        this.scanPlan = scanPlan;
        this.gather = gather;
        this.maxSelectivity = maxSelectivity;
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return indexPlan;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        gather.estimateSelection(executionContext, estimate);
        lastSelected = estimate[0];
        lastScanned = estimate[1];
        if (isIndexPlanCheaper(lastSelected, lastScanned)) {
            indexPlanOpens++;
            return indexPlan.getCursor(executionContext);
        }
        scanPlanOpens++;
        final RecordCursor scanCursor = scanPlan.getCursor(executionContext);
        final MemoryTracker memoryTracker = executionContext.getMemoryTracker();
        if (memoryTracker == null || memoryTracker.getLimit() == 0) {
            // no memory limit to fall back from: the scan plan's cursor as it is, without the
            // forwarding records
            return scanCursor;
        }
        return cursor.of(scanCursor, indexPlan, executionContext);
    }

    @TestOnly
    public long getIndexPlanOpens() {
        return indexPlanOpens;
    }

    @TestOnly
    public RecordCursorFactory getIndexPlan() {
        return indexPlan;
    }

    @TestOnly
    public long getLastScanned() {
        return lastScanned;
    }

    @TestOnly
    public long getLastSelected() {
        return lastSelected;
    }

    @TestOnly
    public long getMemoryLimitFallbacks() {
        return memoryLimitFallbacks;
    }

    @Override
    public int getScanDirection() {
        final int direction = indexPlan.getScanDirection();
        return direction == scanPlan.getScanDirection() ? direction : SCAN_DIRECTION_OTHER;
    }

    @TestOnly
    public RecordCursorFactory getScanPlan() {
        return scanPlan;
    }

    @TestOnly
    public long getScanPlanOpens() {
        return scanPlanOpens;
    }

    @Override
    public boolean isNonDeterministic() {
        return indexPlan.isNonDeterministic() || scanPlan.isNonDeterministic();
    }

    @Override
    public boolean isStableWithinExecution() {
        return indexPlan.isStableWithinExecution() && scanPlan.isStableWithinExecution();
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return indexPlan.recordCursorSupportsLongTopK(columnIndex) && scanPlan.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return indexPlan.recordCursorSupportsRandomAccess() && scanPlan.recordCursorSupportsRandomAccess();
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Index Or Scan");
        sink.attr("maxSelectivity").val(maxSelectivity);
        sink.child(indexPlan);
        sink.child(scanPlan);
    }

    @Override
    public boolean usesIndex() {
        return indexPlan.usesIndex();
    }

    private boolean isIndexPlanCheaper(long selected, long scanned) {
        return selected <= maxSelectivity * scanned;
    }

    @Override
    protected void _close() {
        final RecordCursorFactory indexPlan = this.indexPlan;
        this.indexPlan = null;
        final RecordCursorFactory scanPlan = this.scanPlan;
        this.scanPlan = null;
        gather = null;
        Throwable failure = Misc.freeBestEffort(null, cursor);
        failure = Misc.freeBestEffort(failure, scanPlan);
        failure = Misc.freeBestEffort(failure, indexPlan);
        CairoException.rethrowCleanupFailure(failure);
    }

    private static class ForwardingRecord extends DelegatingRecord {
        Record getBase() {
            return base;
        }
    }

    /**
     * The chosen plan's cursor. Its records forward to that cursor's, so that, if the scan plan hits
     * the memory limit before its first row, the index plan's cursor can take its place behind
     * records the consumer already holds.
     */
    private class ChoiceCursor implements RecordCursor {
        private final ForwardingRecord recordA = new ForwardingRecord();
        private final ForwardingRecord recordB = new ForwardingRecord();
        private RecordCursor delegate;
        private SqlExecutionContext executionContext;
        private RecordCursorFactory fallbackPlan;
        private boolean isRecordBBound;

        @Override
        public void calculateSize(SqlExecutionCircuitBreaker circuitBreaker, Counter counter) {
            try {
                delegate.calculateSize(circuitBreaker, counter);
            } catch (CairoException e) {
                if (!fallBack(e) || counter.get() != 0) {
                    throw e;
                }
                delegate.calculateSize(circuitBreaker, counter);
            }
            fallbackPlan = null;
        }

        @Override
        public void close() {
            delegate = Misc.free(delegate);
            fallbackPlan = null;
            executionContext = null;
        }

        @Override
        public Record getRecord() {
            return recordA;
        }

        @Override
        public Record getRecordB() {
            if (!isRecordBBound) {
                recordB.of(delegate.getRecordB());
                isRecordBBound = true;
            }
            return recordB;
        }

        @Override
        public SymbolTable getSymbolTable(int columnIndex) {
            return delegate.getSymbolTable(columnIndex);
        }

        @Override
        public boolean hasNext() {
            if (fallbackPlan == null) {
                return delegate.hasNext();
            }
            boolean hasNext;
            try {
                hasNext = delegate.hasNext();
            } catch (CairoException e) {
                if (!fallBack(e)) {
                    throw e;
                }
                hasNext = delegate.hasNext();
            }
            // a row is out, or the cursor is done: the plan stays
            fallbackPlan = null;
            return hasNext;
        }

        @Override
        public boolean isUsingIndex() {
            return delegate.isUsingIndex();
        }

        @Override
        public void longTopK(DirectLongLongSortedList list, int columnIndex) {
            try {
                delegate.longTopK(list, columnIndex);
            } catch (CairoException e) {
                if (!fallBack(e) || list.size() != 0) {
                    throw e;
                }
                delegate.longTopK(list, columnIndex);
            }
            fallbackPlan = null;
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return delegate.newSymbolTable(columnIndex);
        }

        @Override
        public long preComputedStateSize() {
            return delegate.preComputedStateSize();
        }

        @Override
        public void recordAt(Record record, long atRowId) {
            delegate.recordAt(record == recordB ? recordB.getBase() : recordA.getBase(), atRowId);
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public void toTop() {
            delegate.toTop();
        }

        private boolean fallBack(CairoException e) {
            if (fallbackPlan == null || !e.isOutOfMemory()) {
                return false;
            }
            LOG.info().$("scan plan hit the memory limit, opening the index plan [error=").$safe(e.getFlyweightMessage()).I$();
            final RecordCursorFactory plan = fallbackPlan;
            fallbackPlan = null;
            delegate = Misc.free(delegate);
            try {
                delegate = plan.getCursor(executionContext);
            } catch (SqlException se) {
                throw CairoException.nonCritical().put(se.getFlyweightMessage());
            }
            memoryLimitFallbacks++;
            bind();
            return true;
        }

        private void bind() {
            recordA.of(delegate.getRecord());
            isRecordBBound = false;
            if (recordB.getBase() != null) {
                // a consumer holds record B already
                recordB.of(delegate.getRecordB());
                isRecordBBound = true;
            }
        }

        RecordCursor of(RecordCursor delegate, RecordCursorFactory fallbackPlan, SqlExecutionContext executionContext) {
            this.delegate = delegate;
            this.fallbackPlan = fallbackPlan;
            this.executionContext = executionContext;
            recordA.of(null);
            recordB.of(null);
            isRecordBBound = false;
            recordA.of(delegate.getRecord());
            return this;
        }
    }
}
