/*******************************************************************************
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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.idx.IndexReader;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.ParquetDecodeHint;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.StaticSymbolTable;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.TimeFrame;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.vm.api.MemoryCARW;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.jit.CompiledFilter;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import org.jetbrains.annotations.Nullable;

/**
 * A fully transparent wrapper that re-labels ONLY the designated timestamp of its base factory.
 * <p>
 * It exists to honor an explicit {@code timestamp(col)} redesignation on a subquery whose generated
 * factory reports a different (or no) designated timestamp -- e.g. a SAMPLE BY result re-timestamped
 * to a {@code first()}/{@code last()} aggregate column, or a DISTINCT-to-GROUP-BY result that would
 * otherwise drop the designation entirely. Every record the base produces is passed through
 * byte-identically; the ONLY thing that changes is which column index is reported as the designated
 * timestamp, at the metadata / time-frame-cursor level. There is no column projection, no cross-index
 * and no per-row work -- {@link #getCursor}, {@link #getPageFrameCursor} and {@link #getSharedCursor}
 * return the base cursor unchanged, so the wrapper is O(1) both to build and to run.
 * <p>
 * Because the whole point is to change the designated timestamp, this wrapper must never be peeled
 * away by an optimizer that would drop the relabel: {@link #isProjection()} and
 * {@link #canPeelForTopK()} both return {@code false}, and {@link #supportsFilterStealing()} returns
 * {@code false} so no filter-stealing site can replace this factory with its base (all such sites are
 * gated on {@code supportsFilterStealing()}). {@link #getBaseFactory()} still exposes the base for the
 * capability-inspection call sites that only READ it (e.g. {@code hasParquetConvertedColumns}).
 */
public class RetimestampedRecordCursorFactory extends AbstractRecordCursorFactory {
    private final RecordCursorFactory base;

    /**
     * @param retimestampedMetadata metadata identical to {@code base.getMetadata()} except that its
     *                              designated timestamp index points at the redesignated column
     * @param base                  the wrapped factory; ownership transfers to this wrapper
     */
    public RetimestampedRecordCursorFactory(RecordMetadata retimestampedMetadata, RecordCursorFactory base) {
        super(retimestampedMetadata);
        this.base = base;
    }

    @Override
    public boolean canPeelForTopK() {
        // The relabel must not be peeled away and lost by top-K splicing.
        return false;
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    @Nullable
    @Override
    public ObjList<Function> getBindVarFunctions() {
        return base.getBindVarFunctions();
    }

    @Nullable
    @Override
    public MemoryCARW getBindVarMemory() {
        return base.getBindVarMemory();
    }

    @Nullable
    @Override
    public CompiledFilter getCompiledFilter() {
        return base.getCompiledFilter();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        // Transparent: records are byte-identical to the base; only the designated timestamp
        // index (reported via getMetadata()) changes.
        return base.getCursor(executionContext);
    }

    @Nullable
    @Override
    public Function getFilter() {
        return base.getFilter();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        return base.getPageFrameCursor(executionContext, order);
    }

    @Override
    public int getScanDirection() {
        // A forward/backward scan direction is a promise that rows are ordered by the DESIGNATED
        // timestamp. We re-designate to a column the base is not sorted by. When the base has its own
        // designated timestamp, we are relabeling one ordered timestamp to another the user asserted is
        // ordered too (the timestamp() contract), so we honor the base's direction. But when the base
        // has NO designated timestamp (e.g. a keyed GROUP BY, including the DISTINCT-to-GROUP-BY and
        // SAMPLE-BY-to-GROUP-BY rewrites), the rows are not ordered by anything -- reporting a forward
        // scan here would let an enclosing ORDER BY (SqlCodeGenerator.generateOrderBy) or an ASOF/LT
        // join master wrongly treat the output as pre-sorted and skip a required sort. Report OTHER so
        // those consumers insert the sort they need.
        if (base.getMetadata().getTimestampIndex() == -1) {
            return SCAN_DIRECTION_OTHER;
        }
        return base.getScanDirection();
    }

    @Override
    public RecordCursor getSharedCursor(SqlExecutionContext executionContext, int sharedId) throws SqlException {
        return base.getSharedCursor(executionContext, sharedId);
    }

    @Override
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        final TimeFrameCursor baseCursor = base.getTimeFrameCursor(executionContext);
        if (baseCursor == null) {
            return null;
        }
        return new RetimestampedTimeFrameCursor(baseCursor, getMetadata().getTimestampIndex());
    }

    @Override
    public void halfClose() {
        base.halfClose();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean isProjection() {
        // Not a projection: no cross-index, no column reshaping. Must stay false so the parallel
        // GROUP BY / join projection-peel paths never strip this wrapper and lose the relabel.
        return false;
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        final ConcurrentTimeFrameCursor baseCursor = base.newTimeFrameCursor();
        if (baseCursor == null) {
            return baseCursor;
        }
        // Reuse the pure-delegating concurrent cursor: it forwards every call to the delegate and
        // reports our timestamp index (and passes it down through of()).
        return new SelectedRecordCursorFactory.SelectedConcurrentTimeFrameCursor(baseCursor, getMetadata().getTimestampIndex());
    }

    @Override
    public boolean recordCursorSupportsLongTopK(int columnIndex) {
        return base.recordCursorSupportsLongTopK(columnIndex);
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsFilterStealing() {
        // GUARD: every filter-stealing call site replaces the factory with getBaseFactory() when this
        // returns true, which would discard the timestamp relabel. Refusing to be a stealable filter
        // keeps this wrapper in the tree; any underlying filter still applies via the base cursor.
        return false;
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsSharedCursors() {
        return base.supportsSharedCursors();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableToken) {
        return base.supportsUpdateRowId(tableToken);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Retimestamp");
        final RecordMetadata metadata = getMetadata();
        final int timestampIndex = metadata.getTimestampIndex();
        if (timestampIndex != -1) {
            sink.attr("designatedTimestamp").val(metadata.getColumnName(timestampIndex));
        }
        sink.child(base);
    }

    @Override
    public int translateOrderByColumnToBase(int projectedIndex) {
        // Identity column mapping -- forward straight to the base.
        return base.translateOrderByColumnToBase(projectedIndex);
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
        Misc.free(base);
    }

    /**
     * A pure-delegating {@link TimeFrameCursor}: forwards every call to the base cursor and reports
     * the retimestamped designated timestamp index. Records are the base cursor's own records (no
     * projection), so random-access {@code recordAt} calls pass straight through.
     */
    private static final class RetimestampedTimeFrameCursor implements TimeFrameCursor {
        private final int timestampIndex;
        private TimeFrameCursor baseCursor;

        private RetimestampedTimeFrameCursor(TimeFrameCursor baseCursor, int timestampIndex) {
            this.baseCursor = baseCursor;
            this.timestampIndex = timestampIndex;
        }

        @Override
        public void close() {
            baseCursor = Misc.free(baseCursor);
        }

        @Override
        public IndexReader getIndexReaderForCurrentFrame(int columnIndex, int direction) {
            return baseCursor.getIndexReaderForCurrentFrame(columnIndex, direction);
        }

        @Override
        public Record getRecord() {
            return baseCursor.getRecord();
        }

        @Override
        public Record getRecordB() {
            return baseCursor.getRecordB();
        }

        @Override
        public StaticSymbolTable getSymbolTable(int columnIndex) {
            return baseCursor.getSymbolTable(columnIndex);
        }

        @Override
        public TimeFrame getTimeFrame() {
            return baseCursor.getTimeFrame();
        }

        @Override
        public int getTimestampIndex() {
            return timestampIndex;
        }

        @Override
        public void jumpTo(int frameIndex) {
            baseCursor.jumpTo(frameIndex);
        }

        @Override
        public SymbolTable newSymbolTable(int columnIndex) {
            return baseCursor.newSymbolTable(columnIndex);
        }

        @Override
        public boolean next() {
            return baseCursor.next();
        }

        @Override
        public long open() {
            return baseCursor.open();
        }

        @Override
        public boolean prev() {
            return baseCursor.prev();
        }

        @Override
        public void recordAt(Record record, long rowId) {
            baseCursor.recordAt(record, rowId);
        }

        @Override
        public void recordAt(Record record, int frameIndex, long rowIndex) {
            baseCursor.recordAt(record, frameIndex, rowIndex);
        }

        @Override
        public void recordAtRowIndex(Record record, long rowIndex) {
            baseCursor.recordAtRowIndex(record, rowIndex);
        }

        @Override
        public void seekEstimate(long timestamp) {
            baseCursor.seekEstimate(timestamp);
        }

        @Override
        public void setParquetDecodeHint(ParquetDecodeHint hint) {
            baseCursor.setParquetDecodeHint(hint);
        }

        @Override
        public void toTop() {
            baseCursor.toTop();
        }
    }
}
