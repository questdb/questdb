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

package io.questdb.griffin.engine.orderby;

import io.questdb.PropertyKey;
import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.sql.DelegatingRecordCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.LimitBounds;
import io.questdb.griffin.engine.RecordComparator;
import io.questdb.std.DirectIntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Same as SortedLightRecordCursorFactory but using LimitedSizeLongTreeChain instead.
 */
public class LimitedSizeSortedLightRecordCursorFactory extends AbstractRecordCursorFactory {
    private final LimitBounds bounds;
    private final RecordComparator comparator;
    private final CairoConfiguration configuration;
    private final ObjList<DirectIntList> rankMaps;
    private final ListColumnFilter sortColumnFilter;
    private final int timestampIndex;
    private RecordCursorFactory base;
    // factory does not own the chain, just keeps the reference to enable updating of the limits
    private LimitedSizeLongTreeChain chain;
    // initialization delayed to getCursor() because lo/hi need to be evaluated
    private DelegatingRecordCursor cursor; // LimitedSizeSortedLightRecordCursor or LimitedSizePartiallySortedLightRecordCursor

    public LimitedSizeSortedLightRecordCursorFactory(
            CairoConfiguration configuration,
            RecordMetadata metadata,
            RecordCursorFactory base,
            RecordComparator comparator,
            LimitBounds bounds,
            ListColumnFilter sortColumnFilter,
            int timestampIndex // index of timestamp that base record cursor is already sorted on
    ) {
        super(metadata);
        this.base = base;
        this.bounds = bounds;
        this.configuration = configuration;
        this.comparator = comparator;
        this.sortColumnFilter = sortColumnFilter;
        this.timestampIndex = timestampIndex;
        try {
            this.rankMaps = SortKeyEncoder.createRankMaps(metadata, sortColumnFilter);
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base;
    }

    // Stable iff the limit expressions (which may be arbitrary functions) and the base are stable;
    // sorting itself introduces no value sources.
    @Override
    public boolean isNonDeterministic() {
        return bounds.isNonDeterministic() || base.isNonDeterministic();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        final RecordCursor baseCursor = base.getCursor(executionContext);
        try {
            initialize(executionContext, baseCursor);
        } catch (Throwable th) {
            Misc.free(baseCursor);
            throw th;
        }

        try {
            cursor.of(baseCursor, executionContext);
            return cursor;
        } catch (Throwable th) {
            Misc.free(cursor);
            throw th;
        }
    }

    @Override
    public int getScanDirection() {
        return SortedRecordCursorFactory.getScanDirection(sortColumnFilter);
    }

    @Override
    public boolean implementsLimit() {
        return true;
    }

    /**
     * Resolves the bounds, then builds the chain that keeps the {@link LimitBounds#getLimit} rows of the order, and
     * the cursor that skips the {@link LimitBounds#getSkipFirst} and {@link LimitBounds#getSkipLast} rows of them.
     */
    public void initializeLimitedSizeCursor(SqlExecutionContext executionContext, RecordCursor baseCursor) throws SqlException {
        bounds.init(baseCursor, executionContext);
        // Lazy variant: the chain skeleton is constructed but the key/value heaps
        // are not allocated until the first cursor's of() binds a MemoryTracker
        // and calls reopen(). This keeps malloc/free symmetric on the per-query
        // counter from the very first cursor.
        this.chain = new LimitedSizeLongTreeChain(
                configuration.getSqlSortKeyPageSize(),
                configuration.getSqlSortKeyMaxBytes(),
                configuration.getSqlSortLightValuePageSize(),
                configuration.getSqlSortLightValueMaxBytes(),
                PropertyKey.CAIRO_SQL_SORT_KEY_MAX_BYTES.getPropertyPath(),
                PropertyKey.CAIRO_SQL_SORT_LIGHT_VALUE_MAX_BYTES.getPropertyPath(),
                false
        );

        if (timestampIndex == -1 || !bounds.isFirstN()) {
            this.cursor = new LimitedSizeSortedLightRecordCursor(chain, comparator, rankMaps);
        } else {
            this.cursor = new LimitedSizePartiallySortedLightRecordCursor(chain, comparator, timestampIndex, rankMaps);
        }
        updateLimits();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return true;
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Sort light");
        bounds.toPlan(sink);
        if (timestampIndex != -1) {
            sink.meta("partiallySorted").val(true);
        }
        SortedLightRecordCursorFactory.addSortKeys(sink, sortColumnFilter);
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

    private void initialize(SqlExecutionContext executionContext, RecordCursor baseCursor) throws SqlException {
        if (isInitialized()) {
            if (chain != null && cursor instanceof DynamicLimitCursor) {
                bounds.init(baseCursor, executionContext);
                // The cursor implementation was picked once, off the first execution's isFirstN;
                // it has to re-gate any first-N-only behaviour on the value we just re-derived.
                updateLimits();
            }
            return;
        }

        initializeLimitedSizeCursor(executionContext, baseCursor);
    }

    private boolean isInitialized() {
        return cursor != null;
    }

    private void updateLimits() {
        chain.updateLimits(bounds.isFirstN(), bounds.getLimit());
        ((DynamicLimitCursor) cursor).updateLimits(bounds.isFirstN(), bounds.getLimit(), bounds.getSkipFirst(), bounds.getSkipLast());
    }

    @Override
    protected void _close() {
        final RecordCursorFactory base = this.base;
        this.base = null;
        final DelegatingRecordCursor cursor = this.cursor;
        this.cursor = null;
        Throwable failure = Misc.freeBestEffort(null, base);
        failure = Misc.freeBestEffort(failure, cursor);
        failure = bounds.close(failure);
        CairoException.rethrowCleanupFailure(failure);
    }
}
