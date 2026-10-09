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

package io.questdb.griffin.engine;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SingleSymbolFilter;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PivotInListCheck;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.mp.SCSequence;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.Transient;

/**
 * Runs a plan's PIVOT {@code IN (subquery)} checks before every execution but the first, see
 * {@link PivotInListCheck}. Everything else is the plan's: the wrapper is transparent, as
 * {@link StaleViewCheckFactory} is, and shows nothing in EXPLAIN.
 */
public class PivotInListCheckFactory implements RecordCursorFactory {
    private final RecordCursorFactory base;
    private final ObjList<PivotInListCheck> checks = new ObjList<>();
    // the compile that built the plan has just read the values: its first execution needs no check
    private boolean isCompileFresh = true;

    /**
     * @param base   the plan
     * @param checks the plan's PIVOT IN subqueries, which the factory takes over: it frees them
     *               when it closes, and the list is emptied
     */
    public PivotInListCheckFactory(RecordCursorFactory base, ObjList<PivotInListCheck> checks) {
        this.base = base;
        this.checks.addAll(checks);
        checks.clear();
    }

    @Override
    public void close() {
        Misc.freeObjListAndClear(checks);
        base.close();
    }

    @Override
    public SingleSymbolFilter convertToSampleByIndexPageFrameCursorFactory() {
        return base.convertToSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public PageFrameSequence<?> execute(@Transient SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        verify(executionContext);
        return base.execute(executionContext, collectSubSeq, order);
    }

    @Override
    public boolean followedOrderByAdvice() {
        return base.followedOrderByAdvice();
    }

    @Override
    public boolean fragmentedSymbolTables() {
        return base.fragmentedSymbolTables();
    }

    @Override
    public String getBaseColumnName(int idx) {
        return base.getBaseColumnName(idx);
    }

    @Override
    public RecordCursorFactory getBaseFactory() {
        return base.getBaseFactory();
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        verify(executionContext);
        return base.getCursor(executionContext);
    }

    @Override
    public RecordMetadata getMetadata() {
        return base.getMetadata();
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        verify(executionContext);
        return base.getPageFrameCursor(executionContext, order);
    }

    /**
     * Page-frame transparent: a page-frame consumer asking this wrapper is served the BASE's
     * frames, so it must be told the base's page-frame direction. Inheriting the default --
     * which forwards to {@link #getScanDirection()} -- degrades the narrow answer back to the
     * wide one at the first wrapper, which is exactly what the narrow answer exists to avoid.
     */
    @Override
    public int getPageFrameScanDirection() {
        return base.getPageFrameScanDirection();
    }

    @Override
    public int getScanDirection() {
        return base.getScanDirection();
    }

    @Override
    public TableToken getTableToken() {
        return base.getTableToken();
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public boolean producesMaterializedPageFrames() {
        // Must delegate to the immediate base like supportsPageFrameCursor() /
        // getPageFrameCursor() do: getBaseFactory() here returns base.getBaseFactory()
        // (it skips this wrapper's own base for top-K peeling), so the default
        // getBaseFactory()-delegating implementation would step over a metadata-only
        // producer (e.g. a single-key covering scan) and wrongly report it as
        // materialized -- re-opening the all-null covered-column parquet export.
        return base.producesMaterializedPageFrames();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return this.base.recordCursorSupportsRandomAccess();
    }

    @Override
    public void revertFromSampleByIndexPageFrameCursorFactory() {
        base.revertFromSampleByIndexPageFrameCursorFactory();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        base.toPlan(sink);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private void verify(SqlExecutionContext executionContext) throws SqlException {
        if (isCompileFresh) {
            isCompileFresh = false;
            return;
        }
        for (int i = 0, n = checks.size(); i < n; i++) {
            checks.getQuick(i).verify(executionContext);
        }
    }
}
