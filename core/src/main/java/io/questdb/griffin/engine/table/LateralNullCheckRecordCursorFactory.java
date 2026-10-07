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
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.TimeFrameCursor;
import io.questdb.cairo.sql.async.PageFrameSequence;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlOptimiser;
import io.questdb.mp.SCSequence;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;

/**
 * Fails a query, once per execution, when one of its checks is true. LateralJoinRewriter accepts a
 * correlated lateral sub-query whose RIGHT or FULL join loses the rows that it NULL-extends when a
 * WHERE comparison, such as {@code t.id >= $1}, drops those rows anyway. The comparison drops them
 * only while its value, here {@code $1}, does not convert to the NULL of the column type, and the
 * value of a bind variable, a function such as {@code now()} or a scalar sub-query is known only
 * when the query runs. Each check is the comparison with the column replaced by a probe column,
 * which it evaluates on the NULL record of the column type; when one is true, the comparison would
 * keep the rows that the join NULL-extends, and the plan would lose them, so the query fails.
 * <p>
 * The checks run when a cursor opens, and the factory then returns the base cursor itself, so rows
 * pass through without any per-row cost. The code generator places this factory at the top of the
 * plan, and it forwards the factory properties that a top-level consumer reads, as QueryProgress
 * does.
 */
public class LateralNullCheckRecordCursorFactory extends AbstractRecordCursorFactory {
    public static final String NULL_VALUE_ERROR = "outer column reference in an ON clause at or before a RIGHT or FULL join " +
            "is not supported in a correlated lateral sub-query when this value is NULL";
    private final RecordCursorFactory base;
    private final ObjList<Function> checks;
    private final ObjList<Record> nullRecords;
    private final IntList positions;
    // names the probe column, the only column that each check reads, in the plan
    private final RecordMetadata probeMetadata;

    public LateralNullCheckRecordCursorFactory(
            RecordCursorFactory base,
            ObjList<Function> checks,
            ObjList<Record> nullRecords,
            IntList positions,
            RecordMetadata probeMetadata
    ) {
        super(base.getMetadata());
        this.base = base;
        this.checks = checks;
        this.nullRecords = nullRecords;
        this.positions = positions;
        this.probeMetadata = probeMetadata;
    }

    @Override
    public PageFrameSequence<?> execute(SqlExecutionContext executionContext, SCSequence collectSubSeq, int order) throws SqlException {
        runChecks(executionContext);
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
        return base;
    }

    @Override
    public RecordCursor getCursor(SqlExecutionContext executionContext) throws SqlException {
        runChecks(executionContext);
        return base.getCursor(executionContext);
    }

    @Override
    public PageFrameCursor getPageFrameCursor(SqlExecutionContext executionContext, int order) throws SqlException {
        runChecks(executionContext);
        return base.getPageFrameCursor(executionContext, order);
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
    public TimeFrameCursor getTimeFrameCursor(SqlExecutionContext executionContext) throws SqlException {
        runChecks(executionContext);
        return base.getTimeFrameCursor(executionContext);
    }

    @Override
    public boolean implementsLimit() {
        return base.implementsLimit();
    }

    @Override
    public ConcurrentTimeFrameCursor newTimeFrameCursor() {
        return base.newTimeFrameCursor();
    }

    @Override
    public boolean recordCursorSupportsRandomAccess() {
        return base.recordCursorSupportsRandomAccess();
    }

    @Override
    public boolean supportsPageFrameCursor() {
        return base.supportsPageFrameCursor();
    }

    @Override
    public boolean supportsTimeFrameCursor() {
        return base.supportsTimeFrameCursor();
    }

    @Override
    public boolean supportsUpdateRowId(TableToken tableName) {
        return base.supportsUpdateRowId(tableName);
    }

    @Override
    public void toPlan(PlanSink sink) {
        sink.type("Lateral Null Check");
        // the checks read the probe column, not a column of this factory
        sink.setMetadata(probeMetadata);
        sink.meta("checks").val(checks);
        sink.setMetadata(null);
        sink.child(base);
    }

    @Override
    public boolean usesCompiledFilter() {
        return base.usesCompiledFilter();
    }

    private void runChecks(SqlExecutionContext executionContext) throws SqlException {
        for (int i = 0, n = checks.size(); i < n; i++) {
            final Function check = checks.getQuick(i);
            check.init(SqlOptimiser.NULL_REJECTING_PROBE_SYMBOL_TABLES, executionContext);
            if (check.getBool(nullRecords.getQuick(i))) {
                throw SqlException.$(positions.getQuick(i), NULL_VALUE_ERROR);
            }
        }
    }

    @Override
    protected void _close() {
        Misc.free(base);
        Misc.freeObjList(checks);
        Misc.freeObjListIfCloseable(nullRecords);
    }
}
