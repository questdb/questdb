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

package io.questdb.test.griffin;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.PlanSink;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.table.RuntimeConstGateRecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * A WHERE predicate whose whole compiled filter is a runtime constant - a scalar boolean
 * sub-query used directly ({@code where (select b from x limit 1)}), its negation, or a bind
 * variable / {@code now()}-driven expression - is gated behind a single per-execution
 * evaluation. A false predicate returns an empty result without opening the outer base (no outer
 * I/O), and a true predicate delegates straight to the base. Compound predicates with a per-row
 * conjunct ({@code where a and (select b)}) keep the ordinary per-row async/serial filter.
 */
public class BooleanSubQueryRuntimeGateTest extends AbstractCairoTest {

    @Test
    public void testBindVarBooleanReEvaluatedPerExecution() throws Exception {
        // A single compiled gate factory must re-read the runtime constant on every open: a false
        // bind value yields no rows, a true bind value yields all rows, proving the value is never
        // baked at compile time.
        assertMemoryLeak(() -> {
            createTables();
            bindVariableService.setBoolean(0, true);
            try (RecordCursorFactory factory = select("select * from t where $1::boolean")) {
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
                bindVariableService.setBoolean(0, true);
                assertRowCount(1, factory);
                bindVariableService.setBoolean(0, false);
                assertRowCount(0, factory);
            }
        });
    }

    @Test
    public void testCompoundPredicateStaysPerRow() throws Exception {
        // With a genuine per-row conjunct the whole filter is not runtime-constant, so the gate
        // must NOT apply and the async per-row filter is retained.
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where v = 1 and (select b from x_false limit 1)");
            TestUtils.assertContains(sink, "Async Filter");
            printSql("explain select * from t where v = 5 or (select b from x_true limit 1)");
            TestUtils.assertContains(sink, "Async Filter");
        });
    }

    @Test
    public void testFalsePredicateDoesNotOpenOuterBase() throws Exception {
        // Deterministic outer-scan counter: with a false runtime constant the gate must return an
        // empty cursor WITHOUT calling getCursor() on the base factory.
        assertMemoryLeak(() -> {
            createTables();
            OpenCountingRecordCursorFactory base = new OpenCountingRecordCursorFactory(select("select * from t"));
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(false));
            try {
                try (RecordCursor cursor = gate.getCursor(sqlExecutionContext)) {
                    Assert.assertFalse("false predicate must yield no rows", cursor.hasNext());
                }
                Assert.assertEquals("base must not be opened when the predicate is false", 0, base.openCount);
            } finally {
                gate.close();
            }
        });
    }

    @Test
    public void testMultiRowSubQueryErrorPathDoesNotLeak() throws Exception {
        // The single-row cardinality check lives in the filter init the gate calls at open; the
        // error must propagate and leave nothing leaked (guarded by assertMemoryLeak).
        assertMemoryLeak(() -> {
            createTables();
            execute("create table x_multi (b boolean)");
            execute("insert into x_multi values (true), (false)");
            try (RecordCursorFactory factory = select("select * from t where (select b from x_multi)")) {
                try (RecordCursor ignore = factory.getCursor(sqlExecutionContext)) {
                    Assert.fail("expected a cardinality error");
                } catch (Exception e) {
                    TestUtils.assertContains(e.getMessage(), "scalar sub-query returned more than one row");
                }
            }
        });
    }

    @Test
    public void testPlanGatesRuntimeConstantPredicate() throws Exception {
        // Before the fix this rendered "Async Filter workers: 1" over the full "Frame forward scan
        // on: t"; the gate renders an ordinary "Filter" over the base and never the async full scan.
        assertMemoryLeak(() -> {
            createTables();
            printSql("explain select * from t where (select b from x_false limit 1)");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "Filter");
            TestUtils.assertContains(sink, "filter: cursor");
            TestUtils.assertContains(sink, "Frame forward scan on: t");

            printSql("explain select * from t where not (select b from x_false limit 1)");
            TestUtils.assertNotContains(sink, "Async Filter");
            TestUtils.assertContains(sink, "not (cursor");
        });
    }

    @Test
    public void testTruePredicateDelegatesToBase() throws Exception {
        // With a true runtime constant the gate delegates straight to the base cursor once, and
        // every base row flows through.
        assertMemoryLeak(() -> {
            createTables();
            OpenCountingRecordCursorFactory base = new OpenCountingRecordCursorFactory(select("select * from t"));
            RuntimeConstGateRecordCursorFactory gate =
                    new RuntimeConstGateRecordCursorFactory(base, new ConstBoolFilter(true));
            try {
                int rows = 0;
                try (RecordCursor cursor = gate.getCursor(sqlExecutionContext)) {
                    while (cursor.hasNext()) {
                        rows++;
                    }
                }
                Assert.assertEquals("true predicate must pass every base row", 1, rows);
                Assert.assertEquals("base must be opened exactly once when the predicate is true", 1, base.openCount);
            } finally {
                gate.close();
            }
        });
    }

    private static void assertRowCount(int expected, RecordCursorFactory factory) throws Exception {
        int rows = 0;
        try (RecordCursor cursor = factory.getCursor(sqlExecutionContext)) {
            while (cursor.hasNext()) {
                rows++;
            }
        }
        Assert.assertEquals(expected, rows);
    }

    private void createTables() throws Exception {
        execute("create table t (ts timestamp, v int, sym symbol index) timestamp(ts) partition by day");
        execute("insert into t values ('2018-01-01T00:00:00.000000Z', 1, 'a')");
        execute("create table x_false (b boolean)");
        execute("insert into x_false values (false)");
        execute("create table x_true (b boolean)");
        execute("insert into x_true values (true)");
    }

    private static final class ConstBoolFilter extends BooleanFunction {
        private final boolean value;

        private ConstBoolFilter(boolean value) {
            this.value = value;
        }

        @Override
        public boolean getBool(Record rec) {
            return value;
        }

        @Override
        public boolean isRuntimeConstant() {
            return true;
        }

        @Override
        public void toPlan(PlanSink sink) {
            sink.val(value);
        }
    }

    private static final class OpenCountingRecordCursorFactory extends AbstractRecordCursorFactory {
        private final RecordCursorFactory base;
        int openCount;

        private OpenCountingRecordCursorFactory(RecordCursorFactory base) {
            super(base.getMetadata());
            this.base = base;
        }

        @Override
        public RecordCursorFactory getBaseFactory() {
            return base;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) throws io.questdb.griffin.SqlException {
            openCount++;
            return base.getCursor(executionContext);
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return base.recordCursorSupportsRandomAccess();
        }

        @Override
        protected void _close() {
            base.close();
        }
    }
}
