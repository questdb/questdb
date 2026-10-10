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

package io.questdb.test.griffin;

import io.questdb.cairo.AbstractRecordCursorFactory;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.TableReferenceOutOfDateException;
import io.questdb.griffin.FunctionFactory;
import io.questdb.griffin.FunctionFactoryDescriptor;
import io.questdb.griffin.SqlCompilerImpl;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.functions.BooleanFunction;
import io.questdb.griffin.engine.functions.CursorFunction;
import io.questdb.griffin.model.QueryModel;
import io.questdb.griffin.plan.logical.LogicalPlan;
import io.questdb.griffin.plan.logical.ScanPlan;
import io.questdb.std.Chars;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

public class TableSourceTest extends AbstractCairoTest {
    private static final String CONSTANT_FILTER_SQL = "SELECT * FROM lp_constant_source() WHERE lp_constant_filter()";

    @Test
    public void testConstantFilterCloseFailureClosesUnpublishedResult() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException closeFailure = new RuntimeException("filter close");
            final TrackingFactory factory = new TrackingFactory(null);
            final ConstantFilter filter = new ConstantFilter(true, null, closeFailure);
            final RuntimeException e = assertConstantFilterFails(factory, filter);
            Assert.assertSame(closeFailure, e);
            Assert.assertEquals(1, factory.closeCount);
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testConstantFilterEvaluationFailureClosesBothInputs() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException evaluationFailure = new RuntimeException("filter evaluation");
            final RuntimeException factoryFailure = new RuntimeException("factory close");
            final RuntimeException filterFailure = new RuntimeException("filter close");
            final TrackingFactory factory = new TrackingFactory(factoryFailure);
            final ConstantFilter filter = new ConstantFilter(false, evaluationFailure, filterFailure);
            final RuntimeException e = assertConstantFilterFails(factory, filter);
            Assert.assertSame(evaluationFailure, e);
            assertFailures(e, evaluationFailure, factoryFailure, filterFailure);
            Assert.assertEquals(1, factory.closeCount);
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testConstantFilterFactoryCloseFailureStillClosesFilter() throws Exception {
        assertMemoryLeak(() -> {
            final RuntimeException factoryFailure = new RuntimeException("factory close");
            final RuntimeException filterFailure = new RuntimeException("filter close");
            final TrackingFactory factory = new TrackingFactory(factoryFailure);
            final ConstantFilter filter = new ConstantFilter(false, null, filterFailure);
            final RuntimeException e = assertConstantFilterFails(factory, filter);
            assertFailures(e, factoryFailure, filterFailure);
            Assert.assertEquals(1, factory.closeCount);
            Assert.assertEquals(1, filter.closeCount);
        });
    }

    @Test
    public void testConstantFilterTransfersOrCopiesMetadata() throws Exception {
        assertMemoryLeak(() -> {
            final TrackingFactory falseFactory = new TrackingFactory(null);
            final ConstantFilter falseFilter = new ConstantFilter(false, null, null);
            registerConstantFilterInputs(falseFactory, falseFilter);
            try (RecordCursorFactory result = select(CONSTANT_FILTER_SQL)) {
                Assert.assertEquals(1, falseFactory.closeCount);
                Assert.assertEquals(1, falseFilter.closeCount);
                Assert.assertEquals(0, falseFactory.getMetadata().getColumnCount());
                Assert.assertEquals(1, result.getMetadata().getColumnCount());
                TestUtils.assertEquals("ts", result.getMetadata().getColumnName(0));
                Assert.assertEquals(0, result.getMetadata().getTimestampIndex());
                assertFactory(result).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("ts\n");
            } finally {
                unregisterConstantFilterInputs();
            }

            final TrackingFactory trueFactory = new TrackingFactory(null);
            final ConstantFilter trueFilter = new ConstantFilter(true, null, null);
            registerConstantFilterInputs(trueFactory, trueFilter);
            try (RecordCursorFactory result = select(CONSTANT_FILTER_SQL)) {
                Assert.assertEquals(0, trueFactory.closeCount);
                Assert.assertEquals(1, trueFilter.closeCount);
                final TextPlanSink plan = new TextPlanSink();
                plan.of(result, sqlExecutionContext);
                Assert.assertFalse(plan.getSink().toString(), Chars.contains(plan.getSink(), "Filter"));
                Assert.assertFalse(plan.getSink().toString(), Chars.contains(plan.getSink(), "Empty table"));
            } finally {
                unregisterConstantFilterInputs();
            }
            Assert.assertEquals(1, trueFactory.closeCount);
        });
    }

    @Test
    public void testConstantPredicatesRemoveFilterFactory() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertSource("SELECT value FROM lp_source WHERE TRUE", "value\n10\n20\n30\n", "Frame forward scan", false);
            assertSource("SELECT value FROM lp_source WHERE FALSE", "value\n", "Empty table", false);
        });
    }

    @Test
    public void testDesignatedTimestampOrderUsesScan() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            assertSource("SELECT value FROM lp_source ORDER BY ts ASC", "value\n10\n20\n30\n", "Frame forward scan", true);
            assertSource("SELECT value FROM lp_source ORDER BY ts DESC", "value\n30\n20\n10\n", "Frame backward scan", true);
            assertSource("SELECT value FROM lp_source ORDER BY ts DESC LIMIT 2", "value\n30\n20\n", "Frame backward scan", true);
            assertSource("SELECT ts AS event_time, value FROM lp_source ORDER BY event_time DESC", """
                    event_time	value
                    2020-01-01T00:00:02.000000Z	30
                    2020-01-01T00:00:01.000000Z	20
                    2020-01-01T00:00:00.000000Z	10
                    """, "Frame backward scan", true);
        });
    }

    @Test
    public void testSourceFactoryRejectsStaleReaderVersion() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT value FROM lp_source", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    execute("ALTER TABLE lp_source ADD COLUMN added INT");
                    // Metadata remains independently owned even though this factory can no longer
                    // open a reader at the schema version captured when it was generated.
                    Assert.assertEquals(1, factory.getMetadata().getColumnCount());
                    TestUtils.assertEquals("value", factory.getMetadata().getColumnName(0));
                    try (RecordCursor ignored = factory.getCursor(sqlExecutionContext)) {
                        Assert.fail("expected stale reader version");
                    } catch (TableReferenceOutOfDateException expected) {
                        // The statement owner retries from binding; the factory must not remap itself.
                    }
                }
            }
        });
    }

    @Test
    public void testSourceMetadataChangeRetriesColumnMapping() throws Exception {
        assertMemoryLeak(() -> {
            createRows();
            final int[] attempts = {0};
            try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine) {
                @Override
                protected RecordCursorFactory generateSelectOneShot(
                        QueryModel model,
                        SqlExecutionContext executionContext,
                        boolean generateProgressLogger
                ) throws SqlException {
                    if (++attempts[0] == 1) {
                        engine.execute("ALTER TABLE lp_source DROP COLUMN unused", executionContext);
                    }
                    return super.generateSelectOneShot(model, executionContext, generateProgressLogger);
                }
            }) {
                try (RecordCursorFactory factory = compiler.compile(
                        "SELECT value, ts FROM lp_source", sqlExecutionContext
                ).getRecordCursorFactory()) {
                    Assert.assertEquals(2, attempts[0]);
                    LogicalPlan plan = compiler.getPlanForTesting();
                    while (plan.inputCount() > 0) {
                        plan = plan.inputAt(0);
                    }
                    final ScanPlan scan = (ScanPlan) plan;
                    Assert.assertEquals(2, scan.getSourceColumnIndexes().size());
                    Assert.assertEquals(0, scan.getSourceColumnIndexes().getQuick(0));
                    Assert.assertEquals(1, scan.getSourceColumnIndexes().getQuick(1));
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns("""
                            value	ts
                            10	2020-01-01T00:00:00.000000Z
                            20	2020-01-01T00:00:01.000000Z
                            30	2020-01-01T00:00:02.000000Z
                            """);
                }
            }
        });
    }

    private static RuntimeException assertConstantFilterFails(TrackingFactory factory, ConstantFilter filter) throws SqlException {
        registerConstantFilterInputs(factory, filter);
        try {
            return Assert.assertThrows(RuntimeException.class, () -> select(CONSTANT_FILTER_SQL));
        } finally {
            unregisterConstantFilterInputs();
        }
    }

    private static void assertFailures(RuntimeException actual, RuntimeException... expected) {
        final ObjList<Throwable> reported = new ObjList<>();
        reported.add(actual);
        for (Throwable suppressed : actual.getSuppressed()) {
            reported.add(suppressed);
        }
        Assert.assertEquals(expected.length, reported.size());
        for (RuntimeException failure : expected) {
            Assert.assertTrue(failure.getMessage(), reported.indexOf(failure) >= 0);
        }
    }

    private static void register(String name, boolean isCursor, Function function) throws SqlException {
        final ObjList<FunctionFactoryDescriptor> descriptors = new ObjList<>();
        descriptors.add(new FunctionFactoryDescriptor(new FunctionFactory() {
            @Override
            public String getSignature() {
                return name + "()";
            }

            @Override
            public boolean isCursor() {
                return isCursor;
            }

            @Override
            public Function newInstance(int position, ObjList<Function> args, IntList argPositions,
                                        CairoConfiguration configuration, SqlExecutionContext executionContext) {
                return function;
            }
        }));
        engine.getFunctionFactoryCache().getFactories().put(name, descriptors);
    }

    private static void registerConstantFilterInputs(TrackingFactory factory, ConstantFilter filter) throws SqlException {
        register("lp_constant_source", true, new CursorFunction(factory));
        register("lp_constant_filter", false, filter);
    }

    private static void unregisterConstantFilterInputs() {
        engine.getFunctionFactoryCache().getFactories().remove("lp_constant_source");
        engine.getFunctionFactoryCache().getFactories().remove("lp_constant_filter");
    }

    private void assertSource(String sql, String expected, String planFragment, boolean hasOrder) throws Exception {
        try (SqlCompilerImpl compiler = new SqlCompilerImpl(engine)) {
            for (int reuse = 0; reuse < 2; reuse++) {
                try (RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()) {
                    final TextPlanSink plan = new TextPlanSink();
                    plan.of(factory, sqlExecutionContext);
                    TestUtils.assertContains(plan.getSink(), planFragment);
                    Assert.assertFalse(plan.getSink().toString(), Chars.contains(plan.getSink(), "Filter"));
                    if (hasOrder) {
                        Assert.assertFalse(plan.getSink().toString(), Chars.contains(plan.getSink(), "Sort"));
                    }
                    assertFactory(factory).withContext(sqlExecutionContext).inferRandomAccess().inferTimestamp().sizeMayVary().returns(expected);
                }
            }
        }
    }

    private void createRows() throws SqlException {
        execute("CREATE TABLE lp_source (unused INT, value INT, ts TIMESTAMP) TIMESTAMP(ts)");
        execute("""
                INSERT INTO lp_source VALUES
                    (1,10,'2020-01-01T00:00:00.000000Z'),
                    (2,20,'2020-01-01T00:00:01.000000Z'),
                    (3,30,'2020-01-01T00:00:02.000000Z')
                """);
    }

    private static class ConstantFilter extends BooleanFunction {
        private final RuntimeException closeFailure;
        private final RuntimeException evaluationFailure;
        private final boolean isTrue;
        private int closeCount;

        private ConstantFilter(boolean isTrue, RuntimeException evaluationFailure, RuntimeException closeFailure) {
            this.isTrue = isTrue;
            this.evaluationFailure = evaluationFailure;
            this.closeFailure = closeFailure;
        }

        @Override
        public void close() {
            closeCount++;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }

        @Override
        public boolean getBool(Record rec) {
            if (evaluationFailure != null) {
                throw evaluationFailure;
            }
            return isTrue;
        }

        @Override
        public boolean isConstant() {
            return true;
        }
    }

    private static class TrackingFactory extends AbstractRecordCursorFactory {
        private final RuntimeException closeFailure;
        private int closeCount;

        private TrackingFactory(RuntimeException closeFailure) {
            super(new GenericRecordMetadata().add(new TableColumnMetadata("ts", ColumnType.TIMESTAMP)));
            ((GenericRecordMetadata) getMetadata()).setTimestampIndex(0);
            this.closeFailure = closeFailure;
        }

        @Override
        public RecordCursor getCursor(SqlExecutionContext executionContext) {
            throw new AssertionError("constant filters must not open the source");
        }

        @Override
        public boolean recordCursorSupportsRandomAccess() {
            return true;
        }

        @Override
        protected void _close() {
            closeCount++;
            ((GenericRecordMetadata) getMetadata()).clear();
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
