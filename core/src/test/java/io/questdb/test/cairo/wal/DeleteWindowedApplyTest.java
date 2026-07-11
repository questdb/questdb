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

package io.questdb.test.cairo.wal;

import io.questdb.cairo.TableToken;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.griffin.engine.ops.DeleteOperation;
import io.questdb.std.Chars;
import io.questdb.std.Rnd;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 4 spike: proves the apply-time survivor factory (built by SqlCompilerImpl.generateDelete when
 * isWalApplication) can be bounded to a per-window designated-timestamp interval via the two named bind
 * variables {@link DeleteOperation#WINDOW_LO_BIND}/{@link DeleteOperation#WINDOW_HI_BIND}, that this bound
 * executes as an INTERVAL SCAN (so N windows sum to one table pass rather than N full scans), and that the
 * SAME factory is rebindable window by window. This is the linchpin: OperationExecutor (Task 5) drives this
 * one factory per window, re-running getCursor with new bounds.
 */
public class DeleteWindowedApplyTest extends AbstractCairoTest {

    // Designated-timestamp day boundaries (micros) for the fixture below: hourly rows x=1..96 over 4 days.
    private static final long DAY1_LO = 0L;                 // 1970-01-01T00:00:00Z, rows x=1..24
    private static final long DAY2_LO = 86_400_000_000L;    // 1970-01-02T00:00:00Z
    private static final long DAY3_LO = 172_800_000_000L;   // 1970-01-03T00:00:00Z, rows x=49..72
    private static final long DAY4_LO = 259_200_000_000L;   // 1970-01-04T00:00:00Z

    // (b) The ACTUAL generated survivor factory (bind-variable bounds, not literals) executes as an interval
    // scan. This is the non-negotiable performance gate: the window bound must engage QuestDB's interval-scan
    // operator so only the window's partitions are read.
    @Test
    public void testGeneratedSurvivorFactoryUsesIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            try (SqlExecutionContext applyContext = newApplyContext();
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                final CompiledQuery cc = compiler.compile("delete from t where x % 2 = 0", applyContext);
                try (DeleteOperation op = cc.getDeleteOperation()) {
                    final RecordCursorFactory factory = op.getSurvivorFactory();
                    Assert.assertNotNull("survivor factory must be built at WAL apply time", factory);
                    planSink.of(factory, applyContext);
                    final CharSequence plan = planSink.getSink();
                    Assert.assertTrue(
                            "expected an interval scan on the generated survivor factory, plan was:\n" + plan,
                            Chars.contains(plan, "Interval forward scan")
                    );
                }
            }
        });
    }

    // (a) + (c) The generated survivor factory returns exactly the in-window survivors, and re-binding a
    // DIFFERENT window on the SAME factory returns that window's survivors (proving one factory serves all
    // windows). Also checks the compiled-in defaults (MIN+1, MAX) return the whole-range survivor set.
    @Test
    public void testGeneratedSurvivorFactoryRebindsPerWindow() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            try (SqlExecutionContext applyContext = newApplyContext();
                 SqlCompiler compiler = engine.getSqlCompiler()) {
                // Mirrors OperationExecutor.executeDelete's apply-context compile (isWalApplication()==true).
                final CompiledQuery cc = compiler.compile("delete from t where x % 2 = 0", applyContext);
                try (DeleteOperation op = cc.getDeleteOperation()) {
                    final RecordCursorFactory factory = op.getSurvivorFactory();
                    Assert.assertNotNull("survivor factory must be built at WAL apply time", factory);

                    // Default bounds compiled in by generateDelete: the whole survivor set (odd x, 1..95).
                    Assert.assertEquals(oddsInclusive(1, 96), collectX(factory, applyContext));

                    // Window [day1, day2) -> rows x=1..24, survivors (odd x) 1..23.
                    setWindow(applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));

                    // Re-bind a different window [day3, day4) on the SAME factory -> rows x=49..72, odds 49..71.
                    setWindow(applyContext, DAY3_LO, DAY4_LO);
                    Assert.assertEquals(oddsInclusive(49, 72), collectX(factory, applyContext));

                    // Re-bind back to the first window to confirm the rebind is stateless/repeatable.
                    setWindow(applyContext, DAY1_LO, DAY2_LO);
                    Assert.assertEquals(oddsInclusive(1, 24), collectX(factory, applyContext));
                }
            }
        });
    }

    // Baseline (Step 2 gate): a designated-ts range on a plain SELECT of the survivor shape compiles to an
    // interval scan. Proves the general mechanism a literal-bound range engages the interval-scan operator;
    // the bind-variable bounds above are the runtime equivalent.
    @Test
    public void testWindowBoundLiteralSelectUsesIntervalScan() throws Exception {
        assertMemoryLeak(() -> {
            createAndPopulate();
            final CharSequence plan = getPlanSink(
                    "select * from t where not (x % 2 = 0) " +
                            "and ts >= '1970-01-02T00:00:00.000000Z' and ts < '1970-01-03T00:00:00.000000Z'"
            ).getSink();
            Assert.assertTrue(
                    "expected an interval scan for a designated-ts range predicate, plan was:\n" + plan,
                    Chars.contains(plan, "Interval forward scan")
            );
        });
    }

    // Regression test (CRITICAL fix): on a TIMESTAMP_NS designated-timestamp WAL table, generateDelete used to
    // set the __del_win_lo/__del_win_hi defaults via BindVariableService.setTimestamp, which is unconditionally
    // MICROS-typed. At apply time the survivor factory's runtime interval evaluated that bind variable through
    // NanosTimestampDriver.from(value, TIMESTAMP_MICRO) -> microsToNanos -> Math.multiplyExact, and the
    // Long.MAX_VALUE default overflowed: ImplicitCastException, WAL apply fails, and the table is SUSPENDED
    // without deleting anything. DeleteOperation.setWindowBound (used by generateDelete) fixes this by setting
    // the bind variable in the designated-timestamp column's OWN unit (setTimestampNano for TIMESTAMP_NANO).
    // Before the fix this test fails with the table suspended; after the fix it deletes exactly the matched
    // rows and leaves the table healthy.
    @Test
    public void testDeleteArbitraryPredicateOnNanosTimestampDoesNotSuspendTable() throws Exception {
        assertMemoryLeak(() -> {
            // Hourly rows over 4 daily partitions (x=1..96), mirroring createAndPopulate() but in nanos.
            execute("create table t (ts timestamp_ns, x long) timestamp(ts) partition by DAY WAL");
            execute("insert into t select timestamp_sequence_ns(0, 60*60*1000000000L), x from long_sequence(96)");
            drainWalQueue();
            // Independent oracle snapshot, never touched by the DELETE.
            execute("create table t_ref as (select * from t)");

            final TableToken tableToken = engine.verifyTableName("t");

            // An arbitrary (non-time-range) predicate forces the whole-range survivor-replace path
            // (OperationExecutor.replaceWithSurvivors), which is exactly where the bind-variable unit bug bites.
            execute("delete from t where x % 2 = 0");
            drainWalQueue();

            Assert.assertFalse(
                    "table must not be suspended by a DELETE on a TIMESTAMP_NS designated-timestamp column",
                    engine.getTableSequencerAPI().isSuspended(tableToken)
            );
            // Surviving rows must be exactly the NOT(x % 2 = 0) set (odd x, 1..95), in table order.
            assertSqlCursors(
                    "select * from t_ref where not (x % 2 = 0)",
                    "select * from t"
            );
        });
    }

    // Collects the survivor cursor's x column (index 1 of SELECT *) in cursor order as a comma-separated string.
    private static String collectX(RecordCursorFactory factory, SqlExecutionContext executionContext) throws SqlException {
        final StringBuilder sink = new StringBuilder();
        try (RecordCursor cursor = factory.getCursor(executionContext)) {
            final Record record = cursor.getRecord();
            while (cursor.hasNext()) {
                if (sink.length() > 0) {
                    sink.append(',');
                }
                sink.append(record.getLong(1));
            }
        }
        return sink.toString();
    }

    // Expected in-order odd x values in [loX, hiX] (the survivors of "delete where x % 2 = 0").
    private static String oddsInclusive(int loX, int hiX) {
        final StringBuilder sink = new StringBuilder();
        for (int x = loX; x <= hiX; x++) {
            if ((x & 1) == 1) {
                if (sink.length() > 0) {
                    sink.append(',');
                }
                sink.append(x);
            }
        }
        return sink.toString();
    }

    private static void setWindow(SqlExecutionContext executionContext, long loInclusive, long hiExclusive) throws SqlException {
        executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_LO_BIND, loInclusive);
        executionContext.getBindVariableService().setTimestamp(DeleteOperation.WINDOW_HI_BIND, hiExclusive);
    }

    private void createAndPopulate() throws Exception {
        execute("create table t (ts timestamp, x long) timestamp(ts) partition by DAY WAL");
        execute("insert into t select timestamp_sequence('1970-01-01T00:00:00.000000Z', 60*60*1000000L), x from long_sequence(96)");
        drainWalQueue();
    }

    // A WAL-apply execution context: isWalApplication()==true, so generateDelete negates the predicate and
    // keeps the survivor factory (with the window bind-var bounds), exactly as OperationExecutor does at apply.
    private SqlExecutionContext newApplyContext() {
        final SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 1) {
            @Override
            public boolean isWalApplication() {
                return true;
            }
        };
        context.with(AllowAllSecurityContext.INSTANCE, new BindVariableServiceImpl(engine.getConfiguration()), new Rnd());
        return context;
    }
}
