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

import io.questdb.PropertyKey;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.CursorPrinter;
import io.questdb.cairo.ImplicitCastException;
import io.questdb.cairo.security.AllowAllSecurityContext;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByAtom;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.HashJoinGroupByBuildChoiceRecordCursorFactory;
import io.questdb.mp.WorkerPoolMode;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.datetime.microtime.MicrosFormatUtils;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.mp.TestWorkerPool;
import io.questdb.test.tools.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static io.questdb.test.griffin.HashJoinGroupByQualificationTest.context;

/**
 * A fused hash join GROUP BY builds its hash table on the first demand for a row, never when its
 * cursor opens: EXPLAIN opens the cursor to initialize bind variables and dynamic intervals, and
 * LIMIT 0 opens it and reads nothing, so neither may pay for the build or fail on a build row.
 * <p>
 * Every test runs under both build modes and both payload layouts. The lifecycle cases run every
 * shape of the operator (INNER, LEFT and the RIGHT join that the planner turns into a LEFT one;
 * keyed and scalar; the lone count that counts by key; INT, SYMBOL, LONG and composite keys)
 * without workers, where the query's own thread runs every task, and with a pool of them.
 */
@RunWith(Parameterized.class)
public class HashJoinGroupByLazyBuildTest extends AbstractCairoTest {
    // A condition on the build input alone that fails to evaluate on lb's one row with j = 10:
    // 10 * 1000 does not fit DECIMAL(4,0). A query that carries it fails as soon as it builds.
    private static final String POISON = "(lb.j * 1000)::DECIMAL(4,0) > 0::DECIMAL(4,0)";
    // lb is the smaller table, so every shape builds it. {W} and {A} take a condition on lb alone,
    // which the build evaluates: as its scan's filter for an INNER join and as an ON filter otherwise.
    private static final String[] SHAPES = {
            "SELECT lp.g, count() c, sum(lb.w) w FROM lp JOIN lb ON lp.k = lb.k{W} GROUP BY lp.g",
            "SELECT count() c, sum(lb.w) w, sum(lp.v) v FROM lp JOIN lb ON lp.k = lb.k{W}",
            // The lone count of an INNER join, which takes each key's pairs from the key's row count.
            "SELECT count() FROM lp JOIN lb ON lp.k = lb.k{W}",
            "SELECT lp.g, count() c, sum(lb.w) w FROM lp LEFT JOIN lb ON lp.k = lb.k{A} GROUP BY lp.g",
            "SELECT count() c, sum(lb.w) w, sum(lp.v) v FROM lp LEFT JOIN lb ON lp.k = lb.k{A}",
            // The planner swaps the inputs of a RIGHT join and runs it as a LEFT one.
            "SELECT lp.g, count() c, sum(lb.w) w FROM lb RIGHT JOIN lp ON lp.k = lb.k{A} GROUP BY lp.g",
            "SELECT count() c, sum(lb.w) w, sum(lp.v) v FROM lb RIGHT JOIN lp ON lp.k = lb.k{A}",
            // A SYMBOL key, which the probe translates, and SYMBOL grouping keys of both inputs.
            "SELECT lp.c, lb.t, count() n, sum(lb.w) w FROM lp LEFT JOIN lb ON lp.s = lb.s{A} GROUP BY lp.c, lb.t",
            // Keys that stage into the build's map: a LONG, and an INT with a SYMBOL.
            "SELECT lp.g, count() c, sum(lb.w) w FROM lp LEFT JOIN lb ON lp.l = lb.l{A} GROUP BY lp.g",
            "SELECT count() c, sum(lb.w) w FROM lp JOIN lb ON lp.k = lb.k AND lp.s = lb.s{W}",
    };
    private final HashJoinBuildMode buildMode;
    private final HashJoinPayloadLayout payloadLayout;

    public HashJoinGroupByLazyBuildTest(HashJoinPayloadLayout payloadLayout, HashJoinBuildMode buildMode) {
        this.payloadLayout = payloadLayout;
        this.buildMode = buildMode;
    }

    @Parameterized.Parameters(name = "{0}-{1}")
    public static Collection<Object[]> parameters() {
        return HashJoinBuildMode.parameters();
    }

    @Before
    public void setUpPayloadLayoutAndBuildMode() {
        payloadLayout.apply(node1.getConfigurationOverrides());
        buildMode.applyProperties(node1.getConfigurationOverrides());
    }

    @Test
    public void testBuildCancellationFailsTheFirstReadAndReusesWithFiberWorkers() throws Exception {
        forEachShape(WorkerPoolMode.FIBER_HOST, this::assertBuildCancellationAndReuse);
    }

    @Test
    public void testBuildCancellationFailsTheFirstReadAndReusesWithLegacyWorkers() throws Exception {
        forEachShape(WorkerPoolMode.LEGACY, this::assertBuildCancellationAndReuse);
    }

    @Test
    public void testBuildCancellationFailsTheFirstReadAndReusesWithoutWorkers() throws Exception {
        forEachShape(null, this::assertBuildCancellationAndReuse);
    }

    @Test
    public void testBuildChoiceOpensEitherOrientationWithoutBuilding() throws Exception {
        assertMemoryLeak(() -> {
            createLifecycleTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = bindingContext()) {
                // An interval on lp makes the planner compile both orientations, and each execution
                // builds the input with fewer rows in its intervals: lb's 20,000, or what $1 leaves of lp.
                bindVariableService.clear();
                bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp("2020-02-01T00:00:00.000000Z"));
                final String sql = "SELECT lp.g, count() c, sum(lb.w) w FROM lp JOIN lb ON lp.k = lb.k"
                        + " WHERE lp.ts < $1 AND " + POISON + " GROUP BY lp.g";
                try (RecordCursorFactory factory = engine.select(sql, context);
                     RecordCursorFactory explain = engine.select("EXPLAIN " + sql, context);
                     RecordCursorFactory limited = engine.select(sql + " LIMIT 0", context)) {
                    final HashJoinGroupByBuildChoiceRecordCursorFactory choice = choice(factory);
                    for (boolean isAlternate : new boolean[]{false, true, false}) {
                        // The whole of lp, which leaves lb the smaller input, or lp's first 100 rows.
                        bindVariableService.setTimestamp(0, MicrosFormatUtils.parseTimestamp(
                                isAlternate ? "2020-01-01T01:40:00.000000Z" : "2020-02-01T00:00:00.000000Z"));
                        final AsyncHashJoinGroupByAtom primary = choice.getPrimary().getAtom();
                        final AsyncHashJoinGroupByAtom alternate = choice.getAlternate().getAtom();
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            // The choice counts the rows of each input's intervals and reads none of them.
                            Assert.assertEquals(isAlternate, choice.isAlternateChosen());
                            Assert.assertNull(primary.getFrozenBuild());
                            Assert.assertNull(alternate.getFrozenBuild());
                            // lb's poisoned row fails the first read: in the build when lb builds,
                            // and in the probe when it probes.
                            try {
                                cursor.hasNext();
                                Assert.fail("expected lb's poisoned row to fail the query");
                            } catch (CairoException | ImplicitCastException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                            }
                        }
                        Assert.assertNull(primary.getFrozenBuild());
                        Assert.assertNull(alternate.getFrozenBuild());
                        Assert.assertNull(context.getMemoryTracker());
                        // EXPLAIN and LIMIT 0 open the chosen orientation and read no row of it.
                        final String text = result(explain, context);
                        TestUtils.assertContains(text, "Hash Join Group By Build Choice");
                        TestUtils.assertContains(text, "buildFilter: ");
                        TestUtils.assertContains(text, "probeFilter: ");
                        try (RecordCursor cursor = limited.getCursor(context)) {
                            Assert.assertFalse(cursor.hasNext());
                            Assert.assertEquals(isAlternate, choice(limited).isAlternateChosen());
                            Assert.assertNull(choice(limited).getPrimary().getAtom().getFrozenBuild());
                            Assert.assertNull(choice(limited).getAlternate().getAtom().getFrozenBuild());
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testBuildMemoryLimitFailsTheFirstReadAndReusesWithFiberWorkers() throws Exception {
        forEachShape(WorkerPoolMode.FIBER_HOST, this::assertBuildMemoryLimitAndReuse);
    }

    @Test
    public void testBuildMemoryLimitFailsTheFirstReadAndReusesWithLegacyWorkers() throws Exception {
        forEachShape(WorkerPoolMode.LEGACY, this::assertBuildMemoryLimitAndReuse);
    }

    @Test
    public void testBuildMemoryLimitFailsTheFirstReadAndReusesWithoutWorkers() throws Exception {
        forEachShape(null, this::assertBuildMemoryLimitAndReuse);
    }

    @Test
    public void testExplainInitializesBindVariablesAndDynamicIntervalsWithoutBuilding() throws Exception {
        assertMemoryLeak(() -> {
            createLifecycleTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = bindingContext()) {
                // $1 feeds an aggregate, and $2 bounds a dynamic interval of the probe scan and one
                // of the build scan. The build's ON filter fails on lb's poisoned row.
                bindVariableService.clear();
                bindVariableService.setDouble(0, 0.5);
                bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
                final String sql = "SELECT lp.g, count() c, sum(lb.w + $1) w"
                        + " FROM lp LEFT JOIN (SELECT k, w, j, ts FROM lb WHERE ts >= $2) lb ON lp.k = lb.k AND " + POISON
                        + " WHERE lp.ts >= $2 AND lp.ts < dateadd('d', 2, $2) GROUP BY lp.g";
                // The plan names the payload copy only when some execution may copy.
                final String buildPayload = payloadLayout == HashJoinPayloadLayout.COPIED
                        ? "  buildPayload: copied when the probe is large enough\n" : "";
                // EXPLAIN opens the cursor, which initializes the functions and both scans'
                // intervals from the bind variables, and reads no row. Both bindings leave lb's
                // poisoned row, of 2020-01-06, inside the build's interval, and it stays unread.
                for (String day : new String[]{"2020-01-02", "2020-01-04"}) {
                    bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp(day + "T00:00:00.000000Z"));
                    final String nextDay = day.equals("2020-01-02") ? "2020-01-03" : "2020-01-05";
                    assertQuery("EXPLAIN " + sql)
                            .withContext(context)
                            .noLeakCheck()
                            .noRandomAccess()
                            .expectSize()
                            .returns("QUERY PLAN\n"
                                    + "Async Hash Join Group By workers: 4\n"
                                    + "  logicalJoinType: left outer\n"
                                    + "  physicalJoinType: left outer\n"
                                    + "  inputSwapped: false\n"
                                    + "  condition: lp.k=lb.k\n"
                                    + "  buildStrategy: shared\n"
                                    + buildPayload
                                    + "  keys: [g]\n"
                                    + "  values: [count(*),sum(lb.w+$0::double)]\n"
                                    + "  buildOnFilter: 0<j*1000::DECIMAL(4,0)\n"
                                    + "    Probe\n"
                                    + "        PageFrame\n"
                                    + "            Row forward scan\n"
                                    + "            Interval forward scan on: lp\n"
                                    + "              intervals: [(\"" + day + "T00:00:00.000000Z\",\"" + nextDay + "T23:59:59.999999Z\")]\n"
                                    + "    Build\n"
                                    + "        SelectedRecord\n"
                                    + "            PageFrame\n"
                                    + "                Row forward scan\n"
                                    + "                Interval forward scan on: lb\n"
                                    + "                  intervals: [(\"" + day + "T00:00:00.000000Z\",\"MAX\")]\n");
                }
                // The query itself opens as EXPLAIN does, and fails once it reads the poisoned row.
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertNull(atom.getFrozenBuild());
                        try {
                            cursor.hasNext();
                            Assert.fail("expected the build's ON filter to fail the query");
                        } catch (CairoException | ImplicitCastException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                        }
                    }
                    Assert.assertNull(atom.getFrozenBuild());
                    Assert.assertNull(context.getMemoryTracker());
                    // Each execution evaluates the build scan's interval again: from 2020-01-10 it
                    // leaves the poisoned row out, and the same factory answers.
                    bindVariableService.setTimestamp(1, MicrosFormatUtils.parseTimestamp("2020-01-10T00:00:00.000000Z"));
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertNull(atom.getFrozenBuild());
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertTrue(atom.getFrozenBuild().getRowCount() > 0);
                    }
                    Assert.assertNull(atom.getFrozenBuild());
                }
            }
        });
    }

    @Test
    public void testExplainPrintsThePlanWhenABuildRowFailsTheOnFilter() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE p (k INT, g INT)");
            execute("CREATE TABLE bm (k INT, j INT)");
            execute("INSERT INTO p VALUES (1, 1), (2, 1), (3, 2)");
            // 10 * 1000 does not fit DECIMAL(4,0), so the build's ON filter throws on the second build row.
            execute("INSERT INTO bm VALUES (1, 1), (2, 10), (3, 2)");
            try (SqlExecutionContextImpl context = enabledContext()) {
                final String sql = "SELECT p.g, count() c FROM p LEFT JOIN bm b ON p.k = b.k"
                        + " AND (b.j * 1000)::DECIMAL(4,0) > 0::DECIMAL(4,0) GROUP BY p.g";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    final String plan = plan(factory, context);
                    TestUtils.assertContains(plan, "Async Hash Join Group By");
                    TestUtils.assertContains(plan, "buildOnFilter");
                    // The query reads rows, so it runs the build and fails on the build row.
                    try {
                        result(factory, context);
                        Assert.fail("expected the build's ON filter to fail the query");
                    } catch (CairoException | ImplicitCastException e) {
                        TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                    }
                }
                // EXPLAIN opens the same cursor and reads no row of it.
                try (RecordCursorFactory explain = engine.select("EXPLAIN " + sql, context)) {
                    final String text = result(explain, context);
                    TestUtils.assertContains(text, "Async Hash Join Group By");
                    TestUtils.assertContains(text, "buildOnFilter");
                }
            }
        });
    }

    @Test
    public void testInitializationFailureSurfacesWhenTheCursorOpens() throws Exception {
        assertMemoryLeak(() -> {
            createLifecycleTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = bindingContext()) {
                // A pattern from a bind variable compiles when its filter initializes, and a
                // malformed one fails there with a SqlException, which only getCursor() can throw.
                // The filter sits, in turn, in each of the operator's four places: the build's ON
                // filter, the build scan's filter, the post-join filter and the probe scan's filter.
                final String[] queries = {
                        "SELECT lp.g, count() c FROM lp LEFT JOIN lb ON lp.k = lb.k AND lb.t::STRING ~ $1 GROUP BY lp.g",
                        "SELECT lp.g, count() c FROM lp JOIN lb ON lp.k = lb.k WHERE lb.t::STRING ~ $1 GROUP BY lp.g",
                        "SELECT lp.g, count() c FROM lp LEFT JOIN lb ON lp.k = lb.k WHERE lb.t::STRING ~ $1 GROUP BY lp.g",
                        "SELECT lp.g, count() c FROM lp LEFT JOIN lb ON lp.k = lb.k WHERE lp.c::STRING ~ $1 GROUP BY lp.g",
                };
                final String[] filters = {"buildOnFilter: ", "buildFilter: ", "postJoinFilter: ", "probeFilter: "};
                for (int i = 0; i < queries.length; i++) {
                    final String sql = queries[i];
                    bindVariableService.clear();
                    bindVariableService.setStr(0, "[tc][0-2]");
                    final List<String> expected = rowsOfOrdinaryPlan(engine, sql, context);
                    Assert.assertFalse(sql, expected.isEmpty());
                    try (RecordCursorFactory factory = engine.select(sql, context);
                         RecordCursorFactory explain = engine.select("EXPLAIN " + sql, context)) {
                        final String plan = plan(factory, context);
                        TestUtils.assertContains(plan, "Async Hash Join Group By");
                        TestUtils.assertContains(plan, filters[i]);
                        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                        for (int execution = 0; execution < 2; execution++) {
                            bindVariableService.setStr(0, "(");
                            try (RecordCursor ignored = factory.getCursor(context)) {
                                Assert.fail("expected the malformed pattern to fail the cursor's opening: " + sql);
                            } catch (SqlException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed group");
                            }
                            // EXPLAIN opens the same cursor, so it reports the same error.
                            try (RecordCursor ignored = explain.getCursor(context)) {
                                Assert.fail("expected the malformed pattern to fail EXPLAIN: " + sql);
                            } catch (SqlException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "Unclosed group");
                            }
                            Assert.assertNull(sql, atom.getFrozenBuild());
                            Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                            Assert.assertNull(sql, context.getMemoryTracker());
                            // The same factory answers once the pattern compiles.
                            bindVariableService.setStr(0, "[tc][0-2]");
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertNull(sql, atom.getFrozenBuild());
                                Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
                            }
                        }
                    }
                }
            }
        });
    }

    @Test
    public void testLifecycleWithFiberWorkers() throws Exception {
        forEachShape(WorkerPoolMode.FIBER_HOST, this::assertLifecycle);
    }

    @Test
    public void testLifecycleWithLegacyWorkers() throws Exception {
        forEachShape(WorkerPoolMode.LEGACY, this::assertLifecycle);
    }

    @Test
    public void testLifecycleWithoutWorkers() throws Exception {
        forEachShape(null, this::assertLifecycle);
    }

    @Test
    public void testLimitZeroDoesNotBuildUnderTheQueryMemoryLimit() throws Exception {
        assertMemoryLeak(() -> {
            createBuildTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                final String sql = "SELECT p.g, count() c, sum(b.w) s FROM p LEFT JOIN b ON p.k = b.k GROUP BY p.g";
                // The build keeps 100,000 rows of 16 bytes each, which no 256 KiB limit admits.
                setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 256 * 1024L);
                try (RecordCursorFactory factory = engine.select(sql + " LIMIT 0", context)) {
                    final String plan = plan(factory, context);
                    TestUtils.assertContains(plan, "Limit");
                    TestUtils.assertContains(plan, "Async Hash Join Group By");
                    for (int execution = 0; execution < 2; execution++) {
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertFalse(cursor.hasNext());
                            Assert.assertEquals(0, cursor.size());
                            Assert.assertNull(fused(factory).getAtom().getFrozenBuild());
                        }
                        Assert.assertNull(context.getMemoryTracker());
                    }
                }
                // The same limit still rejects the query that reads rows: the tracker covers the build.
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    TestUtils.assertContains(plan(factory, context), "Async Hash Join Group By");
                    try {
                        result(factory, context);
                        Assert.fail("expected the query memory limit error");
                    } catch (CairoException e) {
                        Assert.assertTrue(e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    Assert.assertNull(atom.getFrozenBuild());
                    Assert.assertEquals(0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                    Assert.assertNull(context.getMemoryTracker());
                }
            }
        });
    }

    @Test
    public void testLimitZeroFromABindVariableReadsNoBuildRow() throws Exception {
        assertMemoryLeak(() -> {
            createLifecycleTables(engine, sqlExecutionContext);
            try (SqlExecutionContextImpl context = bindingContext()) {
                final String sql = "SELECT lp.g, count() c, sum(lb.w) w FROM lp LEFT JOIN lb ON lp.k = lb.k AND " + POISON + " GROUP BY lp.g";
                // A literal zero, through the query assertion's whole battery of reads.
                assertQuery(sql + " LIMIT 0")
                        .withContext(context)
                        .noLeakCheck()
                        .expectSize()
                        .withPlanContaining("Limit", "Async Hash Join Group By", "buildOnFilter: ")
                        .returns("g\tc\tw\n");
                bindVariableService.clear();
                bindVariableService.setLong(0, 0);
                try (RecordCursorFactory factory = engine.select(sql + " LIMIT $1", context)) {
                    final String plan = plan(factory, context);
                    TestUtils.assertContains(plan, "Limit");
                    TestUtils.assertContains(plan, "Async Hash Join Group By");
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    for (int execution = 0; execution < 2; execution++) {
                        // Bound to zero, the limit opens the cursor and asks it for nothing.
                        bindVariableService.setLong(0, 0);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertFalse(cursor.hasNext());
                            Assert.assertNull(atom.getFrozenBuild());
                        }
                        // Bound to a row, it asks for the keyed result's size when it opens, which
                        // builds nothing, and then reads, which builds and fails on lb's poisoned row.
                        bindVariableService.setLong(0, 1);
                        try (RecordCursor cursor = factory.getCursor(context)) {
                            Assert.assertNull(atom.getFrozenBuild());
                            try {
                                cursor.hasNext();
                                Assert.fail("expected the build's ON filter to fail the query");
                            } catch (CairoException | ImplicitCastException e) {
                                TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                            }
                        }
                        Assert.assertNull(atom.getFrozenBuild());
                        Assert.assertNull(context.getMemoryTracker());
                    }
                }
            }
        });
    }

    @Test
    public void testOpenAndCloseWithoutReadingDoesNotBuild() throws Exception {
        assertMemoryLeak(() -> {
            createBuildTables();
            try (SqlExecutionContextImpl context = enabledContext()) {
                final String sql = "SELECT p.g, count() c, sum(b.w) s FROM p LEFT JOIN b ON p.k = b.k GROUP BY p.g";
                try (RecordCursorFactory factory = engine.select(sql, context)) {
                    TestUtils.assertContains(plan(factory, context), "Async Hash Join Group By");
                    final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                    final long baseline = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
                    for (int execution = 0; execution < 2; execution++) {
                        try (RecordCursor ignored = factory.getCursor(context)) {
                            // Opening the cursor neither builds nor allocates the build's memory.
                            Assert.assertNull(atom.getFrozenBuild());
                            Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                        }
                        Assert.assertNull(atom.getFrozenBuild());
                        Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                    }
                    // The first demand for a row builds, and closing the cursor releases the build.
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertTrue(cursor.hasNext());
                        Assert.assertEquals(100_000, atom.getFrozenBuild().getRowCount());
                        Assert.assertTrue(Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) > baseline);
                    }
                    Assert.assertNull(atom.getFrozenBuild());
                    Assert.assertEquals(baseline, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
                }
            }
        });
    }

    @Test
    public void testOpeningReadsNoBuildRowWithFiberWorkers() throws Exception {
        forEachShape(WorkerPoolMode.FIBER_HOST, this::assertOpeningReadsNoBuildRow);
    }

    @Test
    public void testOpeningReadsNoBuildRowWithLegacyWorkers() throws Exception {
        forEachShape(WorkerPoolMode.LEGACY, this::assertOpeningReadsNoBuildRow);
    }

    @Test
    public void testOpeningReadsNoBuildRowWithoutWorkers() throws Exception {
        forEachShape(null, this::assertOpeningReadsNoBuildRow);
    }

    private static void assertNotBuilt(String sql, AsyncHashJoinGroupByAtom atom, long idle) {
        Assert.assertNull(sql, atom.getFrozenBuild());
        Assert.assertFalse(sql, atom.isCountedByKey());
        Assert.assertFalse(sql, atom.isPayloadCopied());
        Assert.assertEquals(sql, 0, atom.getBuildFrameCount());
        Assert.assertEquals(sql, idle, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
    }

    private static HashJoinGroupByBuildChoiceRecordCursorFactory choice(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof HashJoinGroupByBuildChoiceRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (HashJoinGroupByBuildChoiceRecordCursorFactory) factory;
    }

    private static void createLifecycleTables(CairoEngine db, SqlExecutionContext context) throws SqlException {
        db.execute("CREATE TABLE lp (k INT, l LONG, s SYMBOL, g INT, c SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        db.execute("CREATE TABLE lb (k INT, l LONG, s SYMBOL, t SYMBOL, w DOUBLE, j INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY", context);
        // lp probes with 30,000 rows of 5,000 keys. lb builds from 20,000 rows of 4,000 keys, five
        // rows of each, and interns its key texts in another order, so equal text has different
        // symbol keys in the two tables. One row of lb carries the j that POISON fails on.
        db.execute("""
                INSERT INTO lp
                SELECT (x % 5_000)::INT, x % 5_000, ('s' || (x % 5_000))::SYMBOL, (x % 7)::INT, ('c' || (x % 3))::SYMBOL,
                    x * 0.5, timestamp_sequence('2020-01-01', 60_000_000)
                FROM long_sequence(30_000)
                """, context);
        db.execute("""
                INSERT INTO lb
                SELECT (x % 4_000)::INT, x % 4_000, ('s' || (4_000 - x % 4_000))::SYMBOL, ('t' || (x % 5))::SYMBOL,
                    x * 0.25, CASE WHEN x = 7_777 THEN 10 ELSE 1 END, timestamp_sequence('2020-01-01', 60_000_000)
                FROM long_sequence(20_000)
                """, context);
    }

    private static AsyncHashJoinGroupByRecordCursorFactory fused(RecordCursorFactory factory) {
        while (factory != null && !(factory instanceof AsyncHashJoinGroupByRecordCursorFactory)) {
            factory = factory.getBaseFactory();
        }
        Assert.assertNotNull(factory);
        return (AsyncHashJoinGroupByRecordCursorFactory) factory;
    }

    private static String healthy(String shape) {
        return shape.replace("{W}", "").replace("{A}", "");
    }

    private static String plan(RecordCursorFactory factory, SqlExecutionContext context) {
        TextPlanSink sink = new TextPlanSink();
        sink.of(factory, context);
        return sink.getSink().toString();
    }

    private static String poisoned(String shape) {
        return shape.replace("{W}", " WHERE " + POISON).replace("{A}", " AND " + POISON);
    }

    // The cursor's remaining rows, sorted: a keyed result comes in no particular order.
    private static List<String> readRows(RecordCursor cursor, RecordMetadata metadata) {
        final List<String> rows = new ArrayList<>();
        final StringSink sink = new StringSink();
        final Record record = cursor.getRecord();
        while (cursor.hasNext()) {
            sink.clear();
            CursorPrinter.println(record, metadata, sink, true);
            rows.add(sink.toString());
        }
        Collections.sort(rows);
        return rows;
    }

    private static String result(RecordCursorFactory factory, SqlExecutionContext context) throws Exception {
        try (RecordCursor cursor = factory.getCursor(context)) {
            StringSink sink = new StringSink();
            CursorPrinter.println(cursor, factory.getMetadata(), sink, true, true);
            return sink.toString();
        }
    }

    // The query's rows under the ordinary hash join plan, sorted.
    private static List<String> rowsOfOrdinaryPlan(CairoEngine db, String sql, SqlExecutionContextImpl context) throws Exception {
        context.setParallelHashJoinGroupByEnabled(false);
        try (RecordCursorFactory baseline = db.select(sql, context)) {
            Assert.assertFalse(sql, plan(baseline, context).contains("Hash Join Group By"));
            try (RecordCursor cursor = baseline.getCursor(context)) {
                return readRows(cursor, baseline.getMetadata());
            }
        } finally {
            context.setParallelHashJoinGroupByEnabled(true);
        }
    }

    /**
     * An execution that the breaker cancels fails on its first read, whether the cancellation lands
     * inside the build or once the build is frozen, and leaves nothing behind: the same factory
     * then answers the query.
     */
    private void assertBuildCancellationAndReuse(
            CairoEngine db,
            SqlExecutionContextImpl context,
            String shape,
            RecordCursorFactory factory,
            List<String> expected
    ) throws Exception {
        final String sql = healthy(shape);
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        final long idle = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
        for (boolean isInsideBuild : new boolean[]{true, false}) {
            final HashJoinBuildCircuitBreaker breaker = isInsideBuild
                    ? HashJoinBuildCircuitBreaker.cancellingInsideBuild(db)
                    : HashJoinBuildCircuitBreaker.cancellingOnceBuilt(db, () -> atom.getFrozenBuild() != null);
            context.with(breaker);
            try (RecordCursor cursor = factory.getCursor(context)) {
                // Opening the cursor builds nothing, so there is nothing to cancel yet.
                assertNotBuilt(sql, atom, idle);
                Assert.assertEquals(sql, 0, breaker.getBuildChecks());
                try {
                    cursor.hasNext();
                    Assert.fail("expected the first read to observe the cancellation: " + sql);
                } catch (CairoException e) {
                    Assert.assertTrue(sql + ": " + e.getMessage(), e.isCancellation());
                }
            } finally {
                context.with(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
            }
            Assert.assertTrue(sql, breaker.getBuildChecks() > 0);
            assertNotBuilt(sql, atom, idle);
            Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
            Assert.assertNull(sql, context.getMemoryTracker());
            try (RecordCursor cursor = factory.getCursor(context)) {
                Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
            }
            assertNotBuilt(sql, atom, idle);
        }
    }

    /**
     * A query memory limit that the build exceeds admits the open cursor, fails the first read, and
     * credits every byte back: the same factory answers the query once the limit is lifted.
     */
    private void assertBuildMemoryLimitAndReuse(
            CairoEngine db,
            SqlExecutionContextImpl context,
            String shape,
            RecordCursorFactory factory,
            List<String> expected
    ) throws Exception {
        final String sql = healthy(shape);
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        final long idle = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
        // The build keeps 20,000 rows of at least eight bytes each.
        setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 64 * 1024L);
        try {
            for (int execution = 0; execution < 2; execution++) {
                try (RecordCursor cursor = factory.getCursor(context)) {
                    // The limit covers this execution, and opening the cursor stays within it.
                    Assert.assertNotNull(sql, context.getMemoryTracker());
                    assertNotBuilt(sql, atom, idle);
                    Assert.assertEquals(sql, fused(factory).recordCursorSupportsRandomAccess() ? -1 : 1, cursor.size());
                    try {
                        cursor.hasNext();
                        Assert.fail("expected the build to exceed the query memory limit: " + sql);
                    } catch (CairoException e) {
                        Assert.assertTrue(sql + ": " + e.getMessage(), e.isOutOfMemory());
                        TestUtils.assertContains(e.getFlyweightMessage(), "query memory limit exceeded");
                    }
                }
                assertNotBuilt(sql, atom, idle);
                Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertNull(sql, context.getMemoryTracker());
            }
        } finally {
            setProperty(PropertyKey.CAIRO_QUERY_MEMORY_LIMIT_BYTES, 0);
        }
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
        }
        assertNotBuilt(sql, atom, idle);
    }

    /**
     * Opening and closing without a read, a partial read, a rewind and a second pass, a size
     * calculation, and another execution of the same factory after each of them. Last, an
     * execution without a read between two full reads.
     */
    private void assertLifecycle(
            CairoEngine db,
            SqlExecutionContextImpl context,
            String shape,
            RecordCursorFactory factory,
            List<String> expected
    ) throws Exception {
        final String sql = healthy(shape);
        final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
        final boolean isKeyed = fused(factory).recordCursorSupportsRandomAccess();
        final long idle = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
        Assert.assertFalse(sql, expected.isEmpty());
        // Open and close without a read: no build and none of its memory. size() and toTop() ask
        // for no row, so they start nothing either.
        for (int execution = 0; execution < 2; execution++) {
            try (RecordCursor cursor = factory.getCursor(context)) {
                assertNotBuilt(sql, atom, idle);
                Assert.assertEquals(sql, isKeyed ? -1 : 1, cursor.size());
                cursor.toTop();
                Assert.assertEquals(sql, 0, cursor.preComputedStateSize());
                assertNotBuilt(sql, atom, idle);
            }
            assertNotBuilt(sql, atom, idle);
        }
        // Read one row and close: the first read builds, and the close releases the build.
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertTrue(sql, cursor.hasNext());
            Assert.assertEquals(sql, 20_000, atom.getFrozenBuild().getRowCount());
            Assert.assertTrue(sql, atom.getBuildFrameCount() > 1);
            Assert.assertTrue(sql, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP) > idle);
        }
        assertNotBuilt(sql, atom, idle);
        // Read everything, rewind and read again: one build serves both passes.
        final HashJoinBuildCircuitBreaker breaker = HashJoinBuildCircuitBreaker.counting(db);
        context.with(breaker);
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, 0, breaker.getBuildChecks());
            Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
            final long buildChecks = breaker.getBuildChecks();
            Assert.assertTrue(sql, buildChecks > 0);
            final FrozenHashJoinBuild build = atom.getFrozenBuild();
            final long built = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            Assert.assertEquals(sql, expected.size(), cursor.size());
            cursor.toTop();
            Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
            cursor.toTop();
            Assert.assertTrue(sql, cursor.hasNext());
            Assert.assertEquals(sql, buildChecks, breaker.getBuildChecks());
            Assert.assertSame(sql, build, atom.getFrozenBuild());
            Assert.assertEquals(sql, 20_000, build.getRowCount());
            Assert.assertEquals(sql, built, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
        } finally {
            context.with(SqlExecutionCircuitBreaker.NOOP_CIRCUIT_BREAKER);
        }
        assertNotBuilt(sql, atom, idle);
        // calculateSize() asks for the rows, so it builds too.
        try (RecordCursor cursor = factory.getCursor(context)) {
            final RecordCursor.Counter counter = new RecordCursor.Counter();
            cursor.calculateSize(context.getCircuitBreaker(), counter);
            Assert.assertEquals(sql, expected.size(), counter.get());
            Assert.assertEquals(sql, 20_000, atom.getFrozenBuild().getRowCount());
        }
        assertNotBuilt(sql, atom, idle);
        // The same factory answers once more after all of the above.
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
        }
        assertNotBuilt(sql, atom, idle);
        // Open and close without a read right after a full read: the full read's build and counts
        // are gone, and the idle execution starts none of its own.
        try (RecordCursor cursor = factory.getCursor(context)) {
            assertNotBuilt(sql, atom, idle);
            Assert.assertEquals(sql, isKeyed ? -1 : 1, cursor.size());
            cursor.toTop();
            Assert.assertEquals(sql, 0, cursor.preComputedStateSize());
            assertNotBuilt(sql, atom, idle);
        }
        assertNotBuilt(sql, atom, idle);
        // The next full read builds anew and returns every row.
        try (RecordCursor cursor = factory.getCursor(context)) {
            Assert.assertEquals(sql, expected, readRows(cursor, factory.getMetadata()));
            Assert.assertEquals(sql, 20_000, atom.getFrozenBuild().getRowCount());
            Assert.assertEquals(sql, shape.startsWith("SELECT count() FROM"), atom.isCountedByKey());
        }
        assertNotBuilt(sql, atom, idle);
    }

    /**
     * With a condition that fails on a build row, the query fails on its first read, and every
     * way of opening its cursor without a read succeeds: opening and closing, EXPLAIN, and LIMIT 0.
     */
    private void assertOpeningReadsNoBuildRow(
            CairoEngine db,
            SqlExecutionContextImpl context,
            String shape,
            RecordCursorFactory ignoredFactory,
            List<String> ignoredExpected
    ) throws Exception {
        final String sql = poisoned(shape);
        // An INNER join's condition on lb is the build scan's filter, compiled or not; an outer
        // join's is the build's ON filter.
        final boolean isScanFilter = shape.contains("{W}");
        try (RecordCursorFactory factory = db.select(sql, context)) {
            final String plan = plan(factory, context);
            TestUtils.assertContains(plan, "Async Hash Join Group By");
            Assert.assertEquals(sql + "\n" + plan, isScanFilter, plan.contains("buildFilter: ") || plan.contains("buildJitFilter: "));
            Assert.assertEquals(sql + "\n" + plan, !isScanFilter, plan.contains("buildOnFilter: "));
            final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
            final long idle = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
            for (int execution = 0; execution < 2; execution++) {
                try (RecordCursor cursor = factory.getCursor(context)) {
                    assertNotBuilt(sql, atom, idle);
                    if (execution == 1) {
                        // The first read builds, and the build fails on lb's poisoned row.
                        try {
                            cursor.hasNext();
                            Assert.fail("expected the build to fail the query: " + sql);
                        } catch (CairoException | ImplicitCastException e) {
                            TestUtils.assertContains(e.getFlyweightMessage(), "inconvertible value: 10000 [INT -> DECIMAL(4,0)]");
                        }
                    }
                }
                assertNotBuilt(sql, atom, idle);
                Assert.assertEquals(sql, 0, atom.getPerWorkerLocks().getAcquiredSlotCount());
                Assert.assertNull(sql, context.getMemoryTracker());
            }
        }
        // EXPLAIN opens the cursor and reads no row of it.
        try (RecordCursorFactory explain = db.select("EXPLAIN " + sql, context)) {
            for (int execution = 0; execution < 2; execution++) {
                final String text = result(explain, context);
                TestUtils.assertContains(text, "Async Hash Join Group By");
                Assert.assertEquals(text, isScanFilter, text.contains("buildFilter: ") || text.contains("buildJitFilter: "));
                Assert.assertEquals(text, !isScanFilter, text.contains("buildOnFilter: "));
                Assert.assertEquals(sql, shape.startsWith("SELECT count() FROM"), text.contains("countStrategy: per key"));
            }
        }
        // LIMIT 0 opens the cursor and asks it for nothing.
        for (String limited : new String[]{sql + " LIMIT 0", "SELECT * FROM (" + sql + ") LIMIT 0"}) {
            try (RecordCursorFactory factory = db.select(limited, context)) {
                final String plan = plan(factory, context);
                TestUtils.assertContains(plan, "Limit");
                TestUtils.assertContains(plan, "Async Hash Join Group By");
                for (int execution = 0; execution < 2; execution++) {
                    try (RecordCursor cursor = factory.getCursor(context)) {
                        Assert.assertFalse(limited, cursor.hasNext());
                        Assert.assertEquals(limited, 0, cursor.size());
                        Assert.assertNull(limited, fused(factory).getAtom().getFrozenBuild());
                    }
                }
            }
        }
    }

    // A context whose queries read the test's bind variables.
    private SqlExecutionContextImpl bindingContext() {
        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4)
                .with(AllowAllSecurityContext.INSTANCE, bindVariableService, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    // p probes with 1,000 rows over ten groups; b is the LEFT join's build, with 100,000 rows.
    private void createBuildTables() throws Exception {
        execute("CREATE TABLE p (k INT, g INT)");
        execute("CREATE TABLE b (k INT, w DOUBLE)");
        execute("INSERT INTO p SELECT x::INT, (x % 10)::INT FROM long_sequence(1_000)");
        execute("INSERT INTO b SELECT (x % 2_000)::INT, x FROM long_sequence(100_000)");
    }

    private SqlExecutionContextImpl enabledContext() {
        SqlExecutionContextImpl context = new SqlExecutionContextImpl(engine, 4).with(AllowAllSecurityContext.INSTANCE, null, null, -1, null);
        context.setParallelGroupByEnabled(true);
        context.setParallelHashJoinGroupByEnabled(true);
        return context;
    }

    /**
     * Runs the check on every shape, on an engine of its own: without workers, where the query's
     * own thread runs every task, or with a pool of them.
     */
    private void forEachShape(@Nullable WorkerPoolMode mode, ShapeCheck check) throws Exception {
        assertMemoryLeak(() -> {
            final TestWorkerPool pool = mode == null ? null : new TestWorkerPool(4, mode);
            TestUtils.execute(pool, (db, compiler, ctx) -> {
                createLifecycleTables(db, ctx);
                try (SqlExecutionContextImpl context = context(db, 4)) {
                    // Day partitions of 1,440 rows in frames of up to 500, so that every build walks
                    // dozens of frames and a parallel one runs as many tasks.
                    context.changePageFrameSizes(500, 500);
                    for (String shape : SHAPES) {
                        final String sql = healthy(shape);
                        final boolean isLoneCount = shape.startsWith("SELECT count() FROM");
                        final List<String> expected = rowsOfOrdinaryPlan(db, sql, context);
                        try (RecordCursorFactory factory = db.select(sql, context)) {
                            final String plan = plan(factory, context);
                            final String label = sql + "\n" + plan;
                            Assert.assertTrue(label, plan.contains("Async Hash Join Group By"));
                            Assert.assertFalse(label, plan.contains("Build Choice"));
                            Assert.assertEquals(label, isLoneCount, plan.contains("countStrategy: per key"));
                            check.run(db, context, shape, factory, expected);
                            // A closed cursor no longer shows how its execution built, so one more
                            // execution confirms that the check ran the shape under this test's mode.
                            try (RecordCursor cursor = factory.getCursor(context)) {
                                Assert.assertEquals(label, expected, readRows(cursor, factory.getMetadata()));
                                final AsyncHashJoinGroupByAtom atom = fused(factory).getAtom();
                                Assert.assertEquals(label, buildMode == HashJoinBuildMode.PARALLEL, atom.isBuiltInRounds());
                                Assert.assertEquals(label, buildMode == HashJoinBuildMode.PARALLEL, atom.getBuildPartitionCount() > 1);
                                Assert.assertEquals(label, isLoneCount, atom.isCountedByKey());
                                // The lone count's unfiltered INT key leaves a build in rounds its rows in
                                // the frames' regions, and frames of 500 rows make their runs too short
                                // for the count's tasks, so the owner counts them in either mode.
                                if (isLoneCount) {
                                    Assert.assertEquals(label, buildMode == HashJoinBuildMode.PARALLEL, atom.hasBuildFrameRegions());
                                }
                                Assert.assertFalse(label, atom.isCountedInRounds());
                                // The lone count has no payload column to copy.
                                Assert.assertEquals(label, payloadLayout == HashJoinPayloadLayout.COPIED && !isLoneCount, atom.isPayloadCopied());
                            }
                        }
                        Assert.assertNull(sql, context.getMemoryTracker());
                    }
                }
            }, configuration, LOG);
        });
    }

    @FunctionalInterface
    private interface ShapeCheck {
        void run(
                CairoEngine db,
                SqlExecutionContextImpl context,
                String shape,
                RecordCursorFactory factory,
                List<String> expected
        ) throws Exception;
    }
}
