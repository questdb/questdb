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

package io.questdb.test.cairo.covering;

import io.questdb.PropertyKey;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.engine.table.CoveringIndexRecordCursorFactory;
import io.questdb.std.Chars;
import io.questdb.std.str.StringSink;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * What a covering factory carrying a backup advertises as its scan direction.
 * <p>
 * The two delegates need not agree. The covering scan is row-id ordered, so on its own it is
 * ascending by designated timestamp; the WHERE IN-list backup is a
 * {@code FilterOnValuesRecordCursorFactory}, which under {@code ORDER_BY_INVARIANT} drains one
 * per-key cursor after another and so emits by key, not by row id. Codegen elides an ORDER BY on
 * the designated timestamp from whatever the factory answers here, at compile time, before either
 * delegate is chosen -- so answering FORWARD for the pair would silently return key-grouped rows
 * whenever the backup ran.
 * <p>
 * The same backup is also why such a factory must REFUSE the timestamp-ordering opt-out -- see
 * {@link #testBackupCarryingScanRefusesTheOrderingOptOut()}.
 */
public class CoveringIndexBackupScanDirectionTest extends AbstractCairoTest {

    @Override
    @Before
    public void setUp() {
        // The offer is only ever made by a PARALLEL group by, so the end-to-end case needs it on
        // for its positive control to mean anything.
        setProperty(PropertyKey.CAIRO_SQL_PARALLEL_GROUPBY_ENABLED, "true");
        super.setUp();
    }

    /**
     * A covering factory carrying a backup must refuse {@code tryDisableTimestampOrdering()}.
     * <p>
     * Per-key (unordered) mode lives ONLY on the page-frame path, and a factory carrying a backup
     * advertises no page-frame cursor at all -- {@code supportsPageFrameCursor()} conjoins
     * {@code backup == null}, because the compiler cannot know which of the two delegates will
     * run. Granting the permission to such a factory would let it advertise
     * {@code SCAN_DIRECTION_OTHER} and print {@code frames: per-key (unordered)} in {@code EXPLAIN}
     * for a mode it can never execute: the permission would outrun the capability.
     * <p>
     * <b>Why this is asked here and not through a query.</b> No consumer can reach the bad
     * combination today. Both {@code SqlCodeGenerator#offerUnorderedScan} call sites, and the
     * vectorized site that calls {@code tryDisableTimestampOrdering()} directly, sit behind a
     * {@code supportsPageFrameCursor()} test, so a backup-carrying base is routed to the serial
     * group by before any offer is made. That mutual exclusion is a property of ROUTING, though,
     * not a property either party declares -- and upstream has already moved
     * {@code supportsPageFrameCursor()} once (it is the change that made a backup answer false).
     * So the refusal is asserted at the factory, where it is stated, rather than through a plan
     * that would keep passing on the strength of the routing alone.
     * <p>
     * The last two arms are the non-vacuity guards. The multi-key control MUST accept, so a
     * refusal that started rejecting everything fails here instead of passing twice; and the plan
     * control MUST print the attr, so the "plan does not print per-key" assertion is known to be
     * looking for a string this build can actually emit.
     */
    @Test
    public void testBackupCarryingScanRefusesTheOrderingOptOut() throws Exception {
        assertMemoryLeak(() -> {
            createTopTable("t_optout_backup");
            createPlainTable("t_optout_plain");

            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT ts, sym, val FROM t_optout_backup WHERE sym IN (null, 'A')",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final CoveringIndexRecordCursorFactory covering = findCovering(factory);
                Assert.assertNotNull("no covering factory in the plan", covering);
                // Non-vacuity: this shape is only interesting while it really is the
                // no-page-frames one. If a future change gives a backup-carrying factory page
                // frames, the guard under test is no longer the thing being exercised.
                Assert.assertFalse(
                        "this shape stopped carrying a backup (or a backup stopped suppressing page"
                                + " frames), so the refusal below is no longer the one under test",
                        covering.supportsPageFrameCursor()
                );
                Assert.assertFalse(
                        "a covering factory that cannot produce page frames accepted the"
                                + " timestamp-ordering opt-out. Per-key mode exists only on the"
                                + " page-frame path, so it has granted a permission for a mode it can"
                                + " never run -- and will then advertise SCAN_DIRECTION_OTHER and print"
                                + " \"frames: per-key (unordered)\" for it.",
                        covering.tryDisableTimestampOrdering(false, null)
                );
                // The refusal has to leave the declarations alone, not merely return false: a
                // guard placed after the field write would return false and still have granted it.
                Assert.assertEquals(
                        "the refused opt-out still moved the scan direction, so the permission was"
                                + " written before the refusal returned",
                        RecordCursorFactory.SCAN_DIRECTION_FORWARD,
                        covering.getScanDirection()
                );
                planSink.of(covering, sqlExecutionContext);
                Assert.assertFalse(
                        "EXPLAIN advertises per-key (unordered) frames for a base that has no"
                                + " page-frame cursor to run them on: " + planSink.getSink(),
                        Chars.contains(planSink.getSink(), "per-key (unordered)")
                );
            }

            // Control: the same call on a multi-key covering scan that CAN honour the opt-out
            // must succeed, so the refusal above is the guard and not a blanket false.
            try (
                    SqlCompiler compiler = engine.getSqlCompiler();
                    RecordCursorFactory factory = compiler.compile(
                            "SELECT ts, sym, val FROM t_optout_plain WHERE sym IN ('A', 'B')",
                            sqlExecutionContext
                    ).getRecordCursorFactory()
            ) {
                final CoveringIndexRecordCursorFactory covering = findCovering(factory);
                Assert.assertNotNull("no covering factory in the control plan", covering);
                Assert.assertTrue(
                        "the backup-free multi-key control refused the opt-out too, so the refusal"
                                + " above proves nothing -- tryDisableTimestampOrdering() is answering"
                                + " false to everything",
                        covering.tryDisableTimestampOrdering(false, null)
                );
                planSink.of(covering, sqlExecutionContext);
                Assert.assertTrue(
                        "the control did not print the attr the assertion above searches for, so"
                                + " that assertion cannot fail: " + planSink.getSink(),
                        Chars.contains(planSink.getSink(), "per-key (unordered)")
                );
            }
        });
    }

    @Test
    public void testBackupKeepingForwardOrderIsStillForward() throws Exception {
        // The control. ORDER BY ts leaves the backup on its heap cursor, which merges the per-key
        // streams into row-id order, so both delegates are FORWARD and the pair keeps it. Without
        // this case a getScanDirection() hard-wired to OTHER would pass the case below.
        assertMemoryLeak(() -> {
            createTopTable("t_sd_fwd");
            assertScanDirection(
                    "SELECT ts, sym, val FROM t_sd_fwd WHERE sym IN (null, 'A') ORDER BY ts",
                    RecordCursorFactory.SCAN_DIRECTION_FORWARD
            );
        });
    }

    @Test
    public void testBackupOrderingByKeyMakesThePairUnordered() throws Exception {
        // ORDER BY on the key column, not the timestamp: the IN-list backup takes its sequential
        // cursor and emits per key. The covering delegate would still be FORWARD, so the pair has
        // to answer OTHER.
        assertMemoryLeak(() -> {
            createTopTable("t_sd_other");
            assertScanDirection(
                    "SELECT ts, sym, val FROM t_sd_other WHERE sym IN (null, 'A') ORDER BY sym",
                    RecordCursorFactory.SCAN_DIRECTION_OTHER
            );
        });
    }

    /**
     * The end-to-end half of {@link #testBackupCarryingScanRefusesTheOrderingOptOut()}: a real
     * GROUP BY over a backup-carrying covering scan must neither advertise per-key nor lose rows.
     * <p>
     * Unlike the factory-level test this one cannot fail on the guard alone -- it passes with the
     * guard removed, because the offer never reaches this base. It is here to pin the ROUTING the
     * guard backs up: if a future change lets a page-frame-less base under a parallel group by,
     * this starts printing the attr and fails, pointing at the routing rather than at the
     * factory.
     */
    @Test
    public void testGroupByOverABackupCarryingScanNeverAdvertisesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createTopTable("t_gb_backup");
            createPlainTable("t_gb_plain");

            // Positive control FIRST: the same query shape over a backup-free table must print
            // the attr. Without it, a configuration in which no group by ever takes the offer
            // would make the negative assertion below pass while proving nothing.
            final StringSink controlPlan = new StringSink();
            printSql(
                    "EXPLAIN SELECT sym, sum(val) FROM t_gb_plain WHERE sym IN ('A', 'B')"
                            + " GROUP BY sym ORDER BY sym",
                    controlPlan
            );
            Assert.assertTrue(
                    "the backup-free control did not take the offer either, so the negative"
                            + " assertion below cannot fail: " + controlPlan,
                    Chars.contains(controlPlan, "per-key (unordered)")
            );

            final String sql = "SELECT sym, sum(val) FROM t_gb_backup WHERE sym IN (null, 'A')"
                    + " GROUP BY sym ORDER BY sym";

            final StringSink plan = new StringSink();
            printSql("EXPLAIN " + sql, plan);
            Assert.assertFalse(
                    "a group by over a base with no page-frame cursor advertised per-key"
                            + " (unordered) frames. Either the base was granted a permission it"
                            + " cannot honour, or the routing that used to keep such a base off the"
                            + " parallel group-by path has moved: " + plan,
                    Chars.contains(plan, "per-key (unordered)")
            );

            // ... and it still answers, against a full-scan reference. A guard that refused the
            // offer by breaking the query would pass the assertion above.
            assertSqlCursors(sql.replaceFirst("SELECT ", "SELECT /*+ no_index */ "), sql);
        });
    }

    /**
     * Compiles {@code sql} and asserts the scan direction of the {@link CoveringIndexRecordCursorFactory}
     * inside it. The factory is found by walking the base chain rather than cast from the top: an
     * ORDER BY puts a sort above it, and the plan shape is not what this test is pinning.
     */
    private static void assertScanDirection(String sql, int expectedScanDirection) throws Exception {
        try (
                SqlCompiler compiler = engine.getSqlCompiler();
                RecordCursorFactory factory = compiler.compile(sql, sqlExecutionContext).getRecordCursorFactory()
        ) {
            final CoveringIndexRecordCursorFactory covering = findCovering(factory);
            Assert.assertNotNull("no covering factory in the plan for: " + sql, covering);
            Assert.assertEquals(
                    "the covering factory and its backup disagree on row order, so the pair must"
                            + " advertise none; plan: " + sql,
                    expectedScanDirection,
                    covering.getScanDirection()
            );
        }
    }

    /**
     * The twin of {@link #createTopTable}, written so no partition carries a column top. Every
     * key in the IN-list resolves, so the factory is built WITHOUT a backup and keeps its
     * multi-key page-frame cursor -- the control shape for the opt-out.
     */
    private static void createPlainTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, sym SYMBOL INDEX TYPE POSTING INCLUDE (ts, val), val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("INSERT INTO " + name + " SELECT (x * 600000000L)::timestamp,"
                + " CASE WHEN x % 2 = 0 THEN 'A' ELSE 'B' END, x::double"
                + " FROM long_sequence(1000)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    /**
     * Two rows written before {@code sym} exists carry a column top and match the NULL key
     * implicitly, so the IN-list below is null-capable and the factory is built with a backup.
     */
    private static void createTopTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE)"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0),
                ('2024-01-01T01:00:00', 20.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("INSERT INTO " + name + " VALUES ('2024-01-01T02:00:00', 30.0, 'A')");
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (ts, val)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    /**
     * The first {@link CoveringIndexRecordCursorFactory} on the {@code getBaseFactory()} chain, or
     * null when there is none -- which every caller asserts against rather than dereferencing, so
     * a query that stopped routing through the covering index fails rather than proves nothing.
     * Bounded, so a factory that returns itself as its base fails the test rather than hangs it.
     */
    private static CoveringIndexRecordCursorFactory findCovering(RecordCursorFactory factory) {
        RecordCursorFactory f = factory;
        for (int i = 0; f != null && i < 64; i++) {
            if (f instanceof CoveringIndexRecordCursorFactory c) {
                return c;
            }
            f = f.getBaseFactory();
        }
        return null;
    }
}
