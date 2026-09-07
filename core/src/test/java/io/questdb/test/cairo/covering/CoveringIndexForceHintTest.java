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

package io.questdb.test.cairo.covering;

import io.questdb.cairo.CairoException;
import io.questdb.cairo.sql.PageFrameCursor;
import io.questdb.cairo.sql.PartitionFrameCursorFactory;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The {@code /*+ force_use_covering *}{@code /} hint. Any key that might be NULL -- a literal
 * {@code null}, or a bind variable whose value is not known until it is bound -- gets a backup
 * plan, and a covering factory that carries one reports no page-frame cursor, costing parallel
 * filter and vectorized GROUP BY. On a table whose indexed column has existed since its first
 * partition there is nothing to defer to and that cost buys nothing. The hint is how a query
 * says so.
 * <p>
 * It is a promise about the COLUMN, not the key: no partition carries a top for it. Whether that
 * holds is runtime state the planner cannot check, so it takes the query's word for it -- and
 * then checks it per open. A key that does resolve to NULL over a table that does carry a top
 * throws, rather than answer from a sidecar that holds no value for those rows.
 */
public class CoveringIndexForceHintTest extends AbstractCairoTest {

    @Test
    public void testHintAppliesToLiteralNullKey() throws Exception {
        // The hint speaks about the column, not the key, so a literal null takes it too: no
        // backup is built. Here the promise is false -- the table does carry a top -- so the
        // open throws instead of answering from a sidecar that holds nothing for those rows.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_lit");
            // No plan assertion here: EXPLAIN opens the cursor, so it trips the same throw.
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_lit WHERE sym = null");
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_lit WHERE sym IN (null, 'A')");
        });
    }

    @Test
    public void testHintServesLiteralNullKeyWithoutColumnTop() throws Exception {
        // The same literal null where the promise holds: sym has existed since the table's first
        // partition, so every NULL row has a posting and the covering scan answers it with no
        // backup and no throw.
        assertMemoryLeak(() -> {
            createFlatTable("t_fc_lit_flat");
            final String sql = "SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_lit_flat WHERE sym = null ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanNotContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T02:00:00.000000Z\t\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("/*+ force_use_covering */", "/*+ no_covering */"));
        });
    }

    @Test
    public void testHintKeepsPageFramesForBoundKey() throws Exception {
        // Without the hint a bind-variable key always gets a backup, and a factory that carries
        // one reports no page-frame cursor. With it, the covering factory is the only plan and
        // the page-frame cursor -- and the parallel GROUP BY above it -- comes back.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_frames");
            bindVariableService.setStr(0, "A");
            final String projection = "SELECT %s sym, sum(val) total, avg(-1) marker FROM t_fc_frames WHERE sym = $1";
            assertQuery(projection.formatted(""))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("backup: true")
                    .withPlanNotContaining("Async Group By")
                    .returns("sym\ttotal\tmarker\nA\t30.0\t-1.0\n");
            assertQuery(projection.formatted("/*+ force_use_covering */"))
                    .noLeakCheck()
                    .expectSize()
                    .withPlanContaining("Async Group By")
                    .withPlanNotContaining("backup: true")
                    .returns("sym\ttotal\tmarker\nA\t30.0\t-1.0\n");
        });
    }

    @Test
    public void testHintServesBoundNonNullKeyOverColumnTop() throws Exception {
        // The promise held: the bound key is not NULL, so the covering scan answers it from the
        // sidecar exactly as it does without the hint. Cross-checked against the plain plan.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_ok");
            bindVariableService.setStr(0, "A");
            final String sql = "SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_ok WHERE sym = $1 ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanNotContaining("backup: true")
                    .returns("ts\tsym\tval\n2024-01-01T02:00:00.000000Z\tA\t30.0\n");
            assertSqlCursors(sql, sql.replace("/*+ force_use_covering */", "/*+ no_covering */"));
        });
    }

    @Test
    public void testHintThrowsWhenBoundKeyIsNullOverColumnTop() throws Exception {
        // The promise broken. Without the hint this open takes the backup; with it there is no
        // backup to take, and the sidecar holds no value for a row below the column top. It has
        // to throw rather than answer with dropped rows or fabricated NULLs.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_throw");
            bindVariableService.setStr(0, null);
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_throw WHERE sym = $1");
            // The IN-list site enforces it the same way.
            assertThrowsForcedNullKey("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_throw WHERE sym IN ($1, 'B')");
        });
    }

    @Test
    public void testHintServesBoundNullKeyWithoutColumnTop() throws Exception {
        // The other half of the promise: the key IS NULL, but no partition carries a top, so
        // every matching row has a posting and the covering scan answers it. No throw, no
        // backup, and the same rows the plain plan returns.
        assertMemoryLeak(() -> {
            createFlatTable("t_fc_flat");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_flat WHERE sym = $1 ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .timestamp("ts")
                    .withPlanNotContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T02:00:00.000000Z\t\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("/*+ force_use_covering */", "/*+ no_covering */"));
        });
    }

    @Test
    public void testHintThrowsFromPageFrameCursorWhenBoundKeyIsNull() throws Exception {
        // The promise is enforced on both openings, and getPageFrameCursor() is the one the
        // hint exists to keep reachable -- parallel filter and vectorized GROUP BY come in
        // through it, never through getCursor(). Same tables and same shape as
        // testHintKeepsPageFramesForBoundKey; only the bound value changes.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_pf_throw");
            bindVariableService.setStr(0, null);
            assertThrowsForcedNullKeyOnPageFrames("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_pf_throw WHERE sym = $1");
            // The IN-list cursor has its own branch and its own check.
            assertThrowsForcedNullKeyOnPageFrames("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_pf_throw WHERE sym IN ($1, 'B')");
        });
    }

    private static void assertThrowsForcedNullKey(String sql) throws Exception {
        try (
                RecordCursorFactory factory = select(sql);
                RecordCursor cursor = factory.getCursor(sqlExecutionContext)
        ) {
            //noinspection StatementWithEmptyBody
            while (cursor.hasNext()) {
                // drain
            }
            Assert.fail("expected a CairoException naming the force_use_covering hint");
        } catch (CairoException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), "force_use_covering");
        }
    }

    private static void assertThrowsForcedNullKeyOnPageFrames(String sql) throws Exception {
        try (RecordCursorFactory factory = select(sql)) {
            Assert.assertTrue(
                    "the hint has to leave the page-frame cursor reachable, or this asserts nothing",
                    factory.supportsPageFrameCursor()
            );
            try (PageFrameCursor cursor = factory.getPageFrameCursor(sqlExecutionContext, PartitionFrameCursorFactory.ORDER_ASC)) {
                Assert.fail("expected a CairoException naming the force_use_covering hint, got " + cursor);
            } catch (CairoException e) {
                TestUtils.assertContains(e.getFlyweightMessage(), "force_use_covering");
            }
        }
    }

    /**
     * The same shape with no column top at all: {@code sym} has existed since the table's first
     * partition, so every row -- NULL or not -- has a posting.
     */
    private static void createFlatTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE,"
                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (ts, val))"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0, NULL),
                ('2024-01-01T01:00:00', 20.0, 'A'),
                ('2024-01-01T02:00:00', 30.0, NULL)
                """.formatted(name));
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    /**
     * Two rows written before {@code sym} exists -- they carry a column top and match the NULL
     * key implicitly -- then one row with a real key above it.
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
}
