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
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * The {@code /*+ force_use_covering *}{@code /} hint. A bind-variable key is
 * null-capable-but-unknown at compile time, so it always gets a backup plan and a factory that
 * carries one reports no page-frame cursor -- costing parallel filter and vectorized GROUP BY
 * even on a table with no column top anywhere. The hint suppresses the backup for that case
 * only.
 * <p>
 * Two things it deliberately does not do. It does not act on a literal {@code null}, whose
 * nullness the compiler can already see, so the promise would be visibly false. And it is not
 * trusted: an open whose bound key does resolve to NULL over a table carrying a column top
 * throws rather than answer from a sidecar that holds no value for those rows.
 */
public class CoveringIndexForceHintTest extends AbstractCairoTest {

    @Test
    public void testHintIgnoredForLiteralNullKey() throws Exception {
        // The compiler resolves 'null' to VALUE_IS_NULL right here, so the hint is a promise it
        // can already see is false. The backup is built anyway and the rows come back.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_lit");
            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_lit WHERE sym = null ORDER BY ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    // The backup's cursor implements getRecordB() though its factory declares none.
                    .skipRandomAccessProbe()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex backup: true on: sym with: ts, val")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """);
        });
    }

    @Test
    public void testHintIgnoredForLiteralNullInList() throws Exception {
        // One literal null anywhere in the IN-list is enough: the whole list keeps its backup,
        // even though the other element is a bind variable the hint would otherwise cover.
        assertMemoryLeak(() -> {
            createTopTable("t_fc_lit_in");
            bindVariableService.setStr(0, "A");
            assertQuery("SELECT /*+ force_use_covering */ ts, sym, val FROM t_fc_lit_in WHERE sym IN (null, $1) ORDER BY ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .timestamp("ts")
                    .withPlanContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-01T02:00:00.000000Z\tA\t30.0
                            """);
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
            execute("CREATE TABLE t_fc_flat (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE POSTING INCLUDE (ts, val))"
                    + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_fc_flat VALUES
                    ('2024-01-01T00:00:00', 10.0, NULL),
                    ('2024-01-01T01:00:00', 20.0, 'A'),
                    ('2024-01-01T02:00:00', 30.0, NULL)
                    """);
            engine.releaseAllWriters();
            engine.releaseAllReaders();

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
