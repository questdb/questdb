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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * A covered read whose key arrives in a bind variable must resolve NULL to the NULL key.
 * {@code SymbolMapReader.keyOf(null)} already answers {@code VALUE_IS_NULL}, which the posting
 * chain carries postings for; reporting the key as {@code VALUE_NOT_FOUND} instead makes the
 * cursor treat it as an unknown symbol and drop every matching row.
 * <p>
 * Each case is cross-checked against the spellings that reach the same rows by another route --
 * {@code /*+ no_covering *}{@code /}, and the literal {@code sym = null}.
 */
public class CoveringIndexBindVariableKeyTest extends AbstractCairoTest {

    @Test
    public void testBoundNullKeyLatestOn() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t_bv_latest");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT sym, val FROM t_bv_latest WHERE sym = $1 LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .returns("""
                            sym\tval
                            \t50.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
            // ... and against the literal spelling, which must reach the same row.
            assertSqlCursors(
                    sql,
                    "SELECT /*+ no_covering */ sym, val FROM t_bv_latest WHERE sym = null LATEST ON ts PARTITION BY sym"
            );
        });
    }

    @Test
    public void testBoundNullKeyLatestOnOverColumnTop() throws Exception {
        // The bound NULL key over a partition that carries a column top: the covering scan
        // has no posting to decode there, so the factory runs its backup, which is the
        // plain LATEST ON index scan. That scan resolved a bound NULL key with
        // "symbolKey + 1" instead of TableUtils.toIndexKey(), which sends VALUE_IS_NULL
        // (Integer.MIN_VALUE) to an index key nothing matches -- so the query returned
        // nothing while the literal "sym = null" returned the row.
        assertMemoryLeak(() -> {
            createTopTable("t_bv_top");

            bindVariableService.setStr(0, null);
            final String sql = "SELECT sym, val FROM t_bv_top WHERE sym = $1 LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    // The backup is an index-scan factory: it declares no random access but
                    // its cursor implements getRecordB() anyway.
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .withPlanContaining("CoveringIndex backup: true op: latest on: sym with: val")
                    .returns("""
                            sym\tval
                            \t20.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
            assertSqlCursors(
                    sql,
                    "SELECT /*+ no_covering */ sym, val FROM t_bv_top WHERE sym = null LATEST ON ts PARTITION BY sym"
            );
        });
    }

    @Test
    public void testBoundNullKeyMultiKeyMerge() throws Exception {
        // The IN-list merge resolves each element separately, so the NULL element has to
        // survive alongside a real one and merge with it on row id.
        assertMemoryLeak(() -> {
            createTable("t_bv_in");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT ts, sym, val FROM t_bv_in WHERE sym IN ($1, 'A') ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\tA\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-01T03:00:00.000000Z\t\t40.0
                            2024-01-02T00:00:00.000000Z\tA\t30.0
                            2024-01-02T01:00:00.000000Z\t\t50.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testBoundNullKeySingleKey() throws Exception {
        assertMemoryLeak(() -> {
            createTable("t_bv");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT ts, sym, val FROM t_bv WHERE sym = $1 ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-01T03:00:00.000000Z\t\t40.0
                            2024-01-02T01:00:00.000000Z\t\t50.0
                            """);
            // The two spellings that already worked must still agree with it.
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
            assertSqlCursors(sql, "SELECT ts, sym, val FROM t_bv WHERE sym = null ORDER BY ts");
        });
    }

    @Test
    public void testBoundNonNullKeyUnchanged() throws Exception {
        // The fix must not disturb a bound key that does resolve to a real symbol, nor one
        // that resolves to no symbol at all.
        assertMemoryLeak(() -> {
            createTable("t_bv_nn");
            bindVariableService.setStr(0, "A");
            assertQuery("SELECT ts, sym, val FROM t_bv_nn WHERE sym = $1 ORDER BY ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\tA\t10.0
                            2024-01-02T00:00:00.000000Z\tA\t30.0
                            """);
            bindVariableService.setStr(0, "NOT_A_SYMBOL");
            assertQuery("SELECT ts, sym, val FROM t_bv_nn WHERE sym = $1 ORDER BY ts")
                    .noLeakCheck()
                    .noRandomAccess()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex")
                    .returns("ts\tsym\tval\n");
        });
    }

    @Test
    public void testBoundNullKeyLatestOnInListOverColumnTop() throws Exception {
        // The multi-key LATEST ON backup. Codegen builds it whenever an IN list can carry a
        // NULL key, and it hands the shared filter and the key functions over to it; nothing
        // else drives that construction end to end.
        assertMemoryLeak(() -> {
            createTopTable("t_bv_latest_in_top");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT sym, val FROM t_bv_latest_in_top WHERE sym IN ($1, 'A') LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .withPlanContaining("backup: true")
                    .returns("""
                            sym\tval
                            \t20.0
                            A\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testBoundNullKeyLatestOnPlainBitmapIndex() throws Exception {
        // The toIndexKey fix lives in LatestByValueDeferredIndexedRowCursorFactory, which is the
        // general deferred LATEST ON path -- no covering index involved. A bound NULL key has to
        // reach the same row the literal spelling reaches.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t_bv_bitmap (ts TIMESTAMP, val DOUBLE,"
                    + " sym SYMBOL INDEX TYPE BITMAP) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
            execute("""
                    INSERT INTO t_bv_bitmap VALUES
                    ('2024-01-01T00:00:00', 10.0, 'A'),
                    ('2024-01-01T01:00:00', 20.0, NULL),
                    ('2024-01-02T00:00:00', 30.0, 'A'),
                    ('2024-01-02T01:00:00', 50.0, NULL)
                    """);
            bindVariableService.setStr(0, null);
            final String sql = "SELECT sym, val FROM t_bv_bitmap WHERE sym = $1 LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .sizeMayVary()
                    .returns("""
                            sym\tval
                            \t50.0
                            """);
            assertSqlCursors(sql, "SELECT sym, val FROM t_bv_bitmap WHERE sym = null LATEST ON ts PARTITION BY sym");
        });
    }

    @Test
    public void testBoundNullKeyWhereInListOverColumnTop() throws Exception {
        // The IN-list WHERE site: an element that may bind to NULL builds a backup there too,
        // and the column top is what makes the open actually run it.
        assertMemoryLeak(() -> {
            createTopTable("t_bv_in_top");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT ts, sym, val FROM t_bv_in_top WHERE sym IN ($1, 'A')";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-01T02:00:00.000000Z\tA\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testBoundNullKeyWhereOverColumnTop() throws Exception {
        // The single-key WHERE site. Its sibling test drives LATEST ON; this one drives the
        // plain scan, where the backup has to answer the rows below the top from their own
        // columns rather than from a sidecar that holds nothing for them.
        assertMemoryLeak(() -> {
            createTopTable("t_bv_where_top");
            bindVariableService.setStr(0, null);
            final String sql = "SELECT ts, sym, val FROM t_bv_where_top WHERE sym = $1";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("CoveringIndex backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T00:00:00.000000Z\t\t10.0
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
            assertSqlCursors(sql, "SELECT ts, sym, val FROM t_bv_where_top WHERE sym = null");
        });
    }

    /**
     * Two rows written before {@code sym} exists -- they carry a column top and match the NULL
     * key implicitly -- then one row with a real key above it. Every NULL-key open over this
     * table has to run the backup plan.
     */
    private static void createTopTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE) TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0),
                ('2024-01-01T01:00:00', 20.0)
                """.formatted(name));
        execute("ALTER TABLE " + name + " ADD COLUMN sym SYMBOL");
        execute("INSERT INTO " + name + " VALUES ('2024-01-01T02:00:00', 30.0, 'A')");
        execute("ALTER TABLE " + name + " ALTER COLUMN sym ADD INDEX TYPE POSTING INCLUDE (val)");
        engine.releaseAllWriters();
        engine.releaseAllReaders();
    }

    private static void createTable(String name) throws Exception {
        execute("CREATE TABLE " + name + " (ts TIMESTAMP, val DOUBLE,"
                + " sym SYMBOL INDEX TYPE POSTING INCLUDE (val))"
                + " TIMESTAMP(ts) PARTITION BY DAY BYPASS WAL");
        execute("""
                INSERT INTO %s VALUES
                ('2024-01-01T00:00:00', 10.0, 'A'),
                ('2024-01-01T01:00:00', 20.0, NULL),
                ('2024-01-01T03:00:00', 40.0, NULL),
                ('2024-01-02T00:00:00', 30.0, 'A'),
                ('2024-01-02T01:00:00', 50.0, NULL)
                """.formatted(name));
    }
}
