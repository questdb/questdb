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

import io.questdb.test.AbstractCairoTest;
import org.junit.Test;

/**
 * The covering backup plans that carry a FILTER, and the ones that deliberately do not.
 * <p>
 * Two ownership shapes meet here. On the LATEST ON path codegen hands the residual filter TO the
 * backup ({@code buildLatestByIndexScan}) and clears its own reference, so the backup frees it. On
 * the WHERE path the filter stays with the wrapper above the covering factory, which applies it to
 * whichever of the two delegates runs, so the backup is built without one. Getting either wrong
 * shows up as dropped rows, double-filtered rows, or a leak, and neither shape had a test.
 */
public class CoveringIndexBackupFilterTest extends AbstractCairoTest {

    @Test
    public void testLatestOnBackupCarriesItsFilter() throws Exception {
        // The LATEST ON site with filter != null: the backup is a
        // LatestByValue*IndexedFilteredRecordCursorFactory that owns the filter. The NULL key over
        // a column top forces the backup, so the filter has to survive the hand-off and select the
        // right row -- val > 15 excludes the 10.0 row, leaving 20.0 as the latest NULL-key row.
        assertMemoryLeak(() -> {
            createTopTable("t_bf_lo");
            final String sql = "SELECT sym, val FROM t_bf_lo WHERE sym = null AND val > 15"
                    + " LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .withPlanContaining("backup: true")
                    .returns("sym\tval\n\t20.0\n");
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testLatestOnBackupWithFilterThatMatchesNothing() throws Exception {
        // The filter is genuinely applied, not merely carried: raise the bound above every
        // NULL-key row and the backup must return nothing rather than the unfiltered latest.
        assertMemoryLeak(() -> {
            createTopTable("t_bf_lo_empty");
            final String sql = "SELECT sym, val FROM t_bf_lo_empty WHERE sym = null AND val > 25"
                    + " LATEST ON ts PARTITION BY sym";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .withPlanContaining("backup: true")
                    .returns("sym\tval\n");
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testLatestOnInListBackupCarriesItsFilter() throws Exception {
        // The LATEST ON IN-list site, whose backup is LatestByValuesIndexedFilteredRecordCursorFactory.
        // 'A' is the only non-NULL key and its row is 30.0, so val > 15 keeps both keys' latest rows.
        assertMemoryLeak(() -> {
            createTopTable("t_bf_lo_in");
            final String sql = "SELECT sym, val FROM t_bf_lo_in WHERE sym IN (null, 'A') AND val > 15"
                    + " LATEST ON ts PARTITION BY sym ORDER BY sym";
            // ORDER BY sorts above the scan, and a sorted factory does support random access.
            assertQuery(sql)
                    .noLeakCheck()
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
    public void testWhereInListBackupAppliesResidualFilterExactlyOnce() throws Exception {
        // The WHERE IN-list site builds FilterOnValuesRecordCursorFactory with a null filter --
        // "the filter stays with the wrapper above us" -- and wrapCoveringWithFilter applies it to
        // whichever delegate runs. A dropped filter returns the 10.0 row; a doubled one cannot
        // change these rows but is caught by the no_covering cross-check below, which compiles the
        // filter into the scan instead of above it.
        assertMemoryLeak(() -> {
            createTopTable("t_bf_in");
            final String sql = "SELECT ts, sym, val FROM t_bf_in WHERE sym IN (null, 'A') AND val > 15"
                    + " ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            2024-01-01T02:00:00.000000Z\tA\t30.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    @Test
    public void testWhereSingleKeyBackupAppliesResidualFilterExactlyOnce() throws Exception {
        // The single-key twin of the site above: buildSingleSymbolIndexScan is deliberately handed
        // no filter, for the same reason.
        assertMemoryLeak(() -> {
            createTopTable("t_bf_single");
            final String sql = "SELECT ts, sym, val FROM t_bf_single WHERE sym = null AND val > 15"
                    + " ORDER BY ts";
            assertQuery(sql)
                    .noLeakCheck()
                    .noRandomAccess()
                    .skipRandomAccessProbe()
                    .sizeMayVary()
                    .timestamp("ts")
                    .withPlanContaining("backup: true")
                    .returns("""
                            ts\tsym\tval
                            2024-01-01T01:00:00.000000Z\t\t20.0
                            """);
            assertSqlCursors(sql, sql.replace("SELECT ", "SELECT /*+ no_covering */ "));
        });
    }

    /**
     * Two rows written before {@code sym} exists -- they carry a column top and match the NULL key
     * implicitly -- then one row with a real key above it. The NULL key over that top is what forces
     * every backup under test here.
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
