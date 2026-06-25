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

package io.questdb.test.cairo.mv;

import io.questdb.PropertyKey;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies the window-based EXPIRE ROWS retention modes on PASSTHROUGH materialized views:
 * <ul>
 *     <li>{@code KEEP HIGHEST|LOWEST <col> [PARTITION BY <cols>]} — keep the group max/min (all ties);</li>
 *     <li>{@code KEEP <N> HIGHEST|LOWEST <col> [PARTITION BY <cols>]} — keep the top-N by a column;</li>
 *     <li>{@code WHEN <window predicate>} — an arbitrary window-function predicate (the escape hatch).</li>
 * </ul>
 * All desugar to / are a window predicate behind the projection-CASE read filter. Physical cleanup is
 * verified separately. Shared data (passthrough view over base):
 * <pre>
 *   A: 1.0@d1, 3.0@d2, 2.0@d3   (max=3.0, min=1.0)
 *   B: 5.0@d1, 5.0@d2, 4.0@d3   (max=5.0 TIE, min=4.0)
 *   C: null@d1, null@d2         (all-NULL group)
 *   D: 7.0@d1                   (single row; global max)
 * </pre>
 */
public class RowExpiryWindowTest extends AbstractCairoTest {

    // Bridge: AbstractCairoTest.assertSql(expected, sql) was removed in favor of the QueryAssertion
    // builder (OSS #7195). Drive the builder via returnsOnce() so the suite's calls keep working.
    private void assertSql(CharSequence expected, CharSequence sql) throws Exception {
        assertQuery(sql).noLeakCheck().returnsOnce(expected);
    }

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testKeepHighestAllTiesPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v partition by k");
            // Keep every row tied at the per-key max; NULL-group rows survive (v<max is UNKNOWN, kept).
            assertSql(
                    "k\tv\tts\n" +
                            "A\t3.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-01T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-02T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-01T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-02T00:00:00.000000Z\n" +
                            "D\t7.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k, ts"
            );
        });
    }

    @Test
    public void testKeepLowestPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep lowest v partition by k");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t1.0\t2024-01-01T00:00:00.000000Z\n" +
                            "B\t4.0\t2024-01-03T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-01T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-02T00:00:00.000000Z\n" +
                            "D\t7.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k, ts"
            );
        });
    }

    @Test
    public void testKeepTopNPerKey() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep 2 highest v partition by k");
            // top-2 by v desc, with the designated timestamp as the deterministic tiebreak.
            assertSql(
                    "k\tv\tts\n" +
                            "A\t3.0\t2024-01-02T00:00:00.000000Z\n" +
                            "A\t2.0\t2024-01-03T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-01T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-02T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-01T00:00:00.000000Z\n" +
                            "D\t7.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k, v desc, ts desc"
            );
        });
    }

    @Test
    public void testKeepTopNNullsSortFirstWithinN() throws Exception {
        // Documents top-N NULL handling for a FLOATING-POINT column (v is DOUBLE): QuestDB has no NULLS LAST
        // and sorts a float NULL (NaN) FIRST under DESC, so under KEEP <N> HIGHEST the NULL takes a leading
        // rank and is kept while within N, ahead of real values. Here N=2 with one NULL: NULL=rank1,
        // 9.0=rank2, so 8.0 and 7.0 expire. (An integer/timestamp NULL sorts LAST under DESC and would be
        // expired first instead -- the position is type-dependent; use KEEP HIGHEST without N to keep all NULLs.)
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 8.0, '2024-01-02T00:00:00.000000Z')," +
                    "('A', 7.0, '2024-01-03T00:00:00.000000Z')," +
                    "('A', null, '2024-01-04T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            assertSql(
                    "k\tv\n" +
                            "A\t9.0\n" +
                            "A\tnull\n",
                    "select k, v from mv order by v"
            );
        });
    }

    @Test
    public void testKeepTopNIntegerNullSortsLastExpiredFirst() throws Exception {
        // Type-dependent NULL placement under KEEP <N> HIGHEST. RowExpiryUtil's javadoc: QuestDB has no NULLS
        // LAST and where a NULL sorts is TYPE-DEPENDENT -- an INTEGER NULL (a MIN sentinel) sorts LAST under
        // DESC, so it takes a TRAILING rank and is EXPIRED first (unlike a DOUBLE NaN which sorts FIRST and is
        // kept -- see testKeepTopNNullsSortFirstWithinN). N=2 over an INT column with one NULL: 9=rank1,
        // 8=rank2, 7=rank3, NULL=rank4(last). Kept = {9, 8}; the NULL and 7 expire. Pinned with explicit rows.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, n int, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 8, '2024-01-02T00:00:00.000000Z')," +
                    "('A', 7, '2024-01-03T00:00:00.000000Z')," +
                    "('A', null, '2024-01-04T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest n partition by k");
            drainWalAndMatViewQueues();
            // ORDER BY n -- with NULLs sorting per QuestDB convention; only the two real top values survive.
            assertSql(
                    "k\tn\n" +
                            "A\t8\n" +
                            "A\t9\n",
                    "select k, n from mv order by n"
            );
            // The NULL row is NOT visible -- integer NULL expired first under KEEP N (opposite of DOUBLE).
            assertSql("c\n0\n", "select count() c from mv where n is null");
        });
    }

    @Test
    public void testKeepTopNTimestampNullSortsLastExpiredFirst() throws Exception {
        // Same type-dependent NULL placement for a TIMESTAMP value column: a TIMESTAMP NULL (also a MIN
        // sentinel) sorts LAST under DESC and is EXPIRED first under KEEP <N> HIGHEST. N=2 over a TIMESTAMP
        // column 'w' with one NULL: the two largest non-null w survive, the NULL expires. Pinned explicitly.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, w timestamp, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', '2024-03-03T00:00:00.000000Z', '2024-01-01T00:00:00.000000Z')," +
                    "('A', '2024-02-02T00:00:00.000000Z', '2024-01-02T00:00:00.000000Z')," +
                    "('A', '2024-01-01T00:00:00.000000Z', '2024-01-03T00:00:00.000000Z')," +
                    "('A', null, '2024-01-04T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest w partition by k");
            drainWalAndMatViewQueues();
            assertSql(
                    "k\tw\n" +
                            "A\t2024-02-02T00:00:00.000000Z\n" +
                            "A\t2024-03-03T00:00:00.000000Z\n",
                    "select k, w from mv order by w"
            );
            // The NULL-w row is NOT visible -- timestamp NULL expired first under KEEP N.
            assertSql("c\n0\n", "select count() c from mv where w is null");
        });
    }

    @Test
    public void testKeepHighestNoPartition() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v");
            // Global max is 7.0 (D). NULL rows (C) survive (v<max is UNKNOWN). Everything else expires.
            assertSql(
                    "k\tv\n" +
                            "C\tnull\n" +
                            "C\tnull\n" +
                            "D\t7.0\n",
                    "select k, v from mv order by k, ts"
            );
        });
    }

    @Test
    public void testRawWindowWhen() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows when v < max(v) over (partition by k)");
            // Equivalent to KEEP HIGHEST v PARTITION BY k.
            assertSql(
                    "k\tv\tts\n" +
                            "A\t3.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-01T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-02T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-01T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-02T00:00:00.000000Z\n" +
                            "D\t7.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k, ts"
            );
        });
    }

    @Test
    public void testComposesWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            createViewWith("expire rows keep highest v partition by k");
            // The outer predicate filters the already-kept (per-key max) rows.
            assertSql(
                    "k\tv\n" +
                            "B\t5.0\n" +
                            "B\t5.0\n" +
                            "D\t7.0\n",
                    "select k, v from mv where v > 3 order by k, ts"
            );
        });
    }

    @Test
    public void testSetViaAlter() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertSql("c\n9\n", "select count() c from mv");
            execute("alter materialized view mv set expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            assertSql("c\n7\n", "select count() c from mv"); // 9 rows -> top-2 per key keeps 7 (A2,B2,C2,D1)
        });
    }

    @Test
    public void testShowCreateRoundTrips() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP 2 HIGHEST v PARTITION BY k");

            execute("create materialized view mv2 as (select * from base) expire rows when v < max(v) over (partition by k)");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv2", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS WHEN v < max(v) over (partition by k)");
        });
    }

    @Test
    public void testShowCreateQuotesKeepColumnNeedingQuoting() throws Exception {
        // A keep column whose name needs quoting (a space) must be re-quoted by SHOW CREATE so the rendered
        // DDL round-trips. (The parser unquote()s the stored column, so the renderer must add the quotes back.)
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, \"my val\" double, ts timestamp) timestamp(ts) partition by day wal");
            // The CREATE accepts the quoted keep column -> proves the parse side of the round-trip.
            execute("create materialized view mv as (select * from base) expire rows keep highest \"my val\" partition by k");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv", sink);
            // The render side must emit it quoted (unquoted "my val" would not re-parse).
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP HIGHEST \"my val\" PARTITION BY k");
        });
    }

    @Test
    public void testCatalogueRendersClause() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k cleanup every 30m");
            drainWalAndMatViewQueues();
            assertSql(
                    "expire_clause\texpire_cleanup_every\n" +
                            "KEEP HIGHEST v PARTITION BY k\t30m\n",
                    "select expire_clause, expire_cleanup_every from tables() where table_name = 'mv'"
            );
        });
    }

    @Test
    public void testRejectedOnBaseTable() throws Exception {
        assertMemoryLeak(() -> assertCreateFails(
                "create table t (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal expire rows keep highest v partition by k",
                "EXPIRE ROWS is only supported on materialized views"
        ));
    }

    @Test
    public void testRejectedOnUnpartitionedTable() throws Exception {
        // EXPIRE on an un-partitioned CREATE TABLE must give the SPECIFIC message; the rejection used to live
        // inside the PARTITION BY block, so this case fell through to a generic "unexpected token".
        assertMemoryLeak(() -> assertCreateFails(
                "create table t (a int, ts timestamp) timestamp(ts) expire rows when a < 2",
                "EXPIRE ROWS is only supported on materialized views"
        ));
    }

    @Test
    public void testRejectedOnCtas() throws Exception {
        // Same for CREATE TABLE ... AS SELECT (the PARTITION BY block is likewise skipped here).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertCreateFails(
                    "create table cp as (select * from base) expire rows when v < 2.0",
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testRejectedOnAggregatingView() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            assertCreateFails(
                    "create materialized view mvagg as (select k, last(v) v, ts from base sample by 1d) " +
                            "partition by day expire rows keep highest v partition by k",
                    "passthrough (non-aggregating) materialized views"
            );
        });
    }

    @Test
    public void testRejectedForUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep highest nope partition by k",
                    "invalid EXPIRE ROWS policy"
            );
        });
    }

    @Test
    public void testRejectedZeroRowCount() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep 0 highest v partition by k",
                    "positive row count"
            );
        });
    }

    @Test
    public void testRejectedEmptyPartitionBy() throws Exception {
        // KEEP HIGHEST/LOWEST with a PARTITION BY keyword but no column list must be rejected, not silently
        // treated as a global (un-partitioned) window (which would change the retention semantics).
        assertMemoryLeak(() -> {
            createBase();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep highest v partition by cleanup every 1h",
                    "requires a column list"
            );
        });
    }

    @Test
    public void testCleanupCompactsAndWipes() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // B max -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A max=9)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n3\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // d1 partial (A@d1 expired, B@d1 kept) -> compacted to 1 row; d2 fully expired -> wiped; active
            // d3 untouched. 3 partitions/4 rows -> 2 partitions/2 rows. The read result is unchanged.
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t9.0\t2024-01-03T00:00:00.000000Z\n" +
                            "B\t8.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testCleanupKeepLowest() throws Exception {
        // Physical cleanup for the LOWEST direction (mirror of testCleanupCompactsAndWipes, inverted values).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A min=1)
                    "('B', 2.0, '2024-01-01T00:00:00.000000Z')," +   // B min -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A min=1)
                    "('A', 1.0, '2024-01-03T00:00:00.000000Z')");    // A min (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep lowest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n3\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            runCleanup();

            // d1 partial (A@d1 expired, B@d1 kept) -> 1 row; d2 fully expired -> wiped; active d3 untouched.
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t1.0\t2024-01-03T00:00:00.000000Z\n" +
                            "B\t2.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testCleanupTopN() throws Exception {
        // Physical cleanup for KEEP <N> (the row_number() ranking path, distinct from the max/min keep-by path).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // rank2 -> survives in d1
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +   // rank3 -> expired (d1 partial)
                    "('A', 4.0, '2024-01-02T00:00:00.000000Z')," +   // rank4 -> expired (d2 wiped)
                    "('A', 10.0, '2024-01-03T00:00:00.000000Z')");   // rank1 (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 2 highest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n3\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            runCleanup();

            // top-2 by v desc = {10@d3, 9@d1}. d1 partial (9 kept, 5 expired) -> 1 row; d2 wiped; active d3 kept.
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t10.0\t2024-01-03T00:00:00.000000Z\n" +
                            "A\t9.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by v desc"
            );
        });
    }

    @Test
    public void testCleanupRawWindowWhenIdempotent() throws Exception {
        // Physical cleanup for a RAW window WHEN (the isWindow survivor-query branch, not isKeepBy), and that a
        // second sweep is a no-op once nothing expired remains.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('B', 8.0, '2024-01-01T00:00:00.000000Z')," +   // B max -> survives in d1
                    "('A', 5.0, '2024-01-02T00:00:00.000000Z')," +   // expired (A max=9)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < max(v) over (partition by k)");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n3\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertTrue("first sweep should reclaim", job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");

            // Second sweep: nothing expired remains -> no work, partitions unchanged.
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse("second sweep must be a no-op", job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t9.0\t2024-01-03T00:00:00.000000Z\n" +
                            "B\t8.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testCleanupKeepsNullValueGroup() throws Exception {
        // A group whose values are all NULL is KEPT (v < max(v) is UNKNOWN). Physical cleanup must NOT delete
        // those rows: the partial partition compacts but retains the NULL row; the all-NULL partition is
        // skipped (all survivors).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (A max=9)
                    "('C', null, '2024-01-01T00:00:00.000000Z')," +  // NULL group -> kept (d1 partial)
                    "('C', null, '2024-01-02T00:00:00.000000Z')," +  // NULL group -> kept (d2 all-kept -> skipped)
                    "('A', 9.0, '2024-01-03T00:00:00.000000Z')");    // A max (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n3\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            runCleanup();

            // d1 partial (A expired, C null kept) -> 1 row; d2 all-NULL kept -> skipped; d3 active untouched.
            assertSql("p\tr\n3\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "C\tnull\t2024-01-01T00:00:00.000000Z\n" +
                            "C\tnull\t2024-01-02T00:00:00.000000Z\n" +
                            "A\t9.0\t2024-01-03T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by ts"
            );
        });
    }

    @Test
    public void testCleanupAllSurvivorsNonActivePartitionNotReclaimed() throws Exception {
        // CLEANUP classification edge: a NON-ACTIVE partition in which EVERY row survives (survivors ==
        // rowCount) must NOT be reclaimed -- cleanup must find no work and leave the partition byte-identical.
        // Here every row is tied at the per-key extreme, so KEEP HIGHEST keeps them all; the only expired data
        // lives elsewhere (the active partition is protected, so there is genuinely nothing to reclaim).
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    // 01-01 (non-active): A and B each tied at their per-key max -> ALL survive.
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('A', 5.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 7.0, '2024-01-01T00:00:00.000000Z')," +
                    // 01-02 active partition (protected).
                    "('C', 1.0, '2024-01-02T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n2\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            // Every row is visible (all are per-key maxima or the lone active row).
            assertSql("c\n4\n", "select count() c from mv");

            // No expired rows anywhere reclaimable -> cleanup is a NO-OP; partitions unchanged.
            Assert.assertFalse("all-survivors + protected-active partition => no work", runCleanupReturning());
            assertSql("p\tr\n2\t4\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql("c\n4\n", "select count() c from mv");
        });
    }

    @Test
    public void testCleanupTopNNGreaterThanRowsNotReclaimed() throws Exception {
        // KEEP <N> classification edge: when N >= the group's row count in a NON-ACTIVE partition, every row
        // ranks within N and survives, so that partition must NOT be reclaimed. Here N=5 but key A has only 2
        // rows in the non-active 01-01 partition -> both survive -> nothing to reclaim there; the active
        // partition is protected. Cleanup must report no work and leave all partitions untouched.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 9.0, '2024-01-01T00:00:00.000000Z')," +   // non-active, within N=5
                    "('A', 8.0, '2024-01-01T00:00:00.000000Z')," +   // non-active, within N=5
                    "('A', 7.0, '2024-01-02T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep 5 highest v partition by k");
            drainWalAndMatViewQueues();

            assertSql("p\tr\n2\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql("c\n3\n", "select count() c from mv"); // N >= rows -> all visible

            Assert.assertFalse("N >= rows in every non-active partition => no work", runCleanupReturning());
            assertSql("p\tr\n2\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql("c\n3\n", "select count() c from mv");
        });
    }

    @Test
    public void testCleanupSingleActivePartitionIsNoOp() throws Exception {
        // Edge partition handling: when ALL data lives in the single ACTIVE partition, cleanup never touches it
        // (the active partition is always protected from reclamation), even though the read filter hides the
        // superseded rows. Cleanup must be a no-op and the on-disk rows must remain.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded by A=9 (read-hidden)
                    "('A', 9.0, '2024-01-01T06:00:00.000000Z')," +   // A max
                    "('B', 4.0, '2024-01-01T12:00:00.000000Z')");    // B max -- all in ONE partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k");
            drainWalAndMatViewQueues();

            // One physical partition, 3 rows; the read filter shows the two per-key maxima.
            assertSql("p\tr\n1\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql("c\n2\n", "select count() c from mv");

            // The only partition is the active one -> protected -> cleanup is a no-op; 3 rows stay on disk.
            Assert.assertFalse("the lone active partition must not be reclaimed", runCleanupReturning());
            assertSql("p\tr\n1\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql("c\n2\n", "select count() c from mv");
        });
    }

    private void assertCreateFails(String sql, String contains) throws Exception {
        try {
            execute(sql);
            Assert.fail("expected SqlException containing: " + contains);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), contains);
        }
    }

    // Runs one cleanup sweep over "mv" and asserts it reclaimed (returned true).
    private void runCleanup() throws Exception {
        final TableToken token = engine.verifyTableName("mv");
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            Assert.assertTrue("sweep should reclaim", job.cleanupTable(token, predicate));
        }
        drainWalAndMatViewQueues();
    }

    // Runs one cleanup sweep over "mv" and returns whether it reclaimed anything (no assertion).
    private boolean runCleanupReturning() throws Exception {
        final TableToken token = engine.verifyTableName("mv");
        final String predicate;
        try (TableMetadata m = engine.getTableMetadata(token)) {
            predicate = m.getExpiryPredicate();
        }
        final boolean worked;
        try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
            worked = job.cleanupTable(token, predicate);
        }
        drainWalAndMatViewQueues();
        return worked;
    }

    private void createBase() throws Exception {
        execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
        execute("insert into base values " +
                "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                "('A', 3.0, '2024-01-02T00:00:00.000000Z')," +
                "('A', 2.0, '2024-01-03T00:00:00.000000Z')," +
                "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +
                "('B', 5.0, '2024-01-02T00:00:00.000000Z')," +
                "('B', 4.0, '2024-01-03T00:00:00.000000Z')," +
                "('C', null, '2024-01-01T00:00:00.000000Z')," +
                "('C', null, '2024-01-02T00:00:00.000000Z')," +
                "('D', 7.0, '2024-01-01T00:00:00.000000Z')");
        drainWalAndMatViewQueues();
    }

    private void createViewWith(String expireClause) throws Exception {
        createBase();
        execute("create materialized view mv as (select * from base) " + expireClause);
        drainWalAndMatViewQueues();
    }
}
