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
    public void testCatalogueRendersClause() throws Exception {
        assertMemoryLeak(() -> {
            createBase();
            execute("create materialized view mv as (select * from base) expire rows keep highest v partition by k cleanup every 30m");
            drainWalAndMatViewQueues();
            assertSql(
                    "expire_predicate\texpire_cleanup_every\n" +
                            "KEEP HIGHEST v PARTITION BY k\t30m\n",
                    "select expire_predicate, expire_cleanup_every from tables() where table_name = 'mv'"
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
                job.cleanupTable(token, predicate, 0);
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

    private void assertCreateFails(String sql, String contains) throws Exception {
        try {
            execute(sql);
            Assert.fail("expected SqlException containing: " + contains);
        } catch (SqlException e) {
            TestUtils.assertContains(e.getFlyweightMessage(), contains);
        }
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
