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
 * Verifies {@code EXPIRE ROWS KEEP LATEST [ON <ts>] PARTITION BY <cols>} on PASSTHROUGH materialized views
 * (the relative "keep only the latest row per key" retention mode). The read filter rewrites a reference to
 * a policied view into {@code SELECT * FROM v LATEST ON <ts> PARTITION BY <cols>}, so a passthrough view +
 * KEEP LATEST behaves as an incrementally-maintained "current state per key" table.
 * <p>
 * Physical cleanup is deferred for this mode (the read filter is authoritative), so these tests assert
 * visibility, not on-disk reclamation. Mat views are dev-mode-gated (as in {@link MatViewExpireRowsTest}).
 */
public class RowExpiryKeepLatestTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testKeepLatestCatalogueRendersClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k cleanup every 30m");
            drainWalAndMatViewQueues();
            // The encoded policy is rendered back to a readable clause (no sentinel) in the catalogue.
            assertSql(
                    "expire_predicate\texpire_cleanup_every\n" +
                            "KEEP LATEST PARTITION BY k\t30m\n",
                    "select expire_predicate, expire_cleanup_every from tables() where table_name = 'mv'"
            );
        });
    }

    @Test
    public void testKeepLatestComposesWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            // Latest per key is {A:2.0, B:6.0}; the outer WHERE filters the already-latest rows.
            assertSql(
                    "k\tv\n" +
                            "B\t6.0\n",
                    "select k, v from mv where v > 3 order by k"
            );
        });
    }

    @Test
    public void testKeepLatestHidesSupersededRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            assertSql(
                    "k\tv\tts\n" +
                            "A\t2.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t6.0\t2024-01-03T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestReflectsNewBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            // A newer row for A flows through the passthrough refresh; the view's "current state" updates.
            execute("insert into base values ('A', 9.0, '2024-01-04T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertSql(
                    "k\tv\tts\n" +
                            "A\t9.0\t2024-01-04T00:00:00.000000Z\n" +
                            "B\t6.0\t2024-01-03T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestRejectedOnAggregatingView() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertCreateFails(
                    "create materialized view mvagg as (select k, last(v) v, ts from base sample by 1d) " +
                            "partition by day expire rows keep latest partition by k",
                    "EXPIRE ROWS KEEP LATEST is only supported on passthrough (non-aggregating) materialized views"
            );
        });
    }

    @Test
    public void testKeepLatestRejectedOnBaseTable() throws Exception {
        assertMemoryLeak(() -> assertCreateFails(
                "create table t (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal " +
                        "expire rows keep latest partition by k",
                "EXPIRE ROWS is only supported on materialized views"
        ));
    }

    @Test
    public void testKeepLatestRejectedForUnknownColumn() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep latest partition by nope",
                    "invalid EXPIRE ROWS KEEP LATEST column: nope"
            );
        });
    }

    @Test
    public void testKeepLatestSetViaAlter() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('A', 1.0, '2024-01-01T00:00:00.000000Z'), ('A', 2.0, '2024-01-02T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            // Both rows visible before the policy.
            assertSql("c\n2\n", "select count() c from mv");

            execute("alter materialized view mv set expire rows keep latest partition by k");
            drainWalAndMatViewQueues();
            assertSql(
                    "k\tv\tts\n" +
                            "A\t2.0\t2024-01-02T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestShowCreate() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("show create materialized view mv", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP LATEST PARTITION BY k");
        });
    }

    @Test
    public void testKeepLatestSurvivesReopen() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            // Drop pooled readers/writers; the policy must be re-read from _meta and the filter still apply.
            engine.releaseInactive();
            assertSql(
                    "k\tv\tts\n" +
                            "A\t2.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t6.0\t2024-01-03T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestCleanupReclaimsSupersededPartitions() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded by A@01-03
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," +   // superseded by B@01-03
                    "('A', 3.0, '2024-01-03T00:00:00.000000Z')," +   // latest A (active partition)
                    "('B', 4.0, '2024-01-03T00:00:00.000000Z')");    // latest B (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();

            // Three logical partitions physically present before cleanup.
            assertSql("c\n3\n", "select count() c from table_partitions('mv')");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate, 0);
            }
            drainWalAndMatViewQueues();

            // 01-01 (A superseded) and 01-02 (B superseded) are fully expired -> dropped; the active 01-03
            // (both latest) is protected. The read-filter result is unchanged.
            assertSql("c\n1\n", "select count() c from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t3.0\t2024-01-03T00:00:00.000000Z\n" +
                            "B\t4.0\t2024-01-03T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestCleanupCompactsPartialPartition() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded by A@01-02
                    "('C', 9.0, '2024-01-01T00:00:00.000000Z')," +   // latest C (survives in 01-01)
                    "('A', 2.0, '2024-01-02T00:00:00.000000Z')," +   // latest A
                    "('B', 5.0, '2024-01-03T00:00:00.000000Z')");    // latest B (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();

            // 4 physical rows across 3 partitions before cleanup.
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

            // 01-01 is PARTIALLY expired (A superseded, C latest) -> compacted to 1 row (REPLACE_RANGE);
            // 01-02 (A latest) and active 01-03 untouched. Partition count stays 3; the superseded A@01-01
            // row is physically removed (4 -> 3). The read-filter result is unchanged.
            assertSql("p\tr\n3\t3\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "k\tv\tts\n" +
                            "A\t2.0\t2024-01-02T00:00:00.000000Z\n" +
                            "B\t5.0\t2024-01-03T00:00:00.000000Z\n" +
                            "C\t9.0\t2024-01-01T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
        });
    }

    @Test
    public void testKeepLatestOnDesignatedTimestamp() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('A',1.0,'2024-01-01T00:00:00.000000Z'),('A',2.0,'2024-01-02T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest on ts partition by k");
            drainWalAndMatViewQueues();
            assertSql(
                    "k\tv\tts\n" +
                            "A\t2.0\t2024-01-02T00:00:00.000000Z\n",
                    "select k, v, ts from mv order by k"
            );
            sink.clear();
            printSql("show create materialized view mv", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP LATEST ON ts PARTITION BY k");
        });
    }

    @Test
    public void testKeepLatestOnNonDesignatedRejected() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertCreateFails(
                    "create materialized view mv as (select * from base) expire rows keep latest on v partition by k",
                    "EXPIRE ROWS KEEP LATEST ON must name the designated timestamp 'ts', not 'v'"
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

    private void createBaseAndPassthroughKeepLatest() throws Exception {
        execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
        execute("insert into base values " +
                "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                "('A', 2.0, '2024-01-02T00:00:00.000000Z')," +
                "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +
                "('B', 6.0, '2024-01-03T00:00:00.000000Z')");
        drainWalAndMatViewQueues();
        execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
        drainWalAndMatViewQueues();
    }
}
