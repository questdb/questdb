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
import io.questdb.cairo.MicrosTimestampDriver;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.cairo.wal.WalWriter;
import io.questdb.griffin.SqlException;
import io.questdb.std.ObjList;
import io.questdb.test.AbstractCairoTest;
import io.questdb.test.tools.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.questdb.cairo.wal.WalUtils.WAL_DEDUP_MODE_REPLACE_RANGE;

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
            assertQuery("select expire_clause, expire_cleanup_every from tables() where table_name = 'mv'").noRandomAccess().noLeakCheck().returns("""
                    expire_clause\texpire_cleanup_every
                    KEEP LATEST PARTITION BY k\t30m
                    """);
        });
    }

    @Test
    public void testKeepLatestComposesWithOuterWhere() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            // Latest per key is {A:2.0, B:6.0}; the outer WHERE filters the already-latest rows.
            assertQuery("select k, v from mv where v > 3 order by k").noLeakCheck().returns("""
                    k\tv
                    B\t6.0
                    """);
        });
    }

    @Test
    public void testKeepLatestHidesSupersededRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t6.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLatestReflectsNewBaseRows() throws Exception {
        assertMemoryLeak(() -> {
            createBaseAndPassthroughKeepLatest();
            // A newer row for A flows through the passthrough refresh; the view's "current state" updates.
            execute("insert into base values ('A', 9.0, '2024-01-04T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t9.0\t2024-01-04T00:00:00.000000Z
                    B\t6.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLatestAllowedOnAggregatingView() throws Exception {
        // EXPIRE ROWS on an aggregating (SAMPLE BY) view is allowed but advisory: physical reclamation is
        // best-effort (a later refresh can regenerate reclaimed rows), reads stay correct regardless.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute(
                    "create materialized view mvagg as (select k, last(v) v, ts from base sample by 1d) " +
                            "partition by day expire rows keep latest partition by k"
            );
            drainWalAndMatViewQueues();
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("mvagg"))) {
                Assert.assertNotNull(m.getExpiryPredicate());
            }
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
    public void testKeepLatestEmptyPartitionListRejected() throws Exception {
        // PARTITION BY with no column list before the next boundary (CLEANUP here) must be rejected, not
        // silently treated as an empty key set.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep latest partition by cleanup every 1h",
                    "EXPIRE ROWS KEEP LATEST requires a PARTITION BY column list"
            );
        });
    }

    @Test
    public void testKeepLatestWithoutPartitionRejected() throws Exception {
        // KEEP LATEST requires an explicit PARTITION BY; a bare column where 'partition' is expected fails.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            assertCreateFails(
                    "create materialized view mvbad as (select * from base) expire rows keep latest k",
                    "'partition' expected"
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
            final ObjList<TableToken> graphTokensBeforeRefresh = new ObjList<>();
            engine.getMatViewGraph().getViews(graphTokensBeforeRefresh);
            Assert.assertEquals("before refresh", 1, graphTokensBeforeRefresh.size());
            drainWalAndMatViewQueues();
            // Both rows visible before the policy.
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
            final TableToken currentToken = engine.verifyTableName("mv");
            final ObjList<TableToken> graphTokens = new ObjList<>();
            engine.getMatViewGraph().getViews(graphTokens);
            Assert.assertEquals("current=" + currentToken + ", graph=" + graphTokens, 1, graphTokens.size());
            Assert.assertNotNull("current=" + currentToken + ", graph=" + graphTokens,
                    engine.getMatViewGraph().getViewDefinition(currentToken));

            execute("alter materialized view mv set expire rows keep latest partition by k");
            drainWalAndMatViewQueues();
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    """);
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
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t6.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLatestCleanupDoesNotLoseFallbackAfterBaseReplaceRange() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("""
                    INSERT INTO base VALUES
                    ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                    ('A', 2.0, '2024-01-02T00:00:00.000000Z'),
                    ('B', 3.0, '2024-01-03T00:00:00.000000Z')
                    """);
            drainWalAndMatViewQueues();
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT * FROM base) EXPIRE ROWS KEEP LATEST PARTITION BY k");
            drainWalAndMatViewQueues();

            final TableToken mvToken = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata metadata = engine.getTableMetadata(mvToken)) {
                predicate = metadata.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(mvToken, predicate));
            }
            drainWalAndMatViewQueues();

            final TableToken baseToken = engine.verifyTableName("base");
            try (WalWriter writer = engine.getWalWriter(baseToken)) {
                writer.commitWithParams(
                        MicrosTimestampDriver.floor("2024-01-02T00:00:00.000000Z"),
                        MicrosTimestampDriver.floor("2024-01-03T00:00:00.000000Z"),
                        WAL_DEDUP_MODE_REPLACE_RANGE
                );
            }
            drainWalAndMatViewQueues();

            assertQuery("SELECT k, v, ts FROM mv ORDER BY k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t1.0\t2024-01-01T00:00:00.000000Z
                    B\t3.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLatestCleanupPreservesSupersededPartitions() throws Exception {
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
            assertQuery("select count() c from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("c\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();

            // A later refresh can remove either current winner and reveal its older row again, so cleanup
            // preserves every partition. The read filter remains authoritative for visibility.
            assertQuery("select count() c from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("c\n3\n");
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t3.0\t2024-01-03T00:00:00.000000Z
                    B\t4.0\t2024-01-03T00:00:00.000000Z
                    """);
        });
    }

    @Test
    public void testKeepLatestCleanupPreservesPartialPartition() throws Exception {
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
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();

            // A@01-01 must remain available in case a later refresh removes A@01-02.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    B\t5.0\t2024-01-03T00:00:00.000000Z
                    C\t9.0\t2024-01-01T00:00:00.000000Z
                    """);
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
            assertQuery("select k, v, ts from mv order by k").expectSize().noLeakCheck().returns("""
                    k\tv\tts
                    A\t2.0\t2024-01-02T00:00:00.000000Z
                    """);
            sink.clear();
            printSql("show create materialized view mv", sink);
            TestUtils.assertContains(sink.toString(), "EXPIRE ROWS KEEP LATEST ON ts PARTITION BY k");
        });
    }

    @Test
    public void testKeepLatestTiedMaxTimestampCleanupIsDeferred() throws Exception {
        // TIED max timestamps: key A has TWO rows sharing the SAME max ts, both in NON-active partitions.
        // The read filter rewrites to LATEST ON ts PARTITION BY k, so it keeps exactly ONE of the tied rows
        // (LATEST ON breaks the ts tie deterministically). Cleanup must preserve both candidates because a
        // later refresh can remove the current winner.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    // key A: two rows tied at the SAME max ts (01-02), both non-active; plus an older A.
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // older A (superseded)
                    "('A', 2.0, '2024-01-02T00:00:00.000000Z')," +   // tied-max A (non-active)
                    "('A', 3.0, '2024-01-02T00:00:00.000000Z')," +   // tied-max A (non-active, same ts)
                    // key B: single latest in the active partition so cleanup leaves a protected partition.
                    "('B', 9.0, '2024-01-03T00:00:00.000000Z')");    // latest B (active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();

            // Capture the read-filtered visible set BEFORE cleanup. LATEST ON keeps exactly one of the two
            // tied A rows (one row per key total).
            sink.clear();
            printSql("select k, v, ts from mv order by k", sink);
            final String visibleBefore = sink.toString();

            // Exactly two visible rows (one per key), and exactly one A row despite the tie.
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
            assertQuery("select count() c from mv where k = 'A'").noRandomAccess().expectSize().noLeakCheck().returns("c\n1\n");

            // 4 physical rows across 3 partitions before cleanup.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                Assert.assertFalse(job.cleanupTable(token, predicate));
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");
            sink.clear();
            printSql("select k, v, ts from mv order by k", sink);
            Assert.assertEquals("read filter and cleanup must agree on the tied survivor", visibleBefore, sink.toString());
            assertQuery("select count() c from mv where k = 'A'").noRandomAccess().expectSize().noLeakCheck().returns("c\n1\n");
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
        execute("""
                insert into base values
                ('A', 1.0, '2024-01-01T00:00:00.000000Z'),
                ('A', 2.0, '2024-01-02T00:00:00.000000Z'),
                ('B', 5.0, '2024-01-01T00:00:00.000000Z'),
                ('B', 6.0, '2024-01-03T00:00:00.000000Z')""");
        drainWalAndMatViewQueues();
        execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
        drainWalAndMatViewQueues();
    }
}
