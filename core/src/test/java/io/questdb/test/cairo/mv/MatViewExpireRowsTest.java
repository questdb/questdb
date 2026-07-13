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
import io.questdb.cairo.PartitionBy;
import io.questdb.cairo.RowExpiryCleanupJob;
import io.questdb.cairo.TableToken;
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Verifies the EXPIRE ROWS row-expiry clause on MATERIALIZED VIEWs:
 * <ul>
 *     <li>{@code CREATE MATERIALIZED VIEW ... EXPIRE ROWS WHEN <pred> [CLEANUP EVERY <dur>]}</li>
 *     <li>{@code ALTER MATERIALIZED VIEW ... SET EXPIRE ROWS WHEN <pred> [CLEANUP EVERY <dur>]}</li>
 *     <li>{@code ALTER MATERIALIZED VIEW ... DROP EXPIRE}</li>
 * </ul>
 * Mat views are WAL tables, so the _meta persistence and the read-time row-expiry filter are shared
 * with plain tables; the filter is materialized-view-only ({@code isMatView()}), excluding plain tables
 * and plain views alike.
 * These tests confirm the grammar/threading and that querying a policied mat view hides expired rows.
 * EXPIRE ROWS is allowed on an aggregating (SAMPLE BY) view too, advisory only: reads stay correct via the
 * read filter, but physical reclamation is best-effort since a later refresh can regenerate reclaimed rows
 * ({@link #testCreateAggregatingMatViewWithExpireAllowed()}).
 */
public class MatViewExpireRowsTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // Mat views are gated behind dev mode, exactly as MatViewTest enables them.
        setProperty(PropertyKey.DEV_MODE_ENABLED, "true");
    }

    @Test
    public void testCountReflectsKeepLatestReadFilter() throws Exception {
        // All rows land in one (active) partition that cleanup never touches, so a superseded row stays on
        // disk. count() must still reflect the read filter (one latest row per key), not the raw row count.
        assertMemoryLeak(() -> {
            execute("create table base (k symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('A', 2.0, '2024-01-02T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('B', 3.0, '2024-01-01T00:00:00.000000Z')," +   // superseded (non-active partition)
                    "('A', 4.0, '2024-01-03T00:00:00.000000Z')," +   // latest A (active partition)
                    "('B', 5.0, '2024-01-02T00:00:00.000000Z')");    // latest B (non-active partition)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows keep latest partition by k");
            drainWalAndMatViewQueues();
            // NO cleanup: all 5 rows are physically present across 3 partitions. The read filter must still
            // show exactly the latest row per key, and count() must agree.
            assertQuery("select k, v from mv order by k").expectSize().noLeakCheck().returns("k\tv\nA\t4.0\nB\t5.0\n");
            assertQuery("select count() c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
            assertQuery("select count(distinct k) c from mv").noRandomAccess().expectSize().noLeakCheck().returns("c\n2\n");
        });
    }

    @Test
    public void testExpireScalarCustomCleanupCompactsAndWipes() throws Exception {
        // A non-time (custom) scalar predicate has no bounds fast-path, so cleanup classifies via the count
        // scan: a fully-expired partition is wiped, a partial one compacted.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // v<2 -> expired (d1 partial)
                    "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +   // kept
                    "('C', 1.5, '2024-01-02T00:00:00.000000Z')," +   // v<2 -> d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "B\t5.0\n" +
                            "D\t9.0\n");
        });
    }

    @Test
    public void testExpireScalarCleanupKeepsNullPredicateRow() throws Exception {
        // A row whose scalar predicate operand is NULL is KEPT (v < 2.0 is UNKNOWN, not TRUE). Physical
        // cleanup must not delete it: the partial partition compacts but retains the NULL row. Scalar-WHEN
        // cleanup uses its own keep-filter (buildRowExpiryKeepFilter), so this guards that path's 3VL.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // v<2 -> expired (d1 partial)
                    "('C', null, '2024-01-01T00:00:00.000000Z')," +  // NULL -> kept (d1)
                    "('B', 1.5, '2024-01-02T00:00:00.000000Z')," +   // v<2 -> d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n3\t4\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // d1 partial (A expired, C null kept) -> 1 row; d2 fully expired -> wiped; active d3 kept.
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "C\tnull\n" +
                            "D\t9.0\n");
        });
    }

    @Test
    public void testExpireScalarCleanupReclaimsOldPartition() throws Exception {
        // The physical cleanup reclaims on a mat view via REPLACE_RANGE (DROP PARTITION via SQL is rejected
        // for mat views). Here a wholly-below-threshold partition is wiped.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," +
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();

            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // 01-01 lies wholly below the threshold -> reclaimed; 01-02 (active-protected check aside) and
            // 01-03 retained. The read filter already hid A; now its storage is gone too.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "B\t2.0\n" +
                            "C\t3.0\n");
        });
    }

    @Test
    public void testExpireScalarKeepsNullPredicateRows() throws Exception {
        // A row whose predicate evaluates to NULL/UNKNOWN (here a NULL v under "v < 2.0") is KEPT, not
        // expired: the read filter is CASE WHEN (pred) THEN false ELSE true, so FALSE and NULL both keep.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-05T00:00:00.000000Z')," +   // v < 2 -> expired
                    "('B', 5.0, '2024-01-06T00:00:00.000000Z')," +   // v >= 2 -> kept
                    "('C', null, '2024-01-07T00:00:00.000000Z')");   // v NULL -> kept (UNKNOWN predicate)
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "B\t5.0\n" +
                            "C\tnull\n");
        });
    }

    @Test
    public void testExpireScalarCleanupIsIdempotent() throws Exception {
        // A second cleanup sweep over already-compacted data must be a no-op (no REPLACE, partitions
        // unchanged): otherwise it would re-churn the WAL and re-replicate deletions on every sweep.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +   // expired (d1 partial)
                    "('B', 5.0, '2024-01-01T00:00:00.000000Z')," +   // kept
                    "('C', 1.5, '2024-01-02T00:00:00.000000Z')," +   // d2 fully expired
                    "('D', 9.0, '2024-01-03T00:00:00.000000Z')");    // active partition
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            final boolean first;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                first = job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();
            assertTrue("first sweep should reclaim", first);
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");

            // Second sweep: nothing expired remains -> no work, partitions unchanged.
            final boolean second;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                second = job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();
            assertFalse("second sweep must be a no-op", second);
            assertQuery("select count() p, sum(numRows) r from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\tr\n2\t2\n");
        });
    }

    @Test
    public void testExpireScalarLessEqualThresholdCleanup() throws Exception {
        // "<=" exercises the inclusive bounds fast-path: a partition whose nextFloor <= T is wiped without a
        // scan (01-01), and a boundary partition whose rows are exactly == T is expired too (01-02).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," +
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts <= '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            final TableToken token = engine.verifyTableName("mv");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(token)) {
                predicate = m.getExpiryPredicate();
            }
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();

            // 01-01 (nextFloor <= T) wiped by bounds; 01-02 (ts == T, ts <= T) expired by the scan; 01-03 is
            // the active partition and is retained. Only C remains.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n1\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\nC\t3.0\n");
        });
    }

    @Test
    public void testAlterMatViewDropExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            execute("insert into base values ('BBB', 5.0, '2024-01-09T12:00:00.000000Z')");
            drainWalAndMatViewQueues();

            // Passthrough view, no policy yet.
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();

            // Set a policy: hide v < 2 -> only BBB visible.
            execute("alter materialized view mv set expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "BBB\t5.0\n");

            // Drop the policy: all rows visible again.
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }
            assertQuery("select sym, v from mv order by sym").expectSize().noLeakCheck().returns("sym\tv\n" +
                            "AAA\t1.0\n" +
                            "BBB\t5.0\n");
        });
    }

    @Test
    public void testShowCreateMatViewWithExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0 CLEANUP EVERY 30m");
            drainWalAndMatViewQueues();
            sink.clear();
            printSql("SHOW CREATE MATERIALIZED VIEW mv", sink);
            final String ddl = sink.toString();
            org.junit.Assert.assertTrue("expected EXPIRE clause in: " + ddl, ddl.contains("EXPIRE ROWS WHEN v < 2.0"));
            org.junit.Assert.assertTrue("expected CLEANUP EVERY in: " + ddl, ddl.contains("CLEANUP EVERY 30m"));
        });
    }

    @Test
    public void testAlterMatViewSetExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // v < 2 -> hidden
            execute("insert into base values ('BBB', 2.5, '2024-01-09T12:00:00.000000Z')"); // v >= 2 -> visible
            execute("insert into base values ('CCC', 0.5, '2024-01-09T18:00:00.000000Z')"); // v < 2 -> hidden
            drainWalAndMatViewQueues();

            // Passthrough view with no policy at creation.
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();

            execute("alter materialized view mv set expire rows when v < 2.0 cleanup every 30m");
            drainWalAndMatViewQueues();

            // Metadata predicate + cleanup interval are persisted.
            final TableToken token = engine.verifyTableName("mv");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(30 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // The read-time filter (reading the policy from the metadata cache) hides v<2 rows.
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "BBB\t2.5\n");
        });
    }

    @Test
    public void testCreatePassthroughMatViewMirrorsHourlyBasePartitioning() throws Exception {
        // A passthrough view with no explicit PARTITION BY mirrors the base table's partitioning (here HOUR),
        // so refresh REPLACE_RANGE and expiry DROP/REPLACE align to base partitions. Exercises the non-DAY
        // passthrough chunk-interval path.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by hour wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:30:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                assertEquals(PartitionBy.HOUR, metadata.getPartitionBy());
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreatePassthroughMatViewWithExpire() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // v < 2 -> hidden
            execute("insert into base values ('BBB', 2.5, '2024-01-09T12:00:00.000000Z')"); // v >= 2 -> visible
            execute("insert into base values ('CCC', 0.5, '2024-01-09T18:00:00.000000Z')"); // v < 2 -> hidden
            drainWalAndMatViewQueues();

            // Passthrough (no SAMPLE BY) view that carries the policy from creation.
            execute("create materialized view mv2 as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // Predicate persisted; CLEANUP EVERY omitted -> default 1 hour.
            final TableToken token = engine.verifyTableName("mv2");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(3_600_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }

            // Rows with v < 2 are hidden by the read-time filter.
            assertQuery("select sym, v from mv2 order by sym").noLeakCheck().returns("sym\tv\n" +
                            "BBB\t2.5\n");
        });
    }

    @Test
    public void testCreateMatViewInvalidPredicateRejected() throws Exception {
        // An invalid EXPIRE predicate must be rejected at CREATE MATERIALIZED VIEW (not accepted and then
        // bricking every read of the view). The view must not be left behind.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN no_such_col < now()",
                    25,
                    "invalid EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateAggregatingMatViewWithExpireAllowed() throws Exception {
        // EXPIRE ROWS on an aggregating (SAMPLE BY) view is allowed but advisory: the cleanup job's physical
        // reclamation is best-effort since a later refresh can regenerate reclaimed rows from surviving base
        // rows, but reads stay correct regardless (the read filter is authoritative), so CREATE succeeds.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute(
                    "create materialized view mv as (" +
                            "select sym, last(price) price, ts from base sample by 1h" +
                            ") partition by day EXPIRE ROWS WHEN ts < dateadd('d', -1, now())"
            );
            drainWalAndMatViewQueues();
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterAggregatingMatViewSetExpireAllowed() throws Exception {
        // The same advisory-not-rejected rule applies to ALTER ... SET EXPIRE: an existing aggregating
        // (SAMPLE BY) view may have a policy attached after the fact.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view agg as (select sym, last(price) price, ts from base sample by 1h) partition by day");
            drainWalAndMatViewQueues();
            execute("alter materialized view agg set expire rows when ts < dateadd('d', -1, now())");
            drainWalAndMatViewQueues();
            // The view must be left with the policy attached.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("agg"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreateTableLikeMatViewDoesNotInheritExpire() throws Exception {
        // CREATE TABLE (LIKE <mat view with EXPIRE ROWS>) must NOT copy the policy onto the new PLAIN table:
        // EXPIRE is mat-view-only, and a policy on a plain table would silently hide + physically delete rows.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            // Sanity: the source view does carry the policy.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
            }

            execute("create table cloned (like mv)");
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("cloned"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
                assertEquals(0L, metadata.getExpiryCleanupIntervalMicros());
            }

            // A v < 2 row in the clone must remain visible -- the leaked read filter would have hidden it.
            execute("insert into cloned values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')");
            drainWalQueue();
            assertQuery("select sym, v from cloned").expectSize().noLeakCheck().returns("sym\tv\n" + "AAA\t1.0\n");
        });
    }

    @Test
    public void testCreateKeepColumnCollisionRejected() throws Exception {
        // The window/keep-by read filter projects a synthetic boolean column __qdb_re_keep. A view that already
        // exposes that name (here inherited from the base via select *) would make every read ambiguous, so the
        // policy is rejected at CREATE.
        assertMemoryLeak(() -> {
            execute("create table base (__qdb_re_keep int, v double, k symbol, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows keep highest v partition by k",
                    25,
                    "cannot be used on a view with a column named '__qdb_re_keep'"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testAlterKeepColumnCollisionRejected() throws Exception {
        // Same __qdb_re_keep collision guard on the ALTER ... SET EXPIRE path (error points at the keep clause).
        assertMemoryLeak(() -> {
            execute("create table base (__qdb_re_keep int, v double, k symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows keep highest v partition by k",
                    43,
                    "cannot be used on a view with a column named '__qdb_re_keep'"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testReadFilterCorrectForNonMonotonicFuturePredicate() throws Exception {
        // ts > now() is NON-MONOTONIC: a future-dated row is hidden now but must REAPPEAR once now() advances
        // past its timestamp. The read filter recomputes now() on every read, so it stays correct regardless.
        // (Physical cleanup is documented as unsafe for such predicates -- this locks in the read-side invariant
        // that the row is only hidden, never logically gone.)
        assertMemoryLeak(() -> {
            setCurrentMicros(1_704_844_800_000_000L); // 2024-01-10T00:00:00Z
            execute("create table base (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts > now()");
            execute("insert into base values ('PAST', '2024-01-05T00:00:00.000000Z')");   // < now -> kept
            execute("insert into base values ('FUTURE', '2024-01-20T00:00:00.000000Z')"); // > now -> hidden
            drainWalAndMatViewQueues();

            // At 2024-01-10 only the past row is visible.
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\n" + "PAST\n");

            // Advance now() beyond the future row's ts: it reappears (it was only hidden, never deleted).
            setCurrentMicros(1_706_140_800_000_000L); // 2024-01-25T00:00:00Z
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\n" + "PAST\n" + "FUTURE\n");
        });
    }

    @Test
    public void testReadFilterTimestampNullConstantKeepsAllRows() throws Exception {
        // ts < cast(null as timestamp) is never TRUE, so NO row expires -- all rows stay visible. The
        // null-unsafe flip (NOT(ts < T) -> ts >= T) would have produced `ts >= NULL` and hidden EVERY row
        // (read/cleanup divergence: cleanup keeps them). The provably-non-null guard keeps the CASE form here.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts < cast(null as timestamp)");
            execute("insert into base values ('A', '2024-01-05T00:00:00.000000Z')");
            execute("insert into base values ('B', '2024-01-20T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            assertQuery("select sym from mv order by ts").noLeakCheck().returns("sym\n" + "A\n" + "B\n");
        });
    }

    @Test
    public void testApplyTimeGuardIgnoresExpireOnPlainTable() throws Exception {
        // Defense-in-depth (apply/write side): setMetaExpiry must NOT persist a policy onto a non-mat-view,
        // even if a malformed/forged alter reaches the writer (the SQL compiler already rejects this; here we
        // bypass it by calling the writer directly). It logs + skips rather than throwing -- a throw on WAL
        // apply would suspend the table.
        assertMemoryLeak(() -> {
            execute("create table t (a int, ts timestamp) timestamp(ts) partition by day bypass wal");
            final TableToken token = engine.verifyTableName("t");
            try (TableWriter writer = getWriter("t")) {
                writer.setMetaExpiry("a < 2", 3_600_000_000L);
            }
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                org.junit.Assert.assertNull("policy must not persist on a plain table", metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testCreateLatestOnViewWithExpireRejected() throws Exception {
        // A LATEST ON defining query is NOT a 1:1 passthrough -- its rows are a per-key reduction of the base
        // -- so it must not be classified as a passthrough mat view, otherwise EXPIRE ROWS' physical cleanup
        // would delete rows from a derived view. (LATEST ON lowers onto a NESTED model, which the prior
        // top-model-only passthrough check missed.) It is rejected as a non-passthrough/non-aggregating mat
        // view before EXPIRE is evaluated. Regression guard for the passthrough-classification fix.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mvlo as (select * from base latest on ts partition by sym) EXPIRE ROWS WHEN v < 2.0",
                    34,
                    "TIMESTAMP column is not present in select list"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mvlo"));
        });
    }

    @Test
    public void testMatViewExpirePersistsAfterReopen() throws Exception {
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0 cleanup every 15m");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");

            // Drop pooled readers/writers and re-read the policy from disk (_meta).
            engine.releaseInactive();
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertEquals("v < 2.0", metadata.getExpiryPredicate());
                assertEquals(15 * 60_000_000L, metadata.getExpiryCleanupIntervalMicros());
            }
        });
    }

    @Test
    public void testPoliciedMatViewInJoinHidesExpiredRows() throws Exception {
        // A policied mat view referenced inside a JOIN must still have the read filter applied on its side,
        // so expired view rows are hidden from the join result.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values ('AAA', 1.0, '2024-01-05T00:00:00.000000Z')"); // v<2 -> expired in mv
            execute("insert into base values ('BBB', 5.0, '2024-01-06T00:00:00.000000Z')"); // v>=2 -> live in mv
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN v < 2.0");
            drainWalAndMatViewQueues();

            execute("create table dim (sym symbol, label string)");
            execute("insert into dim values ('AAA', 'a')");
            execute("insert into dim values ('BBB', 'b')");

            // Only BBB survives in mv (v>=2), so the join yields only BBB even though dim has AAA.
            assertQuery("select mv.sym, mv.v, dim.label from mv join dim on mv.sym = dim.sym order by mv.sym").noLeakCheck().returns("sym\tv\tlabel\n" +
                            "BBB\t5.0\tb\n");
        });
    }

    @Test
    public void testCreateExpireRowsUnbalancedOpenParenRejected() throws Exception {
        // M4: an open paren that is never closed must be detected, not silently swallow the trailing
        // CLEANUP clause into the predicate text (which would also drop the custom cleanup interval).
        // The error is reported at the start of the predicate.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN (x > 1 CLEANUP EVERY 1h",
                    69,
                    "unbalanced parentheses in EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateExpireRowsUnbalancedCloseParenRejected() throws Exception {
        // M4: a ')' with no matching '(' (depth would go negative) must be flagged at the offending ')'.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) EXPIRE ROWS WHEN x > 1) AND y",
                    74,
                    "unbalanced parentheses in EXPIRE ROWS predicate"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCreateExpireRowsBalancedParensWithCleanupParses() throws Exception {
        // M4 regression guard: a fully-balanced parenthesised predicate followed by CLEANUP EVERY must
        // still parse, with CLEANUP terminating the predicate (not swallowed) and the custom interval kept.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN (v < 2.0) CLEANUP EVERY 30m");
            sink.clear();
            printSql("SHOW CREATE MATERIALIZED VIEW mv", sink);
            final String ddl = sink.toString();
            org.junit.Assert.assertTrue("expected balanced predicate in: " + ddl, ddl.contains("EXPIRE ROWS WHEN (v < 2.0)") || ddl.contains("EXPIRE ROWS WHEN v < 2.0"));
            org.junit.Assert.assertTrue("expected custom CLEANUP interval in: " + ddl, ddl.contains("CLEANUP EVERY 30m"));
        });
    }

    @Test
    public void testCreateExpireRowsColumnNamedCleanupParses() throws Exception {
        // NIT: a column reference literally named "cleanup" must be treated as predicate content, not as
        // the CLEANUP clause boundary. CLEANUP is a boundary only when immediately followed by EVERY.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, cleanup double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) EXPIRE ROWS WHEN cleanup > 5");
            final String predicate;
            try (TableMetadata m = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                predicate = m.getExpiryPredicate();
            }
            org.junit.Assert.assertNotNull(predicate);
            org.junit.Assert.assertTrue("predicate should reference the cleanup column: " + predicate, predicate.contains("cleanup"));
        });
    }

    @Test
    public void testCreateExpireRowsNonParenSelectRejected() throws Exception {
        // #4 (decision: not supported): EXPIRE ROWS is only reachable when the SELECT is parenthesised.
        // In the non-paren AS form the SELECT parser greedily consumes EXPIRE as a table alias for "base"
        // and then reports the following token (ROWS) as unexpected. The statement is still rejected (the
        // view is not created); supporting the bare form would require the DML parser to stop at EXPIRE.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as select * from base EXPIRE ROWS WHEN v < 2.0",
                    57,
                    "unexpected token [ROWS]"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testAlterMatViewDropExpireUnexpectedTokenRejected() throws Exception {
        // DROP EXPIRE [ROWS] accepts nothing else after it; a trailing token is a clear syntax error, not
        // silently ignored (which could mask a typo intended as a different clause).
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when v < 2.0");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop expire foo",
                    39,
                    "unexpected token [foo] while trying to drop row-expiry policy"
            );
            // The policy is untouched.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNotNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterMatViewDropNonExpireRejected() throws Exception {
        // ALTER MATERIALIZED VIEW ... DROP is only valid as DROP EXPIRE; any other continuation (or none)
        // must report "'expire' expected".
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop ttl",
                    32,
                    "'expire' expected"
            );
            assertExceptionNoLeakCheck(
                    "alter materialized view mv drop",
                    31,
                    "'expire' expected"
            );
        });
    }

    @Test
    public void testAlterMatViewSetExpireInvalidPredicateRejected() throws Exception {
        // ALTER ... SET EXPIRE validates the predicate by probing it against the (existing) view; an unknown
        // column surfaces as "invalid EXPIRE ROWS predicate" and the policy is not applied.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when no_such < 2.0",
                    48,
                    "invalid EXPIRE ROWS predicate"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterMatViewSetExpireTrailingTokenRejected() throws Exception {
        // A well-formed clause followed by a stray token (after CLEANUP EVERY here) is a syntax error, so a
        // typo cannot silently persist a truncated policy.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base)");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view mv set expire rows when v < 2.0 cleanup every 1h foo",
                    73,
                    "unexpected token [foo] while trying to set row-expiry policy"
            );
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("mv"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
            }
        });
    }

    @Test
    public void testAlterTableDropExpireRejected() throws Exception {
        // EXPIRE ROWS is materialized-view-only, so ALTER TABLE ... DROP EXPIRE on a plain table must give the
        // specific message rather than a generic one.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "alter table base drop expire",
                    22,
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testAlterTableSetExpireRejected() throws Exception {
        // Same for ALTER TABLE ... SET EXPIRE on a plain table.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "alter table base set expire rows when v < 2.0",
                    21,
                    "EXPIRE ROWS is only supported on materialized views"
            );
        });
    }

    @Test
    public void testCleanupIntervalTooLargeRejected() throws Exception {
        // A CLEANUP EVERY multiple that overflows when converted to micros must fail cleanly at parse time
        // rather than persisting a garbage (possibly negative) cadence.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 999999999w",
                    91,
                    "cleanup interval is too large"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCleanupIntervalUnsupportedUnitRejected() throws Exception {
        // CLEANUP EVERY accepts s/m/h/d/w; a month/year unit is a valid stride unit elsewhere but not a
        // cleanup cadence, so it is rejected with the specific message.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when v < 2.0 cleanup every 1y",
                    91,
                    "unsupported cleanup interval unit, expected s/m/h/d/w"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testCleanupJobRunSeriallyReclaimsOldPartition() throws Exception {
        // Drive the background job via run() (the discovery sweep) rather than calling cleanupTable directly:
        // runSerially() must find the policied view through the metadata cache and reclaim its wholly-expired
        // non-active partition.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            execute("insert into base values " +
                    "('A', 1.0, '2024-01-01T00:00:00.000000Z')," +
                    "('B', 2.0, '2024-01-02T00:00:00.000000Z')," +
                    "('C', 3.0, '2024-01-03T00:00:00.000000Z')");
            drainWalAndMatViewQueues();
            execute("create materialized view mv as (select * from base) expire rows when ts < '2024-01-02T00:00:00.000000Z'");
            drainWalAndMatViewQueues();
            // Read the view so it is hydrated into the metadata cache the discovery sweep scans.
            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n3\n");

            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                assertTrue("discovery sweep should reclaim", job.run(Job.RUNNING_STATUS));
            }
            drainWalAndMatViewQueues();

            assertQuery("select count() p from table_partitions('mv')").noRandomAccess().expectSize().noLeakCheck().returns("p\n2\n");
            assertQuery("select sym, v from mv order by sym").noLeakCheck().returns("sym\tv\n" +
                            "B\t2.0\n" +
                            "C\t3.0\n");
        });
    }

    @Test
    public void testEmptyWhenPredicateRejected() throws Exception {
        // WHEN with no predicate text before the next boundary (CLEANUP here) is a syntax error.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, v double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (select * from base) expire rows when cleanup every 1h",
                    69,
                    "EXPIRE ROWS WHEN predicate is empty"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }
}
