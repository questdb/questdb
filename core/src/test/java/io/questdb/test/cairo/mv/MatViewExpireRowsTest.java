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
import io.questdb.cairo.TableWriter;
import io.questdb.cairo.sql.TableMetadata;
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
 * EXPIRE ROWS is passthrough-only: an aggregating (SAMPLE BY) view is rejected at CREATE
 * ({@link #testCreateAggregatingMatViewWithExpireRejected()}).
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
            assertSql("k\tv\nA\t4.0\nB\t5.0\n", "select k, v from mv order by k");
            assertSql("c\n2\n", "select count() c from mv");
            assertSql("c\n2\n", "select count(distinct k) c from mv");
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

            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
            assertSql(
                    "sym\tv\n" +
                            "B\t5.0\n" +
                            "D\t9.0\n",
                    "select sym, v from mv order by sym"
            );
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

            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

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
            assertSql("p\n2\n", "select count() p from table_partitions('mv')");
            assertSql(
                    "sym\tv\n" +
                            "B\t2.0\n" +
                            "C\t3.0\n",
                    "select sym, v from mv order by sym"
            );
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
            assertSql(
                    "sym\tv\n" +
                            "B\t5.0\n" +
                            "C\tnull\n",
                    "select sym, v from mv order by sym"
            );
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
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");

            // Second sweep: nothing expired remains -> no work, partitions unchanged.
            final boolean second;
            try (RowExpiryCleanupJob job = new RowExpiryCleanupJob(engine)) {
                second = job.cleanupTable(token, predicate);
            }
            drainWalAndMatViewQueues();
            assertFalse("second sweep must be a no-op", second);
            assertSql("p\tr\n2\t2\n", "select count() p, sum(numRows) r from table_partitions('mv')");
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
            assertSql("p\n3\n", "select count() p from table_partitions('mv')");

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
            assertSql("p\n1\n", "select count() p from table_partitions('mv')");
            assertSql("sym\tv\nC\t3.0\n", "select sym, v from mv order by sym");
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
            assertSql(
                    "sym\tv\n" +
                            "BBB\t5.0\n",
                    "select sym, v from mv order by sym"
            );

            // Drop the policy: all rows visible again.
            execute("alter materialized view mv drop expire");
            drainWalAndMatViewQueues();

            final TableToken token = engine.verifyTableName("mv");
            try (TableMetadata metadata = engine.getTableMetadata(token)) {
                assertNull(metadata.getExpiryPredicate());
                assertEquals(0, metadata.getExpiryCleanupIntervalMicros());
            }
            assertSql(
                    "sym\tv\n" +
                            "AAA\t1.0\n" +
                            "BBB\t5.0\n",
                    "select sym, v from mv order by sym"
            );
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
            assertSql(
                    "sym\tv\n" +
                            "BBB\t2.5\n",
                    "select sym, v from mv order by sym"
            );
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
            assertSql(
                    "sym\tv\n" +
                            "BBB\t2.5\n",
                    "select sym, v from mv2 order by sym"
            );
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
    public void testCreateAggregatingMatViewWithExpireRejected() throws Exception {
        // EXPIRE ROWS is passthrough-only, for EVERY mode (scalar WHEN included): the cleanup job physically
        // reclaims rows, which only makes sense when the view mirrors base rows 1:1. An aggregating (SAMPLE BY)
        // view's rows are derived, so the policy is rejected at CREATE and the view must not be left behind.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            assertExceptionNoLeakCheck(
                    "create materialized view mv as (" +
                            "select sym, last(price) price, ts from base sample by 1h" +
                            ") partition by day EXPIRE ROWS WHEN ts < dateadd('d', -1, now())",
                    25,
                    "EXPIRE ROWS is only supported on passthrough (non-aggregating) materialized views"
            );
            org.junit.Assert.assertNull(engine.getTableTokenIfExists("mv"));
        });
    }

    @Test
    public void testAlterAggregatingMatViewSetExpireRejected() throws Exception {
        // The passthrough-only rule also applies to ALTER ... SET EXPIRE: an existing aggregating (SAMPLE BY)
        // view cannot have a policy attached after the fact.
        assertMemoryLeak(() -> {
            execute("create table base (sym symbol, price double, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view agg as (select sym, last(price) price, ts from base sample by 1h) partition by day");
            drainWalAndMatViewQueues();
            assertExceptionNoLeakCheck(
                    "alter materialized view agg set expire rows when ts < dateadd('d', -1, now())",
                    24,
                    "EXPIRE ROWS is only supported on passthrough (non-aggregating) materialized views"
            );
            // The view must be left without a policy.
            try (TableMetadata metadata = engine.getTableMetadata(engine.verifyTableName("agg"))) {
                org.junit.Assert.assertNull(metadata.getExpiryPredicate());
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
            assertSql("sym\tv\n" + "AAA\t1.0\n", "select sym, v from cloned");
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
            setCurrentMicros(1704844800000000L); // 2024-01-10T00:00:00Z
            execute("create table base (sym symbol, ts timestamp) timestamp(ts) partition by day wal");
            execute("create materialized view mv as (select * from base) expire rows when ts > now()");
            execute("insert into base values ('PAST', '2024-01-05T00:00:00.000000Z')");   // < now -> kept
            execute("insert into base values ('FUTURE', '2024-01-20T00:00:00.000000Z')"); // > now -> hidden
            drainWalAndMatViewQueues();

            // At 2024-01-10 only the past row is visible.
            assertSql("sym\n" + "PAST\n", "select sym from mv order by ts");

            // Advance now() beyond the future row's ts: it reappears (it was only hidden, never deleted).
            setCurrentMicros(1706140800000000L); // 2024-01-25T00:00:00Z
            assertSql("sym\n" + "PAST\n" + "FUTURE\n", "select sym from mv order by ts");
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
            assertSql(
                    "sym\tv\tlabel\n" +
                            "BBB\t5.0\tb\n",
                    "select mv.sym, mv.v, dim.label from mv join dim on mv.sym = dim.sym order by mv.sym"
            );
        });
    }
}
