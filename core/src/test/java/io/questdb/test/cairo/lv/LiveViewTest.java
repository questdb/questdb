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

package io.questdb.test.cairo.lv;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.WindowSPI;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.engine.functions.window.BaseWindowFunction;
import io.questdb.griffin.engine.window.WindowFunction;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Lifecycle and catalogue tests for live views. Complements
 * {@link LiveViewSmokeTest} (CREATE / DROP / refresh / restart / anchor reset)
 * with surface coverage:
 * <ul>
 *     <li>CREATE IF NOT EXISTS / DROP IF EXISTS idempotency.</li>
 *     <li>DROP of a non-existent live view raises an asserted-wording error.</li>
 *     <li>{@code tables()} reports the LV with table_type='L'.</li>
 *     <li>{@code information_schema.tables()} reports it as LIVE VIEW and not insertable.</li>
 *     <li>{@code pg_class()} reports relkind='v'.</li>
 *     <li>{@code SHOW COLUMNS} reflects the LV's projected schema, including the
 *     timestamp designation.</li>
 * </ul>
 */
public class LiveViewTest extends AbstractCairoTest {

    private static boolean drainJob(Job job) {
        boolean any = false;
        for (int i = 0; i < 64 && job.run(); i++) {
            any = true;
        }
        return any;
    }

    private void assertMutationRejected(String sql, String expectedMessageFragment) throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            try {
                execute(sql);
                Assert.fail("expected SqlException for " + sql);
            } catch (SqlException e) {
                Assert.assertTrue(
                        "expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage(),
                        e.getMessage().contains(expectedMessageFragment)
                );
            }
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testCreateLiveViewIfNotExists() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            // IF NOT EXISTS should succeed when the view already exists.
            execute("CREATE LIVE VIEW IF NOT EXISTS lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testDropLiveViewIfExists() throws Exception {
        assertMemoryLeak(() -> {
            // No view exists yet — IF EXISTS must swallow the error.
            execute("DROP LIVE VIEW IF EXISTS nonexistent");
        });
    }

    @Test
    public void testDropNonExistentLiveViewFails() throws Exception {
        assertMemoryLeak(() -> {
            try {
                execute("DROP LIVE VIEW nonexistent");
                Assert.fail("expected SqlException for missing live view");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view does not exist")
                );
            }
        });
    }

    @Test
    public void testTablesShowsLiveViewWithTypeL() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT table_type FROM tables() WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_type\nL\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInformationSchemaTablesShowsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT table_type, is_insertable_into FROM information_schema.tables() " +
                            "WHERE table_name = 'lv'").noLeakCheck().noRandomAccess().returns("table_type\tis_insertable_into\n" +
                            "LIVE VIEW\tfalse\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPgClassReportsLiveViewAsRelkindV() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertQuery("SELECT relkind FROM pg_class() WHERE relname = 'lv'").noLeakCheck().noRandomAccess().returns("relkind\nv\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRejectAlterTable() throws Exception {
        assertMutationRejected(
                "ALTER TABLE lv ADD COLUMN x INT",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectInsertInto() throws Exception {
        assertMutationRejected(
                "INSERT INTO lv VALUES (1, '2026-01-01T00:00:00.000000Z', 1)",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectUpdate() throws Exception {
        assertMutationRejected(
                "UPDATE lv SET val = 0",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectTruncate() throws Exception {
        assertMutationRejected(
                "TRUNCATE TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectReindex() throws Exception {
        assertMutationRejected(
                "REINDEX TABLE lv COLUMN val LOCK EXCLUSIVE",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectVacuum() throws Exception {
        assertMutationRejected(
                "VACUUM TABLE lv",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectRename() throws Exception {
        assertMutationRejected(
                "RENAME TABLE lv TO lv2",
                "cannot modify live view [view=lv]"
        );
    }

    @Test
    public void testRejectDropTableOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP TABLE lv",
                "table name expected, got live view name: lv"
        );
    }

    @Test
    public void testRejectDropViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP VIEW lv",
                "view name expected, got table or materialized view name"
        );
    }

    @Test
    public void testRejectDropMaterializedViewOnLiveView() throws Exception {
        assertMutationRejected(
                "DROP MATERIALIZED VIEW lv",
                "materialized view name expected, got table or view name"
        );
    }

    @Test
    public void testRejectDedupBase() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, val INT, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL DEDUP UPSERT KEYS(ts, sym)");
            final String sql = "CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, val, ts, row_number() OVER () AS rn FROM base";
            try {
                execute(sql);
                Assert.fail("expected SqlException for DEDUP base");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view cannot be created over a base table with DEDUP keys")
                );
                // Position must point at the base table name, not the view name.
                Assert.assertEquals(
                        "position must point at the base table name",
                        sql.lastIndexOf("base"),
                        e.getPosition()
                );
            }
        });
    }

    @Test
    public void testRejectMissingWindowFunction() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS SELECT val, ts FROM base");
                Assert.fail("expected SqlException for missing window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view select must contain at least one window function")
                );
            }
        });
    }

    @Test
    public void testRejectNonSnapshotCapableWindowFunction() {
        // Drive the validation directly with a ZERO_PASS stub lacking snapshot support to
        // pin the exact user-facing message and position deterministically. The same reject
        // also fires through real SQL for un-partitioned aggregate windows - see
        // testRejectUnpartitionedAggregateWindowFunction - but the stub gives a stable
        // function name (test_no_snapshot) and an explicit position to assert on.
        try {
            CairoEngine.validateLiveViewWindowFunction(new NonSnapshotWindowFunction(), 42);
            Assert.fail("expected SqlException for non-snapshot-capable window function");
        } catch (SqlException e) {
            Assert.assertEquals(42, e.getPosition());
            Assert.assertTrue(
                    e.getMessage(),
                    e.getMessage().contains("live view select cannot use window function test_no_snapshot(); incremental snapshot is not supported for this function yet")
            );
        }
    }

    @Test
    public void testRejectUnpartitionedAggregateWindowFunction() throws Exception {
        // An un-partitioned aggregate window (no PARTITION BY) is ZERO_PASS but has no
        // partition Map to snapshot, so it is not live-view-eligible and stays rejected
        // by nature - unlike the per-type migration train, this shape is never migratable.
        // It clears the pass-count check and hits the supportsSnapshot() reject in
        // validateLiveViewWindowFunction, exercising the real-SQL path the stub-driven
        // testRejectNonSnapshotCapableWindowFunction cannot reach. Adding a PARTITION BY to
        // the same query is accepted (the partitioned ZERO_PASS aggregate shapes are
        // migrated), so the missing partition is the sole reason these reject.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (ts TIMESTAMP, sym SYMBOL, v DOUBLE) TIMESTAMP(ts) PARTITION BY DAY WAL");

            // avg() over a bounded ROWS frame with no PARTITION BY.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, avg(v) OVER w AS a FROM base " +
                        "WINDOW w AS (ORDER BY ts ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)");
                Assert.fail("expected SqlException for un-partitioned avg() window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported for this function yet")
                );
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("avg"));
            }

            // sum() over a bounded RANGE frame with no PARTITION BY - same reject.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT ts, sym, sum(v) OVER w AS a FROM base " +
                        "WINDOW w AS (ORDER BY ts RANGE BETWEEN '2' HOUR PRECEDING AND CURRENT ROW)");
                Assert.fail("expected SqlException for un-partitioned sum() window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("incremental snapshot is not supported for this function yet")
                );
                Assert.assertTrue(e.getMessage(), e.getMessage().contains("sum"));
            }
        });
    }

    @Test
    public void testRejectTwoPassWindowFunction() throws Exception {
        // ntile() is a TWO_PASS window function — incremental refresh cannot drive it
        // because the second pass needs the partition's total row count up front.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT val, ts, ntile(4) OVER w AS bucket FROM base " +
                        "WINDOW w AS (PARTITION BY val ORDER BY ts ANCHOR DAILY '00:00')");
                Assert.fail("expected SqlException for TWO_PASS window function");
            } catch (SqlException e) {
                // ntile compiles to a CachedWindowRecordCursorFactory, so the
                // factory-level reject fires - pin its multi-pass tail, not just
                // the shared prefix, so the two distinct reject messages stay
                // distinguishable across refactors.
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains(
                                "live view select may only use window functions that support incremental refresh; "
                                        + "this query requires caching or multi-pass evaluation")
                );
            }
        });
    }

    @Test
    public void testRejectJoinInLiveViewSelect() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base1 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE TABLE base2 (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            // The JOIN-shaped factory tree fails the validateLiveViewFactory check
            // that requires a single WAL base table at the leaf.
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT base1.val, base1.ts, row_number() OVER () AS rn FROM base1 " +
                        "JOIN base2 ON base1.ts = base2.ts");
                Assert.fail("expected SqlException for JOIN in live view select");
            } catch (SqlException e) {
                Assert.assertTrue(
                        "expected an LV-related rejection, got: " + e.getMessage(),
                        e.getMessage().contains("live view") || e.getMessage().contains("simple scan")
                );
            }
        });
    }

    @Test
    public void testRefreshWithWhereClause() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base WHERE val > 5");
            // Mix in rows that fail the filter (val <= 5) — they must NOT advance rn.
            execute("INSERT INTO base (val, ts) VALUES " +
                    "(1, '2026-01-01T00:00:00.000000Z'), " +
                    "(10, '2026-01-01T00:01:00.000000Z'), " +
                    "(3, '2026-01-01T00:02:00.000000Z'), " +
                    "(20, '2026-01-01T00:03:00.000000Z'), " +
                    "(30, '2026-01-01T00:04:00.000000Z')");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                            "10\t2026-01-01T00:01:00.000000Z\t1\n" +
                            "20\t2026-01-01T00:03:00.000000Z\t2\n" +
                            "30\t2026-01-01T00:04:00.000000Z\t3\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testMultipleRefreshBatchesAccumulateState() throws Exception {
        assertMemoryLeak(() -> {
            // Pin a deterministic clock so the FLUSH EVERY rate-limit (1s) does
            // not coalesce batch 2 into batch 1 when both run in the same millisecond.
            setCurrentMicros(0);
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                // Batch 1: rows 1, 2.
                execute("INSERT INTO base (val, ts) VALUES " +
                        "(10, '2026-01-01T00:00:00.000000Z'), " +
                        "(20, '2026-01-01T00:01:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();

                // Advance past FLUSH EVERY so batch 2's refresh is not rate-limited.
                setCurrentMicros(2_000_000L);

                // Batch 2: rows 3, 4 — rn must continue from 3, not restart at 1.
                execute("INSERT INTO base (val, ts) VALUES " +
                        "(30, '2026-01-01T00:02:00.000000Z'), " +
                        "(40, '2026-01-01T00:03:00.000000Z')");
                drainWalQueue();
                drainJob(job);
                drainWalQueue();
            }

            assertQuery("SELECT val, ts, rn FROM lv ORDER BY ts").noLeakCheck().timestamp("ts").expectSize().returns("val\tts\trn\n" +
                            "10\t2026-01-01T00:00:00.000000Z\t1\n" +
                            "20\t2026-01-01T00:01:00.000000Z\t2\n" +
                            "30\t2026-01-01T00:02:00.000000Z\t3\n" +
                            "40\t2026-01-01T00:03:00.000000Z\t4\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testRefreshOnEmptyBaseProducesNoRows() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            drainWalQueue();
            try (LiveViewRefreshJob job = new LiveViewRefreshJob(0, engine, 1)) {
                drainJob(job);
            }
            drainWalQueue();

            assertQuery("SELECT count() FROM lv").noLeakCheck().noRandomAccess().expectSize().returns("count\n0\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testShowColumnsReflectsLiveViewSchema() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (sym SYMBOL, price DOUBLE, ts TIMESTAMP) " +
                    "TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT sym, price, ts, row_number() OVER w AS rn FROM base " +
                    "WINDOW w AS (PARTITION BY sym ORDER BY ts ANCHOR DAILY '00:00')");
            assertQuery("SHOW COLUMNS FROM lv").noLeakCheck().noRandomAccess().returns("column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                            "sym\tSYMBOL\tfalse\t0\ttrue\t128\t0\tfalse\tfalse\t\t\n" +
                            "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                            "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n");
            execute("DROP LIVE VIEW lv");
        });
    }

    // A ZERO_PASS window function that does not support snapshots. No such GA function
    // exists, so this stub is the only way to reach (and pin) the supportsSnapshot reject.
    private static final class NonSnapshotWindowFunction extends BaseWindowFunction {
        NonSnapshotWindowFunction() {
            super(null);
        }

        @Override
        public String getName() {
            return "test_no_snapshot";
        }

        @Override
        public int getPassCount() {
            return WindowFunction.ZERO_PASS;
        }

        @Override
        public int getType() {
            return ColumnType.DOUBLE;
        }

        @Override
        public void pass1(Record record, long recordOffset, WindowSPI spi) {
        }
    }
}
