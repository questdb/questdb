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

import io.questdb.cairo.lv.LiveViewRefreshJob;
import io.questdb.griffin.SqlException;
import io.questdb.mp.Job;
import io.questdb.test.AbstractCairoTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Phase 1 lifecycle and catalogue tests for live views (RFC 123). Complements
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
        Job.RunStatus status = () -> false;
        boolean any = false;
        for (int i = 0; i < 64 && job.run(0, status); i++) {
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
            assertSql(
                    "table_type\nL\n",
                    "SELECT table_type FROM tables() WHERE table_name = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testInformationSchemaTablesShowsLiveView() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertSql(
                    "table_type\tis_insertable_into\n" +
                            "LIVE VIEW\tfalse\n",
                    "SELECT table_type, is_insertable_into FROM information_schema.tables() " +
                            "WHERE table_name = 'lv'"
            );
            execute("DROP LIVE VIEW lv");
        });
    }

    @Test
    public void testPgClassReportsLiveViewAsRelkindV() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val INT, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                    "SELECT val, ts, row_number() OVER () AS rn FROM base");
            assertSql(
                    "relkind\nv\n",
                    "SELECT relkind FROM pg_class() WHERE relname = 'lv'"
            );
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
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view select may only use window functions that support incremental refresh")
                );
            }
        });
    }

    @Test
    public void testRejectNonSnapshotCapableWindowFunction() throws Exception {
        // nth_value()'s bounded RANGE variant (OverPartitionRangeFrameFunction)
        // remains unmigrated and still trips the snapshot-capable gate. Earlier
        // Step 1 / 2 / 3 migrations made the anchored unbounded, the
        // non-anchored UNBOUNDED PRECEDING + K PRECEDING, and the bounded ROWS
        // X-Y variants snapshot-capable. Keep the asserted-wording check pinned
        // against the only remaining unsupported partitioned variant so future
        // migrations either remove this test or repoint it.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (val LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY HOUR WAL");
            try {
                execute("CREATE LIVE VIEW lv FLUSH EVERY 1s AS " +
                        "SELECT val, ts, nth_value(val, 3) OVER w AS third FROM base " +
                        "WINDOW w AS (PARTITION BY val ORDER BY ts RANGE BETWEEN '5' MINUTE PRECEDING AND CURRENT ROW)");
                Assert.fail("expected SqlException for non-snapshot-capable window function");
            } catch (SqlException e) {
                Assert.assertTrue(
                        e.getMessage(),
                        e.getMessage().contains("live view select cannot use window function nth_value(); " +
                                "incremental snapshot is not supported for this function yet")
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

            assertSql(
                    "val\tts\trn\n" +
                            "10\t2026-01-01T00:01:00.000000Z\t1\n" +
                            "20\t2026-01-01T00:03:00.000000Z\t2\n" +
                            "30\t2026-01-01T00:04:00.000000Z\t3\n",
                    "SELECT val, ts, rn FROM lv ORDER BY ts"
            );
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

            assertSql(
                    "val\tts\trn\n" +
                            "10\t2026-01-01T00:00:00.000000Z\t1\n" +
                            "20\t2026-01-01T00:01:00.000000Z\t2\n" +
                            "30\t2026-01-01T00:02:00.000000Z\t3\n" +
                            "40\t2026-01-01T00:03:00.000000Z\t4\n",
                    "SELECT val, ts, rn FROM lv ORDER BY ts"
            );
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

            assertSql("count\n0\n", "SELECT count() FROM lv");
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
            assertSql(
                    "column\ttype\tindexed\tindexBlockCapacity\tsymbolCached\tsymbolCapacity\tsymbolTableSize\tdesignated\tupsertKey\tindexType\tindexInclude\n" +
                            "sym\tSYMBOL\tfalse\t0\ttrue\t128\t0\tfalse\tfalse\t\t\n" +
                            "price\tDOUBLE\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n" +
                            "ts\tTIMESTAMP\tfalse\t0\tfalse\t0\t0\ttrue\tfalse\t\t\n" +
                            "rn\tLONG\tfalse\t0\tfalse\t0\t0\tfalse\tfalse\t\t\n",
                    "SHOW COLUMNS FROM lv"
            );
            execute("DROP LIVE VIEW lv");
        });
    }
}
