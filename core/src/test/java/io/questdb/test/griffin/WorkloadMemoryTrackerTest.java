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

package io.questdb.test.griffin;

import io.questdb.PropertyKey;
import io.questdb.test.AbstractCairoTest;
import org.junit.Before;
import org.junit.Test;

/**
 * SQL-level tests for the per-workload memory limit on the two non-QUERY
 * workloads: {@code MAT_VIEW_REFRESH} and {@code WAL_APPLY}.
 * <p>
 * A materialized view refresh ({@code MatViewRefreshJob}) and a WAL apply batch
 * ({@code ApplyWal2TableJob}) each pre-acquire their workload-specific tracker
 * and bind it on their execution context, so the SQL they compile and run
 * inherits it through the {@code QueryRegistry} nesting check rather than a
 * fresh QUERY tracker.
 * <p>
 * MAT_VIEW_REFRESH runs an arbitrary SELECT, so a runaway aggregation reaches a
 * tracker-wired allocator (the GROUP BY map) and breaches the refresh limit; the
 * failure invalidates the view ({@code materialized_views.view_status} /
 * {@code invalidation_reason}). WAL_APPLY only ever runs simple, join- and
 * subquery-free UPDATEs (richer forms are rejected for WAL tables at submission)
 * plus metadata ALTERs and data commits, none of which reach a tracker-wired
 * allocator -- writer/O3 memory is out of scope by design. To still drive a real
 * WAL_APPLY breach, the WAL tests use the dev-mode {@code alloc_tracked(l)} test
 * function, which allocates through the bound tracker; applied inside an UPDATE
 * its allocation charges the WAL_APPLY tracker, so a large size breaches it and
 * suspends the table ({@code wal_tables().suspended} / {@code errorMessage}).
 * <p>
 * The limits are applied per test in {@link #setUp()} via
 * {@code node1.setProperty} so they survive the per-test override reset; the
 * provider reads them live on each tracker acquisition. The QUERY limit is left
 * unlimited so the test's own setup and assertion queries are never constrained
 * -- only the two background workloads are.
 */
public class WorkloadMemoryTrackerTest extends AbstractCairoTest {

    @Before
    public void setUp() {
        super.setUp();
        // 512 KiB clears the small-workload initial allocations (group-by maps) for
        // the success cases, while the large-workload breach case (tens of thousands
        // of distinct keys) exceeds it after a few heap doublings. Applied per test
        // so they survive the per-test override reset; the provider reads them live.
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_MEMORY_LIMIT_BYTES, 512 * 1024L);
        node1.setProperty(PropertyKey.CAIRO_WAL_APPLY_MEMORY_LIMIT_BYTES, 512 * 1024L);
        // Drop the per-retry backoff sleep: a per-query breach is deterministic
        // across the refresh's step-reduction retries, so the sleeps only slow the
        // test down before the refresh gives up and invalidates the view.
        node1.setProperty(PropertyKey.CAIRO_MAT_VIEW_REFRESH_OOM_RETRY_TIMEOUT, 0);
        // alloc_tracked(l), used by the WAL tests, is dev-mode only. Setting it on the
        // node after super.setUp() keeps dev mode enabled for every test; the static
        // setProperty form does not stick across the class's tests.
        node1.setProperty(PropertyKey.DEV_MODE_ENABLED, true);
    }

    @Test
    public void testMatViewRefreshFailsOnLargeKeySet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            // 50,000 distinct keys packed into a single 1h bucket, so the refresh's
            // GROUP BY map holds all 50,000 groups at once and breaches 512 KiB.
            execute(
                    "INSERT INTO base SELECT ('k' || x)::symbol, x::double, " +
                            "timestamp_sequence('2024-01-01T00:00:00.000000Z', 1) FROM long_sequence(50_000)"
            );
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT k, last(v) AS v, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();

            // The refresh breached the MAT_VIEW_REFRESH limit, so the view is invalid
            // and its reason carries the per-query message tagged with the workload.
            assertQuery("SELECT view_status, " +
                    "invalidation_reason LIKE '%query memory limit exceeded%' AS oom, " +
                    "invalidation_reason LIKE '%workload=MAT_VIEW_REFRESH%' AS workload " +
                    "FROM materialized_views WHERE view_name = 'mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\toom\tworkload\n" +
                            "invalid\ttrue\ttrue\n");
        });
    }

    @Test
    public void testMatViewRefreshRepeatedRunsReleaseAllocations() throws Exception {
        // The load-bearing check is assertMemoryLeak around repeated refresh cycles:
        // a malloc/free asymmetry across the tracker boundary shows up as a residual
        // native allocation at the end, and recordPerQueryMemAlloc's assert mem >= 0
        // is live on every refresh map malloc/free under the default-bound tracker.
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT k, last(v) AS v, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            for (int i = 0; i < 5; i++) {
                execute(
                        "INSERT INTO base SELECT ('k' || (x % 5))::symbol, x::double, " +
                                "timestamp_sequence('2024-01-0" + (i + 1) + "T00:00:00.000000Z', 1) FROM long_sequence(100)"
                );
                drainWalAndMatViewQueues();
            }
            assertQuery("SELECT view_status FROM materialized_views WHERE view_name = 'mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\n" +
                            "valid\n");
        });
    }

    @Test
    public void testMatViewRefreshSucceedsOnSmallKeySet() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE base (k SYMBOL, v DOUBLE, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute(
                    "INSERT INTO base SELECT ('k' || (x % 5))::symbol, x::double, " +
                            "timestamp_sequence('2024-01-01T00:00:00.000000Z', 1) FROM long_sequence(100)"
            );
            execute("CREATE MATERIALIZED VIEW mv AS (SELECT k, last(v) AS v, ts FROM base SAMPLE BY 1h) PARTITION BY DAY");
            drainWalAndMatViewQueues();

            assertQuery("SELECT view_status FROM materialized_views WHERE view_name = 'mv'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("view_status\n" +
                            "valid\n");
        });
    }

    @Test
    public void testWalApplyUpdateFailsOnTrackedAlloc() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
            drainWalQueue();

            // alloc_tracked() allocates through the tracker bound while the UPDATE is
            // applied: the WAL_APPLY one. 1 MiB exceeds the 512 KiB WAL_APPLY limit,
            // so apply fails and the table is suspended with the per-query message.
            execute("UPDATE t SET v = alloc_tracked(1_048_576)");
            drainWalQueue();

            assertQuery("SELECT name, suspended, " +
                    "errorMessage LIKE '%query memory limit exceeded%' AS oom, " +
                    "errorMessage LIKE '%workload=WAL_APPLY%' AS workload " +
                    "FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("name\tsuspended\toom\tworkload\n" +
                            "t\ttrue\ttrue\ttrue\n");
        });
    }

    @Test
    public void testWalApplyUpdateRepeatedRunsReleaseAllocations() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
            drainWalQueue();

            // Each apply charges a small alloc_tracked() allocation to that batch's
            // WAL_APPLY tracker and frees it on cursor close. assertMemoryLeak (plus the
            // live recordPerQueryMemAlloc assert) catches an unbalanced charge/free or a
            // tracker leak across the repeated batches.
            for (int i = 0; i < 5; i++) {
                execute("UPDATE t SET v = alloc_tracked(1024)");
                drainWalQueue();
            }
            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\n" +
                            "false\n");
        });
    }

    @Test
    public void testWalApplyUpdateSucceedsUnderActiveLimit() throws Exception {
        assertMemoryLeak(() -> {
            execute("CREATE TABLE t (k INT, v LONG, ts TIMESTAMP) TIMESTAMP(ts) PARTITION BY DAY WAL");
            execute("INSERT INTO t SELECT x::int, x::long, timestamp_sequence('2024-01-01', 1_000_000) FROM long_sequence(10)");
            drainWalQueue();

            // A small alloc_tracked() allocation stays under the WAL_APPLY limit, so the
            // UPDATE applies cleanly: it charges and frees the bound tracker symmetrically
            // and sets every row to 42.
            execute("UPDATE t SET v = alloc_tracked(1024)");
            drainWalQueue();

            assertQuery("SELECT suspended FROM wal_tables() WHERE name = 't'")
                    .noLeakCheck()
                    .noRandomAccess()
                    .returns("suspended\n" +
                            "false\n");
            // alloc_tracked() returns 42 for all 10 rows -> sum 420. The non-keyed
            // aggregate cursor reports a fixed size of 1, hence the explicit expectSize.
            assertQuery("SELECT sum(v) AS sum FROM t")
                    .noLeakCheck()
                    .noRandomAccess()
                    .expectSize()
                    .returns("sum\n" +
                            "420\n");
        });
    }
}
